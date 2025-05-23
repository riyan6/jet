package org.riyan6.jet.core;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import io.netty.handler.traffic.TrafficCounter;
import org.riyan6.jet.handler.RelayHandler;
import org.riyan6.jet.rule.ForwardingRule;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * 管理客户端连接和到目标服务器的后端连接的建立与中继。
 */
public class ConnectionManager {
    private final EventLoopGroup workerGroup;
    private final ExecutorService virtualThreadExecutor;

    // Key: 后端 Channel, Value: 该 Channel 所属的 ForwardingRule ID
    private final Map<Channel, String> channelToRuleIdMap = new ConcurrentHashMap<>();
    // Key: 后端 Channel, Value: 该 Channel 上的 ChannelTrafficShapingHandler 实例
    private final Map<Channel, ChannelTrafficShapingHandler> activeShapersByChannel = new ConcurrentHashMap<>();

    public ConnectionManager(EventLoopGroup workerGroup, ExecutorService virtualThreadExecutor) {
        this.workerGroup = workerGroup;
        this.virtualThreadExecutor = virtualThreadExecutor;
    }

    /**
     * 处理一个新的客户端接入连接。
     * 此方法应该在虚拟线程中执行。
     * @param frontendChannel 代表客户端连接的 Channel
     * @param rule 此连接匹配的转发规则
     */
    public void handleNewClientConnection(SocketChannel frontendChannel, ForwardingRule rule) {
        frontendChannel.config().setAutoRead(true);
        System.out.printf("[%s] 已接受规则 '%s' 的新连接，来自: %s. 前端 Channel ID: %s. 已设置 AutoRead=true.%n",
                Thread.currentThread().getName(), rule.getId(), frontendChannel.remoteAddress(), frontendChannel.id().asShortText());

        Bootstrap backendBootstrap = new Bootstrap();
        backendBootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel backendChannel) {
                        System.out.printf("[%s] 正在初始化后端 Channel %s (规则: '%s')，目标: %s:%d%n",
                                Thread.currentThread().getName(), backendChannel.id().asShortText(), rule.getId(), rule.getTargetIp(), rule.getTargetPort());

                        TrafficCounter sharedTrafficCounterForRelay = null;
                        if (rule.getRateLimitBps() > 0) {
                            ChannelTrafficShapingHandler shaper = new ChannelTrafficShapingHandler(
                                    rule.getRateLimitBps(), rule.getRateLimitBps(), 1000);
                            backendChannel.pipeline().addLast("trafficShaper", shaper);
                            activeShapersByChannel.put(backendChannel, shaper);
                            channelToRuleIdMap.put(backendChannel, rule.getId());
                            sharedTrafficCounterForRelay = shaper.trafficCounter();
                            System.out.printf("[%s] 已为后端 Channel %s (规则: '%s') 添加流量整形器.%n",
                                    Thread.currentThread().getName(), backendChannel.id().asShortText(), rule.getId());
                        } else {
                            System.out.printf("[%s] 后端 Channel %s (规则: '%s') 未添加流量整形器 (速率限制为 %d Bps).%n",
                                    Thread.currentThread().getName(), backendChannel.id().asShortText(), rule.getId(), rule.getRateLimitBps());
                        }

                        backendChannel.pipeline().addLast("backendRelay",
                                new RelayHandler(frontendChannel, rule, false, sharedTrafficCounterForRelay));

                        backendChannel.closeFuture().addListener(closeFuture -> {
                            String closedRuleId = channelToRuleIdMap.get(backendChannel);
                            System.out.printf("[%s] 后端 Channel %s (规则: '%s') 已关闭. 正在从活动映射中移除.%n",
                                    Thread.currentThread().getName(), backendChannel.id().asShortText(), closedRuleId != null ? closedRuleId : "N/A");
                            activeShapersByChannel.remove(backendChannel);
                            channelToRuleIdMap.remove(backendChannel);
                        });
                    }
                })
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
                .option(ChannelOption.SO_KEEPALIVE, true);

        ChannelFuture connectFuture = backendBootstrap.connect(rule.getTargetIp(), rule.getTargetPort());

        connectFuture.addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                SocketChannel backendChannel = (SocketChannel) future.channel();
                System.out.printf("[%s] 已成功连接到目标服务器 (规则: '%s'): %s. 后端 Channel ID: %s.%n",
                        Thread.currentThread().getName(), rule.getId(), backendChannel.remoteAddress(), backendChannel.id().asShortText());

                TrafficCounter counterForFrontendRelay = null;
                ChannelTrafficShapingHandler existingShaper = backendChannel.pipeline().get(ChannelTrafficShapingHandler.class);
                if (existingShaper != null) {
                    counterForFrontendRelay = existingShaper.trafficCounter();
                } else {
                    System.out.printf("[%s] 在后端 Channel %s (规则: '%s') 上未找到整形器，前端 RelayHandler 将不使用其计数器.%n",
                            Thread.currentThread().getName(), backendChannel.id().asShortText(), rule.getId());
                }
                frontendChannel.pipeline().addLast("frontendRelay",
                        new RelayHandler(backendChannel, rule, true, counterForFrontendRelay));
            } else {
                System.err.printf("[%s] 连接到目标服务器失败 (规则: '%s', 目标: %s:%d): %s.%n",
                        Thread.currentThread().getName(), rule.getId(), rule.getTargetIp(), rule.getTargetPort(), future.cause().getMessage());
                frontendChannel.close();
            }
        });

        frontendChannel.closeFuture().addListener(future -> {
            System.out.printf("[%s] 前端连接 %s (规则: '%s', 来自: %s) 已关闭.%n",
                    Thread.currentThread().getName(), frontendChannel.id().asShortText(), rule.getId(), frontendChannel.remoteAddress());
            if (connectFuture.isDone() && connectFuture.isSuccess() && connectFuture.channel() != null && connectFuture.channel().isActive()) {
                System.out.printf("[%s] 因前端连接 %s (规则: '%s') 关闭，正在关闭对应的后端 Channel %s.%n",
                        Thread.currentThread().getName(), frontendChannel.id().asShortText(), rule.getId(), connectFuture.channel().id().asShortText());
                connectFuture.channel().close();
            } else if (connectFuture.isDone() && !connectFuture.isSuccess()){
                System.out.printf("[%s] 前端连接 %s (规则: '%s') 关闭时，对应的后端连接未成功或已关闭，无需操作.%n",
                        Thread.currentThread().getName(), frontendChannel.id().asShortText(), rule.getId());
            }
        });
    }

    /**
     * 根据规则ID更新活动连接的速率限制。
     * @param ruleIdToUpdate 要更新的规则ID
     * @param newRateBps 新的速率 (Bytes per second)
     */
    public void updateRateLimitForActiveConnections(String ruleIdToUpdate, long newRateBps) {
        activeShapersByChannel.forEach((channel, shaper) -> {
            String associatedRuleId = channelToRuleIdMap.get(channel);
            if (ruleIdToUpdate.equals(associatedRuleId)) {
                if (channel.isActive()) {
                    System.out.printf("[%s] 正在为活动 Channel %s (规则: '%s') 重新配置整形器速率为 %d Bps.%n",
                            Thread.currentThread().getName(), channel.id().asShortText(), ruleIdToUpdate, newRateBps);
                    shaper.configure(newRateBps, newRateBps);
                } else {
                    System.out.printf("[%s] 更新速率时发现 Channel %s (规则: '%s') 已非活动，将移除其整形器信息.%n",
                            Thread.currentThread().getName(), channel.id().asShortText(), ruleIdToUpdate);
                    activeShapersByChannel.remove(channel);
                    channelToRuleIdMap.remove(channel);
                }
            }
        });
    }

    /**
     * 获取活动的整形器信息 (只读)。
     * @return Map of active shapers
     */
    public Map<Channel, ChannelTrafficShapingHandler> getActiveShapersByChannel() {
        return Map.copyOf(activeShapersByChannel);
    }

    /**
     * 获取 Channel 到 RuleID 的映射 (只读)。
     * @return Map of channel to rule IDs
     */
    public Map<Channel, String> getChannelToRuleIdMap() {
        return Map.copyOf(channelToRuleIdMap);
    }

    /**
     * 关闭所有由该 ConnectionManager 管理的活动连接。
     */
    public void shutdownAllConnections() {
        System.out.printf("[%s] ConnectionManager 正在关闭所有活动连接...%n", Thread.currentThread().getName());
        Map<Channel, String> channelsToCloseSnapshot = new ConcurrentHashMap<>(channelToRuleIdMap);
        channelsToCloseSnapshot.keySet().forEach(channel -> {
            if (channel.isOpen()) {
                System.out.printf("[%s] 正在关闭活动后端 Channel %s (规则: %s) 于程序关闭时...%n",
                        Thread.currentThread().getName(), channel.id().asShortText(), channelToRuleIdMap.get(channel));
                channel.close();
            }
        });
        try {
            Thread.sleep(200); // 短暂等待关闭完成
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        channelToRuleIdMap.clear();
        activeShapersByChannel.clear();
        System.out.printf("[%s] ConnectionManager 所有活动连接已关闭.%n", Thread.currentThread().getName());
    }
}