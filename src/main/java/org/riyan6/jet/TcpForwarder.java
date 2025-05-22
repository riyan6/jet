package org.riyan6.jet;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import io.netty.handler.traffic.TrafficCounter;
import org.riyan6.jet.handler.RelayHandler;
import org.riyan6.jet.rule.ForwardingRule;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * TCP 流量转发器主类。
 * 使用 Netty 和 JDK 21 虚拟线程实现。
 * 支持多条转发规则、实时流量统计和动态速率调整。
 */
public class TcpForwarder {

    // 存储所有定义的转发规则，Key 是规则ID
    private final Map<String, ForwardingRule> rules = new ConcurrentHashMap<>();
    // 存储每个规则ID对应的服务器监听 Channel
    private final Map<String, Channel> serverChannels = new ConcurrentHashMap<>();

    // --- 用于动态更新速率和监控 ---
    // Key: 后端 Channel (连接到目标服务器的 Channel)
    // Value: 该 Channel 所属的 ForwardingRule ID
    private final Map<Channel, String> channelToRuleIdMap = new ConcurrentHashMap<>();
    // Key: 后端 Channel
    // Value: 该 Channel 上的 ChannelTrafficShapingHandler 实例
    private final Map<Channel, ChannelTrafficShapingHandler> activeShapersByChannel = new ConcurrentHashMap<>();

    // --- Netty EventLoopGroup ---
    // bossGroup 用于接受进来的连接 (例如，一个线程就足够了)
    private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    // workerGroup 用于处理已接受连接的I/O操作 (通常是 CPU核心数 * 2 个线程)
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();

    // --- JDK 21 虚拟线程执行器 ---
    // 用于执行一些独立的、可能阻塞的任务，如启动服务器监听或处理新连接的初始设置
    private final ExecutorService virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
    // 用于定时任务，如流量监控报告
    private final ScheduledExecutorService monitoringExecutor = Executors.newSingleThreadScheduledExecutor();


    /**
     * 添加一条新的转发规则，并启动相应的监听器。
     * @param rule 要添加的转发规则对象
     */
    public void addRule(ForwardingRule rule) {
        if (rules.containsKey(rule.getId())) {
            System.out.printf("[%s] 添加规则失败: 规则ID '%s' 已存在.%n", Thread.currentThread().getName(), rule.getId());
            return;
        }
        rules.put(rule.getId(), rule);
        // 使用虚拟线程异步启动监听器，避免阻塞当前线程
        virtualThreadExecutor.execute(() -> startListenerForRule(rule));
        System.out.printf("[%s] 规则 '%s' 已添加，正在启动监听器: %s%n", Thread.currentThread().getName(), rule.getId(), rule);
    }

    /**
     * 移除一条转发规则，并关闭其监听器和相关连接。
     * @param ruleIdToRemove 要移除的规则ID
     */
    public void removeRule(String ruleIdToRemove) {
        ForwardingRule rule = rules.remove(ruleIdToRemove); // 从规则列表中移除
        if (rule != null) {
            // 关闭此规则的服务器监听 Channel
            Channel serverChannel = serverChannels.remove(ruleIdToRemove);
            if (serverChannel != null && serverChannel.isOpen()) {
                serverChannel.close().addListener(future -> {
                    if (future.isSuccess()) {
                        System.out.printf("[%s] 规则 '%s' 的监听器已停止.%n", Thread.currentThread().getName(), ruleIdToRemove);
                    } else {
                        System.err.printf("[%s] 停止规则 '%s' 的监听器时出错: %s%n", Thread.currentThread().getName(), ruleIdToRemove, future.cause());
                    }
                });
            }

            // 清理与此规则相关的活动整形器和 Channel-RuleId 映射
            // 为避免在迭代时修改 ConcurrentHashMap 导致的问题，可以先收集要移除的key，或使用其安全迭代方法
            channelToRuleIdMap.entrySet().removeIf(entry -> {
                if (entry.getValue().equals(ruleIdToRemove)) {
                    Channel backendChannel = entry.getKey();
                    activeShapersByChannel.remove(backendChannel); // 从整形器映射中移除
                    // 如果该后端 Channel 仍然打开，则关闭它
                    if (backendChannel.isOpen()) {
                        System.out.printf("[%s] 正在关闭与已移除规则 '%s' 关联的后端活动连接 %s.%n",
                                Thread.currentThread().getName(), ruleIdToRemove, backendChannel.id().asShortText());
                        backendChannel.close(); // 关闭会触发其 closeFuture 监听器，进行进一步清理
                    }
                    return true; // 从 channelToRuleIdMap 中移除此条目
                }
                return false;
            });
            System.out.printf("[%s] 规则 '%s' 已被移除，相关的活动连接正在关闭.%n", Thread.currentThread().getName(), ruleIdToRemove);
        } else {
            System.out.printf("[%s] 移除规则失败: 未找到规则ID '%s'.%n", Thread.currentThread().getName(), ruleIdToRemove);
        }
    }

    /**
     * 动态更新指定规则的速率限制。
     * @param ruleIdToUpdate 要更新速率的规则ID
     * @param newRateMbps 新的速率限制 (单位 Mbps)
     */
    public void updateRuleRateLimit(String ruleIdToUpdate, long newRateMbps) {
        ForwardingRule rule = rules.get(ruleIdToUpdate); // 获取规则对象
        if (rule != null) {
            long oldRateBps = rule.getRateLimitBps();
            rule.setRateLimitMbps(newRateMbps); // 更新规则对象内部的速率值 (转换为 Bps)
            long newRateBps = rule.getRateLimitBps();

            System.out.printf("[%s] 正在更新规则 '%s' 的速率限制: 从 %.2f Mbps ( %d Bps) 到 %d Mbps ( %d Bps).%n",
                    Thread.currentThread().getName(),
                    ruleIdToUpdate,
                    oldRateBps == 0 ? 0.0 : (double) oldRateBps * 8 / (1024 * 1024), oldRateBps,
                    newRateMbps, newRateBps);

            // 遍历当前活动的整形器，如果它们属于要更新的规则，则重新配置它们
            activeShapersByChannel.forEach((channel, shaper) -> {
                String associatedRuleId = channelToRuleIdMap.get(channel); // 获取此 Channel 关联的规则ID
                if (ruleIdToUpdate.equals(associatedRuleId)) { // 检查是否是当前要更新的规则
                    if (channel.isActive()) { // 确保 Channel 仍然是活动的
                        System.out.printf("[%s] 正在为活动 Channel %s (规则: '%s') 重新配置整形器速率为 %d Bps.%n",
                                Thread.currentThread().getName(), channel.id().asShortText(), ruleIdToUpdate, newRateBps);
                        shaper.configure(newRateBps, newRateBps); // 更新整形器的上行和下行速率限制
                    } else {
                        // 如果 Channel 不再活动，从映射中移除 (理论上其 closeFuture 监听器会处理，但这里做双重保险)
                        System.out.printf("[%s] 更新速率时发现 Channel %s (规则: '%s') 已非活动，将移除其整形器信息.%n",
                                Thread.currentThread().getName(), channel.id().asShortText(), ruleIdToUpdate);
                        activeShapersByChannel.remove(channel);
                        channelToRuleIdMap.remove(channel);
                    }
                }
            });

            System.out.printf("[%s] 规则 '%s' 的速率限制已更新。该规则下的活动整形器 (如果有) 已尝试重新配置.%n",
                    Thread.currentThread().getName(), ruleIdToUpdate);
        } else {
            System.out.printf("[%s] 更新速率失败: 未找到规则ID '%s'.%n", Thread.currentThread().getName(), ruleIdToUpdate);
        }
    }

    /**
     * 为指定的转发规则启动一个服务器监听器。
     * 此方法在虚拟线程中执行。
     * @param rule 要启动监听的规则
     */
    private void startListenerForRule(ForwardingRule rule) {
        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup) // 设置 boss 和 worker 线程组
                    .channel(NioServerSocketChannel.class) // 指定使用 NIO 的 ServerSocketChannel
                    .childHandler(new ChannelInitializer<SocketChannel>() { // 设置新连接的 Channel Handler
                        @Override
                        protected void initChannel(SocketChannel frontendChannel) {
                            // 当有新连接接入时，使用虚拟线程处理其后续设置和后端连接逻辑
                            virtualThreadExecutor.execute(() ->
                                    handleNewClientConnection(frontendChannel, rule));
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128) // 设置TCP连接请求队列的最大长度
                    .childOption(ChannelOption.SO_KEEPALIVE, true); // 为已接受的连接启用TCP KeepAlive

            // 绑定到规则指定的源IP和端口，并同步等待完成
            ChannelFuture channelFuture = serverBootstrap.bind(rule.getSourceIp(), rule.getSourcePort()).sync();
            System.out.printf("[%s] 监听器已为规则 '%s' 启动于 %s:%d%n",
                    Thread.currentThread().getName(), rule.getId(), rule.getSourceIp(), rule.getSourcePort());
            serverChannels.put(rule.getId(), channelFuture.channel()); // 存储服务器 Channel

            // 同步等待服务器 Channel 关闭 (即监听停止)
            channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // 恢复中断状态，以便上层代码可以检测到
            System.err.printf("[%s] 规则 '%s' 的监听器被中断.%n", Thread.currentThread().getName(), rule.getId());
        } catch (Exception e) { // 捕获其他所有异常，如地址已绑定等
            System.err.printf("[%s] 启动规则 '%s' 的监听器失败: %s%n", Thread.currentThread().getName(), rule.getId(), e.getMessage());
            // e.printStackTrace(); // 打印完整错误堆栈
            rules.remove(rule.getId()); // 如果启动失败，从规则列表中移除此规则
        } finally {
            // 当监听器最终关闭时 (无论是正常关闭还是异常导致)
            serverChannels.remove(rule.getId()); // 从服务器 Channel 映射中移除
            System.out.printf("[%s] 规则 '%s' 的监听器已关闭/停止.%n", Thread.currentThread().getName(), rule.getId());
        }
    }

    /**
     * 处理一个新的客户端接入连接。
     * 此方法在虚拟线程中执行。
     * @param frontendChannel 代表客户端连接的 Channel
     * @param rule 此连接匹配的转发规则
     */
    private void handleNewClientConnection(SocketChannel frontendChannel, ForwardingRule rule) {
        // 立即为新接入的前端 Channel (客户端连接) 启用自动读取数据
        frontendChannel.config().setAutoRead(true);
        System.out.printf("[%s] 已接受规则 '%s' 的新连接，来自: %s. 前端 Channel ID: %s. 已设置 AutoRead=true.%n",
                Thread.currentThread().getName(), rule.getId(), frontendChannel.remoteAddress(), frontendChannel.id().asShortText());

        // 创建 Netty Bootstrap 以连接到目标服务器 (后端连接)
        Bootstrap backendBootstrap = new Bootstrap();
        backendBootstrap.group(workerGroup) // 复用 workerGroup
                .channel(NioSocketChannel.class) // 指定使用 NIO 的 SocketChannel
                .handler(new ChannelInitializer<SocketChannel>() { // 设置后端连接的 Channel Handler
                    @Override
                    protected void initChannel(SocketChannel backendChannel) {
                        System.out.printf("[%s] 正在初始化后端 Channel %s (规则: '%s')，目标: %s:%d%n",
                                Thread.currentThread().getName(), backendChannel.id().asShortText(), rule.getId(), rule.getTargetIp(), rule.getTargetPort());

                        TrafficCounter sharedTrafficCounterForRelay = null;

                        // 如果规则设置了速率限制 (大于0)，则添加流量整形处理器
                        if (rule.getRateLimitBps() > 0) {
                            ChannelTrafficShapingHandler shaper = new ChannelTrafficShapingHandler(
                                    rule.getRateLimitBps(), // 写出限制 (代理 -> 目标)
                                    rule.getRateLimitBps(), // 读入限制 (目标 -> 代理)
                                    1000);  // 检查间隔 (毫秒)
                            backendChannel.pipeline().addLast("trafficShaper", shaper); // 添加到 Pipeline

                            // 存储整形器实例和 Channel-RuleId 映射，用于后续动态更新和监控
                            activeShapersByChannel.put(backendChannel, shaper);
                            channelToRuleIdMap.put(backendChannel, rule.getId());
                            sharedTrafficCounterForRelay = shaper.trafficCounter(); // 获取流量计数器给 RelayHandler 使用
                            System.out.printf("[%s] 已为后端 Channel %s (规则: '%s') 添加流量整形器.%n",
                                    Thread.currentThread().getName(), backendChannel.id().asShortText(), rule.getId());
                        } else {
                            System.out.printf("[%s] 后端 Channel %s (规则: '%s') 未添加流量整形器 (速率限制为 %d Bps).%n",
                                    Thread.currentThread().getName(), backendChannel.id().asShortText(), rule.getId(), rule.getRateLimitBps());
                        }

                        // 添加后端 RelayHandler: 负责从目标服务器读取数据，并转发给前端 Channel
                        backendChannel.pipeline().addLast("backendRelay",
                                new RelayHandler(frontendChannel, rule, false, sharedTrafficCounterForRelay));

                        // 设置当后端 Channel 关闭时的清理逻辑
                        backendChannel.closeFuture().addListener(closeFuture -> {
                            String closedRuleId = channelToRuleIdMap.get(backendChannel); // 获取关联的规则ID
                            System.out.printf("[%s] 后端 Channel %s (规则: '%s') 已关闭. 正在从活动映射中移除.%n",
                                    Thread.currentThread().getName(), backendChannel.id().asShortText(), closedRuleId != null ? closedRuleId : "N/A");
                            activeShapersByChannel.remove(backendChannel); // 从整形器映射中移除
                            channelToRuleIdMap.remove(backendChannel);   // 从 Channel-RuleId 映射中移除
                        });
                    }
                })
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000) // 设置连接超时时间为10秒
                .option(ChannelOption.SO_KEEPALIVE, true); // 为后端连接启用TCP KeepAlive

        // 异步连接到目标服务器
        ChannelFuture connectFuture = backendBootstrap.connect(rule.getTargetIp(), rule.getTargetPort());

        // 为连接操作添加监听器，以处理连接成功或失败的情况
        connectFuture.addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) { // 后端连接成功
                SocketChannel backendChannel = (SocketChannel) future.channel();
                System.out.printf("[%s] 已成功连接到目标服务器 (规则: '%s'): %s. 后端 Channel ID: %s.%n",
                        Thread.currentThread().getName(), rule.getId(), backendChannel.remoteAddress(), backendChannel.id().asShortText());

                // 获取可能已添加到后端管道的 TrafficCounter，供前端 RelayHandler 使用
                TrafficCounter counterForFrontendRelay = null;
                ChannelTrafficShapingHandler existingShaper = backendChannel.pipeline().get(ChannelTrafficShapingHandler.class);
                if (existingShaper != null) {
                    counterForFrontendRelay = existingShaper.trafficCounter();
                } else {
                    System.out.printf("[%s] 在后端 Channel %s (规则: '%s') 上未找到整形器，前端 RelayHandler 将不使用其计数器.%n",
                            Thread.currentThread().getName(), backendChannel.id().asShortText(), rule.getId());
                }

                // 在前端 Channel 的 Pipeline 中添加前端 RelayHandler
                // 负责从客户端读取数据，并转发给后端 Channel
                frontendChannel.pipeline().addLast("frontendRelay",
                        new RelayHandler(backendChannel, rule, true, counterForFrontendRelay));

                // 前端 Channel 的 autoRead 已在开始时设置为 true。
                // 后端 Channel (NioSocketChannel) 默认的 autoRead 也为 true。
                // Netty 将自动开始从两个方向读取数据。

            } else { // 后端连接失败
                System.err.printf("[%s] 连接到目标服务器失败 (规则: '%s', 目标: %s:%d): %s.%n",
                        Thread.currentThread().getName(), rule.getId(), rule.getTargetIp(), rule.getTargetPort(), future.cause().getMessage());
                frontendChannel.close(); // 如果无法连接到后端，则关闭来自客户端的前端连接
            }
        });

        // 设置当前端 Channel 关闭时的逻辑
        frontendChannel.closeFuture().addListener(future -> {
            System.out.printf("[%s] 前端连接 %s (规则: '%s', 来自: %s) 已关闭.%n",
                    Thread.currentThread().getName(), frontendChannel.id().asShortText(), rule.getId(), frontendChannel.remoteAddress());
            // 如果后端连接已成功建立并且仍然活动，则关闭后端连接
            // connectFuture.isDone() && connectFuture.isSuccess() 确保连接尝试已完成且成功
            if (connectFuture.isDone() && connectFuture.isSuccess() && connectFuture.channel() != null && connectFuture.channel().isActive()) {
                System.out.printf("[%s] 因前端连接 %s (规则: '%s') 关闭，正在关闭对应的后端 Channel %s.%n",
                        Thread.currentThread().getName(), frontendChannel.id().asShortText(), rule.getId(), connectFuture.channel().id().asShortText());
                connectFuture.channel().close(); // 关闭后端 Channel (这将触发其自身的 closeFuture 监听器进行清理)
            } else if (connectFuture.isDone() && !connectFuture.isSuccess()){ // 如果后端连接尝试已完成但未成功
                System.out.printf("[%s] 前端连接 %s (规则: '%s') 关闭时，对应的后端连接未成功或已关闭，无需操作.%n",
                        Thread.currentThread().getName(), frontendChannel.id().asShortText(), rule.getId());
            }
            // 如果后端连接尝试还未完成 (connectFuture 未 isDone)，则当其完成后，如果前端已关闭，它在自己的回调中不会添加 RelayHandler。
        });
    }

    /**
     * 启动一个定时任务，周期性地打印流量统计报告。
     * @param periodSeconds 报告周期，单位秒
     */
    public void startMonitoring(long periodSeconds) {
        monitoringExecutor.scheduleAtFixedRate(() -> {
            try {
                System.out.printf("%n----- 流量报告 ----- %s -----%n", java.time.LocalDateTime.now());
                if (rules.isEmpty() && activeShapersByChannel.isEmpty()) { // 同时检查规则和活动连接
                    System.out.println("当前无活动规则或活动连接。");
                    System.out.println("-----------------------------------------------------\n");
                    return;
                }
                if (!rules.isEmpty()) {
                    System.out.println("--- 规则累计流量 ---");
                    rules.values().forEach(rule -> {
                        long forwardedMB = rule.getTotalBytesForwarded() / (1024 * 1024);
                        long returnedMB = rule.getTotalBytesReturned() / (1024 * 1024);

                        System.out.printf("规则 [%s] (%s:%d -> %s:%d): 已转发: %d MB, 已返回: %d MB, 当前限速: %.2f Mbps%n",
                                rule.getId(), rule.getSourceIp(), rule.getSourcePort(),
                                rule.getTargetIp(), rule.getTargetPort(),
                                forwardedMB, returnedMB,
                                rule.getRateLimitBps() == 0 ? 0.0 : (double) rule.getRateLimitBps() * 8 / (1024 * 1024)
                        );
                    });
                } else {
                    System.out.println("无已定义的规则。");
                }


                if (!activeShapersByChannel.isEmpty()) {
                    System.out.println("--- 活动连接实时速率 (基于后端Channel整形器) ---");
                    activeShapersByChannel.forEach((channel, shaper) -> { // 遍历活动的整形器
                        TrafficCounter tc = shaper.trafficCounter(); // 获取整形器的流量计数器
                        String ruleId = channelToRuleIdMap.get(channel); // 获取此 Channel 关联的规则ID
                        if (tc != null && channel.isActive()) { // 确保计数器存在且 Channel 仍活动
                            System.out.printf("  后端 Channel %s (规则: %s): 写速率(->目标): %.2f KB/s, 读速率(<-目标): %.2f KB/s (检查间隔: %d ms)%n",
                                    channel.id().asShortText(),
                                    ruleId != null ? ruleId : "未知", // 显示规则ID
                                    tc.lastWriteThroughput() / 1024.0, // 将 Bps 转换为 KBps
                                    tc.lastReadThroughput() / 1024.0,  // 将 Bps 转换为 KBps
                                    tc.checkInterval());
                        }
                    });
                } else {
                    System.out.println("当前无活动的带整形器的连接。");
                }
                System.out.println("-----------------------------------------------------\n");
            } catch (Exception e) { // 捕获监控任务中的任何异常，防止任务终止
                System.err.printf("[%s] 监控任务执行出错: %s%n", Thread.currentThread().getName(), e.getMessage());
                // e.printStackTrace(); // 打印完整堆栈
            }
        }, 0, periodSeconds, TimeUnit.SECONDS); // 首次立即执行，之后按周期执行
    }

    /**
     * 关闭整个 TCP 转发器，释放所有资源。
     */
    public void shutdown() {
        System.out.printf("[%s] 正在关闭 TCP 转发器...%n", Thread.currentThread().getName());

        // 1. 关闭定时监控任务
        if (!monitoringExecutor.isShutdown()) {
            monitoringExecutor.shutdown();
        }

        // 2. 关闭所有服务器监听 Channel
        serverChannels.values().forEach(channel -> {
            if (channel.isOpen()) {
                System.out.printf("[%s] 正在关闭服务器监听 Channel %s...%n", Thread.currentThread().getName(), channel.id().asShortText());
                channel.close();
            }
        });
        serverChannels.clear(); // 清空映射

        // 3. 关闭所有活动的后端 Channel (这将触发它们的 closeFuture 监听器进行清理)
        // 创建一个当前活动 Channel 的副本进行迭代，以避免在迭代时修改 ConcurrentHashMap
        Map<Channel, String> channelsToCloseSnapshot = new ConcurrentHashMap<>(channelToRuleIdMap);
        channelsToCloseSnapshot.keySet().forEach(channel -> {
            if (channel.isOpen()) {
                System.out.printf("[%s] 正在关闭活动后端 Channel %s (规则: %s) 于程序关闭时...%n",
                        Thread.currentThread().getName(), channel.id().asShortText(), channelToRuleIdMap.get(channel));
                channel.close();
            }
        });

        // 等待一小段时间，让 Channel 关闭回调（包括从 map 中移除）有机会执行
        try {
            Thread.sleep(500); // 0.5秒，可根据情况调整
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.printf("[%s] 关闭等待期间被中断.%n", Thread.currentThread().getName());
        }
        // 清理映射 (理论上 Channel 关闭时回调已清理，这里是双重保险)
        channelToRuleIdMap.clear();
        activeShapersByChannel.clear();

        // 4. 关闭 Netty 的 EventLoopGroup
        System.out.printf("[%s] 正在关闭 Netty EventLoopGroups...%n", Thread.currentThread().getName());
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();

        // 5. 关闭虚拟线程执行器
        if (!virtualThreadExecutor.isShutdown()) {
            virtualThreadExecutor.shutdown();
        }

        // 6. 等待所有组件终止
        try {
            System.out.printf("[%s] 等待 EventLoopGroups 终止...%n", Thread.currentThread().getName());
            if (!bossGroup.terminationFuture().await(5, TimeUnit.SECONDS)) {
                System.err.printf("[%s] Boss EventLoopGroup 未能在5秒内终止.%n", Thread.currentThread().getName());
            }
            if (!workerGroup.terminationFuture().await(5, TimeUnit.SECONDS)) {
                System.err.printf("[%s] Worker EventLoopGroup 未能在5秒内终止.%n", Thread.currentThread().getName());
            }
            System.out.printf("[%s] 等待虚拟线程执行器终止...%n", Thread.currentThread().getName());
            if (!virtualThreadExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                System.err.printf("[%s] 虚拟线程执行器未能在5秒内终止.%n", Thread.currentThread().getName());
                virtualThreadExecutor.shutdownNow(); // 强制关闭
            }
            System.out.printf("[%s] 等待监控执行器终止...%n", Thread.currentThread().getName());
            if (!monitoringExecutor.awaitTermination(5, TimeUnit.SECONDS)){
                System.err.printf("[%s] 监控执行器未能在5秒内终止.%n", Thread.currentThread().getName());
                if (!monitoringExecutor.isTerminated()) { // 再次检查是否已终止
                    monitoringExecutor.shutdownNow(); // 强制关闭
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // 恢复中断状态
            System.err.printf("[%s] 关闭过程中等待终止时被中断.%n", Thread.currentThread().getName());
            // 尝试强制关闭未完成的组件
            if (!virtualThreadExecutor.isTerminated()) virtualThreadExecutor.shutdownNow();
            if (!monitoringExecutor.isTerminated()) monitoringExecutor.shutdownNow();
        }
        System.out.printf("[%s] TCP 转发器已成功关闭.%n", Thread.currentThread().getName());
    }

    public static void main(String[] args) {
        // 确保在支持虚拟线程的 JDK 版本 (21+) 上运行
        System.out.println("TCP 转发器应用程序正在启动... (需要 JDK 21+ 以支持虚拟线程)");
        final TcpForwarder forwarder = new TcpForwarder(); // 声明为 final 给 ShutdownHook 使用

        // 添加示例转发规则
        ForwardingRule rule1 = new ForwardingRule("rule1", "0.0.0.0", 46573, "92.112.23.178", 56573, 100); // 100 Mbps
        ForwardingRule rule2 = new ForwardingRule("rule2", "0.0.0.0", 46574, "92.112.23.178", 56573, 10);  // 10 Mbps

        forwarder.addRule(rule1);
        forwarder.addRule(rule2);

        // 启动流量监控，每5秒打印一次报告
        forwarder.startMonitoring(5);

        // --- 示例动态操作 (可选，用于演示) ---
        // 20秒后动态更新 rule1 的速率限制为 20 Mbps
        Executors.newSingleThreadScheduledExecutor().schedule(() -> {
            System.out.printf("%n[%s] === 动态操作：将 rule1 的速率更新为 20 Mbps === %n%n", Thread.currentThread().getName());
            forwarder.updateRuleRateLimit("rule1", 20);
        }, 20, TimeUnit.SECONDS);

        // 40秒后动态移除 rule2
        Executors.newSingleThreadScheduledExecutor().schedule(() -> {
            System.out.printf("%n[%s] === 动态操作：移除 rule2 === %n%n", Thread.currentThread().getName());
            forwarder.removeRule("rule2");
        }, 40, TimeUnit.SECONDS);
        // --- 示例动态操作结束 ---

        // 添加JVM关闭钩子，确保程序退出时能优雅地关闭转发器
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.printf("[%s] JVM 关闭钩子已触发。正在关闭转发器...%n", Thread.currentThread().getName());
            if (forwarder != null) {
                forwarder.shutdown();
            }
        }, "TcpForwarderShutdownHook"));

        System.out.println("TCP 转发器正在运行。按 Ctrl+C 停止程序。");

        // 使主线程保持活动状态，直到程序被外部中断 (例如 Ctrl+C)
        // 因为所有的服务器监听和连接处理都在其他线程（Netty I/O 线程或虚拟线程）中进行
        try {
            // Thread.sleep(Long.MAX_VALUE) 是一种简单的方式，但不是最优雅的。
            // 对于守护进程类应用，通常会有更复杂的生命周期管理。
            // 这里我们依赖 Ctrl+C 和关闭钩子。
            // 如果是在一个容器或者服务管理器中运行，它们会有自己的停止信号机制。
            while (!Thread.currentThread().isInterrupted()) {
                Thread.sleep(10000); // 每10秒检查一次中断状态，或者 просто Long.MAX_VALUE
            }
        } catch (InterruptedException e) {
            System.out.println("主线程被中断 (例如通过 Ctrl+C)。将通过关闭钩子执行清理。");
            Thread.currentThread().interrupt(); // 恢复中断状态
            // 不需要在这里显式调用 forwarder.shutdown()，因为关闭钩子会处理
        }
    }
}