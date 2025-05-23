package org.riyan6.jet.core;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.riyan6.jet.rule.ForwardingRule;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

/**
 * 管理转发规则的生命周期，包括启动和停止监听器。
 */
public class RuleManager {
    private final Map<String, ForwardingRule> rules = new ConcurrentHashMap<>();
    private final Map<String, Channel> serverChannels = new ConcurrentHashMap<>(); // 存储规则ID到其监听Channel的映射

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ExecutorService virtualThreadExecutor;
    // 当新连接被接受时要调用的回调 (frontendChannel, rule) -> void
    private final BiConsumer<SocketChannel, ForwardingRule> newConnectionCallback;

    public RuleManager(EventLoopGroup bossGroup, EventLoopGroup workerGroup, ExecutorService virtualThreadExecutor,
                       BiConsumer<SocketChannel, ForwardingRule> newConnectionCallback) {
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.virtualThreadExecutor = virtualThreadExecutor;
        this.newConnectionCallback = newConnectionCallback;
    }

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
        virtualThreadExecutor.execute(() -> startListenerForRule(rule));
        System.out.printf("[%s] 规则 '%s' 已添加，正在启动监听器: %s%n", Thread.currentThread().getName(), rule.getId(), rule);
    }

    /**
     * 移除一条转发规则，并关闭其监听器。
     * @param ruleIdToRemove 要移除的规则ID
     * @return 如果成功移除了规则则返回该规则对象，否则返回 null。
     */
    public ForwardingRule removeRule(String ruleIdToRemove) {
        ForwardingRule removedRule = rules.remove(ruleIdToRemove);
        if (removedRule != null) {
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
            System.out.printf("[%s] 规则 '%s' 已从管理器中移除.%n", Thread.currentThread().getName(), ruleIdToRemove);
        } else {
            System.out.printf("[%s] 移除规则失败: 未找到规则ID '%s'.%n", Thread.currentThread().getName(), ruleIdToRemove);
        }
        return removedRule;
    }

    /**
     * 获取指定ID的规则。
     * @param ruleId 规则ID
     * @return 对应的 ForwardingRule 对象，如果不存在则返回 null
     */
    public ForwardingRule getRule(String ruleId) {
        return rules.get(ruleId);
    }

    /**
     * 获取所有规则的只读视图。
     * @return 包含所有规则的 Map
     */
    public Map<String, ForwardingRule> getAllRules() {
        return Map.copyOf(rules); // 返回一个不可变副本
    }

    /**
     * 为指定的转发规则启动一个服务器监听器。
     * @param rule 要启动监听的规则
     */
    private void startListenerForRule(ForwardingRule rule) {
        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel frontendChannel) {
                            // 当有新连接接入时，调用外部传入的回调函数
                            newConnectionCallback.accept(frontendChannel, rule);
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture channelFuture = serverBootstrap.bind(rule.getSourceIp(), rule.getSourcePort()).sync();
            System.out.printf("[%s] 监听器已为规则 '%s' 启动于 %s:%d%n",
                    Thread.currentThread().getName(), rule.getId(), rule.getSourceIp(), rule.getSourcePort());
            serverChannels.put(rule.getId(), channelFuture.channel());

            channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.printf("[%s] 规则 '%s' 的监听器被中断.%n", Thread.currentThread().getName(), rule.getId());
        } catch (Exception e) {
            System.err.printf("[%s] 启动规则 '%s' 的监听器失败: %s%n", Thread.currentThread().getName(), rule.getId(), e.getMessage());
            rules.remove(rule.getId());
        } finally {
            serverChannels.remove(rule.getId());
            System.out.printf("[%s] 规则 '%s' 的监听器已关闭/停止.%n", Thread.currentThread().getName(), rule.getId());
        }
    }

    /**
     * 关闭所有由该 RuleManager 管理的监听器。
     */
    public void shutdownAllListeners() {
        System.out.printf("[%s] RuleManager 正在关闭所有监听器...%n", Thread.currentThread().getName());
        serverChannels.values().forEach(channel -> {
            if (channel.isOpen()) {
                channel.close();
            }
        });
        serverChannels.clear();
        rules.clear(); // 通常规则也应在此处清理或由外部管理
        System.out.printf("[%s] RuleManager 所有监听器已关闭.%n", Thread.currentThread().getName());
    }
}
