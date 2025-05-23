package org.riyan6.jet;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.riyan6.jet.core.ConnectionManager;
import org.riyan6.jet.core.RuleManager;
import org.riyan6.jet.rule.ForwardingRule;
import org.riyan6.jet.service.MonitoringService;
import org.riyan6.jet.util.GracefulShutdown;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * TCP 流量转发器主应用程序类。
 * 协调 RuleManager, ConnectionManager, 和 MonitoringService。
 */
public class TcpForwarder {

    private final RuleManager ruleManager;
    private final ConnectionManager connectionManager;
    private final MonitoringService monitoringService;

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ExecutorService virtualThreadExecutor;

    public TcpForwarder() {
        // 初始化 Netty EventLoopGroups
        this.bossGroup = new NioEventLoopGroup(1);
        this.workerGroup = new NioEventLoopGroup();
        // 初始化虚拟线程执行器
        this.virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

        // 初始化核心组件
        // ConnectionManager 需要知道当规则的速率更新时如何通知活动的 shaper
        // RuleManager 需要知道当接受新连接时如何回调 ConnectionManager
        this.connectionManager = new ConnectionManager(this.workerGroup, this.virtualThreadExecutor);
        this.ruleManager = new RuleManager(this.bossGroup, this.workerGroup, this.virtualThreadExecutor,
                (frontendChannel, rule) ->
                        // 使用虚拟线程处理新连接
                        this.virtualThreadExecutor.execute(() ->
                                this.connectionManager.handleNewClientConnection(frontendChannel, rule))
        );
        this.monitoringService = new MonitoringService(this.ruleManager, this.connectionManager);
    }

    /**
     * 添加转发规则。
     *
     * @param rule 规则对象
     */
    public void addRule(ForwardingRule rule) {
        this.ruleManager.addRule(rule);
    }

    /**
     * 移除转发规则。
     *
     * @param ruleId 规则ID
     */
    public void removeRule(String ruleId) {
        ForwardingRule removedRule = this.ruleManager.removeRule(ruleId);
        if (removedRule != null) {
            // 通知 ConnectionManager 清理与此规则相关的连接的 shaper (如果需要更主动的清理)
            // 当前 ConnectionManager 主要通过 channel closeFuture 清理
            System.out.printf("[%s] 主程序：规则 '%s' 已移除，ConnectionManager 将通过 Channel 关闭事件清理相关资源.%n",
                    Thread.currentThread().getName(), ruleId);
        }
    }

    /**
     * 更新规则的速率限制。
     *
     * @param ruleId      规则ID
     * @param newRateMbps 新速率 (Mbps)
     */
    public void updateRuleRateLimit(String ruleId, long newRateMbps) {
        ForwardingRule rule = this.ruleManager.getRule(ruleId);
        if (rule != null) {
            long oldRateBps = rule.getRateLimitBps();
            rule.setRateLimitMbps(newRateMbps);
            long newRateBps = rule.getRateLimitBps();
            System.out.printf("[%s] 主程序：正在更新规则 '%s' 的速率限制，从 %.2f Mbps 到 %d Mbps (新 Bps: %d).%n",
                    Thread.currentThread().getName(), ruleId,
                    oldRateBps == 0 ? 0.0 : (double) oldRateBps * 8 / (1024 * 1024),
                    newRateMbps, newRateBps);
            this.connectionManager.updateRateLimitForActiveConnections(ruleId, newRateBps);
        } else {
            System.out.printf("[%s] 主程序：更新速率失败，未找到规则ID '%s'.%n", Thread.currentThread().getName(), ruleId);
        }
    }

    /**
     * 启动监控服务。
     *
     * @param periodSeconds 监控周期 (秒)
     */
    public void startMonitoring(long periodSeconds) {
        this.monitoringService.start(periodSeconds);
    }

    /**
     * 关闭转发器并释放所有资源。
     */
    public void shutdown() {
        System.out.printf("[%s] TcpForwarder 正在关闭...%n", Thread.currentThread().getName());

        // 1. 停止监控服务
        this.monitoringService.stop();

        // 2. 关闭所有规则监听器
        this.ruleManager.shutdownAllListeners(); // RuleManager 会处理其内部 serverChannels

        // 3. 关闭所有活动连接
        this.connectionManager.shutdownAllConnections(); // ConnectionManager 处理 activeShapersByChannel 和 channelToRuleIdMap

        // 4. 关闭 Netty EventLoopGroups
        System.out.printf("[%s] 正在关闭 Netty EventLoopGroups...%n", Thread.currentThread().getName());
        GracefulShutdown.shutdownGracefully(bossGroup, workerGroup);


        // 5. 关闭虚拟线程执行器
        System.out.printf("[%s] 正在关闭虚拟线程执行器...%n", Thread.currentThread().getName());
        GracefulShutdown.shutdownGracefully(virtualThreadExecutor);

        System.out.printf("[%s] TcpForwarder 已成功关闭.%n", Thread.currentThread().getName());
    }


    public static void main(String[] args) {
        System.out.println("TCP 转发器应用程序正在启动... (需要 JDK 21+ 以支持虚拟线程)");
        final TcpForwarder forwarder = new TcpForwarder();

        ForwardingRule rule1 = new ForwardingRule("rule1", "0.0.0.0", 8080, "127.0.0.1", 3000, 0);
        ForwardingRule rule2 = new ForwardingRule("rule2", "0.0.0.0", 8081, "127.0.0.1", 3001, 10);

        forwarder.addRule(rule1);
        forwarder.addRule(rule2);

        forwarder.startMonitoring(5);

        Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().factory()).schedule(() -> {
            System.out.printf("%n[%s] === 动态操作：将 rule1 的速率更新为 20 Mbps === %n%n", Thread.currentThread().getName());
            forwarder.updateRuleRateLimit("rule1", 20);
        }, 20, TimeUnit.SECONDS);

        Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().factory()).schedule(() -> {
            System.out.printf("%n[%s] === 动态操作：移除 rule2 === %n%n", Thread.currentThread().getName());
            forwarder.removeRule("rule2");
        }, 40, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.printf("[%s] JVM 关闭钩子已触发。正在关闭转发器...%n", Thread.currentThread().getName());
            forwarder.shutdown();
        }, "TcpForwarderShutdownHook"));

        System.out.println("TCP 转发器正在运行。按 Ctrl+C 停止程序。");
        try {
            while (!Thread.currentThread().isInterrupted()) {
                Thread.sleep(10000);
            }
        } catch (InterruptedException e) {
            System.out.println("主线程被中断。将通过关闭钩子执行清理。");
            Thread.currentThread().interrupt();
        }
    }
}