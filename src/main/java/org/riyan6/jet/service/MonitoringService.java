package org.riyan6.jet.service;

import io.netty.channel.Channel;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import io.netty.handler.traffic.TrafficCounter;
import org.riyan6.jet.core.ConnectionManager;
import org.riyan6.jet.core.RuleManager;
import org.riyan6.jet.rule.ForwardingRule;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 定时监控服务，用于报告流量统计和活动连接信息。
 */
public class MonitoringService {
    private final RuleManager ruleManager;
    private final ConnectionManager connectionManager;
    private final ScheduledExecutorService monitoringExecutor;

    public MonitoringService(RuleManager ruleManager, ConnectionManager connectionManager) {
        this.ruleManager = ruleManager;
        this.connectionManager = connectionManager;
        this.monitoringExecutor = Executors.newSingleThreadScheduledExecutor(
                Thread.ofVirtual().name("monitoring-service-", 0).factory()
        );
    }

    /**
     * 启动监控任务。
     * @param periodSeconds 报告周期，单位秒
     */
    public void start(long periodSeconds) {
        if (periodSeconds <= 0) {
            System.out.println("监控周期必须大于0秒，监控未启动。");
            return;
        }
        monitoringExecutor.scheduleAtFixedRate(() -> {
            try {
                System.out.printf("%n----- 流量报告 ----- %s -----%n", java.time.LocalDateTime.now());
                Map<String, ForwardingRule> allRules = ruleManager.getAllRules();
                Map<Channel, ChannelTrafficShapingHandler> activeShapers = connectionManager.getActiveShapersByChannel();
                Map<Channel, String> channelToRuleIds = connectionManager.getChannelToRuleIdMap();

                if (allRules.isEmpty() && activeShapers.isEmpty()) {
                    System.out.println("当前无活动规则或活动连接。");
                    System.out.println("-----------------------------------------------------\n");
                    return;
                }

                if (!allRules.isEmpty()) {
                    System.out.println("--- 规则累计流量 ---");
                    allRules.values().forEach(rule -> {
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

                if (!activeShapers.isEmpty()) {
                    System.out.println("--- 活动连接实时速率 (基于后端Channel整形器) ---");
                    activeShapers.forEach((channel, shaper) -> {
                        TrafficCounter tc = shaper.trafficCounter();
                        String ruleId = channelToRuleIds.get(channel);
                        if (tc != null && channel.isActive()) {
                            System.out.printf("  后端 Channel %s (规则: %s): 写速率(->目标): %.2f KB/s, 读速率(<-目标): %.2f KB/s (检查间隔: %d ms)%n",
                                    channel.id().asShortText(),
                                    ruleId != null ? ruleId : "未知",
                                    tc.lastWriteThroughput() / 1024.0,
                                    tc.lastReadThroughput() / 1024.0,
                                    tc.checkInterval());
                        }
                    });
                } else {
                    System.out.println("当前无活动的带整形器的连接。");
                }
                System.out.println("-----------------------------------------------------\n");
            } catch (Exception e) {
                System.err.printf("[%s] 监控任务执行出错: %s%n", Thread.currentThread().getName(), e.getMessage());
            }
        }, 0, periodSeconds, TimeUnit.SECONDS);
        System.out.printf("监控服务已启动，报告周期: %d 秒.%n", periodSeconds);
    }

    /**
     * 停止监控任务。
     */
    public void stop() {
        if (!monitoringExecutor.isShutdown()) {
            System.out.println("正在停止监控服务...");
            monitoringExecutor.shutdown();
            try {
                if (!monitoringExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    System.err.println("监控执行器未能在5秒内终止，将尝试强制关闭。");
                    monitoringExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("停止监控服务时被中断。");
                monitoringExecutor.shutdownNow();
            }
            System.out.println("监控服务已停止。");
        }
    }
}