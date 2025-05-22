package org.riyan6.jet.rule;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 定义了一个转发规则，包含了源、目标IP和端口，以及速率限制和流量统计。
 */
public class ForwardingRule {
    private final String id; // 规则的唯一标识符
    private final String sourceIp;   // 源IP地址
    private final int sourcePort;    // 源端口
    private final String targetIp;   // 目标IP地址
    private final int targetPort;    // 目标端口
    private volatile long rateLimitBps; // 速率限制 (Bytes per second)，0表示无限制

    // 流量计数器
    private final AtomicLong totalBytesForwarded = new AtomicLong(0); // 从客户端转发到目标服务器的总字节数
    private final AtomicLong totalBytesReturned = new AtomicLong(0);  // 从目标服务器返回给客户端的总字节数

    /**
     * 构造函数.
     * @param id 规则ID
     * @param sourceIp 源IP
     * @param sourcePort 源端口
     * @param targetIp 目标IP
     * @param targetPort 目标端口
     * @param initialRateLimitMbps 初始速率限制 (Mbps)
     */
    public ForwardingRule(String id, String sourceIp, int sourcePort, String targetIp, int targetPort, long initialRateLimitMbps) {
        this.id = id;
        this.sourceIp = sourceIp;
        this.sourcePort = sourcePort;
        this.targetIp = targetIp;
        this.targetPort = targetPort;
        this.setRateLimitMbps(initialRateLimitMbps); // 设置初始速率限制
    }

    // --- Getters ---
    public String getId() { return id; }
    public String getSourceIp() { return sourceIp; }
    public int getSourcePort() { return sourcePort; }
    public String getTargetIp() { return targetIp; }
    public int getTargetPort() { return targetPort; }
    public long getRateLimitBps() { return rateLimitBps; }

    // --- 流量监控方法 ---
    public long getTotalBytesForwarded() { return totalBytesForwarded.get(); }
    public long getTotalBytesReturned() { return totalBytesReturned.get(); }

    public void addBytesForwarded(long bytes) {
        this.totalBytesForwarded.addAndGet(bytes);
    }

    public void addBytesReturned(long bytes) {
        this.totalBytesReturned.addAndGet(bytes);
    }

    // --- 速率限制设置 ---
    /**
     * 设置速率限制 (以 Mbps 为单位).
     * @param rateMbps 速率，单位 Mbps
     */
    public void setRateLimitMbps(long rateMbps) {
        if (rateMbps <= 0) {
            this.rateLimitBps = 0; // 0 表示 Netty 流量整形器没有限制
        } else {
            // 将 Mbps 转换为 Bytes per second
            this.rateLimitBps = rateMbps * 1024 * 1024 / 8;
        }
    }

    @Override
    public String toString() {
        return String.format("Rule[%s]: %s:%d -> %s:%d @ %d Bps (%.2f Mbps)",
                id, sourceIp, sourcePort, targetIp, targetPort, rateLimitBps,
                rateLimitBps == 0 ? 0.0 : (double)rateLimitBps * 8 / (1024 * 1024));
    }
}