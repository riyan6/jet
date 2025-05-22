package org.riyan6.jet.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import io.netty.handler.traffic.TrafficCounter;
import org.riyan6.jet.rule.ForwardingRule;

/**
 * Netty Channel Handler，负责在两个 Channel 之间中继数据，并进行流量统计。
 */
public class RelayHandler extends ChannelInboundHandlerAdapter {
    private final Channel relayChannel; // 与当前 Channel 配对的另一个 Channel (数据将发往此 Channel)
    private final ForwardingRule rule;  // 此连接所属的转发规则
    private final boolean isForwardPath; // 标识数据流向：true 表示 客户端->代理->目标，false 表示 目标->代理->客户端
    // private final TrafficCounter trafficCounter; // 如果使用了流量整形器，可以从这里获取更详细的流量数据 (当前版本未使用此字段直接更新规则)

    /**
     * 构造函数.
     * @param relayChannel 中继的目标 Channel
     * @param rule 相关的转发规则
     * @param isForwardPath 是否为正向路径 (客户端到目标)
     * @param trafficCounter 可选的流量计数器 (通常来自 ChannelTrafficShapingHandler)
     */
    public RelayHandler(Channel relayChannel, ForwardingRule rule, boolean isForwardPath, TrafficCounter trafficCounter) {
        this.relayChannel = relayChannel;
        this.rule = rule;
        this.isForwardPath = isForwardPath;
        // this.trafficCounter = trafficCounter; // 如果需要，可以保留并使用
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        // 当 Channel 准备好时，打印日志。
        // 由于 autoRead 通常为 true，Netty 会自动开始读取数据。
        System.out.printf("[%s] RelayHandler Active: Channel %s (Rule: %s, Path: %s). Relaying to: %s. AutoRead: %s.%n",
                Thread.currentThread().getName(),
                ctx.channel().id().asShortText(),
                rule.getId(),
                isForwardPath ? "Client->Target" : "Target->Client",
                relayChannel.id().asShortText(),
                ctx.channel().config().isAutoRead()
        );
        ctx.fireChannelActive(); // 将事件传播到 Pipeline 中的下一个 Handler
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        final String directionLog = isForwardPath ? "Client->Target" : "Target->Client"; // 日志用的方向字符串
        int byteCount = 0;
        if (msg instanceof ByteBuf) {
            byteCount = ((ByteBuf) msg).readableBytes();
        }

        // 频繁的 channelRead 日志会产生大量输出，默认注释掉。如果需要详细追踪数据包，可以取消注释。
        // System.out.printf("[%s] RelayHandler Read: Channel %s (Rule: %s, Path: %s), %d bytes. Relaying to: %s.%n",
        //         Thread.currentThread().getName(),
        //         ctx.channel().id().asShortText(),
        //         rule.getId(),
        //         directionLog,
        //         byteCount,
        //         relayChannel.id().asShortText()
        // );

        if (relayChannel.isActive()) { // 确保目标 Channel 仍然是活动的
            if (msg instanceof ByteBuf) { // 只处理 ByteBuf 类型的消息
                // 根据数据流向，更新相应规则的流量计数
                if (isForwardPath) {
                    rule.addBytesForwarded(byteCount);
                } else {
                    rule.addBytesReturned(byteCount);
                }
            }

            // 将接收到的消息写入到配对的 Channel 中
            relayChannel.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) { // 如果写入失败
                    System.err.printf("[%s] Relay write failed from channel %s to %s (Rule: %s, Path: %s): %s.%n",
                            Thread.currentThread().getName(),
                            ctx.channel().id().asShortText(),    // 源 Channel
                            future.channel().id().asShortText(), // 目标 Channel (即 relayChannel)
                            rule.getId(),
                            directionLog,
                            future.cause().getMessage());
                    // future.cause().printStackTrace(); // 打印详细错误堆栈
                    future.channel().close(); // 关闭目标 Channel
                    ctx.channel().close();    // 关闭当前 Channel
                }
                // 如果写入成功且 autoRead 为 true，Netty 会继续从当前 Channel 读取数据
            });
        } else {
            // 如果目标 Channel 不再活动，则释放消息并关闭当前 Channel
            System.out.printf("[%s] RelayHandler Read: Relay channel %s is INACTIVE. Releasing message from %s of rule %s.%n",
                    Thread.currentThread().getName(),
                    relayChannel.id().asShortText(),
                    ctx.channel().id().asShortText(),
                    rule.getId()
            );
            ReferenceCountUtil.release(msg); // 释放 ByteBuf 资源
            ctx.channel().close(); // 关闭当前 Channel
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        // 当此 Channel变为非活动状态时 (例如，连接断开)
        System.out.printf("[%s] RelayHandler Inactive: Channel %s (Rule: %s, Path: %s). Was relaying to: %s.%n",
                Thread.currentThread().getName(),
                ctx.channel().id().asShortText(),
                rule.getId(),
                isForwardPath ? "Client->Target" : "Target->Client",
                relayChannel.id().asShortText()
        );
        if (relayChannel.isActive()) { // 如果配对的 Channel 仍然活动
            System.out.printf("[%s] Closing active relay channel %s (Rule: %s) due to inactivity of %s.%n",
                    Thread.currentThread().getName(),
                    relayChannel.id().asShortText(),
                    rule.getId(),
                    ctx.channel().id().asShortText()
            );
            // 发送一个空 ByteBuf 以确保所有挂起的数据被刷新，然后关闭连接
            relayChannel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
        ctx.fireChannelInactive(); // 传播事件
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // 处理 Pipeline 中发生的异常
        System.err.printf("[%s] Exception in RelayHandler on channel %s (Rule: %s, Path: %s): %s.%n",
                Thread.currentThread().getName(),
                ctx.channel().id().asShortText(),
                rule.getId(),
                isForwardPath ? "Client->Target" : "Target->Client",
                cause.getMessage()
        );
        // cause.printStackTrace(); // 打印完整堆栈以进行调试
        ctx.close(); // 关闭当前 Channel
        if (relayChannel.isActive()) { // 如果配对的 Channel 仍然活动
            System.err.printf("[%s] Closing relay channel %s (Rule: %s) due to exception on %s.%n",
                    Thread.currentThread().getName(),
                    relayChannel.id().asShortText(),
                    rule.getId(),
                    ctx.channel().id().asShortText()
            );
            relayChannel.close(); // 也关闭配对的 Channel
        }
    }
}