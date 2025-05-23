package org.riyan6.jet.util;

import io.netty.channel.EventLoopGroup;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class GracefulShutdown {

    private static final int DEFAULT_TIMEOUT_SECONDS = 5;

    public static void shutdownGracefully(EventLoopGroup group) {
        shutdownGracefully(group, DEFAULT_TIMEOUT_SECONDS);
    }

    public static void shutdownGracefully(EventLoopGroup group, int timeoutSeconds) {
        if (group == null || group.isShuttingDown() || group.isShutdown()) {
            return;
        }
        group.shutdownGracefully();
        try {
            if (!group.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
                System.err.printf("[%s] EventLoopGroup %s 未能在 %d 秒内终止.%n",
                        Thread.currentThread().getName(), group.getClass().getSimpleName(), timeoutSeconds);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.printf("[%s] EventLoopGroup %s 关闭等待期间被中断.%n",
                    Thread.currentThread().getName(), group.getClass().getSimpleName());
        }
    }

    public static void shutdownGracefully(ExecutorService executor) {
        shutdownGracefully(executor, DEFAULT_TIMEOUT_SECONDS);
    }

    public static void shutdownGracefully(ExecutorService executor, int timeoutSeconds) {
        if (executor == null || executor.isShutdown()) {
            return;
        }
        executor.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
                System.err.printf("[%s] ExecutorService %s 未能在 %d 秒内终止，尝试强制关闭.%n",
                        Thread.currentThread().getName(), executor.getClass().getSimpleName(), timeoutSeconds);
                executor.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
                    System.err.printf("[%s] ExecutorService %s (在 shutdownNow 后) 仍未终止.%n",
                            Thread.currentThread().getName(), executor.getClass().getSimpleName());
                }
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            executor.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
            System.err.printf("[%s] ExecutorService %s 关闭等待期间被中断.%n",
                    Thread.currentThread().getName(), executor.getClass().getSimpleName());
        }
    }

    public static void shutdownGracefully(EventLoopGroup... groups) {
        for (EventLoopGroup group : groups) {
            shutdownGracefully(group);
        }
    }
}