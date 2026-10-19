package com.zkube.notifications;

import io.javalin.Javalin;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;

public class NotificationsApplication {

    private static final String REDIS_HOST = System.getenv().getOrDefault("REDIS_HOST", "redis-cache");
    private static final int REDIS_PORT = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
    private static final JedisPool jedisPool = new JedisPool(REDIS_HOST, REDIS_PORT);

    public static void main(String[] args) {
        
        // 1. Booting up a lightweight web server 
        Javalin app = Javalin.create().start(8083);

        app.get("/api/notifications/health", ctx -> {
            ctx.json("{\"status\": \"UP\", \"service\": \"notifications\"}");
        });

        System.out.println("Notifications Service running on port 8083");

        // 2. Starting the Asynchronous Background Worker
        startRedisListener();
    }

    private static void startRedisListener() {
        Thread workerThread = new Thread(() -> {
            System.out.println("Listening for events on Redis 'notification_queue'...");
            
            while (!Thread.currentThread().isInterrupted()) {
                try (Jedis jedis = jedisPool.getResource()) {
                    while (!Thread.currentThread().isInterrupted()) {
                        // BRPOP with 5s timeout allows the thread to check interrupted state periodically
                        List<String> message = jedis.brpop(5, "notification_queue");
                        
                        // message.get(0) is the queue name, message.get(1) is the actual payload
                        if (message != null && !message.isEmpty()) {
                            String payload = message.get(1);
                            System.out.println("MOCK SMS DISPATCHED -> " + payload);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Redis connection error: " + e.getMessage() + ". Reconnecting in 2s...");
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        });
        workerThread.setDaemon(true);
        workerThread.setName("notification-redis-worker");
        workerThread.start();
    }
}