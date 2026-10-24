package com.zkube.notifications;

import io.javalin.Javalin;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class NotificationsApplication {

    private static final String REDIS_HOST = System.getenv().getOrDefault("REDIS_HOST", "redis-cache");
    private static final int REDIS_PORT = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
    private static JedisPool jedisPool = new JedisPool(REDIS_HOST, REDIS_PORT);
    private static volatile boolean running = true;
    private static Thread workerThread;

    public static void setJedisPool(JedisPool pool) {
        jedisPool = pool;
    }

    public static void stopWorker() {
        running = false;
        if (workerThread != null) {
            workerThread.interrupt();
        }
    }

    public static Javalin createApp() {
        Javalin app = Javalin.create();

        app.get("/api/notifications/health", ctx -> {
            ctx.json("{\"status\": \"UP\", \"service\": \"notifications\"}");
        });

        return app;
    }

    public static void main(String[] args) {
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "8083"));
        createApp().start(port);
        System.out.println("Notifications Service running on port " + port);

        // Start crash-safe stream worker
        startStreamWorker("worker-" + UUID.randomUUID().toString().substring(0, 8));
    }

    public static void startStreamWorker(String consumerName) {
        running = true;
        workerThread = new Thread(() -> {
            System.out.println("Listening for crash-safe events on Redis Stream 'notification_stream' and Queue 'notification_queue'...");

            while (running && !Thread.currentThread().isInterrupted()) {
                try (Jedis jedis = jedisPool.getResource()) {
                    // 1. Ensure consumer group exists
                    try {
                        jedis.xgroupCreate("notification_stream", "notification_group", StreamEntryID.LAST_ENTRY, true);
                    } catch (Exception ignored) {
                        // Group already exists
                    }

                    // 2. Recovery phase: Read and process any unacknowledged pending messages for this consumer
                    processPendingStreamEntries(jedis, consumerName);

                    // 3. Continuous consumption loop
                    while (running && !Thread.currentThread().isInterrupted()) {
                        boolean receivedStream = false;

                        // A. Check Redis Stream with XREADGROUP (Crash-Safe with XACK)
                        Map<String, StreamEntryID> streamQuery = new HashMap<>();
                        streamQuery.put("notification_stream", StreamEntryID.UNRECEIVED_ENTRY);
                        XReadGroupParams groupParams = XReadGroupParams.xReadGroupParams().block(1000).count(10);
                        List<Map.Entry<String, List<StreamEntry>>> streamEvents = jedis.xreadGroup(
                            "notification_group",
                            consumerName,
                            groupParams,
                            streamQuery
                        );

                        if (streamEvents != null && !streamEvents.isEmpty()) {
                            for (Map.Entry<String, List<StreamEntry>> streamEntry : streamEvents) {
                                for (StreamEntry entry : streamEntry.getValue()) {
                                    receivedStream = true;
                                    String payload = entry.getFields().get("payload");
                                    if (payload == null) {
                                        payload = entry.getFields().toString();
                                    }
                                    handleDispatchedNotification(payload);
                                    // Acknowledge after successful processing
                                    jedis.xack("notification_stream", "notification_group", entry.getID());
                                }
                            }
                        }

                        // B. Fallback check on legacy list queue (Backward Compatibility)
                        if (!receivedStream) {
                            List<String> listMsg = jedis.brpop(1, "notification_queue");
                            if (listMsg != null && listMsg.size() > 1) {
                                handleDispatchedNotification(listMsg.get(1));
                            }
                        }
                    }
                } catch (Exception e) {
                    if (!running || Thread.currentThread().isInterrupted()) {
                        break;
                    }
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
        workerThread.setName("notification-stream-worker");
        workerThread.start();
    }

    private static void processPendingStreamEntries(Jedis jedis, String consumerName) {
        try {
            Map<String, StreamEntryID> pendingQuery = new HashMap<>();
            pendingQuery.put("notification_stream", new StreamEntryID("0-0"));
            List<Map.Entry<String, List<StreamEntry>>> pending = jedis.xreadGroup(
                "notification_group",
                consumerName,
                XReadGroupParams.xReadGroupParams().count(50),
                pendingQuery
            );

            if (pending != null) {
                for (Map.Entry<String, List<StreamEntry>> pEntry : pending) {
                    for (StreamEntry entry : pEntry.getValue()) {
                        String payload = entry.getFields().get("payload");
                        if (payload != null) {
                            System.out.println("[RECOVERY] Reprocessing pending stream entry " + entry.getID());
                            handleDispatchedNotification(payload);
                            jedis.xack("notification_stream", "notification_group", entry.getID());
                        }
                    }
                }
            }
        } catch (Exception e) {
            // No pending or stream doesn't exist yet
        }
    }

    public static void handleDispatchedNotification(String payload) {
        System.out.println("MOCK SMS DISPATCHED -> " + payload);
    }
}