package com.zkube.notifications;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XPendingParams;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.resps.StreamPendingEntry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class CrashSafeNotificationTest {

    static GenericContainer<?> redis = new GenericContainer<>("redis:7.2-alpine")
            .withExposedPorts(6379);

    private static JedisPool jedisPool;

    @BeforeAll
    static void setup() {
        redis.start();
        jedisPool = new JedisPool(redis.getHost(), redis.getMappedPort(6379));
        NotificationsApplication.setJedisPool(jedisPool);
    }

    @AfterAll
    static void tearDown() {
        NotificationsApplication.stopWorker();
        if (jedisPool != null) {
            jedisPool.close();
        }
        redis.stop();
    }

    @Test
    void testMessagePreservedOnWorkerCrashAndRecoveredByNextWorker() throws Exception {
        try (Jedis jedis = jedisPool.getResource()) {
            // 1. Create consumer group on stream
            jedis.xgroupCreate("notification_stream", "notification_group", StreamEntryID.LAST_ENTRY, true);

            // 2. Publish two transfer notification events
            Map<String, String> event1 = new HashMap<>();
            event1.put("payload", "Transfer tx-crash-01 completed for $50.00");
            StreamEntryID id1 = jedis.xadd("notification_stream", XAddParams.xAddParams(), event1);

            Map<String, String> event2 = new HashMap<>();
            event2.put("payload", "Transfer tx-crash-02 completed for $75.00");
            StreamEntryID id2 = jedis.xadd("notification_stream", XAddParams.xAddParams(), event2);

            // 3. Worker 1 reads entry 1, but "crashes" before sending XACK (simulating ungraceful pod kill)
            Map<String, StreamEntryID> query = new HashMap<>();
            query.put("notification_stream", StreamEntryID.UNRECEIVED_ENTRY);
            List<Map.Entry<String, List<StreamEntry>>> readEvents = jedis.xreadGroup(
                "notification_group",
                "crashed-worker",
                XReadGroupParams.xReadGroupParams().count(1),
                query
            );

            assertNotNull(readEvents);
            assertEquals(1, readEvents.get(0).getValue().size());
            assertEquals(id1, readEvents.get(0).getValue().get(0).getID());
            // Intentionally do NOT call XACK -> Simulates worker crash mid-message!

            // 4. Verify message is NOT lost in Redis: It exists in Pending Entries List (PEL)
            XPendingParams params = new XPendingParams().count(10);
            List<StreamPendingEntry> pendingBefore = jedis.xpending("notification_stream", "notification_group", params);
            assertEquals(1, pendingBefore.size(), "Message must remain in Pending Entries List across worker crash");
            assertEquals(id1, pendingBefore.get(0).getID());

            // 5. Start recovery stream worker
            NotificationsApplication.startStreamWorker("crashed-worker");

            // Wait for worker to recover pending messages and process unreceived messages
            Thread.sleep(2500);

            // 6. Verify that all entries were re-processed and acknowledged
            List<StreamPendingEntry> pendingAfter = jedis.xpending("notification_stream", "notification_group", params);
            assertTrue(pendingAfter.isEmpty(), "All pending unacknowledged entries should be recovered and ACKed");
        }
    }
}
