package com.zkube.transactions;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.javalin.Javalin;
import org.jooq.DSLContext;
import org.jooq.Record;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.XAddParams;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.jooq.impl.DSL.*;

public class TransactionsApplication {

    private static final String REDIS_HOST = System.getenv().getOrDefault("REDIS_HOST", "redis-cache");
    private static final int REDIS_PORT = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
    private static JedisPool jedisPool = new JedisPool(REDIS_HOST, REDIS_PORT);

    public static void setJedisPool(JedisPool pool) {
        jedisPool = pool;
    }

    public static Javalin createApp() {
        Javalin app = Javalin.create();

        app.get("/api/transactions/health", ctx -> {
            ctx.json("{\"status\": \"UP\", \"service\": \"transactions\"}");
        });

        app.post("/api/transfers", ctx -> {
            TransferRequest req = ctx.bodyAsClass(TransferRequest.class);
            if (req.fromAccount() == null || req.toAccount() == null || req.amount() == null) {
                ctx.status(400).json("{\"status\": \"FAILED\", \"error\": \"Missing required transfer fields\"}");
                return;
            }

            UUID fromAccount;
            UUID toAccount;
            try {
                fromAccount = UUID.fromString(req.fromAccount());
                toAccount = UUID.fromString(req.toAccount());
            } catch (IllegalArgumentException e) {
                ctx.status(400).json("{\"status\": \"FAILED\", \"error\": \"Invalid UUID format for account IDs\"}");
                return;
            }

            if (fromAccount.equals(toAccount)) {
                ctx.status(400).json("{\"status\": \"FAILED\", \"error\": \"Cannot transfer funds to the same account\"}");
                return;
            }

            BigDecimal amount = req.amount();
            if (amount.compareTo(BigDecimal.ZERO) <= 0) {
                ctx.status(400).json("{\"status\": \"FAILED\", \"error\": \"Transfer amount must be greater than zero\"}");
                return;
            }

            String idempotencyKey = ctx.header("Idempotency-Key");
            if (idempotencyKey != null) {
                idempotencyKey = idempotencyKey.trim();
                if (idempotencyKey.isEmpty()) {
                    idempotencyKey = null;
                }
            }

            DSLContext dsl = Database.getContext();

            // 1. Fast-path check: Return cached response if Idempotency-Key was already processed
            if (idempotencyKey != null) {
                Record existing = dsl.select(field("id"), field("status"))
                                     .from(table("transfers"))
                                     .where(field("idempotency_key").eq(idempotencyKey))
                                     .fetchOne();
                if (existing != null) {
                    UUID existingId = existing.get(field("id", UUID.class));
                    ctx.status(200).json("{\"status\": \"SUCCESS\", \"transferId\": \"" + existingId + "\", \"idempotent\": true}");
                    return;
                }
            }

            try {
                final String finalIdempotencyKey = idempotencyKey;

                // 2. Deadlock-Free Ordered Locking: enforce global deterministic lock order
                UUID firstLock = fromAccount.compareTo(toAccount) < 0 ? fromAccount : toAccount;
                UUID secondLock = fromAccount.compareTo(toAccount) < 0 ? toAccount : fromAccount;

                // START ACID TRANSACTION
                UUID transferId = dsl.transactionResult(configuration -> {
                    DSLContext tx = using(configuration);

                    // Lock rows sequentially in deterministic order
                    tx.selectFrom(table("accounts"))
                      .where(field("id").in(firstLock, secondLock))
                      .orderBy(field("id").asc())
                      .forUpdate()
                      .execute();

                    // Verify sender account exists and balance is sufficient
                    Record sender = tx.select(field("balance"))
                                      .from(table("accounts"))
                                      .where(field("id").eq(fromAccount))
                                      .fetchOne();

                    if (sender == null) {
                        throw new IllegalArgumentException("Sender account not found.");
                    }

                    // Verify receiver account exists
                    Record receiver = tx.select(field("balance"))
                                        .from(table("accounts"))
                                        .where(field("id").eq(toAccount))
                                        .fetchOne();

                    if (receiver == null) {
                        throw new IllegalArgumentException("Receiver account not found.");
                    }

                    BigDecimal senderBalance = sender.get(field("balance", BigDecimal.class));
                    if (senderBalance.compareTo(amount) < 0) {
                        throw new IllegalStateException("Insufficient funds.");
                    }

                    // Deduct from Sender
                    tx.update(table("accounts"))
                      .set(field("balance"), senderBalance.subtract(amount))
                      .where(field("id").eq(fromAccount))
                      .execute();

                    // Add to Receiver
                    BigDecimal receiverBalance = receiver.get(field("balance", BigDecimal.class));
                    tx.update(table("accounts"))
                      .set(field("balance"), receiverBalance.add(amount))
                      .where(field("id").eq(toAccount))
                      .execute();

                    // Record the Transfer with Idempotency Key
                    return tx.insertInto(table("transfers"),
                                         field("from_account"),
                                         field("to_account"),
                                         field("amount"),
                                         field("status"),
                                         field("idempotency_key"))
                             .values(fromAccount, toAccount, amount, "COMPLETED", finalIdempotencyKey)
                             .returningResult(field("id", UUID.class))
                             .fetchOneInto(UUID.class);
                });
                // END ACID TRANSACTION - COMMIT CONFIRMED

                // 3. Publish to Redis Streams (crash-safe) and List (backward compatibility)
                try {
                    if (jedisPool != null) {
                        try (var jedis = jedisPool.getResource()) {
                            String eventPayload = "Transfer " + transferId + " completed for $" + amount;
                            Map<String, String> streamData = new HashMap<>();
                            streamData.put("transferId", transferId.toString());
                            streamData.put("amount", amount.toString());
                            streamData.put("payload", eventPayload);
                            jedis.xadd("notification_stream", XAddParams.xAddParams(), streamData);
                            jedis.lpush("notification_queue", eventPayload);
                        }
                    }
                } catch (Exception redisErr) {
                    System.err.println("Warning: Redis publish error: " + redisErr.getMessage());
                }

                ctx.status(200).json("{\"status\": \"SUCCESS\", \"transferId\": \"" + transferId + "\"}");

            } catch (Exception e) {
                // Handle concurrent race on idempotency key
                if (idempotencyKey != null && e.getMessage() != null &&
                    (e.getMessage().contains("idx_transfers_idempotency_key") || e.getMessage().contains("transfers_idempotency_key_key"))) {
                    Record existing = dsl.select(field("id"))
                                         .from(table("transfers"))
                                         .where(field("idempotency_key").eq(idempotencyKey))
                                         .fetchOne();
                    if (existing != null) {
                        UUID existingId = existing.get(field("id", UUID.class));
                        ctx.status(200).json("{\"status\": \"SUCCESS\", \"transferId\": \"" + existingId + "\", \"idempotent\": true}");
                        return;
                    }
                }
                ctx.status(400).json("{\"status\": \"FAILED\", \"error\": \"" + e.getMessage() + "\"}");
            }
        });

        return app;
    }

    public static void main(String[] args) {
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "8082"));
        Database.init();
        createApp().start(port);
        System.out.println("Transactions Service running on port " + port);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record TransferRequest(String fromAccount, String toAccount, BigDecimal amount) {}
}