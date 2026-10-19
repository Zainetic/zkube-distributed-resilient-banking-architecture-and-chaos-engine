package com.zkube.transactions;

import io.javalin.Javalin;
import org.jooq.DSLContext;
import org.jooq.Record;
import redis.clients.jedis.JedisPool;

import java.math.BigDecimal;
import java.util.UUID;

import static org.jooq.impl.DSL.*;

public class TransactionsApplication {

    private static final String REDIS_HOST = System.getenv().getOrDefault("REDIS_HOST", "redis-cache");
    private static final int REDIS_PORT = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
    private static final JedisPool jedisPool = new JedisPool(REDIS_HOST, REDIS_PORT);

    public static void main(String[] args) {
        
        Database.init();
        Javalin app = Javalin.create().start(8082); // Runs on 8082!
        
        app.get("/api/transactions/health", ctx -> {
            ctx.json("{\"status\": \"UP\", \"service\": \"transactions\"}");
        });

        app.post("/api/transfers", ctx -> {
            TransferRequest req = ctx.bodyAsClass(TransferRequest.class);
            UUID fromAccount = UUID.fromString(req.fromAccount());
            UUID toAccount = UUID.fromString(req.toAccount());
            BigDecimal amount = req.amount();

            DSLContext dsl = Database.getContext();

            try {
                // START ACID TRANSACTION
                UUID transferId = dsl.transactionResult(configuration -> {
                    DSLContext tx = using(configuration);

                    // 1. Lock BOTH rows simultaneously to prevent race conditions
                    tx.selectFrom(table("accounts"))
                      .where(field("id").in(fromAccount, toAccount))
                      .forUpdate()
                      .execute();

                    // 2. Check Sender's Balance
                    Record sender = tx.select(field("balance"))
                                      .from(table("accounts"))
                                      .where(field("id").eq(fromAccount))
                                      .fetchOne();

                    if (sender == null || sender.get(field("balance", BigDecimal.class)).compareTo(amount) < 0) {
                        throw new RuntimeException("Insufficient funds or account not found.");
                    }

                    // 3. Deduct from Sender
                    tx.update(table("accounts"))
                      .set(field("balance"), field("balance", BigDecimal.class).minus(amount))
                      .where(field("id").eq(fromAccount))
                      .execute();

                    // 4. Add to Receiver
                    tx.update(table("accounts"))
                      .set(field("balance"), field("balance", BigDecimal.class).plus(amount))
                      .where(field("id").eq(toAccount))
                      .execute();

                    // 5. Record the Transfer
                    return tx.insertInto(table("transfers"), field("from_account"), field("to_account"), field("amount"), field("status"))
                             .values(fromAccount, toAccount, amount, "COMPLETED")
                             .returningResult(field("id", UUID.class))
                             .fetchOneInto(UUID.class);
                });
                // END ACID TRANSACTION - COMMIT CONFIRMED

                // 6. Push to Redis Notification Queue after successful commit
                try (var jedis = jedisPool.getResource()) {
                    jedis.lpush("notification_queue", "Transfer " + transferId + " completed for $" + amount);
                }
                
                ctx.status(200).json("{\"status\": \"SUCCESS\", \"transferId\": \"" + transferId + "\"}");

            } catch (Exception e) {
                ctx.status(400).json("{\"status\": \"FAILED\", \"error\": \"" + e.getMessage() + "\"}");
            }
        });

        System.out.println("Transactions Service running on port 8082");
    }

    record TransferRequest(String fromAccount, String toAccount, BigDecimal amount) {}
}