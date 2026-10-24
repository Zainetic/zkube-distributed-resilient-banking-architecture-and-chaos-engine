package com.zkube.transactions;

import io.javalin.Javalin;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TransferConcurrencyTest {

    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("zkube_chaos")
            .withUsername("postgres")
            .withPassword("admin");

    private static Javalin app;
    private static UUID accountA;
    private static UUID accountB;

    @BeforeAll
    static void setup() throws Exception {
        postgres.start();

        // 1. Initialize DB Schema with Idempotency Key
        try (Connection conn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE IF NOT EXISTS accounts (" +
                    "id UUID PRIMARY KEY DEFAULT gen_random_uuid(), " +
                    "owner_name VARCHAR(100) NOT NULL, " +
                    "balance DECIMAL(15, 2) NOT NULL CHECK (balance >= 0), " +
                    "status VARCHAR(20) DEFAULT 'ACTIVE', " +
                    "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);");

            stmt.execute("CREATE TABLE IF NOT EXISTS transfers (" +
                    "id UUID PRIMARY KEY DEFAULT gen_random_uuid(), " +
                    "from_account UUID NOT NULL REFERENCES accounts(id), " +
                    "to_account UUID NOT NULL REFERENCES accounts(id), " +
                    "amount DECIMAL(15, 2) NOT NULL CHECK (amount > 0), " +
                    "status VARCHAR(20) NOT NULL, " +
                    "idempotency_key VARCHAR(100) UNIQUE, " +
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);");

            // Seed Account A ($1000.00) and Account B ($1000.00) -> Total $2000.00
            accountA = UUID.randomUUID();
            accountB = UUID.randomUUID();
            stmt.execute(String.format("INSERT INTO accounts (id, owner_name, balance) VALUES ('%s', 'Alice', 1000.00);", accountA));
            stmt.execute(String.format("INSERT INTO accounts (id, owner_name, balance) VALUES ('%s', 'Bob', 1000.00);", accountB));
        }

        // 2. Initialize Database pool targeting testcontainer
        TransactionsApplication.setJedisPool(null); // Bypass Redis during pure DB concurrency test
        Database.init(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());

        // 3. Start real Javalin application on dynamic port
        app = TransactionsApplication.createApp().start(0);
        RestAssured.port = app.port();
    }

    @AfterAll
    static void tearDown() {
        if (app != null) {
            app.stop();
        }
        postgres.stop();
    }

    @Test
    void testConcurrentOppositeTransfersNoDeadlockAndBalanceConserved() throws Exception {
        int threadCount = 20;
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch endGate = new CountDownLatch(threadCount);

        List<Future<Integer>> futures = new ArrayList<>();

        // Submit 10 transfers A -> B ($10 each) and 10 transfers B -> A ($10 each)
        for (int i = 0; i < threadCount; i++) {
            final boolean aToB = (i % 2 == 0);
            futures.add(executor.submit(() -> {
                startGate.await();
                String from = aToB ? accountA.toString() : accountB.toString();
                String to = aToB ? accountB.toString() : accountA.toString();
                String body = String.format("{\"fromAccount\": \"%s\", \"toAccount\": \"%s\", \"amount\": 10.00}", from, to);

                int statusCode = given()
                        .contentType(ContentType.JSON)
                        .body(body)
                        .when()
                        .post("/api/transfers")
                        .then()
                        .extract()
                        .statusCode();

                endGate.countDown();
                return statusCode;
            }));
        }

        // Fire all threads simultaneously
        startGate.countDown();
        assertTrue(endGate.await(30, TimeUnit.SECONDS), "All concurrent transfers should complete within 30s without deadlocks");

        // Verify all transfers succeeded (200 OK, zero 40P01 deadlocks)
        for (Future<Integer> f : futures) {
            assertEquals(200, f.get().intValue(), "Transfer should succeed without deadlock");
        }

        // Verify Total Balance Conservation Invariant: Initial ($2000.00) == Final ($2000.00)
        try (Connection conn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement stmt = conn.createStatement()) {

            ResultSet rsA = stmt.executeQuery(String.format("SELECT balance FROM accounts WHERE id = '%s';", accountA));
            assertTrue(rsA.next());
            BigDecimal balanceA = rsA.getBigDecimal("balance");

            ResultSet rsB = stmt.executeQuery(String.format("SELECT balance FROM accounts WHERE id = '%s';", accountB));
            assertTrue(rsB.next());
            BigDecimal balanceB = rsB.getBigDecimal("balance");

            BigDecimal total = balanceA.add(balanceB);
            assertEquals(0, new BigDecimal("2000.00").compareTo(total),
                    "Total invariant violated! Balances: A=" + balanceA + ", B=" + balanceB + ", Total=" + total);
        }

        executor.shutdown();
    }
}
