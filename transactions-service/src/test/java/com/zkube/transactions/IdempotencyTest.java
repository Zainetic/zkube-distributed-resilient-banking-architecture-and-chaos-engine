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
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IdempotencyTest {

    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("zkube_chaos")
            .withUsername("postgres")
            .withPassword("admin");

    private static Javalin app;
    private static UUID accountSender;
    private static UUID accountReceiver;

    @BeforeAll
    static void setup() throws Exception {
        postgres.start();

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

            accountSender = UUID.randomUUID();
            accountReceiver = UUID.randomUUID();
            stmt.execute(String.format("INSERT INTO accounts (id, owner_name, balance) VALUES ('%s', 'Sender', 100.00);", accountSender));
            stmt.execute(String.format("INSERT INTO accounts (id, owner_name, balance) VALUES ('%s', 'Receiver', 50.00);", accountReceiver));
        }

        TransactionsApplication.setJedisPool(null);
        Database.init(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());

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
    void testIdempotentRetryReturnsCachedResponseWithoutDoubleDebit() throws Exception {
        String idempotencyKey = "key-" + UUID.randomUUID();
        String payload = String.format("{\"fromAccount\": \"%s\", \"toAccount\": \"%s\", \"amount\": 30.00}", accountSender, accountReceiver);

        // 1. First execution
        String firstTransferId = given()
                .contentType(ContentType.JSON)
                .header("Idempotency-Key", idempotencyKey)
                .body(payload)
                .when()
                .post("/api/transfers")
                .then()
                .statusCode(200)
                .body("status", equalTo("SUCCESS"))
                .body("transferId", notNullValue())
                .extract()
                .path("transferId");

        // 2. Second execution with identical idempotency key (simulating network timeout retry)
        given()
                .contentType(ContentType.JSON)
                .header("Idempotency-Key", idempotencyKey)
                .body(payload)
                .when()
                .post("/api/transfers")
                .then()
                .statusCode(200)
                .body("status", equalTo("SUCCESS"))
                .body("transferId", equalTo(firstTransferId))
                .body("idempotent", equalTo(true));

        // 3. Verify Sender was debited EXACTLY ONCE ($100 - $30 = $70, NOT $40)
        try (Connection conn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement stmt = conn.createStatement()) {

            ResultSet rs = stmt.executeQuery(String.format("SELECT balance FROM accounts WHERE id = '%s';", accountSender));
            assertTrue(rs.next());
            BigDecimal balance = rs.getBigDecimal("balance");
            assertEquals(0, new BigDecimal("70.00").compareTo(balance), "Sender balance should be $70.00 after idempotent retry");

            ResultSet rsRecv = stmt.executeQuery(String.format("SELECT balance FROM accounts WHERE id = '%s';", accountReceiver));
            assertTrue(rsRecv.next());
            BigDecimal recvBalance = rsRecv.getBigDecimal("balance");
            assertEquals(0, new BigDecimal("80.00").compareTo(recvBalance), "Receiver balance should be $80.00 after idempotent retry");
        }
    }
}
