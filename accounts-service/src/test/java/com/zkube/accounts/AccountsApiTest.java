package com.zkube.accounts;

import io.javalin.Javalin;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class AccountsApiTest {

    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("zkube_chaos")
            .withUsername("postgres")
            .withPassword("admin");

    private static Javalin app;

    @BeforeAll
    static void startServer() throws Exception {
        postgres.start();

        try (Connection conn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE IF NOT EXISTS accounts (" +
                    "id UUID PRIMARY KEY DEFAULT gen_random_uuid(), " +
                    "owner_name VARCHAR(100) NOT NULL, " +
                    "balance DECIMAL(15, 2) NOT NULL CHECK (balance >= 0), " +
                    "status VARCHAR(20) DEFAULT 'ACTIVE', " +
                    "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);");
        }

        Database.init(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());

        app = AccountsApplication.createApp().start(0);
        RestAssured.port = app.port();
    }

    @AfterAll
    static void stopServer() {
        if (app != null) {
            app.stop();
        }
        postgres.stop();
    }

    @Test
    void shouldReturn201AndAccountDetailsWhenCreatingAccount() {
        String requestBody = "{\n" +
                "  \"ownerName\": \"Alice\",\n" +
                "  \"initialBalance\": 1000.00\n" +
                "}";

        given()
            .contentType(ContentType.JSON)
            .body(requestBody)
        .when()
            .post("/api/accounts")
        .then()
            .statusCode(201)
            .body("ownerName", equalTo("Alice"))
            .body("balance", equalTo(1000.0f))
            .body("id", notNullValue());
    }

    @Test
    void shouldReturnUpStatusOnHealthCheck() {
        given()
        .when()
            .get("/api/accounts/health")
        .then()
            .statusCode(200)
            .body("status", equalTo("UP"))
            .body("service", equalTo("accounts"));
    }
}