package com.zkube.transactions;

import io.javalin.Javalin;
import io.restassured.RestAssured;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

public class TransactionsApiTest {

    private static Javalin app;

    @BeforeAll
    static void startServer() {
        app = TransactionsApplication.createApp().start(0);
        RestAssured.port = app.port();
    }

    @AfterAll
    static void stopServer() {
        if (app != null) {
            app.stop();
        }
    }

    @Test
    void shouldReturnUpStatusOnTransactionsHealthCheck() {
        given()
        .when()
            .get("/api/transactions/health")
        .then()
            .statusCode(200)
            .body("status", equalTo("UP"))
            .body("service", equalTo("transactions"));
    }
}
