package com.zkube.notifications;

import io.javalin.Javalin;
import io.restassured.RestAssured;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

public class NotificationsApiTest {

    private static Javalin app;

    @BeforeAll
    static void startServer() {
        app = Javalin.create().start(0);
        RestAssured.port = app.port();

        app.get("/api/notifications/health", ctx -> {
            ctx.json("{\"status\": \"UP\", \"service\": \"notifications\"}");
        });
    }

    @AfterAll
    static void stopServer() {
        app.stop();
    }

    @Test
    void shouldReturnUpStatusOnNotificationsHealthCheck() {
        given()
        .when()
            .get("/api/notifications/health")
        .then()
            .statusCode(200)
            .body("status", equalTo("UP"))
            .body("service", equalTo("notifications"));
    }
}
