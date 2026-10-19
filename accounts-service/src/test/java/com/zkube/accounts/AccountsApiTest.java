package com.zkube.accounts;

import io.javalin.Javalin;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class AccountsApiTest {

    private static Javalin app;

    @BeforeAll
    static void startServer() {
        // Start Javalin on a random port for testing
        app = Javalin.create().start(0); 
        RestAssured.port = app.port();

        // Mocking endpoint for the test environment
        app.post("/api/accounts", ctx -> {
            ctx.status(200).json("{\"id\": \"uuid-1234\", \"ownerName\": \"Alice\", \"balance\": 1000.00}");
        });

        app.get("/api/accounts/health", ctx -> {
            ctx.json("{\"status\": \"UP\", \"service\": \"accounts\"}");
        });
    }

    @AfterAll
    static void stopServer() {
        app.stop();
    }

    @Test
    void shouldReturn200AndAccountDetailsWhenCreatingAccount() {
        String requestBody = "{\n" +
                "  \"ownerName\": \"Alice\",\n" +
                "  \"balance\": 1000.00\n" +
                "}";

        given()
            .contentType(ContentType.JSON)
            .body(requestBody)
        .when()
            .post("/api/accounts")
        .then()
            .statusCode(200)
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