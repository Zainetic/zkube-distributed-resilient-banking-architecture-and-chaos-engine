package com.zkube.accounts;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.javalin.Javalin;
import org.jooq.DSLContext;
import org.jooq.Record;

import java.math.BigDecimal;
import java.util.UUID;

import static org.jooq.impl.DSL.*;

public class AccountsApplication {

    public static Javalin createApp() {
        Javalin app = Javalin.create();

        app.get("/api/accounts/health", ctx -> {
            ctx.json("{\"status\": \"UP\", \"service\": \"accounts\"}");
        });

        // Endpoint: Creating a new bank account
        app.post("/api/accounts", ctx -> {
            AccountRequest req = ctx.bodyAsClass(AccountRequest.class);
            String ownerName = req.ownerName();
            BigDecimal balance = req.getEffectiveBalance();

            if (ownerName == null || ownerName.isBlank()) {
                ctx.status(400).json("{\"error\": \"ownerName is required\"}");
                return;
            }

            DSLContext dsl = Database.getContext();
            
            // Execute jOOQ Insert
            UUID newId = dsl.insertInto(table("accounts"), field("owner_name"), field("balance"))
               .values(ownerName, balance)
               .returningResult(field("id", UUID.class))
               .fetchOneInto(UUID.class);

            ctx.status(201).json("{\"accountId\": \"" + newId + "\", \"id\": \"" + newId + "\", \"ownerName\": \"" + ownerName + "\", \"balance\": " + balance + ", \"status\": \"CREATED\"}");
        });

        // Endpoint: Fetch an account's balance
        app.get("/api/accounts/{id}", ctx -> {
            UUID id;
            try {
                id = UUID.fromString(ctx.pathParam("id"));
            } catch (IllegalArgumentException e) {
                ctx.status(400).json("{\"error\": \"Invalid account UUID format\"}");
                return;
            }
            
            DSLContext dsl = Database.getContext();
            
            // Execute jOOQ Select
            Record record = dsl.select(field("id"), field("owner_name"), field("balance"))
               .from(table("accounts"))
               .where(field("id").eq(id))
               .fetchOne();

            if (record != null) {
                ctx.json(record.intoMap());
            } else {
                ctx.status(404).json("{\"error\": \"Account not found\"}");
            }
        });

        return app;
    }

    public static void main(String[] args) {
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "8081"));
        Database.init();
        createApp().start(port);
        System.out.println("Accounts Service running on port " + port);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record AccountRequest(String ownerName, BigDecimal initialBalance, BigDecimal balance) {
        public BigDecimal getEffectiveBalance() {
            if (initialBalance != null) return initialBalance;
            if (balance != null) return balance;
            return BigDecimal.ZERO;
        }
    }
}