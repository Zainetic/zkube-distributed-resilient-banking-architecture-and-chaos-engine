package com.zkube.accounts;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

public class Database {
    private static HikariDataSource dataSource;

    public static void init() {
        HikariConfig config = new HikariConfig();
        String jdbcUrl = System.getenv().getOrDefault("DB_URL", "jdbc:postgresql://postgres-db:5432/zkube_chaos");
        String username = System.getenv().getOrDefault("DB_USER", "postgres");
        String password = System.getenv().getOrDefault("DB_PASSWORD", "admin");

        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        
        // Connection pool settings optimized for microservices
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        
        dataSource = new HikariDataSource(config);
        System.out.println("Database Connection Pool Initialized.");
    }

    public static DSLContext getContext() {
        try {
            return DSL.using(dataSource, SQLDialect.POSTGRES);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get database context", e);
        }
    }

    public static HikariDataSource getDataSource() {
        return dataSource;
    }
}