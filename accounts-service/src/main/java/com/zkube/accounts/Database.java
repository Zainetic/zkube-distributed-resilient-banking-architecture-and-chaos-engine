package com.zkube.accounts;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

public class Database {
    private static HikariDataSource dataSource;

    public static void init() {
        String jdbcUrl = System.getProperty("DB_URL", System.getenv().getOrDefault("DB_URL", "jdbc:postgresql://postgres-db:5432/zkube_chaos"));
        String username = System.getProperty("DB_USER", System.getenv().getOrDefault("DB_USER", "postgres"));
        String password = System.getProperty("DB_PASSWORD", System.getenv().getOrDefault("DB_PASSWORD", "admin"));
        init(jdbcUrl, username, password);
    }

    public static synchronized void init(String jdbcUrl, String username, String password) {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        
        // Connection pool settings optimized for microservices
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        
        dataSource = new HikariDataSource(config);
        System.out.println("Database Connection Pool Initialized: " + jdbcUrl);
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