-- ==============================================================================
-- ZKube FinTech Initial Schema
-- Auto-executed by Postgres /docker-entrypoint-initdb.d/ on first boot
-- ==============================================================================

-- 1. Accounts Table (The Source of Truth for Balances)
CREATE TABLE IF NOT EXISTS accounts (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    owner_name VARCHAR(100) NOT NULL,
    balance DECIMAL(15,2) NOT NULL CHECK (balance >= 0), -- Prevents negative balances at the DB level
    status VARCHAR(20) DEFAULT 'ACTIVE',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Transactions Table (The Immutable Transfer Ledger)
CREATE TABLE IF NOT EXISTS transfers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    from_account UUID NOT NULL REFERENCES accounts(id),
    to_account UUID NOT NULL REFERENCES accounts(id),
    amount DECIMAL(15, 2) NOT NULL CHECK (amount > 0),
    status VARCHAR(20) NOT NULL,
    idempotency_key VARCHAR(100) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
    
-- 3. Performance Indexes (Crucial for high-velocity lookups)
CREATE INDEX IF NOT EXISTS idx_transfers_source ON transfers(from_account);
CREATE INDEX IF NOT EXISTS idx_transfers_target ON transfers(to_account);
CREATE INDEX IF NOT EXISTS idx_transfers_status ON transfers(status);
CREATE INDEX IF NOT EXISTS idx_transfers_idempotency_key ON transfers(idempotency_key);