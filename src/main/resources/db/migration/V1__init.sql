CREATE TABLE wallets (
    id UUID PRIMARY KEY,
    entity_id UUID NOT NULL,
    entity_type VARCHAR(255) NOT NULL,
    balance NUMERIC(14,2) NOT NULL DEFAULT 0.00,
    currency VARCHAR(3) NOT NULL DEFAULT 'INR',
    active BOOLEAN NOT NULL DEFAULT TRUE,
    version BIGINT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    CONSTRAINT uq_wallets_entity UNIQUE (entity_id, entity_type)
);

CREATE TABLE wallet_transactions (
    id UUID PRIMARY KEY,
    wallet_id UUID NOT NULL REFERENCES wallets(id),
    amount NUMERIC(14,2) NOT NULL,
    transaction_type VARCHAR(50) NOT NULL,
    reference_id UUID NOT NULL,
    category VARCHAR(50) NOT NULL,
    description VARCHAR(255),
    created_at TIMESTAMP NOT NULL,
    metadata TEXT,
    CONSTRAINT uq_wallet_transactions_ref UNIQUE (wallet_id, reference_id, transaction_type)
);

CREATE TABLE wallet_topups (
    id UUID PRIMARY KEY,
    advertiser_id UUID NOT NULL,
    amount NUMERIC(14,2) NOT NULL,
    gateway_order_id VARCHAR(255) NOT NULL DEFAULT '',
    gateway_name VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    idempotency_key VARCHAR(255) UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_wallet_transactions_wallet_id_created_at ON wallet_transactions(wallet_id, created_at);
CREATE INDEX idx_wallets_entity_id ON wallets (entity_id);
CREATE INDEX idx_wallet_transactions_reference_id ON wallet_transactions (reference_id);
