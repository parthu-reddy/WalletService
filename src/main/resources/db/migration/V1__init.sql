CREATE TABLE wallets (
    id UUID PRIMARY KEY,
    entity_id UUID NOT NULL,
    entity_type VARCHAR(255) NOT NULL,
    balance NUMERIC(19, 4) NOT NULL DEFAULT 0,
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    status VARCHAR(50) NOT NULL,
    version BIGINT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    CONSTRAINT uq_wallets_entity UNIQUE (entity_id, entity_type)
);

CREATE TABLE wallet_transactions (
    id UUID PRIMARY KEY,
    wallet_id UUID NOT NULL,
    amount NUMERIC(19, 4) NOT NULL,
    transaction_type VARCHAR(50) NOT NULL,
    reference_id VARCHAR(255) NOT NULL UNIQUE,
    description VARCHAR(255),
    created_at TIMESTAMP NOT NULL,
    metadata TEXT
);

CREATE TABLE wallet_topups (
    id UUID PRIMARY KEY,
    advertiser_id UUID NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    order_id VARCHAR(255) NOT NULL,
    gateway_name VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (order_id)
);






CREATE INDEX idx_wallet_transactions_wallet_id_created_at ON wallet_transactions(wallet_id, created_at);

CREATE INDEX idx_wallets_entity_id ON wallets (entity_id);

CREATE INDEX idx_wallet_transactions_reference_id ON wallet_transactions (reference_id);