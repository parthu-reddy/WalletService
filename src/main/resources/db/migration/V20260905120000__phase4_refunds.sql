ALTER TABLE wallets ALTER COLUMN balance TYPE NUMERIC(14, 2);
ALTER TABLE wallets ALTER COLUMN currency TYPE CHAR(3);
ALTER TABLE wallets ADD COLUMN active BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE wallets DROP CONSTRAINT uq_wallets_entity;
ALTER TABLE wallets ADD CONSTRAINT uq_wallets_entity UNIQUE (entity_type, entity_id);

ALTER TABLE wallet_transactions ALTER COLUMN amount TYPE NUMERIC(14, 2);
ALTER TABLE wallet_transactions DROP COLUMN reference_id;
ALTER TABLE wallet_transactions ADD COLUMN reference_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000'::uuid;
ALTER TABLE wallet_transactions ADD COLUMN category VARCHAR(40) NOT NULL DEFAULT 'UNKNOWN';
ALTER TABLE wallet_transactions DROP COLUMN metadata;
ALTER TABLE wallet_transactions ADD COLUMN metadata JSONB;
ALTER TABLE wallet_transactions ADD CONSTRAINT uq_wallet_transactions UNIQUE (wallet_id, reference_id, transaction_type);

ALTER TABLE wallet_topups ALTER COLUMN amount TYPE NUMERIC(14, 2);
ALTER TABLE wallet_topups DROP CONSTRAINT wallet_topups_order_id_key;
ALTER TABLE wallet_topups DROP COLUMN order_id;
ALTER TABLE wallet_topups ADD COLUMN gateway_order_id VARCHAR(255) NOT NULL DEFAULT '';
ALTER TABLE wallet_topups ADD COLUMN idempotency_key VARCHAR(255) UNIQUE;
