ALTER TABLE wallets DROP COLUMN status;
ALTER TABLE wallets ADD COLUMN active BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE wallets ALTER COLUMN balance TYPE NUMERIC(14,2);
ALTER TABLE wallets ALTER COLUMN currency SET DEFAULT 'INR';

ALTER TABLE wallet_transactions DROP CONSTRAINT IF EXISTS wallet_transactions_reference_id_key;
ALTER TABLE wallet_transactions ADD COLUMN category VARCHAR(50) NOT NULL DEFAULT 'GENERAL';
ALTER TABLE wallet_transactions ALTER COLUMN amount TYPE NUMERIC(14,2);
ALTER TABLE wallet_transactions ALTER COLUMN reference_id TYPE UUID USING (uuid(reference_id));
ALTER TABLE wallet_transactions ADD CONSTRAINT uq_wallet_txn_ref UNIQUE (wallet_id, reference_id, transaction_type);

ALTER TABLE wallet_topups ALTER COLUMN amount TYPE NUMERIC(14,2);
ALTER TABLE wallet_topups ALTER COLUMN order_id TYPE UUID USING (uuid(order_id));
