-- Wallet money timestamps become TIMESTAMPTZ.
--
-- Why: the entities map Instant, which Hibernate stores as TIMESTAMPTZ (JDBC 2014), while these
-- columns were TIMESTAMP (JDBC 93). WalletService runs `ddl-auto: validate`, so Hibernate refused
-- to build the SessionFactory and the service would not have started in production. Caught by
-- WalletSchemaConsistencyTest, which compares the entities against these migrations without a
-- database (Testcontainers are excluded by project rule and H2 cannot execute this schema).
--
-- Direction of the fix: the columns move, not the entities. Every money timestamp elsewhere on the
-- platform -- the whole ledger schema -- is TIMESTAMPTZ, and a naive timestamp on a money row
-- silently drops the offset, which is not recoverable afterwards.
--
-- Forward-only: V1__init.sql is not edited. Existing values are read as UTC, which is what the
-- application wrote them as (Instant.now() and NOW() under a UTC container clock).

ALTER TABLE wallets
    ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at AT TIME ZONE 'UTC',
    ALTER COLUMN updated_at TYPE TIMESTAMPTZ USING updated_at AT TIME ZONE 'UTC';

ALTER TABLE wallet_transactions
    ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at AT TIME ZONE 'UTC';

-- wallet_topups matched its LocalDateTime entity, so it was not a startup failure, but a money row
-- should not carry a naive timestamp either. The entity moves to OffsetDateTime alongside this.
ALTER TABLE wallet_topups
    ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at AT TIME ZONE 'UTC',
    ALTER COLUMN updated_at TYPE TIMESTAMPTZ USING updated_at AT TIME ZONE 'UTC';
