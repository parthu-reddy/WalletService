-- Deduplicate wallets
WITH ranked AS (
  SELECT id, ROW_NUMBER() OVER (PARTITION BY entity_id, entity_type ORDER BY created_at) AS rn
    FROM wallets)
DELETE FROM wallets WHERE id IN (SELECT id FROM ranked WHERE rn > 1);

-- Add unique constraint (and index)
ALTER TABLE wallets ADD CONSTRAINT uq_wallets_entity UNIQUE (entity_id, entity_type);

-- Drop unused tables
DROP TABLE IF EXISTS wallet_outbox_events;
DROP TABLE IF EXISTS processed_events;
