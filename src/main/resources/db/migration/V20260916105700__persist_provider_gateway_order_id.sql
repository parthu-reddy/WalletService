ALTER TABLE wallet_topups
    ADD COLUMN IF NOT EXISTS provider_gateway_order_id VARCHAR(255);

-- Existing rows predate provider-id persistence. Retain their former response value for safe
-- idempotent retries; every newly-created row stores the provider's real identifier.
UPDATE wallet_topups
SET provider_gateway_order_id = gateway_order_id
WHERE provider_gateway_order_id IS NULL;

ALTER TABLE wallet_topups
    ALTER COLUMN provider_gateway_order_id SET NOT NULL;
