-- WalletTopupRepository.findByGatewayOrderId is the gateway-callback lookup: every top-up webhook
-- resolves its topup row by this column, and it had no index, so each callback was a sequential
-- scan of wallet_topups.
--
-- Not UNIQUE. The column is NOT NULL DEFAULT '' (V1__init.sql:31), so any row written before a
-- gateway order id was assigned carries the empty string, and more than one of them can exist --
-- a unique index would fail to build and take the service's migration with it.
CREATE INDEX IF NOT EXISTS idx_wallet_topups_gateway_order_id
    ON wallet_topups (gateway_order_id);
