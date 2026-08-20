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
