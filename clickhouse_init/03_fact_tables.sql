-- ============================================
-- ClickHouse Data Warehouse - Fact Tables
-- ============================================

USE ecommerce_dw;

-- ============================================
-- FACT_ORDERS - Main Fact Table
-- Granularity: One row per order item
-- ============================================
CREATE TABLE IF NOT EXISTS fact_orders (
    -- Surrogate Keys (FK to dimensions)
    order_key UInt64,                   -- Degenerate dimension (order_id hash)
    date_key UInt32,                    -- FK to dim_time
    customer_key UInt64,                -- FK to dim_customers
    product_key UInt64,                 -- FK to dim_products
    seller_key UInt64,                  -- FK to dim_sellers
    payment_type_key UInt8,             -- FK to dim_payment_type
    status_key UInt8,                   -- FK to dim_order_status
    
    -- Natural Keys (for debugging/auditing)
    order_id String,
    order_item_id UInt8,
    
    -- Measures
    quantity UInt32 DEFAULT 1,
    unit_price Decimal(10, 2),
    freight_value Decimal(10, 2),
    total_amount Decimal(10, 2),        -- unit_price + freight_value
    payment_value Decimal(10, 2),
    payment_installments UInt8,
    
    -- Review Measures
    review_score Nullable(UInt8),
    
    -- Timestamps
    order_purchase_timestamp DateTime,
    order_approved_at Nullable(DateTime),
    order_delivered_carrier_date Nullable(DateTime),
    order_delivered_customer_date Nullable(DateTime),
    order_estimated_delivery_date Nullable(DateTime),
    
    -- Calculated Measures
    delivery_days Nullable(Int32),      -- Actual delivery days
    estimated_delivery_days Nullable(Int32),
    delivery_delay_days Nullable(Int32), -- Positive = late, Negative = early
    
    -- ETL Metadata
    etl_inserted_at DateTime DEFAULT now(),
    etl_batch_id String
    
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(order_purchase_timestamp)
ORDER BY (date_key, customer_key, order_id, order_item_id)
SETTINGS index_granularity = 8192;

-- ============================================
-- FACT_PAYMENTS - Payment Transactions
-- Granularity: One row per payment
-- ============================================
CREATE TABLE IF NOT EXISTS fact_payments (
    order_key UInt64,
    payment_sequential UInt8,
    date_key UInt32,
    customer_key UInt64,
    payment_type_key UInt8,
    
    -- Natural Key
    order_id String,
    
    -- Measures
    payment_value Decimal(10, 2),
    payment_installments UInt8,
    
    -- Timestamps
    order_purchase_timestamp DateTime,
    
    -- ETL Metadata
    etl_inserted_at DateTime DEFAULT now()
    
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(order_purchase_timestamp)
ORDER BY (date_key, order_id, payment_sequential);

-- ============================================
-- FACT_REVIEWS - Customer Reviews
-- Granularity: One row per review
-- ============================================
CREATE TABLE IF NOT EXISTS fact_reviews (
    review_key UInt64,
    order_key UInt64,
    date_key UInt32,
    customer_key UInt64,
    product_key UInt64,
    seller_key UInt64,
    
    -- Natural Keys
    review_id String,
    order_id String,
    
    -- Measures
    review_score UInt8,
    has_comment UInt8,
    comment_length UInt32,
    
    -- Timestamps
    review_creation_date DateTime,
    review_answer_timestamp Nullable(DateTime),
    response_time_hours Nullable(Float32),
    
    -- ETL Metadata
    etl_inserted_at DateTime DEFAULT now()
    
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(review_creation_date)
ORDER BY (date_key, review_id);

-- ============================================
-- RAW TABLES - For Bronze Layer Data
-- ============================================

-- Raw orders from CDC
CREATE TABLE IF NOT EXISTS orders_raw (
    order_id String,
    customer_id String,
    order_status String,
    order_purchase_timestamp Nullable(DateTime64(3)),
    order_approved_at Nullable(DateTime64(3)),
    order_delivered_carrier_date Nullable(DateTime64(3)),
    order_delivered_customer_date Nullable(DateTime64(3)),
    order_estimated_delivery_date Nullable(DateTime64(3)),
    _op String,                         -- CDC operation: c/u/d
    _ts_ms UInt64,                      -- CDC timestamp
    _source_table String DEFAULT 'orders',
    _ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(_ingested_at)
ORDER BY (order_id, _ts_ms);

-- Raw order items from CDC
CREATE TABLE IF NOT EXISTS order_items_raw (
    order_id String,
    order_item_id UInt8,
    product_id String,
    seller_id String,
    shipping_limit_date Nullable(DateTime64(3)),
    price Decimal(10, 2),
    freight_value Decimal(10, 2),
    _op String,
    _ts_ms UInt64,
    _source_table String DEFAULT 'order_items',
    _ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(_ingested_at)
ORDER BY (order_id, order_item_id, _ts_ms);
