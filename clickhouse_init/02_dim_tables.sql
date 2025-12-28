-- ============================================
-- ClickHouse Data Warehouse - Dimension Tables
-- ============================================

USE ecommerce_dw;

-- ============================================
-- DIM_TIME - Time/Date Dimension
-- ============================================
CREATE TABLE IF NOT EXISTS dim_time (
    date_key UInt32,                    -- YYYYMMDD format
    full_date Date,
    year UInt16,
    quarter UInt8,
    month UInt8,
    month_name String,
    week_of_year UInt8,
    day_of_month UInt8,
    day_of_week UInt8,
    day_name String,
    is_weekend UInt8,
    is_holiday UInt8
) ENGINE = MergeTree()
ORDER BY date_key;

-- Populate dim_time for years 2016-2025
INSERT INTO dim_time
SELECT
    toUInt32(formatDateTime(date, '%Y%m%d')) as date_key,
    date as full_date,
    toYear(date) as year,
    toQuarter(date) as quarter,
    toMonth(date) as month,
    multiIf(
        toMonth(date) = 1, 'January',
        toMonth(date) = 2, 'February',
        toMonth(date) = 3, 'March',
        toMonth(date) = 4, 'April',
        toMonth(date) = 5, 'May',
        toMonth(date) = 6, 'June',
        toMonth(date) = 7, 'July',
        toMonth(date) = 8, 'August',
        toMonth(date) = 9, 'September',
        toMonth(date) = 10, 'October',
        toMonth(date) = 11, 'November',
        'December'
    ) as month_name,
    toWeek(date) as week_of_year,
    toDayOfMonth(date) as day_of_month,
    toDayOfWeek(date) as day_of_week,
    multiIf(
        toDayOfWeek(date) = 1, 'Monday',
        toDayOfWeek(date) = 2, 'Tuesday',
        toDayOfWeek(date) = 3, 'Wednesday',
        toDayOfWeek(date) = 4, 'Thursday',
        toDayOfWeek(date) = 5, 'Friday',
        toDayOfWeek(date) = 6, 'Saturday',
        'Sunday'
    ) as day_name,
    if(toDayOfWeek(date) >= 6, 1, 0) as is_weekend,
    0 as is_holiday
FROM (
    SELECT arrayJoin(
        arrayMap(x -> toDate('2016-01-01') + x, range(3653))
    ) as date
);

-- ============================================
-- DIM_CUSTOMERS - Customer Dimension (SCD Type 1)
-- ============================================
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key UInt64,                -- Surrogate key
    customer_id String,                 -- Natural key
    customer_unique_id String,
    customer_city String,
    customer_state String,
    customer_zip_code_prefix String,
    customer_region String,             -- Derived: Norte, Nordeste, etc.
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY customer_key;

-- ============================================
-- DIM_PRODUCTS - Product Dimension (SCD Type 1)
-- ============================================
CREATE TABLE IF NOT EXISTS dim_products (
    product_key UInt64,                 -- Surrogate key
    product_id String,                  -- Natural key
    product_category_name String,
    product_category_name_english String,
    product_weight_g Nullable(UInt32),
    product_length_cm Nullable(UInt16),
    product_height_cm Nullable(UInt16),
    product_width_cm Nullable(UInt16),
    product_photos_qty Nullable(UInt8),
    product_size_category String,       -- Derived: Small, Medium, Large
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY product_key;

-- ============================================
-- DIM_SELLERS - Seller Dimension (SCD Type 1)
-- ============================================
CREATE TABLE IF NOT EXISTS dim_sellers (
    seller_key UInt64,                  -- Surrogate key
    seller_id String,                   -- Natural key
    seller_city String,
    seller_state String,
    seller_zip_code_prefix String,
    seller_region String,               -- Derived
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY seller_key;

-- ============================================
-- DIM_PAYMENT_TYPE - Payment Type Dimension
-- ============================================
CREATE TABLE IF NOT EXISTS dim_payment_type (
    payment_type_key UInt8,
    payment_type String,
    payment_type_description String
) ENGINE = MergeTree()
ORDER BY payment_type_key;

INSERT INTO dim_payment_type (payment_type_key, payment_type, payment_type_description) VALUES (1, 'credit_card', 'Credit Card Payment');
INSERT INTO dim_payment_type (payment_type_key, payment_type, payment_type_description) VALUES (2, 'boleto', 'Bank Slip (Boleto)');
INSERT INTO dim_payment_type (payment_type_key, payment_type, payment_type_description) VALUES (3, 'voucher', 'Voucher/Gift Card');
INSERT INTO dim_payment_type (payment_type_key, payment_type, payment_type_description) VALUES (4, 'debit_card', 'Debit Card Payment');
INSERT INTO dim_payment_type (payment_type_key, payment_type, payment_type_description) VALUES (5, 'not_defined', 'Not Defined');

-- ============================================
-- DIM_ORDER_STATUS - Order Status Dimension
-- ============================================
CREATE TABLE IF NOT EXISTS dim_order_status (
    status_key UInt8,
    order_status String,
    status_description String,
    is_completed UInt8,
    is_cancelled UInt8
) ENGINE = MergeTree()
ORDER BY status_key;

INSERT INTO dim_order_status (status_key, order_status, status_description, is_completed, is_cancelled) VALUES (1, 'created', 'Order Created', 0, 0);
INSERT INTO dim_order_status (status_key, order_status, status_description, is_completed, is_cancelled) VALUES (2, 'approved', 'Payment Approved', 0, 0);
INSERT INTO dim_order_status (status_key, order_status, status_description, is_completed, is_cancelled) VALUES (3, 'invoiced', 'Invoice Generated', 0, 0);
INSERT INTO dim_order_status (status_key, order_status, status_description, is_completed, is_cancelled) VALUES (4, 'processing', 'Processing', 0, 0);
INSERT INTO dim_order_status (status_key, order_status, status_description, is_completed, is_cancelled) VALUES (5, 'shipped', 'Shipped', 0, 0);
INSERT INTO dim_order_status (status_key, order_status, status_description, is_completed, is_cancelled) VALUES (6, 'delivered', 'Delivered', 1, 0);
INSERT INTO dim_order_status (status_key, order_status, status_description, is_completed, is_cancelled) VALUES (7, 'unavailable', 'Product Unavailable', 0, 1);
INSERT INTO dim_order_status (status_key, order_status, status_description, is_completed, is_cancelled) VALUES (8, 'canceled', 'Cancelled', 0, 1);

