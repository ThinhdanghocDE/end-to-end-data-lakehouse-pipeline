-- ============================================
-- ClickHouse Data Warehouse - Materialized Views
-- Pre-aggregated tables for dashboard performance
-- ============================================

USE ecommerce_dw;

-- ============================================
-- SALES BY DAY - Daily Sales Summary
-- ============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sales_daily
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_date)
AS SELECT
    toDate(order_purchase_timestamp) as order_date,
    count() as total_orders,
    countDistinct(order_id) as unique_orders,
    countDistinct(customer_key) as unique_customers,
    sum(total_amount) as total_revenue,
    sum(freight_value) as total_freight,
    avg(total_amount) as avg_order_value,
    avg(review_score) as avg_review_score
FROM fact_orders
GROUP BY order_date;

-- ============================================
-- SALES BY MONTH - Monthly Sales Summary
-- ============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sales_monthly
ENGINE = SummingMergeTree()
ORDER BY (year, month)
AS SELECT
    toYear(order_purchase_timestamp) as year,
    toMonth(order_purchase_timestamp) as month,
    count() as total_orders,
    countDistinct(order_id) as unique_orders,
    countDistinct(customer_key) as unique_customers,
    sum(total_amount) as total_revenue,
    avg(total_amount) as avg_order_value
FROM fact_orders
GROUP BY year, month;

-- ============================================
-- SALES BY PRODUCT CATEGORY
-- ============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sales_by_category
ENGINE = SummingMergeTree()
ORDER BY (product_category_name_english)
AS SELECT
    p.product_category_name_english,
    count() as total_items_sold,
    countDistinct(f.order_id) as total_orders,
    sum(f.total_amount) as total_revenue,
    avg(f.unit_price) as avg_unit_price,
    avg(f.review_score) as avg_review_score
FROM fact_orders f
LEFT JOIN dim_products p ON f.product_key = p.product_key
GROUP BY p.product_category_name_english;

-- ============================================
-- SALES BY STATE (Geographic Analysis)
-- ============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sales_by_state
ENGINE = SummingMergeTree()
ORDER BY (customer_state)
AS SELECT
    c.customer_state,
    count() as total_orders,
    countDistinct(f.customer_key) as unique_customers,
    sum(f.total_amount) as total_revenue,
    avg(f.total_amount) as avg_order_value,
    avg(f.delivery_days) as avg_delivery_days
FROM fact_orders f
LEFT JOIN dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.customer_state;

-- ============================================
-- SELLER PERFORMANCE
-- ============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_seller_performance
ENGINE = SummingMergeTree()
ORDER BY (seller_key)
AS SELECT
    f.seller_key,
    s.seller_id,
    s.seller_city,
    s.seller_state,
    count() as total_items_sold,
    countDistinct(f.order_id) as total_orders,
    sum(f.total_amount) as total_revenue,
    avg(f.review_score) as avg_review_score,
    avg(f.delivery_delay_days) as avg_delivery_delay
FROM fact_orders f
LEFT JOIN dim_sellers s ON f.seller_key = s.seller_key
GROUP BY f.seller_key, s.seller_id, s.seller_city, s.seller_state;

-- ============================================
-- PAYMENT METHOD ANALYSIS
-- ============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_payment_analysis
ENGINE = SummingMergeTree()
ORDER BY (payment_type)
AS SELECT
    pt.payment_type,
    count() as total_payments,
    sum(f.payment_value) as total_payment_value,
    avg(f.payment_installments) as avg_installments,
    avg(f.payment_value) as avg_payment_value
FROM fact_orders f
LEFT JOIN dim_payment_type pt ON f.payment_type_key = pt.payment_type_key
GROUP BY pt.payment_type;

-- ============================================
-- HOURLY SALES PATTERN
-- ============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sales_hourly_pattern
ENGINE = SummingMergeTree()
ORDER BY (hour_of_day, day_of_week)
AS SELECT
    toHour(order_purchase_timestamp) as hour_of_day,
    toDayOfWeek(order_purchase_timestamp) as day_of_week,
    count() as total_orders,
    sum(total_amount) as total_revenue
FROM fact_orders
GROUP BY hour_of_day, day_of_week;

-- ============================================
-- CUSTOMER SEGMENTATION (RFM-like)
-- ============================================
CREATE TABLE IF NOT EXISTS customer_segments (
    customer_key UInt64,
    customer_id String,
    total_orders UInt32,
    total_spent Decimal(12, 2),
    first_order_date Date,
    last_order_date Date,
    days_since_last_order UInt32,
    avg_order_value Decimal(10, 2),
    avg_review_score Float32,
    customer_segment String,            -- VIP, Regular, At Risk, Lost
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY customer_key;

-- ============================================
-- REAL-TIME DASHBOARD METRICS
-- ============================================
CREATE TABLE IF NOT EXISTS realtime_metrics (
    metric_date Date,
    metric_hour UInt8,
    orders_count UInt32,
    revenue Decimal(12, 2),
    avg_order_value Decimal(10, 2),
    new_customers UInt32,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (metric_date, metric_hour);
