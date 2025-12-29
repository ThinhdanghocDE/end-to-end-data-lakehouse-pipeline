"""
Spark Batch Job: Gold Layer → ClickHouse Data Warehouse
Loads curated data from Gold/Silver layer into ClickHouse Star Schema.
"""

import os
from itertools import chain
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# --- Configuration ---
MINIO_PARAMS = {
    "endpoint": os.getenv("MINIO_ENDPOINT", "http://localhost:9002"),
    "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin")
}
GOLD_BUCKET = os.getenv("GOLD_BUCKET", "gold")
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver")

CH_CONFIG = {
    "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
    "port": os.getenv("CLICKHOUSE_PORT", "8124"),
    "http_port": os.getenv("CLICKHOUSE_HTTP_PORT", "8124"),
    "db": os.getenv("CLICKHOUSE_DB", "ecommerce_dw"),
    "user": os.getenv("CLICKHOUSE_USER", "default"),
    "password": os.getenv("CLICKHOUSE_PASSWORD", "")
}

# Brazilian state to region mapping
STATE_TO_REGION = {
    "AC": "Norte", "AP": "Norte", "AM": "Norte", "PA": "Norte", "RO": "Norte", "RR": "Norte", "TO": "Norte",
    "AL": "Nordeste", "BA": "Nordeste", "CE": "Nordeste", "MA": "Nordeste", "PB": "Nordeste",
    "PE": "Nordeste", "PI": "Nordeste", "RN": "Nordeste", "SE": "Nordeste",
    "DF": "Centro-Oeste", "GO": "Centro-Oeste", "MT": "Centro-Oeste", "MS": "Centro-Oeste",
    "ES": "Sudeste", "MG": "Sudeste", "RJ": "Sudeste", "SP": "Sudeste",
    "PR": "Sul", "RS": "Sul", "SC": "Sul"
}

def create_spark_session():
    """Create Spark session with Delta Lake, S3, and ClickHouse support"""
    builder = SparkSession.builder \
        .appName("EcommerceLoadWarehouse") \
        .config("spark.jars.packages", 
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.367,"
                "com.clickhouse:clickhouse-jdbc:0.6.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_PARAMS["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_PARAMS["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PARAMS["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    
    return builder.getOrCreate()

def execute_ch_sql(sql):
    """Execute SQL directly on ClickHouse via HTTP"""
    import requests
    url = f"http://{CH_CONFIG['host']}:{CH_CONFIG['http_port']}/"
    params = {"query": sql, "user": CH_CONFIG['user'], "password": CH_CONFIG['password']}
    try:
        response = requests.post(url, params=params)
        if response.status_code != 200:
            print(f"    [WARN] ClickHouse: {response.text[:100]}")
    except Exception as e:
        print(f"    [ERROR] Connection: {e}")

def write_to_clickhouse(df, table_name):
    """Write DataFrame to ClickHouse table"""
    url = f"jdbc:clickhouse://{CH_CONFIG['host']}:{CH_CONFIG['port']}/{CH_CONFIG['db']}"
    print(f"  -> Writing to: {table_name}")
    
    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", f"{CH_CONFIG['db']}.{table_name}") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("user", CH_CONFIG["user"]) \
        .option("password", CH_CONFIG["password"]) \
        .option("batchsize", 20000) \
        .mode("append") \
        .save()

def read_silver(spark, table_name):
    """Read a table from Silver layer"""
    return spark.read.format("delta").load(f"s3a://{SILVER_BUCKET}/{table_name}")

def read_gold(spark, table_name):
    """Read a table from Gold layer"""
    return spark.read.format("delta").load(f"s3a://{GOLD_BUCKET}/{table_name}")

# ============================================
# DIMENSION LOADERS
# ============================================

def load_dim_customers(spark):
    """Load customer dimension from Silver layer"""
    print("\n  [1/3] Loading dim_customers...")
    execute_ch_sql(f"TRUNCATE TABLE {CH_CONFIG['db']}.dim_customers")
    
    region_map = create_map([lit(x) for x in chain.from_iterable(STATE_TO_REGION.items())])
    
    customers = read_silver(spark, "customers") \
        .withColumn("customer_key", xxhash64(col("customer_id"))) \
        .withColumn("customer_region", coalesce(region_map[col("customer_state")], lit("Unknown"))) \
        .withColumn("customer_zip_code_prefix", coalesce(col("customer_zip_code_prefix"), lit(""))) \
        .select(
            "customer_key", "customer_id", "customer_unique_id",
            "customer_city", "customer_state", "customer_zip_code_prefix", "customer_region"
        ).distinct()
    
    write_to_clickhouse(customers, "dim_customers")

def load_dim_products(spark):
    """Load product dimension from Silver layer"""
    print("\n  [2/3] Loading dim_products...")
    execute_ch_sql(f"TRUNCATE TABLE {CH_CONFIG['db']}.dim_products")
    
    products = read_silver(spark, "products")
    
    # Try to join with product_categories if available
    try:
        categories = read_silver(spark, "product_categories")
        # Rename columns to match products table
        categories = categories.withColumnRenamed("category_name", "product_category_name") \
                               .withColumnRenamed("category_name_english", "product_category_name_english")
        products = products.join(categories, "product_category_name", "left")
    except:
        products = products.withColumn("product_category_name_english", col("product_category_name"))
    
    dim_products = products \
        .withColumn("product_key", xxhash64(col("product_id"))) \
        .withColumn("product_category_name", coalesce(col("product_category_name"), lit("unknown"))) \
        .withColumn("product_category_name_english", 
                    coalesce(col("product_category_name_english"), col("product_category_name"))) \
        .withColumn("product_weight_g", col("product_weight_g").cast("int")) \
        .withColumn("product_length_cm", col("product_length_cm").cast("int")) \
        .withColumn("product_height_cm", col("product_height_cm").cast("int")) \
        .withColumn("product_width_cm", col("product_width_cm").cast("int")) \
        .withColumn("product_photos_qty", col("product_photos_qty").cast("int")) \
        .withColumn("product_size_category",
                    when(col("product_weight_g") < 500, "Small")
                    .when(col("product_weight_g") < 5000, "Medium")
                    .otherwise("Large")) \
        .select(
            "product_key", "product_id", "product_category_name", "product_category_name_english",
            "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm",
            "product_photos_qty", "product_size_category"
        )
    
    write_to_clickhouse(dim_products, "dim_products")

def load_dim_sellers(spark):
    """Load seller dimension from Silver layer"""
    print("\n  [3/3] Loading dim_sellers...")
    execute_ch_sql(f"TRUNCATE TABLE {CH_CONFIG['db']}.dim_sellers")
    
    region_map = create_map([lit(x) for x in chain.from_iterable(STATE_TO_REGION.items())])
    
    sellers = read_silver(spark, "sellers") \
        .withColumn("seller_key", xxhash64(col("seller_id"))) \
        .withColumn("seller_region", coalesce(region_map[col("seller_state")], lit("Unknown"))) \
        .withColumn("seller_city", coalesce(col("seller_city"), lit(""))) \
        .withColumn("seller_state", coalesce(col("seller_state"), lit(""))) \
        .withColumn("seller_zip_code_prefix", coalesce(col("seller_zip_code_prefix"), lit(""))) \
        .select(
            "seller_key", "seller_id", "seller_city", "seller_state",
            "seller_zip_code_prefix", "seller_region"
        )
    
    write_to_clickhouse(sellers, "dim_sellers")

# ============================================
# FACT TABLE LOADERS
# ============================================

def load_fact_orders(spark):
    """Load fact orders from Gold/Silver layers"""
    print("\n  [1/3] Loading fact_orders...")
    execute_ch_sql(f"TRUNCATE TABLE {CH_CONFIG['db']}.fact_orders")
    
    orders_enriched = read_gold(spark, "orders_enriched")
    order_items = read_silver(spark, "order_items")
    
    # Payment type mapping
    payment_type_map = {"credit_card": 1, "boleto": 2, "voucher": 3, "debit_card": 4}
    payment_mapping = create_map([lit(x) for x in chain.from_iterable(payment_type_map.items())])
    
    # Order status mapping  
    status_map = {"created": 1, "approved": 2, "invoiced": 3, "processing": 4,
                  "shipped": 5, "delivered": 6, "unavailable": 7, "canceled": 8}
    status_mapping = create_map([lit(x) for x in chain.from_iterable(status_map.items())])
    
    # Gold layer already has TIMESTAMP type - no conversion needed
    fact_df = order_items.join(broadcast(orders_enriched), "order_id", "left")
    
    fact_orders = fact_df.select(
        # Keys
        xxhash64(col("order_id")).alias("order_key"),
        coalesce(date_format(col("order_purchase_timestamp"), "yyyyMMdd").cast("int"), lit(19700101)).alias("date_key"),
        xxhash64(col("customer_id")).alias("customer_key"),
        xxhash64(col("product_id")).alias("product_key"),
        xxhash64(col("seller_id")).alias("seller_key"),
        coalesce(payment_mapping[col("primary_payment_type")], lit(5)).alias("payment_type_key"),
        coalesce(status_mapping[col("order_status")], lit(1)).alias("status_key"),
        
        # Natural keys
        coalesce(col("order_id"), lit("")).alias("order_id"),
        coalesce(col("order_item_id").cast("int"), lit(1)).alias("order_item_id"),
        
        # Measures
        lit(1).alias("quantity"),
        coalesce(col("price").cast("decimal(10,2)"), lit(0)).alias("unit_price"),
        coalesce(col("freight_value").cast("decimal(10,2)"), lit(0)).alias("freight_value"),
        (coalesce(col("price"), lit(0)) + coalesce(col("freight_value"), lit(0))).cast("decimal(10,2)").alias("total_amount"),
        coalesce(col("total_payment").cast("decimal(10,2)"), lit(0)).alias("payment_value"),
        coalesce(col("max_installments").cast("int"), lit(1)).alias("payment_installments"),
        
        # Review
        coalesce(col("review_score").cast("int"), lit(0)).alias("review_score"),
        
        # Timestamps (already TIMESTAMP type from Gold layer)
        col("order_purchase_timestamp"),
        col("order_approved_at"),
        col("order_delivered_carrier_date"),
        col("order_delivered_customer_date"),
        col("order_estimated_delivery_date"),
        
        # Calculated measures
        coalesce(col("delivery_days").cast("int"), lit(0)).alias("delivery_days"),
        coalesce(datediff(col("order_estimated_delivery_date"), col("order_purchase_timestamp")), lit(0)).alias("estimated_delivery_days"),
        coalesce(col("delivery_delay_days").cast("int"), lit(0)).alias("delivery_delay_days"),
        
        # ETL metadata
        lit(f"batch_{current_date()}").alias("etl_batch_id")
    )
    
    write_to_clickhouse(fact_orders, "fact_orders")

def load_fact_payments(spark):
    """Load fact payments from Silver layer"""
    print("\n  [2/3] Loading fact_payments...")
    execute_ch_sql(f"TRUNCATE TABLE {CH_CONFIG['db']}.fact_payments")
    
    order_payments = read_silver(spark, "order_payments")
    orders = read_silver(spark, "orders")
    
    payment_type_map = {"credit_card": 1, "boleto": 2, "voucher": 3, "debit_card": 4}
    payment_mapping = create_map([lit(x) for x in chain.from_iterable(payment_type_map.items())])
    
    fact_payments = order_payments \
        .join(orders.select("order_id", "customer_id", "order_purchase_timestamp"), "order_id", "left") \
        .withColumn("payment_date", 
                    when(col("order_purchase_timestamp").isNotNull(),
                         from_unixtime(col("order_purchase_timestamp").cast("long") / 1000000))
                    .otherwise(lit("1970-01-01 00:00:00"))) \
        .select(
            xxhash64(col("order_id")).alias("order_key"),
            coalesce(col("payment_sequential").cast("int"), lit(1)).alias("payment_sequential"),
            coalesce(date_format(col("payment_date"), "yyyyMMdd").cast("int"), lit(19700101)).alias("date_key"),
            xxhash64(col("customer_id")).alias("customer_key"),
            coalesce(payment_mapping[col("payment_type")], lit(5)).alias("payment_type_key"),
            coalesce(col("order_id"), lit("")).alias("order_id"),
            coalesce(col("payment_value").cast("decimal(10,2)"), lit(0)).alias("payment_value"),
            coalesce(col("payment_installments").cast("int"), lit(1)).alias("payment_installments"),
            col("payment_date").alias("order_purchase_timestamp")
        )
    
    write_to_clickhouse(fact_payments, "fact_payments")

def load_fact_reviews(spark):
    """Load fact reviews from Silver layer"""
    print("\n  [3/3] Loading fact_reviews...")
    execute_ch_sql(f"TRUNCATE TABLE {CH_CONFIG['db']}.fact_reviews")
    
    order_reviews = read_silver(spark, "order_reviews")
    orders = read_silver(spark, "orders")
    order_items = read_silver(spark, "order_items")
    
    # Get first item per order for product/seller keys
    first_items = order_items \
        .withColumn("rn", row_number().over(Window.partitionBy("order_id").orderBy("order_item_id"))) \
        .filter(col("rn") == 1) \
        .select("order_id", "product_id", "seller_id")
    
    fact_reviews = order_reviews \
        .join(orders.select("order_id", "customer_id"), "order_id", "left") \
        .join(first_items, "order_id", "left") \
        .withColumn("review_date", 
                    when(col("review_creation_date").isNotNull(),
                         from_unixtime(col("review_creation_date").cast("long") / 1000000))
                    .otherwise(lit("1970-01-01 00:00:00"))) \
        .withColumn("answer_date",
                    when(col("review_answer_timestamp").isNotNull(),
                         from_unixtime(col("review_answer_timestamp").cast("long") / 1000000))
                    .otherwise(lit(None))) \
        .select(
            xxhash64(col("review_id")).alias("review_key"),
            xxhash64(col("order_id")).alias("order_key"),
            coalesce(date_format(col("review_date"), "yyyyMMdd").cast("int"), lit(19700101)).alias("date_key"),
            xxhash64(col("customer_id")).alias("customer_key"),
            xxhash64(col("product_id")).alias("product_key"),
            xxhash64(col("seller_id")).alias("seller_key"),
            coalesce(col("review_id"), lit("")).alias("review_id"),
            coalesce(col("order_id"), lit("")).alias("order_id"),
            coalesce(col("review_score").cast("int"), lit(0)).alias("review_score"),
            when(col("review_comment_message").isNotNull(), lit(1)).otherwise(lit(0)).alias("has_comment"),
            coalesce(length(col("review_comment_message")), lit(0)).alias("comment_length"),
            col("review_date").alias("review_creation_date"),
            col("answer_date").alias("review_answer_timestamp"),
            when(col("answer_date").isNotNull() & col("review_date").isNotNull(),
                 ((unix_timestamp(col("answer_date")) - unix_timestamp(col("review_date"))) / 3600).cast("float"))
            .otherwise(lit(0.0)).alias("response_time_hours")
        )
    
    write_to_clickhouse(fact_reviews, "fact_reviews")

# ============================================
# MAIN
# ============================================

def main():
    print("=" * 60)
    print("Starting Gold -> ClickHouse Data Warehouse Load")
    print("=" * 60)
    print(f"MinIO: {MINIO_PARAMS['endpoint']}")
    print(f"ClickHouse: {CH_CONFIG['host']}:{CH_CONFIG['port']}/{CH_CONFIG['db']}")
    print("=" * 60)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        # Phase 1: Dimensions
        print("\n=== PHASE 1: DIMENSIONS ===")
        load_dim_customers(spark)
        load_dim_products(spark)
        load_dim_sellers(spark)
        
        # Phase 2: Facts
        print("\n=== PHASE 2: FACTS ===")
        load_fact_orders(spark)
        load_fact_payments(spark)
        load_fact_reviews(spark)
        
        print("\n" + "=" * 60)
        print("[SUCCESS] Data Warehouse loaded successfully!")
        print("Tables loaded:")
        print("  Dimensions: dim_customers, dim_products, dim_sellers")
        print("  Facts: fact_orders, fact_payments, fact_reviews")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n[ERROR] ETL Failed: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()