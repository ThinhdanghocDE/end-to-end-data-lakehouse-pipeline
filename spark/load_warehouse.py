"""
Spark Batch Job: Gold Layer → ClickHouse Data Warehouse
Loads curated data from Gold layer into ClickHouse Star Schema.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *

# Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9002")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
GOLD_BUCKET = os.getenv("GOLD_BUCKET", "gold")
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "8124")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "ecommerce_dw")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

def create_spark_session():
    """Create Spark session with Delta Lake, S3, and ClickHouse support"""
    builder = SparkSession.builder \
        .appName("EcommerceLoadWarehouse") \
        .config("spark.jars.packages", 
                "io.delta:delta-spark_2.12:3.0.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.clickhouse:clickhouse-jdbc:0.4.6,"
                "org.apache.httpcomponents.client5:httpclient5:5.2.1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    
    return configure_spark_with_delta_pip(builder).getOrCreate()

def get_clickhouse_url():
    """Get ClickHouse JDBC URL"""
    return f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}"

def get_clickhouse_properties():
    """Get ClickHouse connection properties"""
    return {
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "user": CLICKHOUSE_USER,
        "password": CLICKHOUSE_PASSWORD
    }

def write_to_clickhouse(df, table_name, mode="append"):
    """Write DataFrame to ClickHouse table"""
    print(f"  Writing to ClickHouse: {table_name}")
    
    df.write \
        .format("jdbc") \
        .option("url", get_clickhouse_url()) \
        .option("dbtable", table_name) \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("user", CLICKHOUSE_USER) \
        .option("password", CLICKHOUSE_PASSWORD) \
        .option("batchsize", 10000) \
        .mode(mode) \
        .save()
    
    print(f"  [OK] Wrote {df.count()} records to {table_name}")

def load_dim_customers(spark):
    """Load customer dimension from Silver layer"""
    print("\nLoading: dim_customers")
    
    customers = spark.read.format("delta").load(f"s3a://{SILVER_BUCKET}/customers")
    
    # Map Brazilian states to regions
    state_to_region = {
        "AC": "Norte", "AP": "Norte", "AM": "Norte", "PA": "Norte", "RO": "Norte", "RR": "Norte", "TO": "Norte",
        "AL": "Nordeste", "BA": "Nordeste", "CE": "Nordeste", "MA": "Nordeste", "PB": "Nordeste", 
        "PE": "Nordeste", "PI": "Nordeste", "RN": "Nordeste", "SE": "Nordeste",
        "DF": "Centro-Oeste", "GO": "Centro-Oeste", "MT": "Centro-Oeste", "MS": "Centro-Oeste",
        "ES": "Sudeste", "MG": "Sudeste", "RJ": "Sudeste", "SP": "Sudeste",
        "PR": "Sul", "RS": "Sul", "SC": "Sul"
    }
    
    region_mapping = create_map([lit(x) for x in sum(state_to_region.items(), ())])
    
    dim_customers = customers \
        .withColumn("customer_key", hash(col("customer_id")).cast("long").bitwiseAND(0x7FFFFFFFFFFFFFFF)) \
        .withColumn("customer_region", coalesce(region_mapping[col("customer_state")], lit("Unknown"))) \
        .select(
            "customer_key",
            "customer_id",
            "customer_unique_id",
            "customer_city",
            "customer_state",
            "customer_zip_code_prefix",
            "customer_region",
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at")
        )
    
    write_to_clickhouse(dim_customers, "dim_customers", "overwrite")
    return dim_customers

def load_dim_products(spark):
    """Load product dimension from Silver layer"""
    print("\nLoading: dim_products")
    
    products = spark.read.format("delta").load(f"s3a://{SILVER_BUCKET}/products")
    
    dim_products = products \
        .withColumn("product_key", hash(col("product_id")).cast("long").bitwiseAND(0x7FFFFFFFFFFFFFFF)) \
        .withColumn("product_category_name_english", 
                    coalesce(col("product_category_name"), lit("unknown"))) \
        .withColumn("product_size_category",
                    when(col("product_weight_g") < 500, "Small")
                    .when(col("product_weight_g") < 5000, "Medium")
                    .otherwise("Large")) \
        .select(
            "product_key",
            "product_id",
            "product_category_name",
            "product_category_name_english",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
            "product_photos_qty",
            "product_size_category",
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at")
        )
    
    write_to_clickhouse(dim_products, "dim_products", "overwrite")
    return dim_products

def load_dim_sellers(spark):
    """Load seller dimension from Silver layer"""
    print("\nLoading: dim_sellers")
    
    sellers = spark.read.format("delta").load(f"s3a://{SILVER_BUCKET}/sellers")
    
    state_to_region = {
        "AC": "Norte", "AP": "Norte", "AM": "Norte", "PA": "Norte", "RO": "Norte", "RR": "Norte", "TO": "Norte",
        "AL": "Nordeste", "BA": "Nordeste", "CE": "Nordeste", "MA": "Nordeste", "PB": "Nordeste", 
        "PE": "Nordeste", "PI": "Nordeste", "RN": "Nordeste", "SE": "Nordeste",
        "DF": "Centro-Oeste", "GO": "Centro-Oeste", "MT": "Centro-Oeste", "MS": "Centro-Oeste",
        "ES": "Sudeste", "MG": "Sudeste", "RJ": "Sudeste", "SP": "Sudeste",
        "PR": "Sul", "RS": "Sul", "SC": "Sul"
    }
    
    region_mapping = create_map([lit(x) for x in sum(state_to_region.items(), ())])
    
    dim_sellers = sellers \
        .withColumn("seller_key", hash(col("seller_id")).cast("long").bitwiseAND(0x7FFFFFFFFFFFFFFF)) \
        .withColumn("seller_region", coalesce(region_mapping[col("seller_state")], lit("Unknown"))) \
        .select(
            "seller_key",
            "seller_id",
            "seller_city",
            "seller_state",
            "seller_zip_code_prefix",
            "seller_region",
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at")
        )
    
    write_to_clickhouse(dim_sellers, "dim_sellers", "overwrite")
    return dim_sellers

def load_fact_orders(spark):
    """Load fact orders from Gold layer"""
    print("\nLoading: fact_orders")
    
    orders_enriched = spark.read.format("delta").load(f"s3a://{GOLD_BUCKET}/orders_enriched")
    order_items = spark.read.format("delta").load(f"s3a://{SILVER_BUCKET}/order_items")
    
    # Join to get item-level granularity
    fact_base = order_items \
        .join(orders_enriched, "order_id", "left")
    
    # Payment type mapping
    payment_type_map = {
        "credit_card": 1, "boleto": 2, "voucher": 3, "debit_card": 4
    }
    payment_mapping = create_map([lit(x) for x in sum(payment_type_map.items(), ())])
    
    # Order status mapping
    status_map = {
        "created": 1, "approved": 2, "invoiced": 3, "processing": 4,
        "shipped": 5, "delivered": 6, "unavailable": 7, "canceled": 8
    }
    status_mapping = create_map([lit(x) for x in sum(status_map.items(), ())])
    
    fact_orders = fact_base \
        .withColumn("order_key", hash(col("order_id")).cast("long").bitwiseAND(0x7FFFFFFFFFFFFFFF)) \
        .withColumn("date_key", date_format(col("order_purchase_timestamp"), "yyyyMMdd").cast("int")) \
        .withColumn("customer_key", hash(col("customer_id")).cast("long").bitwiseAND(0x7FFFFFFFFFFFFFFF)) \
        .withColumn("product_key", hash(col("product_id")).cast("long").bitwiseAND(0x7FFFFFFFFFFFFFFF)) \
        .withColumn("seller_key", hash(col("seller_id")).cast("long").bitwiseAND(0x7FFFFFFFFFFFFFFF)) \
        .withColumn("payment_type_key", coalesce(payment_mapping[col("primary_payment_type")], lit(5))) \
        .withColumn("status_key", coalesce(status_mapping[col("order_status")], lit(1))) \
        .withColumn("quantity", lit(1)) \
        .withColumn("unit_price", col("price")) \
        .withColumn("total_amount", col("price") + col("freight_value")) \
        .withColumn("payment_value", col("total_payment")) \
        .withColumn("payment_installments", coalesce(col("max_installments"), lit(1))) \
        .withColumn("delivery_days", col("delivery_days")) \
        .withColumn("estimated_delivery_days", 
                    datediff(col("order_estimated_delivery_date"), col("order_purchase_timestamp"))) \
        .withColumn("delivery_delay_days", col("delivery_delay_days")) \
        .withColumn("etl_batch_id", lit(f"batch_{current_date()}")) \
        .select(
            "order_key", "date_key", "customer_key", "product_key", "seller_key",
            "payment_type_key", "status_key", "order_id", "order_item_id",
            "quantity", "unit_price", "freight_value", "total_amount",
            "payment_value", "payment_installments", "review_score",
            "order_purchase_timestamp", "order_approved_at",
            "order_delivered_carrier_date", "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "delivery_days", "estimated_delivery_days", "delivery_delay_days",
            "etl_batch_id"
        )
    
    write_to_clickhouse(fact_orders, "fact_orders")
    return fact_orders

def main():
    print("=" * 60)
    print("Starting Gold -> ClickHouse Data Warehouse Load")
    print("=" * 60)
    print(f"MinIO: {MINIO_ENDPOINT}")
    print(f"ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}")
    print("=" * 60)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Load dimension tables first
        load_dim_customers(spark)
        load_dim_products(spark)
        load_dim_sellers(spark)
        
        # Load fact table
        load_fact_orders(spark)
        
        print("\n" + "=" * 60)
        print("Data Warehouse loaded successfully!")
        print("Tables loaded:")
        print("  - dim_customers")
        print("  - dim_products")
        print("  - dim_sellers")
        print("  - fact_orders")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n[FAIL] Error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
