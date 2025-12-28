"""
Spark Batch Job: Silver → Gold Layer
Creates business-ready aggregated datasets from cleaned Silver data.
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
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver")
GOLD_BUCKET = os.getenv("GOLD_BUCKET", "gold")

def create_spark_session():
    """Create Spark session with Delta Lake and S3 support"""
    builder = SparkSession.builder \
        .appName("EcommerceGoldLayer") \
        .config("spark.jars.packages", 
                "io.delta:delta-spark_2.12:3.0.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    
    return configure_spark_with_delta_pip(builder).getOrCreate()

def read_silver_table(spark, table_name):
    """Read a table from Silver layer"""
    silver_path = f"s3a://{SILVER_BUCKET}/{table_name}"
    return spark.read.format("delta").load(silver_path)

def create_orders_enriched(spark):
    """
    Create enriched orders dataset with all related information.
    This is the main Gold table for analytics.
    """
    print("\nCreating: orders_enriched")
    
    # Read Silver tables
    orders = read_silver_table(spark, "orders")
    order_items = read_silver_table(spark, "order_items")
    customers = read_silver_table(spark, "customers")
    products = read_silver_table(spark, "products")
    sellers = read_silver_table(spark, "sellers")
    payments = read_silver_table(spark, "order_payments")
    reviews = read_silver_table(spark, "order_reviews")
    
    # Cast timestamp strings to proper timestamps (they come as strings from JSON parsing)
    orders = orders \
        .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp").cast("long") / 1000000)) \
        .withColumn("order_approved_at", to_timestamp(col("order_approved_at").cast("long") / 1000000)) \
        .withColumn("order_delivered_carrier_date", to_timestamp(col("order_delivered_carrier_date").cast("long") / 1000000)) \
        .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date").cast("long") / 1000000)) \
        .withColumn("order_estimated_delivery_date", to_timestamp(col("order_estimated_delivery_date").cast("long") / 1000000))
    
    # Cast numeric columns in order_items
    order_items = order_items \
        .withColumn("price", col("price").cast("double")) \
        .withColumn("freight_value", col("freight_value").cast("double"))
    
    # Cast payment values
    payments = payments \
        .withColumn("payment_value", col("payment_value").cast("double")) \
        .withColumn("payment_installments", col("payment_installments").cast("int"))
    
    # Cast review score
    reviews = reviews \
        .withColumn("review_score", col("review_score").cast("int"))
    
    # Aggregate order items per order
    order_items_agg = order_items.groupBy("order_id").agg(
        count("order_item_id").alias("total_items"),
        sum("price").alias("total_price"),
        sum("freight_value").alias("total_freight"),
        countDistinct("product_id").alias("unique_products"),
        countDistinct("seller_id").alias("unique_sellers")
    )
    
    # Aggregate payments per order
    payments_agg = payments.groupBy("order_id").agg(
        sum("payment_value").alias("total_payment"),
        first("payment_type").alias("primary_payment_type"),
        max("payment_installments").alias("max_installments")
    )
    
    # Get first review per order (if multiple)
    reviews_first = reviews.dropDuplicates(["order_id"]).select(
        "order_id",
        col("review_score").alias("review_score"),
        col("review_creation_date")
    )
    
    # Join all tables
    enriched = orders \
        .join(customers, "customer_id", "left") \
        .join(order_items_agg, "order_id", "left") \
        .join(payments_agg, "order_id", "left") \
        .join(reviews_first, "order_id", "left") \
        .select(
            # Order info
            orders["order_id"],
            orders["order_status"],
            orders["order_purchase_timestamp"],
            orders["order_approved_at"],
            orders["order_delivered_carrier_date"],
            orders["order_delivered_customer_date"],
            orders["order_estimated_delivery_date"],
            
            # Customer info
            customers["customer_id"],
            customers["customer_unique_id"],
            customers["customer_city"],
            customers["customer_state"],
            
            # Order metrics
            order_items_agg["total_items"],
            order_items_agg["total_price"],
            order_items_agg["total_freight"],
            order_items_agg["unique_products"],
            order_items_agg["unique_sellers"],
            
            # Payment info
            payments_agg["total_payment"],
            payments_agg["primary_payment_type"],
            payments_agg["max_installments"],
            
            # Review info
            reviews_first["review_score"],
            
            # Derived fields
            (col("total_price") + col("total_freight")).alias("order_total"),
            datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp")).alias("delivery_days"),
            datediff(col("order_delivered_customer_date"), col("order_estimated_delivery_date")).alias("delivery_delay_days"),
            
            # Time dimensions
            year(col("order_purchase_timestamp")).alias("order_year"),
            month(col("order_purchase_timestamp")).alias("order_month"),
            dayofweek(col("order_purchase_timestamp")).alias("order_day_of_week"),
            hour(col("order_purchase_timestamp")).alias("order_hour"),
            
            # Metadata
            current_timestamp().alias("_gold_processed_at")
        )
    
    # Write to Gold
    gold_path = f"s3a://{GOLD_BUCKET}/orders_enriched"
    enriched.write.format("delta").mode("overwrite").save(gold_path)
    print(f"  [OK] Saved {enriched.count()} records to {gold_path}")
    
    return enriched

def create_daily_sales_summary(spark):
    """Create daily sales summary for dashboard"""
    print("\nCreating: daily_sales_summary")
    
    orders_enriched = spark.read.format("delta").load(f"s3a://{GOLD_BUCKET}/orders_enriched")
    
    daily_summary = orders_enriched \
        .filter(col("order_status") != "canceled") \
        .groupBy(
            date_format(col("order_purchase_timestamp"), "yyyy-MM-dd").alias("order_date")
        ).agg(
            count("order_id").alias("total_orders"),
            countDistinct("customer_id").alias("unique_customers"),
            sum("order_total").alias("total_revenue"),
            avg("order_total").alias("avg_order_value"),
            sum("total_items").alias("total_items_sold"),
            avg("review_score").alias("avg_review_score"),
            avg("delivery_days").alias("avg_delivery_days")
        ).orderBy("order_date")
    
    gold_path = f"s3a://{GOLD_BUCKET}/daily_sales_summary"
    daily_summary.write.format("delta").mode("overwrite").save(gold_path)
    print(f"  [OK] Saved {daily_summary.count()} records")
    
    return daily_summary

def create_customer_segments(spark):
    """Create RFM-based customer segmentation"""
    print("\nCreating: customer_segments")
    
    orders_enriched = spark.read.format("delta").load(f"s3a://{GOLD_BUCKET}/orders_enriched")
    
    # Calculate RFM metrics
    max_date = orders_enriched.agg(max("order_purchase_timestamp")).collect()[0][0]
    
    rfm = orders_enriched \
        .filter(col("order_status") == "delivered") \
        .groupBy("customer_unique_id") \
        .agg(
            datediff(lit(max_date), max("order_purchase_timestamp")).alias("recency_days"),
            count("order_id").alias("frequency"),
            sum("order_total").alias("monetary"),
            min("order_purchase_timestamp").alias("first_order_date"),
            max("order_purchase_timestamp").alias("last_order_date"),
            avg("review_score").alias("avg_review_score")
        )
    
    # Assign segments based on RFM
    customer_segments = rfm.withColumn(
        "segment",
        when((col("recency_days") < 90) & (col("frequency") >= 3) & (col("monetary") >= 500), "VIP")
        .when((col("recency_days") < 180) & (col("frequency") >= 2), "Regular")
        .when((col("recency_days") < 365), "At Risk")
        .otherwise("Lost")
    )
    
    gold_path = f"s3a://{GOLD_BUCKET}/customer_segments"
    customer_segments.write.format("delta").mode("overwrite").save(gold_path)
    print(f"  [OK] Saved {customer_segments.count()} records")
    
    return customer_segments

def create_product_performance(spark):
    """Create product performance metrics"""
    print("\nCreating: product_performance")
    
    order_items = read_silver_table(spark, "order_items")
    products = read_silver_table(spark, "products")
    reviews = read_silver_table(spark, "order_reviews")
    
    # Join order items with reviews
    items_with_reviews = order_items \
        .join(reviews.select("order_id", "review_score"), "order_id", "left")
    
    product_perf = items_with_reviews \
        .groupBy("product_id") \
        .agg(
            count("order_item_id").alias("times_sold"),
            countDistinct("order_id").alias("order_count"),
            sum("price").alias("total_revenue"),
            avg("price").alias("avg_price"),
            avg("review_score").alias("avg_review_score"),
            sum("freight_value").alias("total_freight")
        )
    
    # Join with product info
    product_perf_enriched = product_perf \
        .join(products.select("product_id", "product_category_name"), "product_id", "left") \
        .withColumn("_gold_processed_at", current_timestamp())
    
    gold_path = f"s3a://{GOLD_BUCKET}/product_performance"
    product_perf_enriched.write.format("delta").mode("overwrite").save(gold_path)
    print(f"  [OK] Saved {product_perf_enriched.count()} records")
    
    return product_perf_enriched

def main():
    print("=" * 60)
    print("Starting Silver -> Gold ETL Job")
    print("=" * 60)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Create Gold layer tables
        create_orders_enriched(spark)
        create_daily_sales_summary(spark)
        create_customer_segments(spark)
        create_product_performance(spark)
        
        print("\n" + "=" * 60)
        print("Gold layer created successfully!")
        print("Available tables:")
        print("  - orders_enriched")
        print("  - daily_sales_summary")
        print("  - customer_segments")
        print("  - product_performance")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n[FAIL] Error: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
