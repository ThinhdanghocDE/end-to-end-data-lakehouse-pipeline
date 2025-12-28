"""
Spark Batch Job: Bronze → Silver Layer
Reads raw CDC data from Bronze layer, cleans and deduplicates, writes to Silver layer.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta import *

# Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9002")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver")

TABLES = ["customers", "sellers", "products", "orders", "order_items", "order_payments", "order_reviews"]

def create_spark_session():
    """Create Spark session with Delta Lake and S3 support"""
    builder = SparkSession.builder \
        .appName("EcommerceSilverLayer") \
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

def get_primary_key(table_name):
    """Get primary key column(s) for each table"""
    pk_mapping = {
        "customers": ["customer_id"],
        "sellers": ["seller_id"],
        "products": ["product_id"],
        "orders": ["order_id"],
        "order_items": ["order_id", "order_item_id"],
        "order_payments": ["order_id", "payment_sequential"],
        "order_reviews": ["review_id"]
    }
    return pk_mapping.get(table_name, ["id"])

def clean_and_dedupe(df, table_name):
    """Clean and deduplicate data"""
    pk_cols = get_primary_key(table_name)
    
    # Parse the raw JSON value
    json_df = df.withColumn("parsed", from_json(col("_raw_value"), MapType(StringType(), StringType())))
    
    # Flatten the parsed JSON
    for field in ["__op", "__source_ts_ms", "__deleted"]:
        json_df = json_df.withColumn(field, col(f"parsed.{field}"))
    
    # Remove CDC metadata fields and keep business data
    business_cols = [c for c in json_df.columns if not c.startswith("_") and c != "parsed"]
    
    # Deduplicate: keep latest version based on CDC timestamp
    window = Window.partitionBy(*pk_cols).orderBy(col("_kafka_timestamp").desc())
    
    deduped_df = json_df \
        .withColumn("_row_num", row_number().over(window)) \
        .filter(col("_row_num") == 1) \
        .drop("_row_num")
    
    # Filter out deleted records (or mark them)
    # For SCD Type 1, we simply exclude deleted records
    # For SCD Type 2, we would keep them with a flag
    final_df = deduped_df.filter(
        (col("__deleted").isNull()) | (col("__deleted") != "true")
    )
    
    # Add Silver layer metadata
    final_df = final_df \
        .withColumn("_silver_processed_at", current_timestamp()) \
        .withColumn("_silver_batch_id", lit(f"batch_{current_date()}"))
    
    return final_df

def process_table(spark, table_name):
    """Process a single table from Bronze to Silver"""
    bronze_path = f"s3a://{BRONZE_BUCKET}/{table_name}"
    silver_path = f"s3a://{SILVER_BUCKET}/{table_name}"
    
    print(f"\nProcessing table: {table_name}")
    print(f"  Bronze: {bronze_path}")
    print(f"  Silver: {silver_path}")
    
    try:
        # Read from Bronze
        bronze_df = spark.read.format("delta").load(bronze_path)
        print(f"  Bronze records: {bronze_df.count()}")
        
        # Clean and deduplicate
        silver_df = clean_and_dedupe(bronze_df, table_name)
        print(f"  Silver records (after dedup): {silver_df.count()}")
        
        # Write to Silver (merge/upsert for incremental processing)
        pk_cols = get_primary_key(table_name)
        merge_condition = " AND ".join([f"target.{c} = source.{c}" for c in pk_cols])
        
        # Check if Silver table exists
        try:
            silver_table = DeltaTable.forPath(spark, silver_path)
            
            # Merge (upsert)
            silver_table.alias("target") \
                .merge(silver_df.alias("source"), merge_condition) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
            
            print(f"  [OK] Merged into existing Silver table")
        except:
            # First time: create new Delta table
            silver_df.write.format("delta").mode("overwrite").save(silver_path)
            print(f"  [OK] Created new Silver table")
        
        return True
        
    except Exception as e:
        print(f"  [FAIL] Error processing {table_name}: {e}")
        return False

def main():
    print("=" * 60)
    print("Starting Bronze -> Silver ETL Job")
    print("=" * 60)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    success_count = 0
    for table in TABLES:
        if process_table(spark, table):
            success_count += 1
    
    print("\n" + "=" * 60)
    print(f"Completed: {success_count}/{len(TABLES)} tables processed")
    print("=" * 60)
    
    spark.stop()

if __name__ == "__main__":
    main()
