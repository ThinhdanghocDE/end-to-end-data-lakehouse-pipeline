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
    """Clean and deduplicate data from Debezium CDC format"""
    pk_cols = get_primary_key(table_name)
    
    # Parse the raw JSON value (Debezium format has: before, after, source, op, etc.)
    # First, parse as generic JSON to get the structure
    parsed_df = df.withColumn("_parsed_json", from_json(col("_raw_value"), MapType(StringType(), StringType())))
    
    # Extract the 'after' field which contains the actual row data
    # Also extract 'op' for operation type and 'source' for metadata
    parsed_df = parsed_df.withColumn("_after_json", col("_parsed_json.after"))
    parsed_df = parsed_df.withColumn("_op", col("_parsed_json.op"))
    parsed_df = parsed_df.withColumn("_deleted", 
        when(col("_parsed_json.__deleted") == "true", lit(True))
        .when(col("_op") == "d", lit(True))
        .otherwise(lit(False))
    )
    
    # Parse the 'after' JSON string to get actual column values
    parsed_df = parsed_df.withColumn("_data", 
        from_json(col("_after_json"), MapType(StringType(), StringType()))
    )
    
    # If 'after' is null (for delete operations), try to use 'before'
    parsed_df = parsed_df.withColumn("_data",
        when(col("_data").isNull(), 
             from_json(col("_parsed_json.before"), MapType(StringType(), StringType()))
        ).otherwise(col("_data"))
    )
    
    # Get all keys from the data map and create columns for each
    # We need to explode the map to get column names dynamically
    # For now, extract known columns based on table
    column_mappings = {
        "customers": ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"],
        "sellers": ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
        "products": ["product_id", "product_category_name", "product_name_length", "product_description_length", 
                     "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"],
        "orders": ["order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at",
                   "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"],
        "order_items": ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"],
        "order_payments": ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"],
        "order_reviews": ["review_id", "order_id", "review_score", "review_comment_title", "review_comment_message",
                          "review_creation_date", "review_answer_timestamp"]
    }
    
    # Extract each column from the data map
    cols_to_extract = column_mappings.get(table_name, [])
    for col_name in cols_to_extract:
        parsed_df = parsed_df.withColumn(col_name, col(f"_data.{col_name}"))
    
    # Deduplicate: keep latest version based on Kafka timestamp
    window = Window.partitionBy(*pk_cols).orderBy(col("_kafka_timestamp").desc())
    
    deduped_df = parsed_df \
        .withColumn("_row_num", row_number().over(window)) \
        .filter(col("_row_num") == 1) \
        .drop("_row_num")
    
    # Filter out deleted records
    final_df = deduped_df.filter(col("_deleted") == False)
    
    # Select only business columns plus metadata
    select_cols = cols_to_extract + ["_kafka_timestamp", "_op"]
    final_df = final_df.select(*select_cols)
    
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
