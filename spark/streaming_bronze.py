"""
Spark Streaming Job: Kafka CDC → Bronze Layer (MinIO)
Reads CDC events from Debezium/Kafka and writes raw data to MinIO in Delta format.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9002")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")

# Kafka topics to consume (Debezium CDC topics)
CDC_TOPICS = [
    "ecommerce.public.customers",
    "ecommerce.public.sellers", 
    "ecommerce.public.products",
    "ecommerce.public.product_categories",  # Added: needed for product category English names
    "ecommerce.public.orders",
    "ecommerce.public.order_items",
    "ecommerce.public.order_payments",
    "ecommerce.public.order_reviews"
]

CHECKPOINT_DIR = "checkpoints/bronze"

def create_spark_session():
    """Create Spark session with Delta Lake and S3 support"""
    builder = SparkSession.builder \
        .appName("EcommerceBronzeLayer") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
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

def get_cdc_schema():
    """Schema for Debezium CDC events after ExtractNewRecordState transform"""
    return StructType([
        StructField("payload", StructType([
            # Common CDC metadata
            StructField("__op", StringType(), True),
            StructField("__source_ts_ms", LongType(), True),
            StructField("__table", StringType(), True),
            StructField("__deleted", StringType(), True),
        ]), True)
    ])

def process_cdc_stream(spark, topic):
    """Process CDC stream for a single topic"""
    table_name = topic.split(".")[-1]  # Extract table name from topic
    
    print(f"Starting stream for topic: {topic} (table: {table_name})")
    
    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON value and add metadata
    parsed_df = df.select(
        col("key").cast("string").alias("_key"),
        col("value").cast("string").alias("_raw_value"),
        col("topic").alias("_topic"),
        col("partition").alias("_partition"),
        col("offset").alias("_offset"),
        col("timestamp").alias("_kafka_timestamp"),
        current_timestamp().alias("_ingested_at"),
        lit(table_name).alias("_source_table")
    )
    
    # Write to Bronze layer in MinIO (Delta format)
    bronze_path = f"s3a://{BRONZE_BUCKET}/{table_name}"
    checkpoint_path = f"{CHECKPOINT_DIR}/{table_name}"
    
    def write_batch(batch_df, batch_id):
        count = batch_df.count()
        if count > 0:
            batch_df.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .partitionBy("_source_table") \
                .save(bronze_path)
            print(f"[{table_name}] Batch {batch_id}: {count} records written")
    
    query = parsed_df.writeStream \
        .foreachBatch(write_batch) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime="5 seconds") \
        .start()
    
    print(f"  -> Writing to: {bronze_path}")
    
    return query

def main():
    print("=" * 60)
    print("Starting Bronze Layer Streaming Job")
    print("=" * 60)
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"MinIO: {MINIO_ENDPOINT}")
    print(f"Bronze Bucket: {BRONZE_BUCKET}")
    print(f"Topics: {CDC_TOPICS}")
    print("=" * 60)
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Start streams for all CDC topics
    queries = []
    for topic in CDC_TOPICS:
        try:
            query = process_cdc_stream(spark, topic)
            queries.append(query)
            print(f"[OK] Stream started for {topic}")
        except Exception as e:
            print(f"[FAIL] Failed to start stream for {topic}: {e}")
    
    if queries:
        print(f"\n{len(queries)} streams running. Waiting for termination...")
        # Wait for all queries
        for query in queries:
            query.awaitTermination()
    else:
        print("No streams started. Exiting.")

if __name__ == "__main__":
    main()
