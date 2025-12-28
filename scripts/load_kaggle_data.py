"""
Load Kaggle Olist E-commerce Dataset into PostgreSQL
Downloads and processes the Brazilian E-commerce dataset from Kaggle.

Prerequisites:
1. Download dataset from: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
2. Extract to data/ folder
"""

import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# Configuration
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5433")
PG_DB = os.getenv("PG_DB", "ecommerce_db")
PG_USER = os.getenv("PG_USER", "ecommerce")
PG_PASSWORD = os.getenv("PG_PASSWORD", "ecommerce123")

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

# File mappings: Kaggle filename -> (table_name, column_mapping)
FILE_MAPPINGS = {
    "product_category_name_translation.csv": {
        "table": "product_categories",
        "columns": {
            "product_category_name": "category_name",
            "product_category_name_english": "category_name_english"
        }
    },
    "olist_customers_dataset.csv": {
        "table": "customers",
        "columns": {
            "customer_id": "customer_id",
            "customer_unique_id": "customer_unique_id",
            "customer_zip_code_prefix": "customer_zip_code_prefix",
            "customer_city": "customer_city",
            "customer_state": "customer_state"
        }
    },
    "olist_sellers_dataset.csv": {
        "table": "sellers",
        "columns": {
            "seller_id": "seller_id",
            "seller_zip_code_prefix": "seller_zip_code_prefix",
            "seller_city": "seller_city",
            "seller_state": "seller_state"
        }
    },
    "olist_products_dataset.csv": {
        "table": "products",
        "columns": {
            "product_id": "product_id",
            "product_category_name": "product_category_name",
            "product_name_lenght": "product_name_length",
            "product_description_lenght": "product_description_length",
            "product_photos_qty": "product_photos_qty",
            "product_weight_g": "product_weight_g",
            "product_length_cm": "product_length_cm",
            "product_height_cm": "product_height_cm",
            "product_width_cm": "product_width_cm"
        }
    },
    "olist_orders_dataset.csv": {
        "table": "orders",
        "columns": {
            "order_id": "order_id",
            "customer_id": "customer_id",
            "order_status": "order_status",
            "order_purchase_timestamp": "order_purchase_timestamp",
            "order_approved_at": "order_approved_at",
            "order_delivered_carrier_date": "order_delivered_carrier_date",
            "order_delivered_customer_date": "order_delivered_customer_date",
            "order_estimated_delivery_date": "order_estimated_delivery_date"
        }
    },
    "olist_order_items_dataset.csv": {
        "table": "order_items",
        "columns": {
            "order_id": "order_id",
            "order_item_id": "order_item_id",
            "product_id": "product_id",
            "seller_id": "seller_id",
            "shipping_limit_date": "shipping_limit_date",
            "price": "price",
            "freight_value": "freight_value"
        }
    },
    "olist_order_payments_dataset.csv": {
        "table": "order_payments",
        "columns": {
            "order_id": "order_id",
            "payment_sequential": "payment_sequential",
            "payment_type": "payment_type",
            "payment_installments": "payment_installments",
            "payment_value": "payment_value"
        }
    },
    "olist_order_reviews_dataset.csv": {
        "table": "order_reviews",
        "columns": {
            "review_id": "review_id",
            "order_id": "order_id",
            "review_score": "review_score",
            "review_comment_title": "review_comment_title",
            "review_comment_message": "review_comment_message",
            "review_creation_date": "review_creation_date",
            "review_answer_timestamp": "review_answer_timestamp"
        }
    }
}

def get_connection():
    """Create PostgreSQL connection"""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )

def check_data_files():
    """Check if Kaggle data files exist"""
    missing_files = []
    for filename in FILE_MAPPINGS.keys():
        filepath = os.path.join(DATA_DIR, filename)
        if not os.path.exists(filepath):
            missing_files.append(filename)
    
    if missing_files:
        print("=" * 60)
        print("MISSING DATA FILES")
        print("=" * 60)
        print("\nPlease download the Olist dataset from Kaggle:")
        print("https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce")
        print(f"\nExtract the files to: {DATA_DIR}")
        print("\nMissing files:")
        for f in missing_files:
            print(f"  - {f}")
        print("=" * 60)
        return False
    
    return True

def load_csv_to_table(conn, filename, config, batch_size=5000):
    """Load a CSV file into PostgreSQL table"""
    filepath = os.path.join(DATA_DIR, filename)
    table = config["table"]
    columns = config["columns"]
    
    print(f"\nLoading: {filename} → {table}")
    
    # Read CSV
    df = pd.read_csv(filepath, low_memory=False)
    
    # Rename columns
    df = df.rename(columns=columns)
    
    # Keep only mapped columns
    target_cols = list(columns.values())
    df = df[[c for c in target_cols if c in df.columns]]
    
    # Handle NaN values
    df = df.where(pd.notnull(df), None)
    
    # Special handling for products table - convert float columns to int
    if table == "products":
        numeric_cols = ['product_name_length', 'product_description_length', 'product_photos_qty',
                       'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']
        for col in numeric_cols:
            if col in df.columns:
                # Convert to numeric, coerce errors to NaN, then fill NaN with None
                df[col] = pd.to_numeric(df[col], errors='coerce')
                # Replace inf and very large values
                df[col] = df[col].replace([float('inf'), float('-inf')], None)
                # Convert to nullable integer (will become None where NaN)
                df[col] = df[col].apply(lambda x: int(x) if pd.notnull(x) and abs(x) < 2147483647 else None)
    
    # Convert dates and handle NaT values
    date_columns = [c for c in df.columns if 'date' in c.lower() or 'timestamp' in c.lower()]
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            # Replace NaT with None for PostgreSQL compatibility
            df[col] = df[col].apply(lambda x: x if pd.notnull(x) else None)
    
    print(f"  Records to load: {len(df)}")
    
    # Insert in batches
    cur = conn.cursor()
    
    # Build INSERT statement
    cols_str = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    
    insert_sql = f"""
        INSERT INTO {table} ({cols_str})
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING
    """
    
    # Convert DataFrame to list of tuples with proper None handling
    def convert_value(val):
        """Convert pandas values to Python native types"""
        if pd.isna(val):
            return None
        if isinstance(val, pd.Timestamp):
            return val.to_pydatetime() if not pd.isna(val) else None
        return val
    
    records = [tuple(convert_value(v) for v in row) for row in df.values]
    
    # Insert in batches
    total_inserted = 0
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            cur.executemany(insert_sql, batch)
            conn.commit()
            total_inserted += len(batch)
            print(f"  Progress: {total_inserted}/{len(records)}", end="\r")
        except Exception as e:
            conn.rollback()
            print(f"\n  Error in batch {i}: {e}")
            # Try inserting one by one to find problematic records
            for record in batch:
                try:
                    cur.execute(insert_sql, record)
                    conn.commit()
                    total_inserted += 1
                except Exception as e2:
                    conn.rollback()
                    # Skip problematic record
                    continue
    
    cur.close()
    print(f"  ✓ Loaded {total_inserted} records")
    
    return total_inserted

def main():
    print("=" * 60)
    print("Loading Kaggle Olist E-commerce Dataset")
    print("=" * 60)
    print(f"PostgreSQL: {PG_HOST}:{PG_PORT}/{PG_DB}")
    print(f"Data directory: {DATA_DIR}")
    print("=" * 60)
    
    # Check data files exist
    if not check_data_files():
        sys.exit(1)
    
    # Connect to PostgreSQL
    try:
        conn = get_connection()
        print("\n✓ Connected to PostgreSQL")
    except Exception as e:
        print(f"\n✗ Failed to connect to PostgreSQL: {e}")
        print("\nMake sure Docker containers are running:")
        print("  docker-compose up -d postgres")
        sys.exit(1)
    
    # Load order matters due to foreign keys
    load_order = [
        "product_category_name_translation.csv",
        "olist_customers_dataset.csv",
        "olist_sellers_dataset.csv",
        "olist_products_dataset.csv",
        "olist_orders_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_order_payments_dataset.csv",
        "olist_order_reviews_dataset.csv"
    ]
    
    total_records = 0
    start_time = datetime.now()
    
    for filename in load_order:
        if filename in FILE_MAPPINGS:
            try:
                count = load_csv_to_table(conn, filename, FILE_MAPPINGS[filename])
                total_records += count
            except Exception as e:
                print(f"\n✗ Error loading {filename}: {e}")
    
    conn.close()
    
    elapsed = (datetime.now() - start_time).total_seconds()
    
    print("\n" + "=" * 60)
    print("LOAD COMPLETE")
    print("=" * 60)
    print(f"Total records loaded: {total_records:,}")
    print(f"Time elapsed: {elapsed:.1f} seconds")
    print("=" * 60)

if __name__ == "__main__":
    main()
