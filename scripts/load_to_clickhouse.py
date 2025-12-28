"""
Direct ETL: PostgreSQL → ClickHouse (Fixed Version)
Loads data from PostgreSQL OLTP to ClickHouse Data Warehouse without Spark.
"""

import psycopg2
from clickhouse_driver import Client
import os
import math
from datetime import datetime

# PostgreSQL connection (Docker mapped port)
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5433")  # Docker mapped from 5432
PG_DB = os.getenv("POSTGRES_DB", "ecommerce_db")
PG_USER = os.getenv("POSTGRES_USER", "ecommerce")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "ecommerce123")

# ClickHouse connection (Docker mapped port)
CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "9010"))  # Docker mapped from 9000
CH_DB = os.getenv("CLICKHOUSE_DB", "ecommerce_dw")

def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )

def get_ch_client():
    return Client(host=CH_HOST, port=CH_PORT, database=CH_DB)

def load_dim_customers():
    """Load customers to dim_customers"""
    print("Loading dim_customers...")
    
    pg_conn = get_pg_connection()
    pg_cursor = pg_conn.cursor()
    
    pg_cursor.execute("""
        SELECT 
            customer_id,
            customer_unique_id,
            customer_city,
            customer_state,
            customer_zip_code_prefix
        FROM customers
    """)
    rows = pg_cursor.fetchall()
    
    ch_client = get_ch_client()
    ch_client.execute("TRUNCATE TABLE dim_customers")
    
    # Insert in batches with auto-increment customer_key
    batch_size = 10000
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        # Add customer_key (i + row index) and customer_region placeholder
        batch_with_keys = [
            (i + j + 1, row[0], row[1], row[2] or '', row[3] or '', row[4] or '', '')
            for j, row in enumerate(batch)
        ]
        ch_client.execute(
            """INSERT INTO dim_customers 
               (customer_key, customer_id, customer_unique_id, customer_city, 
                customer_state, customer_zip_code_prefix, customer_region) VALUES""",
            batch_with_keys
        )
    
    print(f"  Loaded {len(rows)} customers")
    pg_cursor.close()
    pg_conn.close()

def load_dim_products():
    """Load products to dim_products"""
    print("Loading dim_products...")
    
    pg_conn = get_pg_connection()
    pg_cursor = pg_conn.cursor()
    
    pg_cursor.execute("""
        SELECT 
            product_id,
            COALESCE(product_category_name, ''),
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm,
            product_photos_qty
        FROM products
    """)
    rows = pg_cursor.fetchall()
    
    ch_client = get_ch_client()
    ch_client.execute("TRUNCATE TABLE dim_products")
    
    batch_size = 10000
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        batch_with_keys = []
        for j, row in enumerate(batch):
            # Helper to convert numeric, handling None and NaN
            def safe_int(val):
                if val is None:
                    return None
                try:
                    if math.isnan(float(val)):
                        return None
                    return int(val)
                except (ValueError, TypeError):
                    return None
            
            weight = safe_int(row[2])
            length = safe_int(row[3])
            height = safe_int(row[4])
            width = safe_int(row[5])
            photos = safe_int(row[6])
            batch_with_keys.append(
                (i + j + 1, row[0], row[1], '', weight, length, height, width, photos, '')
            )
        ch_client.execute(
            """INSERT INTO dim_products 
               (product_key, product_id, product_category_name, product_category_name_english,
                product_weight_g, product_length_cm, product_height_cm, product_width_cm, 
                product_photos_qty, product_size_category) VALUES""",
            batch_with_keys,
            types_check=True
        )
    
    print(f"  Loaded {len(rows)} products")
    pg_cursor.close()
    pg_conn.close()

def load_dim_sellers():
    """Load sellers to dim_sellers"""
    print("Loading dim_sellers...")
    
    pg_conn = get_pg_connection()
    pg_cursor = pg_conn.cursor()
    
    pg_cursor.execute("""
        SELECT 
            seller_id,
            seller_city,
            seller_state,
            seller_zip_code_prefix
        FROM sellers
    """)
    rows = pg_cursor.fetchall()
    
    ch_client = get_ch_client()
    ch_client.execute("TRUNCATE TABLE dim_sellers")
    
    batch_size = 5000
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        batch_with_keys = [
            (i + j + 1, row[0], row[1] or '', row[2] or '', row[3] or '', '')
            for j, row in enumerate(batch)
        ]
        ch_client.execute(
            """INSERT INTO dim_sellers 
               (seller_key, seller_id, seller_city, seller_state, 
                seller_zip_code_prefix, seller_region) VALUES""",
            batch_with_keys
        )
    
    print(f"  Loaded {len(rows)} sellers")
    pg_cursor.close()
    pg_conn.close()

def load_fact_orders():
    """Load orders to fact_orders"""
    print("Loading fact_orders...")
    
    pg_conn = get_pg_connection()
    pg_cursor = pg_conn.cursor()
    
    # Get order data with items
    pg_cursor.execute("""
        SELECT 
            o.order_id,
            COALESCE(oi.order_item_id, 1),
            COALESCE(oi.price, 0),
            COALESCE(oi.freight_value, 0),
            o.order_purchase_timestamp,
            o.order_approved_at,
            o.order_delivered_carrier_date,
            o.order_delivered_customer_date,
            o.order_estimated_delivery_date
        FROM orders o
        LEFT JOIN order_items oi ON o.order_id = oi.order_id
        LIMIT 200000
    """)
    rows = pg_cursor.fetchall()
    
    ch_client = get_ch_client()
    ch_client.execute("TRUNCATE TABLE fact_orders")
    
    batch_size = 10000
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        batch_data = []
        for j, row in enumerate(batch):
            order_key = i + j + 1
            order_id = row[0]
            order_item_id = row[1] or 1
            price = float(row[2] or 0)
            freight = float(row[3] or 0)
            
            # Handle timestamps
            purchase_ts = row[4] or datetime(1970, 1, 1)
            approved_at = row[5]
            carrier_date = row[6]
            customer_date = row[7]
            estimated_date = row[8]
            
            # date_key from purchase timestamp
            date_key = int(purchase_ts.strftime('%Y%m%d')) if purchase_ts else 19700101
            
            batch_data.append((
                order_key, date_key, 0, 0, 0, 0, 0,  # Keys (fill later)
                order_id, order_item_id, 1,  # order_id, item_id, quantity
                price, freight, price + freight, price, 1, None,  # amounts
                purchase_ts, approved_at, carrier_date, customer_date, estimated_date,
                None, None, None,  # delivery days
                ''  # batch_id
            ))
        
        ch_client.execute(
            """INSERT INTO fact_orders 
               (order_key, date_key, customer_key, product_key, seller_key, payment_type_key, status_key,
                order_id, order_item_id, quantity, unit_price, freight_value, total_amount, 
                payment_value, payment_installments, review_score,
                order_purchase_timestamp, order_approved_at, order_delivered_carrier_date,
                order_delivered_customer_date, order_estimated_delivery_date,
                delivery_days, estimated_delivery_days, delivery_delay_days, etl_batch_id) VALUES""",
            batch_data
        )
        if i % 50000 == 0:
            print(f"    Processed {i}/{len(rows)} orders...")
    
    print(f"  Loaded {len(rows)} orders")
    pg_cursor.close()
    pg_conn.close()

def load_fact_payments():
    """Load payments to fact_payments"""
    print("Loading fact_payments...")
    
    pg_conn = get_pg_connection()
    pg_cursor = pg_conn.cursor()
    
    pg_cursor.execute("""
        SELECT 
            op.order_id,
            op.payment_sequential,
            op.payment_value,
            op.payment_installments,
            o.order_purchase_timestamp
        FROM order_payments op
        JOIN orders o ON op.order_id = o.order_id
    """)
    rows = pg_cursor.fetchall()
    
    ch_client = get_ch_client()
    ch_client.execute("TRUNCATE TABLE fact_payments")
    
    batch_size = 10000
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        batch_data = []
        for j, row in enumerate(batch):
            order_key = i + j + 1
            order_id = row[0]
            payment_seq = row[1] or 1
            payment_value = float(row[2] or 0)
            installments = row[3] or 1
            purchase_ts = row[4] or datetime(1970, 1, 1)
            date_key = int(purchase_ts.strftime('%Y%m%d')) if purchase_ts else 19700101
            
            batch_data.append((
                order_key, payment_seq, date_key, 0, 0,  # keys
                order_id, payment_value, installments, purchase_ts
            ))
        
        ch_client.execute(
            """INSERT INTO fact_payments 
               (order_key, payment_sequential, date_key, customer_key, payment_type_key,
                order_id, payment_value, payment_installments, order_purchase_timestamp) VALUES""",
            batch_data
        )
    
    print(f"  Loaded {len(rows)} payments")
    pg_cursor.close()
    pg_conn.close()

def main():
    print("=" * 60)
    print("Direct ETL: PostgreSQL -> ClickHouse")
    print("=" * 60)
    
    try:
        # Test connections
        print("\nTesting PostgreSQL connection...")
        pg_conn = get_pg_connection()
        pg_conn.close()
        print("  PostgreSQL: OK")
        
        print("Testing ClickHouse connection...")
        ch_client = get_ch_client()
        ch_client.execute("SELECT 1")
        print("  ClickHouse: OK")
        
        # Load dimensions
        print("\n--- Loading Dimension Tables ---")
        load_dim_customers()
        load_dim_products()
        load_dim_sellers()
        
        # Load facts
        print("\n--- Loading Fact Tables ---")
        load_fact_orders()
        load_fact_payments()
        
        print("\n" + "=" * 60)
        print("ETL Complete!")
        print("=" * 60)
        
        # Verify counts
        print("\nVerifying loaded data:")
        ch_client = get_ch_client()
        for table in ['dim_customers', 'dim_products', 'dim_sellers', 'fact_orders', 'fact_payments']:
            count = ch_client.execute(f"SELECT count() FROM {table}")[0][0]
            print(f"  {table}: {count:,} rows")
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
