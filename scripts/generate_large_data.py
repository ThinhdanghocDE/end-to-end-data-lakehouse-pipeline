#!/usr/bin/env python3
"""
Large Scale Synthetic Data Generator
Generates 10GB+ of realistic e-commerce data for big data demonstration

Target sizes:
- customers: 5 million records (~500MB)
- sellers: 100,000 records (~10MB)
- products: 1 million records (~200MB)
- orders: 20 million records (~2GB)
- order_items: 50 million records (~4GB)
- order_payments: 25 million records (~2GB)
- order_reviews: 15 million records (~1.5GB)

Total: ~10GB+
"""

import os
import random
import string
import csv
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor
import multiprocessing

# Configuration
OUTPUT_DIR = "data/generated"
BATCH_SIZE = 100000  # Write in batches to manage memory

# Scale factors (adjust these to control total data size)
NUM_CUSTOMERS = 5_000_000
NUM_SELLERS = 100_000
NUM_PRODUCTS = 1_000_000
NUM_ORDERS = 20_000_000
AVG_ITEMS_PER_ORDER = 2.5
AVG_PAYMENTS_PER_ORDER = 1.25
REVIEW_RATE = 0.75  # 75% of orders have reviews

# Reference data
STATES = ['SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'GO', 'PE', 'CE', 
          'DF', 'ES', 'MA', 'MT', 'MS', 'PA', 'PB', 'RN', 'PI', 'AL',
          'SE', 'TO', 'RO', 'AC', 'AP', 'AM', 'RR']

CITIES = ['Sao Paulo', 'Rio de Janeiro', 'Belo Horizonte', 'Porto Alegre',
          'Curitiba', 'Salvador', 'Brasilia', 'Fortaleza', 'Recife', 'Manaus',
          'Goiania', 'Campinas', 'Guarulhos', 'Santos', 'Osasco', 'Niteroi']

CATEGORIES = ['electronics', 'computers', 'home_appliances', 'furniture', 
              'sports', 'fashion', 'beauty', 'toys', 'books', 'food',
              'automotive', 'garden', 'pet_supplies', 'office', 'music',
              'games', 'health', 'baby', 'watches', 'jewelry']

ORDER_STATUS = ['delivered', 'shipped', 'processing', 'canceled', 'unavailable']
PAYMENT_TYPES = ['credit_card', 'boleto', 'voucher', 'debit_card', 'pix']

def random_string(length):
    return ''.join(random.choices(string.ascii_lowercase, k=length))

def random_zip():
    return f"{random.randint(10000, 99999):05d}{random.randint(100, 999):03d}"

def random_date(start_year=2017, end_year=2023):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    return start + timedelta(days=random_days)

def generate_customers(start_id, count, filename):
    """Generate customer records"""
    with open(filename, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        batch = []
        for i in range(count):
            customer_id = f"cust_{start_id + i:010d}"
            unique_id = f"unique_{start_id + i:012d}"
            zip_code = random_zip()
            city = random.choice(CITIES)
            state = random.choice(STATES)
            
            batch.append([customer_id, unique_id, zip_code, city, state])
            
            if len(batch) >= BATCH_SIZE:
                writer.writerows(batch)
                batch = []
        
        if batch:
            writer.writerows(batch)
    
    return count

def generate_sellers(start_id, count, filename):
    """Generate seller records"""
    with open(filename, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        batch = []
        for i in range(count):
            seller_id = f"seller_{start_id + i:08d}"
            zip_code = random_zip()
            city = random.choice(CITIES)
            state = random.choice(STATES)
            
            batch.append([seller_id, zip_code, city, state])
            
            if len(batch) >= BATCH_SIZE:
                writer.writerows(batch)
                batch = []
        
        if batch:
            writer.writerows(batch)
    
    return count

def generate_products(start_id, count, filename):
    """Generate product records"""
    with open(filename, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        batch = []
        for i in range(count):
            product_id = f"prod_{start_id + i:010d}"
            category = random.choice(CATEGORIES)
            name_length = random.randint(3, 8)
            weight = round(random.uniform(0.1, 50.0), 2)
            length = random.randint(10, 100)
            height = random.randint(5, 50)
            width = random.randint(5, 50)
            photos = random.randint(1, 6)
            
            batch.append([product_id, category, name_length, photos, weight, length, height, width])
            
            if len(batch) >= BATCH_SIZE:
                writer.writerows(batch)
                batch = []
        
        if batch:
            writer.writerows(batch)
    
    return count

def generate_orders_batch(args):
    """Generate a batch of orders (for parallel processing)"""
    batch_id, start_id, count, num_customers, filename = args
    
    orders = []
    for i in range(count):
        order_id = f"order_{start_id + i:012d}"
        customer_id = f"cust_{random.randint(0, num_customers - 1):010d}"
        status = random.choices(ORDER_STATUS, weights=[70, 15, 8, 5, 2])[0]
        
        purchase_date = random_date()
        approved_date = purchase_date + timedelta(hours=random.randint(1, 24))
        delivered_date = purchase_date + timedelta(days=random.randint(3, 30))
        estimated_date = purchase_date + timedelta(days=random.randint(7, 45))
        
        orders.append([
            order_id, customer_id, status,
            purchase_date.strftime('%Y-%m-%d %H:%M:%S'),
            approved_date.strftime('%Y-%m-%d %H:%M:%S'),
            delivered_date.strftime('%Y-%m-%d %H:%M:%S'),
            estimated_date.strftime('%Y-%m-%d %H:%M:%S')
        ])
    
    return orders

def generate_order_items_batch(args):
    """Generate order items for a batch of orders"""
    batch_id, order_ids, num_products, num_sellers = args
    
    items = []
    for order_id in order_ids:
        num_items = random.choices([1, 2, 3, 4, 5], weights=[40, 30, 15, 10, 5])[0]
        
        for item_num in range(1, num_items + 1):
            product_id = f"prod_{random.randint(0, num_products - 1):010d}"
            seller_id = f"seller_{random.randint(0, num_sellers - 1):08d}"
            shipping_date = random_date()
            price = round(random.uniform(10, 5000), 2)
            freight = round(random.uniform(5, 100), 2)
            
            items.append([
                order_id, item_num, product_id, seller_id,
                shipping_date.strftime('%Y-%m-%d %H:%M:%S'),
                price, freight
            ])
    
    return items

def generate_payments_batch(order_ids):
    """Generate payments for a batch of orders"""
    payments = []
    for order_id in order_ids:
        num_payments = random.choices([1, 2, 3], weights=[80, 15, 5])[0]
        
        for seq in range(1, num_payments + 1):
            payment_type = random.choice(PAYMENT_TYPES)
            installments = random.randint(1, 12) if payment_type == 'credit_card' else 1
            value = round(random.uniform(50, 2000), 2)
            
            payments.append([order_id, seq, payment_type, installments, value])
    
    return payments

def generate_reviews_batch(order_ids):
    """Generate reviews for a batch of orders"""
    reviews = []
    for order_id in order_ids:
        if random.random() < REVIEW_RATE:
            review_id = f"review_{order_id}"
            score = random.choices([1, 2, 3, 4, 5], weights=[5, 5, 10, 30, 50])[0]
            title = random_string(random.randint(5, 20)) if random.random() > 0.3 else ""
            message = random_string(random.randint(20, 100)) if random.random() > 0.4 else ""
            creation_date = random_date()
            answer_date = creation_date + timedelta(days=random.randint(1, 7))
            
            reviews.append([
                review_id, order_id, score, title, message,
                creation_date.strftime('%Y-%m-%d %H:%M:%S'),
                answer_date.strftime('%Y-%m-%d %H:%M:%S')
            ])
    
    return reviews

def write_csv_header(filename, headers):
    """Write CSV header"""
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)

def append_to_csv(filename, rows):
    """Append rows to CSV file"""
    with open(filename, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(rows)

def format_size(size_bytes):
    """Format bytes to human readable"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"

def get_file_size(filename):
    """Get file size"""
    try:
        return os.path.getsize(filename)
    except:
        return 0

def main():
    print("=" * 70)
    print("Large Scale E-commerce Data Generator")
    print("=" * 70)
    print(f"Target: ~10GB of synthetic data")
    print(f"- Customers: {NUM_CUSTOMERS:,}")
    print(f"- Sellers: {NUM_SELLERS:,}")
    print(f"- Products: {NUM_PRODUCTS:,}")
    print(f"- Orders: {NUM_ORDERS:,}")
    print("=" * 70)
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # File paths
    files = {
        'customers': os.path.join(OUTPUT_DIR, 'customers_large.csv'),
        'sellers': os.path.join(OUTPUT_DIR, 'sellers_large.csv'),
        'products': os.path.join(OUTPUT_DIR, 'products_large.csv'),
        'orders': os.path.join(OUTPUT_DIR, 'orders_large.csv'),
        'order_items': os.path.join(OUTPUT_DIR, 'order_items_large.csv'),
        'order_payments': os.path.join(OUTPUT_DIR, 'order_payments_large.csv'),
        'order_reviews': os.path.join(OUTPUT_DIR, 'order_reviews_large.csv'),
    }
    
    # Write headers
    print("\n[1/7] Generating customers...")
    write_csv_header(files['customers'], 
                     ['customer_id', 'customer_unique_id', 'customer_zip_code', 
                      'customer_city', 'customer_state'])
    generate_customers(0, NUM_CUSTOMERS, files['customers'])
    print(f"  -> Created {format_size(get_file_size(files['customers']))}")
    
    print("\n[2/7] Generating sellers...")
    write_csv_header(files['sellers'],
                     ['seller_id', 'seller_zip_code', 'seller_city', 'seller_state'])
    generate_sellers(0, NUM_SELLERS, files['sellers'])
    print(f"  -> Created {format_size(get_file_size(files['sellers']))}")
    
    print("\n[3/7] Generating products...")
    write_csv_header(files['products'],
                     ['product_id', 'product_category_name', 'product_name_length',
                      'product_photos_qty', 'product_weight_g', 'product_length_cm',
                      'product_height_cm', 'product_width_cm'])
    generate_products(0, NUM_PRODUCTS, files['products'])
    print(f"  -> Created {format_size(get_file_size(files['products']))}")
    
    print("\n[4/7] Generating orders...")
    write_csv_header(files['orders'],
                     ['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp',
                      'order_approved_at', 'order_delivered_timestamp', 
                      'order_estimated_delivery_date'])
    
    # Generate orders in batches
    order_ids = []
    for batch_start in range(0, NUM_ORDERS, BATCH_SIZE):
        batch_count = min(BATCH_SIZE, NUM_ORDERS - batch_start)
        orders = generate_orders_batch((0, batch_start, batch_count, NUM_CUSTOMERS, None))
        append_to_csv(files['orders'], orders)
        order_ids.extend([o[0] for o in orders])
        
        if batch_start % (BATCH_SIZE * 10) == 0:
            print(f"  -> Progress: {batch_start:,}/{NUM_ORDERS:,}")
    
    print(f"  -> Created {format_size(get_file_size(files['orders']))}")
    
    print("\n[5/7] Generating order items...")
    write_csv_header(files['order_items'],
                     ['order_id', 'order_item_id', 'product_id', 'seller_id',
                      'shipping_limit_date', 'price', 'freight_value'])
    
    for i in range(0, len(order_ids), BATCH_SIZE):
        batch_orders = order_ids[i:i + BATCH_SIZE]
        items = generate_order_items_batch((0, batch_orders, NUM_PRODUCTS, NUM_SELLERS))
        append_to_csv(files['order_items'], items)
        
        if i % (BATCH_SIZE * 10) == 0:
            print(f"  -> Progress: {i:,}/{len(order_ids):,}")
    
    print(f"  -> Created {format_size(get_file_size(files['order_items']))}")
    
    print("\n[6/7] Generating payments...")
    write_csv_header(files['order_payments'],
                     ['order_id', 'payment_sequential', 'payment_type',
                      'payment_installments', 'payment_value'])
    
    for i in range(0, len(order_ids), BATCH_SIZE):
        batch_orders = order_ids[i:i + BATCH_SIZE]
        payments = generate_payments_batch(batch_orders)
        append_to_csv(files['order_payments'], payments)
        
        if i % (BATCH_SIZE * 10) == 0:
            print(f"  -> Progress: {i:,}/{len(order_ids):,}")
    
    print(f"  -> Created {format_size(get_file_size(files['order_payments']))}")
    
    print("\n[7/7] Generating reviews...")
    write_csv_header(files['order_reviews'],
                     ['review_id', 'order_id', 'review_score', 'review_comment_title',
                      'review_comment_message', 'review_creation_date', 
                      'review_answer_timestamp'])
    
    for i in range(0, len(order_ids), BATCH_SIZE):
        batch_orders = order_ids[i:i + BATCH_SIZE]
        reviews = generate_reviews_batch(batch_orders)
        append_to_csv(files['order_reviews'], reviews)
        
        if i % (BATCH_SIZE * 10) == 0:
            print(f"  -> Progress: {i:,}/{len(order_ids):,}")
    
    print(f"  -> Created {format_size(get_file_size(files['order_reviews']))}")
    
    # Summary
    print("\n" + "=" * 70)
    print("GENERATION COMPLETE")
    print("=" * 70)
    
    total_size = 0
    for name, filepath in files.items():
        size = get_file_size(filepath)
        total_size += size
        print(f"  {name}: {format_size(size)}")
    
    print("-" * 70)
    print(f"  TOTAL: {format_size(total_size)}")
    print("=" * 70)
    print(f"\nFiles saved to: {OUTPUT_DIR}/")

if __name__ == "__main__":
    main()
