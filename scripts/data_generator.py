"""
E-commerce Real-time Data Generator
Generates realistic e-commerce orders continuously to simulate real-time activity.
This creates CDC events that flow through the pipeline.
"""

import os
import sys
import random
import time
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
import psycopg2
from faker import Faker

# Configuration
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5433")
PG_DB = os.getenv("PG_DB", "ecommerce_db")
PG_USER = os.getenv("PG_USER", "ecommerce")
PG_PASSWORD = os.getenv("PG_PASSWORD", "ecommerce123")

# Generator settings
ORDERS_PER_MINUTE = int(os.getenv("ORDERS_PER_MINUTE", "10"))
UPDATE_PROBABILITY = float(os.getenv("UPDATE_PROBABILITY", "0.3"))

# Brazilian states with weights (population-based)
BRAZILIAN_STATES = {
    "SP": 0.22, "MG": 0.10, "RJ": 0.08, "BA": 0.07, "RS": 0.06,
    "PR": 0.06, "PE": 0.05, "CE": 0.04, "PA": 0.04, "SC": 0.04,
    "MA": 0.03, "GO": 0.03, "PB": 0.02, "AM": 0.02, "ES": 0.02,
    "RN": 0.02, "AL": 0.02, "PI": 0.02, "MT": 0.02, "DF": 0.01,
    "MS": 0.01, "SE": 0.01, "RO": 0.01, "TO": 0.01, "AC": 0.01,
    "AP": 0.01, "RR": 0.01
}

PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card"]
PAYMENT_WEIGHTS = [0.75, 0.15, 0.05, 0.05]

ORDER_STATUSES = ["created", "approved", "invoiced", "processing", "shipped", "delivered"]

fake = Faker('pt_BR')

def get_connection():
    """Create PostgreSQL connection"""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )

def get_random_state():
    """Get random Brazilian state based on population weights"""
    states = list(BRAZILIAN_STATES.keys())
    weights = list(BRAZILIAN_STATES.values())
    return random.choices(states, weights=weights, k=1)[0]

def create_customer(conn):
    """Create a new customer"""
    cur = conn.cursor()
    
    customer_id = str(uuid.uuid4()).replace('-', '')
    customer_unique_id = str(uuid.uuid4()).replace('-', '')
    state = get_random_state()
    city = fake.city()
    zip_code = fake.postcode()[:5]
    
    cur.execute("""
        INSERT INTO customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (customer_id) DO NOTHING
        RETURNING customer_id
    """, (customer_id, customer_unique_id, zip_code, city, state))
    
    result = cur.fetchone()
    conn.commit()
    cur.close()
    
    return result[0] if result else customer_id

def get_random_customer(conn):
    """Get a random existing customer or create new one"""
    cur = conn.cursor()
    
    # 30% chance to create new customer
    if random.random() < 0.3:
        return create_customer(conn)
    
    cur.execute("SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 1")
    result = cur.fetchone()
    cur.close()
    
    if result:
        return result[0]
    else:
        return create_customer(conn)

def get_random_products(conn, count=None):
    """Get random products"""
    if count is None:
        count = random.randint(1, 5)
    
    cur = conn.cursor()
    cur.execute(f"SELECT product_id FROM products ORDER BY RANDOM() LIMIT {count}")
    products = [row[0] for row in cur.fetchall()]
    cur.close()
    
    return products

def get_random_seller(conn):
    """Get a random seller"""
    cur = conn.cursor()
    cur.execute("SELECT seller_id FROM sellers ORDER BY RANDOM() LIMIT 1")
    result = cur.fetchone()
    cur.close()
    
    return result[0] if result else None

def create_order(conn):
    """Create a complete order with items and payment"""
    cur = conn.cursor()
    
    order_id = str(uuid.uuid4()).replace('-', '')
    customer_id = get_random_customer(conn)
    
    if not customer_id:
        print("No customers available. Please load Kaggle data first.")
        return None
    
    # Order timestamps
    purchase_time = datetime.now()
    approved_time = purchase_time + timedelta(minutes=random.randint(1, 60))
    
    # Create order
    cur.execute("""
        INSERT INTO orders (order_id, customer_id, order_status, order_purchase_timestamp, 
                          order_approved_at, order_estimated_delivery_date)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING order_id
    """, (
        order_id,
        customer_id,
        "approved",
        purchase_time,
        approved_time,
        purchase_time + timedelta(days=random.randint(7, 21))
    ))
    
    # Get random products for this order
    products = get_random_products(conn)
    
    if not products:
        print("No products available. Please load Kaggle data first.")
        conn.rollback()
        cur.close()
        return None
    
    total_amount = Decimal('0')
    
    # Create order items
    for i, product_id in enumerate(products, 1):
        seller_id = get_random_seller(conn)
        
        if not seller_id:
            continue
        
        price = Decimal(str(round(random.uniform(20, 500), 2)))
        freight = Decimal(str(round(random.uniform(5, 50), 2)))
        total_amount += price + freight
        
        cur.execute("""
            INSERT INTO order_items (order_id, order_item_id, product_id, seller_id, 
                                   shipping_limit_date, price, freight_value)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            order_id,
            i,
            product_id,
            seller_id,
            purchase_time + timedelta(days=3),
            float(price),
            float(freight)
        ))
    
    # Create payment
    payment_type = random.choices(PAYMENT_TYPES, weights=PAYMENT_WEIGHTS, k=1)[0]
    installments = 1 if payment_type != "credit_card" else random.randint(1, 12)
    
    cur.execute("""
        INSERT INTO order_payments (order_id, payment_sequential, payment_type, 
                                   payment_installments, payment_value)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        order_id,
        1,
        payment_type,
        installments,
        float(total_amount)
    ))
    
    conn.commit()
    cur.close()
    
    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "items": len(products),
        "total": float(total_amount),
        "payment_type": payment_type
    }

def update_order_status(conn):
    """Update status of a random pending order"""
    cur = conn.cursor()
    
    # Get a random order that can be updated
    cur.execute("""
        SELECT order_id, order_status FROM orders 
        WHERE order_status NOT IN ('delivered', 'canceled')
        ORDER BY RANDOM() LIMIT 1
    """)
    
    result = cur.fetchone()
    if not result:
        cur.close()
        return None
    
    order_id, current_status = result
    
    # Determine next status
    try:
        current_idx = ORDER_STATUSES.index(current_status)
        if current_idx < len(ORDER_STATUSES) - 1:
            next_status = ORDER_STATUSES[current_idx + 1]
        else:
            cur.close()
            return None
    except ValueError:
        next_status = "approved"
    
    # Update fields based on status
    update_fields = []
    update_values = []
    
    if next_status == "shipped":
        update_fields.append("order_delivered_carrier_date = %s")
        update_values.append(datetime.now())
    elif next_status == "delivered":
        update_fields.append("order_delivered_customer_date = %s")
        update_values.append(datetime.now())
    
    update_fields.append("order_status = %s")
    update_values.append(next_status)
    update_values.append(order_id)
    
    cur.execute(f"""
        UPDATE orders 
        SET {", ".join(update_fields)}, updated_at = CURRENT_TIMESTAMP
        WHERE order_id = %s
    """, update_values)
    
    conn.commit()
    cur.close()
    
    return {"order_id": order_id, "old_status": current_status, "new_status": next_status}

def create_review(conn):
    """Create a review for a delivered order"""
    cur = conn.cursor()
    
    # Get a delivered order without review
    cur.execute("""
        SELECT o.order_id FROM orders o
        LEFT JOIN order_reviews r ON o.order_id = r.order_id
        WHERE o.order_status = 'delivered' AND r.review_id IS NULL
        ORDER BY RANDOM() LIMIT 1
    """)
    
    result = cur.fetchone()
    if not result:
        cur.close()
        return None
    
    order_id = result[0]
    review_id = str(uuid.uuid4()).replace('-', '')
    
    # Generate realistic review score (weighted towards higher scores)
    score = random.choices([1, 2, 3, 4, 5], weights=[0.05, 0.05, 0.10, 0.30, 0.50], k=1)[0]
    
    # Generate review message based on score
    if score >= 4:
        messages = [
            "Ótimo produto, recomendo!",
            "Chegou rápido, produto excelente.",
            "Muito satisfeito com a compra.",
            "Qualidade excelente, superou expectativas.",
            None  # Some reviews have no message
        ]
    elif score == 3:
        messages = [
            "Produto ok, nada demais.",
            "Entrega demorou um pouco.",
            "Poderia ser melhor.",
            None
        ]
    else:
        messages = [
            "Produto não correspondeu às expectativas.",
            "Demorou muito para chegar.",
            "Qualidade inferior ao esperado.",
            None
        ]
    
    message = random.choice(messages)
    
    cur.execute("""
        INSERT INTO order_reviews (review_id, order_id, review_score, review_comment_message, 
                                  review_creation_date, review_answer_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        review_id,
        order_id,
        score,
        message,
        datetime.now(),
        datetime.now() + timedelta(hours=random.randint(1, 48)) if random.random() < 0.7 else None
    ))
    
    conn.commit()
    cur.close()
    
    return {"order_id": order_id, "score": score}

def main():
    print("=" * 60)
    print("E-commerce Real-time Data Generator")
    print("=" * 60)
    print(f"PostgreSQL: {PG_HOST}:{PG_PORT}/{PG_DB}")
    print(f"Orders per minute: {ORDERS_PER_MINUTE}")
    print(f"Update probability: {UPDATE_PROBABILITY}")
    print("=" * 60)
    print("\nPress Ctrl+C to stop\n")
    
    # Connect to PostgreSQL
    try:
        conn = get_connection()
        print("✓ Connected to PostgreSQL")
    except Exception as e:
        print(f"✗ Failed to connect to PostgreSQL: {e}")
        print("\nMake sure Docker containers are running:")
        print("  docker-compose up -d postgres")
        sys.exit(1)
    
    # Calculate delay between orders
    delay = 60.0 / ORDERS_PER_MINUTE
    
    orders_created = 0
    orders_updated = 0
    reviews_created = 0
    start_time = datetime.now()
    
    try:
        while True:
            # Create new order
            order = create_order(conn)
            if order:
                orders_created += 1
                print(f"[NEW ORDER] {order['order_id'][:8]}... | Items: {order['items']} | "
                      f"${order['total']:.2f} | {order['payment_type']}")
            
            # Maybe update existing order
            if random.random() < UPDATE_PROBABILITY:
                update = update_order_status(conn)
                if update:
                    orders_updated += 1
                    print(f"[UPDATE] {update['order_id'][:8]}... | "
                          f"{update['old_status']} → {update['new_status']}")
            
            # Maybe create a review
            if random.random() < 0.1:
                review = create_review(conn)
                if review:
                    reviews_created += 1
                    stars = "⭐" * review['score']
                    print(f"[REVIEW] {review['order_id'][:8]}... | {stars}")
            
            # Stats every 10 orders
            if orders_created % 10 == 0:
                elapsed = (datetime.now() - start_time).total_seconds() / 60
                rate = orders_created / elapsed if elapsed > 0 else 0
                print(f"\n--- Stats: {orders_created} orders | {orders_updated} updates | "
                      f"{reviews_created} reviews | {rate:.1f} orders/min ---\n")
            
            time.sleep(delay)
            
    except KeyboardInterrupt:
        print("\n\nStopping generator...")
    finally:
        conn.close()
        elapsed = (datetime.now() - start_time).total_seconds() / 60
        print("\n" + "=" * 60)
        print("GENERATOR STOPPED")
        print("=" * 60)
        print(f"Orders created: {orders_created}")
        print(f"Orders updated: {orders_updated}")
        print(f"Reviews created: {reviews_created}")
        print(f"Time elapsed: {elapsed:.1f} minutes")
        print(f"Avg rate: {orders_created / elapsed:.1f} orders/min" if elapsed > 0 else "")
        print("=" * 60)

if __name__ == "__main__":
    main()
