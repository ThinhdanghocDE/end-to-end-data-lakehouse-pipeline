#!/usr/bin/env python3
"""
Script to register Debezium connector for CDC from PostgreSQL to Kafka
"""
import os
import requests
import json
import time

# Configuration from environment variables
DEBEZIUM_URL = os.getenv("DEBEZIUM_URL", "http://localhost:8084")
PG_HOST = os.getenv("PG_HOST", "postgres")  # Use 'postgres' for Docker network
PG_PORT = os.getenv("PG_PORT", "5432")
PG_USER = os.getenv("POSTGRES_USER", "ecommerce")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "ecommerce123")
PG_DB = os.getenv("POSTGRES_DB", "ecommerce_db")
CONNECTOR_NAME = os.getenv("DEBEZIUM_CONNECTOR_NAME", "ecommerce-connector")

def get_connector_config():
    """Build connector configuration from environment"""
    return {
        "name": CONNECTOR_NAME,
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "tasks.max": "1",
            "database.hostname": PG_HOST,
            "database.port": PG_PORT,
            "database.user": PG_USER,
            "database.password": PG_PASSWORD,
            "database.dbname": PG_DB,
            "topic.prefix": "ecommerce",
            "plugin.name": "pgoutput",
            "slot.name": "debezium_ecommerce_slot",
            "publication.name": "dbz_publication",
            "publication.autocreate.mode": "filtered",
            "table.include.list": "public.customers,public.sellers,public.products,public.product_categories,public.orders,public.order_items,public.order_payments,public.order_reviews",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "snapshot.mode": "initial",
            "decimal.handling.mode": "double",
            "time.precision.mode": "adaptive_time_microseconds"
        }
    }

def wait_for_connect():
    """Wait for Kafka Connect to be ready"""
    print(f"Waiting for Kafka Connect at {DEBEZIUM_URL}...")
    for i in range(30):
        try:
            response = requests.get(f"{DEBEZIUM_URL}/connectors")
            if response.status_code == 200:
                print("[OK] Kafka Connect is ready!")
                return True
        except:
            pass
        print(f"  Attempt {i+1}/30...")
        time.sleep(2)
    print("[FAIL] Kafka Connect not ready after 60 seconds")
    return False

def delete_connector():
    """Delete existing connector if exists"""
    try:
        response = requests.delete(f"{DEBEZIUM_URL}/connectors/{CONNECTOR_NAME}")
        if response.status_code in [200, 204]:
            print(f"[OK] Deleted existing connector: {CONNECTOR_NAME}")
        elif response.status_code == 404:
            print(f"[INFO] Connector {CONNECTOR_NAME} doesn't exist, will create new one")
    except Exception as e:
        print(f"[WARN] Could not delete connector: {e}")

def create_connector():
    """Create the Debezium connector"""
    config = get_connector_config()
    try:
        response = requests.post(
            f"{DEBEZIUM_URL}/connectors",
            headers={"Content-Type": "application/json"},
            data=json.dumps(config)
        )
        if response.status_code in [200, 201]:
            print(f"[OK] Connector {CONNECTOR_NAME} created successfully!")
            return True
        else:
            print(f"[FAIL] Failed to create connector: {response.status_code}")
            print(response.text)
            return False
    except Exception as e:
        print(f"[FAIL] Error creating connector: {e}")
        return False

def check_connector_status():
    """Check connector status"""
    time.sleep(5)
    try:
        response = requests.get(f"{DEBEZIUM_URL}/connectors/{CONNECTOR_NAME}/status")
        if response.status_code == 200:
            status = response.json()
            connector_state = status.get("connector", {}).get("state", "UNKNOWN")
            print(f"[INFO] Connector state: {connector_state}")
            
            tasks = status.get("tasks", [])
            for task in tasks:
                task_state = task.get("state", "UNKNOWN")
                print(f"[INFO] Task {task.get('id')}: {task_state}")
            
            return connector_state == "RUNNING"
    except Exception as e:
        print(f"[WARN] Could not check status: {e}")
    return False

def main():
    print("=" * 60)
    print("Setting up Debezium Connector")
    print("=" * 60)
    print(f"Debezium URL: {DEBEZIUM_URL}")
    print(f"PostgreSQL: {PG_USER}@{PG_HOST}:{PG_PORT}/{PG_DB}")
    print("=" * 60)
    
    if not wait_for_connect():
        return
    
    delete_connector()
    time.sleep(2)
    
    if create_connector():
        check_connector_status()
        print("=" * 60)
        print("Debezium setup complete!")
        print("Topics should be created: ecommerce.public.*")
        print("=" * 60)

if __name__ == "__main__":
    main()