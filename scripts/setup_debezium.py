#!/usr/bin/env python3
"""
Script to register Debezium connector for CDC from PostgreSQL to Kafka
"""
import requests
import json
import time

DEBEZIUM_URL = "http://localhost:8083"

CONNECTOR_CONFIG = {
    "name": "ecommerce-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "ecommerce",
        "database.password": "ecommerce123",
        "database.dbname": "ecommerce_db",
        "database.server.name": "ecommerce",
        "topic.prefix": "ecommerce",
        "table.include.list": "public.customers,public.sellers,public.products,public.orders,public.order_items,public.order_payments,public.order_reviews",
        "plugin.name": "pgoutput",
        "slot.name": "debezium_slot",
        "publication.name": "dbz_publication",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false"
    }
}

def wait_for_connect():
    """Wait for Kafka Connect to be ready"""
    print("Waiting for Kafka Connect to be ready...")
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
        response = requests.delete(f"{DEBEZIUM_URL}/connectors/ecommerce-connector")
        if response.status_code in [200, 204]:
            print("[OK] Deleted existing connector")
        elif response.status_code == 404:
            print("[INFO] Connector doesn't exist, will create new one")
    except Exception as e:
        print(f"[WARN] Could not delete connector: {e}")

def create_connector():
    """Create the Debezium connector"""
    try:
        response = requests.post(
            f"{DEBEZIUM_URL}/connectors",
            headers={"Content-Type": "application/json"},
            data=json.dumps(CONNECTOR_CONFIG)
        )
        if response.status_code in [200, 201]:
            print("[OK] Connector created successfully!")
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
        response = requests.get(f"{DEBEZIUM_URL}/connectors/ecommerce-connector/status")
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
