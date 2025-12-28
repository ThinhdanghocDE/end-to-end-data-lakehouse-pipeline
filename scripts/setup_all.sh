#!/bin/bash
# E-commerce Data Platform - Setup Script (Linux/Mac)
# This script sets up the entire data platform

echo "============================================================"
echo "E-commerce Data Platform Setup"
echo "============================================================"

# Check Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "ERROR: Docker is not running. Please start Docker."
    exit 1
fi

echo ""
echo "[1/5] Starting Docker containers..."
docker-compose up -d

echo ""
echo "[2/5] Waiting for services to be ready (60 seconds)..."
sleep 60

echo ""
echo "[3/5] Checking service health..."
docker-compose ps

echo ""
echo "[4/5] Setting up Debezium connector..."
sleep 10

curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/connector_config.json

echo ""
echo "[5/5] Verifying connector status..."
sleep 5
curl http://localhost:8083/connectors/ecommerce-connector/status

echo ""
echo "============================================================"
echo "SETUP COMPLETE!"
echo "============================================================"
echo ""
echo "Access URLs:"
echo "  Superset Dashboard:  http://localhost:8088 (admin/admin)"
echo "  MinIO Console:       http://localhost:9001 (minioadmin/minioadmin)"
echo "  Kafka UI:            http://localhost:8080"
echo "  PgAdmin:             http://localhost:5050 (admin@admin.com/admin)"
echo "  ClickHouse HTTP:     http://localhost:8123"
echo ""
echo "Next steps:"
echo "  1. Load Kaggle data:     python scripts/load_kaggle_data.py"
echo "  2. Start data generator: python scripts/data_generator.py"
echo "  3. Run Spark streaming:  python spark/streaming_bronze.py"
echo ""
