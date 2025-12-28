"""
Apache Superset Configuration
Custom configuration for connecting to ClickHouse Data Warehouse
"""

import os
from datetime import timedelta

# ============================================
# General Configuration
# ============================================
ROW_LIMIT = 5000
SUPERSET_WEBSERVER_PORT = 8088
SECRET_KEY = os.getenv('SUPERSET_SECRET_KEY', 'supersecretkey123')

# ============================================
# Database Configuration
# ============================================
# Superset's own metadata database
SQLALCHEMY_DATABASE_URI = 'sqlite:////app/superset_home/superset.db'

# ============================================
# Cache Configuration (Redis)
# ============================================
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_HOST': os.getenv('REDIS_HOST', 'redis'),
    'CACHE_REDIS_PORT': int(os.getenv('REDIS_PORT', 6379)),
    'CACHE_REDIS_DB': 1,
}

DATA_CACHE_CONFIG = CACHE_CONFIG

# ============================================
# Feature Flags
# ============================================
FEATURE_FLAGS = {
    'ENABLE_TEMPLATE_PROCESSING': True,
    'DASHBOARD_NATIVE_FILTERS': True,
    'DASHBOARD_CROSS_FILTERS': True,
    'DASHBOARD_NATIVE_FILTERS_SET': True,
    'ALERT_REPORTS': True,
    'EMBEDDED_SUPERSET': True,
}

# ============================================
# SQL Lab Configuration
# ============================================
SQLLAB_TIMEOUT = 300
SUPERSET_WEBSERVER_TIMEOUT = 300

# ============================================
# Theme Configuration
# ============================================
APP_NAME = "E-commerce Analytics"
APP_ICON = "/static/assets/images/superset-logo-horiz.png"

# ============================================
# Database Connections
# ============================================
# ClickHouse connection string format:
# clickhousedb://{username}:{password}@{hostname}:{port}/{database}
#
# After starting Superset, add this connection manually:
# 
# Database Name: ClickHouse DW
# SQLAlchemy URI: clickhousedb://default:@clickhouse:8123/ecommerce_dw
#
# Or use the native ClickHouse connector:
# clickhouse+native://default:@clickhouse:9000/ecommerce_dw

# Additional database drivers to install
# pip install clickhouse-connect sqlalchemy-clickhouse

# ============================================
# Security Configuration
# ============================================
AUTH_TYPE = 1  # Database authentication
ENABLE_PROXY_FIX = True

# ============================================
# Logging Configuration
# ============================================
ENABLE_TIME_ROTATE = True
TIME_ROTATE_LOG_LEVEL = 'INFO'
