# Real-Time E-commerce Data Pipeline

## Tổng Quan

Dự án này xây dựng một **hệ thống Data Pipeline hoàn chỉnh** cho nền tảng thương mại điện tử, áp dụng các công nghệ và phương pháp hiện đại trong lĩnh vực Data Engineering.

---

## Bài Toán

Các doanh nghiệp thương mại điện tử đang đối mặt với những thách thức lớn trong việc xử lý dữ liệu:

1. **Dữ liệu phân tán**: Thông tin khách hàng, đơn hàng, sản phẩm nằm rải rác trong các hệ thống OLTP, khó tổng hợp để phân tích.

2. **Độ trễ cao**: Các báo cáo kinh doanh thường phải chờ đến cuối ngày hoặc cuối tuần để được cập nhật, dẫn đến việc ra quyết định chậm trễ.

3. **Thiếu khả năng mở rộng**: Khi lượng giao dịch tăng lên hàng triệu bản ghi mỗi ngày, hệ thống truyền thống không đáp ứng được yêu cầu về hiệu năng.

4. **Không có lịch sử thay đổi**: Chỉ lưu trữ trạng thái hiện tại của dữ liệu, không theo dõi được lịch sử thay đổi để phân tích xu hướng.

---

## Giải Pháp

Dự án này triển khai một **Modern Data Stack** với các thành phần:

### 1. Change Data Capture (CDC) với Debezium
- Theo dõi và bắt mọi thay đổi từ database nguồn theo thời gian thực
- Không ảnh hưởng đến hiệu năng của hệ thống OLTP
- Đảm bảo không mất dữ liệu với cơ chế exactly-once delivery

### 2. Event Streaming với Apache Kafka
- Xử lý hàng triệu sự kiện mỗi giây
- Đảm bảo thứ tự và độ tin cậy của dữ liệu
- Cho phép nhiều consumer xử lý song song

### 3. Data Lake với Kiến Trúc Medallion
- **Bronze Layer**: Lưu trữ dữ liệu thô từ CDC, giữ nguyên định dạng gốc
- **Silver Layer**: Dữ liệu đã được làm sạch, chuẩn hóa, loại bỏ trùng lặp
- **Gold Layer**: Dữ liệu đã được tổng hợp, sẵn sàng cho phân tích

### 4. Data Warehouse với Star Schema
- Thiết kế tối ưu cho truy vấn OLAP
- Fact table chứa các metrics kinh doanh
- Dimension tables chứa thông tin mô tả

### 5. Visualization với Apache Superset
- Dashboard tương tác cho các phòng ban
- Báo cáo tự động cập nhật theo thời gian thực

---

## Kiến Trúc Hệ Thống

```
                        NGUON DU LIEU                    STREAMING LAYER
                    +------------------+              +------------------+
                    |   PostgreSQL     |     CDC      |   Apache Kafka   |
                    |   (OLTP Source)  | -----------> |   (Event Queue)  |
                    +------------------+   Debezium   +------------------+
                                                              |
                                                              v
    +-------------------------------------------------------------------------+
    |                          DATA LAKE (MinIO/S3)                           |
    |                                                                         |
    |   +-------------+      +-------------+      +-------------+             |
    |   |   BRONZE    | ---> |   SILVER    | ---> |    GOLD     |             |
    |   | (Raw Data)  |      | (Cleaned)   |      | (Aggregated)|             |
    |   +-------------+      +-------------+      +-------------+             |
    |                                                                         |
    |                    Apache Spark ETL Jobs                                |
    +-------------------------------------------------------------------------+
                                                              |
                                                              v
                                               +------------------+
                                               |   ClickHouse     |
                                               | (Data Warehouse) |
                                               |                  |
                                               | - fact_orders    |
                                               | - dim_customers  |
                                               | - dim_products   |
                                               | - dim_sellers    |
                                               +------------------+
                                                              |
                                                              v
                                               +------------------+
                                               | Apache Superset  |
                                               | (BI Dashboard)   |
                                               +------------------+
```

---

## Công Nghệ Sử Dụng

| Thành phần | Công nghệ | Mục đích |
|------------|-----------|----------|
| Database nguồn | PostgreSQL | Lưu trữ dữ liệu giao dịch OLTP |
| CDC | Debezium | Bắt thay đổi dữ liệu theo thời gian thực |
| Message Queue | Apache Kafka | Xử lý và phân phối sự kiện |
| ETL Engine | Apache Spark | Xử lý dữ liệu batch và streaming |
| Data Lake | MinIO + Delta Lake | Lưu trữ dữ liệu với khả năng versioning |
| Data Warehouse | ClickHouse | Phân tích OLAP với tốc độ cao |
| Visualization | Apache Superset | Tạo dashboard và báo cáo |
| Infrastructure | Docker Compose | Triển khai và quản lý container |

---

## Cấu Trúc Dự Án

```
ecommerce_data_platform/
|
|-- docker-compose.yml          # Cau hinh tat ca services
|-- .env                        # Bien moi truong
|
|-- spark/                      # Cac job ETL
|   |-- streaming_bronze.py     # Kafka -> Bronze (Streaming)
|   |-- batch_silver.py         # Bronze -> Silver (Batch)
|   |-- batch_gold.py           # Silver -> Gold (Batch)
|   |-- load_warehouse.py       # Gold -> ClickHouse
|
|-- scripts/                    # Cac script tien ich
|   |-- load_kaggle_data.py     # Load du lieu ban dau
|   |-- setup_debezium.py       # Cai dat CDC connector
|   |-- data_generator.py       # Gia lap don hang thoi gian thuc
|
|-- clickhouse_init/            # Schema Data Warehouse
|   |-- 01_create_db.sql
|   |-- 02_dim_tables.sql       # Bang dimension
|   |-- 03_fact_tables.sql      # Bang fact
|   |-- 04_materialized_views.sql
|
|-- debezium/                   # Cau hinh CDC
|   |-- connector_config.json
|
|-- data/                       # Dataset (Kaggle Olist)
```

---

## Mo Hinh Du Lieu

### Database Nguon (PostgreSQL - 3NF)

Cac bang giao dich cua he thong e-commerce:
- `customers` - Thong tin khach hang
- `sellers` - Thong tin nguoi ban
- `products` - Danh muc san pham
- `orders` - Don hang
- `order_items` - Chi tiet don hang
- `order_payments` - Thanh toan
- `order_reviews` - Danh gia

### Data Warehouse (ClickHouse - Star Schema)

**Fact Table:**
- `fact_orders` - Chua cac so lieu: tong tien, so luong, phi van chuyen

**Dimension Tables:**
- `dim_customers` - Thong tin khach hang, vi tri dia ly
- `dim_products` - Thong tin san pham, danh muc
- `dim_sellers` - Thong tin nguoi ban
- `dim_time` - Chieu thoi gian (ngay, thang, quy, nam)

---

## Huong Dan Cai Dat

### Yeu Cau He Thong
- Docker va Docker Compose
- Python 3.10+
- Apache Spark 3.5+
- RAM toi thieu 8GB

### Buoc 1: Khoi Dong Services

```bash
# Clone repository
git clone https://github.com/YOUR_USERNAME/realtime-ecommerce-data-pipeline.git
cd realtime-ecommerce-data-pipeline

# Sao chep file cau hinh moi truong
cp .env.example .env

# Sua file .env voi cac thong so cua ban (dac biet la POSTGRES_PASSWORD)
# notepad .env  # Windows
# nano .env     # Linux/Mac

# Khoi dong tat ca services
docker-compose up -d

# Kiem tra trang thai (cho khoang 2 phut)
docker-compose ps
```

### Buoc 2: Load Du Lieu Ban Dau

Tai dataset [Olist E-commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) va dat vao thu muc `data/`.

```bash
# Load du lieu vao PostgreSQL
python scripts/load_kaggle_data.py

# Cai dat Debezium CDC connector
python scripts/setup_debezium.py
```

### Buoc 3: Chay ETL Pipeline

```bash
# Terminal 1: Chay streaming job (CDC -> Bronze)
python spark/streaming_bronze.py

# Terminal 2: Chay batch transformations
python spark/batch_silver.py
python spark/batch_gold.py
python spark/load_warehouse.py
```

### Buoc 4: Truy Cap Dashboard

| Service | URL | Tai khoan |
|---------|-----|-----------|
| Superset Dashboard | http://localhost:8088 | admin / admin |
| Kafka UI | http://localhost:8082 | - |
| MinIO Console | http://localhost:9003 | minioadmin / minioadmin |
| ClickHouse | http://localhost:8124 | default / (khong co mat khau) |

---

## Truy Van Mau

```sql
-- Doanh thu theo thang
SELECT 
    toStartOfMonth(order_date) as thang,
    sum(total_amount) as doanh_thu,
    count(DISTINCT order_id) as so_don_hang
FROM fact_orders
GROUP BY thang
ORDER BY thang;

-- Top 10 danh muc theo doanh thu
SELECT 
    p.category as danh_muc,
    sum(f.total_amount) as doanh_thu,
    count(*) as so_don
FROM fact_orders f
JOIN dim_products p ON f.product_key = p.product_key
GROUP BY danh_muc
ORDER BY doanh_thu DESC
LIMIT 10;
```

---

## Ket Qua Dat Duoc

1. **Xu ly thoi gian thuc**: Du lieu duoc cap nhat trong vong vai giay sau khi co thay doi
2. **Kha nang mo rong**: Kien truc cho phep xu ly hang trieu su kien moi ngay
3. **Du lieu co cau truc**: Medallion architecture dam bao chat luong du lieu qua tung layer
4. **Phan tich nhanh**: ClickHouse cho phep truy van tren hang ty ban ghi trong vai giay
5. **Toan ven du lieu**: CDC dam bao khong mat bat ky thay doi nao

---

## Tai Lieu Tham Khao

- [Debezium Documentation](https://debezium.io/documentation/)
- [Delta Lake Guide](https://docs.delta.io/)
- [ClickHouse Documentation](https://clickhouse.com/docs)
- [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

---

## Giay Phep

Du an nay duoc xay dung voi muc dich hoc tap va portfolio. Dataset su dung tu [Kaggle Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
