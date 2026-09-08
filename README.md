# 🚀 Data Lakehouse Architecture & Time Travel Demo

> **Kiến trúc Data Lakehouse hiện đại kết hợp Apache Iceberg, Parquet Storage, ETL Pipelines và StarRocks OLAP Query Engine**

Dự án này là một bộ **Demo hoàn chỉnh** minh họa và đo đạc các ưu điểm của mô hình **Data Lakehouse** với Apache Iceberg, bao gồm khả năng **Time Travel**, **ACID transactions**, và **Schema Evolution**.

## 🐳 Docker Services

Hệ thống bao gồm 2 services chính:

### 1. Apache Iceberg REST Catalog
```bash
docker-compose up -d iceberg-catalog
```
- **Port**: `8181`
- **Backend**: SQLite
- **Warehouse**: `./storage/silver`

### 2. StarRocks All-in-One (Optional - cho advanced query)
```bash
docker-compose up -d starrocks
```
- **MySQL Protocol Port**: `9030` 
- **Web Console**: `8030`
- **User**: `root` / **Password**: *(trống)*

> **Lưu ý**: Các benchmark chính sử dụng **DuckDB** và **PyIceberg** nên không bắt buộc phải chạy StarRocks.

---

## 📌 Các Tính Năng Chính

Dự án demo 3 khả năng cốt lõi của Data Lakehouse:

### 1. ⚡ Hiệu Năng Query (Query Performance Benchmark)
So sánh tốc độ truy vấn aggregation trên **100,000 bản ghi**:
- **CSV Raw** (traditional approach)
- **Parquet Columnar** (DuckDB engine)
- **Pandas DataFrame** (in-memory processing)
- **StarRocks OLAP** (nếu có container chạy)

### 2. 🔄 Khả Năng Tương Thích ETL (Multi-Engine Compatibility)
Chứng minh tính mở của Apache Iceberg - cùng một bộ dữ liệu có thể được đọc bởi:
- **PyIceberg** - Native Iceberg Python client
- **DuckDB** - Embedded OLAP engine
- **Polars** - Rust-based DataFrame library
- **PyArrow** - Columnar in-memory format

### 3. ⏳ Time Travel & Snapshot Management
Minh họa khả năng **Time Travel** của Apache Iceberg:
- **Snapshot 1**: Initial ETL ingestion (chỉ lấy orders có status='COMPLETED')
- **Snapshot 2**: CDC update (giảm 50% giá cho Electronics ở Hà Nội)
- Truy vấn dữ liệu tại bất kỳ snapshot nào mà không cần backup
- Các operations: **UPDATE**, **DELETE**, **ROLLBACK**

---

## 📐 Kiến Trúc Hệ Thống

```text
┌─────────────────────────────────────────────────────────┐
│              SOURCE DATA (E-commerce Orders)            │
│                   100,000 transactions                  │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼ (generate_data.py)
┌─────────────────────────────────────────────────────────┐
│                    BRONZE LAYER                         │
│          Raw Parquet Files + CSV (for benchmark)        │
│              storage/bronze/, storage/raw_csv/          │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼ (etl_pipeline.py)
┌─────────────────────────────────────────────────────────┐
│                    SILVER LAYER                         │
│              Apache Iceberg Tables                      │
│      Snapshots + Metadata + Time Travel Support         │
│                  storage/silver/                        │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│               QUERY ENGINES (Multi-Engine)              │
│    DuckDB | PyIceberg | Polars | PyArrow | StarRocks   │
└─────────────────────────────────────────────────────────┘
```

---

## 🛠️ Cấu Trúc Thư Mục Dự Án

```text
demo/
├── storage/                      # Data storage layers
│   ├── raw_csv/                  # CSV files cho benchmark
│   │   └── sales_raw.csv
│   ├── bronze/                   # Bronze layer (Parquet)
│   │   └── bronze_sales.parquet
│   └── silver/                   # Silver layer (Iceberg tables)
│       ├── catalog.db            # SQLite catalog
│       └── default/              # Iceberg table metadata & data
│
├── generate_data.py              # Sinh dữ liệu giả lập 100k records
├── etl_pipeline.py               # ETL pipeline: Bronze → Silver + Snapshots
├── benchmark_time_travel.py      # Benchmark: Performance + Compatibility + Time Travel
│
├── test_iceberg_update.py        # Test: UPDATE một record + Time Travel
├── test_iceberg_delete.py        # Test: DELETE records + Time Travel
├── test_iceberg_rollback.py      # Test: ROLLBACK về snapshot cũ
│
├── docker-compose.yml            # Docker services (Iceberg REST + StarRocks)
├── requirements.txt              # Python dependencies
└── README.md                     # Documentation
```

---

## 🚀 Hướng Dẫn Sử Dụng

### Bước 1: Cài Đặt Dependencies

**Yêu cầu**: Python 3.10+

```bash
pip install -r requirements.txt
```

### Bước 2: Sinh Dữ Liệu & Chạy ETL Pipeline

Script này sẽ:
- Sinh 100,000 bản ghi e-commerce orders
- Tạo Bronze layer (Parquet + CSV)
- Chạy ETL tạo Silver layer với 2 Iceberg snapshots

```bash
python etl_pipeline.py
```

**Output mong đợi**:
```
[INFO] Generating 100000 synthetic e-commerce records...
[OK] Generated raw CSV (X.XX MB)
[OK] Generated Bronze Parquet (Y.YY MB)
[ETL Engine] Starting Bronze to Silver Transformation...
[OK] Iceberg Snapshot 1 Created! ID: XXXXXXXXXX | Records: ~90000
[ETL Engine] Applying CDC Update/Upsert Data...
[OK] Iceberg Snapshot 2 Created! ID: YYYYYYYYYY | Total Snapshots: 2
```

### Bước 3: Chạy Benchmark & Time Travel Tests

```bash
python benchmark_time_travel.py
```

Kết quả sẽ hiển thị:
1. **Query Performance**: So sánh tốc độ CSV vs Parquet vs Pandas
2. **ETL Compatibility**: Test đọc data bằng PyIceberg, DuckDB, Polars, PyArrow
3. **Time Travel**: Hiển thị dữ liệu khác nhau giữa 2 snapshots

### Bước 4: Test Các Operations Iceberg

#### Test UPDATE + Time Travel
```bash
python test_iceberg_update.py
```
Cập nhật 1 record và kiểm tra dữ liệu ở snapshot cũ vs mới.

#### Test DELETE + Time Travel
```bash
python test_iceberg_delete.py
```
Xóa records và xem dữ liệu vẫn còn ở snapshot cũ.

#### Test ROLLBACK
```bash
python test_iceberg_rollback.py
```
Rollback về snapshot trước đó (undo changes).

---

## 📊 Kết Quả Benchmark Mẫu

### Query Performance (100k records)

| Engine | Thời Gian | Speedup |
|--------|-----------|---------|
| CSV (DuckDB) | ~45ms | 1.0x |
| **Parquet (DuckDB)** | **~8ms** | **5.6x** |
| Pandas | ~120ms | 0.4x |
| StarRocks OLAP | ~5ms | 9.0x |

### ETL Compatibility

✅ **PyIceberg** - Native Iceberg support  
✅ **DuckDB** - Parquet + Iceberg metadata  
✅ **Polars** - Zero-copy Arrow format  
✅ **PyArrow** - Columnar in-memory  

### Time Travel Demo

**Snapshot 1** (Initial): 89,547 records  
**Snapshot 2** (After CDC): 89,547 records, nhưng Electronics ở Hà Nội giảm 50% giá

---

## 🐳 (Optional) Chạy với Docker

Nếu muốn test với StarRocks hoặc Iceberg REST Catalog:

```bash
# Chạy tất cả services
docker-compose up -d

# Kiểm tra status
docker-compose ps

# Xem logs
docker-compose logs -f starrocks

# Dừng services
docker-compose down
```

**Kết nối StarRocks** (sau khi container chạy):
```python
import pymysql
conn = pymysql.connect(host='127.0.0.1', port=9030, user='root', password='')
```

---

## 💡 Các Tính Năng Nổi Bật của Apache Iceberg

✨ **Time Travel**: Truy vấn dữ liệu tại bất kỳ snapshot nào  
✨ **ACID Transactions**: Đảm bảo consistency khi concurrent writes  
✨ **Schema Evolution**: Thêm/xóa/đổi tên columns không cần rewrite data  
✨ **Partition Evolution**: Thay đổi partition strategy mà không di chuyển data  
✨ **Hidden Partitioning**: Tự động partition, user không cần chỉ định trong query  
✨ **Multi-Engine Support**: DuckDB, Spark, Flink, Trino, StarRocks, etc.

---

## 📚 Tài Liệu Tham Khảo

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [PyIceberg Python Library](https://py.iceberg.apache.org/)
- [DuckDB Iceberg Extension](https://duckdb.org/docs/extensions/iceberg.html)
- [StarRocks Documentation](https://docs.starrocks.io/)

---

## 📜 License

Dự án demo phục vụ mục đích học tập và nghiên cứu Data Lakehouse Architecture.

#   s t a r r o c k s - i c e b e r g  
 