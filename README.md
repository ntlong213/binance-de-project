# Crypto Real-Time Data Pipeline

Pipeline dữ liệu streaming thời gian thực, thu thập dữ liệu thị trường tiền điện tử trực tiếp từ Binance, xử lý bằng Apache Spark Structured Streaming, lưu trữ dữ liệu thô vào MinIO (Data Lake), tổng hợp vào ClickHouse (Data Warehouse) và trực quan hóa trên Grafana — toàn bộ chạy local qua Docker Compose.

---

## Kiến trúc hệ thống

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Binance WebSocket                           │
│          wss://stream.binance.com  ·  10 symbols × 3 streams        │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  @trade  @kline_1m  @bookTicker
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Apache Kafka  (KRaft)                          │
│   binance.trades   │   binance.klines   │   binance.bookticker      │
│                    3 partitions mỗi topic                           │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
              ┌─────────────┴─────────────┐
              │   Spark Structured        │
              │   Streaming  (stream.py)  │
              │   trigger: 10 giây        │
              └──────┬────────────────────┘
                     │
          ┌──────────┴──────────┐
          ▼                     ▼
┌──────────────────┐   ┌────────────────────────────────────────────┐
│  MinIO  (S3)     │   │  ClickHouse  (Data Warehouse)              │
│  Data Lake       │   │                                            │
│  /trades         │   │  fact_trades      — giao dịch đã làm sạch  │
│  /klines         │   │  agg_trade_1m     — VWAP, volume/phút      │
│  /bookticker     │   │  agg_ohlcv_1m     — nến OHLCV              │
│  /checkpoints    │   │  agg_spread_1m    — spread bid-ask tb      │
└──────────────────┘   └────────────────┬───────────────────────────┘
          │                             │
          │  Spark Batch (transform.py) │
          └────────────────────────────┘
                                        │
                              ┌─────────▼──────────┐
                              │      Grafana        │
                              │  Dashboard thời     │
                              │  gian thực          │
                              └─────────────────────┘
```

---

## Công nghệ sử dụng

| Tầng | Công nghệ |
|---|---|
| Nguồn dữ liệu | Binance WebSocket API |
| Message Broker | Apache Kafka 3.8 (KRaft — không cần ZooKeeper) |
| Xử lý Streaming | Apache Spark 3.5 Structured Streaming |
| Xử lý Batch | Apache Spark 3.5 (batch transform) |
| Data Lake | MinIO (object storage tương thích S3) |
| Data Warehouse | ClickHouse 24.3 |
| Trực quan hóa | Grafana |
| Triển khai | Docker Compose |
| Ngôn ngữ | Python 3.10+ |

---

## Luồng dữ liệu

10 cặp giao dịch: `BTCUSDT · ETHUSDT · BNBUSDT · SOLUSDT · XRPUSDT · ADAUSDT · DOGEUSDT · TRXUSDT · AVAXUSDT · DOTUSDT`

| Stream | Kafka Topic | Mô tả |
|---|---|---|
| `@trade` | `binance.trades` | Giao dịch thực tế theo thời gian thực (giá, khối lượng, chiều) |
| `@kline_1m` | `binance.klines` | Nến OHLCV 1 phút |
| `@bookTicker` | `binance.bookticker` | Giá và khối lượng bid/ask tốt nhất |

---

## Cấu trúc thư mục

```
PJ3/
├── apps/
│   ├── stream.py           # Spark Streaming job (Kafka → MinIO + ClickHouse)
│   └── transform.py        # Spark batch job (MinIO → ClickHouse, tăng dần)
├── clickhouse/
│   └── init.sql            # Schema ClickHouse (tự chạy khi container khởi động)
├── ingestion/
│   └── binance_ws.py       # WebSocket client Binance → Kafka producer
├── kafka/
│   └── producer.py         # Wrapper Confluent Kafka producer
├── scripts/
│   ├── submit_stream.sh    # Script spark-submit cho stream.py
│   └── submit_transform.sh # Script spark-submit cho transform.py
├── .env                    # Biến môi trường (không commit)
├── docker-compose.yaml     # Toàn bộ stack hạ tầng
├── Makefile                # Các lệnh tắt cho developer
└── requirements.txt        # Thư viện Python
```

---

## Yêu cầu

- [Docker](https://docs.docker.com/get-docker/) + Docker Compose v2
- Python 3.10+ (cho script ingestion, chạy trên máy host)
- `make` (không bắt buộc nhưng khuyến khích)

---

## Hướng dẫn chạy

### 1. Cấu hình môi trường

```bash
cp .env.example .env
# Chỉnh sửa .env nếu cần — giá trị mặc định dùng được ngay cho local dev
```

### 2. Khởi động hạ tầng

```bash
make up
```

Chờ khoảng 30 giây để tất cả service sẵn sàng.

### 3. Tạo Kafka topics

```bash
make topics
```

### 4. Cài đặt thư viện Python

```bash
pip install -r requirements.txt
```

### 5. Chạy ingestion (Binance → Kafka)

```bash
make ingest
```

### 6. Submit Spark Streaming job (Kafka → MinIO + ClickHouse)

```bash
make stream
```

### 7. (Tùy chọn) Chạy batch transform

Dùng để tái xử lý dữ liệu lịch sử đã có trong MinIO:

```bash
make transform
```

---

## Dịch vụ & Cổng kết nối

| Dịch vụ | URL | Thông tin đăng nhập |
|---|---|---|
| Kafka UI | http://localhost:8090 | — |
| Spark Master UI | http://localhost:8080 | — |
| Spark Worker UI | http://localhost:8081 | — |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| ClickHouse HTTP | http://localhost:8123 | `admin` / `admin123` |
| Grafana | http://localhost:3000 | `admin` / `admin` |

---

## Schema ClickHouse

```sql
-- Giao dịch thực tế đã làm sạch — dùng cho truy vấn chi tiết
fact_trades (symbol, event_time DateTime64(3), trade_id UInt64,
             price Float64, quantity Float64, side)
ENGINE = ReplacingMergeTree()
ORDER BY (symbol, event_time, trade_id)

-- VWAP, volume, tỷ lệ mua/bán theo phút — dùng cho biểu đồ Grafana
agg_trade_1m (symbol, time DateTime, volume Float64, vwap Float64,
              trade_count UInt32, buy_volume Float64, sell_volume Float64)
ENGINE = ReplacingMergeTree()
ORDER BY (symbol, time)

-- Nến OHLCV 1 phút từ Binance klines — dùng cho biểu đồ nến
agg_ohlcv_1m (symbol, time DateTime, open, high, low, close, volume Float64)
ENGINE = ReplacingMergeTree()
ORDER BY (symbol, time)

-- Spread bid-ask trung bình theo phút — dùng để theo dõi thanh khoản
agg_spread_1m (symbol, time DateTime, avg_spread Float64)
ENGINE = ReplacingMergeTree()
ORDER BY (symbol, time)
```

Tất cả bảng đều được partition theo `(toDate(time), symbol)` để tối ưu truy vấn theo khoảng thời gian.

---

## Tham chiếu Makefile

```bash
make help            # Liệt kê tất cả lệnh

# Hạ tầng
make up              # Khởi động toàn bộ Docker services
make down            # Dừng services (giữ nguyên volumes)
make restart         # Khởi động lại toàn bộ services
make ps              # Xem trạng thái containers
make logs s=kafka-pj # Xem log của một container cụ thể

# Pipeline
make topics          # Tạo Kafka topics
make ingest          # Chạy Binance WebSocket producer (trên máy host)
make stream          # Submit Spark Streaming job
make transform       # Submit Spark Batch Transform job

# Tiện ích
make status          # Liệt kê Kafka topics + Spark apps đang chạy
make clickhouse-cli  # Mở ClickHouse client tương tác
make clean-volumes   # Xóa toàn bộ volumes — MẤT DỮ LIỆU HOÀN TOÀN
```

---

## Cơ chế hoạt động

### Luồng Streaming (`stream.py`)
Spark đọc đồng thời từ 3 Kafka topics với micro-batch trigger 10 giây. Mỗi hàm `foreachBatch` thực hiện:
1. Ghi dữ liệu thô vào MinIO dạng Parquet (phân vùng theo ngày)
2. Ghi dữ liệu đã làm sạch/tổng hợp vào ClickHouse qua JDBC

`bookTicker` sử dụng timestamp của Kafka message làm `event_time` vì stream này không có trường timestamp riêng.

### Luồng Batch (`transform.py`)
Đọc toàn bộ file Parquet từ MinIO, lọc theo `LAST_PROCESSED_TIME`, loại bỏ trùng lặp, tổng hợp lại và upsert vào ClickHouse. Dùng để backfill hoặc tái xử lý sau khi streaming bị gián đoạn.

### Chống trùng lặp
ClickHouse `ReplacingMergeTree` tự động xử lý các dòng trùng do Spark retry. Spark cũng áp dụng `dropDuplicates()` trong mỗi micro-batch trước khi ghi.

---

## Biến môi trường

| Biến | Mô tả | Mặc định |
|---|---|---|
| `BINANCE_SYMBOLS` | Danh sách cặp giao dịch, phân cách bằng dấu phẩy | `btcusdt,...` (10 cặp) |
| `KLINE_INTERVAL` | Chu kỳ nến | `1m` |
| `KAFKA_MAX_OFFSETS_PER_TRIGGER` | Giới hạn backpressure Kafka | `15000` |
| `S3_LAKE_BUCKET` | Tên bucket MinIO | `crypto-lake` |
| `SPARK_WORKER_MEMORY` | Bộ nhớ cấp cho mỗi Spark worker | `2g` |
| `SPARK_WORKER_CORES` | Số CPU core mỗi Spark worker | `2` |
| `JDBC_BATCH_SIZE` | Số dòng mỗi lần ghi JDBC | `10000` |
| `LAST_PROCESSED_TIME` | Mốc thời gian cho batch transform | `2026-01-01 00:00:00` |
