# Crypto Real-Time Data Pipeline

**Data Engineering Pipeline** — Real-time cryptocurrency market data ingestion, transformation, and analytics on a modern data stack.

Captures live trade, kline (OHLCV), and book ticker data from Binance WebSocket API, streams through Apache Kafka, processes with Apache Spark Structured Streaming, persists raw data to MinIO (data lake), aggregates to ClickHouse (OLAP warehouse), and exposes metrics via Grafana dashboards.

**Stack:** Binance WebSocket → Kafka (KRaft) → Spark 3.5 → MinIO + ClickHouse → Grafana | Deployed on Docker Compose.

---

## Architecture
<img width="1123" height="436" alt="Screenshot 2026-04-28 155308" src="https://github.com/user-attachments/assets/707231dd-c52d-4806-8ccb-5bde8048724d" />

### System Design

```
Source (Binance WS)
        ↓
        ├─ @trade       → Executed transactions (price, qty, side)
        ├─ @kline_1m    → 1m candlestick OHLCV
        └─ @bookTicker  → Best bid/ask prices & liquidity
        ↓
   Kafka Topics (KRaft)
        ├─ binance.trades       (3 partitions)
        ├─ binance.klines       (3 partitions)
        └─ binance.bookticker   (3 partitions)
        ↓
   Spark Structured Streaming
   (10s micro-batch trigger)
        ↓
        ├─ Raw data       → MinIO (S3 compatible)
        │  ├─ /trades     (Parquet, partitioned by date+symbol)
        │  ├─ /klines     (Parquet, partitioned by date+symbol)
        │  └─ /bookticker (Parquet, partitioned by date+symbol)
        │
        └─ Aggregations  → ClickHouse (OLAP)
           ├─ fact_trades        (cleaned transactions)
           ├─ agg_trade_1m       (VWAP, volume, buy/sell)
           ├─ agg_ohlcv_1m       (candlestick OHLCV)
           └─ agg_spread_1m      (bid-ask spread)
           ↓
        Spark Batch Transform
        (optional: historical reprocessing)
           ↓
        Grafana Dashboards
```

### Data Sources

| Stream | Topic | Frequency | Fields |
|--------|-------|-----------|--------|
| **@trade** | `binance.trades` | Per transaction | `symbol, event_time, trade_id, price, qty, side` |
| **@kline_1m** | `binance.klines` | Per minute | `symbol, open_time, close_time, open, high, low, close, volume, trade_count, is_closed` |
| **@bookTicker** | `binance.bookticker` | Per update | `symbol, bid_price, bid_qty, ask_price, ask_qty, update_id` |

**Supported symbols (10):** `BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, XRPUSDT, ADAUSDT, DOGEUSDT, TRXUSDT, AVAXUSDT, DOTUSDT`

---

## Technology Stack

| Layer | Technology | Version | Role |
|-------|-----------|---------|------|
| **Data Source** | Binance WebSocket API | — | Market data feed |
| **Message Queue** | Apache Kafka | 3.8.0 | Event streaming (KRaft mode, no ZooKeeper) |
| **Stream Processing** | Apache Spark | 3.5.0 | Micro-batch ETL, transformations |
| **Data Lake** | MinIO | Latest | S3-compatible object storage (raw Parquet) |
| **Data Warehouse** | ClickHouse | 24.3 | OLAP for analytics & aggregations |
| **BI/Visualization** | Grafana | Latest | Real-time dashboards |
| **Orchestration** | Docker Compose | v2 | Local dev environment |
| **Language** | Python | 3.10+ | Application code |

---

## ClickHouse Schema

### Fact Table: `fact_trades`
Cleaned and deduplicated individual trade records. Primary source for drill-down analysis.

```sql
CREATE TABLE IF NOT EXISTS crypto.fact_trades (
    symbol      LowCardinality(String),
    event_time  DateTime64(3),
    trade_id    UInt64,
    price       Float64,
    quantity    Float64,
    side        LowCardinality(String)  -- 'BUY' or 'SELL'
)
ENGINE = ReplacingMergeTree()
PARTITION BY (toDate(event_time), symbol)
ORDER BY (symbol, event_time, trade_id);
```

**Partitioning strategy:** Date + symbol enables efficient time-range & symbol-specific queries.

### Aggregate Table: `agg_trade_1m`
1-minute trade aggregates: VWAP, volume, trade count, buy/sell split. Used for Grafana line/bar charts.

```sql
CREATE TABLE IF NOT EXISTS crypto.agg_trade_1m (
    symbol      LowCardinality(String),
    time        DateTime,
    volume      Float64,
    vwap        Float64,
    trade_count UInt32,
    buy_volume  Float64,
    sell_volume Float64
)
ENGINE = ReplacingMergeTree()
PARTITION BY (toDate(time), symbol)
ORDER BY (symbol, time);
```

### Aggregate Table: `agg_ohlcv_1m`
1-minute candlesticks from Binance kline stream. Used for candlestick charts.

```sql
CREATE TABLE IF NOT EXISTS crypto.agg_ohlcv_1m (
    symbol  LowCardinality(String),
    time    DateTime,
    open    Float64,
    high    Float64,
    low     Float64,
    close   Float64,
    volume  Float64
)
ENGINE = ReplacingMergeTree()
PARTITION BY (toDate(time), symbol)
ORDER BY (symbol, time);
```

### Aggregate Table: `agg_spread_1m`
1-minute average bid-ask spread. Used for liquidity monitoring.

```sql
CREATE TABLE IF NOT EXISTS crypto.agg_spread_1m (
    symbol     LowCardinality(String),
    time       DateTime,
    avg_spread Float64
)
ENGINE = ReplacingMergeTree()
PARTITION BY (toDate(time), symbol)
ORDER BY (symbol, time);
```

**Design rationale:**
- **ReplacingMergeTree:** Handles duplicate writes from Spark retries.
- **Partition by date + symbol:** Prunes partitions for time-range queries; symbol cardinality is low (10).
- **LowCardinality:** Reduces memory footprint for string columns with low distinct values.
- **DateTime vs DateTime64(3):** `fact_trades` uses millisecond precision; aggregates use second precision (sufficient).

---

## Project Structure

```
PJ3/
├── apps/
│   ├── stream.py              # Spark Structured Streaming job
│   │   ├─ Reads: Kafka (3 topics, 10s trigger)
│   │   ├─ Writes: MinIO (raw Parquet)
│   │   └─ Writes: ClickHouse (fact + aggregates)
│   │
│   └── transform.py           # Spark Batch Transform job
│       ├─ Reads: MinIO (all Parquet files)
│       ├─ Filter by LAST_PROCESSED_TIME
│       ├─ Deduplicate
│       └─ Upsert: ClickHouse aggregates
│
├── ingestion/
│   └── binance_ws.py          # WebSocket client
│       ├─ Connect: Binance WSS
│       ├─ Subscribe: @trade, @kline_1m, @bookTicker
│       ├─ Parse: JSON → Python dicts
│       └─ Produce: Kafka topics
│
├── kafka/
│   └── producer.py            # Confluent Kafka producer wrapper
│
├── clickhouse/
│   └── init.sql               # DDL for all tables (auto-run on startup)
│
├── scripts/
│   ├── submit_stream.sh       # spark-submit command for stream.py
│   └── submit_transform.sh    # spark-submit command for transform.py
│
├── docs/
│   └── dashboard-*.json       # Grafana dashboard JSON
│
├── .env                       # Environment variables (git-ignored)
├── .env.example               # Template for .env
├── docker-compose.yaml        # Full stack definition
├── Makefile                   # Command shortcuts
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

---

## Getting Started

### Prerequisites

- **Docker Desktop** (with Docker Compose v2)
- **Python 3.10+** (for ingestion script, runs on host)
- **make** (optional, for command shortcuts)
- **4+ GB free RAM** (recommended for comfortable development)

### 1. Clone & Configure

```bash
git clone <repo-url>
cd PJ3

cp .env.example .env
# Edit .env if needed; defaults work for local development
```

### 2. Start Infrastructure

```bash
make up
```

Services will be ready in ~30 seconds. Verify:

```bash
make ps
```

Expected output:

```
CONTAINER ID   STATUS              NAMES
abc123...      Up 30s              kafka-pj
def456...      Up 28s              kafka-ui-pj
ghi789...      Up 25s              spark-master-pj
jkl012...      Up 24s              spark-worker-pj
mno345...      Up 22s              minio-pj
pqr678...      Up 20s              clickhouse-pj
stu901...      Up 18s              grafana-pj
```

### 3. Create Kafka Topics

```bash
make topics
```

Verify in Kafka UI: http://localhost:8090

### 4. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 5. Start Ingestion (WebSocket → Kafka)

```bash
make ingest
```

Expected output:

```
Subscribing 10 symbols × 3 stream types = 30 streams

Connected.
[HH:MM:SS] BTCUSDT     | BUY  | Price:    98,567.1234 | Qty:      0.523456
[HH:MM:SS] ETHUSDT     | SELL | Price:     3,456.5678 | Qty:      2.345678
...
```

### 6. Submit Spark Streaming Job

In a new terminal:

```bash
make stream
```

Monitor job:
- **Spark UI:** http://localhost:8080
- **Worker UI:** http://localhost:8081

### 7. (Optional) Submit Batch Transform

To reprocess historical data in MinIO:

```bash
make transform
```

---

## Operations & Monitoring

### Service Access

| Service | URL | Credentials |
|---------|-----|-------------|
| **Kafka UI** | http://localhost:8090 | None |
| **Spark Master** | http://localhost:8080 | None |
| **Spark Worker** | http://localhost:8081 | None |
| **MinIO Console** | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| **ClickHouse HTTP** | http://localhost:8123 | `admin` / `admin123` |
| **Grafana** | http://localhost:3000 | `admin` / `admin` |

### Monitoring Commands

**View logs for a service:**

```bash
make logs s=kafka-pj        # Kafka
make logs s=spark-master-pj # Spark Master
make logs s=spark-worker-pj # Spark Worker
```

**Kafka topic status:**

```bash
docker exec kafka-pj /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --list

docker exec kafka-pj /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --list
```

**ClickHouse SQL CLI:**

```bash
make clickhouse-cli
```

Then query (e.g.):

```sql
SELECT symbol, COUNT(*) as trade_count, 
       AVG(price) as avg_price 
FROM fact_trades 
WHERE symbol = 'BTCUSDT' AND toDate(event_time) = today()
GROUP BY symbol;

SELECT * FROM agg_trade_1m WHERE symbol = 'ETHUSDT' ORDER BY time DESC LIMIT 10;
```

**Spark job status:**

```bash
curl -s http://localhost:8080/api/v1/applications | jq '.'
```

---

## Data Flow & Processing Logic

### Streaming Pipeline (`stream.py`)

1. **Read Kafka:** Consume from 3 topics concurrently with 10-second micro-batch trigger.
2. **Parse JSON:** Deserialize Kafka message values into structured DataFrames.
3. **Timestamp handling:**
   - `@trade` and `@kline`: Use `event_time` from Binance message.
   - `@bookTicker`: Use Kafka message timestamp (lacks native `event_time`).
4. **Write to MinIO:**
   - Raw (unparsed) data as Parquet files.
   - Partitioned by `toDate(event_time)` and `symbol`.
   - Enables historical backfill and data recovery.
5. **Compute aggregates in-stream:**
   - `fact_trades`: Clean trade records (keep all fields).
   - `agg_trade_1m`: 1m window → VWAP, volume, trade count, buy/sell split.
   - `agg_ohlcv_1m`: 1m window → OHLCV from klines.
   - `agg_spread_1m`: 1m window → average spread from bookticker.
6. **Write to ClickHouse:** Insert via JDBC with batch size = 10,000 rows.
7. **Deduplication:** Spark applies `dropDuplicates()` per micro-batch; ClickHouse handles retries via `ReplacingMergeTree`.

### Batch Transform Pipeline (`transform.py`)

Used for:
- Backfilling historical data after upstream outages.
- Reprocessing with corrected logic.
- Catching up on accumulated Parquet files from MinIO.

**Process:**

1. Read all Parquet from MinIO (`/trades`, `/klines`, `/bookticker`).
2. Filter by `LAST_PROCESSED_TIME` (env var).
3. Apply same transformations as streaming (clean, aggregate, deduplicate).
4. Upsert into ClickHouse with `mode="append"`.
5. `ReplacingMergeTree` automatically resolves duplicates.

### Deduplication Strategy

**Why duplicates occur:**
- Spark job crash/retry during Kafka offset commit → messages reprocessed.
- Network hiccups during JDBC write → partial batches reinserted.

**Mitigation:**
- **Spark:** `dropDuplicates(["symbol", "trade_id"])` before each write.
- **ClickHouse:** `ReplacingMergeTree` with `(symbol, event_time, trade_id)` as ORDER KEY.
- **Merge trigger:** Run `OPTIMIZE TABLE fact_trades;` periodically or via cron.

---

## Configuration & Tuning

### Environment Variables

```bash
# Data Source
BINANCE_SYMBOLS="btcusdt,ethusdt,bnbusdt,solusdt,xrpusdt,adausdt,dogeusdt,trxusdt,avaxusdt,dotusdt"
KLINE_INTERVAL="1m"
RECONNECT_DELAY_SECONDS=3

# Kafka
KAFKA_BROKERS="kafka:9092"
KAFKA_TOPIC_TRADES="binance.trades"
KAFKA_TOPIC_KLINES="binance.klines"
KAFKA_TOPIC_BOOKTICKER="binance.bookticker"
KAFKA_MAX_OFFSETS_PER_TRIGGER=15000

# MinIO (S3)
MINIO_ENDPOINT="http://minio:9000"
MINIO_ROOT_USER="minioadmin"
MINIO_ROOT_PASSWORD="minioadmin123"
S3_LAKE_BUCKET="crypto-lake"

# Spark
SPARK_WORKER_MEMORY="2g"
SPARK_WORKER_CORES=2

# ClickHouse
CLICKHOUSE_HOST="clickhouse"
CLICKHOUSE_PORT=9000
CLICKHOUSE_DB="crypto"
CLICKHOUSE_USER="admin"
CLICKHOUSE_PASSWORD="admin123"
JDBC_BATCH_SIZE=10000

# Batch Transform
LAST_PROCESSED_TIME="2026-01-01 00:00:00"
```

### Performance Tuning

**Spark Streaming:**

- **Batch interval:** Increase from 10s → 30s to reduce overhead (trade latency for throughput).
- **Partition count:** Adjust `KAFKA_MAX_OFFSETS_PER_TRIGGER` based on data volume & memory.
- **Shuffle partitions:** Lower `spark.sql.shuffle.partitions` (4 works for 10 symbols).

**ClickHouse:**

- **Merge throttle:** Tune `max_parts_in_total` to control table parts.
- **TTL policies:** Add to auto-delete old data (e.g., 30 days retention).
- **Compression:** Use `lz4` codec for Parquet export.

**MinIO:**

- **Block size:** Increase upload chunk size for better throughput.
- **Replication:** Not needed in local dev; enable in production.

---

## Troubleshooting

### Kafka Producer Hangs

**Symptom:** Ingestion stalls, no data flowing to Kafka.

**Debug:**

```bash
docker exec kafka-pj /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic binance.trades \
  --from-beginning \
  --max-messages 10
```

**Fix:**
- Verify network: `docker exec ingestion-pj ping kafka`
- Check Binance WebSocket: Are events being received?
- Increase producer timeout: `socket.timeout.ms` in `kafka/producer.py`.

### Spark Job Fails with "Task Timeout"

**Symptom:** Spark driver shows "Task deserialization took X seconds" warnings.

**Fix:**
- Increase Spark task timeout: Add to docker-compose.yaml:
  ```yaml
  environment:
    SPARK_DRIVER_JAVA_OPTIONS: "-Dspark.task.maxFailures=4"
  ```
- Reduce batch size: Lower `JDBC_BATCH_SIZE` from 10,000 → 5,000.

### ClickHouse JDBC Connection Refused

**Symptom:** Spark logs: "Connection refused: clickhouse:9000"

**Fix:**
- Verify ClickHouse is running: `make ps | grep clickhouse`
- Wait longer for startup: `docker logs clickhouse-pj | tail -20`
- Check JDBC version compatibility (should be 0.4.6+).

### MinIO Bucket Not Found

**Symptom:** Spark logs: "NoSuchBucket: The specified bucket does not exist"

**Fix:**
- Create bucket via MinIO UI: http://localhost:9001
  - Login with `minioadmin` / `minioadmin123`
  - Create bucket: `crypto-lake`
- Or via CLI:
  ```bash
  docker exec minio-pj mc mb minio/crypto-lake
  ```

### Data Missing in ClickHouse After Stream Job Starts

**Symptom:** `fact_trades` is empty; klines/trades in Kafka are flowing.

**Debug:**
1. Check Spark driver logs:
   ```bash
   make logs s=spark-master-pj | grep ERROR
   ```
2. Verify JDBC URL:
   ```bash
   docker exec spark-master-pj ps aux | grep stream.py
   ```
3. Test ClickHouse connectivity:
   ```bash
   docker exec spark-master-pj telnet clickhouse 9000
   ```

---

## Scaling & Production Considerations

### Horizontal Scaling

**Kafka:**
- Increase partitions per topic: 3 → 6+ (currently 3).
- Add consumer group coordination.

**Spark:**
- Add workers: `spark-worker-2`, `spark-worker-3` in docker-compose.yaml.
- Set `spark.cores.max` & memory per executor.

**ClickHouse:**
- Enable replication (ClickHouse Keeper).
- Partition by date + symbol + hash (for large volumes).

### Data Retention

Add TTL policies to ClickHouse:

```sql
ALTER TABLE crypto.fact_trades 
MODIFY TTL toDateTime(event_time) + INTERVAL 30 DAY;

OPTIMIZE TABLE crypto.fact_trades FINAL;
```

### Backup Strategy

**MinIO (Parquet lake):**

```bash
aws s3 sync s3://crypto-lake s3://backup-bucket/crypto-lake \
  --endpoint-url http://minio:9000
```

**ClickHouse (tables):**

```bash
clickhouse-client --query "BACKUP DATABASE crypto TO 'S3(s3://backup-bucket/backups)'"
```

---

## Common Queries

**Real-time VWAP (last 10 minutes):**

```sql
SELECT 
    symbol,
    time,
    vwap,
    volume,
    trade_count
FROM crypto.agg_trade_1m
WHERE symbol = 'BTCUSDT' 
  AND time >= now() - INTERVAL 10 MINUTE
ORDER BY time DESC;
```

**Daily summary:**

```sql
SELECT 
    symbol,
    toDate(event_time) as day,
    COUNT(*) as trade_count,
    MIN(price) as min_price,
    MAX(price) as max_price,
    AVG(price) as avg_price,
    SUM(quantity) as total_quantity
FROM crypto.fact_trades
WHERE toDate(event_time) = today()
GROUP BY symbol, day
ORDER BY symbol;
```

**Buy/sell imbalance:**

```sql
SELECT 
    symbol,
    time,
    buy_volume,
    sell_volume,
    (buy_volume - sell_volume) / (buy_volume + sell_volume) as imbalance_ratio
FROM crypto.agg_trade_1m
WHERE symbol IN ('BTCUSDT', 'ETHUSDT')
  AND time >= now() - INTERVAL 1 HOUR
ORDER BY time DESC;
```

---

## Make Commands Reference

```bash
# Infrastructure
make help                # Show all available commands
make up                  # Start all Docker services
make down                # Stop services (preserves volumes)
make restart             # Restart all services
make ps                  # Show container status
make logs s=kafka-pj     # View logs (replace service name as needed)

# Pipeline
make topics              # Create Kafka topics
make ingest              # Start Binance WebSocket producer
make stream              # Submit Spark Streaming job
make transform           # Submit Spark Batch Transform job

# Utilities
make status              # Show Kafka topics & Spark applications
make clickhouse-cli      # Open ClickHouse interactive CLI
make clean-volumes       # DELETE all volumes (PERMANENT DATA LOSS)
```

---

## Development Workflow

### Adding a New Symbol

1. Update `.env`:
   ```bash
   BINANCE_SYMBOLS="btcusdt,...,<NEW_SYMBOL>"
   ```

2. Restart ingestion:
   ```bash
   # Kill ingestion (Ctrl+C)
   make ingest
   ```

3. Data flows immediately into existing Kafka topics.

### Modifying Stream Logic

1. Edit `apps/stream.py` or `apps/transform.py`.
2. Stop job: Kill Spark application via UI or:
   ```bash
   docker exec spark-master-pj curl -X POST http://localhost:8080/api/v1/submissions/kill/<driver-id>
   ```
3. Resubmit:
   ```bash
   make stream
   ```

### Adding a New Aggregate Table

1. Add DDL to `clickhouse/init.sql`.
2. Add compute logic in `apps/stream.py` (`foreachBatch` function).
3. Restart stream job.

---

## Maintenance

### Daily Checks

```bash
# Verify data freshness
docker exec clickhouse-pj clickhouse-client --query \
  "SELECT symbol, MAX(event_time) as latest FROM crypto.fact_trades GROUP BY symbol"

# Check Spark job status
curl -s http://localhost:8080/api/v1/applications | jq '.[] | {id, status}'
```

### Weekly Tasks

```bash
# Optimize ClickHouse tables
docker exec clickhouse-pj clickhouse-client --query "OPTIMIZE TABLE crypto.fact_trades FINAL"

# Check MinIO bucket size
docker exec minio-pj mc du minio/crypto-lake

# Restart all services for clean state
make restart
```

### Before Production Deployment

- [ ] Increase `SPARK_WORKER_MEMORY` to 4g+ (based on data volume).
- [ ] Enable ClickHouse replication & backup.
- [ ] Configure persistent MinIO storage.
- [ ] Set up Grafana alerts.
- [ ] Document runbooks for failure scenarios.
- [ ] Load test: verify throughput with production data volume.

---

## License

MIT

---

## Support & Documentation

- **Binance API:** https://developers.binance.com/docs/spot-api
- **Apache Kafka:** https://kafka.apache.org/documentation/
- **Apache Spark:** https://spark.apache.org/docs/
- **ClickHouse:** https://clickhouse.com/docs
- **Grafana:** https://grafana.com/docs/
