.PHONY: help up down restart logs \
        topics ingest stream transform \
        ps status clickhouse-cli \
        clean-volumes

# Load .env để dùng các biến trong Makefile
include .env
export

KAFKA_CONTAINER   := kafka-pj
SPARK_CONTAINER   := spark-master-pj

# ══════════════════════════════════════════════════════════════════════════════
# DEFAULT
# ══════════════════════════════════════════════════════════════════════════════

help:
	@echo ""
	@echo "  Infrastructure"
	@echo "    make up              Khởi động toàn bộ stack"
	@echo "    make down            Dừng stack (giữ volume)"
	@echo "    make restart         Restart toàn bộ stack"
	@echo "    make ps              Xem trạng thái containers"
	@echo "    make logs s=<name>   Xem log container (vd: make logs s=kafka-pj)"
	@echo ""
	@echo "  Pipeline"
	@echo "    make topics          Tạo Kafka topics"
	@echo "    make ingest          Chạy Binance WebSocket producer"
	@echo "    make stream          Submit Spark Streaming job"
	@echo "    make transform       Submit Spark Batch Transform job"
	@echo ""
	@echo "  Utilities"
	@echo "    make status          Xem Kafka topics + Spark apps đang chạy"
	@echo "    make clickhouse-cli  Mở ClickHouse client"
	@echo "    make clean-volumes   Xoá toàn bộ volumes (XOÁ DỮ LIỆU)"
	@echo ""

# ══════════════════════════════════════════════════════════════════════════════
# INFRASTRUCTURE
# ══════════════════════════════════════════════════════════════════════════════

up:
	docker compose up -d
	@echo "Stack started. Services:"
	@echo "  Kafka UI   → http://localhost:8090"
	@echo "  Spark UI   → http://localhost:8080"
	@echo "  MinIO      → http://localhost:9001"
	@echo "  ClickHouse → http://localhost:8123"
	@echo "  Grafana    → http://localhost:3000"

down:
	docker compose down

restart:
	docker compose down
	docker compose up -d

ps:
	docker compose ps

logs:
	docker logs -f $(s)

# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

topics:
	@echo "Creating Kafka topics..."
	docker exec $(KAFKA_CONTAINER) /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:9092 --create --if-not-exists \
		--topic $(KAFKA_TOPIC_TRADES) --partitions 3 --replication-factor 1
	docker exec $(KAFKA_CONTAINER) /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:9092 --create --if-not-exists \
		--topic $(KAFKA_TOPIC_KLINES) --partitions 3 --replication-factor 1
	docker exec $(KAFKA_CONTAINER) /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:9092 --create --if-not-exists \
		--topic $(KAFKA_TOPIC_BOOKTICKER) --partitions 3 --replication-factor 1
	@echo "Topics ready."

ingest:
	python ingestion/binance_ws.py

stream:
	bash scripts/submit_stream.sh

transform:
	bash scripts/submit_transform.sh

# ══════════════════════════════════════════════════════════════════════════════
# UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

status:
	@echo "=== Kafka Topics ==="
	docker exec $(KAFKA_CONTAINER) /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:9092 --list
	@echo ""
	@echo "=== Spark Applications ==="
	docker exec $(SPARK_CONTAINER) /opt/spark/bin/spark-submit --status all \
		--master spark://spark-master:7077 2>/dev/null || true

clickhouse-cli:
	docker exec -it clickhouse-pj clickhouse-client \
		--user $(CLICKHOUSE_USER) --password $(CLICKHOUSE_PASSWORD) \
		--database $(CLICKHOUSE_DB)

clean-volumes:
	@echo "WARNING: This will delete ALL data (MinIO, ClickHouse, Grafana)."
	@read -p "Are you sure? [y/N] " ans && [ "$$ans" = "y" ]
	docker compose down -v
	@echo "Volumes deleted."
