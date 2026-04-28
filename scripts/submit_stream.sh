#!/bin/bash
# Chạy từ thư mục gốc dự án: bash submit.sh

set -e

# Load biến môi trường từ .env
set -a; source "$(dirname "$0")/../.env"; set +a

CONTAINER="${SPARK_CONTAINER:-spark-master-pj}"
APP="/opt/spark-apps/stream.py"

PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262,\
com.clickhouse:clickhouse-jdbc:0.4.6"

docker exec \
  -e MINIO_ENDPOINT="$MINIO_ENDPOINT" \
  -e MINIO_ROOT_USER="$MINIO_ROOT_USER" \
  -e MINIO_ROOT_PASSWORD="$MINIO_ROOT_PASSWORD" \
  -e KAFKA_BROKERS="kafka:9092" \
  -e SPARK_MASTER_URL="spark://spark-master:7077" \
  -e S3_LAKE_BUCKET="$S3_LAKE_BUCKET" \
  -e KAFKA_TOPIC_TRADES="$KAFKA_TOPIC_TRADES" \
  -e KAFKA_TOPIC_KLINES="$KAFKA_TOPIC_KLINES" \
  -e KAFKA_TOPIC_BOOKTICKER="$KAFKA_TOPIC_BOOKTICKER" \
  -e KAFKA_MAX_OFFSETS_PER_TRIGGER="$KAFKA_MAX_OFFSETS_PER_TRIGGER" \
  -e LOG_LEVEL="WARN" \
  -e CLICKHOUSE_HOST="$CLICKHOUSE_HOST" \
  -e CLICKHOUSE_PORT="$CLICKHOUSE_PORT" \
  -e CLICKHOUSE_DB="$CLICKHOUSE_DB" \
  -e CLICKHOUSE_USER="$CLICKHOUSE_USER" \
  -e CLICKHOUSE_PASSWORD="$CLICKHOUSE_PASSWORD" \
  "$CONTAINER" \
  /opt/spark/bin/spark-submit \
    --master "spark://spark-master:7077" \
    --deploy-mode client \
    --packages "$PACKAGES" \
    --conf "spark.jars.ivy=/tmp/.ivy" \
    --conf "spark.sql.shuffle.partitions=4" \
    --conf "spark.sql.streaming.checkpointLocation=s3a://${S3_LAKE_BUCKET}/checkpoints" \
    --conf "spark.hadoop.fs.s3a.endpoint=$MINIO_ENDPOINT" \
    --conf "spark.hadoop.fs.s3a.access.key=$MINIO_ROOT_USER" \
    --conf "spark.hadoop.fs.s3a.secret.key=$MINIO_ROOT_PASSWORD" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
    --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
    "$APP"
