#!/bin/bash
# Chạy từ thư mục gốc dự án: bash submit_transform.sh

set -e

# Load biến môi trường từ .env
set -a; source "$(dirname "$0")/../.env"; set +a

CONTAINER="${SPARK_CONTAINER:-spark-master-pj}"
APP="/opt/spark-apps/transform.py"

PACKAGES="org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262,\
com.clickhouse:clickhouse-jdbc:0.4.6"

docker exec \
  -e MINIO_ENDPOINT="$MINIO_ENDPOINT" \
  -e MINIO_ROOT_USER="$MINIO_ROOT_USER" \
  -e MINIO_ROOT_PASSWORD="$MINIO_ROOT_PASSWORD" \
  -e CLICKHOUSE_HOST="$CLICKHOUSE_HOST" \
  -e CLICKHOUSE_PORT="$CLICKHOUSE_PORT" \
  -e CLICKHOUSE_DB="$CLICKHOUSE_DB" \
  -e CLICKHOUSE_USER="$CLICKHOUSE_USER" \
  -e CLICKHOUSE_PASSWORD="$CLICKHOUSE_PASSWORD" \
  -e SPARK_MASTER_URL="spark://spark-master:7077" \
  -e S3_LAKE_BUCKET="$S3_LAKE_BUCKET" \
  -e JDBC_BATCH_SIZE="$JDBC_BATCH_SIZE" \
  -e LOG_LEVEL="WARN" \
  -e LAST_PROCESSED_TIME="$LAST_PROCESSED_TIME" \
  "$CONTAINER" \
  /opt/spark/bin/spark-submit \
    --master "spark://spark-master:7077" \
    --deploy-mode client \
    --packages "$PACKAGES" \
    --conf "spark.jars.ivy=/tmp/.ivy" \
    --conf "spark.sql.shuffle.partitions=4" \
    --conf "spark.hadoop.fs.s3a.endpoint=$MINIO_ENDPOINT" \
    --conf "spark.hadoop.fs.s3a.access.key=$MINIO_ROOT_USER" \
    --conf "spark.hadoop.fs.s3a.secret.key=$MINIO_ROOT_PASSWORD" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
    --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
    "$APP"
