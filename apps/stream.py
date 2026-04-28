import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_timestamp, date_trunc,
    avg, count, sum as _sum, when, lit,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, BooleanType, LongType,
)

# ── ENV ───────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT")
MINIO_USER       = os.getenv("MINIO_ROOT_USER")
MINIO_PASSWORD   = os.getenv("MINIO_ROOT_PASSWORD")
S3_LAKE_BUCKET   = os.getenv("S3_LAKE_BUCKET")
TOPIC_TRADES     = os.getenv("KAFKA_TOPIC_TRADES")
TOPIC_KLINES     = os.getenv("KAFKA_TOPIC_KLINES")
TOPIC_BOOKTICKER = os.getenv("KAFKA_TOPIC_BOOKTICKER")
MAX_OFFSETS      = os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER")
CLICKHOUSE_HOST  = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT  = os.getenv("CLICKHOUSE_PORT")
CLICKHOUSE_DB    = os.getenv("CLICKHOUSE_DB")
CLICKHOUSE_USER  = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASS  = os.getenv("CLICKHOUSE_PASSWORD")

JDBC_URL    = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}"
JDBC_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"
S3_BASE     = f"s3a://{S3_LAKE_BUCKET}"
S3_CKP      = f"{S3_BASE}/checkpoints"

# ── Spark Session ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("BinanceCryptoStream")
    .master("spark://spark-master:7077")
    .config("spark.jars.ivy", "/tmp/.ivy")
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            "com.clickhouse:clickhouse-jdbc:0.4.6",
        ]),
    )
    .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key",             MINIO_USER)
    .config("spark.hadoop.fs.s3a.secret.key",             MINIO_PASSWORD)
    .config("spark.hadoop.fs.s3a.path.style.access",      "true")
    .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .config("spark.sql.streaming.checkpointLocation", S3_CKP)
    .config("spark.sql.shuffle.partitions",           "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ── Schemas ───────────────────────────────────────────────────────────────────
schema_trade = StructType([
    StructField("event_time", StringType(),  True),
    StructField("symbol",     StringType(),  True),
    StructField("side",       StringType(),  True),
    StructField("price",      DoubleType(),  True),
    StructField("quantity",   DoubleType(),  True),
    StructField("trade_id",   StringType(),  True),
])

schema_kline = StructType([
    StructField("event_time",  StringType(),  True),
    StructField("symbol",      StringType(),  True),
    StructField("interval",    StringType(),  True),
    StructField("open_time",   StringType(),  True),
    StructField("close_time",  StringType(),  True),
    StructField("open",        DoubleType(),  True),
    StructField("close",       DoubleType(),  True),
    StructField("high",        DoubleType(),  True),
    StructField("low",         DoubleType(),  True),
    StructField("volume",      DoubleType(),  True),
    StructField("trade_count", IntegerType(), True),
    StructField("is_closed",   BooleanType(), True),
])

schema_bookticker = StructType([
    StructField("symbol",    StringType(), True),
    StructField("update_id", StringType(), True),
    StructField("bid_price", DoubleType(), True),
    StructField("bid_qty",   DoubleType(), True),
    StructField("ask_price", DoubleType(), True),
    StructField("ask_qty",   DoubleType(), True),
])

# ── Helper functions ──────────────────────────────────────────────────────────
def read_kafka(topic: str) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe",               topic)
        .option("startingOffsets",         "latest")
        .option("failOnDataLoss",          "false")
        .option("maxOffsetsPerTrigger",    MAX_OFFSETS)
        .load()
    )

def parse_json(raw_df: DataFrame, schema: StructType) -> DataFrame:
    return (
        raw_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("d"))
        .select("d.*")
    )

def write_jdbc(df: DataFrame, table: str):
    (
        df.write
        .format("jdbc")
        .option("url",       JDBC_URL)
        .option("dbtable",   table)
        .option("driver",    JDBC_DRIVER)
        .option("user",      CLICKHOUSE_USER)
        .option("password",  CLICKHOUSE_PASS)
        .option("batchsize", "5000")
        .mode("append")
        .save()
    )

# ── foreachBatch: Trades ──────────────────────────────────────────────────────
def write_trades(batch_df: DataFrame, _: int):
    if batch_df.isEmpty():
        return
    batch_df.persist()

    batch_df.write.mode("append").parquet(f"{S3_BASE}/trades")

    fact_df = (
        batch_df
        .withColumn("event_time", to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss.SSSSSS"))
        .withColumn("trade_id",   col("trade_id").cast(LongType()))
        .dropDuplicates(["symbol", "trade_id"])
        .select("symbol", "event_time", "trade_id", "price", "quantity", "side")
    )
    fact_df.persist()
    write_jdbc(fact_df, "fact_trades")

    agg_df = (
        fact_df
        .withColumn("time", date_trunc("minute", "event_time"))
        .groupBy("symbol", "time")
        .agg(
            _sum("quantity").alias("volume"),
            (_sum(col("price") * col("quantity")) / _sum("quantity")).alias("vwap"),
            count("*").cast(IntegerType()).alias("trade_count"),
            _sum(when(col("side") == "BUY",  col("quantity")).otherwise(lit(0.0))).alias("buy_volume"),
            _sum(when(col("side") == "SELL", col("quantity")).otherwise(lit(0.0))).alias("sell_volume"),
        )
        .select("symbol", "time", "volume", "vwap", "trade_count", "buy_volume", "sell_volume")
    )
    write_jdbc(agg_df, "agg_trade_1m")

    fact_df.unpersist()
    batch_df.unpersist()


# ── foreachBatch: Klines ──────────────────────────────────────────────────────
def write_klines(batch_df: DataFrame, _: int):
    if batch_df.isEmpty():
        return
    batch_df.persist()

    batch_df.write.mode("append").parquet(f"{S3_BASE}/klines")

    ohlcv_df = (
        batch_df
        .withColumn("time", to_timestamp("open_time", "yyyy-MM-dd HH:mm:ss"))
        .dropDuplicates(["symbol", "time"])
        .select("symbol", "time", "open", "high", "low", "close", "volume")
    )
    write_jdbc(ohlcv_df, "agg_ohlcv_1m")

    batch_df.unpersist()


# ── foreachBatch: BookTicker ──────────────────────────────────────────────────
def write_bookticker(batch_df: DataFrame, _: int):
    if batch_df.isEmpty():
        return
    batch_df.persist()

    batch_df.write.mode("append").parquet(f"{S3_BASE}/bookticker")

    spread_df = (
        batch_df
        .withColumn("spread", col("ask_price") - col("bid_price"))
        .withColumn("time",   date_trunc("minute", "event_time"))
        .groupBy("symbol", "time")
        .agg(avg("spread").alias("avg_spread"))
        .select("symbol", "time", "avg_spread")
    )
    write_jdbc(spread_df, "agg_spread_1m")

    batch_df.unpersist()


# ── Streaming queries ─────────────────────────────────────────────────────────
TRIGGER = {"processingTime": "10 seconds"}

(
    parse_json(read_kafka(TOPIC_TRADES), schema_trade)
    .writeStream
    .foreachBatch(write_trades)
    .option("checkpointLocation", f"{S3_CKP}/trades")
    .trigger(**TRIGGER)
    .start()
)

(
    parse_json(read_kafka(TOPIC_KLINES), schema_kline)
    .filter(col("is_closed") == True)
    .writeStream
    .foreachBatch(write_klines)
    .option("checkpointLocation", f"{S3_CKP}/klines")
    .trigger(**TRIGGER)
    .start()
)

(
    read_kafka(TOPIC_BOOKTICKER)
    .selectExpr("CAST(value AS STRING) as value", "timestamp as event_time")
    .select(from_json(col("value"), schema_bookticker).alias("d"), col("event_time"))
    .select("d.*", "event_time")
    .writeStream
    .foreachBatch(write_bookticker)
    .option("checkpointLocation", f"{S3_CKP}/bookticker")
    .trigger(**TRIGGER)
    .start()
)

spark.streams.awaitAnyTermination()
