import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_timestamp, window,
    sum as _sum, avg, count,
    when, lit
)
from pyspark.sql.types import LongType, IntegerType
from pyspark.sql.functions import to_utc_timestamp

# ── ENV ───────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT      = os.getenv("MINIO_ENDPOINT")
MINIO_USER          = os.getenv("MINIO_ROOT_USER")
MINIO_PASSWORD      = os.getenv("MINIO_ROOT_PASSWORD")
CLICKHOUSE_HOST     = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT     = os.getenv("CLICKHOUSE_PORT")
CLICKHOUSE_DB       = os.getenv("CLICKHOUSE_DB")
CLICKHOUSE_USER     = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
S3_LAKE_BUCKET      = os.getenv("S3_LAKE_BUCKET")
JDBC_BATCH_SIZE     = os.getenv("JDBC_BATCH_SIZE")
LAST_PROCESSED_TIME = os.getenv("LAST_PROCESSED_TIME")

JDBC_URL    = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}"
JDBC_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"
LAKE_BASE   = f"s3a://{S3_LAKE_BUCKET}"

# ── Spark Session ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("BinanceCryptoTransform")
    .master("spark://spark-master:7077")
    .config("spark.jars.ivy", "/tmp/.ivy")
    .config(
        "spark.jars.packages",
        ",".join([
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
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ── Write helper ──────────────────────────────────────────────────────────────
def write_to_clickhouse(df: DataFrame, table: str):
    (
        df.write
        .format("jdbc")
        .option("url",          JDBC_URL)
        .option("dbtable",      table)
        .option("driver",       JDBC_DRIVER)
        .option("user",         CLICKHOUSE_USER)
        .option("password",     CLICKHOUSE_PASSWORD)
        .option("batchsize",    JDBC_BATCH_SIZE)
        .option("numPartitions", "2")
        .mode("append")
        .save()
    )
    print(f"[Transform] {table} → done")

# ── 1. fact_trades (incremental + clean) ─────────────────────────────────────
df_raw_trades = spark.read.parquet(f"{LAKE_BASE}/trades")

df_fact_trades = (
    df_raw_trades
    .withColumn("event_time", to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss.SSSSSS"))
    .withColumn("event_time", to_utc_timestamp(col("event_time"), "UTC"))
    .withColumn("trade_id", col("trade_id").cast(LongType()))
    .filter(col("event_time") >= lit(LAST_PROCESSED_TIME))
    .dropDuplicates(["symbol", "trade_id"])
    .select("symbol", "event_time", "trade_id", "price", "quantity", "side")
)

write_to_clickhouse(df_fact_trades, "fact_trades")

# ── 2. agg_trade_1m (VWAP) ───────────────────────────────────────────────────
df_agg_trade_1m = (
    df_fact_trades
    .groupBy("symbol", window("event_time", "1 minute"))
    .agg(
        _sum("quantity").alias("volume"),
        (_sum(col("price") * col("quantity")) / _sum("quantity")).alias("vwap"),
        count("*").cast(IntegerType()).alias("trade_count"),
        _sum(when(col("side") == "BUY",  col("quantity")).otherwise(lit(0.0))).alias("buy_volume"),
        _sum(when(col("side") == "SELL", col("quantity")).otherwise(lit(0.0))).alias("sell_volume"),
    )
    .withColumn("time", col("window.start"))
    .drop("window")
    .select("symbol", "time", "volume", "vwap", "trade_count", "buy_volume", "sell_volume")
)

write_to_clickhouse(df_agg_trade_1m, "agg_trade_1m")

# ── 3. agg_ohlcv_1m (from klines) ────────────────────────────────────────────
df_raw_klines = spark.read.parquet(f"{LAKE_BASE}/klines")

df_agg_ohlcv_1m = (
    df_raw_klines
    .withColumn("time", to_timestamp("open_time", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("time", to_utc_timestamp(col("time"), "UTC"))
    .dropDuplicates(["symbol", "time"])
    .select("symbol", "time", "open", "high", "low", "close", "volume")
)

write_to_clickhouse(df_agg_ohlcv_1m, "agg_ohlcv_1m")

# ── 4. agg_spread_1m ─────────────────────────────────────────────────────────
df_raw_bookticker = spark.read.parquet(f"{LAKE_BASE}/bookticker")

df_agg_spread_1m = (
    df_raw_bookticker
    .withColumn("event_time", to_timestamp("event_time"))
    .withColumn("event_time", to_utc_timestamp(col("event_time"), "UTC"))
    .withColumn("spread", col("ask_price") - col("bid_price"))
    .groupBy("symbol", window("event_time", "1 minute"))
    .agg(avg("spread").alias("avg_spread"))
    .withColumn("time", col("window.start"))
    .drop("window")
    .select("symbol", "time", "avg_spread")
)

write_to_clickhouse(df_agg_spread_1m, "agg_spread_1m")

spark.stop()
