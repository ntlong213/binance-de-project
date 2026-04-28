CREATE DATABASE IF NOT EXISTS crypto;

-- ── fact_trades ───────────────────────────────────────────────────────────────
-- Giao dịch thực tế đã clean — dùng cho drilldown chi tiết
CREATE TABLE IF NOT EXISTS crypto.fact_trades (
    symbol      LowCardinality(String),
    event_time  DateTime64(3),
    trade_id    UInt64,
    price       Float64,
    quantity    Float64,
    side        LowCardinality(String)
)
ENGINE = ReplacingMergeTree()
PARTITION BY (toDate(event_time), symbol)
ORDER BY (symbol, event_time, trade_id);

-- ── agg_trade_1m ─────────────────────────────────────────────────────────────
-- Volume, VWAP, buy/sell ratio theo từng phút — dùng cho Grafana bar/line chart
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

-- ── agg_ohlcv_1m ─────────────────────────────────────────────────────────────
-- Nến 1 phút từ Binance klines stream — dùng cho Grafana candlestick chart
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

-- ── agg_spread_1m ────────────────────────────────────────────────────────────
-- Spread bid-ask trung bình theo phút — dùng cho liquidity monitoring
CREATE TABLE IF NOT EXISTS crypto.agg_spread_1m (
    symbol     LowCardinality(String),
    time       DateTime,
    avg_spread Float64
)
ENGINE = ReplacingMergeTree()
PARTITION BY (toDate(time), symbol)
ORDER BY (symbol, time);
