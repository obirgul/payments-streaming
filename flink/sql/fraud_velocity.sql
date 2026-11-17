-- Flink SQL: SAME rule as Spark 03_stream_velocity.py (30 s window, 10 s slide, >= 6 transactions).
-- Run with: make flink-up && make flink-sql      (Flink UI: http://localhost:8081)

SET 'pipeline.name' = 'fraud-velocity-flink';
-- In Flink, fault tolerance = periodic checkpoints (state + Kafka offsets together)
SET 'execution.checkpointing.interval' = '10s';
-- do not let an idle partition block the watermark
SET 'table.exec.source.idle-timeout' = '10s';

-- Source: Kafka topic transactions. WATERMARK = event time minus 2 min late-data tolerance (same as withWatermark in Spark).
CREATE TABLE transactions (
  txn_id       STRING,
  wallet_id    STRING,
  user_id      STRING,
  amount       DOUBLE,
  currency     STRING,
  merchant_id  STRING,
  country      STRING,
  channel      STRING,
  event_time   TIMESTAMP_LTZ(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '2' MINUTE
) WITH (
  'connector' = 'kafka',
  'topic' = 'transactions',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-fraud-velocity',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.ignore-parse-errors' = 'true'
);

-- Sink: Kafka topic fraud-alerts-flink
CREATE TABLE fraud_alerts_flink (
  wallet_id     STRING,
  window_start  TIMESTAMP(3),
  window_end    TIMESTAMP(3),
  txn_count     BIGINT,
  total_amount  DOUBLE,
  engine        STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'fraud-alerts-flink',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

-- HOP = sliding window (same as window(col, "30 seconds", "10 seconds") in Spark).
-- The window TVF produces append-only results: one row is written when the window CLOSES via the watermark
-- (like the append mode in Spark, the early or repeated alerts of Spark update mode do not occur here).
INSERT INTO fraud_alerts_flink
SELECT
  wallet_id,
  window_start,
  window_end,
  COUNT(*)                    AS txn_count,
  ROUND(SUM(amount), 2)       AS total_amount,
  'flink'                     AS engine
FROM TABLE(
  HOP(TABLE transactions, DESCRIPTOR(event_time), INTERVAL '10' SECOND, INTERVAL '30' SECOND)
)
GROUP BY wallet_id, window_start, window_end
HAVING COUNT(*) >= 6;
