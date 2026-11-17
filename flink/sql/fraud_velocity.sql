-- Flink SQL: Spark 03_stream_velocity.py ile AYNI kural (30 sn pencere, 10 sn kayma, >= 6 islem).
-- Calistirma: make flink-up && make flink-sql      (Flink UI: http://localhost:8081)

SET 'pipeline.name' = 'fraud-velocity-flink';
-- Flinkte fault tolerance = periyodik checkpoint (state + kafka offset birlikte)
SET 'execution.checkpointing.interval' = '10s';
-- bos kalan partition watermarki bloklamasin
SET 'table.exec.source.idle-timeout' = '10s';

-- Kaynak: Kafka `transactions`. WATERMARK = event time + 2 dk gec veri toleransi (Sparktaki withWatermark ile ayni).
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

-- Hedef: Kafka `fraud-alerts-flink`
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

-- HOP = sliding window (Sparkta window(col, "30 seconds", "10 seconds")).
-- Window TVF append-only sonuc uretir: pencere watermark ile KAPANINCA tek satir yazar
-- (Sparktaki "append" modu gibi; Spark update modunun erken/tekrarlayan alarmlari burada yok).
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
