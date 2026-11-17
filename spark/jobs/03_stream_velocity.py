"""03 - Fraud velocity: event-time sliding window + watermark -> Kafka `fraud-alerts`.

Kural: ayni cuzdanda 30 sn'lik pencerede (10 sn kayarak) >= esik kadar islem varsa alarm.

Kavramlar: event time vs processing time, sliding window, watermark, geç veri,
output mode (update vs append), Kafka sink, checkpoint.

Kullanim:
  make spark JOB=03_stream_velocity                           # update mode, surekli
  make spark JOB=03_stream_velocity ARGS="--mode append"      # pencere kapaninca (watermark gecince) tek sefer yazar
  make spark JOB=03_stream_velocity ARGS=--available-now      # eldeki veriyi isle ve dur

NOT: Bu is bilerek dedup yapmiyor (Flink SQL surumuyle ayni kural). Duplicate sayim yapay olarak sisirebilir;
uretimde dedup upstream (silver) yapilir ya da bu is silver topic'ten okur.
"""
import argparse

from pyspark.sql import functions as F

from common import DATA, finish, get_spark, KAFKA, ProgressPrinter, read_transactions

ap = argparse.ArgumentParser()
ap.add_argument("--threshold", type=int, default=6)
ap.add_argument("--mode", choices=["update", "append"], default="update")
ap.add_argument("--sink", choices=["kafka", "console"], default="kafka")
ap.add_argument("--available-now", action="store_true")
args = ap.parse_args()

spark = get_spark("03-stream-velocity")
listener = ProgressPrinter()
spark.streams.addListener(listener)

txns = read_transactions(spark)

agg = (txns
       .withWatermark("event_time", "2 minutes")  # 2 dk'dan daha gec gelen olaylar pencereye girmez (atilir)
       .groupBy(F.window("event_time", "30 seconds", "10 seconds"), "wallet_id")
       .agg(F.count("*").alias("txn_count"), F.round(F.sum("amount"), 2).alias("total_amount")))

alerts = agg.where(F.col("txn_count") >= args.threshold)

out = alerts.select(
    F.col("wallet_id").alias("key"),
    F.to_json(F.struct(
        "wallet_id",
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "txn_count", "total_amount",
        F.lit("spark").alias("engine"),
    )).alias("value"))

# Update mode: pencere guncellenir guncellenmez alarm (dusuk gecikme) ama ayni pencere icin birden fazla alarm cikabilir.
# Append mode: pencere watermark ile kapaninca TEK sonuc (en az 2 dk gecikme, tekrar yok).
if args.sink == "kafka":
    writer = (out.writeStream.format("kafka")
              .option("kafka.bootstrap.servers", KAFKA)
              .option("topic", "fraud-alerts"))
else:
    writer = out.writeStream.format("console").option("truncate", "false")
writer = (writer.outputMode(args.mode)
          .option("checkpointLocation", f"{DATA}/checkpoints/velocity-{args.sink}-{args.mode}"))
writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(processingTime="5 seconds")

q = writer.start()
q.awaitTermination()
finish(spark, listener)  # cikista py4j callback hatasi gormemek icin
