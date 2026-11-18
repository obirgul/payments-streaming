# payments-streaming — Kafka + Spark Structured Streaming + Flink SQL

Fintech tarzı bir ödeme/cüzdan senaryosunda **batch + streaming + CDC + lakehouse** kavramlarını
elle çalıştırmak için küçük bir öğrenme projesi (Kafka, Spark Structured Streaming, Flink SQL, Delta Lake).
Hiçbir şey makineye Java/Kafka kurmaz; hepsi Docker (OrbStack) içinde çalışır.

## Mimari

```
                     ┌──────────────────────────────────────────────────────────────┐
 producer.py         │                         Kafka (KRaft)                        │
 (sentetik veri) ───►│  transactions (3 part.)      wallet_cdc (Debezium zarfı)     │
                     └───────┬───────────────┬──────────────────────┬───────────────┘
                             │               │                      │
              ┌──────────────▼───┐   ┌───────▼──────────┐   ┌───────▼────────────┐
              │ Spark 02 silver  │   │ Spark 03 velocity│   │ Spark 04 CDC→Delta │
              │ dedup + enrich   │   │ 30s/10s window   │   │ foreachBatch MERGE │
              └────────┬─────────┘   └───────┬──────────┘   └───────┬────────────┘
                       ▼                     ▼                      ▼
              data/silver (Parquet)    fraud-alerts           data/delta/wallets
                                               ▲
                     Flink SQL (aynı kural) ───┴──► fraud-alerts-flink
```

Veri bilerek "kirli": **duplicate txn_id**, **geç gelen olaylar** (1-3 dk), **fraud burst'leri** (aynı cüzdanda ~2.5 sn'de 8-10 işlem).

## Gereksinim ve başlatma

OrbStack (veya Docker Desktop) çalışıyor olmalı. Docker CLI PATH'te değilse Makefile OrbStack'in binary'lerini kendisi bulur.

```bash
make up                 # Kafka + Spark ayağa kalkar, topic'ler oluşur (ilk sefer imaj indirir)
make help               # tüm komutlar
```

## Adım adım

```bash
# 0) Batch temelleri (Kafka gerekmez). Spark UI: http://localhost:4040
make spark JOB=01_batch_basics

# 1) Veri üret (240 sn, ~5 olay/sn) -- ayrı terminalde çalışırken Spark işlerini canlı izleyebilirsin
make produce
make produce-cdc

# 2) Streaming işleri (sürekli çalışır, Ctrl-C ile durdur) -- ya da ARGS=--available-now ile eldekini işle ve bitir
make spark JOB=02_stream_silver
make spark JOB=03_stream_velocity            # alarmlar: make consume TOPIC=fraud-alerts
make spark JOB=04_cdc_to_delta

# 3) Flink SQL ile aynı kural (Flink UI: http://localhost:8081)
make flink-up
make flink-sql
make consume TOPIC=fraud-alerts-flink

# 4) Doğrulama (producer'ın ground truth'u ile karşılaştırır)
make verify

# Hepsini otomatik: sıfırla -> üret -> işle -> doğrula
make smoke

# Birim testleri (Kafka gerekmez, ~30 sn): dedup, fraud penceresi, CDC MERGE, producer tutarlılığı
make test                  # tek modül: make test TEST=test_cdc
```

## Ne var?

| Dosya | Konu |
|---|---|
| `producer/producer.py` | Sentetik işlem akışı ve Debezium biçimli CDC; ground truth üretir |
| `spark/jobs/01_batch_basics.py` | Lazy evaluation, `explain()`, broadcast vs sort-merge join, window function, skew + salting, repartition/coalesce, partition pruning |
| `spark/jobs/02_stream_silver.py` | Kafka → parse → watermark + `dropDuplicatesWithinWatermark` → stream-static join → Parquet, checkpoint |
| `spark/jobs/03_stream_velocity.py` | Event-time sliding window, watermark, update vs append mode, Kafka sink |
| `spark/jobs/04_cdc_to_delta.py` | CDC zarfı, batch içinde son kaydı seçme, idempotent Delta `MERGE` (insert/update/delete) |
| `flink/sql/fraud_velocity.sql` | Aynı fraud kuralı Flink SQL ile (`HOP` window TVF, watermark, checkpoint) |
| `spark/jobs/verify.py` | Silver/Delta/alarm çıktılarını ground truth ile karşılaştırır |

## Denemeler (öğrenmek için)

1. **Checkpoint:** `03` çalışırken `docker-compose restart spark` ile öldür, tekrar başlat. `batch` numarası kaldığı yerden devam eder, eski veri yeniden işlenmez.
2. **Geç veri:** `03`'ü canlı çalıştır; çıktıdaki `watermark'la atilan` sayacı 2 dk'dan daha geç gelen olayları gösterir. `withWatermark("event_time", "2 minutes")` değerini değiştirip farkı gör.
3. **Update vs append:** `ARGS="--mode append"` ile alarmlar pencere kapanana kadar gecikir ama tekrar etmez; update modunda hemen gelir ama aynı pencere için birden fazla.
4. **Dedup:** `02`'de `dropDuplicatesWithinWatermark` satırını kaldırıp silver satır sayısını producer'ın `total_sent` değeriyle karşılaştır.
5. **Idempotency:** `04`'ün checkpoint'ini silip (`rm -rf data/checkpoints/cdc`) yeniden çalıştır; `s.lsn > t.lsn` koşulu sayesinde Delta durumu bozulmaz.
6. **Spark vs Flink:** İkisi de `transactions` topic'inden okur; aynı burst'ler için alarmların ne zaman geldiğini kıyasla.

## Notlar

- **Portlar:** Kafka host'tan `localhost:29092`, Spark UI `4040` (iş çalışırken), Flink UI `8081`.
- **Veri:** `./data` (silver, delta, checkpoint, ground truth). `make reset` hepsini ve topic'leri sıfırlar.
- **Flink SQL notu:** `sql-client.sh -f` dosyasında yorumlarda tek tırnak (`'`) ve `;` sonrası aynı satırda yorum kullanma; ifadeler birleşip `only single statement supported` hatası verir.
- **Kapsam dışı:** Gerçek Debezium + Postgres (CDC burada Debezium zarfını taklit eder), Spark cluster modu, schema registry. Sonraki adım olarak eklenebilir.
- **Temizlik:** `make down`; imajları/volume'u da silmek için `docker-compose down -v`.

## Lisans

MIT, bkz. [LICENSE](LICENSE).
