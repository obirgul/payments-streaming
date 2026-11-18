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
make produce-cdc                             # sentetik CDC (Postgres'siz); gercek CDC icin asagidaki "Postgres + Debezium"

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

## Postgres + Debezium (gerçek CDC)

`wallet_cdc` olaylarını sentetik üretmek yerine uygulama Postgres'e yazar, Debezium WAL'dan okuyup Kafka'ya akıtır:
`wallet-db (producer) → Postgres → Debezium (Kafka Connect) → wallet_cdc → Spark 04 → Delta`. Spark tarafı değişmez,
çünkü gerçek Debezium zarfı (`before`/`after`/`op`/`source.lsn`) sentetik olanla aynı biçimdedir.

```bash
make up
make cdc-up                  # Postgres + Kafka Connect baslar, connector kaydedilir, ilk snapshot (op=r) akar
make produce-wallets         # 200 cuzdan insert, ~600 bakiye degisikligi (tek satir + transfer), 5 kapatma
make spark JOB=04_cdc_to_delta ARGS=--available-now
make verify                  # Delta == Postgres'teki son durum (ground truth, tablodan okunur)

# Bakiye mutabakati: cuzdan bakiyesi == ledger toplami mi?
make produce-wallets PRODUCER_ARGS="--drift 3"   # 3 cuzdanda ledger'siz bakiye degisikligi enjekte et
make spark JOB=05_ledger_to_delta ARGS=--available-now
make spark JOB=06_reconcile                      # uyusmayanlari yazdirir, varsa exit 1
make cdc-status              # connector durumu;  make psql  -> SELECT * FROM wallets;
WITH_PG=1 ./scripts/smoke_test.sh   # hepsini otomatik (sentetik CDC yerine Postgres)
```

**Mutabakat:** `ledger_entries` tablosuna her bakiye değişikliği, cüzdan güncellemesiyle AYNI transaction'da bir satır
olarak yazılır (açılış, düzeltme, transfer çıkış/giriş, kapatma). Değişmez kural: `balance_kurus == SUM(delta_kurus)`.
`05` ledger'ı Delta'ya aktarır, `06` cüzdan Delta'sı ile ledger toplamını karşılaştırıp yalnızca uyuşmazlıkları
(`balance_mismatch`, `missing_ledger`, `orphan_ledger`) `data/delta/reconciliation`'a yazar. Bu kontrol hem uygulama
hatalarını (ledger'ı atlayan yol, elle düzeltme) hem de CDC hattında kayıp/uygulanmamış olayları yakalar.
`--drift N` ile bilerek ledger'sız değişiklik yapılır; `verify` tam olarak bu cüzdanların ve tutarların bulunduğunu,
yanlış pozitif olmadığını doğrular. İki Delta tablosu bağımsız akışlarla beslenir: ikisi de yetişmeden çalıştırırsan
uçuştaki olaylar geçici uyuşmazlık gibi görünür.

Bilinenler: connector ayari `debezium/wallets-connector.json`, tablo `postgres/init.sql` (`REPLICA IDENTITY FULL`:
güncelleme/silmede tam eski satır `before`'da gelir). `make reset` slot'u, connect topic'lerini ve tabloyu da sıfırlar;
sonra `make cdc-up` ile yeniden başlat. Connect REST API: `localhost:8083`, Postgres: `localhost:5432` (payments/payments, sadece yerel demo).

## Gözlemlenebilirlik (Prometheus + Grafana)

```bash
make obs-up      # Prometheus :9090, Grafana :3000 (admin/admin, anonim okuma açık), kafka-exporter
make obs-down
```

Grafana'da hazır **Payments Pipeline** panosu gelir (5 sn yenilenir):

| Bölüm | Kaynak | Ne gösterir |
|---|---|---|
| Kafka | `kafka-exporter` | topic başına mesaj/sn ve toplam offset (alarm topic'leri dahil) |
| Spark | driver `/metrics/prometheus` | sorgu başına giriş/işleme hızı, batch gecikmesi, watermark gerisi, state satırı |
| Flink | Prometheus reporter (`:9249`) | kaynak hızı, görev meşguliyeti, checkpoint süresi/hataları |
| Alarmlar | Kafka offset'leri | Spark ve Flink'in ürettiği alarm hızı yan yana |

Spark metrikleri sadece iş çalışırken vardır (her iş UI portu `4040`, `4041`, ... alır; Prometheus ilk dördünü tarar).
Sorgular `queryName` ile adlandırıldı (`silver`, `velocity`, `cdc_wallets`, `cdc_ledger`); Spark metrik adlarına uygulama
kimliğini koyar, `observability/prometheus.yml` bunu `spark_streaming_<metrik>{query="..."}` biçimine çevirir.
Flink için `make flink-up && make flink-sql` gerekir (imaj Prometheus reporter jar'ını `plugins/` altına alır).
Alarm kuralları `observability/alerts.yml` içinde; Alertmanager yok, durum Prometheus arayüzünde görünür.

## Ne var?

| Dosya | Konu |
|---|---|
| `producer/producer.py` | Sentetik işlem akışı, Debezium biçimli sentetik CDC (`cdc`) ve Postgres'e yazan cüzdan servisi (`wallet-db`); ground truth üretir |
| `postgres/init.sql`, `debezium/wallets-connector.json` | Cüzdan + ledger tabloları ve Debezium Postgres connector ayarı (`wallet_cdc`, `ledger_cdc`) |
| `spark/jobs/05_ledger_to_delta.py`, `06_reconcile.py` | Ledger CDC → Delta; bakiye == ledger toplamı mutabakatı |
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

- **Portlar:** Kafka host'tan `localhost:29092`, Spark UI `4040` (iş çalışırken), Flink UI `8081`, Prometheus `9090` ve Grafana `3000` (`obs` profili), Postgres `5432` ve Connect `8083` (`cdc` profili).
- **Veri:** `./data` (silver, delta, checkpoint, ground truth). `make reset` hepsini ve topic'leri sıfırlar.
- **Flink SQL notu:** `sql-client.sh -f` dosyasında yorumlarda tek tırnak (`'`) ve `;` sonrası aynı satırda yorum kullanma; ifadeler birleşip `only single statement supported` hatası verir.
- **Kapsam dışı:** Spark cluster modu, schema registry (Debezium burada şemasız JSON yazar).
- **Temizlik:** `make down`; imajları/volume'u da silmek için `docker-compose down -v`.

## Lisans

MIT, bkz. [LICENSE](LICENSE).
