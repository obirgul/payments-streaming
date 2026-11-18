# Usage: `make help`
# If the Docker CLI is not on PATH, the binaries bundled with OrbStack are used.
ORB_BIN := /Applications/OrbStack.app/Contents/MacOS/xbin
export PATH := $(ORB_BIN):$(PATH)

DC      := $(ORB_BIN)/docker-compose
DCF     := $(DC) --profile flink
DCC     := $(DC) --profile cdc
DCO     := $(DC) --profile obs
CONNECT := http://localhost:8083
PKGS    := org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,io.delta:delta-spark_2.12:3.2.1
SPARK_SUBMIT := /opt/spark/bin/spark-submit --master local[2] --driver-memory 1g \
                --packages $(PKGS) --conf spark.jars.ivy=/ivy \
                --conf spark.sql.streaming.metricsEnabled=true \
                --conf 'spark.metrics.conf.*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet' \
                --conf 'spark.metrics.conf.*.sink.prometheusServlet.path=/metrics/prometheus' \
                --conf spark.driver.extraJavaOptions=-Dlog4j2.configurationFile=file:/jobs/log4j2.properties

JOB      ?= 02_stream_silver
ARGS     ?=
RATE     ?= 5
DURATION ?= 240
TOPIC    ?= fraud-alerts

.PHONY: help up down topics-init topics produce produce-cdc spark consume flink-up flink-sql flink-down obs-up obs-down cdc-up cdc-status cdc-down produce-wallets psql reset verify smoke test

help:
	@echo "make up                      Kafka + Spark konteynerlerini baslat, topic'leri olustur"
	@echo "make produce [RATE=5 DURATION=240]  islem akisi uret (transactions)"
	@echo "make produce-cdc             Debezium bicimli bakiye degisiklikleri uret (wallet_cdc)"
	@echo "make spark JOB=02_stream_silver [ARGS=--available-now]   Spark isi calistir"
	@echo "      JOB: 01_batch_basics | 02_stream_silver | 03_stream_velocity | 04_cdc_to_delta | 05_ledger_to_delta | 06_reconcile | verify"
	@echo "make cdc-up                   Postgres + Kafka Connect/Debezium baslat, connector'u kaydet"
	@echo "make produce-wallets         cuzdan degisikliklerini Postgres'e yaz (Debezium wallet_cdc'ye akitir)"
	@echo "make cdc-status / psql / cdc-down   connector durumu / SQL kabugu / durdur"
	@echo "make obs-up / obs-down       Prometheus :9090 + Grafana :3000 (admin/admin) + kafka-exporter"
	@echo "make flink-up / flink-sql / flink-down   Flink cluster + SQL isi"
	@echo "make consume TOPIC=fraud-alerts          topic'i oku"
	@echo "make reset                   topic, checkpoint ve cikti verisini sifirla"
	@echo "make test [TEST=test_cdc]   birim testleri (Kafka gerekmez, ~1-2 dk)"
	@echo "make smoke                   uctan uca dogrulama (scripts/smoke_test.sh)"
	@echo "make down                    her seyi durdur"

up:
	@mkdir -p data
	$(DC) up -d --build --wait kafka spark
	$(DC) run --rm kafka-init

topics-init:
	$(DC) run --rm kafka-init

topics:
	$(DC) exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe

produce:
	$(DC) --profile tools run --rm --build producer txn --rate $(RATE) --duration $(DURATION) $(PRODUCER_ARGS)

produce-cdc:
	$(DC) --profile tools run --rm --build producer cdc

spark:
	$(DC) exec $(shell [ -t 0 ] || echo -T) spark $(SPARK_SUBMIT) /jobs/$(JOB).py $(ARGS)

produce-wallets:
	$(DC) --profile tools --profile cdc run --rm --build producer wallet-db $(PRODUCER_ARGS)

# Starts Postgres + Connect and (re)registers the Debezium connector. PUT on /config is idempotent.
cdc-up:
	$(DCC) up -d --wait postgres connect
	curl -sf -X PUT -H 'Content-Type: application/json' --data @debezium/wallets-connector.json \
		$(CONNECT)/connectors/wallets-cdc/config >/dev/null && echo "connector kaydedildi: wallets-cdc"

cdc-status:
	@curl -s $(CONNECT)/connectors/wallets-cdc/status | python3 -m json.tool

psql:
	$(DCC) exec postgres psql -U payments -d payments

cdc-down:
	$(DCC) stop connect postgres

consume:
	$(DC) exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
		--topic $(TOPIC) --from-beginning --timeout-ms 8000

flink-up:
	$(DCF) up -d --build --wait jobmanager taskmanager

flink-sql:
	$(DCF) exec -T jobmanager ./bin/sql-client.sh -f /sql/fraud_velocity.sql

flink-down:
	$(DCF) stop jobmanager taskmanager

# Prometheus + Grafana + kafka-exporter. Spark/Flink metrics appear while their jobs run.
obs-up:
	$(DCO) up -d --wait prometheus grafana kafka-exporter
	@echo "Grafana: http://localhost:3000 (admin/admin) | Prometheus: http://localhost:9090"

obs-down:
	$(DCO) stop prometheus grafana kafka-exporter

reset:
	-$(DCF) stop jobmanager taskmanager
	-$(DCC) stop connect
	-$(DCC) exec -T postgres psql -U payments -d payments -c "TRUNCATE wallets, ledger_entries" -c "SELECT pg_drop_replication_slot('wallets_slot')"
	rm -rf data/checkpoints data/silver data/delta data/ground_truth_*.json
	-$(DC) exec -T kafka bash -c 'for t in transactions wallet_cdc ledger_cdc fraud-alerts fraud-alerts-flink connect_configs connect_offsets connect_statuses; do /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic $$t; done'
	sleep 5
	$(DC) run --rm kafka-init

verify:
	$(DC) exec -T spark $(SPARK_SUBMIT) /jobs/verify.py

# Unit tests: a fresh spark container (no Kafka needed); `--no-deps` skips the Kafka dependency.
test:
	@mkdir -p data
	$(DC) run --rm --no-deps -T spark $(SPARK_SUBMIT) /tests/run_tests.py $(TEST)

smoke:
	./scripts/smoke_test.sh

down:
	$(DCF) --profile tools --profile cdc --profile obs down
