# Usage: `make help`
# If the Docker CLI is not on PATH, the binaries bundled with OrbStack are used.
ORB_BIN := /Applications/OrbStack.app/Contents/MacOS/xbin
export PATH := $(ORB_BIN):$(PATH)

DC      := $(ORB_BIN)/docker-compose
DCF     := $(DC) --profile flink
PKGS    := org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,io.delta:delta-spark_2.12:3.2.1
SPARK_SUBMIT := /opt/spark/bin/spark-submit --master local[2] --driver-memory 1g \
                --packages $(PKGS) --conf spark.jars.ivy=/ivy \
                --conf spark.driver.extraJavaOptions=-Dlog4j2.configurationFile=file:/jobs/log4j2.properties

JOB      ?= 02_stream_silver
ARGS     ?=
RATE     ?= 5
DURATION ?= 240
TOPIC    ?= fraud-alerts

.PHONY: help up down topics-init topics produce produce-cdc spark consume flink-up flink-sql flink-down reset verify smoke

help:
	@echo "make up                      Kafka + Spark konteynerlerini baslat, topic'leri olustur"
	@echo "make produce [RATE=5 DURATION=240]  islem akisi uret (transactions)"
	@echo "make produce-cdc             Debezium bicimli bakiye degisiklikleri uret (wallet_cdc)"
	@echo "make spark JOB=02_stream_silver [ARGS=--available-now]   Spark isi calistir"
	@echo "      JOB: 01_batch_basics | 02_stream_silver | 03_stream_velocity | 04_cdc_to_delta | verify"
	@echo "make flink-up / flink-sql / flink-down   Flink cluster + SQL isi"
	@echo "make consume TOPIC=fraud-alerts          topic'i oku"
	@echo "make reset                   topic, checkpoint ve cikti verisini sifirla"
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

consume:
	$(DC) exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
		--topic $(TOPIC) --from-beginning --timeout-ms 8000

flink-up:
	$(DCF) up -d --build --wait jobmanager taskmanager

flink-sql:
	$(DCF) exec -T jobmanager ./bin/sql-client.sh -f /sql/fraud_velocity.sql

flink-down:
	$(DCF) stop jobmanager taskmanager

reset:
	-$(DCF) stop jobmanager taskmanager
	rm -rf data/checkpoints data/silver data/delta data/ground_truth_*.json
	-$(DC) exec -T kafka bash -c 'for t in transactions wallet_cdc fraud-alerts fraud-alerts-flink; do /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic $$t; done'
	sleep 5
	$(DC) run --rm kafka-init

verify:
	$(DC) exec -T spark $(SPARK_SUBMIT) /jobs/verify.py

smoke:
	./scripts/smoke_test.sh

down:
	$(DCF) --profile tools down
