#!/usr/bin/env bash
# End-to-end verification: reset environment -> produce data -> Spark (+Flink) jobs -> compare with ground truth.
#
#   ./scripts/smoke_test.sh                  # ~8 min (Spark + Flink)
#   WITH_FLINK=0 DURATION=90 QUIET=40 ./scripts/smoke_test.sh   # fast, Spark only
#   WITH_PG=1 ./scripts/smoke_test.sh        # CDC from real Postgres + Debezium instead of the synthetic wallet_cdc producer
#
# DURATION: transaction production time (s). QUIET: trailing "no bursts produced" time (s);
# since the Flink window closes via the watermark, QUIET must be >= 2 min late tolerance + 30 s window.
set -euo pipefail
cd "$(dirname "$0")/.."

DURATION="${DURATION:-240}"
QUIET="${QUIET:-160}"
WITH_FLINK="${WITH_FLINK:-1}"
WITH_PG="${WITH_PG:-0}"

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

step "1/5 Ortam (Kafka + Spark)"
make up

step "2/5 Sifirlama (topic, checkpoint, cikti)"
make reset

if [ "$WITH_PG" = "1" ]; then
  step "2b/5 Postgres + Debezium (connector kaydi, ilk snapshot)"
  make cdc-up
fi

if [ "$WITH_FLINK" = "1" ]; then
  step "3/5 Flink cluster + SQL job (veri akarken calissin)"
  make flink-up
  make flink-sql
fi

step "4/5 Veri uretimi: ${DURATION}s islem akisi + CDC"
make produce DURATION="$DURATION" PRODUCER_ARGS="--burst-interval 15 --quiet-tail $QUIET"
if [ "$WITH_PG" = "1" ]; then
  make produce-wallets PRODUCER_ARGS="--drift 3"  # 3 wallets get a balance change WITHOUT a ledger entry
  echo "Debezium'un son degisiklikleri Kafka'ya yazmasi icin 15 sn bekleniyor..."
  sleep 15
else
  make produce-cdc
fi

step "5/5 Spark isleri (--available-now) ve dogrulama"
for job in 02_stream_silver 03_stream_velocity 04_cdc_to_delta; do
  echo "--- $job"
  make spark JOB="$job" ARGS=--available-now 2>&1 | grep -E '^\[batch|ERROR|Exception' || true
done
if [ "$WITH_PG" = "1" ]; then
  echo "--- 05_ledger_to_delta"
  make spark JOB=05_ledger_to_delta ARGS=--available-now 2>&1 | grep -E '^\[batch|ERROR|Exception' || true
  echo "--- 06_reconcile (3 enjekte sapma bulmali; cikis kodu 1 beklenir)"
  make spark JOB=06_reconcile 2>&1 | grep -E '^\[reconcile\]|Exception' || true
fi
if [ "$WITH_FLINK" = "1" ]; then
  echo "Flink'in son pencereleri yazmasi icin 20 sn bekleniyor..."
  sleep 20
fi
make verify 2>&1 | grep -E '^(PASS|FAIL|SKIP|SONUC)'
