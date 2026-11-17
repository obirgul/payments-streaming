#!/usr/bin/env bash
# Uctan uca dogrulama: ortami sifirla -> veri uret -> Spark (+Flink) isleri -> ground truth ile karsilastir.
#
#   ./scripts/smoke_test.sh                  # ~8 dk (Spark + Flink)
#   WITH_FLINK=0 DURATION=90 QUIET=40 ./scripts/smoke_test.sh   # hizli, sadece Spark
#
# DURATION: islem uretim suresi (sn). QUIET: sondaki "burst uretilmeyen" sure (sn);
# Flink penceresi watermark ile kapandigi icin QUIET >= 2 dk gec tolerans + 30 sn pencere olmali.
set -euo pipefail
cd "$(dirname "$0")/.."

DURATION="${DURATION:-240}"
QUIET="${QUIET:-160}"
WITH_FLINK="${WITH_FLINK:-1}"

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

step "1/5 Ortam (Kafka + Spark)"
make up

step "2/5 Sifirlama (topic, checkpoint, cikti)"
make reset

if [ "$WITH_FLINK" = "1" ]; then
  step "3/5 Flink cluster + SQL job (veri akarken calissin)"
  make flink-up
  make flink-sql
fi

step "4/5 Veri uretimi: ${DURATION}s islem akisi + CDC"
make produce DURATION="$DURATION" PRODUCER_ARGS="--burst-interval 15 --quiet-tail $QUIET"
make produce-cdc

step "5/5 Spark isleri (--available-now) ve dogrulama"
for job in 02_stream_silver 03_stream_velocity 04_cdc_to_delta; do
  echo "--- $job"
  make spark JOB="$job" ARGS=--available-now 2>&1 | grep -E '^\[batch|ERROR|Exception' || true
done
if [ "$WITH_FLINK" = "1" ]; then
  echo "Flink'in son pencereleri yazmasi icin 20 sn bekleniyor..."
  sleep 20
fi
make verify 2>&1 | grep -E '^(PASS|FAIL|SKIP|SONUC)'
