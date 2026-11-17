#!/usr/bin/env python3
"""Synthetic payment/wallet data generator (fintech scenario).

Subcommands:
  txn          Produces a transaction stream to the Kafka `transactions` topic.
  cdc          Produces balance changes in Debezium envelope format (before/after/op) to the Kafka `wallet_cdc` topic.
  gen-wallets  Generates spark/data/wallets.csv (the static dimension table).

The data is deliberately "dirty": it contains duplicate txn_ids, late events and fraud bursts.
At the end of each run the expected result is written to /data/ground_truth_*.json;
`spark/jobs/verify.py` compares the job outputs against it.
"""
import argparse
import json
import os
import random
import sys
import time
from collections import deque
from datetime import datetime, timedelta, timezone

BOOT = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
DATA_DIR = os.environ.get("DATA_DIR", "/data")
N_WALLETS = 200
MERCHANTS = ["M%03d" % i for i in range(1, 41)]
COUNTRIES = ["TR"] * 18 + ["DE", "NL", "AE"]
CHANNELS = ["app", "web", "pos", "qr"]
CITIES = ["Istanbul", "Ankara", "Izmir", "Bursa", "Antalya"]


def wallet_id(i):
    return "W%04d" % i


def user_id(i):
    return "U%05d" % (10000 + i)


def iso(dt):
    # Format readable by both Spark to_timestamp and Flink JSON (ISO-8601)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + "%03dZ" % (dt.microsecond // 1000)


def utcnow():
    return datetime.now(timezone.utc)


def write_json(name, obj):
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, name)
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)
    print("ground truth ->", path)


def make_producer():
    from confluent_kafka import Producer  # lazy import: gen-wallets does not need Kafka

    return Producer({"bootstrap.servers": BOOT, "linger.ms": 20, "enable.idempotence": True})


# ---------------------------------------------------------------- gen-wallets
def gen_wallets(args):
    rng = random.Random(42)
    out = args.out
    with open(out, "w") as f:
        f.write("wallet_id,user_id,segment,daily_limit_try,city\n")
        for i in range(1, N_WALLETS + 1):
            r = rng.random()
            segment = "standard" if r < 0.7 else ("premium" if r < 0.9 else "new")
            limit = {"standard": 5000, "premium": 20000, "new": 1000}[segment]
            f.write("%s,%s,%s,%d,%s\n" % (wallet_id(i), user_id(i), segment, limit, rng.choice(CITIES)))
    print("yazildi:", out)


# ------------------------------------------------------------------------ txn
def new_event(rng, widx, event_time):
    amount = round(min(rng.lognormvariate(4.5, 1.0), 15000), 2)  # median ~90 TRY, long tail
    return {
        "txn_id": "T%016x" % rng.getrandbits(64),
        "wallet_id": wallet_id(widx),
        "user_id": user_id(widx),
        "amount": amount,
        "currency": "TRY",
        "merchant_id": rng.choice(MERCHANTS),
        "country": rng.choice(COUNTRIES),
        "channel": rng.choice(CHANNELS),
        "event_time": iso(event_time),
    }


def run_txn(a):
    rng = random.Random(a.seed)
    p = make_producer()
    recent = deque(maxlen=300)
    gt = {"unique_txn_ids": 0, "total_sent": 0, "duplicates_sent": 0, "late_events": 0, "burst_wallets": []}

    def send(ev):
        p.produce("transactions", key=ev["wallet_id"], value=json.dumps(ev))
        p.poll(0)
        gt["total_sent"] += 1

    start = time.time()
    end = start + a.duration
    next_burst = start + 10
    print("txn: %.1f ev/s, %ds, burst her %ds, son %ds burst yok" % (a.rate, a.duration, a.burst_interval, a.quiet_tail))
    last_log = start
    while time.time() < end:
        t0 = time.time()
        # --- fraud burst: 8-10 transactions on the same wallet in a short time
        if a.burst_interval > 0 and t0 >= next_burst and t0 < end - a.quiet_tail:
            widx = rng.randint(1, N_WALLETS)
            n = rng.randint(8, 10)
            gt["burst_wallets"].append({"wallet_id": wallet_id(widx), "txns": n, "at": iso(utcnow())})
            for _ in range(n):
                ev = new_event(rng, widx, utcnow())
                send(ev)
                gt["unique_txn_ids"] += 1
                time.sleep(0.25)
            next_burst = time.time() + a.burst_interval
            continue

        r = rng.random()
        if r < 0.03 and recent:  # duplicate (simulates at-least-once delivery)
            send(rng.choice(recent))
            gt["duplicates_sent"] += 1
        else:
            et = utcnow()
            if rng.random() < 0.04:  # late event: occurred 1-3 min ago
                et = et - timedelta(seconds=rng.uniform(60, 180))
                gt["late_events"] += 1
            ev = new_event(rng, rng.randint(1, N_WALLETS), et)
            recent.append(ev)
            send(ev)
            gt["unique_txn_ids"] += 1

        if t0 - last_log >= 15:
            print("  %3ds  gonderilen=%d (tekil=%d, dup=%d, gec=%d, burst=%d)" % (
                t0 - start, gt["total_sent"], gt["unique_txn_ids"], gt["duplicates_sent"],
                gt["late_events"], len(gt["burst_wallets"])))
            last_log = t0
        sleep = 1.0 / a.rate - (time.time() - t0)
        if sleep > 0:
            time.sleep(sleep)

    p.flush(30)
    gt["started_at"] = iso(datetime.fromtimestamp(start, timezone.utc))
    gt["ended_at"] = iso(utcnow())
    write_json("ground_truth_txn.json", gt)
    print("bitti:", {k: v for k, v in gt.items() if k != "burst_wallets"}, "burst_sayisi=%d" % len(gt["burst_wallets"]))


# ------------------------------------------------------------------------ cdc
def run_cdc(a):
    rng = random.Random(a.seed + 1)
    p = make_producer()
    lsn = [0]
    state = {}

    def row(widx, bal, status="ACTIVE"):
        return {"wallet_id": wallet_id(widx), "user_id": user_id(widx), "balance_kurus": bal, "status": status}

    def emit(op, before, after):
        lsn[0] += 1
        env = {
            "before": before,
            "after": after,
            "op": op,
            "ts_ms": int(time.time() * 1000),
            "source": {"db": "payments", "table": "wallets", "lsn": lsn[0]},
        }
        key = (after or before)["wallet_id"]
        p.produce("wallet_cdc", key=key, value=json.dumps(env))
        p.poll(0)
        if a.delay:
            time.sleep(a.delay)

    # 1) initial records (insert)
    for i in range(1, N_WALLETS + 1):
        state[i] = 100000  # 1000.00 TRY (in kurus, to avoid float errors)
        emit("c", None, row(i, state[i]))
    # 2) updates; sometimes several in a row on the same wallet (the "many updates in one micro-batch" scenario)
    done = 0
    while done < a.events:
        i = rng.choice(list(state))
        for _ in range(rng.choice([1, 1, 1, 3])):
            old = state[i]
            state[i] = max(0, old + rng.randint(-20000, 50000))
            emit("u", row(i, old), row(i, state[i]))
            done += 1
    # 3) deleted (closed) wallets
    deleted = rng.sample(list(state), a.deletes)
    for i in deleted:
        emit("d", row(i, state[i]), None)
        del state[i]
    p.flush(30)
    write_json("ground_truth_cdc.json", {
        "final_state": {wallet_id(i): b for i, b in state.items()},
        "deleted": [wallet_id(i) for i in deleted],
        "events": lsn[0],
    })
    print("cdc bitti: %d olay, %d aktif cuzdan, %d silinen" % (lsn[0], len(state), len(deleted)))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    t = sub.add_parser("txn")
    t.add_argument("--rate", type=float, default=5.0, help="saniyedeki olay sayisi")
    t.add_argument("--duration", type=int, default=240, help="saniye")
    t.add_argument("--burst-interval", type=int, default=15, help="fraud burst araligi (sn); 0 = burst yok")
    t.add_argument("--quiet-tail", type=int, default=150, help="sonda burst uretilmeyen sure (sn); watermark'in ilerlemesi icin")
    t.add_argument("--seed", type=int, default=7)
    t.set_defaults(fn=run_txn)
    c = sub.add_parser("cdc")
    c.add_argument("--events", type=int, default=600, help="guncelleme sayisi")
    c.add_argument("--deletes", type=int, default=5)
    c.add_argument("--delay", type=float, default=0.0)
    c.add_argument("--seed", type=int, default=7)
    c.set_defaults(fn=run_cdc)
    g = sub.add_parser("gen-wallets")
    g.add_argument("--out", default="wallets.csv")
    g.set_defaults(fn=gen_wallets)
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
