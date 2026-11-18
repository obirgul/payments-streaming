"""Shared fixtures: one SparkSession for all test modules, small DataFrame builders."""
import json
import unittest

from pyspark.sql import types as T

from common import get_spark, parse_transactions

BASE = "2026-01-01T10:00:00.000Z"


class SparkTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark("unit-tests", delta=True)

    def json_df(self, events):
        """List of dicts -> frame with a Kafka-style `value` column (JSON strings)."""
        schema = T.StructType([T.StructField("value", T.StringType())])
        return self.spark.createDataFrame([(e if isinstance(e, str) else json.dumps(e),) for e in events], schema)

    def txns(self, events):
        """List of transaction dicts -> typed transactions, parsed exactly like the Kafka source does."""
        return parse_transactions(self.json_df(events))


def txn(txn_id, wallet_id="W0001", amount=100.0, event_time=BASE, **extra):
    ev = {"txn_id": txn_id, "wallet_id": wallet_id, "user_id": "U10001", "amount": amount, "currency": "TRY",
          "merchant_id": "M001", "country": "TR", "channel": "app", "event_time": event_time}
    ev.update(extra)
    return ev


def stream_collect(case, events, transform):
    """Runs `transform` on a real streaming query (text file source -> memory sink, availableNow) and returns the rows.
    For logic that has no batch mode, such as dropDuplicatesWithinWatermark."""
    import os
    import shutil
    import tempfile
    from common import parse_transactions

    d = tempfile.mkdtemp(prefix="stream_in_")
    case.addCleanup(shutil.rmtree, d, True)
    with open(os.path.join(d, "events.jsonl"), "w") as f:
        f.write("\n".join(e if isinstance(e, str) else json.dumps(e) for e in events) + "\n")
    name = "sink_%d" % id(events)
    src = case.spark.readStream.format("text").load(d)
    q = (transform(parse_transactions(src)).writeStream.format("memory").queryName(name)
         .outputMode("append").trigger(availableNow=True).start())
    q.awaitTermination()
    return case.spark.table(name).collect()
