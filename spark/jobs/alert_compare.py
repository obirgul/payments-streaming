"""Compares fraud alerts of two engines (Spark vs Flink) that implement the same velocity rule."""


def compare_alerts(spark_alerts, flink_alerts):
    """Each argument: iterable of (wallet_id, window_start, txn_count).

    Spark update mode can emit the same window several times (growing count), so per window the max count is used.
    Returns windows only one engine reported and windows where the counts differ, split by direction:
    spark_higher = Spark counted more (expected for late events when Spark runs with --available-now: it processes
                   everything in one batch, so the watermark has not advanced yet and late events are still counted,
                   while Flink drops events later than its 2 min watermark),
    flink_higher = Flink counted more (never expected: a real discrepancy).
    """
    def collapse(rows):
        out = {}
        for wallet_id, window_start, count in rows:
            key = (wallet_id, window_start)
            out[key] = max(out.get(key, 0), count)
        return out

    s, f = collapse(spark_alerts), collapse(flink_alerts)
    return {
        "only_spark": sorted(set(s) - set(f)),
        "only_flink": sorted(set(f) - set(s)),
        "count_mismatch": sorted((k, s[k], f[k]) for k in set(s) & set(f) if s[k] != f[k]),
        "spark_higher": sorted((k, s[k], f[k]) for k in set(s) & set(f) if s[k] > f[k]),
        "flink_higher": sorted((k, s[k], f[k]) for k in set(s) & set(f) if s[k] < f[k]),
        "common": len(set(s) & set(f)),
    }
