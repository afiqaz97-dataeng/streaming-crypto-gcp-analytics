"""
Local test for pipeline.py logic using Beam's TestStream + DirectRunner.
Does NOT touch GCP -- validates that stateful anomaly detection and fixed
windowing produce correct results before deploying to Dataflow.

Run: python test_pipeline.py
"""

import json
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import FixedWindows, TimestampedValue
from apache_beam.utils.timestamp import Timestamp

from pipeline import DetectAnomalyStateful, PriceStats


class TestStatefulAnomalyDetection(unittest.TestCase):
    def test_flags_large_price_jump(self):
        events = [
            ("BTC", {"symbol": "BTC", "price_usd": 100.0, "timestamp": "t1", "injected_anomaly": False}),
            ("BTC", {"symbol": "BTC", "price_usd": 103.0, "timestamp": "t2", "injected_anomaly": False}),  # +3% -> anomaly
            ("BTC", {"symbol": "BTC", "price_usd": 103.5, "timestamp": "t3", "injected_anomaly": False}),  # +0.49% -> ok
        ]

        with TestPipeline() as p:
            result = (
                p
                | beam.Create(events)
                | beam.ParDo(DetectAnomalyStateful())
                | beam.Map(lambda r: (r["symbol"], r["price_usd"], r["is_anomaly"]))
            )
            assert_that(result, equal_to([
                ("BTC", 100.0, False),
                ("BTC", 103.0, True),
                ("BTC", 103.5, False),
            ]))
        print("PASS: stateful anomaly detection correctly flags >1.5% jumps")


class TestWindowedAggregation(unittest.TestCase):
    def test_fixed_window_stats(self):
        # 3 events for BTC, all within one 30s window, timestamps 0s/5s/10s
        with TestPipeline() as p:
            stream = (
                TestStream()
                .add_elements([
                    TimestampedValue(("BTC", 100.0), 0),
                    TimestampedValue(("BTC", 110.0), 5),
                    TimestampedValue(("BTC", 90.0), 10),
                ])
                .advance_watermark_to_infinity()
            )
            result = (
                p
                | stream
                | beam.WindowInto(FixedWindows(30))
                | beam.CombinePerKey(PriceStats())
            )

            def check(results):
                results = list(results)
                assert len(results) == 1, f"expected 1 windowed result, got {len(results)}"
                symbol, stats = results[0]
                assert symbol == "BTC"
                assert stats["avg_price"] == 100.0, stats
                assert stats["min_price"] == 90.0, stats
                assert stats["max_price"] == 110.0, stats
                assert stats["volatility"] == 20.0, stats
                assert stats["event_count"] == 3, stats

            assert_that(result, check)
        print("PASS: fixed window correctly aggregates avg/min/max/volatility")


if __name__ == "__main__":
    unittest.main()
