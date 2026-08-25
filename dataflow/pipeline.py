"""
Crypto streaming analytics pipeline (Apache Beam / Dataflow).

Reads price events published by publisher.py from a Pub/Sub subscription and
processes them two ways in parallel:

1. RAW ANOMALY DETECTION (stateful DoFn)
   Keyed by symbol, keeps the *previous price in Beam state* (not a database
   lookup) and flags an event as anomalous if the price moved more than
   ANOMALY_THRESHOLD_PCT since the last event for that symbol. Demonstrates
   Beam's per-key state API (ReadModifyWriteStateSpec).
   -> written to BigQuery table: crypto_prices_raw

2. WINDOWED AGGREGATION (fixed windows + CombinePerKey)
   Groups events into WINDOW_SIZE_SECONDS fixed windows per symbol and
   computes avg/min/max/count/volatility (max-min spread) per window.
   Demonstrates FixedWindows, GroupByKey/CombinePerKey, and extracting
   window start/end timestamps.
   -> written to BigQuery table: crypto_price_windows

Run locally (DirectRunner):
    python pipeline.py \
        --project=crypto-etl-project-465203 \
        --input_subscription=projects/crypto-etl-project-465203/subscriptions/crypto-prices-sub \
        --runner=DirectRunner \
        --streaming

Run on Dataflow:
    python pipeline.py \
        --project=crypto-etl-project-465203 \
        --region=us-central1 \
        --input_subscription=projects/crypto-etl-project-465203/subscriptions/crypto-prices-sub \
        --runner=DataflowRunner \
        --temp_location=gs://crypto-etl-project-465203-dataflow/temp \
        --staging_location=gs://crypto-etl-project-465203-dataflow/staging \
        --streaming \
        --requirements_file=requirements.txt
"""

import argparse
import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.coders import FloatCoder
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterWatermark, AccumulationMode

WINDOW_SIZE_SECONDS = 30
ANOMALY_THRESHOLD_PCT = 1.5

RAW_TABLE_SCHEMA = (
    "symbol:STRING, price_usd:FLOAT, timestamp:TIMESTAMP, "
    "pct_change:FLOAT, is_anomaly:BOOLEAN"
)

WINDOW_TABLE_SCHEMA = (
    "symbol:STRING, window_start:TIMESTAMP, window_end:TIMESTAMP, "
    "avg_price:FLOAT, min_price:FLOAT, max_price:FLOAT, "
    "volatility:FLOAT, event_count:INTEGER"
)


def parse_message(raw_bytes: bytes) -> dict:
    """Parse a Pub/Sub message payload into a dict, keyed by symbol."""
    payload = json.loads(raw_bytes.decode("utf-8"))
    return payload


class DetectAnomalyStateful(beam.DoFn):
    """Flags a price event as anomalous by comparing it to the previous
    price for the same key, held in Beam per-key state.

    This is the "stateful streaming" building block: Beam persists
    LAST_PRICE across elements for each symbol, even across worker
    restarts / autoscaling, without needing an external database lookup.
    """

    LAST_PRICE = ReadModifyWriteStateSpec("last_price", FloatCoder())

    def process(self, element, last_price_state=beam.DoFn.StateParam(LAST_PRICE)):
        symbol, payload = element
        price = float(payload["price_usd"])
        previous_price = last_price_state.read()

        if previous_price is None:
            pct_change = 0.0
        else:
            pct_change = round((price - previous_price) / previous_price * 100, 4)

        injected_flag = payload.get("injected_anomaly", False)
        is_anomaly = injected_flag or abs(pct_change) >= ANOMALY_THRESHOLD_PCT

        last_price_state.write(price)

        yield {
            "symbol": symbol,
            "price_usd": price,
            "timestamp": payload["timestamp"],
            "pct_change": pct_change,
            "is_anomaly": is_anomaly,
        }


class PriceStats(beam.CombineFn):
    """CombineFn that computes count/sum/min/max for a window of prices,
    used to derive avg and volatility (max - min) per symbol per window.
    """

    def create_accumulator(self):
        return {"count": 0, "sum": 0.0, "min": None, "max": None}

    def add_input(self, accumulator, price):
        accumulator["count"] += 1
        accumulator["sum"] += price
        accumulator["min"] = price if accumulator["min"] is None else min(accumulator["min"], price)
        accumulator["max"] = price if accumulator["max"] is None else max(accumulator["max"], price)
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for acc in accumulators:
            merged["count"] += acc["count"]
            merged["sum"] += acc["sum"]
            if acc["min"] is not None:
                merged["min"] = acc["min"] if merged["min"] is None else min(merged["min"], acc["min"])
            if acc["max"] is not None:
                merged["max"] = acc["max"] if merged["max"] is None else max(merged["max"], acc["max"])
        return merged

    def extract_output(self, accumulator):
        if accumulator["count"] == 0:
            return None
        avg = accumulator["sum"] / accumulator["count"]
        volatility = accumulator["max"] - accumulator["min"]
        return {
            "avg_price": round(avg, 4),
            "min_price": accumulator["min"],
            "max_price": accumulator["max"],
            "volatility": round(volatility, 4),
            "event_count": accumulator["count"],
        }


def attach_window_info(element, window=beam.DoFn.WindowParam):
    """Attach the fixed window's start/end timestamps to a windowed stats row."""
    symbol, stats = element
    window_start = window.start.to_utc_datetime().isoformat()
    window_end = window.end.to_utc_datetime().isoformat()
    return {
        "symbol": symbol,
        "window_start": window_start,
        "window_end": window_end,
        **stats,
    }


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_subscription", required=True,
                         help="Pub/Sub subscription, e.g. projects/PROJECT/subscriptions/crypto-prices-sub")
    parser.add_argument("--output_dataset", default="crypto_analytics")
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True
    project = options.get_all_options().get("project")

    raw_table = f"{project}:{known_args.output_dataset}.crypto_prices_raw"
    window_table = f"{project}:{known_args.output_dataset}.crypto_price_windows"

    with beam.Pipeline(options=options) as pipeline:
        events = (
            pipeline
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=known_args.input_subscription)
            | "ParseJSON" >> beam.Map(parse_message)
            | "KeyBySymbol" >> beam.Map(lambda payload: (payload["symbol"], payload))
        )

        # --- Branch 1: stateful anomaly detection on the raw stream ---
        (
            events
            | "DetectAnomaly" >> beam.ParDo(DetectAnomalyStateful())
            | "WriteRawToBQ" >> beam.io.WriteToBigQuery(
                raw_table,
                schema=RAW_TABLE_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
              )
        )

        # --- Branch 2: fixed-window aggregation per symbol ---
        (
            events
            | "ExtractPrice" >> beam.MapTuple(lambda symbol, payload: (symbol, float(payload["price_usd"])))
            | "FixedWindow" >> beam.WindowInto(
                FixedWindows(WINDOW_SIZE_SECONDS),
                trigger=AfterWatermark(),
                accumulation_mode=AccumulationMode.DISCARDING,
              )
            | "CombinePerSymbol" >> beam.CombinePerKey(PriceStats())
            | "AttachWindowInfo" >> beam.Map(attach_window_info)
            | "WriteWindowsToBQ" >> beam.io.WriteToBigQuery(
                window_table,
                schema=WINDOW_TABLE_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
              )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
