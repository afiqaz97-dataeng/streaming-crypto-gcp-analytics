"""
Cloud Function (Gen 2, Pub/Sub triggered): crypto price processor.

Consumes a price reading published by publisher.py, looks up the previous
price for that coin in BigQuery, computes % change, flags it as an anomaly
if the move exceeds ANOMALY_THRESHOLD_PCT (or if the publisher already
tagged it as an injected anomaly), then streams the enriched row into
BigQuery.

Deployed as an event-driven function triggered by messages on the
`crypto-prices` Pub/Sub topic.
"""

import base64
import json
import os

from google.cloud import bigquery

PROJECT_ID = os.environ.get("PROJECT_ID", os.environ.get("GOOGLE_CLOUD_PROJECT"))
DATASET_ID = os.environ.get("BQ_DATASET", "crypto_analytics")
TABLE_ID = os.environ.get("BQ_TABLE", "crypto_prices")
ANOMALY_THRESHOLD_PCT = 1.5  # flag if price moved more than this % since last reading

bq_client = bigquery.Client()


def get_last_price(symbol: str):
    """Return the most recent price for a symbol, or None if no history yet."""
    query = f"""
        SELECT price_usd
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE symbol = @symbol
        ORDER BY timestamp DESC
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("symbol", "STRING", symbol)]
    )
    results = list(bq_client.query(query, job_config=job_config).result())
    return results[0]["price_usd"] if results else None


def compute_pct_change(current: float, previous: float) -> float:
    if previous in (None, 0):
        return 0.0
    return round((current - previous) / previous * 100, 4)


def insert_row(row: dict):
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    errors = bq_client.insert_rows_json(table_ref, [row])
    if errors:
        raise RuntimeError(f"BigQuery insert failed: {errors}")


def process_price_event(cloud_event):
    """Entry point for the Pub/Sub-triggered Cloud Function (Gen 2)."""
    envelope = cloud_event.data
    pubsub_message = envelope["message"]

    data = base64.b64decode(pubsub_message["data"]).decode("utf-8")
    payload = json.loads(data)

    symbol = payload["symbol"]
    price = float(payload["price_usd"])
    timestamp = payload["timestamp"]
    injected_flag = payload.get("injected_anomaly", False)

    previous_price = get_last_price(symbol)
    pct_change = compute_pct_change(price, previous_price)
    is_anomaly = injected_flag or abs(pct_change) >= ANOMALY_THRESHOLD_PCT

    row = {
        "symbol": symbol,
        "price_usd": price,
        "timestamp": timestamp,
        "pct_change": pct_change,
        "is_anomaly": is_anomaly,
    }

    insert_row(row)
    tag = "ANOMALY" if is_anomaly else "ok"
    print(f"[{tag}] {symbol} ${price} ({pct_change:+.2f}%)")
