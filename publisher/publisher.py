"""
Crypto price publisher for streaming analytics pipeline.

Polls CoinGecko's free public API every POLL_INTERVAL seconds for BTC, ETH,
SOL prices and publishes each reading as a JSON message to a Pub/Sub topic.

To make anomaly detection demoable on demand, this script randomly injects
a fake price spike/drop every so often (controlled by ANOMALY_CHANCE). This
is clearly logged so it's obvious in the console which readings are real vs.
injected -- useful for demo/interview purposes, not a source of truth.

Usage:
    python publisher.py --project_id crypto-etl-project-465203
"""

import argparse
import json
import random
import time
from datetime import datetime, timezone

import requests
from google.cloud import pubsub_v1

COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
COIN_IDS = {
    "bitcoin": "BTC",
    "ethereum": "ETH",
    "solana": "SOL",
}
POLL_INTERVAL_SECONDS = 30  # CoinGecko free tier is rate-limited; keep this conservative
ANOMALY_CHANCE = 0.15  # ~15% of readings get an injected spike/drop
ANOMALY_MAGNITUDE = (0.03, 0.08)  # 3%-8% fake price jump
MAX_RETRIES = 3


def fetch_prices() -> dict:
    """Fetch current prices for tracked coins from CoinGecko, with retry
    and exponential backoff on rate limiting (HTTP 429)."""
    params = {"ids": ",".join(COIN_IDS.keys()), "vs_currencies": "usd"}
    backoff = 15
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(COINGECKO_URL, params=params, timeout=10)
            if resp.status_code == 429:
                print(f"Rate limited (attempt {attempt}/{MAX_RETRIES}), backing off {backoff}s...")
                time.sleep(backoff)
                backoff *= 2
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            print(f"CoinGecko fetch failed (attempt {attempt}/{MAX_RETRIES}): {e}")
            time.sleep(backoff)
            backoff *= 2
    raise RuntimeError("CoinGecko fetch failed after all retries")


def maybe_inject_anomaly(price: float) -> tuple[float, bool]:
    """Randomly perturb a price to simulate an anomaly. Returns (price, is_injected)."""
    if random.random() < ANOMALY_CHANCE:
        pct = random.uniform(*ANOMALY_MAGNITUDE)
        direction = random.choice([1, -1])
        new_price = price * (1 + direction * pct)
        return round(new_price, 2), True
    return price, False


def build_message(symbol: str, price: float, injected: bool) -> dict:
    return {
        "symbol": symbol,
        "price_usd": price,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "injected_anomaly": injected,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", required=True, help="GCP project ID")
    parser.add_argument("--topic_id", default="crypto-prices", help="Pub/Sub topic ID")
    args = parser.parse_args()

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(args.project_id, args.topic_id)

    print(f"Publishing to {topic_path}")
    print(f"Polling every {POLL_INTERVAL_SECONDS}s for {list(COIN_IDS.values())}")
    print("Press Ctrl+C to stop.\n")

    while True:
        try:
            prices = fetch_prices()
            for coin_id, symbol in COIN_IDS.items():
                raw_price = prices[coin_id]["usd"]
                price, injected = maybe_inject_anomaly(raw_price)
                message = build_message(symbol, price, injected)

                data = json.dumps(message).encode("utf-8")
                future = publisher.publish(topic_path, data)
                future.result()  # wait for publish confirmation

                tag = " [INJECTED ANOMALY]" if injected else ""
                print(f"Published {symbol}: ${price}{tag}")

        except requests.RequestException as e:
            print(f"CoinGecko fetch failed: {e}")
        except RuntimeError as e:
            print(f"Giving up this cycle: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()