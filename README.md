# Crypto Streaming Analytics Pipeline

A real-time streaming analytics pipeline on Google Cloud Platform that ingests
live cryptocurrency prices (BTC, ETH, SOL), detects anomalous price
movements, and surfaces them in a live dashboard.

## Architecture

```
 ┌────────────────────┐
 │  publisher.py       │   Polls CoinGecko every 10s, occasionally
 │  (Python script)    │   injects a fake spike/drop for demo purposes
 └──────────┬───────────┘
            │ publishes JSON
            ▼
 ┌────────────────────┐
 │  Pub/Sub            │   Topic: crypto-prices
 │  (message queue)    │
 └──────────┬───────────┘
            │ triggers
            ▼
 ┌────────────────────┐
 │  Cloud Function     │   Gen 2, Pub/Sub-triggered
 │  (Gen 2)            │   - Looks up last price for the coin in BigQuery
 │                      │   - Computes % change
 │                      │   - Flags anomaly if |% change| >= 1.5%
 └──────────┬───────────┘
            │ streaming insert
            ▼
 ┌────────────────────┐
 │  BigQuery            │   Table: crypto_analytics.crypto_prices
 │  (data warehouse)    │
 └──────────┬───────────┘
            │
            ▼
 ┌────────────────────┐
 │  Looker Studio       │   Live line chart of prices + anomaly markers
 │  (dashboard)          │
 └────────────────────┘
```

## Why this architecture

This pipeline intentionally uses **Cloud Functions instead of Dataflow** for
the processing layer. For a low-throughput stream like this (3 coins, one
reading every ~10s), Dataflow's windowing/watermark machinery is more
operational overhead than the workload needs. An event-driven Cloud
Function keeps the pipeline simple, cheap (mostly within the free tier), and
easy to reason about, while still demonstrating the core streaming pattern:
**ingest → process → store → visualize**, decoupled through a message queue.

If throughput grew (many more symbols, sub-second ticks, or a need for
stateful windowed aggregations like rolling VWAP), Dataflow would be the
natural next step — the Pub/Sub topic this pipeline already publishes to
could feed a Beam pipeline without changing the ingestion layer at all.

## Anomaly detection logic

Each incoming price is compared to the last stored price for that symbol.
A row is flagged `is_anomaly = true` if:
- the price moved more than **1.5%** since the last reading, OR
- the publisher tagged it as an injected demo anomaly

This is a simple threshold rule by design — easy to explain, easy to verify
correctness of, and a reasonable v1 for a real system. A natural extension
would be a rolling z-score or an anomaly-detection model served from Vertex AI.

## Project structure

```
crypto-streaming-analytics/
├── publisher/
│   ├── publisher.py        # Polls CoinGecko, publishes to Pub/Sub
│   └── requirements.txt
├── cloud_function/
│   ├── main.py              # Pub/Sub-triggered function -> BigQuery
│   └── requirements.txt
├── schema.json               # BigQuery table schema
├── deploy.sh                 # One-shot deploy script for Cloud Shell
└── README.md
```

## Setup (Google Cloud Shell)

1. Clone this repo and `cd` into it in Cloud Shell.
2. Make sure billing is enabled on your project.
3. Run the deploy script:
   ```bash
   bash deploy.sh
   ```
   This enables the required APIs, creates the Pub/Sub topic, creates the
   BigQuery dataset/table, and deploys the Cloud Function.
4. Start the publisher (in Cloud Shell or locally with `gcloud auth
   application-default login`):
   ```bash
   cd publisher
   pip install -r requirements.txt --break-system-packages
   python publisher.py --project_id=crypto-etl-project-465203
   ```
5. Watch data land in BigQuery:
   ```bash
   bq query --use_legacy_sql=false \
     'SELECT * FROM `crypto-etl-project-465203.crypto_analytics.crypto_prices`
      ORDER BY timestamp DESC LIMIT 20'
   ```
6. Connect [Looker Studio](https://lookerstudio.google.com) to the
   `crypto_analytics.crypto_prices` BigQuery table and build a time-series
   chart of `price_usd` by `symbol`, with `is_anomaly` as a marker/filter.

## Cost notes

- **Pub/Sub**: free tier covers 10GB/month, this pipeline uses a fraction of that.
- **Cloud Functions**: free tier covers 2M invocations/month; at 3 messages
  every 10s this is ~26k invocations/month.
- **BigQuery**: streaming inserts and storage for this data volume are
  effectively free-tier; querying is billed per TB scanned (negligible here).
- **Looker Studio**: free.

Realistically this pipeline costs **$0** to run for a portfolio demo, as
long as you stop the publisher script when you're done (it's the only
long-running process).

## Possible extensions

- Swap Cloud Function for a Dataflow (Apache Beam) pipeline to support
  windowed aggregations (rolling VWAP, volatility bands).
- Add Slack/email alerting on anomalies via a second Pub/Sub topic.
- Add Terraform to provision all infra as code.
- Add a Vertex AI-based anomaly model instead of a fixed threshold.
