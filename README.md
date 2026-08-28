# Crypto Streaming Analytics Pipeline (Apache Beam / Dataflow)

A real-time streaming analytics pipeline on GCP that ingests live
cryptocurrency prices (BTC, ETH, SOL), runs them through an Apache Beam
pipeline on Dataflow, and produces two outputs: a per-event anomaly-flagged
stream (stateful processing) and windowed price statistics (fixed windows +
CombinePerKey).

## Architecture

```
 ┌────────────────────┐
 │  publisher.py       │   Polls CoinGecko every 10s, occasionally
 │  (Python script)    │   injects a fake spike/drop for demo purposes
 └──────────┬───────────┘
            │ publishes JSON
            ▼
 ┌────────────────────┐
 │  Pub/Sub             │   Topic: crypto-prices
 │  (message queue)     │   Subscription: crypto-prices-sub
 └──────────┬───────────┘
            │
            ▼
 ┌─────────────────────────────────────────────┐
 │  Dataflow (Apache Beam, Python)              │
 │                                               │
 │   ┌─────────────────────────────────────┐   │
 │   │ Branch 1: Stateful anomaly detection │   │
 │   │ - Keyed by symbol                     │   │
 │   │ - Beam per-key state holds last price │   │
 │   │ - Flags if |% change| >= 1.5%         │   │
 │   └──────────────┬──────────────────────┘   │
 │                  │                            │
 │   ┌──────────────▼──────────────────────┐   │
 │   │ Branch 2: Windowed aggregation       │   │
 │   │ - FixedWindows(30s), keyed by symbol │   │
 │   │ - CombinePerKey: avg/min/max/volatility│  │
 │   └──────────────┬──────────────────────┘   │
 └──────────────────┼────────────────────────────┘
                     │
        ┌────────────┴────────────┐
        ▼                          ▼
 ┌──────────────┐          ┌───────────────────┐
 │ BigQuery      │          │ BigQuery           │
 │ crypto_prices_│          │ crypto_price_      │
 │ raw           │          │ windows             │
 └──────┬────────┘          └─────────┬──────────┘
        │                              │
        └──────────────┬───────────────┘
                        ▼
                ┌──────────────┐
                │ Looker Studio │
                │ (dashboards)  │
                └──────────────┘
```

## Why this architecture

This version deliberately trades the simplicity of a Cloud Function for
**Dataflow + Apache Beam**, to demonstrate two core streaming-processing
concepts that a simple pub/sub-triggered function can't show:

1. **Stateful processing** — the anomaly detector doesn't query a database
   for the last price; it holds it in Beam's per-key state
   (`ReadModifyWriteStateSpec`), which Beam manages transparently across
   worker autoscaling and restarts. This is the pattern behind things like
   session tracking, deduplication, and running aggregates in real systems.

2. **Windowing** — `FixedWindows(30s)` groups events into time buckets per
   symbol, and `CombinePerKey` reduces each window to avg/min/max/volatility.
   This is the same primitive used for things like "requests per minute" or
   "revenue per hour" in production analytics pipelines, and it's usually
   the concept interviewers probe hardest on.

The pipeline logic is unit-tested locally with `TestStream` and
`DirectRunner` before ever touching Dataflow (see `dataflow/test_pipeline.py`)
— both the stateful anomaly logic and the windowed aggregation are verified
independent of GCP infra.

## Anomaly detection logic

Each event is compared to the previous price *for that symbol*, held in
Beam state. A row is flagged `is_anomaly = true` if:
- the price moved more than **1.5%** since the last event, OR
- the publisher tagged it as an injected demo anomaly

## Windowed aggregation logic

Every **30 seconds**, per symbol, the pipeline emits:
- `avg_price`, `min_price`, `max_price`
- `volatility` (max − min within the window)
- `event_count`

Windows use the default `AfterWatermark` trigger with `DISCARDING`
accumulation mode — each window fires exactly once, when Beam's watermark
passes the window's end. A natural extension (noted below) is adding early
firings for a live "in-progress window" view.

## Project structure

```
crypto-streaming-analytics/
├── publisher/
│   ├── publisher.py          # Polls CoinGecko, publishes to Pub/Sub
│   └── requirements.txt
├── dataflow/
│   ├── pipeline.py           # Beam pipeline: stateful DoFn + windowing
│   ├── test_pipeline.py      # Local unit tests (TestStream, DirectRunner)
│   └── requirements.txt
├── schema_raw.json           # BigQuery schema: crypto_prices_raw
├── schema_windows.json       # BigQuery schema: crypto_price_windows
├── deploy.sh                 # Provisions Pub/Sub, GCS, BigQuery infra
└── README.md
```

## Setup (Google Cloud Shell)

1. Clone this repo and `cd` into it in Cloud Shell.
2. Make sure billing is enabled on your project.
3. Provision infra:
   ```bash
   bash deploy.sh
   ```
   This enables required APIs and creates the Pub/Sub topic/subscription,
   GCS staging bucket, and both BigQuery tables.
4. (Optional but recommended) Run the local pipeline tests first:
   ```bash
   cd dataflow
   pip install -r requirements.txt --break-system-packages
   python test_pipeline.py -v
   ```
5. Start the publisher (Cloud Shell tab 1):
   ```bash
   cd publisher
   pip install -r requirements.txt --break-system-packages
   python publisher.py --project_id=crypto-etl-project-465203
   ```
6. Launch the Dataflow job (Cloud Shell tab 2) — `deploy.sh` prints the
   exact command with your project/bucket filled in. It takes ~3-5 minutes
   for Dataflow to spin up worker VMs; track it at
   `console.cloud.google.com/dataflow/jobs`.
7. Query BigQuery to confirm data is flowing (commands printed by `deploy.sh`).
8. Connect Looker Studio to `crypto_prices_raw` (live price + anomaly
   markers) and `crypto_price_windows` (volatility over time).

## Cost notes

- **Dataflow** is the main cost here — a streaming job runs continuously on
  at least one worker VM (n1-standard-1 by default) until you cancel it.
  Budget roughly $0.05-$0.10/hour for a minimal streaming job. **Cancel the
  job when you're done demoing**: `gcloud dataflow jobs list --region=us-central1`
  then `gcloud dataflow jobs cancel <JOB_ID> --region=us-central1`.
- **Pub/Sub, BigQuery, GCS**: negligible at this data volume, within free tier.
- **Looker Studio**: free.
