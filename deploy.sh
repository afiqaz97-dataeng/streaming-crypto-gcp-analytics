#!/usr/bin/env bash
# Deploy script for the crypto streaming analytics pipeline.
# Run this in Cloud Shell (already authenticated to your GCP project).
#
# Usage: bash deploy.sh

set -euo pipefail

PROJECT_ID="crypto-etl-project-465203"
REGION="us-central1"
TOPIC_ID="crypto-prices"
DATASET_ID="crypto_analytics"
TABLE_ID="crypto_prices"
FUNCTION_NAME="crypto-price-processor"

echo "== Setting active project =="
gcloud config set project "$PROJECT_ID"

echo "== Enabling required APIs =="
gcloud services enable \
  pubsub.googleapis.com \
  cloudfunctions.googleapis.com \
  cloudbuild.googleapis.com \
  bigquery.googleapis.com \
  run.googleapis.com \
  eventarc.googleapis.com

echo "== Creating Pub/Sub topic =="
gcloud pubsub topics create "$TOPIC_ID" || echo "Topic already exists, skipping."

echo "== Creating BigQuery dataset =="
bq mk --dataset --location=US "${PROJECT_ID}:${DATASET_ID}" || echo "Dataset already exists, skipping."

echo "== Creating BigQuery table =="
bq mk --table \
  "${PROJECT_ID}:${DATASET_ID}.${TABLE_ID}" \
  schema.json || echo "Table already exists, skipping."

echo "== Deploying Cloud Function (Gen 2, Pub/Sub triggered) =="
gcloud functions deploy "$FUNCTION_NAME" \
  --gen2 \
  --runtime=python312 \
  --region="$REGION" \
  --source=./cloud_function \
  --entry-point=process_price_event \
  --trigger-topic="$TOPIC_ID" \
  --set-env-vars=PROJECT_ID="$PROJECT_ID",BQ_DATASET="$DATASET_ID",BQ_TABLE="$TABLE_ID" \
  --memory=256MB \
  --timeout=60s

echo ""
echo "== Deploy complete =="
echo "Next steps:"
echo "1. cd publisher && pip install -r requirements.txt --break-system-packages"
echo "2. python publisher.py --project_id=$PROJECT_ID --topic_id=$TOPIC_ID"
echo "3. Check BigQuery table for streaming rows:"
echo "   bq query --use_legacy_sql=false 'SELECT * FROM \`${PROJECT_ID}.${DATASET_ID}.${TABLE_ID}\` ORDER BY timestamp DESC LIMIT 20'"
echo "4. Connect Looker Studio to the BigQuery table for a live dashboard."
