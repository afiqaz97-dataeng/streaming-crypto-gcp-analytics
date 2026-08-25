#!/usr/bin/env bash
# Deploy script for the crypto streaming analytics pipeline (Dataflow version).
# Run this in Cloud Shell (already authenticated to your GCP project).
#
# This script provisions infra (Pub/Sub, GCS, BigQuery) but does NOT launch
# the Dataflow job itself -- that's a long-running streaming job, so it's
# launched separately (see step printed at the end).
#
# Usage: bash deploy.sh

set -euo pipefail

PROJECT_ID="crypto-etl-project-465203"
REGION="us-central1"
TOPIC_ID="crypto-prices"
SUBSCRIPTION_ID="crypto-prices-sub"
DATASET_ID="crypto_analytics"
RAW_TABLE_ID="crypto_prices_raw"
WINDOW_TABLE_ID="crypto_price_windows"
BUCKET_NAME="${PROJECT_ID}-dataflow"

echo "== Setting active project =="
gcloud config set project "$PROJECT_ID"

echo "== Enabling required APIs =="
gcloud services enable \
  pubsub.googleapis.com \
  dataflow.googleapis.com \
  bigquery.googleapis.com \
  storage.googleapis.com \
  compute.googleapis.com

echo "== Creating Pub/Sub topic + subscription =="
gcloud pubsub topics create "$TOPIC_ID" || echo "Topic already exists, skipping."
gcloud pubsub subscriptions create "$SUBSCRIPTION_ID" --topic="$TOPIC_ID" || echo "Subscription already exists, skipping."

echo "== Creating GCS bucket for Dataflow staging/temp =="
gsutil mb -l "$REGION" "gs://${BUCKET_NAME}" || echo "Bucket already exists, skipping."

echo "== Creating BigQuery dataset =="
bq mk --dataset --location=US "${PROJECT_ID}:${DATASET_ID}" || echo "Dataset already exists, skipping."

echo "== Creating BigQuery tables =="
bq mk --table "${PROJECT_ID}:${DATASET_ID}.${RAW_TABLE_ID}" schema_raw.json \
  || echo "Raw table already exists, skipping."
bq mk --table "${PROJECT_ID}:${DATASET_ID}.${WINDOW_TABLE_ID}" schema_windows.json \
  || echo "Windowed table already exists, skipping."

echo ""
echo "== Infra provisioning complete =="
echo ""
echo "Next steps:"
echo "1. Start the publisher (in a Cloud Shell tab):"
echo "   cd publisher && pip install -r requirements.txt --break-system-packages"
echo "   python publisher.py --project_id=$PROJECT_ID --topic_id=$TOPIC_ID"
echo ""
echo "2. In a second Cloud Shell tab, launch the Dataflow job:"
echo "   cd dataflow && pip install -r requirements.txt --break-system-packages"
echo "   python pipeline.py \\"
echo "       --project=$PROJECT_ID \\"
echo "       --region=$REGION \\"
echo "       --input_subscription=projects/$PROJECT_ID/subscriptions/$SUBSCRIPTION_ID \\"
echo "       --runner=DataflowRunner \\"
echo "       --temp_location=gs://${BUCKET_NAME}/temp \\"
echo "       --staging_location=gs://${BUCKET_NAME}/staging \\"
echo "       --streaming \\"
echo "       --requirements_file=requirements.txt"
echo ""
echo "   This takes ~3-5 min to spin up worker VMs. Track progress at:"
echo "   https://console.cloud.google.com/dataflow/jobs?project=$PROJECT_ID"
echo ""
echo "3. Once running, check BigQuery for data:"
echo "   bq query --use_legacy_sql=false 'SELECT * FROM \`${PROJECT_ID}.${DATASET_ID}.${RAW_TABLE_ID}\` ORDER BY timestamp DESC LIMIT 20'"
echo "   bq query --use_legacy_sql=false 'SELECT * FROM \`${PROJECT_ID}.${DATASET_ID}.${WINDOW_TABLE_ID}\` ORDER BY window_start DESC LIMIT 20'"
echo ""
echo "4. Connect Looker Studio to both BigQuery tables for dashboards."
echo ""
echo "IMPORTANT: Dataflow jobs keep running (and billing) until you cancel them."
echo "When done: gcloud dataflow jobs list --region=$REGION   (then 'cancel' the job ID)"
