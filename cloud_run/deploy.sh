#!/bin/bash

# Set variables
PROJECT_ID=weedme-379116
SERVICE_NAME=crm-accounts-pipeline
REGION=northamerica-northeast1  # Montreal

# Build the container using Cloud Build
echo "Building container image using Cloud Build..."
gcloud builds submit --tag gcr.io/$PROJECT_ID/$SERVICE_NAME .

# Deploy to Cloud Run
echo "Deploying to Cloud Run in $REGION..."
gcloud run deploy $SERVICE_NAME \
  --image gcr.io/$PROJECT_ID/$SERVICE_NAME \
  --platform managed \
  --region $REGION \
  --memory 512Mi \
  --no-allow-unauthenticated \
  --set-env-vars="PROJECT_ID=$PROJECT_ID"

# Set up Cloud Scheduler (optional)
# echo "Setting up Cloud Scheduler for daily runs..."
# gcloud scheduler jobs create http crm-data-daily-job \
#   --schedule="0 1 * * *" \
#   --uri="$(gcloud run services describe $SERVICE_NAME --platform managed --region $REGION --format='value(status.url)')" \
#   --oidc-service-account-email="$PROJECT_ID@appspot.gserviceaccount.com" \
#   --oidc-token-audience="$(gcloud run services describe $SERVICE_NAME --platform managed --region $REGION --format='value(status.url)')" \
#   --http-method=POST

echo "Deployment completed successfully!" 