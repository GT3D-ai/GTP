#!/usr/bin/env bash
# IAM setup for gcs-uploader
# Usage: ./setup-iam.sh <PROJECT_ID> <DEPLOYER_EMAIL>
# Example: ./setup-iam.sh my-gcp-project me@example.com

set -euo pipefail

PROJECT_ID="${1:-}"
DEPLOYER="${2:-}"

if [[ -z "$PROJECT_ID" || -z "$DEPLOYER" ]]; then
  echo "Usage: $0 <PROJECT_ID> <DEPLOYER_EMAIL>"
  exit 1
fi

SA_NAME="gcs-uploader-sa"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
BUCKETS=("gt-platform-360-photos-bucket" "gt_platform_image_storage")

echo ">> Setting active project to ${PROJECT_ID}"
gcloud config set project "${PROJECT_ID}"

echo ">> Enabling required APIs"
gcloud services enable \
  run.googleapis.com \
  storage.googleapis.com \
  cloudbuild.googleapis.com \
  iam.googleapis.com \
  iamcredentials.googleapis.com

echo ">> Creating service account ${SA_EMAIL} (if missing)"
if ! gcloud iam service-accounts describe "${SA_EMAIL}" >/dev/null 2>&1; then
  gcloud iam service-accounts create "${SA_NAME}" \
    --display-name="GCS Uploader Runtime"
else
  echo "   already exists — skipping"
fi

echo ">> Granting bucket-scoped roles/storage.objectUser"
for BUCKET in "${BUCKETS[@]}"; do
  echo "   - gs://${BUCKET}"
  gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/storage.objectUser" \
    --condition=None >/dev/null
done

echo ">> Granting ${DEPLOYER} the serviceAccountUser role on ${SA_EMAIL}"
gcloud iam service-accounts add-iam-policy-binding "${SA_EMAIL}" \
  --member="user:${DEPLOYER}" \
  --role="roles/iam.serviceAccountUser" >/dev/null

echo ""
echo "Done."
echo "Runtime service account: ${SA_EMAIL}"
echo ""
echo "Deploy to Cloud Run with:"
echo "  gcloud run deploy gcs-uploader \\"
echo "    --source . \\"
echo "    --region us-central1 \\"
echo "    --service-account=${SA_EMAIL} \\"
echo "    --allow-unauthenticated"
