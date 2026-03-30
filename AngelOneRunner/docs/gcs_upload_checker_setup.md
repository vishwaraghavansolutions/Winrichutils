# GCS Upload Checker — Setup Guide

Runs daily at **12:00 PM IST** as a Cloud Run Job.
Checks that all expected files were uploaded to GCS by `ao_daily_runner.py`.
Fires a Cloud Monitoring alert (with missing file details) if any are absent.

---

## Prerequisites

| Tool | Check / Install |
|------|----------------|
| gcloud CLI | `gcloud --version` |
| Docker Desktop | Must be running during build |
| gcloud authenticated | `gcloud auth login` |
| Project set | `gcloud config set project elegant-tendril-399501` |

Enable required APIs (run once):
```
gcloud services enable run.googleapis.com cloudscheduler.googleapis.com cloudbuild.googleapis.com monitoring.googleapis.com
```

---

## Part 1 — Build & Deploy

### 1.1 Build the Docker image

From the `AngelOneRunner` root folder:

```
gcloud builds submit --config cloudbuild_check_gcs.yaml --substitutions=_IMAGE=gcr.io/elegant-tendril-399501/check-gcs-uploads .
```

### 1.2 Deploy the Cloud Run Job

```
gcloud run jobs deploy check-gcs-uploads ^
  --image gcr.io/elegant-tendril-399501/check-gcs-uploads ^
  --region asia-south1 ^
  --task-timeout=120 ^
  --max-retries=0 ^
  --service-account screener-agent-sa@elegant-tendril-399501.iam.gserviceaccount.com
```

### 1.3 Schedule at 12:00 PM IST

```
gcloud scheduler jobs create http check-gcs-uploads-daily ^
  --schedule "0 12 * * *" ^
  --time-zone "Asia/Kolkata" ^
  --uri "https://asia-south1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/elegant-tendril-399501/jobs/check-gcs-uploads:run" ^
  --message-body "{}" ^
  --oauth-service-account-email screener-agent-sa@elegant-tendril-399501.iam.gserviceaccount.com ^
  --location asia-south1
```

### 1.4 Run once to verify

```
gcloud run jobs execute check-gcs-uploads --region asia-south1 --wait
```

Check the output in Cloud Logging:
```
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=check-gcs-uploads" --limit=50 --format="value(textPayload)"
```

---

## Part 2 — Set Up Alert

### 2.1 Create a Notification Channel (your email)

1. Open **Cloud Monitoring → Alerting → Notification Channels**
   `https://console.cloud.google.com/monitoring/alerting/notifications`
2. Click **Add New → Email**
3. Enter the alert recipient email and save
4. Note the **Channel Name** — you'll select it in step 2.2

### 2.2 Create a Log-based Alert

1. Open **Cloud Logging → Log-based Alerts**
   `https://console.cloud.google.com/logs/alerts`
2. Click **Create alert**
3. Fill in:

   | Field | Value |
   |-------|-------|
   | Alert name | `GCS Upload Missing Files` |
   | Description | `One or more daily GCS uploads are missing` |

4. **Define the log entries to alert on** — paste this filter:
   ```
   resource.type="cloud_run_job"
   resource.labels.job_name="check-gcs-uploads"
   severity=ERROR
   ```

5. **Set notification frequency:** `5 minutes`

6. **Who should be notified:** select the email channel created in step 2.1

7. Click **Save**

### What the alert email will contain

When files are missing the alert notification includes the actual ERROR log lines, for example:

```
ERROR  MISSING  Unify Daily AUM       gs://winrich/Datawarehouse/Unify/2026/03/30/WAWYA_Daily_AUM_30-03-2026.csv
ERROR  MISSING  ASK PMS               gs://winrich/Datawarehouse/ASK/2026/03/30/ask_pms.csv
ERROR  UPLOAD INCOMPLETE — 2 file(s) missing: Unify Daily AUM, ASK PMS
```

---

## Part 3 — Redeployment (after script changes)

Whenever `check_gcs_uploads.py` is updated, rebuild and redeploy:

```
gcloud builds submit --config cloudbuild_check_gcs.yaml --substitutions=_IMAGE=gcr.io/elegant-tendril-399501/check-gcs-uploads .

gcloud run jobs deploy check-gcs-uploads ^
  --image gcr.io/elegant-tendril-399501/check-gcs-uploads ^
  --region asia-south1 ^
  --task-timeout=120 ^
  --max-retries=0 ^
  --service-account screener-agent-sa@elegant-tendril-399501.iam.gserviceaccount.com
```

The Cloud Scheduler trigger does **not** need to be recreated.

---

## Reference

| Resource | Details |
|----------|---------|
| GCP Project | `elegant-tendril-399501` |
| Region | `asia-south1` |
| Cloud Run Job | `check-gcs-uploads` |
| Scheduler Job | `check-gcs-uploads-daily` |
| Schedule | `0 12 * * *` (12:00 PM IST) |
| Service Account | `screener-agent-sa@elegant-tendril-399501.iam.gserviceaccount.com` |
| Docker Image | `gcr.io/elegant-tendril-399501/check-gcs-uploads` |
| Buckets checked | `winrich`, `winrich_shared` |

## Files

| File | Purpose |
|------|---------|
| `scripts/check_gcs_uploads.py` | The checker script |
| `Dockerfile.check_gcs` | Docker image definition |
| `cloudbuild_check_gcs.yaml` | Cloud Build config for custom Dockerfile name |
| `requirements_check.txt` | Python deps for the image |
| `deploy_check_gcs.bat` | One-click deploy script (Windows) |
