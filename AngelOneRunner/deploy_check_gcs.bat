@echo off
:: ============================================================================
:: deploy_check_gcs.bat
:: Builds + deploys the GCS upload-checker as a Cloud Run Job
:: and schedules it at 12:00 PM IST via Cloud Scheduler.
::
:: How alerting works (no Gmail / app-password needed):
::   Cloud Run Job exits 1 → job marked FAILED
::   → Cloud Monitoring Alerting Policy fires
::   → sends email via the Notification Channel you configure once in GCP console
::
:: Prerequisites
:: ─────────────
::   1. gcloud CLI installed:  https://cloud.google.com/sdk/docs/install
::   2. Authenticated:         gcloud auth login
::   3. Project set:           gcloud config set project YOUR_PROJECT_ID
::   4. APIs enabled (run once):
::        gcloud services enable run.googleapis.com ^
::                               cloudscheduler.googleapis.com ^
::                               cloudbuild.googleapis.com ^
::                               monitoring.googleapis.com
::
:: After deploy — wire the alert (one-time in GCP Console):
::   Cloud Monitoring → Alerting → Create Policy
::     Condition:  Cloud Run Job / Completed task count  (failed_task_count > 0)
::     Resource:   job_name = check-gcs-uploads
::     Notification: add your email as a Notification Channel
:: ============================================================================

:: ── EDIT THESE ──────────────────────────────────────────────────────────────
set PROJECT_ID=elegant-tendril-399501
set REGION=asia-south1
set IMAGE=gcr.io/%PROJECT_ID%/check-gcs-uploads
set JOB_NAME=check-gcs-uploads
set SCHEDULER_NAME=check-gcs-uploads-daily
set SCHEDULE=0 12 * * *
set TIMEZONE=Asia/Kolkata
set SERVICE_ACCOUNT=screener-agent-sa@elegant-tendril-399501.iam.gserviceaccount.com
:: ────────────────────────────────────────────────────────────────────────────

echo.
echo [1/4] Building and pushing Docker image...
gcloud builds submit --config cloudbuild_check_gcs.yaml --substitutions=_IMAGE=%IMAGE% .
if %errorlevel% neq 0 ( echo ERROR: Build failed. && pause && exit /b 1 )

echo.
echo [2/4] Deploying Cloud Run Job...
gcloud run jobs deploy %JOB_NAME% ^
  --image %IMAGE% ^
  --region %REGION% ^
  --task-timeout=120 ^
  --max-retries=0 ^
  --service-account %SERVICE_ACCOUNT%
if %errorlevel% neq 0 ( echo ERROR: Job deploy failed. && pause && exit /b 1 )

echo.
echo [3/4] Scheduling at %SCHEDULE% (%TIMEZONE%)...
gcloud scheduler jobs describe %SCHEDULER_NAME% --location %REGION% >nul 2>&1
if %errorlevel% == 0 (
  echo   Scheduler job already exists — updating schedule only...
  gcloud scheduler jobs update http %SCHEDULER_NAME% ^
    --schedule "%SCHEDULE%" ^
    --location %REGION%
) else (
  gcloud scheduler jobs create http %SCHEDULER_NAME% ^
    --schedule "%SCHEDULE%" ^
    --time-zone "%TIMEZONE%" ^
    --uri "https://%REGION%-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/%PROJECT_ID%/jobs/%JOB_NAME%:run" ^
    --message-body "{}" ^
    --oauth-service-account-email %SERVICE_ACCOUNT% ^
    --location %REGION%
)
if %errorlevel% neq 0 ( echo ERROR: Scheduler setup failed. && pause && exit /b 1 )

echo.
echo [4/4] Running job once now to verify...
gcloud run jobs execute %JOB_NAME% --region %REGION% --wait

echo.
echo ============================================================
echo  Done! Job scheduled: %SCHEDULE% IST (%TIMEZONE%)
echo.
echo  Next step — wire the alert (one-time in GCP Console):
echo    1. Open: https://console.cloud.google.com/monitoring/alerting
echo    2. Create Policy:
echo         Metric : run.googleapis.com/job/failed_task_count
echo         Filter : job_name = "%JOB_NAME%"
echo         Trigger: threshold > 0
echo    3. Add your email as a Notification Channel
echo    4. Save. Done — no Gmail app password needed.
echo ============================================================
pause
