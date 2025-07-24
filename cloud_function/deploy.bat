@echo off
setlocal

REM Set variables
set PROJECT_ID=weedme-379116
set FUNCTION_NAME=crm-accounts-pipeline
set REGION=northamerica-northeast1
set RUNTIME=python39
set TRIGGER_TYPE=http
set MEMORY=512MB
set TIMEOUT=540s

REM Deploy the Cloud Function
echo Deploying Cloud Function %FUNCTION_NAME%...

gcloud functions deploy %FUNCTION_NAME% ^
  --gen2 ^
  --runtime=%RUNTIME% ^
  --region=%REGION% ^
  --source=. ^
  --entry-point=process_accounts_data_pipeline ^
  --trigger-http ^
  --memory=%MEMORY% ^
  --timeout=%TIMEOUT% ^
  --no-allow-unauthenticated ^
  --set-env-vars=PROJECT_ID=%PROJECT_ID%

echo Function deployment complete!

REM Set up Cloud Scheduler (optional - uncomment to use)
REM echo Setting up Cloud Scheduler for daily runs at 1 AM...
REM 
REM gcloud scheduler jobs create http crm-accounts-daily-job ^
REM   --schedule="0 1 * * *" ^
REM   --location=%REGION% ^
REM   --uri="https://%REGION%-weedme-379116.cloudfunctions.net/%FUNCTION_NAME%" ^
REM   --http-method=POST ^
REM   --attempt-deadline=10m ^
REM   --oidc-service-account-email=%PROJECT_ID%@appspot.gserviceaccount.com

echo Deployment completed successfully!
pause 