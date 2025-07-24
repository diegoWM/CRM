@echo off
setlocal

REM Set variables
set PROJECT_ID=weedme-379116
set SERVICE_NAME=crm-accounts-pipeline
set REGION=northamerica-northeast1

REM Prepare build environment
echo Running preparation script...
call prepare_build.bat

REM Build the container using Cloud Build
echo Building container image using Cloud Build...
gcloud builds submit --tag gcr.io/%PROJECT_ID%/%SERVICE_NAME% .

REM Deploy to Cloud Run
echo Deploying to Cloud Run in %REGION%...
gcloud run deploy %SERVICE_NAME% ^
  --image gcr.io/%PROJECT_ID%/%SERVICE_NAME% ^
  --platform managed ^
  --region %REGION% ^
  --memory 512Mi ^
  --no-allow-unauthenticated ^
  --set-env-vars="PROJECT_ID=%PROJECT_ID%"

echo Deployment completed successfully!
pause 