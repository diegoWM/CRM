@echo off
echo Preparing build environment...

REM Create keys directory if it doesn't exist
if not exist "keys" mkdir keys

REM Copy service account key to the build directory
echo Copying service account key...
copy "..\APIs\Key_GCP\weedme-379116-7bad7173874d.json" "keys\service-account-key.json"

echo Build environment prepared successfully. 