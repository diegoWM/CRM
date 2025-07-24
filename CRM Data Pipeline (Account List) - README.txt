CRM Data Pipeline - README
Project Overview
The CRM Data Pipeline extracts accounts data from the EZ Focus CRM system and loads it into Google BigQuery for reporting and analysis. The pipeline runs automatically on a daily schedule.

Components
Data Sources
EZ Focus CRM API: https://v1.ezfocus.ca/api
API Key: 131b35eb-d1b8-4ac2-8fc5-cd08c9c3071a
Parameters: Ontario stores (prov=ON), Store types: SUCC,LRS,MED

Cloud Infrastructure
Project ID: weedme-379116
Cloud Function: crm-accounts-pipeline
Region: northamerica-northeast1 (Montreal)
URL: https://crm-accounts-pipeline-32688366426.northamerica-northeast1.run.app
Storage Bucket: ez_focus_crm
BigQuery Dataset: WM_SalesTeam
BigQuery Table: Accounts_List
Cloud Scheduler Job: crm-accounts-daily (runs daily at 1:00 AM ET)

Technical Stack
Python Version: 3.11
Container Runtime: Docker
CI/CD: GitHub → Cloud Build → Artifact Registry → Cloud Run

Schema Details (BigQuery Table)
| Field Name | Type | Mode | Description |
|------------|------|------|-------------|
| accnt_class | STRING | NULLABLE | Store classification |
| accnt_name | STRING | NULLABLE | Store name |
| accnt_address | STRING | NULLABLE | Store address |
| accnt_city | STRING | NULLABLE | City |
| accnt_post_code | STRING | NULLABLE | Postal code |
| accnt_prov | STRING | NULLABLE | Province (Ontario) |
| latitude | FLOAT | NULLABLE | Geographical coordinate |
| longitude | FLOAT | NULLABLE | Geographical coordinate |
| accnt_phone | STRING | NULLABLE | Store phone number |
| accnt_email | STRING | NULLABLE | Store email |
| website | STRING | NULLABLE | Website URL |
| instagram | STRING | NULLABLE | Instagram handle |
| facebook | STRING | NULLABLE | Facebook page |
| pos_system | STRING | NULLABLE | Point of sale system |
| accnt_type | STRING | NULLABLE | Account type |
| store_status | STRING | NULLABLE | Store status |
| chain_group | STRING | NULLABLE | Chain affiliation |
| visit_days | STRING | NULLABLE | Days available for visits |
| client_since | DATE | NULLABLE | First client date |
| client_until | DATE | NULLABLE | End client date |
| load_timestamp | TIMESTAMP | REQUIRED | Data load timestamp |

Data Flow
Extraction: Cloud Function connects to EZ Focus CRM API to retrieve accounts data
Storage: Raw JSON data is stored in ez_focus_crm bucket with timestamp in filename
Transformation: Data is processed into a pandas DataFrame, with data type conversions and column mapping
Validation: Data is validated for required fields, valid provinces, coordinates, and duplicate detection
Loading: Processed data is loaded into BigQuery table WM_SalesTeam.Accounts_List

Code Structure
Main Components
main.py: Entry point for Cloud Function
utils.py: Supporting functions for API access, processing, validation, and storage

Key Functions
fetch_crm_data(): Retrieves data from CRM API
process_accounts_data(): Transforms JSON data to DataFrame with correct data types
validate_accounts_data(): Ensures data quality with various validation checks
save_to_gcs(): Archives raw data to Google Cloud Storage
load_to_bigquery(): Loads processed data to BigQuery

Automation and Triggers
Daily automated execution at 1:00 AM ET via Cloud Scheduler
Manually triggerable via Cloud Run URL with proper authentication
Eventarc trigger configured to run the function
CI/CD pipeline automatically deploys on GitHub push to main branch

Monitoring
Logs available in Cloud Run function logs
Successful execution returns JSON with status and record count
Error messages include detailed descriptions for troubleshooting
Raw data archived in GCS with timestamps for historical tracking

Common Issues and Troubleshooting
Schema Mismatches: Ensure schema definitions match exact field types in BigQuery
API Access Issues: Check credentials if data extraction fails
Data Validation Failures: Review logs for detailed validation errors

Maintenance Tasks
Periodically review logs to ensure successful execution
Monitor BigQuery for data quality and completeness
Check GCS storage for properly archived raw data
Update API credentials if they change

Migration Notes (January 2025)
- Upgraded from Python 3.9 to Python 3.11 for better performance
- Migrated from Sales_Team dataset (US region) to WM_SalesTeam dataset (Montreal region)
- Updated all dependencies to latest compatible versions
- Maintained backward compatibility with existing data structure

This CRM data pipeline provides automated, reliable data transfer from the EZ Focus CRM system to BigQuery for analytics and reporting purposes.