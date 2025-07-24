import requests
import pandas as pd
import json
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery
import os
import logging
from utils import (
    fetch_crm_data,
    process_accounts_data,
    validate_accounts_data,
    save_to_gcs,
    load_to_bigquery,
    ACCOUNTS_TABLE
)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('crm_data_pipeline')

# API Configuration
API_CONFIG = {
    'accounts': {
        'url': 'https://v1.ezfocus.ca/api',
        'key': '131b35eb-d1b8-4ac2-8fc5-cd08c9c3071a',
        'params': {
            'prov': 'ON',
            'seg': '',
            'tpcl': 'SUCC,LRS,MED',
            'agent': '1420'
        }
    },
    'inventory': {
        'url': 'https://v1.ezfocus.ca/api',
        'key': '79790282-19f7-4b4e-a452-a195bc94bd95',
        'params': {
            'prov': 'ON',
            'seg': '',
            'tpcl': 'SUCC,LRS,MED',
            'agent': '1420'
        }
    }
}

CREDENTIALS = {
    'username': 'dcardenas',
    'password': 'diego$456'
}

def fetch_data(api_type):
    """Fetch data from CRM API"""
    config = API_CONFIG[api_type]
    session = requests.Session()
    session.auth = (CREDENTIALS['username'], CREDENTIALS['password'])
    
    params = config['params'].copy()
    params['key'] = config['key']
    
    response = session.get(config['url'], params=params)
    response.raise_for_status()
    return response.json()

def process_to_bigquery(data, table_name, project_id, dataset_id):
    """Process and load data into BigQuery"""
    client = bigquery.Client(project=project_id)
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Add load metadata
    current_time = datetime.now()
    df['load_date'] = current_time.date()
    df['load_timestamp'] = current_time
    
    # Load to BigQuery
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
        ]
    )
    
    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    job.result()

def crm_to_bigquery(request):
    """Main Cloud Function entry point"""
    try:
        # Get configuration from environment variables
        bucket_name = os.environ.get('GCS_BUCKET')
        project_id = os.environ.get('GCP_PROJECT')
        dataset_id = os.environ.get('BQ_DATASET')
        
        if not all([bucket_name, project_id, dataset_id]):
            raise ValueError("Missing required environment variables")
        
        # Process accounts data
        print("Fetching accounts data...")
        accounts_data = fetch_data('accounts')
        save_to_gcs(accounts_data, 'accounts')
        process_to_bigquery(accounts_data, 'accounts', project_id, dataset_id)
        
        # Process inventory data
        print("Fetching inventory data...")
        inventory_data = fetch_data('inventory')
        save_to_gcs(inventory_data, 'inventory')
        process_to_bigquery(inventory_data, 'inventory', project_id, dataset_id)
        
        return "Successfully processed data", 200
        
    except Exception as e:
        print(f"Error processing data: {str(e)}")
        return str(e), 500

class DataQualityError(Exception):
    """Custom exception for data quality issues"""
    pass

def process_accounts_data_pipeline(request):
    """
    Cloud Function to process CRM accounts data
    Triggered by Cloud Scheduler or HTTP request
    """
    try:
        # Process accounts data
        logger.info("Starting accounts data processing pipeline")
        
        try:
            # Fetch data
            logger.info("Fetching accounts data from CRM API...")
            accounts_data = fetch_crm_data()
            logger.info(f"Successfully fetched {len(accounts_data)} records")
            
            # Save raw data to GCS
            try:
                logger.info("Saving raw data to Google Cloud Storage...")
                save_to_gcs(accounts_data, 'accounts')
                logger.info("Raw data successfully saved to GCS")
            except Exception as gcs_error:
                logger.error(f"Failed to save data to GCS: {str(gcs_error)}", exc_info=True)
                # Continue with pipeline even if GCS save fails
            
            # Process and validate data
            try:
                logger.info("Processing accounts data...")
                accounts_df = process_accounts_data(accounts_data)
                
                logger.info("Validating accounts data...")
                validate_accounts_data(accounts_df)
                logger.info("Data validation successful")
            except Exception as process_error:
                logger.error(f"Error processing or validating data: {str(process_error)}", exc_info=True)
                raise
            
            # Load data to BigQuery
            try:
                logger.info("Loading data to BigQuery...")
                # This will now raise an exception on failure instead of returning False
                load_to_bigquery(accounts_df, ACCOUNTS_TABLE)
                logger.info("Data successfully loaded to BigQuery")
            except Exception as bq_error:
                logger.error(f"BigQuery load failed: {str(bq_error)}", exc_info=True)
                # Re-raise to be caught by outer try/except
                raise
            
            logger.info("CRM accounts data processing completed successfully")
            return {"status": "success", "records_processed": len(accounts_data)}
            
        except requests.exceptions.RequestException as api_error:
            error_message = f"API connection error: {str(api_error)}"
            logger.error(error_message, exc_info=True)
            return {"status": "error", "message": error_message}, 500
            
    except Exception as e:
        error_message = f"Error processing data: {str(e)}"
        logger.error(error_message, exc_info=True)
        return {"status": "error", "message": error_message}, 500 