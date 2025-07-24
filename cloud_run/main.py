# CRM Data Pipeline - Cloud Run Service
# Last updated: CI/CD Pipeline Test

import os
import logging
from datetime import datetime
import google.cloud.logging
from utils import (
    fetch_crm_data,
    process_accounts_data,
    validate_accounts_data,
    save_to_gcs,
    load_to_bigquery,
    ACCOUNTS_TABLE
)

# Setup logging
logging_client = google.cloud.logging.Client()
logger = logging_client.logger('crm_data_pipeline')

class DataQualityError(Exception):
    """Custom exception for data quality issues"""
    pass

def main(request=None):
    """Main function to process CRM accounts data"""
    try:
        # Process accounts data
        logger.info("Starting accounts data processing pipeline")
        
        # Fetch data
        logger.info("Fetching accounts data from CRM API...")
        accounts_data = fetch_crm_data()
        logger.info(f"Successfully fetched {len(accounts_data)} records")
        
        # Save raw data to GCS
        logger.info("Saving raw data to Google Cloud Storage...")
        save_to_gcs(accounts_data, 'accounts')
        
        # Process and validate data
        logger.info("Processing accounts data...")
        accounts_df = process_accounts_data(accounts_data)
        
        logger.info("Validating accounts data...")
        validate_accounts_data(accounts_df)
        
        # Load data to BigQuery
        logger.info("Loading data to BigQuery...")
        load_to_bigquery(accounts_df, ACCOUNTS_TABLE)
        
        logger.info("CRM accounts data processing completed successfully")
        return "Success", 200
        
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main() 