import os
import json
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
import google.cloud.logging
import requests
from retry import retry
from datetime import datetime
import pytz

# Initialize logging
logging_client = google.cloud.logging.Client()
logger = logging_client.logger('crm_data_pipeline')

# Constants
PROJECT_ID = 'weedme-379116'
BUCKET_NAME = 'ez_focus_crm'
DATASET_ID = 'WM_SalesTeam'
ACCOUNTS_TABLE = 'Accounts_List'

# API Configuration
API_CONFIG = {
    'url': 'https://v1.ezfocus.ca/api',
    'key': '131b35eb-d1b8-4ac2-8fc5-cd08c9c3071a',
    'params': {
        'prov': 'ON',
        'seg': '',
        'tpcl': 'SUCC,LRS,MED',
        'agent': '1420'
    }
}
CREDENTIALS = {
    'username': 'dcardenas',
    'password': 'diego$456'
}

@retry(tries=3, delay=5, backoff=2)
def fetch_crm_data():
    """Fetch data from CRM API with retry logic"""
    session = requests.Session()
    session.auth = (CREDENTIALS['username'], CREDENTIALS['password'])
    
    # Construct URL with parameters
    params = API_CONFIG['params'].copy()
    params['key'] = API_CONFIG['key']
    
    try:
        response = session.get(API_CONFIG['url'], params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data: {str(e)}")
        raise

def process_accounts_data(data):
    """Process accounts data into a pandas DataFrame"""
    df = pd.DataFrame(data)
    
    # Convert date fields
    date_columns = ['client_since', 'client_until']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Convert numeric fields
    numeric_columns = {
        'latitude': float,
        'longitude': float,
        'accnt_no': int  # Store ID
    }
    for col, dtype in numeric_columns.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(dtype)
    
    # Add processing timestamp - formatted for BigQuery compatibility
    montreal_tz = pytz.timezone('America/Montreal')
    # Using ISO format with timezone info for BigQuery compatibility
    df['load_timestamp'] = datetime.now(montreal_tz)
    
    # Ensure consistent column names - map to standardized names
    column_mapping = {
        'accnt_no': 'store_id',
        'accnt_name': 'store_name',
        'accnt_address': 'address',
        'accnt_city': 'city',
        'accnt_prov': 'province',
        'accnt_post_code': 'postal_code',
        'accnt_email': 'email',
        'accnt_phone': 'phone'
    }
    df = df.rename(columns=column_mapping)
    
    return df

def validate_accounts_data(df):
    """Validate accounts data quality"""
    # Check for required columns based on actual schema
    required_columns = [
        'store_id', 
        'store_name', 
        'city', 
        'province'
    ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Check for valid provinces
    valid_provinces = ['ON']  # Based on the API parameter
    invalid_provinces = df[~df['province'].isin(valid_provinces)]['province'].unique()
    if len(invalid_provinces) > 0:
        logger.warning(f"Found invalid provinces: {invalid_provinces}")
    
    # Check for missing critical data
    missing_data = {
        'store_id': df['store_id'].isna().sum(),
        'store_name': df['store_name'].isna().sum()
    }
    for field, count in missing_data.items():
        if count > 0:
            logger.warning(f"Found {count} records with missing {field}")
    
    # Validate coordinates
    invalid_coords = df[
        ((df['latitude'] == 0) & (df['longitude'] == 0)) |  # Zero coordinates
        (df['latitude'].isna()) | 
        (df['longitude'].isna()) |
        (df['latitude'] < 41.6) | 
        (df['latitude'] > 57.0) |  # Ontario latitude range
        (df['longitude'] < -95.2) | 
        (df['longitude'] > -74.3)  # Ontario longitude range
    ]
    if len(invalid_coords) > 0:
        logger.warning(f"Found {len(invalid_coords)} records with invalid or missing coordinates")
    
    # Check for duplicate stores
    duplicates = df.groupby(['store_id']).size().reset_index(name='count')
    duplicates = duplicates[duplicates['count'] > 1]
    if len(duplicates) > 0:
        logger.warning(f"Found {len(duplicates)} duplicate store IDs")

def save_to_gcs(data, prefix):
    """Save raw JSON data to Google Cloud Storage"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    
    montreal_tz = pytz.timezone('America/Montreal')
    timestamp = datetime.now(montreal_tz).strftime('%Y%m%d_%H%M%S')
    blob_name = f"{prefix}/raw/accounts_{timestamp}.json"  # Updated naming convention
    
    blob = bucket.blob(blob_name)
    blob.upload_from_string(
        data=json.dumps(data),
        content_type='application/json'
    )
    
    logger.info(f"Data saved to GCS: gs://{BUCKET_NAME}/{blob_name}")

def load_to_bigquery(df, table_name):
    """Load processed data to BigQuery"""
    try:
        client = bigquery.Client()
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        
        # Make a copy of the dataframe to avoid modifying the original
        upload_df = df.copy()
        
        # Print sample data for debugging
        logger.info(f"DataFrame sample before processing:\n{upload_df.head(2)}")
        logger.info(f"Original data types: {upload_df.dtypes}")
        
        # CRITICAL FIX: Convert problematic columns to strings to avoid PyArrow conversion errors
        for col in ['store_id', 'accnt_no']:
            if col in upload_df.columns:
                upload_df[col] = upload_df[col].astype(str)
                logger.info(f"Explicitly converted {col} to string type")
        
        # Let BigQuery auto-detect schema, which is safer than specifying types
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True  # Let BigQuery infer schema
        )
        
        logger.info(f"Loading {len(upload_df)} rows to BigQuery table {table_id}")
        
        try:
            job = client.load_table_from_dataframe(
                upload_df,
                table_id,
                job_config=job_config
            )
            
            # Wait for the job to complete and check for errors
            result = job.result()  
            
            if job.error_result:
                error_msg = f"BigQuery job had errors: {job.error_result}"
                logger.error(error_msg)
                raise RuntimeError(error_msg)
                
            logger.info(f"BigQuery job {job.job_id} completed successfully with {job.output_rows} rows")
            
            # Verify data was loaded
            table = client.get_table(table_id)
            logger.info(f"VERIFICATION: Table now has {table.num_rows} total rows")
            
            return True
            
        except Exception as load_error:
            error_msg = f"Failed to load data to BigQuery: {str(load_error)}"
            logger.error(error_msg)
            logger.error(f"Error type: {type(load_error)}")
            raise RuntimeError(error_msg)
            
    except Exception as e:
        error_msg = f"BigQuery operation failed: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Error type: {type(e)}")
        raise RuntimeError(error_msg) 