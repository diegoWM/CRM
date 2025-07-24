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
DATASET_ID = 'crm_data'
ACCOUNTS_TABLE = 'accounts'

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
    'password': 'diego4456'
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
    
    # Add processing timestamp
    montreal_tz = pytz.timezone('America/Montreal')
    df['etl_timestamp'] = datetime.now(montreal_tz)
    
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
    client = bigquery.Client()
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ],
        # Define schema to ensure consistent data types
        schema=[
            bigquery.SchemaField("store_id", "INTEGER"),
            bigquery.SchemaField("store_name", "STRING"),
            bigquery.SchemaField("address", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("province", "STRING"),
            bigquery.SchemaField("postal_code", "STRING"),
            bigquery.SchemaField("latitude", "FLOAT"),
            bigquery.SchemaField("longitude", "FLOAT"),
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("phone", "STRING"),
            bigquery.SchemaField("accnt_class", "STRING"),
            bigquery.SchemaField("accnt_type", "STRING"),
            bigquery.SchemaField("chain_group", "STRING"),
            bigquery.SchemaField("client_since", "DATE"),
            bigquery.SchemaField("client_until", "DATE"),
            bigquery.SchemaField("NAT_KEY", "STRING"),
            bigquery.SchemaField("rep_name", "STRING"),
            bigquery.SchemaField("etl_timestamp", "TIMESTAMP")
        ]
    )
    
    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config
    )
    job.result()  # Wait for the job to complete
    
    logger.info(f"Data loaded to BigQuery: {table_id}")
    
    # Log row count for monitoring
    table = client.get_table(table_id)
    logger.info(f"Total rows in table after load: {table.num_rows}") 