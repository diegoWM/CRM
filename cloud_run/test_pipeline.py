import json
import pandas as pd
import requests
from datetime import datetime
import pytz

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

def fetch_crm_data():
    """Fetch data from CRM API"""
    print("Fetching data from CRM API...")
    
    session = requests.Session()
    session.auth = (CREDENTIALS['username'], CREDENTIALS['password'])
    
    # Construct URL with parameters
    params = API_CONFIG['params'].copy()
    params['key'] = API_CONFIG['key']
    
    try:
        response = session.get(API_CONFIG['url'], params=params)
        response.raise_for_status()
        data = response.json()
        print(f"Successfully fetched {len(data)} records")
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {str(e)}")
        raise

def process_accounts_data(data):
    """Process accounts data into a pandas DataFrame"""
    print("Processing accounts data...")
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
    
    print(f"Processing complete. DataFrame has {len(df)} rows and {len(df.columns)} columns")
    return df

def validate_accounts_data(df):
    """Validate accounts data quality"""
    print("Validating accounts data...")
    
    # Check for required columns
    required_columns = [
        'store_id', 
        'store_name', 
        'city', 
        'province'
    ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"ERROR: Missing required columns: {missing_columns}")
        return False
    
    # Check for valid provinces
    valid_provinces = ['ON']  # Based on the API parameter
    invalid_provinces = df[~df['province'].isin(valid_provinces)]['province'].unique()
    if len(invalid_provinces) > 0:
        print(f"WARNING: Found invalid provinces: {invalid_provinces}")
    
    # Check for missing critical data
    missing_data = {
        'store_id': df['store_id'].isna().sum(),
        'store_name': df['store_name'].isna().sum()
    }
    for field, count in missing_data.items():
        if count > 0:
            print(f"WARNING: Found {count} records with missing {field}")
    
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
        print(f"WARNING: Found {len(invalid_coords)} records with invalid or missing coordinates")
    
    # Check for duplicate stores
    duplicates = df.groupby(['store_id']).size().reset_index(name='count')
    duplicates = duplicates[duplicates['count'] > 1]
    if len(duplicates) > 0:
        print(f"WARNING: Found {len(duplicates)} duplicate store IDs")
    
    print("Validation complete")
    return True

def save_to_local_file(df, filename="processed_accounts_data.csv"):
    """Save processed data to a local CSV file for verification"""
    print(f"Saving data to local file: {filename}...")
    df.to_csv(filename, index=False)
    print(f"Data saved successfully with {len(df)} rows")

def main():
    try:
        # Fetch data
        accounts_data = fetch_crm_data()
        
        # Save raw data to local JSON file for reference
        with open("raw_accounts_data.json", "w") as f:
            json.dump(accounts_data[:10], f, indent=2)  # Save first 10 records as sample
        
        # Process data
        accounts_df = process_accounts_data(accounts_data)
        
        # Validate data
        validation_result = validate_accounts_data(accounts_df)
        if validation_result:
            print("Validation passed with warnings (see above)")
        else:
            print("Validation failed (see errors above)")
            return
        
        # Save processed data locally for review
        save_to_local_file(accounts_df)
        
        # Print summary of data
        print("\nData Summary:")
        print(f"Total records: {len(accounts_df)}")
        print(f"Unique stores: {accounts_df['store_id'].nunique()}")
        print(f"Stores by province: {accounts_df['province'].value_counts().to_dict()}")
        print(f"Stores by type: {accounts_df['accnt_type'].value_counts().to_dict() if 'accnt_type' in accounts_df.columns else 'N/A'}")
        
        print("\nColumn names in final dataset:")
        print(", ".join(accounts_df.columns))
        
        print("\nSample data (first 5 rows):")
        print(accounts_df[['store_id', 'store_name', 'city', 'province']].head())
        
        print("\nProcessing completed successfully!")
        
    except Exception as e:
        print(f"Error in processing: {str(e)}")
        raise

if __name__ == "__main__":
    main() 