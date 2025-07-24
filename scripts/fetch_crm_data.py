import requests
import pandas as pd
from datetime import datetime
import os
import json

# API Configuration
###  BASE_URL Original prototype = "https://v1.ezfocus.ca/api"
BASE_URL = "https://v1.ezfocus.ca/api?key=131b35eb-d1b8-4ac2-8fc5-cd08c9c3071a&prov=ON&seg=&tpcl=SUCC,LRS,MED&agent=1420"
API_KEY = "131b35eb-d1b8-4ac2-8fc5-cd08c9c3071a"
CREDENTIALS = {
    'username': 'dcardenas',
    'password': 'diego$456'
}

def fetch_account_data():
    """
    Fetch account list and contacts data from the API
    """
    params = {
        'key': API_KEY,
        'prov': 'ON',
        'seg': '',  # All segments
        'tpcl': 'SUCC,LRS,MED',
        'agent': '1420'
    }
    
    try:
        # Create a session with authentication
        session = requests.Session()
        session.auth = (CREDENTIALS['username'], CREDENTIALS['password'])
        
        # Make the API request
        response = session.get(BASE_URL, params=params)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        # Parse the JSON response
        data = response.json()
        
        # Save raw JSON data
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        raw_file_path = f'CRM_Data/raw_data/account_data_{timestamp}.json'
        with open(raw_file_path, 'w') as f:
            json.dump(data, f, indent=4)
        
        print(f"Raw data saved to {raw_file_path}")
        
        # Convert to DataFrame and save as CSV
        # Note: The exact structure will depend on the API response
        # We'll need to adjust this after seeing the actual data structure
        df = pd.json_normalize(data)
        csv_file_path = f'CRM_Data/processed_data/account_data_{timestamp}.csv'
        df.to_csv(csv_file_path, index=False)
        
        print(f"Processed data saved to {csv_file_path}")
        
        # Display basic information about the data
        print("\nData Overview:")
        print(f"Number of records: {len(df)}")
        print("\nColumns in the dataset:")
        print(df.columns.tolist())
        
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

if __name__ == "__main__":
    print("Fetching CRM account data...")
    df = fetch_account_data()
    
    if df is not None:
        print("\nFirst few records:")
        print(df.head())
        
        print("\nBasic statistics:")
        print(df.describe()) 