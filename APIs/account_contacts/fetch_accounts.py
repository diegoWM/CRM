import requests
import json
from datetime import datetime
import os

# API Configuration
API_URL = "https://v1.ezfocus.ca/api"
API_KEY = "131b35eb-d1b8-4ac2-8fc5-cd08c9c3071a"
CREDENTIALS = {
    'username': 'dcardenas',
    'password': 'diego$456'
}

def fetch_account_data(save_raw=True):
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
        print("Making API request...")
        response = session.get(API_URL, params=params)
        
        # Check if the request was successful
        if response.status_code == 200:
            print("Successfully received data from API")
            
            # Parse the JSON response
            data = response.json()
            
            if save_raw:
                # Save raw JSON data
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                raw_file_path = os.path.join(os.path.dirname(__file__), 'raw_data', f'account_data_{timestamp}.json')
                
                # Create raw_data directory if it doesn't exist
                os.makedirs(os.path.dirname(raw_file_path), exist_ok=True)
                
                with open(raw_file_path, 'w') as f:
                    json.dump(data, f, indent=4)
                print(f"Raw data saved to: {raw_file_path}")
            
            return data
            
        else:
            print(f"Error: API request failed with status code {response.status_code}")
            print(f"Response: {response.text}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Error making API request: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}")
        return None

if __name__ == "__main__":
    print("Starting data fetch process...")
    data = fetch_account_data()
    
    if data is not None:
        print("\nAPI Response Structure:")
        if isinstance(data, dict):
            print("Keys in response:", list(data.keys()))
        elif isinstance(data, list):
            print(f"Number of records: {len(data)}")
            if len(data) > 0:
                print("Sample record keys:", list(data[0].keys()))
        else:
            print(f"Unexpected data type: {type(data)}")
    else:
        print("Failed to fetch data from API") 