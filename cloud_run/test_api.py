import requests
import json

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

def test_api_connection():
    """Test API connection and print sample data"""
    print("Testing API connection...")
    
    session = requests.Session()
    session.auth = (CREDENTIALS['username'], CREDENTIALS['password'])
    
    # Construct URL with parameters
    params = API_CONFIG['params'].copy()
    params['key'] = API_CONFIG['key']
    
    try:
        print(f"Connecting to: {API_CONFIG['url']}")
        print(f"With params: {params}")
        response = session.get(API_CONFIG['url'], params=params)
        response.raise_for_status()
        
        data = response.json()
        print(f"Connection successful!")
        print(f"Received {len(data)} records")
        
        # Print sample data (first 2 records)
        if data:
            print("\nSample data (first 2 records):")
            print(json.dumps(data[:2], indent=2))
            
            # Print available columns
            print("\nAvailable columns:")
            columns = list(data[0].keys())
            print(", ".join(columns))
        
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to API: {str(e)}")
        return False

if __name__ == "__main__":
    test_api_connection() 