import pandas as pd
import json
import os
from datetime import datetime
from fetch_accounts import fetch_account_data

def process_accounts_to_csv():
    """
    Process the account data into CSV format and perform basic analysis
    """
    # Fetch fresh data
    print("Fetching fresh data from API...")
    data = fetch_account_data()
    
    if not data:
        print("No data to process")
        return
    
    # Convert to DataFrame
    print("\nConverting data to DataFrame...")
    df = pd.DataFrame(data)
    
    # Save to CSV
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_path = os.path.join(os.path.dirname(__file__), 'processed_data', f'accounts_{timestamp}.csv')
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df.to_csv(csv_path, index=False)
    print(f"Data saved to CSV: {csv_path}")
    
    # Basic Analysis
    print("\n=== Basic Analysis ===")
    
    # 1. Store Classifications
    print("\nStore Classifications (accnt_class):")
    class_dist = df['accnt_class'].value_counts()
    print(class_dist)
    
    # 2. Chain Levels
    print("\nChain Levels:")
    chain_level_dist = df['chain_level'].value_counts()
    print(chain_level_dist)
    
    # 3. Store Types by Classification
    print("\nStore Types by Classification:")
    print(df.groupby('accnt_class')['accnt_type'].value_counts())
    
    # 4. Cities by Classification
    print("\nTop 5 Cities by Classification:")
    for class_val in df['accnt_class'].unique():
        if class_val:  # Skip empty classifications
            print(f"\nClass {class_val}:")
            print(df[df['accnt_class'] == class_val]['accnt_city'].value_counts().head(5))
    
    # 5. Store Status by Classification
    print("\nStore Status by Classification:")
    print(df.groupby('accnt_class')['store_status'].value_counts())
    
    return df

if __name__ == "__main__":
    print("Starting data processing and analysis...")
    df = process_accounts_to_csv() 