import pandas as pd
import json
import os
from datetime import datetime
from fetch_inventory import fetch_inventory_data

def process_inventory_to_csv():
    """
    Process the inventory data into CSV format and perform basic analysis
    """
    # Fetch fresh data
    print("Fetching fresh inventory data from API...")
    data = fetch_inventory_data()
    
    if not data:
        print("No data to process")
        return
    
    # Convert to DataFrame
    print("\nConverting data to DataFrame...")
    df = pd.DataFrame(data)
    
    # Save to CSV
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_path = os.path.join(os.path.dirname(__file__), 'processed_data', f'inventory_{timestamp}.csv')
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df.to_csv(csv_path, index=False)
    print(f"Data saved to CSV: {csv_path}")
    
    # Basic Analysis
    print("\n=== Basic Analysis ===")
    
    # 1. Product Segments Distribution
    print("\nProduct Segments Distribution:")
    print(df['SEGMENT'].value_counts())
    
    # 2. Brands Distribution (top 10)
    print("\nTop 10 Brands by Number of Products:")
    print(df['BRAND'].value_counts().head(10))
    
    # 3. Categories Distribution
    print("\nNational Categories Distribution:")
    print(df['NAT_CATEG'].value_counts())
    
    # 4. Listing Types Distribution
    print("\nListing Types Distribution:")
    print(df['LISTING_TYPE'].value_counts())
    
    # 5. Current Inventory Analysis
    print("\nCurrent Inventory Summary:")
    print(df['CURR_INV'].describe())
    
    # 6. Cities Distribution (top 10)
    print("\nTop 10 Cities by Number of Store-Product Combinations:")
    print(df['CITY'].value_counts().head(10))
    
    # 7. Chain Analysis
    print("\nChain Distribution:")
    print(df['CHAIN'].value_counts().head(10))
    
    return df

if __name__ == "__main__":
    print("Starting inventory data processing and analysis...")
    df = process_inventory_to_csv() 