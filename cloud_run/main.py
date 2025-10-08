# CRM Data Pipeline - Cloud Run Service
# Last updated: CI/CD Pipeline Test

import os
import logging
from datetime import datetime
import google.cloud.logging
from flask import Flask, request, jsonify
from utils import (
    fetch_crm_data,
    process_accounts_data,
    validate_accounts_data,
    save_to_gcs,
    load_to_bigquery,
    ACCOUNTS_TABLE,
    # NEW: Call Reports functions
    fetch_call_reports_data,
    process_call_reports_data,
    validate_call_reports_data,
    CALL_REPORTS_TABLE
)

# Setup Google Cloud logging
logging_client = google.cloud.logging.Client()
logging_client.setup_logging()

# Setup standard logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)

class DataQualityError(Exception):
    """Custom exception for data quality issues"""
    pass

def process_crm_data():
    """Main function to process ALL CRM data (accounts + call reports)"""
    results = {}
    overall_status = "success"
    total_records = 0
    
    try:
        logger.info("🚀 Starting COMPLETE CRM data processing pipeline")
        start_time = datetime.now()
        
        # ===== PROCESS ACCOUNTS DATA (EXISTING - UNCHANGED) =====
        try:
            logger.info("=== Processing ACCOUNTS data ===")
            
            # Fetch accounts data
            logger.info("Fetching accounts data from CRM API...")
            accounts_data = fetch_crm_data()
            logger.info(f"Successfully fetched {len(accounts_data)} account records")
            
            # Save raw accounts data to GCS
            logger.info("Saving raw accounts data to Google Cloud Storage...")
            save_to_gcs(accounts_data, 'accounts')
            
            # Process and validate accounts data
            logger.info("Processing accounts data...")
            accounts_df = process_accounts_data(accounts_data)
            
            logger.info("Validating accounts data...")
            validate_accounts_data(accounts_df)
            
            # Load accounts data to BigQuery
            logger.info("Loading accounts data to BigQuery...")
            load_to_bigquery(accounts_df, ACCOUNTS_TABLE)
            
            results["accounts"] = {"status": "success", "records_processed": len(accounts_data)}
            total_records += len(accounts_data)
            logger.info("✅ ACCOUNTS data processing completed successfully")
            
        except Exception as e:
            logger.error(f"❌ ACCOUNTS pipeline failed: {str(e)}")
            results["accounts"] = {"status": "error", "message": str(e)}
            overall_status = "partial_failure"
        
        # ===== PROCESS CALL REPORTS DATA (NEW) =====
        try:
            logger.info("=== Processing CALL REPORTS data ===")
            
            # Fetch call reports data
            logger.info("Fetching call reports data from CRM API...")
            call_reports_data = fetch_call_reports_data()
            logger.info(f"Successfully fetched {len(call_reports_data)} call report records")
            
            # Save raw call reports data to GCS
            logger.info("Saving raw call reports data to Google Cloud Storage...")
            save_to_gcs(call_reports_data, 'call_reports')
            
            # Process and validate call reports data
            logger.info("Processing call reports data...")
            call_reports_df = process_call_reports_data(call_reports_data)
            
            logger.info("Validating call reports data...")
            validate_call_reports_data(call_reports_df)
            
            # Load call reports data to BigQuery
            logger.info("Loading call reports data to BigQuery...")
            load_to_bigquery(call_reports_df, CALL_REPORTS_TABLE)
            
            results["call_reports"] = {"status": "success", "records_processed": len(call_reports_data)}
            total_records += len(call_reports_data)
            logger.info("✅ CALL REPORTS data processing completed successfully")
            
        except Exception as e:
            logger.error(f"❌ CALL REPORTS pipeline failed: {str(e)}")
            results["call_reports"] = {"status": "error", "message": str(e)}
            overall_status = "partial_failure" if overall_status == "success" else "failure"
        
        # ===== SUMMARY =====
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        
        logger.info(f"🎉 COMPLETE CRM pipeline finished - Status: {overall_status}")
        logger.info(f"📊 Total records processed: {total_records}")
        logger.info(f"⏱️ Processing time: {processing_time:.2f} seconds")
        
        return {
            "overall_status": overall_status,
            "processing_time_seconds": processing_time,
            "total_records_processed": total_records,
            "details": results
        }, 200 if overall_status == "success" else 207  # 207 = Multi-Status
        
    except Exception as e:
        logger.error(f"💥 Critical error in CRM pipeline: {str(e)}", exc_info=True)
        return {
            "overall_status": "failure",
            "message": str(e),
            "details": results
        }, 500

@app.route('/', methods=['GET', 'POST'])
def main():
    """HTTP endpoint for Cloud Run"""
    result, status_code = process_crm_data()
    return jsonify(result), status_code

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy"}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port) 