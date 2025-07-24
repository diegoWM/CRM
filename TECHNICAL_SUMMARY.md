# CRM Data Pipeline - Technical Summary & Reference Guide

## 📋 Quick Index

- [System Overview](#system-overview)
- [Current Configuration](#current-configuration)
- [CI/CD Pipeline](#cicd-pipeline)
- [Data Flow](#data-flow)
- [Key Files & Locations](#key-files--locations)
- [Commands Reference](#commands-reference)
- [Troubleshooting Guide](#troubleshooting-guide)
- [Migration History](#migration-history)
- [Glossary](#glossary)

---

## 🎯 System Overview

**Purpose**: Automated daily extraction of CRM data from EZ Focus API → BigQuery for analytics  
**Current Status**: ✅ Production-ready with enterprise CI/CD pipeline  
**Last Updated**: January 2025 (Python 3.11 + BigQuery migration)

### Architecture Flow
```
EZ Focus CRM API → Cloud Run (Python 3.11) → Cloud Storage (backup) → BigQuery (WM_SalesTeam)
                           ↑
                    GitHub → Cloud Build → Artifact Registry
```

---

## ⚙️ Current Configuration

### **Infrastructure**
| Component | Value | Location |
|-----------|-------|----------|
| **GCP Project** | `weedme-379116` | Global |
| **Region** | `northamerica-northeast1` (Montreal) | All services |
| **Python Version** | `3.11-slim` | Container |
| **Runtime** | Docker container | Cloud Run |

### **Data Storage**
| Type | Location | Purpose |
|------|----------|---------|
| **Raw Data Backup** | `gs://ez_focus_crm/accounts/raw/` | Daily JSON snapshots |
| **Migration Backup** | `gs://ez_focus_crm/migration/` | One-time migration files |
| **Production Data** | `weedme-379116.WM_SalesTeam.Accounts_List` | Live analytics data |
| **Legacy Data** | `weedme-379116.Sales_Team.Accounts_List` | Old US region (kept temporarily) |

### **API Configuration**
| Parameter | Value |
|-----------|-------|
| **Base URL** | `https://v1.ezfocus.ca/api` |
| **API Key** | `131b35eb-d1b8-4ac2-8fc5-cd08c9c3071a` |
| **Username** | `dcardenas` |
| **Password** | `diego$456` |
| **Province Filter** | `ON` (Ontario only) |
| **Store Types** | `SUCC,LRS,MED` |
| **Agent ID** | `1420` |

### **Scheduling**
- **Frequency**: Daily at 1:00 AM ET
- **Trigger**: Cloud Scheduler → Cloud Run
- **Timeout**: 540 seconds (9 minutes)
- **Memory**: 512MB

---

## 🚀 CI/CD Pipeline

### **GitHub Repository**
- **URL**: `https://github.com/diegoWM/CRM`
- **Main Branch**: `main`
- **Trigger**: Any push to `main` branch

### **Build Process**
1. **Source**: GitHub detects push
2. **Build**: Cloud Build creates Docker image
3. **Store**: Artifact Registry stores container
4. **Deploy**: Cloud Run automatically updates service

### **Container Registry**
- **Location**: `northamerica-northeast1-docker.pkg.dev/weedme-379116/crm-pipeline/crm`
- **Format**: `crm:$COMMIT_SHA`

---

## 📊 Data Flow

### **Daily Pipeline Steps**
1. **Trigger** (1:00 AM ET): Cloud Scheduler → Cloud Run
2. **Fetch**: API call to EZ Focus CRM
3. **Backup**: Save raw JSON to Cloud Storage
4. **Process**: Transform data (pandas DataFrame)
5. **Validate**: Data quality checks
6. **Load**: Insert into BigQuery (WRITE_TRUNCATE)
7. **Log**: Success/failure status

### **Data Volume**
- **Current Records**: ~2,684 accounts
- **Daily Updates**: Full refresh (not incremental)
- **File Size**: ~800KB CSV, ~2MB JSON

---

## 📁 Key Files & Locations

### **Production Files (cloud_run/)**
```
cloud_run/
├── Dockerfile          # Python 3.11 container definition
├── main.py             # Entry point for Cloud Run service
├── utils.py            # Core pipeline functions
└── requirements.txt    # Python dependencies
```

### **Configuration Constants**
**File**: `cloud_run/utils.py`
```python
PROJECT_ID = 'weedme-379116'
BUCKET_NAME = 'ez_focus_crm'
DATASET_ID = 'WM_SalesTeam'
ACCOUNTS_TABLE = 'Accounts_List'
```

### **Key Functions**
| Function | Purpose | File |
|----------|---------|------|
| `fetch_crm_data()` | API data retrieval | `utils.py` |
| `process_accounts_data()` | Data transformation | `utils.py` |
| `validate_accounts_data()` | Quality checks | `utils.py` |
| `save_to_gcs()` | Backup to storage | `utils.py` |
| `load_to_bigquery()` | Database insertion | `utils.py` |
| `main()` | Pipeline orchestration | `main.py` |

---

## 💻 Commands Reference

### **Development Workflow**
```bash
# Make changes
vim cloud_run/main.py

# Deploy via CI/CD
git add .
git commit -m "Description of changes"
git push origin main
# Wait 2-3 minutes for automatic deployment
```

### **Manual Testing**
```bash
# Test locally (if needed)
cd cloud_run
python main.py

# Check BigQuery data
bq query --location=northamerica-northeast1 --use_legacy_sql=false \
"SELECT COUNT(*), MAX(load_timestamp) FROM weedme-379116.WM_SalesTeam.Accounts_List"
```

### **Monitoring Commands**
```bash
# Check recent builds
gcloud builds list --limit=5

# View Cloud Run logs
gcloud logs read "resource.type=cloud_run_revision" --limit=50

# Check Cloud Storage backups
gsutil ls gs://ez_focus_crm/accounts/raw/

# BigQuery table info
bq show weedme-379116:WM_SalesTeam.Accounts_List
```

---

## 🔧 Troubleshooting Guide

### **Common Issues**

#### **Build Failures**
- **Symptom**: Cloud Build shows failed status
- **Check**: GitHub commit triggered build
- **Fix**: Review build logs in Cloud Build console

#### **API Connection Issues**
- **Symptom**: "Error fetching data" in logs
- **Check**: API credentials and EZ Focus service status
- **Fix**: Verify username/password in `utils.py`

#### **BigQuery Load Failures**
- **Symptom**: Data not appearing in table
- **Check**: Cloud Run logs for BigQuery errors
- **Fix**: Verify dataset location and permissions

#### **Scheduling Issues**
- **Symptom**: Pipeline not running at 1:00 AM
- **Check**: Cloud Scheduler job status
- **Fix**: Verify scheduler configuration and service URL

### **Log Locations**
- **Cloud Run**: Google Cloud Console → Cloud Run → Service logs
- **Cloud Build**: Google Cloud Console → Cloud Build → Build history
- **Scheduler**: Google Cloud Console → Cloud Scheduler → Job logs

---

## 📈 Migration History

### **January 2025 - Major Update**
- **Python**: 3.9 → 3.11
- **BigQuery**: `Sales_Team` (US) → `WM_SalesTeam` (Montreal)
- **Dependencies**: Updated to latest versions
- **Records Migrated**: 2,684 accounts
- **Reason**: Dataform compatibility + regional consolidation

### **Previous Versions**
- **Original**: Python 3.9, manual deployment
- **V2**: Added CI/CD pipeline
- **V3**: Current production version

---

## 📚 Glossary

### **Technical Terms**
- **CI/CD**: Continuous Integration/Continuous Deployment
- **Cloud Run**: Google's serverless container platform
- **Artifact Registry**: Container image storage service
- **BigQuery**: Google's data warehouse service
- **Cloud Build**: Automated build service
- **GCS**: Google Cloud Storage

### **Business Terms**
- **EZ Focus**: CRM system used by cannabis retail industry
- **SUCC/LRS/MED**: Store type classifications
- **Ontario (ON)**: Target province for data collection
- **Agent 1420**: Specific sales agent identifier

### **Data Terms**
- **WRITE_TRUNCATE**: BigQuery mode that replaces all data
- **Load Timestamp**: When data was processed
- **ETL**: Extract, Transform, Load process
- **Data Pipeline**: Automated data processing workflow

### **File Extensions**
- **`.py`**: Python source code
- **`.md`**: Markdown documentation
- **`.json`**: JSON data format
- **`.csv`**: Comma-separated values
- **`.txt`**: Plain text files

---

## 🎯 Quick Start Checklist

### **For New Developers**
- [ ] Clone repository: `git clone https://github.com/diegoWM/CRM.git`
- [ ] Review this technical summary
- [ ] Check Cloud Run service status
- [ ] Verify BigQuery table has recent data
- [ ] Test CI/CD by making small change

### **For Maintenance**
- [ ] Monitor daily pipeline execution
- [ ] Check Cloud Storage for backup files
- [ ] Review error logs weekly
- [ ] Update dependencies quarterly
- [ ] Verify API credentials annually

---

## 📞 Emergency Contacts

### **Services Status**
- **EZ Focus API**: Check with EZ Focus support
- **Google Cloud**: Check GCP status page
- **GitHub**: Check GitHub status page

### **Key Metrics**
- **Expected Records**: ~2,600-2,700 accounts
- **Pipeline Duration**: 2-5 minutes
- **Success Rate**: >99% expected
- **Data Freshness**: Updated daily at 1:00 AM ET

---

*Last Updated: January 2025*  
*Document Version: 1.0*  
*Pipeline Version: Python 3.11 + WM_SalesTeam* 