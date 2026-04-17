# Project Setup & Deployment Guide

## Prerequisites

### Required Infrastructure
- **Microsoft Fabric** workspace with:
  - Lakehouse capacity (F64 or higher recommended)
  - Spark compute pools configured
  - Admin permissions for lakehouse creation
  
- **Azure Key Vault**:
  - Vault created in same subscription
  - Network access configured for Fabric
  - Secrets stored:
    - `hcm-api-username`
    - `hcm-api-password`

- **Source System Access**:
  - REST API endpoint accessible from Fabric
  - OAuth2 credentials provisioned
  - Network firewall rules configured

### Required Permissions
| Resource | Role | Purpose |
|----------|------|---------|
| Fabric Workspace | Admin | Create lakehouses, run notebooks |
| Azure Key Vault | Secret Reader | Retrieve API credentials |
| Source API | API Consumer | Read payroll data |

---

## Step-by-Step Setup

### Phase 1: Infrastructure Provisioning

#### 1.1 Create Lakehouses

```python
# Execute in Fabric Notebook

lakehouses = [
    'lh_bronze',  # Raw data landing
    'lh_silver',  # Typed validated data
    'lh_gold',    # Star schema analytics
    'lh_control'  # Metadata and config
]

for lakehouse_name in lakehouses:
    # Create via Fabric UI or API
    print(f"Creating lakehouse: {lakehouse_name}")
    # fabric.create_lakehouse(name=lakehouse_name)
```

#### 1.2 Create Schemas

```sql
-- Run in Fabric SQL endpoint

-- Bronze layer
CREATE SCHEMA IF NOT EXISTS lh_bronze.sens COMMENT 'Sensitive raw data';

-- Silver layer
CREATE SCHEMA IF NOT EXISTS lh_silver.sens COMMENT 'Validated typed data';

-- Gold layer
CREATE SCHEMA IF NOT EXISTS lh_gold.dbo COMMENT 'Analytics star schema';

-- Control layer
CREATE SCHEMA IF NOT EXISTS lh_control.ctrl COMMENT 'Metadata and config';
```

---

### Phase 2: Control Tables Setup

#### 2.1 Create Control Tables

```bash
# Execute SQL script
notebooks/setup/setup_control_tables.ipynb

# OR run SQL file directly
spark.sql(open('sql/create_control_tables.sql').read())
```

**Verify**:
```sql
SHOW TABLES IN lh_control.ctrl;

-- Expected output: 8 tables
-- ✅ entity_catalog
-- ✅ ingestion_watermarks
-- ✅ gold_catalog
-- ✅ gold_update_columns
-- ✅ gold_zorder_columns
-- ✅ execution_log
-- ✅ orchestrator_execution_log
-- ✅ orchestrator_job_log
```

#### 2.2 Populate Entity Catalog

```python
# Run: notebooks/setup/populate_catalog.ipynb

# OR execute SQL

# Dimension entities (full load)
INSERT INTO lh_control.ctrl.entity_catalog VALUES
    ('dim_employee', 'Employee master data', '/api/v1/reports/employee_master', 'FULL_LOAD', NULL, NULL, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('dim_location', 'Location/company data', '/api/v1/reports/company_master', 'FULL_LOAD', NULL, NULL, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

# Fact entities (incremental)
INSERT INTO lh_control.ctrl.entity_catalog VALUES
    ('fact_earnings', 'Earnings scorecard', '/api/v1/reports/earnings_scorecard', 'INCREMENTAL', 1, 26, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('fact_deductions', 'Deductions scorecard', '/api/v1/reports/deductions_scorecard', 'INCREMENTAL', 1, 26, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    ('fact_taxes', 'Taxes scorecard', '/api/v1/reports/taxes_scorecard', 'INCREMENTAL', 1, 26, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
```

#### 2.3 Populate Gold Catalog

```python
# Load from JSON configs
import json

# Dimensions
with open('config/catalog_dimensions.json') as f:
    dim_config = json.load(f)

for dim in dim_config:
    spark.sql(f"""
        INSERT INTO lh_control.ctrl.gold_catalog VALUES (
            '{dim['table_name']}',
            'DIMENSION',
            '{dim['silver_source']['table']}',
            '{dim['silver_source']['lakehouse']}',
            '{dim['silver_source']['schema']}',
            '{dim['description']}',
            '{dim['keys']['business_key']}',
            '{dim['keys']['surrogate_key']}',
            {dim['keys']['surrogate_start_value']},
            NULL,
            NULL,
            NULL,
            '{','.join(dim['zorder_columns'])}',
            TRUE,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
        )
    """)

# Facts
with open('config/catalog_facts.json') as f:
    fact_config = json.load(f)

for fact in fact_config:
    spark.sql(f"""
        INSERT INTO lh_control.ctrl.gold_catalog VALUES (
            '{fact['table_name']}',
            'FACT',
            '{fact['silver_source']['table']}',
            '{fact['silver_source']['lakehouse']}',
            '{fact['silver_source']['schema']}',
            '{fact['description']}',
            NULL,
            NULL,
            NULL,
            '{fact['watermark']['column']}',
            '{fact['watermark']['type']}',
            NULL,
            '{','.join(fact['zorder_columns'])}',
            TRUE,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
        )
    """)
```

---

### Phase 3: Data Layer Tables

#### 3.1 Create Bronze Tables

```bash
# Run SQL script
spark.sql(open('sql/create_bronze_tables.sql').read())

# Verify
SELECT COUNT(*) FROM lh_bronze.sens.hcm_api_extracts;
# Expected: 0 (empty, awaiting data)
```

#### 3.2 Create Silver Tables

```bash
spark.sql(open('sql/create_silver_tables.sql').read())

# Verify
SHOW TABLES IN lh_silver.sens;
# Expected: 5 tables (2 dims + 3 facts)
```

#### 3.3 Create Gold Tables

```bash
spark.sql(open('sql/create_gold_tables.sql').read())

# Verify
SHOW TABLES IN lh_gold.dbo;
# Expected: 5 tables (2 dims + 3 facts)
```

---

### Phase 4: Secret Configuration

#### 4.1 Store Credentials in Key Vault

```bash
# Azure CLI commands
az keyvault secret set \
  --vault-name your-keyvault-name \
  --name hcm-api-username \
  --value "your_api_username"

az keyvault secret set \
  --vault-name your-keyvault-name \
  --name hcm-api-password \
  --value "your_api_password"
```

#### 4.2 Link Key Vault to Fabric

```python
# In Fabric Notebook
# Test secret retrieval

vault_url = "https://your-keyvault.vault.azure.net"

username = notebookutils.credentials.getSecret(vault_url, "hcm-api-username")
password = notebookutils.credentials.getSecret(vault_url, "hcm-api-password")

print("✅ Secrets retrieved successfully")
# Do NOT print actual values
```

---

### Phase 5: Configuration Files

#### 5.1 Update API Config

Edit `config/api_config_template.json`:
```json
{
  "api_connection": {
    "base_url": "https://your-actual-api-endpoint.com",
    "oauth_token_url": "https://your-auth-server.com/connect/token",
    ...
  }
}
```

#### 5.2 Update Notebook Parameters

In each notebook, update:
```python
# API Configuration
API_BASE_URL = "https://your-actual-api.com"
OAUTH_TOKEN_URL = "https://your-auth-server.com/connect/token"
KEYVAULT_URL = "https://your-keyvault.vault.azure.net"

# Lakehouse names (if different)
BRONZE_LAKEHOUSE = "lh_bronze"
SILVER_LAKEHOUSE = "lh_silver"
GOLD_LAKEHOUSE = "lh_gold"
CONTROL_LAKEHOUSE = "lh_control"
```

---

## Execution Workflow

### Initial Load (First Run)

**Recommended Order**:

```
1. Bronze Ingestion → 2. Silver Transformation → 3. Gold Load (Dims) → 4. Gold Load (Facts)
```

#### Step 1: Bronze Ingestion

```python
# Run: notebooks/bronze/ingest_api_bronze.ipynb

# Parameters:
CURRENT_PAY_PERIOD = 1  # Start with period 1
ENTITY_FILTER = None    # Process all entities

# Expected output:
# ✅ Ingested 5 entities
# ✅ Total records: ~45K
# ✅ Duration: ~17 minutes
```

#### Step 2: Silver Transformation

```python
# Run: notebooks/silver/transform_silver.ipynb

# Parameters:
ENTITY_FILTER = None  # Process all
ENABLE_SCHEMA_EVOLUTION = True

# Expected output:
# ✅ Transformed 5 tables
# ✅ Records: ~45K
# ✅ Duration: ~12 minutes
```

#### Step 3: Gold Dimensions

```python
# Run: notebooks/gold/load_dimensions.ipynb

# Expected output:
# ✅ Loaded 2 dimensions
# ✅ Employee count: ~10K
# ✅ Location count: ~150
# ✅ Duration: ~4 minutes
```

#### Step 4: Gold Facts

```python
# Run: notebooks/gold/load_facts.ipynb

# Parameters:
WATERMARK_START = None  # First run (process all available)

# Expected output:
# ✅ Loaded 3 fact tables
# ✅ Total records: ~45K
# ✅ Duration: ~12 minutes
```

**Total Initial Load Time**: ~45 minutes (for 1 period)

---

### Incremental Load (Subsequent Runs)

Use orchestrator for automated execution:

```python
# Run: notebooks/orchestration/orchestrator.ipynb

# Parameters:
CURRENT_PAY_PERIOD = 26           # Current period to process
RUN_MODE = "INCREMENTAL"          # Skip full-load dims if not needed
ENABLE_RETRY = True               # Retry failed jobs
MAX_RETRIES = 3

# Expected output:
# ✅ Bronze: ~17 minutes
# ✅ Silver: ~12 minutes
# ✅ Gold Dims: SKIPPED (no change)
# ✅ Gold Facts: ~12 minutes
# ✅ Total: ~41 minutes
```

---

## Validation & Testing

### Data Quality Checks

```sql
-- Verify record counts
SELECT 
    'Bronze' as layer,
    COUNT(*) as record_count
FROM lh_bronze.sens.hcm_api_extracts
WHERE partition_column = '26'

UNION ALL

SELECT 
    'Silver - Earnings',
    COUNT(*)
FROM lh_silver.sens.fact_earnings
WHERE pay_period_id = 26

UNION ALL

SELECT
    'Gold - Earnings',
    COUNT(*)
FROM lh_gold.dbo.fact_payroll_earnings
WHERE PAY_PERIOD_ID = 26;
```

### Referential Integrity

```sql
-- Check for orphan records (NULL FKs)
SELECT COUNT(*) as orphan_count
FROM lh_gold.dbo.fact_payroll_earnings
WHERE EMPLOYEE_KEY IS NULL OR LOCATION_KEY IS NULL;

-- Expected: 0
```

### Watermark Validation

```sql
-- Verify watermarks updated
SELECT 
    entity_name,
    last_processed_value,
    last_ingestion_time,
    records_ingested
FROM lh_control.ctrl.ingestion_watermarks
WHERE is_active = TRUE
ORDER BY entity_name;
```

---

## Troubleshooting

### Common Issues

**Issue 1: OAuth token expired**
```python
# Symptom: 401 Unauthorized errors
# Solution: Token auto-renewal is built-in; check Key Vault secrets

username = notebookutils.credentials.getSecret(vault_url, "hcm-api-username")
# Verify secret exists and is correct
```

**Issue 2: Rate limit exceeded**
```python
# Symptom: 429 Too Many Requests
# Solution: Reduce max_calls_per_hour in config

MAX_CALLS_PER_HOUR = 40  # Lower from 48
```

**Issue 3: Schema evolution failures**
```sql
-- Symptom: New columns not appearing in Silver
-- Solution: Check column evolution setting

ENABLE_SCHEMA_EVOLUTION = True  # In transformation notebook
```

**Issue 4: Surrogate key collisions**
```sql
-- Symptom: Duplicate EMPLOYEE_KEY values
-- Solution: Re-run dimension load with MERGE (not OVERWRITE)

# Verify MERGE logic in load_dimensions.ipynb
# load_strategy should be 'MERGE', not 'OVERWRITE'
```

---

## Monitoring & Maintenance

### Daily Checks

```sql
-- Check execution logs for failures
SELECT execution_id, table_name, status, error_message
FROM lh_control.ctrl.execution_log
WHERE execution_start >= CURRENT_DATE() - INTERVAL 1 DAY
  AND status = 'FAILED'
ORDER BY execution_start DESC;
```

### Weekly Maintenance

```sql
-- File compaction (OPTIMIZE)
OPTIMIZE lh_gold.dbo.fact_payroll_earnings;
OPTIMIZE lh_gold.dbo.fact_payroll_deductions;
OPTIMIZE lh_gold.dbo.fact_payroll_taxes;

-- Remove old versions (VACUUM)
VACUUM lh_bronze.sens.hcm_api_extracts RETAIN 168 HOURS;
VACUUM lh_silver.sens.fact_earnings RETAIN 168 HOURS;
VACUUM lh_gold.dbo.fact_payroll_earnings RETAIN 168 HOURS;
```

### Monthly Performance Review

```sql
-- Check table sizes and file counts
DESCRIBE DETAIL lh_gold.dbo.fact_payroll_earnings;

-- Refresh Z-ORDER if needed
OPTIMIZE lh_gold.dbo.fact_payroll_earnings
ZORDER BY (EMPLOYEE_KEY, PAY_PERIOD_ID);
```

---

## Scaling Considerations

### Current Limits
- **API rate limit**: 48 calls/hour
- **Processing time**: ~60 min for 26 periods (full backfill)
- **Storage**: ~2 GB per period in Bronze

### Scale-Up Options

**Option 1: Increase Capacity**
- Upgrade Fabric capacity: F64 → F128
- Add more Spark executors
- Expected: ~40% faster processing

**Option 2: Parallel API Calls**
- Modify Bronze to call multiple entities concurrently
- Requires rate limit coordination
- Expected: ~30% faster Bronze ingestion

**Option 3: Partitioning Refinement**
- Add year-based partitions to fact tables
- Better partition pruning for historical queries
- Expected: ~50% faster queries on old data

---

## Next Steps

After successful deployment:

1. **Schedule Orchestrator**: Set up bi-weekly pipeline runs
2. **Create BI Reports**: Connect Power BI to Gold layer
3. **Implement Alerts**: Set up notifications for failures
4. **Document SLAs**: Define and monitor SLA metrics
5. **Extend Catalog**: Add new entities as business needs evolve

---

## Support Resources

- **Architecture Documentation**: [docs/01_ARCHITECTURE.md](docs/01_ARCHITECTURE.md)
- **Layer-Specific Guides**: 
  - [Bronze](docs/02_BRONZE_LAYER.md)
  - [Silver](docs/03_SILVER_LAYER.md)
  - [Gold](docs/04_GOLD_LAYER.md)
- **Visual Diagrams**: [docs/diagrams/](docs/diagrams/)
- **Config Templates**: [config/](config/)

---

**Setup Complete! 🎉**

Your Medallion Architecture is now ready for production data processing.
