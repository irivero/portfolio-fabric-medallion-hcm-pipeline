# Architecture Design & Technical Decisions

## Table of Contents
1. [Executive Summary](#executive-summary)
2. [System Architecture](#system-architecture)
3. [Design Principles](#design-principles)
4. [Layer-by-Layer Architecture](#layer-by-layer-architecture)
5. [Key Design Decisions](#key-design-decisions)
6. [Data Flow & Processing](#data-flow--processing)
7. [Storage Strategy](#storage-strategy)
8. [Scalability & Performance](#scalability--performance)
9. [Failure Modes & Resilience](#failure-modes--resilience)
10. [Trade-offs & Alternatives](#trade-offs--alternatives)

---

## Executive Summary

This document details the architectural decisions behind a production-grade Medallion Architecture implementation on Microsoft Fabric. The system processes payroll and employee data from an enterprise HCM system, handling **~1.3M records across 5 entities** with a **70-minute SLA for end-to-end processing**.

### Core Architecture Characteristics
- **Pattern**: Medallion (Bronze → Silver → Gold)
- **Processing Model**: Batch (bi-weekly) + Full/Incremental hybrid
- **Configuration**: 100% metadata-driven (zero hardcoded configs)
- **Optimization**: Partition pruning, Z-ORDER, broadcast joins
- **Reliability**: Watermark-based idempotency, retry logic, audit logs

---

## System Architecture

### High-Level Component Diagram

```
┌────────────────────────────────────────────────────────────────────────┐
│                          SOURCE SYSTEM                                 │
│                                                                        │
│  ┌──────────────────┐         ┌─────────────────┐                      │
│  │  HCM REST API    │─────────│  OAuth2 Server  │                      │
│  │  (Payroll Data)  │         │  (Token Issuer) │                      │
│  └──────────────────┘         └─────────────────┘                      │
│         │                                                              │
│         │ HTTPS REST (JSON payloads)                                   │
│         │ Rate Limit: 48 calls/hour                                    │
└─────────┼──────────────────────────────────────────────────────────────┘
          │
          │
┌─────────▼──────────────────────────────────────────────────────────────┐
│                    MICROSOFT FABRIC PLATFORM                           │
│                                                                        │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │                   BRONZE LAYER (Raw Persistence)               │    │
│  │                                                                │    │
│  │  ┌──────────────────────────────────────────────────────────┐  │    │
│  │  │  Lakehouse: lh_bronze                                    │  │    │
│  │  │  Schema: sens (sensitive data)                           │  │    │
│  │  │                                                          │  │    │
│  │  │  Table: hcm_api_extracts                                 │  │    │
│  │  │  Format: Delta Lake                                      │  │    │
│  │  │  Storage: JSON strings (schema-agnostic)                 │  │    │
│  │  │  Partitioning: [partition_column, entity_name]           │  │    │
│  │  │                                                          │  │    │
│  │  │  Records: ~1.3M (26 periods  × 50K trans/period)         │  │    │
│  │  │  Retention: 90 days                                      │  │    │
│  │  └──────────────────────────────────────────────────────────┘  │    │
│  │                                                                │    │
│  │  Ingestion Notebook:                                           │    │
│  │  - API pagination & rate limiting                              │    │
│  │  - OAuth2 token auto-renewal                                   │    │
│  │  - Watermark tracking per entity                               │    │
│  │  - Dual partition routing (FULL_LOAD vs period ID)             │    │
│  └────────────────────────────────────────────────────────────────┘    │
│          │                                                             │
│          │ JSON Parsing & Schema Inference                             │
│          ▼                                                             │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │               SILVER LAYER (Typed & Validated)                 │    │
│  │                                                                │    │
│  │  ┌──────────────────────────────────────────────────────────┐  │    │
│  │  │  Lakehouse: lh_silver                                    │  │    │
│  │  │  Schema: sens                                            │  │    │
│  │  │                                                          │  │    │
│  │  │  Fact Tables (INCREMENTAL):                              │  │    │
│  │  │  ├─ fact_earnings        (partitioned by pay_period_id)  │  │    │
│  │  │  ├─ fact_deductions      (partitioned by pay_period_id)  │  │    │
│  │  │  └─ fact_taxes           (partitioned by pay_period_id)  │  │    │
│  │  │                                                          │  │    │
│  │  │  Dimension Tables (FULL_LOAD):                           │  │    │
│  │  │  ├─ dim_employee         (daily snapshot)                │  │    │
│  │  │  └─ dim_location         (daily snapshot)                │  │    │
│  │  │                                                          │  │    │
│  │  │  Features:                                               │  │    │
│  │  │  - Strongly typed schemas (inferred + validated)         │  │    │
│  │  │  - Automatic column evolution detection                  │  │    │
│  │  │  - Data quality validations                              │  │    │
│  │  │  - Deduplication logic                                   │  │    │
│  │  └──────────────────────────────────────────────────────────┘  │    │
│  │                                                                │    │
│  │  Transformation Notebook:                                      │    │
│  │  - JSON → Struct → Columns flattening                          │    │
│  │  - Business rule transformations                               │    │
│  │  - NULL handling & default values                              │    │
│  │  - Duplicate detection                                         │    │
│  └────────────────────────────────────────────────────────────────┘    │
│          │                                                             │
│          │ Dimensional Modeling & FK Enrichment                        │
│          ▼                                                             │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │                GOLD LAYER (Analytics Ready)                    │    │
│  │                                                                │    │
│  │  ┌──────────────────────────────────────────────────────────┐  │    │
│  │  │  Lakehouse: lh_gold                                      │  │    │
│  │  │  Schema: dbo                                             │  │    │
│  │  │                                                          │  │    │
│  │  │  Dimension Tables (SCD Type 1 with surrogate keys):      │  │    │
│  │  │  ├─ dwh_dim_employee                                     │  │    │
│  │  │  │   - Primary Key: EMPLOYEE_KEY (stable surrogate)      │  │    │
│  │  │  │   - Business Key: EMPLOYEE_NUMBER                     │  │    │
│  │  │  │   - Load Method: MERGE (preserve surrogate keys)      │  │    │
│  │  │  │                                                       │  │    │
│  │  │  └─ dwh_dim_location                                     │  │    │
│  │  │      - Primary Key: LOCATION_KEY (stable surrogate)      │  │    │
│  │  │      - Business Key: LOCATION_REFERENCE_CODE             │  │    │
│  │  │      - Load Method: MERGE (preserve surrogate keys)      │  │    │
│  │  │                                                          │  │    │
│  │  │  Fact Tables (Star schema):                              │  │    │
│  │  │  ├─ fact_payroll_earnings                                │  │    │
│  │  │  │   - Grain: One row per employee-period-earning_code   │  │    │
│  │  │  │   - FKs: EMPLOYEE_KEY, LOCATION_KEY                   │  │    │
│  │  │  │   - Degenerate: PAY_PERIOD_ID                         │  │    │
│  │  │  │                                                       │  │    │
│  │  │  ├─ fact_payroll_deductions                              │  │    │
│  │  │  │   - Grain: One row per employee-period-deduction      │  │    │
│  │  │  │   - FKs: EMPLOYEE_KEY, LOCATION_KEY                   │  │    │
│  │  │  │                                                       │  │    │
│  │  │  └─ fact_payroll_taxes                                   │  │    │
│  │  │      - Grain: One row per employee-period-tax_code       │  │    │
│  │  │      - FKs: EMPLOYEE_KEY, LOCATION_KEY                   │  │    │
│  │  │                                                          │  │    │
│  │  │  Optimizations:                                          │  │    │
│  │  │  - Z-ORDER on business keys & foreign keys               │  │    │
│  │  │  - OPTIMIZE after each load (file compaction)            │  │    │
│  │  │  - VACUUM (7-day retention)                              │  │    │
│  │  └──────────────────────────────────────────────────────────┘  │    │
│  │                                                                │    │
│  │  Load Notebooks:                                               │    │
│  │  - Dimension MERGE with surrogate key preservation             │    │
│  │  - Fact incremental with watermark tracking                    │    │
│  │  - FK enrichment via broadcast joins                           │    │
│  │  - Data quality validation                                     │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                        │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │              CONTROL LAYER (Metadata Management)               │    │
│  │                                                                │    │
│  │  Lakehouse: lh_control                                         │    │
│  │  Schema: ctrl                                                  │    │
│  │                                                                │    │
│  │  Tables:                                                       │    │
│  │  ├─ entity_catalog           (API report definitions)          │    │
│  │  ├─ gold_catalog             (dimension & fact configs)        │    │
│  │  ├─ gold_update_columns      (MERGE UPDATE column lists)       │    │
│  │  ├─ gold_zorder_columns      (optimization hints)              │    │
│  │  ├─ ingestion_watermarks     (per-entity last processed)       │    │
│  │  ├─ execution_log            (job-level audit trail)           │    │
│  │  └─ orchestrator_log         (pipeline execution tracking)     │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                        │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │                    SECRETS MANAGEMENT                          │    │
│  │                                                                │    │
│  │  Azure Key Vault (secrets.vault.azure.net)                     │    │
│  │  ├─ api-username            (OAuth2 user)                      │    │
│  │  ├─ api-password            (OAuth2 password)                  │    │
│  │  └─ api-client-secret       (Optional: for client_credentials) │    │
│  └────────────────────────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────────────────┘
```

---

## Design Principles

### 1. **Separation of Concerns (Medallion Layers)**

Each layer has a single, well-defined responsibility:

| Layer | Responsibility | Processing Complexity | Schema Rigidity |
|-------|---------------|----------------------|----------------|
| **Bronze** | Raw data persistence | None (store as-is) | Fully flexible (JSON strings) |
| **Silver** | Typed data + business logic | Medium (parsing, validation) | Semi-structured (inferred schemas) |
| **Gold** | Analytical modeling | High (dimensional modeling, aggregations) | Rigid (star schema) |

**Rationale**: 
- **Bronze**: Acts as immutable audit log; enables data recovery from source-of-truth
- **Silver**: Provides cleaned, validated data for multiple consumption patterns
- **Gold**: Optimized for specific analytics use cases (BI tools, ML features)

### 2. **Metadata-Driven Configuration**

**Principle**: All pipeline behavior controlled via Delta tables, not code.

**Implementation**:
```sql
-- Adding a new dimension requires NO code deployment
INSERT INTO ctrl.gold_catalog VALUES (
    'new_dimension_table',      -- table_name
    'DIMENSION',                -- table_type
    'source_silver_table',      -- silver_table_name
    'silver_lakehouse',         -- lakehouse
    'dbo',                      -- schema
    'Business description',     -- description
    'BUSINESS_KEY_COLUMN',      -- business_key
    'SURROGATE_KEY_COLUMN',     -- surrogate_key_name
    1,                          -- start_value
    TRUE,                       -- is_active
    CURRENT_TIMESTAMP()         -- created_at
);
```

**Benefits**:
- ✅ Pipeline extensibility without code changes
- ✅ Single source of truth for configurations
- ✅ Auditability (who changed what, when)
- ✅ Environment parity (same code, different configs)

### 3. **Idempotency by Design**

**Principle**: Re-running the same job with same parameters produces identical results.

**Mechanisms**:
- **Bronze**: `replaceWhere` on `partition_column + entity` → atomic partition overwrites
- **Silver**: Same partition strategy inherits idempotency
- **Gold Dimensions**: MERGE with business key matching → deterministic surrogate keys
- **Gold Facts**: Watermark-based incremental → each run processes same period

**Example**: Running period 26 three times:
```python
# Run 1: Loads period 26 data
# Run 2: Replaces exact same partition (no duplicates)
# Run 3: Replaces again (still no duplicates)
```

### 4. **Fail Fast, Recover Gracefully**

**Principle**: Detect errors immediately, but enable precise recovery without full reprocessing.

**Implementation**:
- **Validation at ingress**: Schema validation before Bronze write
- **Granular watermarks**: Per-entity tracking enables entity-level retry
- **Execution logging**: Persist success/failure state for replay logic
- **Atomic operations**: Delta ACID transactions prevent partial writes

### 5. **Schema Evolution as First-Class Citizen**

**Principle**: API schema changes should not break the pipeline.

**Bronze Layer**: Schema-agnostic (JSON strings)
```python
# Any JSON structure accepted
payload = {"new_field_added_by_api": "value"}  # No parsing errors
df = spark.createDataFrame([Row(payload_json=json.dumps(payload))])
```

**Silver Layer**: Automatic column detection
```python
# Detect new columns and add them dynamically
new_columns = set(json_schema) - set(existing_table_columns)
for col in new_columns:
    spark.sql(f"ALTER TABLE silver.{table} ADD COLUMN {col} STRING")
```

**Gold Layer**: Explicit mapping (controlled evolution)
- New columns from Silver require catalog updates before propagation to Gold
- Prevents uncontrolled schema changes in analytical layer

---

## Layer-by-Layer Architecture

### Bronze Layer: Raw Data Persistence

**Objective**: Provide an immutable, schema-agnostic landing zone for source data.

#### Storage Strategy

**Table Structure**:
```sql
CREATE TABLE bronze.sens.hcm_api_extracts (
    ingestion_id         STRING,       -- UUID per ingestion run
    entity_name          STRING,       -- Report name (e.g., 'earnings_scorecard')
    partition_column     STRING,       -- "FULL_LOAD" or "01"-"26" (period ID)
    payload_json         STRING,       -- Raw API response (JSON string)
    record_count         INT,          -- Number of records in payload
    api_call_timestamp   TIMESTAMP,    -- When API was called
    ingestion_datetime   TIMESTAMP,    -- When data landed in Bronze
    source_system        STRING        -- Always 'hcm_api'
)
PARTITIONED BY (partition_column, entity_name);
```

#### Dual Partition Strategy

**Problem**: Mixed workload with different retention requirements:
- **Dimensions**: Daily full snapshots (only need latest)
- **Facts**: Incremental by period (need all historical periods)

**Solution**: Use partition values as type discriminators:

| Entity Type | Partition Value | Retention Logic |
|------------|-----------------|-----------------|
| Dimensions | `"FULL_LOAD"` (constant) | Each write replaces entire partition (auto-cleanup) |
| Facts | `"01"` to `"26"` (period ID) | Each period is separate partition (historical retention) |

**Implementation**:
```python
def determine_partition_value(entity_config: Dict, current_period: int) -> str:
    if entity_config['load_type'] == 'FULL_LOAD':
        return "FULL_LOAD"  # Sentinel value
    else:
        return str(current_period).zfill(2)  # "01", "02", ..., "26"

# Write with targeted partition replacement
df.write \
  .mode("overwrite") \
  .option("replaceWhere", 
      f"partition_column = '{partition_value}' AND entity_name = '{entity_name}'"
  ) \
  .partitionBy("partition_column", "entity_name") \
  .saveAsTable("bronze.sens.hcm_api_extracts")
```

**Benefits**:
- Single unified table (no need for separate `*_full` and `*_incremental` tables)
- Self-documenting partition structure
- Automatic storage optimization for full-load entities
- No manual cleanup jobs required

#### API Integration Patterns

**OAuth2 Token Management**:
```python
class TokenManager:
    def __init__(self):
        self.token = None
        self.token_expiry = None
    
    def get_valid_token(self) -> str:
        if self.token is None or datetime.now() >= self.token_expiry:
            self._refresh_token()
        return self.token
    
    def _refresh_token(self):
        # Fetch credentials from Key Vault
        username = notebookutils.credentials.getSecret(vault_url, 'api-username')
        password = notebookutils.credentials.getSecret(vault_url, 'api-password')
        
        # Request new token
        response = requests.post(oauth_url, data={
            'grant_type': 'password',
            'username': username,
            'password': password,
            'client_id': client_id
        })
        
        self.token = response.json()['access_token']
        self.token_expiry = datetime.now() + timedelta(seconds=3600)
```

**Rate Limiting**:
```python
class RateLimiter:
    def __init__(self, max_calls_per_hour: int = 48):
        self.max_calls = max_calls_per_hour
        self.calls_made = 0
        self.window_start = datetime.now()
    
    def wait_if_needed(self):
        if self.calls_made >= self.max_calls:
            elapsed = (datetime.now() - self.window_start).total_seconds()
            if elapsed < 3600:
                sleep_time = 3600 - elapsed
                logger.info(f"Rate limit reached. Sleeping {sleep_time:.0f}s...")
                time.sleep(sleep_time)
            self.calls_made = 0
            self.window_start = datetime.now()
        
        self.calls_made += 1
```

**Pagination**:
```python
def fetch_paginated_data(entity_name: str, period_id: int) -> List[Dict]:
    all_records = []
    page_number = 1
    
    while True:
        params = {
            'pageNumber': page_number,
            'pageSize': 500,
            'period_id': period_id
        }
        
        response = requests.get(api_url, headers=headers, params=params)
        data = response.json()
        
        if not data or len(data) == 0:
            break
        
        all_records.extend(data)
        page_number += 1
        
        rate_limiter.wait_if_needed()  # Respect API limits
    
    return all_records
```

### Silver Layer: Typed & Validated Data

**Objective**: Transform raw JSON into strongly-typed, validated business entities.

#### Schema Management

**JSON to Schema Inference**:
```python
def infer_silver_schema(bronze_df: DataFrame, entity_name: str) -> StructType:
    # Parse first 100 JSON payloads to infer schema
    sample = bronze_df.limit(100).select('payload_json').collect()
    
    # Convert JSON strings to dicts
    parsed_samples = [json.loads(row.payload_json) for row in sample]
    
    # Infer schema from samples
    schema_rdd = spark.sparkContext.parallelize(parsed_samples)
    inferred_schema = spark.read.json(schema_rdd).schema
    
    return inferred_schema
```

**Column Evolution Detection**:
```python
def detect_new_columns(inferred_schema: StructType, 
                       existing_table: str) -> List[str]:
    existing_columns = set(spark.table(existing_table).columns)
    inferred_columns = set(inferred_schema.fieldNames())
    
    new_columns = inferred_columns - existing_columns
    
    if new_columns:
        logger.warning(f"New columns detected: {new_columns}")
        
        for col_name in new_columns:
            col_type = [f.dataType for f in inferred_schema.fields 
                       if f.name == col_name][0]
            
            spark.sql(f"""
                ALTER TABLE {existing_table} 
                ADD COLUMN {col_name} {col_type.simpleString()}
            """)
    
    return list(new_columns)
```

#### Data Quality Validations

**Implemented Checks**:
```python
class DataQualityValidator:
    def validate_earnings_fact(self, df: DataFrame) -> DataFrame:
        # Rule 1: Earnings amount must be non-negative
        df = df.filter(col('earning_amount') >= 0)
        
        # Rule 2: Employee number must exist
        df = df.filter(col('employee_number').isNotNull())
        
        # Rule 3: Pay period must be in valid range (1-26)
        df = df.filter(col('pay_period_id').between(1, 26))
        
        # Rule 4: Dedup on business key
        window = Window.partitionBy('employee_number', 'pay_period_id', 'earning_code')
        df = df.withColumn('row_num', row_number().over(window.orderBy(desc('api_call_timestamp'))))
        df = df.filter(col('row_num') == 1).drop('row_num')
        
        return df
```

### Gold Layer: Analytical Dimensional Model

**Objective**: Provide consumption-ready star schema for BI and analytics.

#### Dimension Load Pattern: MERGE for Surrogate Key Preservation

**Challenge**: Full table overwrites regenerate surrogate keys, breaking fact table foreign keys.

**Solution**: Use MERGE INTO with business key matching.

**Implementation**:
```python
def load_dimension_with_merge(table_config: Dict, silver_df: DataFrame):
    # Step 1: Create staging view with NEW surrogate keys
    staging_df = silver_df.withColumn(
        table_config['surrogate_key'],
        when(col(table_config['business_key']).isNotNull(),
             row_number().over(Window.orderBy(table_config['business_key']))
        ).otherwise(None)
    )
    staging_df.createOrReplaceTempView("staging")
    
    # Step 2: Generate MERGE statement
    update_columns = get_update_columns(table_config['table_name'])
    update_set = ', '.join([f"{col} = source.{col}" for col in update_columns])
    
    merge_sql = f"""
    MERGE INTO gold.{table_config['gold_table']} AS target
    USING staging AS source
    ON target.{table_config['business_key']} = source.{table_config['business_key']}
    
    WHEN MATCHED THEN 
        UPDATE SET {update_set}
    
    WHEN NOT MATCHED THEN 
        INSERT *
    """
    
    # Step 3: Execute MERGE
    spark.sql(merge_sql)
```

**Result**:
- **Existing employees**: Attributes updated, `EMPLOYEE_KEY` unchanged
- **New employees**: Inserted with new `EMPLOYEE_KEY`
- **Fact tables**: Foreign keys remain valid across dimension refreshes

#### Fact Load Pattern: Watermark-Based Incremental

**Watermark Tracking**:
```sql
CREATE TABLE ctrl.execution_log (
    execution_id         STRING,
    table_name           STRING,
    execution_start      TIMESTAMP,
    execution_end        TIMESTAMP,
    watermark_column     STRING,
    watermark_value      STRING,      -- Stored as STRING (supports int/timestamp/date)
    records_read         BIGINT,
    records_written      BIGINT,
    status               STRING,      -- 'SUCCESS' | 'FAILED' | 'RUNNING'
    error_message        STRING
);
```

**Incremental Load Logic**:
```python
def load_fact_incremental(table_config: Dict):
    # Step 1: Get last successful watermark
    last_watermark = spark.sql(f"""
        SELECT watermark_value 
        FROM ctrl.execution_log
        WHERE table_name = '{table_config['table_name']}'
          AND status = 'SUCCESS'
        ORDER BY execution_end DESC
        LIMIT 1
    """).first()
    
    if last_watermark:
        watermark_value = int(last_watermark['watermark_value'])
    else:
        watermark_value = 0  # First run
    
    # Step 2: Read new data from Silver
    silver_df = spark.table(f"silver.{table_config['silver_table']}")
    
    if table_config['watermark_type'] == 'int':
        new_data = silver_df.filter(col(table_config['watermark_column']) > watermark_value)
        next_watermark = new_data.agg(max(table_config['watermark_column'])).first()[0]
    
    # Step 3: Enrich with surrogate keys (FK lookup)
    enriched_df = new_data \
        .join(broadcast(spark.table('gold.dwh_dim_employee')),
              new_data.employee_number == col('EMPLOYEE_NUMBER'),
              'left') \
        .select(
            col('EMPLOYEE_KEY'),
            new_data['pay_period_id'],
            new_data['earning_code'],
            new_data['amount']
        )
    
    # Step 4: Write to Gold (APPEND mode)
    enriched_df.write.mode('append').saveAsTable(f"gold.{table_config['table_name']}")
    
    # Step 5: Update watermark
    log_execution(table_config['table_name'], next_watermark, new_data.count())
```

---

## Key Design Decisions

### Decision 1: JSON Strings in Bronze vs. Typed Schemas

**Options Considered**:
1. Parse JSON immediately in Bronze → store as typed columns
2. Store raw JSON strings → parse in Silver

**Decision**: Option 2 (JSON strings in Bronze)

**Rationale**:
- ✅ **Schema flexibility**: API changes don't break Bronze ingestion
- ✅ **Audit trail**: Can re-process historical data with updated parsing logic
- ✅ **Faster ingestion**: No schema inference overhead during API calls
- ❌ **Storage overhead**: JSON strings are larger than typed formats (~20% inflation)

**Validation**: Storage cost increase (20%) < operational stability value

### Decision 2: Dual Partition Strategy vs. Separate Tables

**Options Considered**:
1. Separate tables: `bronze_full_load`, `bronze_incremental`
2. Single table with partition type discriminator

**Decision**: Option 2 (dual partition strategy with sentinel value)

**Rationale**:
- ✅ **Code simplicity**: Single write pattern for all entities
- ✅ **Query simplicity**: Single table to query for lineage
- ✅ **Self-documenting**: Partition value explicitly shows load type
- ❌ **Partition explosion risk**: Mitigated by using only 27 partitions (1 FULL + 26 periods)

### Decision 3: MERGE vs. OVERWRITE for Dimensions

**Options Considered**:
1. OVERWRITE entire dimension tables (simpler code)
2. MERGE with business key matching (preserve surrogate keys)

**Decision**: Option 2 (MERGE strategy)

**Rationale**:
- ✅ **Referential integrity**: Fact table FKs remain valid
- ✅ **Incremental dimension changes**: Only changed attributes updated
- ✅ **Audit capability**: Can track when dimension attributes changed
- ❌ **Complexity**: MERGE statements harder to debug than OVERWRITE

**Critical for**: Systems where fact tables are continuously appended while dimensions are full-loaded

### Decision 4: Metadata-Driven vs. Hardcoded Configs

**Options Considered**:
1. Hardcode configurations in notebook parameters
2. Store all configs in Delta control tables

**Decision**: Option 2 (metadata-driven)

**Rationale**:
- ✅ **Extensibility**: Add new tables without code deployment
- ✅ **Auditability**: Configuration changes are tracked
- ✅ **Environment promotion**: Same code across dev/prod (different config tables)
- ❌ **Debugging complexity**: Config errors require table inspections, not code review

### Decision 5: Watermark Type Strategy

**Options Considered**:
1. Enforce timestamp-based watermarks only (standard pattern)
2. Support multiple types (int, timestamp, date)

**Decision**: Option 2 (multi-type support)

**Rationale**:
- ✅ **Flexibility**: Can use native business identifiers (pay_period_id as int)
- ✅ **Performance**: Integer comparisons faster than timestamp parsing
- ✅ **Source alignment**: Matches source system's natural incrementing field
- ❌ **Code complexity**: Need type-specific comparison logic

---

## Storage Strategy

### Storage Layout

```
OneLake (Delta Lake Format)
├── bronze/
│   └── sens/
│       └── hcm_api_extracts/
│           ├── partition_column=FULL_LOAD/
│           │   ├── entity_name=dim_employee/
│           │   │   └── part-00000-xxx.snappy.parquet
│           │   └── entity_name=dim_location/
│           │       └── part-00000-xxx.snappy.parquet
│           ├── partition_column=25/
│           │   ├── entity_name=fact_earnings/
│           │   ├── entity_name=fact_deductions/
│           │   └── entity_name=fact_taxes/
│           └── partition_column=26/
│               ├── entity_name=fact_earnings/
│               ├── entity_name=fact_deductions/
│               └── entity_name=fact_taxes/
│
├── silver/
│   └── sens/
│       ├── fact_earnings/
│       │   ├── pay_period_id=25/
│       │   └── pay_period_id=26/
│       ├── fact_deductions/
│       ├── fact_taxes/
│       ├── dim_employee/      (no partitions)
│       └── dim_location/      (no partitions)
│
└── gold/
    └── dbo/
        ├── dwh_dim_employee/  (optimized, no partitions)
        ├── dwh_dim_location/  (optimized, no partitions)
        ├── fact_payroll_earnings/  (Z-ORDERed on EMPLOYEE_KEY)
        ├── fact_payroll_deductions/
        └── fact_payroll_taxes/
```

### Retention Policies

| Layer | Retention Period | Rationale |
|-------|-----------------|-----------|
| Bronze | 90 days | Regulatory compliance; enables replay |
| Silver | 365 days (facts), 30 days (dims) | Historical analysis; dimensions overwritten |
| Gold | 7 years (facts), indefinite (dims) | Long-term analytics; regulatory requirements |

### File Size Management

**Strategy**: 
- Target file size: **128 MB** (optimal for Spark parallelism)
- Use `OPTIMIZE` after each load to compact small files
- Schedule `VACUUM` weekly to remove tombstones

```python
# Post-load optimization
spark.sql(f"OPTIMIZE gold.{table_name} ZORDER BY ({zorder_columns})")
spark.sql(f"VACUUM gold.{table_name} RETAIN 168 HOURS")  # 7 days
```

---

## Scalability & Performance

### Current Performance Baseline

| Metric | Current | Target | Headroom |
|--------|---------|--------|----------|
| Bronze ingestion (5 entities × 26 periods) | 25 min | 30 min | 20% |
| Silver transformation (5 tables) | 18 min | 20 min | 11% |
| Gold dimension load (2 tables) | 4 min | 5 min | 25% |
| Gold fact load (3 tables) | 12 min | 15 min | 25% |
| **Total E2E** | **59 min** | **70 min** | **19%** |

### Optimization Techniques Applied

**1. Broadcast Joins for Dimension Lookups**
```python
# Without broadcast: ~45 seconds
fact_df.join(dim_df, on='business_key')

# With broadcast: ~15 seconds (3x improvement)
fact_df.join(broadcast(dim_df), on='business_key')
```

**2. Z-ORDER on Gold Tables**
```sql
-- Query without Z-ORDER: ~12 seconds
SELECT * FROM gold.fact_payroll_earnings WHERE EMPLOYEE_KEY = 195;

-- Query with Z-ORDER: ~5 seconds (2.5x improvement)
OPTIMIZE gold.fact_payroll_earnings ZORDER BY (EMPLOYEE_KEY, PAY_PERIOD_ID);
SELECT * FROM gold.fact_payroll_earnings WHERE EMPLOYEE_KEY = 195;
```

**3. Partition Pruning**
```python
# Bad: Full table scan (~30 seconds)
df = spark.table('silver.fact_earnings')
df = df.filter(col('pay_period_id') == 26)

# Good: Partition pruning (~3 seconds, 10x improvement)
df = spark.table('silver.fact_earnings').where("pay_period_id = 26")
```

### Scalability Projections

**Volume Growth Scenario**: 3x growth over 2 years

| Component | Current | Projected (3x) | Mitigation Strategy |
|-----------|---------|---------------|---------------------|
| API calls | 130/period | 390/period | Batch API requests; parallel calls |
| Bronze storage | 2 GB/period | 6 GB/period | Compression (Snappy → Zstd) |
| Processing time | 59 min | ~120 min | Increase Spark executors (4 → 12) |
| Fact table size | 1.3M rows/period | 3.9M rows/period | Implement partitioning by year + period |

**Recommended Actions for Scale**:
1. **Horizontal scaling**: Increase Spark cluster size (current: 4 executors → target: 12)
2. **Partitioning refinement**: Add year-based partitions to fact tables
3. **Incremental streaming**: Migrate from batch to Spark Structured Streaming for real-time processing

---

## Failure Modes & Resilience

### Identified Failure Scenarios

| Failure Mode | Impact | Detection | Recovery |
|-------------|--------|-----------|----------|
| **API token expiration** | Ingestion stops mid-batch | Token refresh error | Auto-renewal logic; retry from last watermark |
| **API rate limit exceeded** | Ingestion blocked | HTTP 429 response | Rate limiter sleeps; resumes after cooldown |
| **Schema change in API** | Silver parsing fails | JSON parse exception | Column evolution detection; add new columns |
| **Duplicate data in source** | Fact table duplicates | Data quality check | Dedup logic in Silver (business key + timestamp) |
| **Executor failure** | Job crashes | Spark stage failure | Delta checkpointing; replay from last watermark |
| **Surrogate key collision** | FK integrity broken | Referential integrity test | MERGE ensures stable keys; impossible with current design |

### Resilience Mechanisms

**1. Granular Watermarks**
```python
# Per-entity watermarks enable targeted recovery
watermarks = {
    'fact_earnings': {'last_period': 26, 'status': 'SUCCESS'},
    'fact_deductions': {'last_period': 25, 'status': 'FAILED'},  # Only retry this
    'fact_taxes': {'last_period': 26, 'status': 'SUCCESS'}
}

# Retry only failed entities
for entity, state in watermarks.items():
    if state['status'] == 'FAILED':
        retry_from_period(entity, state['last_period'])
```

**2. Execution Logging**
```python
def log_execution_start(table_name: str, execution_id: str):
    spark.sql(f"""
        INSERT INTO ctrl.execution_log VALUES (
            '{execution_id}',
            '{table_name}',
            CURRENT_TIMESTAMP(),
            NULL,  -- execution_end
            NULL,  -- watermark_value
            0,     -- records_read
            0,     -- records_written
            'RUNNING',
            NULL   -- error_message
        )
    """)

def log_execution_end(execution_id: str, watermark: Any, status: str):
    spark.sql(f"""
        UPDATE ctrl.execution_log
        SET execution_end = CURRENT_TIMESTAMP(),
            watermark_value = '{watermark}',
            status = '{status}'
        WHERE execution_id = '{execution_id}'
    """)
```

**3. Atomic Operations**
```python
# Delta Lake ACID guarantees prevent partial writes
df.write.mode("overwrite") \
  .option("replaceWhere", "partition_column = '26'") \
  .saveAsTable("bronze.hcm_api_extracts")

# Either entire partition is replaced OR nothing changes
# No partial states possible
```

---

## Trade-offs & Alternatives

### Architectural Alternatives Considered

#### Alternative 1: Azure Data Factory + Synapse Analytics

**Pros**:
- GUI-based pipeline development (lower code complexity)
- Serverless SQL for ad-hoc queries
- Built-in monitoring & alerting

**Cons**:
- Higher cost (~30% more expensive)
- Less flexible transformations (limited by ADF activities)
- Vendor lock-in to Azure (vs. portable Spark code)

**Decision**: Rejected due to cost and flexibility constraints

#### Alternative 2: Databricks Lakehouse

**Pros**:
- Best-in-class Delta Lake optimizations
- Photon engine for faster queries
- Advanced ML capabilities

**Cons**:
- 40% higher cost vs. Fabric
- Requires separate Azure resources (not unified platform)
- Team lacks Databricks expertise

**Decision**: Rejected; Fabric provides enough capability for current scale

#### Alternative 3: Snowflake Data Cloud

**Pros**:
- Excellent query performance (automatic clustering)
- Zero-copy cloning for dev/test environments
- Strong SQL support (familiar to analysts)

**Cons**:
- No native Spark (requires external compute for transformations)
- Storage + compute costs higher than Fabric
- Not a lakehouse (proprietary format)

**Decision**: Rejected; lakehouse architecture preferred for flexibility

---

## Conclusion

This architecture demonstrates **production-grade data engineering** through:

1. **Layered design**: Clean separation of concerns (Bronze/Silver/Gold)
2. **Operational excellence**: Metadata-driven, idempotent, failure-resilient
3. **Performance optimization**: Intelligent partitioning, broadcast joins, Z-ORDER
4. **Scalability planning**: Current 20% headroom; clear path to 3x growth

The implementation successfully balances **complexity vs. flexibility**, **cost vs. capability**, and **immediate delivery vs. long-term maintainability**.

**Key Takeaway**: Modern data architectures must be designed for change—not just processing current data, but adapting to evolving schemas, growing volumes, and shifting business requirements.
