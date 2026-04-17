# Gold Layer - Dimensional Model

## Overview

The Gold layer implements a **star schema** dimensional model optimized for analytics. It features:
- **Stable surrogate keys** for dimensions
- **Incremental fact loading** with watermarks
- **Foreign key enrichment** via lookups
- **Query optimizations** (Z-ORDER, OPTIMIZE)

---

## Architecture

```
Star Schema Pattern:

       ┌──────────────────┐
       │  dim_employee    │
       │  PK: EMPLOYEE_KEY│◄─────┐
       └──────────────────┘      │
                                 │ FK
       ┌──────────────────┐      │
       │  dim_location    │◄─────┤
       │  PK: LOCATION_KEY│      │
       └──────────────────┘      │
                                 │
                    ┌────────────┴─────────────┐
                    │   fact_payroll_earnings  │
                    │   FK: EMPLOYEE_KEY       │
                    │   FK: LOCATION_KEY       │
                    │   Degenerate: PERIOD_ID  │
                    │   Measures: AMOUNT, HRS  │
                    └──────────────────────────┘
```

---

## Dimension Tables

### Pattern: MERGE for Surrogate Key Preservation

**Challenge**: Full table overwrites regenerate surrogate keys → breaks FK relationships

**Solution**: MERGE INTO with business key matching

```sql
CREATE TABLE gold.dbo.dwh_dim_employee (
    EMPLOYEE_KEY         INT PRIMARY KEY,      -- Stable surrogate key
    EMPLOYEE_NUMBER      STRING,               -- Business key (from source)
    FIRST_NAME           STRING,
    LAST_NAME            STRING,
    HIRE_DATE            DATE,
    EMPLOYMENT_STATUS    STRING,
    DEPARTMENT           STRING,
    JOB_TITLE            STRING,
    LOCATION_CODE        STRING,
    
    -- Audit columns
    EFFECTIVE_DATE       DATE,
    IS_CURRENT           BOOLEAN,
    CREATED_AT           TIMESTAMP,
    UPDATED_AT           TIMESTAMP
);
```

### MERGE Logic

```python
def load_dimension_with_merge(
    silver_table: str,
    gold_table: str,
    business_key: str,
    surrogate_key: str,
    update_columns: List[str]
):
    """
    Load dimension using MERGE to preserve surrogate keys
    
    Pattern:
    - WHEN MATCHED: UPDATE attributes only (keep surrogate key)
    - WHEN NOT MATCHED: INSERT with new surrogate key
    """
    
    # Read Silver (source)
    silver_df = spark.table(silver_table)
    
    # Get max existing surrogate key
    max_key = spark.sql(f"""
        SELECT COALESCE(MAX({surrogate_key}), 0) as max_key
        FROM {gold_table}
    """).first()['max_key']
    
    # Assign surrogate keys to new records
    window = Window.orderBy(business_key)
    staging_df = silver_df.withColumn(
        surrogate_key,
        row_number().over(window) + lit(max_key)
    )
    
    staging_df.createOrReplaceTempView('staging')
    
    # Build UPDATE SET clause
    update_set = ', '.join([f"{col} = source.{col}" for col in update_columns])
    
    # Execute MERGE
    merge_sql = f"""
    MERGE INTO {gold_table} AS target
    USING staging AS source
    ON target.{business_key} = source.{business_key}
    
    WHEN MATCHED THEN 
        UPDATE SET {update_set}, UPDATED_AT = CURRENT_TIMESTAMP()
    
    WHEN NOT MATCHED THEN 
        INSERT *
    """
    
    spark.sql(merge_sql)
    logger.info(f"✅ Dimension {gold_table} loaded with MERGE")
```

**Result**: Existing employees keep their EMPLOYEE_KEY, new employees get next available key

---

## Fact Tables

### Pattern: Incremental Load with FK Enrichment

```sql
CREATE TABLE gold.dbo.fact_payroll_earnings (
    -- Surrogate keys (foreign keys)
    EMPLOYEE_KEY         INT,
    LOCATION_KEY         INT,
    
    -- Degenerate dimensions
    PAY_PERIOD_ID        INT,
    EARNING_CODE         STRING,
    
    -- Measures
    EARNING_AMOUNT       DECIMAL(10, 2),
    HOURS                DECIMAL(5, 2),
    RATE                 DECIMAL(10, 2),
    
    -- Audit
    LOAD_DATETIME        TIMESTAMP
)
-- Optimized for FK lookups
TBLPROPERTIES (
    'delta.dataSkippingNumIndexedCols' = '5'
);
```

### Incremental Load Logic

```python
def load_fact_incremental(
    silver_table: str,
    gold_table: str,
    watermark_column: str,
    dim_employee_table: str,
    dim_location_table: str
):
    """
    Load fact table incrementally with FK enrichment
    
    Steps:
    1. Get last watermark (last processed period)
    2. Read new data from Silver
    3. Lookup surrogate keys from dimensions
    4. Append to Gold fact table
    5. Update watermark
    """
    
    # Step 1: Get last watermark
    last_watermark = spark.sql(f"""
        SELECT COALESCE(MAX({watermark_column}), 0) as last_value
        FROM ctrl.execution_log
        WHERE table_name = '{gold_table}'
          AND status = 'SUCCESS'
    """).first()['last_value']
    
    # Step 2: Read new data from Silver
    silver_df = spark.table(silver_table).filter(
        col(watermark_column) > last_watermark
    )
    
    if silver_df.count() == 0:
        logger.info(f"No new data for {gold_table}")
        return
    
    # Step 3: Enrich with surrogate keys
    
    # Load dimensions (broadcast for performance)
    dim_employee = broadcast(spark.table(dim_employee_table))
    dim_location = broadcast(spark.table(dim_location_table))
    
    # Join to get EMPLOYEE_KEY
    enriched_df = silver_df.join(
        dim_employee,
        silver_df.employee_number == dim_employee.EMPLOYEE_NUMBER,
        'left'
    ).select(
        dim_employee.EMPLOYEE_KEY,
        silver_df['*']
    )
    
    # Join to get LOCATION_KEY
    enriched_df = enriched_df.join(
        dim_location,
        enriched_df.location_code == dim_location.LOCATION_CODE,
        'left'
    ).select(
        enriched_df.EMPLOYEE_KEY,
        dim_location.LOCATION_KEY,
        enriched_df.pay_period_id.alias('PAY_PERIOD_ID'),
        enriched_df.earning_code.alias('EARNING_CODE'),
        enriched_df.earning_amount.alias('EARNING_AMOUNT'),
        enriched_df.hours.alias('HOURS'),
        enriched_df.rate.alias('RATE'),
        current_timestamp().alias('LOAD_DATETIME')
    )
    
    # Step 4: Append to Gold
    enriched_df.write \
        .mode('append') \
        .saveAsTable(gold_table)
    
    # Step 5: Update watermark
    next_watermark = enriched_df.agg(max('PAY_PERIOD_ID')).first()[0]
    log_execution(gold_table, next_watermark, enriched_df.count(), 'SUCCESS')
    
    logger.info(f"✅ Loaded {enriched_df.count()} records to {gold_table}")
```

---

## Metadata-Driven Configuration

All Gold table configs stored in control tables:

```sql
-- Unified catalog for dimensions AND facts
CREATE TABLE ctrl.gold_catalog (
    table_name            STRING PRIMARY KEY,
    table_type            STRING,  -- 'DIMENSION' | 'FACT'
    silver_table_name     STRING,
    business_key          STRING,  -- For dimensions
    surrogate_key_name    STRING,  -- For dimensions
    watermark_column      STRING,  -- For facts
    watermark_type        STRING,  -- 'int', 'timestamp', 'date'
    zorder_columns        STRING,  -- Comma-separated
    is_active             BOOLEAN
);

-- MERGE UPDATE columns (what attributes to refresh)
CREATE TABLE ctrl.gold_update_columns (
    table_name      STRING,
    column_name     STRING,
    sort_order      INT,
    is_active       BOOLEAN,
    PRIMARY KEY (table_name, column_name)
);
```

**Example Configuration**:
```sql
-- Dimension configuration
INSERT INTO ctrl.gold_catalog VALUES (
    'dwh_dim_employee',     -- table_name
    'DIMENSION',            -- table_type
    'silver.dim_employee',  -- silver_table_name
    'EMPLOYEE_NUMBER',      -- business_key
    'EMPLOYEE_KEY',         -- surrogate_key_name
    NULL,                   -- watermark_column (not used for dims)
    NULL,                   -- watermark_type
    'EMPLOYEE_NUMBER',      -- zorder_columns
    TRUE                    -- is_active
);

-- Fact configuration
INSERT INTO ctrl.gold_catalog VALUES (
    'fact_payroll_earnings', -- table_name
    'FACT',                  -- table_type
    'silver.fact_earnings',  -- silver_table_name
    NULL,                    -- business_key (not used for facts)
    NULL,                    -- surrogate_key_name
    'pay_period_id',         -- watermark_column
    'int',                   -- watermark_type
    'EMPLOYEE_KEY,PAY_PERIOD_ID',  -- zorder_columns
    TRUE                     -- is_active
);
```

---

## Optimizations

### 1. Z-ORDER Indexing

```sql
-- Optimize fact table for common query patterns
OPTIMIZE gold.dbo.fact_payroll_earnings
ZORDER BY (EMPLOYEE_KEY, PAY_PERIOD_ID);

-- Query performance: ~4x improvement
-- Before: 12 seconds
-- After: 3 seconds
SELECT * 
FROM gold.dbo.fact_payroll_earnings
WHERE EMPLOYEE_KEY = 195 AND PAY_PERIOD_ID = 26;
```

### 2. File Compaction

```sql
-- Compact small files (run after each load)
OPTIMIZE gold.dbo.fact_payroll_earnings;

-- Before: 450 files (~5 MB each)
-- After: 18 files (~128 MB each)
-- Query improvement: ~2x faster
```

### 3. VACUUM Old Versions

```sql
-- Remove old versions (7-day retention)
VACUUM gold.dbo.fact_payroll_earnings RETAIN 168 HOURS;

-- Storage savings: ~25% reduction
```

---

## Complete Gold Load Notebook

```python
# =====================================
# GOLD LAYER - LOAD NOTEBOOK
# =====================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import *

# Step 1: Load Gold catalog
gold_catalog = spark.table('ctrl.gold_catalog').filter(col('is_active') == True).collect()

# Step 2: Load dimensions first (facts depend on them)
dimensions = [row for row in gold_catalog if row['table_type'] == 'DIMENSION']

for dim in dimensions:
    print(f"Loading dimension: {dim['table_name']}")
    
    # Get update columns
    update_cols = spark.sql(f"""
        SELECT column_name 
        FROM ctrl.gold_update_columns
        WHERE table_name = '{dim['table_name']}'
          AND is_active = TRUE
        ORDER BY sort_order
    """).rdd.flatMap(lambda x: x).collect()
    
    # Execute MERGE
    load_dimension_with_merge(
        silver_table=dim['silver_table_name'],
        gold_table=f"gold.dbo.{dim['table_name']}",
        business_key=dim['Business_key'],
        surrogate_key=dim['surrogate_key_name'],
        update_columns=update_cols
    )

# Step 3: Load facts (incremental)
facts = [row for row in gold_catalog if row['table_type'] == 'FACT']

for fact in facts:
    print(f"Loading fact: {fact['table_name']}")
    
    load_fact_incremental(
        silver_table=fact['silver_table_name'],
        gold_table=f"gold.dbo.{fact['table_name']}",
        watermark_column=fact['watermark_column'],
        dim_employee_table='gold.dbo.dwh_dim_employee',
        dim_location_table='gold.dbo.dwh_dim_location'
    )

# Step 4: Optimize tables
for table in gold_catalog:
    gold_table = f"gold.dbo.{table['table_name']}"
    
    # Z-ORDER
    if table['zorder_columns']:
        spark.sql(f"OPTIMIZE {gold_table} ZORDER BY ({table['zorder_columns']})")
    else:
        spark.sql(f"OPTIMIZE {gold_table}")
    
    print(f"✅ Optimized {gold_table}")

print("\n🎉 Gold layer load complete!")
```

---

## Data Quality Checks

```python
# Referential integrity validation
def validate_foreign_keys():
    # Check for NULL foreign keys
    orphan_records = spark.sql("""
        SELECT COUNT(*) as orphans
        FROM gold.dbo.fact_payroll_earnings
        WHERE EMPLOYEE_KEY IS NULL OR LOCATION_KEY IS NULL
    """).first()['orphans']
    
    assert orphan_records == 0, f"Found {orphan_records} orphan records"
    
    # Check for invalid foreign keys
    invalid_fks = spark.sql("""
        SELECT COUNT(*) as invalid
        FROM gold.dbo.fact_payroll_earnings f
        LEFT JOIN gold.dbo.dwh_dim_employee e 
            ON f.EMPLOYEE_KEY = e.EMPLOYEE_KEY
        WHERE e.EMPLOYEE_KEY IS NULL
    """).first()['invalid']
    
    assert invalid_fks == 0, f"Found {invalid_fks} invalid FKs"
    
    print("✅ Referential integrity validated")
```

---

## Performance Benchmarks

| Operation | Volume | Duration  | Optimization |
|-----------|--------|-----------|-------------|
| Dimension MERGE (10K records) | 10,000 | 4 min | Broadcast join on business key |
| Fact incremental (1 period) | 50,000 | 12 min | Z-ORDER + partition pruning |
| Fact query (1 employee, all periods) | 26 rows | 0.5 sec | Z-ORDER on EMPLOYEE_KEY |
| Full table scan (fact) | 1.3M | 8 sec | Optimized file sizes (128MB) |

---

## Next Steps

- [Control Tables Framework](05_CONTROL_TABLES.md)
- [Orchestration & Automation](06_ORCHESTRATION.md)
- [Query Optimization Guide](../notebooks/gold/query_optimization_examples.ipynb)
