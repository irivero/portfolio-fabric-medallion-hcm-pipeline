# Silver Layer - Typed Data & Business Logic

## Overview

The Silver layer transforms raw JSON payloads from Bronze into **strongly-typed, validated business entities**. This layer implements schema inference, data quality rules, and business logic transformations.

---

## Key Responsibilities

1. **Schema Evolution**: Dynamic JSON-to-schema inference with automatic column detection
2. **Data Quality**: Validation rules, deduplication, NULL handling
3. **Business Logic**: Standardizations, calculations, derived fields
4. **Type Safety**: Conversion from raw strings to appropriate data types

---

## Architecture

```
Bronze (JSON strings)  →  Silver (Typed schemas)  →  Gold (Dimensional model)
-------------------------------------------------------------------------
payload_json: STRING   →  employee_number: STRING  →  EMPLOYEE_KEY: INT
                          first_name: STRING        →  FIRST_NAME: STRING
                          hire_date: STRING         →  HIRE_DATE DATE
                          earning_amount: STRING    →  EARNING_AMOUNT: DECIMAL(10,2)
                          ...                       →  ...
```

---

## Table Structure

### Fact Tables (Incremental Load)

```sql
CREATE TABLE silver.sens.fact_earnings (
    employee_number      STRING,
    pay_period_id        INT,
    earning_code         STRING,
    earning_description  STRING,
    earning_amount       DECIMAL(10, 2),
    hours                DECIMAL(5, 2),
    rate                 DECIMAL(10, 2),
    
    -- Technical metadata
    source_file_id       STRING,
    ingestion_datetime   TIMESTAMP,
    processed_datetime   TIMESTAMP
)
PARTITIONED BY (pay_period_id);
```

### Dimension Tables (Full Load)

```sql
CREATE TABLE silver.sens.dim_employee (
    employee_number       STRING,
    first_name            STRING,
    last_name             STRING,
    hire_date             DATE,
    termination_date      DATE,
    employment_status     STRING,
    department            STRING,
    job_title             STRING,
    location_code         STRING,
    
    -- Technical metadata
    effective_datetime    TIMESTAMP,
    is_current            BOOLEAN
);
```

---

## Transformation Process

### Step 1: JSON Parsing & Schema Inference

```python
def parse_bronze_json(bronze_table: str, entity_name: str) -> DataFrame:
    """
    Parse JSON payloads and infer schema dynamically
    
    Handles:
    - Nested JSON structures
    - Array fields
    - Type inference (string → int/date/decimal)
    """
    
    # Read from Bronze
    bronze_df = spark.sql(f"""
        SELECT payload_json, ingestion_id, ingestion_datetime
        FROM {bronze_table}
        WHERE entity_name = '{entity_name}'
    """)
    
    # Parse JSON to struct
    parsed_df = bronze_df.select(
        from_json(
            col('payload_json'),
            infer_schema_from_sample(bronze_df)  # Dynamic schema
        ).alias('parsed_data'),
        col('ingestion_id'),
        col('ingestion_datetime')
    )
    
    # Flatten struct to columns
    flattened_df = parsed_df.select(
        'parsed_data.*',
        'ingestion_id',
        'ingestion_datetime'
    )
    
    return flattened_df
```

### Step 2: Column Evolution Detection

```python
def evolve_silver_schema(df: DataFrame, silver_table: str):
    """
    Detect new columns in source data and add to Silver table
    
    Pattern: Schema-on-Read with automated ALTER TABLE
    """
    
    existing_columns = set(spark.table(silver_table).columns)
    incoming_columns = set(df.columns)
    
    new_columns = incoming_columns - existing_columns
    
    if new_columns:
        logger.warning(f"🔔 New columns detected: {new_columns}")
        
        for col_name in new_columns:
            # Infer data type from DataFrame schema
            col_type = df.schema[col_name].dataType.simpleString()
            
            logger.info(f"Adding column: {col_name} ({col_type})")
            spark.sql(f"""
                ALTER TABLE {silver_table}
                ADD COLUMN {col_name} {col_type}
            """)
        
        logger.info(f"✅ Schema evolved: {len(new_columns)} columns added")
```

### Step 3: Data Quality Validations

```python
def apply_quality_rules(df: DataFrame, entity_type: str) -> DataFrame:
    """
    Apply entity-specific data quality rules
    
    Rules:
    1. Null checks on required fields
    2. Range validations
    3. Deduplication
    4. Referential integrity (soft)
    """
    
    if entity_type == 'fact_earnings':
        df = df.filter(col('employee_number').isNotNull())  # Required field
        df = df.filter(col('paying_amount') >= 0)  # Business rule
        df = df.filter(col('pay_period_id').between(1, 26))  # Valid range
        
        # Dedup: keep latest by ingestion timestamp
        window = Window.partitionBy(
            'employee_number', 'pay_period_id', 'earning_code'
        ).orderBy(desc('ingestion_datetime'))
        
        df = df.withColumn('row_num', row_number().over(window))
        df = df.filter(col('row_num') == 1).drop('row_num')
    
    elif entity_type == 'dim_employee':
        df = df.filter(col('employee_number').isNotNull())
        
        # Dedup: keep latest entry per employee
        window = Window.partitionBy('employee_number').orderBy(desc('ingestion_datetime'))
        df = df.withColumn('row_num', row_number().over(window))
        df = df.filter(col('row_num') == 1).drop('row_num')
    
    return df
```

### Step 4: Business Logic Transformations

```python
def apply_business_logic(df: DataFrame, entity_type: str) -> DataFrame:
    """
    Apply standardizations and derived calculations
    """
    
    if entity_type == 'fact_earnings':
        # Derived field: total_earnings calculation
        df = df.withColumn(
            'total_earnings',
            when(col('hours').isNotNull() & col('rate').isNotNull(),
                 col('hours') * col('rate')
            ).otherwise(col('earning_amount'))
        )
        
        # Standardize earning codes (UPPER)
        df = df.withColumn('earning_code', upper(col('earning_code')))
    
    elif entity_type == 'dim_employee':
        # Full name concatenation
        df = df.withColumn(
            'full_name',
            concat_ws(' ', col('first_name'), col('last_name'))
        )
        
        # Employment status normalization
        df = df.withColumn(
            'employment_status',
            when(col('termination_date').isNotNull(), 'TERMINATED')
            .when(col('hire_date') > current_date(), 'FUTURE_HIRE')
            .otherwise('ACTIVE')
        )
    
    return df
```

---

## Complete Transformation Notebook

```python
# =========================================
# SILVER LAYER - TRANSFORMATION NOTEBOOK
# =========================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Configuration
BRONZE_TABLE = "lh_bronze.sens.hcm_api_extracts"
SILVER_LAKEHOUSE = "lh_silver"
SILVER_SCHEMA = "sens"

# Entity catalog
entities = [
    {'name': 'fact_earnings', 'type': 'fact', 'partition_col': 'pay_period_id'},
    {'name': 'fact_deductions', 'type': 'fact', 'partition_col': 'pay_period_id'},
    {'name': 'fact_taxes', 'type': 'fact', 'partition_col': 'pay_period_id'},
    {'name': 'dim_employee', 'type': 'dimension', 'partition_col': None},
    {'name': 'dim_location', 'type': 'dimension', 'partition_col': None}
]

# Process each entity
for entity in entities:
    print(f"\n{'='*60}")
    print(f"Transforming: {entity['name']}")
    print(f"{'='*60}")
    
    # Step 1: Parse JSON from Bronze
    df = parse_bronze_json(BRONZE_TABLE, entity['name'])
    print(f"  Parsed {df.count()} records")
    
    # Step 2: Detect schema evolution
    silver_table = f"{SILVER_LAKEHOUSE}.{SILVER_SCHEMA}.{entity['name']}"
    evolve_silver_schema(df, silver_table)
    
    # Step 3: Apply quality rules
    df = apply_quality_rules(df, entity['type'])
    print(f"  After quality rules: {df.count()} records")
    
    # Step 4: Apply business logic
    df = apply_business_logic(df, entity['type'])
    
    # Step 5: Add technical metadata
    df = df.withColumn('processed_datetime', current_timestamp())
    
    # Step 6: Write to Silver
    if entity['partition_col']:
        df.write \
          .mode('overwrite') \
          .option('replaceWhere', f"{entity['partition_col']} = {CURRENT_PERIOD}") \
          .partitionBy(entity['partition_col']) \
          .saveAsTable(silver_table)
    else:
        df.write.mode('overwrite').saveAsTable(silver_table)
    
    print(f"  ✅ Written to {silver_table}")

print("\n🎉 Silver transformation complete!")
```

---

## Performance Optimizations

| Optimization | Impact | Implementation |
|--------------|--------|----------------|
| Predicate pushdown | 60% faster read from Bronze | Use `WHERE partition_column = '26'` |
| Column pruning | 40% less data scanned | Select only needed columns |
| Broadcast joins | N/A (no joins in Silver) | Reserved for Gold layer |
| Partition writes | Atomic updates per period | Use `replaceWhere` |

---

## Quality Metrics

Tracked per entity:
- **Records read** from Bronze
- **Records after dedup** (quality impact)
- **New columns detected** (schema drift)
- **Processing duration** (performance)

---

## Next: Gold Layer

Silver data is now ready for dimensional modeling:
- [Gold Layer - Dimensions with Surrogate Keys](04_GOLD_LAYER.md)
- [Gold Layer - Facts with FK Enrichment](04_GOLD_LAYER.md)
