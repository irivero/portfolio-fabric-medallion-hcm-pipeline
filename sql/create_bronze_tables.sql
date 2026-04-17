-- ========================================
-- BRONZE LAYER TABLES
-- ========================================
-- Purpose: Raw data landing zone (schema-agnostic)
-- Pattern: Single unified table for all entities
-- Format: JSON strings (no schema enforcement)
-- ========================================

-- ========================================
-- Bronze: API Extracts (Unified Table)
-- ========================================
CREATE TABLE IF NOT EXISTS bronze.sens.hcm_api_extracts (
    -- Identifiers
    ingestion_id           STRING NOT NULL,
    entity_name            STRING NOT NULL,
    partition_column       STRING NOT NULL,    -- 'FULL_LOAD' | '01'-'26'
    
    -- Raw payload
    payload_json           STRING,             -- Raw API response (JSON string)
    record_count           INT,                -- Number of records in payload
    
    -- API metadata
    api_endpoint           STRING,
    api_http_status        INT,
    api_call_timestamp     TIMESTAMP,
    
    -- Technical metadata
    ingestion_datetime     TIMESTAMP NOT NULL,
    source_system          STRING,             -- Always 'hcm_api'
    pipeline_run_id        STRING,
    
    -- Audit
    created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
PARTITIONED BY (partition_column, entity_name)
COMMENT 'Raw API data landing zone - schema-agnostic JSON storage'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.logRetentionDuration' = 'interval 90 days',
    'delta.deletedFileRetentionDuration' = 'interval 7 days'
);

-- ========================================
-- Partition Strategy
-- ========================================
/*
Dual Partition Pattern:

partition_column = 'FULL_LOAD'
  - Used for dimension tables (daily snapshots)
  - Each write REPLACES entire partition
  - Auto-cleanup (no historical accumulation)
  
partition_column = '01' to '26'
  - Used for fact tables (incremental by period)
  - Each period is separate partition
  - Historical retention (all periods preserved)

Benefits:
  ✅ Single unified table (no *_full vs *_incremental split)
  ✅ Self-documenting (partition value = load type)
  ✅ Automatic cleanup for full-load entities
  ✅ Historical preservation for incremental entities
*/

-- ========================================
-- Sample Query Patterns
-- ========================================

-- Example 1: Get latest full-load dimension
-- SELECT payload_json, ingestion_datetime
-- FROM bronze.sens.hcm_api_extracts
-- WHERE partition_column = 'FULL_LOAD'
--   AND entity_name = 'dim_employee'
-- ORDER BY ingestion_datetime DESC
-- LIMIT 1;

-- Example 2: Get specific period for fact table
-- SELECT payload_json
-- FROM bronze.sens.hcm_api_extracts
-- WHERE partition_column = '26'
--   AND entity_name = 'fact_earnings';

-- Example 3: Get all historical periods for analysis
-- SELECT partition_column as period_id, 
--        COUNT(*) as ingestion_runs,
--        MAX(ingestion_datetime) as latest_ingestion
-- FROM bronze.sens.hcm_api_extracts
-- WHERE entity_name = 'fact_earnings'
--   AND partition_column != 'FULL_LOAD'
-- GROUP BY partition_column
-- ORDER BY partition_column;

-- ========================================
-- Optimization Commands
-- ========================================

-- Compact small files (run weekly)
-- OPTIMIZE bronze.sens.hcm_api_extracts;

-- Compact specific partition
-- OPTIMIZE bronze.sens.hcm_api_extracts
-- WHERE partition_column = '26';

-- Remove old versions (7-day retention)
-- VACUUM bronze.sens.hcm_api_extracts RETAIN 168 HOURS;

-- Z-ORDER for faster queries (optional)
-- OPTIMIZE bronze.sens.hcm_api_extracts
-- ZORDER BY (entity_name, ingestion_datetime);

-- ========================================
-- Monitoring Queries
-- ========================================

-- Check partition sizes
-- SELECT partition_column,
--        entity_name,
--        COUNT(*) as file_count,
--        SUM(record_count) as total_records
-- FROM bronze.sens.hcm_api_extracts
-- GROUP BY partition_column, entity_name
-- ORDER BY partition_column, entity_name;

-- Identify oldest data (for cleanup)
-- SELECT entity_name,
--        partition_column,
--        MIN(ingestion_datetime) as oldest_data,
--        DATEDIFF(DAY, MIN(ingestion_datetime), CURRENT_DATE()) as age_days
-- FROM bronze.sens.hcm_api_extracts
-- GROUP BY entity_name, partition_column
-- HAVING age_days > 90
-- ORDER BY age_days DESC;
