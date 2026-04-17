-- ========================================
-- CONTROL TABLES - Metadata Management
-- ========================================
-- Purpose: Store pipeline configurations and execution metadata
-- Schema: ctrl (control)
-- ========================================

-- ========================================
-- 1. Entity Catalog (API Configuration)
-- ========================================
CREATE TABLE IF NOT EXISTS ctrl.entity_catalog (
    entity_name          STRING,
    entity_description   STRING,
    api_endpoint         STRING,      -- e.g., '/reports/earnings_scorecard'
    load_type            STRING,      -- 'FULL_LOAD' | 'INCREMENTAL'
    start_period         INT,         -- For incremental: starting period
    end_period           INT,         -- For incremental: ending period
    is_active            BOOLEAN,
    created_at           TIMESTAMP,
    updated_at           TIMESTAMP,
    
    CONSTRAINT pk_entity_catalog PRIMARY KEY (entity_name)
)
COMMENT 'API entity configurations for Bronze ingestion';

-- ========================================
-- 2. Ingestion Watermarks
-- ========================================
CREATE TABLE IF NOT EXISTS ctrl.ingestion_watermarks (
    source_system         STRING,
    entity_name           STRING,
    watermark_column      STRING,
    watermark_type        STRING,     -- 'int', 'timestamp', 'date'
    last_processed_value  STRING,     -- Stored as string (universal)
    last_ingestion_time   TIMESTAMP,
    records_ingested      BIGINT,
    is_active             BOOLEAN,
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP,
    
    CONSTRAINT pk_ingestion_watermarks PRIMARY KEY (source_system, entity_name)
)
COMMENT 'Tracks last successfully processed watermark per entity';

-- ========================================
-- 3. Gold Catalog (Unified: Dimensions + Facts)
-- ========================================
CREATE TABLE IF NOT EXISTS ctrl.gold_catalog (
    table_name             STRING,
    table_type             STRING,      -- 'DIMENSION' | 'FACT'
    silver_table_name      STRING,
    silver_lakehouse       STRING,
    silver_schema          STRING,
    description            STRING,
    
    -- Dimension-specific
    business_key           STRING,
    surrogate_key_name     STRING,
    surrogate_start_value  INT,
    
    -- Fact-specific
    watermark_column       STRING,
    watermark_type         STRING,
    
    -- Common
    partition_columns      STRING,      -- Comma-separated
    zorder_columns         STRING,      -- Comma-separated
    is_active              BOOLEAN,
    created_at             TIMESTAMP,
    updated_at             TIMESTAMP,
    
    CONSTRAINT pk_gold_catalog PRIMARY KEY (table_name)
)
COMMENT 'Unified catalog for all Gold layer tables (dimensions + facts)';

-- ========================================
-- 4. Gold Update Columns (MERGE Configuration)
-- ========================================
CREATE TABLE IF NOT EXISTS ctrl.gold_update_columns (
    table_name      STRING,
    column_name     STRING,
    sort_order      INT,
    is_active       BOOLEAN,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    
    CONSTRAINT pk_gold_update_columns PRIMARY KEY (table_name, column_name)
)
COMMENT 'Columns to UPDATE in MERGE statements (dimension loads)';

-- ========================================
-- 5. Gold Z-ORDER Columns
-- ========================================
CREATE TABLE IF NOT EXISTS ctrl.gold_zorder_columns (
    table_name      STRING,
    column_name     STRING,
    sort_order      INT,
    is_active       BOOLEAN,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    
    CONSTRAINT pk_gold_zorder_columns PRIMARY KEY (table_name, column_name)
)
COMMENT 'Columns for Z-ORDER optimization (data skipping)';

-- ========================================
-- 6. Execution Log (Job-Level Tracking)
-- ========================================
CREATE TABLE IF NOT EXISTS ctrl.execution_log (
    execution_id        STRING,
    table_name          STRING,
    execution_start     TIMESTAMP,
    execution_end       TIMESTAMP,
    watermark_column    STRING,
    watermark_value     STRING,
    records_read        BIGINT,
    records_written     BIGINT,
    status              STRING,      -- 'RUNNING', 'SUCCESS', 'FAILED'
    error_message       STRING,
    created_at          TIMESTAMP,
    
    CONSTRAINT pk_execution_log PRIMARY KEY (execution_id)
)
COMMENT 'Execution audit trail for all pipeline jobs';

-- ========================================
-- 7. Orchestrator Execution Log
-- ========================================
CREATE TABLE IF NOT EXISTS ctrl.orchestrator_execution_log (
    orchestrator_run_id  STRING,
    run_start            TIMESTAMP,
    run_end              TIMESTAMP,
    total_jobs           INT,
    successful_jobs      INT,
    failed_jobs          INT,
    status               STRING,      -- 'RUNNING', 'SUCCESS', 'PARTIAL', 'FAILED'
    error_summary        STRING,
    created_at           TIMESTAMP,
    
    CONSTRAINT pk_orchestrator_log PRIMARY KEY (orchestrator_run_id)
)
COMMENT 'High-level pipeline orchestration tracking';

-- ========================================
-- 8. Orchestrator Job Log (Per-Job Details)
-- ========================================
CREATE TABLE IF NOT EXISTS ctrl.orchestrator_job_log (
    orchestrator_run_id  STRING,
    job_name             STRING,
    job_type             STRING,      -- 'BRONZE', 'SILVER', 'GOLD_DIM', 'GOLD_FACT'
    job_start            TIMESTAMP,
    job_end              TIMESTAMP,
    records_processed    BIGINT,
    status               STRING,
    error_message        STRING,
    created_at           TIMESTAMP,
    
    CONSTRAINT pk_orchestrator_job_log PRIMARY KEY (orchestrator_run_id, job_name)
)
COMMENT 'Per-job execution details within orchestration runs';

-- ========================================
-- Table Properties (Optimizations)
-- ========================================

-- Enable auto-optimize for control tables
ALTER TABLE ctrl.entity_catalog SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

ALTER TABLE ctrl.gold_catalog SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

ALTER TABLE ctrl.execution_log SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.logRetentionDuration' = 'interval 90 days',
    'delta.deletedFileRetentionDuration' = 'interval 7 days'
);

-- ========================================
-- Sample Data (for reference)
-- ========================================

-- Example: Entity catalog entry
-- INSERT INTO ctrl.entity_catalog VALUES (
--     'fact_earnings',                    -- entity_name
--     'Employee earnings scorecard',      -- entity_description
--     '/reports/earnings_scorecard',      -- api_endpoint
--     'INCREMENTAL',                      -- load_type
--     1,                                  -- start_period
--     26,                                 -- end_period
--     TRUE,                               -- is_active
--     CURRENT_TIMESTAMP(),
--     CURRENT_TIMESTAMP()
-- );

-- Example: Gold catalog entry (dimension)
-- INSERT INTO ctrl.gold_catalog VALUES (
--     'dwh_dim_employee',                 -- table_name
--     'DIMENSION',                        -- table_type
--     'dim_employee',                     -- silver_table_name
--     'lh_silver',                        -- silver_lakehouse
--     'sens',                             -- silver_schema
--     'Employee dimension',               -- description
--     'EMPLOYEE_NUMBER',                  -- business_key
--     'EMPLOYEE_KEY',                     -- surrogate_key_name
--     1,                                  -- surrogate_start_value
--     NULL,                               -- watermark_column (N/A for dims)
--     NULL,                               -- watermark_type
--     NULL,                               -- partition_columns
--     'EMPLOYEE_NUMBER,DEPARTMENT',       -- zorder_columns
--     TRUE,                               -- is_active
--     CURRENT_TIMESTAMP(),
--     CURRENT_TIMESTAMP()
-- );

-- ========================================
-- ✅ Control Tables Created Successfully
-- ========================================
