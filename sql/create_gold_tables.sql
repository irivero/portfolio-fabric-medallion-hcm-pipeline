-- ========================================
-- GOLD LAYER TABLES - Star Schema
-- ========================================
-- Purpose: Analytics-ready dimensional model
-- Pattern: Star schema (dimensions + facts)
-- Format: Optimized for query performance
-- ========================================

-- ========================================
-- DIMENSION: Employee (with surrogate key)
-- ========================================
CREATE TABLE IF NOT EXISTS gold.dbo.dwh_dim_employee (
    -- Surrogate key (stable across refreshes)
    EMPLOYEE_KEY         INT NOT NULL,
    
    -- Business key (from source system)
    EMPLOYEE_NUMBER      STRING NOT NULL,
    
    -- Attributes
    FIRST_NAME           STRING,
    LAST_NAME            STRING,
    FULL_NAME            STRING,
    HIRE_DATE            DATE,
    TERMINATION_DATE     DATE,
    EMPLOYMENT_STATUS    STRING,
    DEPARTMENT           STRING,
    JOB_TITLE            STRING,
    LOCATION_CODE        STRING,
    EMAIL                STRING,
    PHONE                STRING,
    
    -- Audit columns
    EFFECTIVE_DATE       DATE,
    IS_CURRENT           BOOLEAN DEFAULT TRUE,
    CREATED_AT           TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT           TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    
    CONSTRAINT pk_dwh_dim_employee PRIMARY KEY (EMPLOYEE_KEY),
    CONSTRAINT uk_employee_number UNIQUE (EMPLOYEE_NUMBER)
)
USING DELTA
COMMENT 'Employee dimension with stable surrogate keys (SCD Type 1)'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

-- ========================================
-- DIMENSION: Location (with surrogate key)
-- ========================================
CREATE TABLE IF NOT EXISTS gold.dbo.dwh_dim_location (
    -- Surrogate key
    LOCATION_KEY         INT NOT NULL,
    
    -- Business key
    LOCATION_CODE        STRING NOT NULL,
    
    -- Attributes
    LOCATION_NAME        STRING,
    CITY                 STRING,
    STATE                STRING,
    COUNTRY              STRING,
    REGION               STRING,
    IS_ACTIVE            BOOLEAN,
    
    -- Audit columns
    CREATED_AT           TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT           TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    
    CONSTRAINT pk_dwh_dim_location PRIMARY KEY (LOCATION_KEY),
    CONSTRAINT uk_location_code UNIQUE (LOCATION_CODE)
)
USING DELTA
COMMENT 'Location dimension with stable surrogate keys'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ========================================
-- FACT: Payroll Earnings
-- ========================================
CREATE TABLE IF NOT EXISTS gold.dbo.fact_payroll_earnings (
    -- Foreign keys (references to dimensions)
    EMPLOYEE_KEY         INT,
    LOCATION_KEY         INT,
    
    -- Degenerate dimensions (no separate dimension table)
    PAY_PERIOD_ID        INT NOT NULL,
    EARNING_CODE         STRING NOT NULL,
    EARNING_DESCRIPTION  STRING,
    
    -- Measures
    EARNING_AMOUNT       DECIMAL(10, 2),
    HOURS                DECIMAL(5, 2),
    RATE                 DECIMAL(10, 2),
    TOTAL_EARNINGS       DECIMAL(10, 2),
    
    -- Audit
    LOAD_DATETIME        TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_PERIOD_ID     INT,           -- Original period from source
    
    CONSTRAINT pk_fact_earnings PRIMARY KEY (EMPLOYEE_KEY, PAY_PERIOD_ID, EARNING_CODE)
)
USING DELTA
COMMENT 'Employee earnings fact table - grain: one row per employee-period-earning_code'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.dataSkippingNumIndexedCols' = '5'
);

-- ========================================
-- FACT: Payroll Deductions
-- ========================================
CREATE TABLE IF NOT EXISTS gold.dbo.fact_payroll_deductions (
    -- Foreign keys
    EMPLOYEE_KEY         INT,
    LOCATION_KEY         INT,
    
    -- Degenerate dimensions
    PAY_PERIOD_ID        INT NOT NULL,
    DEDUCTION_CODE       STRING NOT NULL,
    DEDUCTION_DESCRIPTION STRING,
    
    -- Measures
    DEDUCTION_AMOUNT     DECIMAL(10, 2),
    
    -- Audit
    LOAD_DATETIME        TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    
    CONSTRAINT pk_fact_deductions PRIMARY KEY (EMPLOYEE_KEY, PAY_PERIOD_ID, DEDUCTION_CODE)
)
USING DELTA
COMMENT 'Employee deductions fact table - grain: one row per employee-period-deduction_code'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.dataSkippingNumIndexedCols' = '5'
);

-- ========================================
-- FACT: Payroll Taxes
-- ========================================
CREATE TABLE IF NOT EXISTS gold.dbo.fact_payroll_taxes (
    -- Foreign keys
    EMPLOYEE_KEY         INT,
    LOCATION_KEY         INT,
    
    -- Degenerate dimensions
    PAY_PERIOD_ID        INT NOT NULL,
    TAX_CODE             STRING NOT NULL,
    TAX_DESCRIPTION      STRING,
    TAX_TYPE             STRING,        -- FEDERAL, STATE, LOCAL
    
    -- Measures
    TAX_AMOUNT           DECIMAL(10, 2),
    
    -- Audit
    LOAD_DATETIME        TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    
    CONSTRAINT pk_fact_taxes PRIMARY KEY (EMPLOYEE_KEY, PAY_PERIOD_ID, TAX_CODE)
)
USING DELTA
COMMENT 'Tax withholdings fact table - grain: one row per employee-period-tax_code'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.dataSkippingNumIndexedCols' = '5'
);

-- ========================================
-- Referential Integrity (soft constraints)
-- ========================================
-- Note: Delta Lake doesn't enforce FKs, but we document them for clarity

-- ALTER TABLE gold.dbo.fact_payroll_earnings
-- ADD CONSTRAINT fk_earnings_employee 
-- FOREIGN KEY (EMPLOYEE_KEY) REFERENCES gold.dbo.dwh_dim_employee(EMPLOYEE_KEY);

-- ALTER TABLE gold.dbo.fact_payroll_earnings
-- ADD CONSTRAINT fk_earnings_location 
-- FOREIGN KEY (LOCATION_KEY) REFERENCES gold.dbo.dwh_dim_location(LOCATION_KEY);

-- (Repeat for deductions and taxes facts)

-- ========================================
-- Z-ORDER Optimization (recommended)
-- ========================================
-- Run after initial load and periodically

-- Dimensions: Z-ORDER on business keys
OPTIMIZE gold.dbo.dwh_dim_employee
ZORDER BY (EMPLOYEE_NUMBER, DEPARTMENT);

OPTIMIZE gold.dbo.dwh_dim_location
ZORDER BY (LOCATION_CODE, COUNTRY);

-- Facts: Z-ORDER on foreign keys + period
OPTIMIZE gold.dbo.fact_payroll_earnings
ZORDER BY (EMPLOYEE_KEY, PAY_PERIOD_ID);

OPTIMIZE gold.dbo.fact_payroll_deductions
ZORDER BY (EMPLOYEE_KEY, PAY_PERIOD_ID);

OPTIMIZE gold.dbo.fact_payroll_taxes
ZORDER BY (EMPLOYEE_KEY, PAY_PERIOD_ID);

-- ========================================
-- Sample Analytical Queries
-- ========================================

-- Example 1: Total earnings per employee for current period
-- SELECT 
--     e.EMPLOYEE_NUMBER,
--     e.FULL_NAME,
--     e.DEPARTMENT,
--     SUM(f.EARNING_AMOUNT) as total_earnings
-- FROM gold.dbo.fact_payroll_earnings f
-- JOIN gold.dbo.dwh_dim_employee e ON f.EMPLOYEE_KEY = e.EMPLOYEE_KEY
-- WHERE f.PAY_PERIOD_ID = 26
-- GROUP BY e.EMPLOYEE_NUMBER, e.FULL_NAME, e.DEPARTMENT
-- ORDER BY total_earnings DESC;

-- Example 2: Year-to-date earnings by department
-- SELECT 
--     e.DEPARTMENT,
--     COUNT(DISTINCT e.EMPLOYEE_KEY) as employee_count,
--     SUM(f.EARNING_AMOUNT) as ytd_earnings,
--     AVG(f.EARNING_AMOUNT) as avg_earnings_per_employee
-- FROM gold.dbo.fact_payroll_earnings f
-- JOIN gold.dbo.dwh_dim_employee e ON f.EMPLOYEE_KEY = e.EMPLOYEE_KEY
-- WHERE f.PAY_PERIOD_ID BETWEEN 1 AND 26
-- GROUP BY e.DEPARTMENT
-- ORDER BY ytd_earnings DESC;

-- Example 3: Employee payroll summary (earnings - deductions - taxes)
-- WITH employee_totals AS (
--     SELECT 
--         EMPLOYEE_KEY,
--         PAY_PERIOD_ID,
--         SUM(EARNING_AMOUNT) as gross_earnings
--     FROM gold.dbo.fact_payroll_earnings
--     GROUP BY EMPLOYEE_KEY, PAY_PERIOD_ID
-- ),
-- deduction_totals AS (
--     SELECT 
--         EMPLOYEE_KEY,
--         PAY_PERIOD_ID,
--         SUM(DEDUCTION_AMOUNT) as total_deductions
--     FROM gold.dbo.fact_payroll_deductions
--     GROUP BY EMPLOYEE_KEY, PAY_PERIOD_ID
-- ),
-- tax_totals AS (
--     SELECT 
--         EMPLOYEE_KEY,
--         PAY_PERIOD_ID,
--         SUM(TAX_AMOUNT) as total_taxes
--     FROM gold.dbo.fact_payroll_taxes
--     GROUP BY EMPLOYEE_KEY, PAY_PERIOD_ID
-- )
-- SELECT 
--     e.EMPLOYEE_NUMBER,
--     e.FULL_NAME,
--     et.PAY_PERIOD_ID,
--     et.gross_earnings,
--     COALESCE(dt.total_deductions, 0) as deductions,
--     COALESCE(tt.total_taxes, 0) as taxes,
--     et.gross_earnings - COALESCE(dt.total_deductions, 0) - COALESCE(tt.total_taxes, 0) as net_pay
-- FROM employee_totals et
-- JOIN gold.dbo.dwh_dim_employee e ON et.EMPLOYEE_KEY = e.EMPLOYEE_KEY
-- LEFT JOIN deduction_totals dt ON et.EMPLOYEE_KEY = dt.EMPLOYEE_KEY AND et.PAY_PERIOD_ID = dt.PAY_PERIOD_ID
-- LEFT JOIN tax_totals tt ON et.EMPLOYEE_KEY = tt.EMPLOYEE_KEY AND et.PAY_PERIOD_ID = tt.PAY_PERIOD_ID
-- WHERE et.PAY_PERIOD_ID = 26
-- ORDER BY e.EMPLOYEE_NUMBER;

-- ========================================
-- Data Quality Validation Queries
-- ========================================

-- Check for NULL foreign keys (orphan records)
-- SELECT COUNT(*) as orphan_records
-- FROM gold.dbo.fact_payroll_earnings
-- WHERE EMPLOYEE_KEY IS NULL OR LOCATION_KEY IS NULL;

-- Check for invalid foreign keys
-- SELECT COUNT(*) as invalid_fk_records
-- FROM gold.dbo.fact_payroll_earnings f
-- LEFT JOIN gold.dbo.dwh_dim_employee e ON f.EMPLOYEE_KEY = e.EMPLOYEE_KEY
-- WHERE e.EMPLOYEE_KEY IS NULL;

-- Verify surrogate key uniqueness
-- SELECT EMPLOYEE_KEY, COUNT(*) as dup_count
-- FROM gold.dbo.dwh_dim_employee
-- GROUP BY EMPLOYEE_KEY
-- HAVING COUNT(*) > 1;

-- ========================================
-- Maintenance Commands
-- ========================================

-- File compaction (run weekly)
-- OPTIMIZE gold.dbo.fact_payroll_earnings;
-- OPTIMIZE gold.dbo.fact_payroll_deductions;
-- OPTIMIZE gold.dbo.fact_payroll_taxes;

-- Remove old versions (7-day retention)
-- VACUUM gold.dbo.fact_payroll_earnings RETAIN 168 HOURS;
-- VACUUM gold.dbo.fact_payroll_deductions RETAIN 168 HOURS;
-- VACUUM gold.dbo.fact_payroll_taxes RETAIN 168 HOURS;

-- Statistics refresh (for query optimization)
-- ANALYZE TABLE gold.dbo.fact_payroll_earnings COMPUTE STATISTICS;
-- ANALYZE TABLE gold.dbo.dwh_dim_employee COMPUTE STATISTICS;

-- ========================================
-- Performance Monitoring
-- ========================================

-- Check table sizes
-- DESCRIBE DETAIL gold.dbo.fact_payroll_earnings;
-- DESCRIBE DETAIL gold.dbo.dwh_dim_employee;

-- Check file statistics
-- SELECT 
--     numFiles,
--     sizeInBytes / 1024 / 1024 as size_mb,
--     sizeInBytes / numFiles / 1024 / 1024 as avg_file_size_mb
-- FROM (DESCRIBE DETAIL gold.dbo.fact_payroll_earnings);
