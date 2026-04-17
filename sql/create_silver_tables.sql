-- ========================================
-- SILVER LAYER TABLES
-- ========================================
-- Purpose: Typed, validated business entities
-- Pattern: Separate tables per entity type
-- Format: Strongly-typed schemas
-- ========================================

-- ========================================
-- DIMENSION: Employee
-- ========================================
CREATE TABLE IF NOT EXISTS silver.dbo.dim_employee (
    -- Business keys
    employee_number       STRING NOT NULL,
    
    -- Attributes
    first_name            STRING,
    last_name             STRING,
    full_name             STRING,         -- Derived: first + last
    hire_date             DATE,
    termination_date      DATE,
    employment_status     STRING,         -- ACTIVE, TERMINATED, FUTURE_HIRE
    department            STRING,
    job_title             STRING,
    location_code         STRING,
    
    -- Technical metadata
    source_file_id        STRING,
    ingestion_datetime    TIMESTAMP,
    processed_datetime    TIMESTAMP,
    is_current            BOOLEAN DEFAULT TRUE,
    
    CONSTRAINT pk_dim_employee PRIMARY KEY (employee_number)
)
USING DELTA
COMMENT 'Employee dimension - daily full snapshot'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

-- ========================================
-- DIMENSION: Location
-- ========================================
CREATE TABLE IF NOT EXISTS silver.dbo.dim_location (
    -- Business keys
    location_code         STRING NOT NULL,
    
    -- Attributes
    location_name         STRING,
    city                  STRING,
    state                 STRING,
    country               STRING,
    region                STRING,
    is_active             BOOLEAN,
    
    -- Technical metadata
    source_file_id        STRING,
    ingestion_datetime    TIMESTAMP,
    processed_datetime    TIMESTAMP,
    
    CONSTRAINT pk_dim_location PRIMARY KEY (location_code)
)
USING DELTA
COMMENT 'Location/Company dimension'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- ========================================
-- FACT: Earnings
-- ========================================
CREATE TABLE IF NOT EXISTS silver.sens.fact_earnings (
    -- Business keys
    employee_number       STRING NOT NULL,
    pay_period_id         INT NOT NULL,
    earning_code          STRING NOT NULL,
    
    -- Dimensions
    location_code         STRING,
    
    -- Measures
    earning_description   STRING,
    earning_amount        DECIMAL(10, 2),
    hours                 DECIMAL(5, 2),
    rate                  DECIMAL(10, 2),
    total_earnings        DECIMAL(10, 2),    -- Derived: hours * rate
    
    -- Technical metadata
    source_file_id        STRING,
    ingestion_datetime    TIMESTAMP,
    processed_datetime    TIMESTAMP,
    
    CONSTRAINT pk_fact_earnings PRIMARY KEY (employee_number, pay_period_id, earning_code)
)
USING DELTA
PARTITIONED BY (pay_period_id)
COMMENT 'Employee earnings by pay period (incremental)'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

-- ========================================
-- FACT: Deductions
-- ========================================
CREATE TABLE IF NOT EXISTS silver.sens.fact_deductions (
    -- Business keys
    employee_number       STRING NOT NULL,
    pay_period_id         INT NOT NULL,
    deduction_code        STRING NOT NULL,
    
    -- Dimensions
    location_code         STRING,
    
    -- Measures
    deduction_description STRING,
    deduction_amount      DECIMAL(10, 2),
    
    -- Technical metadata
    source_file_id        STRING,
    ingestion_datetime    TIMESTAMP,
    processed_datetime    TIMESTAMP,
    
    CONSTRAINT pk_fact_deductions PRIMARY KEY (employee_number, pay_period_id, deduction_code)
)
USING DELTA
PARTITIONED BY (pay_period_id)
COMMENT 'Employee deductions by pay period (incremental)'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- ========================================
-- FACT: Taxes
-- ========================================
CREATE TABLE IF NOT EXISTS silver.sens.fact_taxes (
    -- Business keys
    employee_number       STRING NOT NULL,
    pay_period_id         INT NOT NULL,
    tax_code              STRING NOT NULL,
    
    -- Dimensions
    location_code         STRING,
    
    -- Measures
    tax_description       STRING,
    tax_amount            DECIMAL(10, 2),
    tax_type              STRING,          -- FEDERAL, STATE, LOCAL
    
    -- Technical metadata
    source_file_id        STRING,
    ingestion_datetime    TIMESTAMP,
    processed_datetime    TIMESTAMP,
    
    CONSTRAINT pk_fact_taxes PRIMARY KEY (employee_number, pay_period_id, tax_code)
)
USING DELTA
PARTITIONED BY (pay_period_id)
COMMENT 'Tax withholdings by pay period (incremental)'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- ========================================
-- Data Quality Constraints (Check Constraints)
-- ========================================

-- Earnings: non-negative amounts
ALTER TABLE silver.sens.fact_earnings 
ADD CONSTRAINT chk_earnings_amount_positive 
CHECK (earning_amount >= 0);

-- Deductions: non-negative amounts
ALTER TABLE silver.sens.fact_deductions 
ADD CONSTRAINT chk_deductions_amount_positive 
CHECK (deduction_amount >= 0);

-- Taxes: non-negative amounts
ALTER TABLE silver.sens.fact_taxes 
ADD CONSTRAINT chk_taxes_amount_positive 
CHECK (tax_amount >= 0);

-- Pay periods: valid range (1-26)
ALTER TABLE silver.sens.fact_earnings 
ADD CONSTRAINT chk_earnings_period_range 
CHECK (pay_period_id BETWEEN 1 AND 26);

-- ========================================
-- Optimization Commands
-- ========================================

-- Optimize dimension tables
-- OPTIMIZE silver.sens.dim_employee;
-- OPTIMIZE silver.sens.dim_location;

-- Optimize fact tables (specific partition)
-- OPTIMIZE silver.sens.fact_earnings WHERE pay_period_id = 26;
-- OPTIMIZE silver.sens.fact_deductions WHERE pay_period_id = 26;
-- OPTIMIZE silver.sens.fact_taxes WHERE pay_period_id = 26;

-- VACUUM (cleanup old versions)
-- VACUUM silver.sens.fact_earnings RETAIN 168 HOURS;
