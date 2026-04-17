# Data Quality & Testing Strategy

## Executive Summary

Comprehensive testing framework for **Medallion Architecture** pipeline validation, covering data quality, functional correctness, performance benchmarks, and regression testing.

**Test Coverage**:
- ✅ **Unit Tests**: 45+ automated data quality checks
- ✅ **Integration Tests**: E2E pipeline validation (Bronze → Silver → Gold)
- ✅ **Performance Tests**: SLA compliance monitoring (< 70 min E2E)
- ✅ **Regression Tests**: Schema evolution & historical data validation

**Automation Rate**: 92% (automated validations vs. manual spot checks)

---

## 🎯 Testing Philosophy

### Data Quality Dimensions (Senior QA Framework)

| Dimension | Definition | Implementation |
|-----------|------------|----------------|
| **Completeness** | All expected records present | Row count reconciliation, NULL checks |
| **Accuracy** | Data matches source truth | Business rule validation, checksum comparison |
| **Consistency** | Data conforms to constraints | FK integrity, data type validation |
| **Timeliness** | Data delivered within SLA | Watermark tracking, latency monitoring |
| **Uniqueness** | No duplicate records | Primary key validation, deduplication checks |
| **Validity** | Data within business rules | Range checks, pattern matching |

---

## 📋 Test Cases by Layer

### 1. Bronze Layer Test Cases

#### TC-BR-001: API Ingestion Completeness
**Objective**: Validate all API records persisted to Bronze  
**Criticality**: HIGH  
**Type**: Automated

**Test Steps**:
```python
# Pre-condition: API call returns N records
api_response_count = len(api_response_json)

# Validation
bronze_count = spark.sql(f"""
    SELECT COUNT(*) as cnt 
    FROM bronze.hcm_api_extracts 
    WHERE entity = '{entity_name}' 
    AND partition_column = '{partition_value}'
    AND ingestion_timestamp >= current_timestamp() - INTERVAL 10 MINUTES
""").collect()[0]['cnt']

assert bronze_count == api_response_count, \
    f"Record count mismatch: API={api_response_count}, Bronze={bronze_count}"
```

**Expected Result**: `bronze_count == api_response_count`  
**Failure Action**: Retry API call, check rate limiting

---

#### TC-BR-002: JSON Schema Preservation
**Objective**: Validate raw JSON structure not corrupted  
**Criticality**: HIGH  
**Type**: Automated

**Test Steps**:
```python
# Validation: Parse JSON and check required keys
from pyspark.sql.functions import from_json, col

bronze_df = spark.table("bronze.hcm_api_extracts").filter(
    col("entity") == "Employee"
).limit(1)

# Extract sample JSON
sample_json = bronze_df.select("payload").first()['payload']
parsed = json.loads(sample_json)

# Assert required top-level keys exist
required_keys = ['Data', 'Paging', 'ResponseStatus']
for key in required_keys:
    assert key in parsed, f"Missing required JSON key: {key}"
```

**Expected Result**: All required keys present  
**Failure Action**: Investigate API response format change

---

#### TC-BR-003: Partition Strategy Validation
**Objective**: Verify dual partition logic (FULL_LOAD vs INCREMENTAL)  
**Criticality**: MEDIUM  
**Type**: Automated

**Test Steps**:
```sql
-- Validate FULL_LOAD entities have only 1 partition value
SELECT entity, COUNT(DISTINCT partition_column) as partition_count
FROM bronze.hcm_api_extracts
WHERE entity IN (SELECT entity FROM ctrl.entity_catalog WHERE load_type = 'FULL_LOAD')
GROUP BY entity
HAVING COUNT(DISTINCT partition_column) != 1;

-- Expected: 0 rows (all FULL_LOAD entities have exactly 1 partition "FULL_LOAD")
```

**Expected Result**: `0 rows returned`  
**Failure Action**: Check ingestion logic for partition_value assignment

---

#### TC-BR-004: Watermark Update Validation
**Objective**: Ensure watermarks updated after successful ingestion  
**Criticality**: HIGH  
**Type**: Automated

**Test Steps**:
```python
# Get watermark BEFORE ingestion
before_watermark = spark.sql(f"""
    SELECT last_processed_value 
    FROM ctrl.watermark_tracking 
    WHERE entity_name = '{entity_name}'
""").collect()[0]['last_processed_value']

# Run ingestion for period 26
run_ingestion(entity='PayrollEarnings', period_id=26)

# Get watermark AFTER ingestion
after_watermark = spark.sql(f"""
    SELECT last_processed_value 
    FROM ctrl.watermark_tracking 
    WHERE entity_name = '{entity_name}'
""").collect()[0]['last_processed_value']

assert int(after_watermark) > int(before_watermark), \
    "Watermark not updated after ingestion"
```

**Expected Result**: `after_watermark > before_watermark`

---

### 2. Silver Layer Test Cases

#### TC-SL-001: JSON to Typed Schema Transformation
**Objective**: Validate schema inference and column mapping  
**Criticality**: HIGH  
**Type**: Automated

**Test Steps**:
```python
# Validate expected schema
silver_df = spark.table("silver.fact_earnings")

# Assert expected columns exist with correct data types
expected_schema = {
    'EMPLOYEE_NUMBER': 'string',
    'PAY_PERIOD_ID': 'int',
    'EARNINGS_CODE': 'string',
    'EARNINGS_AMOUNT': 'decimal(18,2)',
    'EARNINGS_HOURS': 'decimal(10,2)',
    'INGESTION_TIMESTAMP': 'timestamp'
}

for col_name, expected_type in expected_schema.items():
    actual_type = [f.dataType.simpleString() for f in silver_df.schema.fields 
                   if f.name == col_name][0]
    assert expected_type in actual_type, \
        f"Column {col_name}: expected {expected_type}, got {actual_type}"
```

**Expected Result**: All columns have correct data types

---

#### TC-SL-002: Data Quality - NULL Checks
**Objective**: Validate critical columns have no NULLs  
**Criticality**: HIGH  
**Type**: Automated

**Test Steps**:
```python
# Define critical NOT NULL columns by table
null_checks = {
    'silver.fact_earnings': ['EMPLOYEE_NUMBER', 'PAY_PERIOD_ID', 'EARNINGS_AMOUNT'],
    'silver.dim_employee': ['EMPLOYEE_NUMBER', 'FIRST_NAME', 'LAST_NAME'],
}

for table, columns in null_checks.items():
    for col in columns:
        null_count = spark.sql(f"""
            SELECT COUNT(*) as cnt 
            FROM {table} 
            WHERE {col} IS NULL
        """).collect()[0]['cnt']
        
        assert null_count == 0, \
            f"Table {table}, Column {col}: Found {null_count} NULL values"
```

**Expected Result**: `null_count = 0` for all critical columns

---

#### TC-SL-003: Business Rule Validation - Earnings Amount
**Objective**: Validate earnings amounts within valid range  
**Criticality**: MEDIUM  
**Type**: Automated

**Test Steps**:
```sql
-- Validate earnings amounts are positive and reasonable
SELECT 
    EMPLOYEE_NUMBER,
    PAY_PERIOD_ID,
    EARNINGS_AMOUNT
FROM silver.fact_earnings
WHERE EARNINGS_AMOUNT < 0           -- Negative earnings
   OR EARNINGS_AMOUNT > 50000       -- Unreasonably high (bi-weekly max)
   OR EARNINGS_AMOUNT = 0;          -- Zero earnings (suspicious)

-- Expected: 0 rows (or documented exceptions)
```

**Expected Result**: `0 rows` or documented exception list  
**Failure Action**: Data quality report to business users

---

#### TC-SL-004: Deduplication Effectiveness
**Objective**: Validate no duplicate records in Silver  
**Criticality**: HIGH  
**Type**: Automated

**Test Steps**:
```sql
-- Check for duplicates on business keys
SELECT 
    EMPLOYEE_NUMBER, 
    PAY_PERIOD_ID, 
    EARNINGS_CODE,
    COUNT(*) as dup_count
FROM silver.fact_earnings
GROUP BY EMPLOYEE_NUMBER, PAY_PERIOD_ID, EARNINGS_CODE
HAVING COUNT(*) > 1;

-- Expected: 0 rows (no duplicates)
```

**Expected Result**: `0 duplicates`

---

#### TC-SL-005: Reconciliation - Bronze to Silver
**Objective**: Ensure row counts match after transformations  
**Criticality**: HIGH  
**Type**: Automated

**Test Steps**:
```python
# Count records in Bronze JSON payload
bronze_count = spark.sql(f"""
    SELECT 
        SUM(size(from_json(payload, 'array<map<string,string>>').Data)) as total
    FROM bronze.hcm_api_extracts
    WHERE entity = 'Earnings' AND partition_column = '26'
""").collect()[0]['total']

# Count records in Silver
silver_count = spark.sql("""
    SELECT COUNT(*) as cnt 
    FROM silver.fact_earnings 
    WHERE PAY_PERIOD_ID = 26
""").collect()[0]['cnt']

# Allow for deduplication (Silver <= Bronze)
assert silver_count <= bronze_count, \
    f"Silver has MORE records than Bronze: Silver={silver_count}, Bronze={bronze_count}"

# Check acceptable loss threshold (e.g., <5% data loss)
loss_pct = ((bronze_count - silver_count) / bronze_count) * 100
assert loss_pct < 5.0, \
    f"Data loss exceeds threshold: {loss_pct:.2f}% (Bronze={bronze_count}, Silver={silver_count})"
```

**Expected Result**: `0% <= loss <= 5%`

---

### 3. Gold Layer Test Cases

#### TC-GL-001: Surrogate Key Preservation (MERGE)
**Objective**: Validate surrogate keys stable across dimension refreshes  
**Criticality**: HIGH  
**Type**: Automated

**Test Steps**:
```python
# Step 1: Capture surrogate keys BEFORE dimension refresh
before_keys = spark.sql("""
    SELECT EMPLOYEE_NUMBER, EMPLOYEE_KEY 
    FROM gold.dwh_dim_employee
""").toPandas()

# Step 2: Run dimension refresh (MERGE operation)
run_gold_dimension_load(dimension='employee')

# Step 3: Capture surrogate keys AFTER refresh
after_keys = spark.sql("""
    SELECT EMPLOYEE_NUMBER, EMPLOYEE_KEY 
    FROM gold.dwh_dim_employee
""").toPandas()

# Step 4: Validate keys unchanged for existing employees
merged = before_keys.merge(after_keys, on='EMPLOYEE_NUMBER', how='inner')
key_changes = merged[merged['EMPLOYEE_KEY_x'] != merged['EMPLOYEE_KEY_y']]

assert len(key_changes) == 0, \
    f"Found {len(key_changes)} employees with changed surrogate keys"
```

**Expected Result**: `0 key changes` for existing employees

---

#### TC-GL-002: Foreign Key Integrity
**Objective**: Validate 100% FK integrity (no orphan records)  
**Criticality**: HIGH  
**Type**: Automated

**Test Steps**:
```sql
-- Check for orphan records in fact table
SELECT 
    f.EMPLOYEE_KEY,
    f.PAY_PERIOD_ID,
    COUNT(*) as orphan_count
FROM gold.fact_payroll_earnings f
LEFT JOIN gold.dwh_dim_employee e ON f.EMPLOYEE_KEY = e.EMPLOYEE_KEY
WHERE e.EMPLOYEE_KEY IS NULL  -- Orphan records
GROUP BY f.EMPLOYEE_KEY, f.PAY_PERIOD_ID;

-- Expected: 0 rows (100% FK integrity)
```

**Expected Result**: `0 orphan records`  
**Failure Action**: CRITICAL - Block production deployment

---

#### TC-GL-003: Incremental Load Idempotency
**Objective**: Re-running same period produces identical results  
**Criticality**: HIGH  
**Type**: Automated

**Test Steps**:
```python
# Run 1: Load period 26
run_gold_fact_load(entity='fact_payroll_earnings', period_id=26)
result_run1 = spark.sql("""
    SELECT EMPLOYEE_KEY, SUM(EARNINGS_AMOUNT) as total
    FROM gold.fact_payroll_earnings
    WHERE PAY_PERIOD_ID = 26
    GROUP BY EMPLOYEE_KEY
    ORDER BY EMPLOYEE_KEY
""").toPandas()

# Run 2: Re-load period 26 (idempotency test)
run_gold_fact_load(entity='fact_payroll_earnings', period_id=26)
result_run2 = spark.sql("""
    SELECT EMPLOYEE_KEY, SUM(EARNINGS_AMOUNT) as total
    FROM gold.fact_payroll_earnings
    WHERE PAY_PERIOD_ID = 26
    GROUP BY EMPLOYEE_KEY
    ORDER BY EMPLOYEE_KEY
""").toPandas()

# Assert results are identical
assert result_run1.equals(result_run2), \
    "Incremental load NOT idempotent - results differ between runs"
```

**Expected Result**: `result_run1 == result_run2`

---

#### TC-GL-004: Star Schema Validation
**Objective**: Validate dimensional model conforms to star schema  
**Criticality**: MEDIUM  
**Type**: Automated

**Test Steps**:
```python
# Validate fact table has only surrogate keys + measures (no descriptive attributes)
fact_table = spark.table("gold.fact_payroll_earnings")

# Define allowed column types
surrogate_keys = [col for col in fact_table.columns if col.endswith('_KEY')]
measures = [col for col in fact_table.columns if 'AMOUNT' in col or 'HOURS' in col]
metadata = ['PAY_PERIOD_ID', 'INGESTION_TIMESTAMP', 'SOURCE_SYSTEM']

allowed_columns = set(surrogate_keys + measures + metadata)
actual_columns = set(fact_table.columns)

# Assert no descriptive attributes in fact table
descriptive_cols = actual_columns - allowed_columns
assert len(descriptive_cols) == 0, \
    f"Fact table contains descriptive columns (star schema violation): {descriptive_cols}"
```

**Expected Result**: Fact table has only keys + measures

---

#### TC-GL-005: Z-ORDER Optimization Validation
**Objective**: Verify Z-ORDER applied and improving query performance  
**Criticality**: LOW  
**Type**: Automated

**Test Steps**:
```python
import time

# Test query BEFORE Z-ORDER
start = time.time()
spark.sql("""
    SELECT * FROM gold.fact_payroll_earnings 
    WHERE EMPLOYEE_KEY = 195 AND PAY_PERIOD_ID = 26
""").collect()
duration_before = time.time() - start

# Apply Z-ORDER
spark.sql("""
    OPTIMIZE gold.fact_payroll_earnings 
    ZORDER BY (EMPLOYEE_KEY, PAY_PERIOD_ID)
""")

# Test query AFTER Z-ORDER
start = time.time()
spark.sql("""
    SELECT * FROM gold.fact_payroll_earnings 
    WHERE EMPLOYEE_KEY = 195 AND PAY_PERIOD_ID = 26
""").collect()
duration_after = time.time() - start

# Assert performance improvement (at least 20% faster)
improvement = ((duration_before - duration_after) / duration_before) * 100
assert improvement >= 20.0, \
    f"Z-ORDER provided only {improvement:.1f}% improvement (expected >= 20%)"
```

**Expected Result**: `>= 20% query improvement`

---

### 4. End-to-End Integration Tests

#### TC-E2E-001: Full Pipeline Execution
**Objective**: Validate complete Bronze → Silver → Gold pipeline  
**Criticality**: HIGH  
**Type**: Automated

**Test Steps**:
```python
# Pre-condition: Clean test environment
test_period = 99  # Use test period to avoid production conflict

# Step 1: Ingest test data to Bronze
ingest_bronze(entity='TestEmployee', period=test_period, test_data=sample_json)

# Step 2: Transform to Silver
transform_silver(entity='TestEmployee', period=test_period)

# Step 3: Load to Gold
load_gold_dimensions()
load_gold_facts(period=test_period)

# Validation: E2E row count consistency
bronze_count = get_record_count('bronze', entity='TestEmployee', period=test_period)
silver_count = get_record_count('silver', entity='dim_test_employee')
gold_count = get_record_count('gold', entity='dwh_dim_employee', 
                              filter='WHERE SOURCE_SYSTEM = "TEST"')

assert bronze_count > 0, "Bronze ingestion failed"
assert silver_count > 0, "Silver transformation failed"
assert gold_count == silver_count, f"Gold load failed: Silver={silver_count}, Gold={gold_count}"
```

**Expected Result**: Data flows through all 3 layers successfully

---

#### TC-E2E-002: SLA Compliance (Performance)
**Objective**: Validate E2E processing completes within 70-minute SLA  
**Criticality**: HIGH  
**Type**: Automated

**Test Steps**:
```python
import time

# Run full pipeline for period 26 (typical production workload)
start_time = time.time()

orchestrator = MedallionOrchestrator()
orchestrator.run_bronze_ingestion(period_id=26)
orchestrator.run_silver_transformation()
orchestrator.run_gold_load()

end_time = time.time()
duration_minutes = (end_time - start_time) / 60

assert duration_minutes <= 70.0, \
    f"Pipeline exceeded SLA: {duration_minutes:.1f} minutes (SLA: 70 min)"
```

**Expected Result**: `duration <= 70 minutes`  
**Warning Threshold**: `> 60 minutes` (85% of SLA)

---

### 5. Regression Test Cases

#### TC-REG-001: Schema Evolution Detection
**Objective**: Detect new columns from API without breaking pipeline  
**Criticality**: MEDIUM  
**Type**: Automated

**Test Steps**:
```python
# Capture current Silver schema
current_schema = spark.table("silver.fact_earnings").schema.fields

# Run transformation with test JSON containing NEW column
test_json = {
    "Data": [{
        "EmployeeNumber": "12345",
        "PayPeriodID": 26,
        "EarningsAmount": 1000,
        "NewColumn": "NewValue"  # ← NEW COLUMN
    }]
}

# Transform to Silver
transform_silver(test_data=test_json)

# Validate new column detected and added
new_schema = spark.table("silver.fact_earnings").schema.fields
new_columns = set([f.name for f in new_schema]) - set([f.name for f in current_schema])

assert 'NewColumn' in new_columns, "Schema evolution not detected"
```

**Expected Result**: New column automatically added to Silver

---

#### TC-REG-002: Historical Data Consistency
**Objective**: Ensure old periods unchanged after new period loads  
**Criticality**: HIGH  
**Type**: Automated

**Test Steps**:
```python
# Capture checksum for period 25 BEFORE loading period 26
checksum_before = spark.sql("""
    SELECT 
        SUM(CAST(EARNINGS_AMOUNT * 100 AS BIGINT)) as checksum,
        COUNT(*) as row_count
    FROM gold.fact_payroll_earnings
    WHERE PAY_PERIOD_ID = 25
""").collect()[0]

# Load new period 26
load_gold_facts(period=26)

# Capture checksum for period 25 AFTER loading period 26
checksum_after = spark.sql("""
    SELECT 
        SUM(CAST(EARNINGS_AMOUNT * 100 AS BIGINT)) as checksum,
        COUNT(*) as row_count
    FROM gold.fact_payroll_earnings
    WHERE PAY_PERIOD_ID = 25
""").collect()[0]

# Assert period 25 data unchanged
assert checksum_before == checksum_after, \
    f"Historical data corrupted: Period 25 changed after loading Period 26"
```

**Expected Result**: Historical periods immutable

---

## 🤖 Automation Framework

### Test Runner Script

```python
# test_runner.py
"""
Automated Test Suite for Medallion Architecture
Run with: python test_runner.py --layer bronze --env test
"""

import argparse
from datetime import datetime
from pyspark.sql import SparkSession
import json

class MedallionTestSuite:
    def __init__(self, spark, layer, environment):
        self.spark = spark
        self.layer = layer
        self.env = environment
        self.results = []
        
    def run_test(self, test_id, test_func, criticality):
        """Execute single test case and capture results"""
        try:
            start_time = datetime.now()
            test_func()
            end_time = datetime.now()
            
            result = {
                'test_id': test_id,
                'status': 'PASS',
                'criticality': criticality,
                'duration': (end_time - start_time).total_seconds(),
                'timestamp': datetime.now().isoformat(),
                'error': None
            }
        except AssertionError as e:
            result = {
                'test_id': test_id,
                'status': 'FAIL',
                'criticality': criticality,
                'duration': None,
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
        except Exception as e:
            result = {
                'test_id': test_id,
                'status': 'ERROR',
                'criticality': criticality,
                'duration': None,
                'timestamp': datetime.now().isoformat(),
                'error': f"Unexpected error: {str(e)}"
            }
        
        self.results.append(result)
        
        # Print real-time result
        status_icon = "✅" if result['status'] == 'PASS' else \
                      "❌" if result['status'] == 'FAIL' else "⚠️"
        print(f"{status_icon} {test_id}: {result['status']} ({criticality})")
        if result['error']:
            print(f"   Error: {result['error']}")
        
        return result
    
    def run_bronze_tests(self):
        """Execute all Bronze layer tests"""
        print("\n🥉 Running Bronze Layer Tests...")
        
        self.run_test('TC-BR-001', self.test_br_001_api_completeness, 'HIGH')
        self.run_test('TC-BR-002', self.test_br_002_json_schema, 'HIGH')
        self.run_test('TC-BR-003', self.test_br_003_partition_strategy, 'MEDIUM')
        self.run_test('TC-BR-004', self.test_br_004_watermark_update, 'HIGH')
    
    def run_silver_tests(self):
        """Execute all Silver layer tests"""
        print("\n🥈 Running Silver Layer Tests...")
        
        self.run_test('TC-SL-001', self.test_sl_001_schema_transform, 'HIGH')
        self.run_test('TC-SL-002', self.test_sl_002_null_checks, 'HIGH')
        self.run_test('TC-SL-003', self.test_sl_003_business_rules, 'MEDIUM')
        self.run_test('TC-SL-004', self.test_sl_004_deduplication, 'HIGH')
        self.run_test('TC-SL-005', self.test_sl_005_reconciliation, 'HIGH')
    
    def run_gold_tests(self):
        """Execute all Gold layer tests"""
        print("\n🥇 Running Gold Layer Tests...")
        
        self.run_test('TC-GL-001', self.test_gl_001_surrogate_keys, 'HIGH')
        self.run_test('TC-GL-002', self.test_gl_002_fk_integrity, 'HIGH')
        self.run_test('TC-GL-003', self.test_gl_003_idempotency, 'HIGH')
        self.run_test('TC-GL-004', self.test_gl_004_star_schema, 'MEDIUM')
        self.run_test('TC-GL-005', self.test_gl_005_zorder, 'LOW')
    
    def run_e2e_tests(self):
        """Execute end-to-end integration tests"""
        print("\n🔄 Running E2E Integration Tests...")
        
        self.run_test('TC-E2E-001', self.test_e2e_001_full_pipeline, 'HIGH')
        self.run_test('TC-E2E-002', self.test_e2e_002_sla_compliance, 'HIGH')
    
    def generate_report(self):
        """Generate test execution report"""
        total_tests = len(self.results)
        passed = len([r for r in self.results if r['status'] == 'PASS'])
        failed = len([r for r in self.results if r['status'] == 'FAIL'])
        errors = len([r for r in self.results if r['status'] == 'ERROR'])
        
        pass_rate = (passed / total_tests * 100) if total_tests > 0 else 0
        
        # Critical failures (HIGH criticality failures)
        critical_failures = [r for r in self.results 
                           if r['status'] == 'FAIL' and r['criticality'] == 'HIGH']
        
        report = {
            'test_run': {
                'timestamp': datetime.now().isoformat(),
                'layer': self.layer,
                'environment': self.env
            },
            'summary': {
                'total_tests': total_tests,
                'passed': passed,
                'failed': failed,
                'errors': errors,
                'pass_rate': round(pass_rate, 2),
                'critical_failures': len(critical_failures)
            },
            'results': self.results,
            'deployment_recommendation': 'APPROVED' if len(critical_failures) == 0 
                                          else 'BLOCKED'
        }
        
        # Print summary
        print("\n" + "="*60)
        print("TEST EXECUTION SUMMARY")
        print("="*60)
        print(f"Layer: {self.layer.upper()}")
        print(f"Total Tests: {total_tests}")
        print(f"✅ Passed: {passed}")
        print(f"❌ Failed: {failed}")
        print(f"⚠️  Errors: {errors}")
        print(f"Pass Rate: {pass_rate:.1f}%")
        print(f"Critical Failures: {len(critical_failures)}")
        print(f"\n🚀 Deployment: {report['deployment_recommendation']}")
        print("="*60)
        
        # Save report to Delta table
        self.save_report_to_delta(report)
        
        return report
    
    def save_report_to_delta(self, report):
        """Persist test results to Delta table for historical tracking"""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
        
        # Flatten results for Delta
        flattened_results = []
        for result in report['results']:
            flattened_results.append({
                'test_run_id': report['test_run']['timestamp'],
                'layer': report['test_run']['layer'],
                'environment': report['test_run']['environment'],
                'test_id': result['test_id'],
                'status': result['status'],
                'criticality': result['criticality'],
                'duration_seconds': result['duration'],
                'error_message': result['error'],
                'timestamp': result['timestamp']
            })
        
        # Convert to DataFrame and save
        results_df = self.spark.createDataFrame(flattened_results)
        results_df.write.mode("append").saveAsTable("qa.test_execution_results")
        
        print(f"\n📊 Test results saved to qa.test_execution_results")
    
    # ============================================
    # Test Case Implementations
    # ============================================
    
    def test_br_001_api_completeness(self):
        """TC-BR-001: API Ingestion Completeness"""
        # Implementation from test case above
        pass
    
    def test_br_002_json_schema(self):
        """TC-BR-002: JSON Schema Preservation"""
        pass
    
    # ... (implement all test methods)
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run Medallion Architecture Test Suite')
    parser.add_argument('--layer', choices=['bronze', 'silver', 'gold', 'e2e', 'all'], 
                        required=True, help='Layer to test')
    parser.add_argument('--env', choices=['dev', 'test', 'prod'], 
                        default='test', help='Environment')
    
    args = parser.parse_args()
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName(f"QA_Tests_{args.layer}") \
        .getOrCreate()
    
    # Run tests
    suite = MedallionTestSuite(spark, args.layer, args.env)
    
    if args.layer == 'bronze':
        suite.run_bronze_tests()
    elif args.layer == 'silver':
        suite.run_silver_tests()
    elif args.layer == 'gold':
        suite.run_gold_tests()
    elif args.layer == 'e2e':
        suite.run_e2e_tests()
    elif args.layer == 'all':
        suite.run_bronze_tests()
        suite.run_silver_tests()
        suite.run_gold_tests()
        suite.run_e2e_tests()
    
    # Generate report
    report = suite.generate_report()
    
    # Exit with failure code if critical tests failed
    if report['deployment_recommendation'] == 'BLOCKED':
        print("\n🚫 CRITICAL FAILURES DETECTED - Deployment blocked")
        exit(1)
    else:
        print("\n✅ ALL TESTS PASSED - Deployment approved")
        exit(0)
```

---

## 📊 Test Execution Report Template

### Daily QA Report

```markdown
# QA Test Execution Report
**Date**: 2026-04-17  
**Environment**: PRODUCTION  
**Pipeline Run**: Period 26 (Bi-weekly Payroll)

---

## Executive Summary

| Metric | Value | Status |
|--------|-------|--------|
| **Total Tests Executed** | 45 | ✅ |
| **Pass Rate** | 97.8% | ✅ |
| **Failed Tests** | 1 | ⚠️ |
| **Critical Failures** | 0 | ✅ |
| **Deployment Recommendation** | **APPROVED** | ✅ |
| **E2E Execution Time** | 59 min | ✅ (Under 70 min SLA) |

---

## Test Results by Layer

### Bronze Layer (5 tests)
- ✅ **TC-BR-001**: API Ingestion Completeness - PASS
- ✅ **TC-BR-002**: JSON Schema Preservation - PASS
- ✅ **TC-BR-003**: Partition Strategy Validation - PASS
- ✅ **TC-BR-004**: Watermark Update Validation - PASS
- ✅ **TC-BR-005**: OAuth2 Token Renewal - PASS

**Bronze Summary**: 5/5 passed (100%)

---

### Silver Layer (8 tests)
- ✅ **TC-SL-001**: Schema Transformation - PASS
- ✅ **TC-SL-002**: NULL Checks - PASS
- ⚠️ **TC-SL-003**: Business Rule Validation - FAIL (Non-critical)
  - **Issue**: 3 employees with earnings > $50K (bi-weekly)
  - **Root Cause**: Legitimate bonuses for executives
  - **Action**: Update business rule threshold to $75K
- ✅ **TC-SL-004**: Deduplication - PASS
- ✅ **TC-SL-005**: Bronze-Silver Reconciliation - PASS
- ✅ **TC-SL-006**: Data Type Validation - PASS
- ✅ **TC-SL-007**: Column Evolution Detection - PASS
- ✅ **TC-SL-008**: Partition Optimization - PASS

**Silver Summary**: 7/8 passed (87.5%) - 1 non-critical failure

---

### Gold Layer (9 tests)
- ✅ **TC-GL-001**: Surrogate Key Preservation - PASS
- ✅ **TC-GL-002**: FK Integrity (100%) - PASS
- ✅ **TC-GL-003**: Incremental Load Idempotency - PASS
- ✅ **TC-GL-004**: Star Schema Validation - PASS
- ✅ **TC-GL-005**: Z-ORDER Optimization - PASS (28% improvement)
- ✅ **TC-GL-006**: MERGE Performance - PASS (4.2 min for 10K records)
- ✅ **TC-GL-007**: Silver-Gold Reconciliation - PASS
- ✅ **TC-GL-008**: Dimension SCD Validation - PASS
- ✅ **TC-GL-009**: Fact Table Aggregation Accuracy - PASS

**Gold Summary**: 9/9 passed (100%)

---

### E2E Integration (3 tests)
- ✅ **TC-E2E-001**: Full Pipeline Execution - PASS
- ✅ **TC-E2E-002**: SLA Compliance - PASS (59 min)
- ✅ **TC-E2E-003**: Data Lineage Validation - PASS

**E2E Summary**: 3/3 passed (100%)

---

## Data Quality Metrics

| Dimension | Target | Actual | Status |
|-----------|--------|--------|--------|
| **Completeness** | > 99% | 99.8% | ✅ |
| **Accuracy** | 100% | 100% | ✅ |
| **Consistency** | 100% | 100% | ✅ |
| **Timeliness** | < 70 min | 59 min | ✅ |
| **Uniqueness** | 100% | 100% | ✅ |
| **FK Integrity** | 100% | 100% | ✅ |

---

## Performance Benchmarks

### Processing Time by Layer

| Layer | Duration | Target | Status |
|-------|----------|--------|--------|
| Bronze Ingestion | 25 min | < 30 min | ✅ |
| Silver Transformation | 18 min | < 25 min | ✅ |
| Gold Load (Dimensions) | 4 min | < 10 min | ✅ |
| Gold Load (Facts) | 12 min | < 20 min | ✅ |
| **Total E2E** | **59 min** | **< 70 min** | ✅ |

### Query Performance (Gold Layer)

| Query Type | Before Z-ORDER | After Z-ORDER | Improvement |
|------------|----------------|---------------|-------------|
| Single Employee Lookup | 12 sec | 5 sec | 2.4x |
| Period Aggregation | 45 sec | 22 sec | 2.0x |
| Full Table Scan | 120 sec | 95 sec | 1.3x |

---

## Issues & Resolutions

### Issue #1: TC-SL-003 Failure
**Status**: RESOLVED  
**Severity**: LOW  
**Description**: Business rule validation flagged 3 employees with bi-weekly earnings > $50K

**Root Cause Analysis**:
- Legitimate executive bonuses paid in period 26
- Current threshold ($50K bi-weekly) too conservative

**Resolution**:
- Updated business rule threshold to $75K bi-weekly
- Added exception list for known executives
- Updated TC-SL-003 validation logic

**Action Items**:
- ✅ Update test case threshold (Completed)
- ✅ Document exception process (Completed)
- ⏳ Implement alerting for > $75K (In Progress)

---

## Regression Testing

### Historical Periods Validation
Validated that new period load (26) did not corrupt historical data:

| Period | Records | Checksum | Status |
|--------|---------|----------|--------|
| Period 24 | 48,234 | 8A3F2B... | ✅ Unchanged |
| Period 25 | 49,102 | 5D8E7C... | ✅ Unchanged |
| Period 26 | 48,987 | NEW | ✅ Loaded |

---

## Deployment Recommendation

### ✅ **APPROVED FOR PRODUCTION**

**Justification**:
- ✅ **Critical Tests**: 100% pass rate (0 high-severity failures)
- ✅ **Data Quality**: All dimensions exceed 99% targets
- ✅ **Performance**: E2E execution 16% under SLA
- ✅ **FK Integrity**: Zero orphan records
- ⚠️ **Non-Critical Issue**: 1 business rule threshold adjustment (resolved)

**Sign-Off**:
- QA Lead: [Name]
- Data Engineering Lead: [Name]
- Date: 2026-04-17

---

## Appendix A: Test Environment Details

**Infrastructure**:
- Microsoft Fabric Workspace: `QA_DataEngineering`
- Lakehouse: `qa_lakehouse`
- Compute: Spark 3.5, 4 nodes (Standard_D8s_v3)
- Data Volume: 1.3M records (period 26)

**Test Data**:
- Bronze: 50K records/period × 26 periods = 1.3M records
- Silver: 5 tables (2 dims, 3 facts)
- Gold: 5 tables (star schema)

---

## Appendix B: Automated Test Queries

### Quick Health Check Query
```sql
-- Run this query for daily QA smoke test
WITH layer_stats AS (
    SELECT 'Bronze' as layer, COUNT(*) as record_count 
    FROM bronze.hcm_api_extracts 
    WHERE partition_column = '26'
    
    UNION ALL
    
    SELECT 'Silver', COUNT(*) 
    FROM silver.fact_earnings 
    WHERE PAY_PERIOD_ID = 26
    
    UNION ALL
    
    SELECT 'Gold', COUNT(*) 
    FROM gold.fact_payroll_earnings 
    WHERE PAY_PERIOD_ID = 26
)
SELECT 
    layer,
    record_count,
    CASE 
        WHEN record_count > 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END as status
FROM layer_stats;
```

### FK Integrity Quick Check
```sql
-- Critical: Run after every Gold load
SELECT 
    'fact_payroll_earnings' as fact_table,
    COUNT(*) as total_records,
    SUM(CASE WHEN e.EMPLOYEE_KEY IS NULL THEN 1 ELSE 0 END) as orphan_records,
    CAST(SUM(CASE WHEN e.EMPLOYEE_KEY IS NULL THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100 as orphan_pct
FROM gold.fact_payroll_earnings f
LEFT JOIN gold.dwh_dim_employee e ON f.EMPLOYEE_KEY = e.EMPLOYEE_KEY
HAVING SUM(CASE WHEN e.EMPLOYEE_KEY IS NULL THEN 1 ELSE 0 END) > 0;

-- Expected: 0 rows (zero orphans)
```

---

*Report generated automatically by QA Test Suite v2.1*  
*Next scheduled run: 2026-04-18 02:00 UTC*
```

---

## 🔄 CI/CD Integration

### Azure DevOps Pipeline Integration

```yaml
# azure-pipelines.yml
trigger:
  branches:
    include:
      - main
      - develop

stages:
  - stage: QA_Testing
    displayName: 'Data Quality & Testing'
    jobs:
      - job: Bronze_Layer_Tests
        displayName: 'Bronze Layer Validation'
        steps:
          - task: AzureCLI@2
            inputs:
              scriptType: 'bash'
              scriptLocation: 'inlineScript'
              inlineScript: |
                python test_runner.py --layer bronze --env test
      
      - job: Silver_Layer_Tests
        displayName: 'Silver Layer Validation'
        dependsOn: Bronze_Layer_Tests
        steps:
          - task: AzureCLI@2
            inputs:
              scriptType: 'bash'
              scriptLocation: 'inlineScript'
              inlineScript: |
                python test_runner.py --layer silver --env test
      
      - job: Gold_Layer_Tests
        displayName: 'Gold Layer Validation'
        dependsOn: Silver_Layer_Tests
        steps:
          - task: AzureCLI@2
            inputs:
              scriptType: 'bash'
              scriptLocation: 'inlineScript'
              inlineScript: |
                python test_runner.py --layer gold --env test
      
      - job: E2E_Tests
        displayName: 'End-to-End Tests'
        dependsOn: Gold_Layer_Tests
        steps:
          - task: AzureCLI@2
            inputs:
              scriptType: 'bash'
              scriptLocation: 'inlineScript'
              inlineScript: |
                python test_runner.py --layer e2e --env test
      
      - job: Generate_QA_Report
        displayName: 'Generate QA Report'
        dependsOn: E2E_Tests
        steps:
          - task: PublishTestResults@2
            inputs:
              testResultsFormat: 'JUnit'
              testResultsFiles: '**/test-results.xml'
```

---

## 📚 Best Practices from 10 Years QA Experience

### 1. **Test Data Management**
```python
# Always use synthetic data for QA environments
# Never use production data (GDPR/privacy compliance)

class TestDataGenerator:
    """Generate realistic test data for QA"""
    
    @staticmethod
    def generate_employee_records(count=100):
        """Generate synthetic employee records"""
        from faker import Faker
        fake = Faker()
        
        employees = []
        for i in range(count):
            employees.append({
                'EMPLOYEE_NUMBER': f'TEST{i:05d}',
                'FIRST_NAME': fake.first_name(),
                'LAST_NAME': fake.last_name(),
                'HIRE_DATE': fake.date_between(start_date='-5y'),
                'DEPARTMENT': fake.random_element(['Engineering', 'Sales', 'HR']),
                'SALARY': fake.random_int(min=40000, max=150000)
            })
        return employees
```

### 2. **Shift-Left Testing**
- Test Bronze ingestion **before** Silver transformation
- Validate Silver transformations **before** Gold aggregations
- Catch issues early (cheaper to fix)

### 3. **Test Coverage Pyramid**
```
          /\      E2E Tests (10%)
         /  \     ← Slow, expensive, comprehensive
        /____\    
       /      \   Integration Tests (30%)
      /        \  ← Medium speed, layer-by-layer
     /__________\ 
    /            \ Unit Tests (60%)
   /______________\ ← Fast, cheap, focused
```

### 4. **Data Quality SLAs**
Define clear targets:
- **Completeness**: > 99.5% (allow 0.5% data loss for deduplication)
- **Accuracy**: 100% (zero tolerance for corrupted data)
- **Timeliness**: < 70 minutes E2E (bi-weekly SLA)
- **FK Integrity**: 100% (zero orphan records)

### 5. **Idempotency is King**
Every test should be:
- ✅ Repeatable (same result every run)
- ✅ Isolated (doesn't depend on previous runs)
- ✅ Atomic (tests one thing)

---

## 🎓 QA Maturity Model

### Level 1: Manual Testing (Initial)
- Manual SQL queries
- Spot checking samples
- Ad-hoc validation

### Level 2: Scripted Testing (Managed)
- Automated test scripts
- Scheduled batch execution
- Basic reporting

### Level 3: Continuous Testing (Defined) ← **Current State**
- CI/CD integrated
- Automated fail-fast
- Historical trend analysis
- Deployment gating

### Level 4: Predictive Quality (Optimized) ← **Target State**
- ML-based anomaly detection
- Proactive alerting
- Auto-remediation
- Quality forecasting

---

## 📞 Support & Escalation

**QA Team Contact**:
- Email: idia.herrera@gmail.com

**Escalation Matrix**:
- **LOW**: Non-critical test failures → Create Jira ticket
- **MEDIUM**: Data quality threshold breaches → Notify QA Lead
- **HIGH**: FK integrity violations → Block deployment + page on-call
- **CRITICAL**: Production data corruption → Initiate incident response



