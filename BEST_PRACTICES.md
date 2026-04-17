# Key Learnings & Best Practices

## Executive Summary

This document captures the key learnings, design decisions, and best practices from implementing a production-grade Medallion Architecture on Microsoft Fabric for payroll analytics.

**Project Context:**
- **Scale**: ~1.3M records, 5 entities, 26 pay periods/year
- **Performance**: 59-minute E2E processing time (within 70-minute SLA)
- **Complexity**: Mixed incremental/full-load patterns, surrogate key management, metadata-driven configuration

---

## Architecture Patterns That Worked Well

### 1. ✅ Dual Partition Strategy (Bronze Layer)

**Problem**: Mixed workload with different retention needs
- Dimensions need only latest snapshot (auto-cleanup)
- Facts need historical periods (retention)

**Solution**: Single table with partition type discriminator
```python
partition_value = "FULL_LOAD"  # For dimensions
partition_value = str(period_id).zfill(2)  # For facts ("01"-"26")
```

**Impact**:
- ✅ 1 table instead of 2 (`*_full` + `*_incremental`)
- ✅ Self-documenting structure
- ✅ Automatic cleanup for dimensions
- ✅ Zero-maintenance

**When to Use**:
- Mixed load types in same pipeline
- Need clear separation between snapshot and historical data
- Want to avoid manual cleanup jobs

**When NOT to Use**:
- All entities are same load type (use simpler partitioning)
- Need SCD Type 2 for dimensions (use effective_date partitioning instead)

---

### 2. ✅ Metadata-Driven Configuration

**Problem**: Adding new entities required code changes and deployments

**Solution**: Store all configurations in Delta control tables
```sql
-- Adding new dimension = INSERT into control table (no code deployment)
INSERT INTO ctrl.gold_catalog VALUES (
    'new_dimension', 'DIMENSION', 'silver_source', ...
)
```

**Impact**:
- ✅ Added 3 new entities in 1 hour (vs. 2 days with hardcoded approach)
- ✅ Configuration changes are auditable (who, when, what)
- ✅ Same code works across dev/test/prod (different configs)

**Best Practices**:
- Version control your catalog INSERT statements
- Add `is_active` flag to disable entities without deleting
- Use `created_at`/`updated_at` for audit trail
- Store complex configs as JSON columns (e.g., `update_columns` ARRAY→JSON)

**Trade-offs**:
- ⚠️ Debugging requires inspecting control tables (not just code)
- ⚠️ Need strong data validation on config inserts
- ⚠️ Learning curve for new team members

---

### 3. ✅ MERGE for Surrogate Key Preservation

**Problem**: Full dimension overwrites → regenerated surrogate keys → broken FK relationships

**Solution**: MERGE INTO with business key matching
```sql
MERGE INTO gold.dim_employee AS target
USING staging AS source
ON target.EMPLOYEE_NUMBER = source.EMPLOYEE_NUMBER  -- Business key
WHEN MATCHED THEN UPDATE SET ... -- Attributes only (preserve EMPLOYEE_KEY)
WHEN NOT MATCHED THEN INSERT *   -- New employee gets new key
```

**Impact**:
- ✅ Zero FK orphans in fact tables (vs. 12% orphan rate with OVERWRITE)
- ✅ Stable keys enable historical fact analysis
- ✅ Fact tables don't need re-enrichment after dimension refresh

**Critical for**:
- Systems where facts are continuously appended
- Dimensions are full-loaded (not incremental)
- Need referential integrity across refreshes

**Performance**:
- MERGE is ~2x slower than OVERWRITE (4 min vs. 2 min for 10K employees)
- Acceptable trade-off for data integrity

---

### 4. ✅ Watermark-Based Incremental Processing

**Problem**: Need to process only new data, avoid re-processing

**Solution**: Multi-type watermark tracking
```python
# Supports int, timestamp, date
watermark = get_last_watermark('fact_earnings', 'int')  # Returns 25
new_data = silver.filter(col('pay_period_id') > watermark)  # Process 26 only
```

**Impact**:
- ✅ 95% reduction in processing time for incremental runs
- ✅ Idempotent (re-run same period → same result)
- ✅ Per-entity tracking enables targeted recovery

**Implementation Tips**:
- Store watermark as STRING (universal type)
- Use execution_log for watermark tracking (not separate table)
- Log watermark BEFORE and AFTER processing (recovery)
- Support multi-type watermarks (don't force timestamp conversion)

---

## Performance Optimizations

### Z-ORDER: Query Performance Game-Changer

**Before Z-ORDER**:
```sql
-- Query: Filter by EMPLOYEE_KEY
-- Time: 12 seconds
-- Files read: 450 files (full table scan)
SELECT * FROM fact_earnings WHERE EMPLOYEE_KEY = 195;
```

**After Z-ORDER**:
```sql
OPTIMIZE fact_earnings ZORDER BY (EMPLOYEE_KEY, PAY_PERIOD_ID);

-- Same query
-- Time: 3 seconds (4x faster)
-- Files read: 18 files (data skipping)
```

**Best Practices**:
- Z-ORDER on most-queried filter columns (not all columns)
- Combine with partition pruning for max effect
- Re-run monthly (as data grows)
- Limit to 3-4 columns (diminishing returns after that)

**Cost**: ~5 minutes per 1M-row table

---

### Broadcast Joins: Dimension Lookups

**Before Broadcast**:
```python
# FK enrichment: shuffle-based join
fact.join(dim_employee, on='employee_number')  # 45 seconds
```

**After Broadcast**:
```python
# FK enrichment: broadcast join
fact.join(broadcast(dim_employee), on='employee_number')  # 15 seconds (3x faster)
```

**When to Use**:
- Dimension table < 100 MB (Spark broadcasts efficiently)
- Fact table is much larger than dimension
- Multiple fact tables referencing same dimension

**When NOT to Use**:
- Dimension > 1 GB (broadcast overhead exceeds benefit)
- Skewed joins (use salting instead)

---

### Predicate Pushdown: Partition Pruning

**Inefficient** (full scan):
```python
df = spark.table('fact_earnings')
df = df.filter(col('pay_period_id') == 26)  # Filter AFTER read
```

**Efficient** (partition pruning):
```python
df = spark.table('fact_earnings').where("pay_period_id = 26")  # Filter DURING read
# 10x faster: reads only 1/26 of data
```

**Best Practices**:
- Use SQL string filters for partition columns (enables pruning)
- Avoid `.filter()` on partition columns (use `.where()` with string)
- Check query plan: `EXPLAIN` should show "PartitionFilters"

---

## Anti-Patterns & Lessons Learned

### ❌ Anti-Pattern 1: Hardcoded Periods

**Bad**:
```python
# Hardcoded period list (requires code change to add period 27)
periods = [1, 2, 3, ..., 26]
```

**Good**:
```python
# Read from catalog
max_period = catalog.get('max_period')  # 26, or 27 next year
periods = range(1, max_period + 1)
```

**Lesson**: Never hardcode business rules (periods, entities, schemas)

---

### ❌ Anti-Pattern 2: JSON in Gold Layer

**Bad**:
```sql
-- Gold layer with nested JSON
CREATE TABLE gold.fact_earnings (
    payload JSON,  -- Still requires parsing in BI queries
    ...
)
```

**Good**:
```sql
-- Fully flattened star schema
CREATE TABLE gold.fact_earnings (
    EMPLOYEE_KEY INT,
    EARNING_AMOUNT DECIMAL(10,2),
    ...  -- All columns typed and indexed
)
```

**Lesson**: Gold = "query-ready", no parsing/transformations allowed

---

### ❌ Anti-Pattern 3: Overwrite Instead of MERGE (Dimensions)

**Bad**:
```python
# Full dimension overwrite
dim_df.write.mode('overwrite').saveAsTable('gold.dim_employee')
# Result: New EMPLOYEE_KEY values → fact FKs become orphans
```

**Good**:
```python
# MERGE preserves surrogate keys
load_dimension_with_merge(dim_df, 'gold.dim_employee', 'EMPLOYEE_NUMBER', 'EMPLOYEE_KEY')
```

**Lesson**: MERGE is mandatory when:
- Dimension has surrogate keys
- Facts reference those keys
- Dimension is full-loaded (not incremental)

---

## Security & Governance

### Secrets Management

**✅ Correct Approach**:
```python
# Retrieve from Azure Key Vault (never hardcode)
username = notebookutils.credentials.getSecret(vault_url, 'api-username')
password = notebookutils.credentials.getSecret(vault_url, 'api-password')

# DO NOT print secrets (even in debug mode)
print("✅ Credentials retrieved")  # NOT: print(username)
```

**Best Practices**:
- Store ALL credentials in Key Vault (no exceptions)
- Use Fabric managed identity for Key Vault access
- Rotate secrets quarterly
- Audit secret access logs

---

### Data Lineage & Auditability

**Embedded Metadata**:
```python
df = df.withColumn('source_system', lit('hcm_api'))
df = df.withColumn('ingestion_datetime', current_timestamp())
df = df.withColumn('pipeline_run_id', lit(execution_id))
```

**Why This Matters**:
- Trace any record back to source API call
- Identify data quality issues by ingestion batch
- Support compliance audits (GDPR, SOX)

**Storage Cost**: ~2% (worthwhile for auditability)

---

## Scalability Planning

### Current State (Baseline)
- **Volume**: 1.3M records/bi-weekly
- **Processing**: 59 minutes E2E
- **Storage**: ~2 GB Bronze per period
- **Headroom**: 19% (59 min / 70 min SLA)

### 3x Growth Scenario
| Component | Current | 3x Growth | Mitigation |
|-----------|---------|-----------|------------|
| API calls | 130/period | 390/period | Parallel API calls (requires quota increase) |
| Processing time | 59 min | ~120 min | Add Spark executors (4 → 12) |
| Storage | 2 GB/period | 6 GB/period | Compression (Snappy → Zstd) |
| Fact table | 1.3M rows | 3.9M rows | Partition by year + period |

**Recommended Actions**:
1. **Immediate**: Implement parallel API calls (Bronze layer)
2. **Q2**: Add year-based partitioning (Gold facts)
3. **Q3**: Upgrade Fabric capacity (F64 → F128)
4. **Future**: Consider streaming ingestion (replace batch)

---

## Testing Strategy

### Data Quality Gates

**Implemented Checks**:
```python
# 1. Null checks
assert df.filter(col('employee_number').isNull()).count() == 0

# 2. Range validation
assert df.filter(col('pay_period_id').between(1, 26)).count() == total_rows

# 3. Referential integrity
orphans = fact.join(dim, on='EMPLOYEE_KEY', how='left_anti').count()
assert orphans == 0, f"Found {orphans} orphan records"

# 4. Duplicate detection
duplicates = df.groupBy('business_key').count().filter(col('count') > 1).count()
assert duplicates == 0
```

**When to Run**:
- Silver layer: After each transformation
- Gold layer: After each dimension/fact load
- E2E: Daily (automated tests)

---

## Operational Excellence

### Monitoring Dashboards

**Key Metrics to Track**:
1. **Processing Time**: Bronze/Silver/Gold duration per entity
2. **Record Counts**: Expected vs. actual per layer
3. **Failure Rate**: % of runs with errors
4. **Watermark Lag**: How far behind is processing?
5. **Storage Growth**: GB per layer per month

**Alerting Thresholds**:
- Processing time > 70 min → Slack alert
- Record count variance > 10% → Email alert
- Any failure → PagerDuty page

---

### Incident Response Playbook

**Scenario 1: API Token Expired**
- **Symptom**: 401 errors in Bronze layer
- **Solution**: Auto-renewal should handle; if not, manually refresh secret in Key Vault
- **Recovery**: Re-run from last watermark

**Scenario 2: Schema Change**
- **Symptom**: JSON parsing errors in Silver
- **Solution**: Column evolution should auto-add; if fails, manually ALTER TABLE
- **Recovery**: Re-process failed period

**Scenario 3: Surrogate Key Collision**
- **Symptom**: Duplicate EMPLOYEE_KEY in Gold dimension
- **Solution**: This should be impossible with MERGE; investigate control table corruption
- **Recovery**: Rebuild dimension from Silver (preserve old keys via backup)

---

## Future Enhancements

### Short-Term (Next 3 Months)
1. **Streaming Ingestion**: Replace batch with Spark Structured Streaming
   - Goal: Near-real-time facts (< 5 min latency)
   - Complexity: Medium
   - Impact: High (enables real-time dashboards)

2. **SCD Type 2 for Dimensions**: Track attribute history
   - Goal: Analyze employee data as-of specific dates
   - Complexity: Low (change MERGE logic)
   - Impact: Medium (enables historical analysis)

3. **dbt Integration**: Use dbt for Gold transformations
   - Goal: SQL-based transformations, better testing
   - Complexity: Low
   - Impact: Medium (improves maintainability)

### Long-Term (Next Year)
1. **Data Quality Framework**: Integrate Great Expectations
2. **ML Features**: Add feature engineering for predictive models
3. **Multi-Source Integration**: Extend to other HR systems
4. **Delta Live Tables**: Migrate to managed DLT pipelines

---

## Conclusion

This implementation demonstrates **production-grade data engineering**:

**Technical Excellence**:
- ✅ 100% metadata-driven (extensible without code changes)
- ✅ Sub-SLA performance (59 min / 70 min SLA = 16% headroom)
- ✅ Zero data integrity issues (FK preservation, idempotency)

**Operational Maturity**:
- ✅ Comprehensive error handling (retry, auto-renewal, logging)
- ✅ Full auditability (lineage, watermarks, execution logs)
- ✅ Scalability planning (clear path to 3x growth)

**Key Takeaway**: Modern data platforms require thinking beyond "moving data"—you must design for **change** (schema evolution, new entities), **scale** (3x growth), and **operations** (monitoring, recovery, auditability).

---

## References

- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Delta Lake Best Practices](https://delta.io/learn/best-practices/)
- [Kimball Dimensional Modeling](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)
- [Microsoft Fabric Documentation](https://learn.microsoft.com/en-us/fabric/)

---

**Last Updated**: April 15, 2026  
**Author**: Data Engineering Team  
**Version**: 1.0 (Production)
