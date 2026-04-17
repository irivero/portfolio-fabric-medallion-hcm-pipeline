# Enterprise HCM Data Integration: Medallion Architecture on Microsoft Fabric

[![Microsoft Fabric](https://img.shields.io/badge/Microsoft-Fabric-blue)](https://www.microsoft.com/fabric)
[![Delta Lake](https://img.shields.io/badge/Delta-Lake-00ADD8)](https://delta.io/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-orange)](https://spark.apache.org/)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion-green)](https://www.databricks.com/glossary/medallion-architecture)

## 📊 Project Overview

A production-grade **Medallion Architecture** data pipeline implementing **Bronze → Silver → Gold** layers for HR/Payroll analytics on **Microsoft Fabric**. This project demonstrates enterprise-level data engineering patterns including **metadata-driven orchestration**, **incremental processing**, **surrogate key management**, and **watermark-based CDC**.

### Business Context

This system integrates payroll and employee data from an **enterprise HCM system** (Human Capital Management API) to enable:
- **Bi-weekly payroll analytics** (26 pay periods/year)
- **Employee dimension management** (~10K active employees)
- **Multi-fact reporting** (earnings, deductions, taxes)
- **Historical trend analysis** with proper dimensional modeling

---

> **📌 Portfolio Disclaimer**
> 
> This repository contains **conceptual documentation and architectural patterns** from a production Medallion Architecture implementation on Microsoft Fabric. 
> 
> **For security and data protection reasons**:
> - Notebook names, file paths, and folder structures have been **generalized** for portfolio presentation
> - Real company names, API endpoints, credentials, and sensitive business data are **not included**
> - Configuration values and table names have been **anonymized or abstracted**
> 
> **What IS real**:
> - ✅ Technical patterns and architectural decisions (based on actual implementation)
> - ✅ Code samples and design patterns (functionally accurate)
> - ✅ Performance metrics and optimization techniques (real results)
> - ✅ Problem-solving approaches and engineering decisions (authentic experience)
> 
> This documentation represents **genuine production engineering experience** while respecting confidentiality and security best practices.

---

## 🏗️ Architecture Highlights

### Medallion Pattern Implementation

```
┌──────────────────────────────────────────────────────────────┐
│                    HCM REST API (Source)                     │
│            OAuth2 + Rate Limiting (48 calls/hour)            │
└─────────────────────────┬────────────────────────────────────┘
                          │
                          │ JSON Payload Streaming
                          ▼
┌──────────────────────────────────────────────────────────────┐
│           🥉 BRONZE LAYER - Raw Data Ingestion               │
│                                                              │
│  • Schema-agnostic JSON storage (Delta Lake)                │
│  • Dual partition strategy (INCREMENTAL + FULL_LOAD)        │
│  • Automatic watermark tracking per entity                  │
│  • Built-in retry & token renewal                           │
│                                                              │
│  Table: bronze.hcm_api_extracts                             │
│  Partitions: [partition_column, entity_name]                │
└─────────────────────────┬────────────────────────────────────┘
                          │
                          │ JSON → Typed Schemas
                          ▼
┌──────────────────────────────────────────────────────────────┐
│        🥈 SILVER LAYER - Validated & Typed Data              │
│                                                              │
│  • Dynamic schema inference from JSON payloads              │
│  • Automatic column evolution detection                     │
│  • Data quality validation rules                            │
│  • Deduplication & business logic transformations           │
│                                                              │
│  Tables:                                                     │
│  ├─ silver.fact_earnings (INCREMENTAL by pay_period)        │
│  ├─ silver.fact_deductions (INCREMENTAL by pay_period)      │
│  ├─ silver.fact_taxes (INCREMENTAL by pay_period)           │
│  ├─ silver.dim_employee (FULL_LOAD daily snapshot)          │
│  └─ silver.dim_location (FULL_LOAD daily snapshot)          │
└─────────────────────────┬────────────────────────────────────┘
                          │
                          │ Dimensional Modeling
                          ▼
┌──────────────────────────────────────────────────────────────┐
│         🥇 GOLD LAYER - Analytical Ready Data                │
│                                                              │
│  DIMENSIONS (MERGE with surrogate key preservation):        │
│  ├─ gold.dwh_dim_employee (EMPLOYEE_KEY)                    │
│  └─ gold.dwh_dim_location (LOCATION_KEY)                    │
│                                                              │
│  FACTS (Incremental watermark-based):                       │
│  ├─ gold.fact_payroll_earnings (FK enrichment)              │
│  ├─ gold.fact_payroll_deductions (FK enrichment)            │
│  └─ gold.fact_payroll_taxes (FK enrichment)                 │
│                                                              │
│  Optimizations: Z-ORDER, OPTIMIZE, VACUUM                   │
└──────────────────────────────────────────────────────────────┘
```

---

## 🎯 Key Technical Achievements

### 1. **Metadata-Driven Configuration**
- **Zero-hardcode philosophy**: All pipeline configurations stored in Delta control tables
- **Dynamic catalog loading**: Add new entities without code deployment
- **Centralized governance**: Single source of truth for schemas, watermarks, and business rules

### 2. **Dual Partition Strategy (Bronze Layer)**
A unique pattern solving the mixed workload challenge:
- **FULL_LOAD**: Uses sentinel partition value `"FULL_LOAD"` (auto-cleanup of old snapshots)
- **INCREMENTAL**: Uses pay period IDs `01-26` (retains historical data)
- **Benefits**: Single unified table, storage optimization, self-documenting structure

```python
# Intelligent partition routing
partition_value = "FULL_LOAD" if load_type == "FULL" else str(period_id).zfill(2)

df.write.option("replaceWhere", 
    f"partition_column = '{partition_value}' AND entity = '{entity_name}'"
).partitionBy("partition_column", "entity").saveAsTable(table)
```

### 3. **Surrogate Key Preservation**
Implemented **MERGE-based dimension loading** to maintain referential integrity:
- **Problem**: Full table overwrites regenerate surrogate keys → broken FK relationships
- **Solution**: MERGE INTO with business key matching
  - `WHEN MATCHED`: UPDATE attributes only (preserve surrogate key)
  - `WHEN NOT MATCHED`: INSERT with new surrogate key (auto-increment)

This ensures **stable dimension keys** across refreshes, critical for fact table consistency.

### 4. **Watermark-Based Incremental Processing**
- **Multi-type watermark support**: `timestamp`, `int` (pay_period_id), `date`
- **Per-entity tracking**: Independent watermarks for each fact table
- **Failure recovery**: Execution logs enable precise replay from last success
- **Idempotency**: Multiple runs of same period produce identical results

### 5. **Enterprise-Grade Error Handling**
- **OAuth2 token auto-renewal**: Detects expired tokens mid-execution and refreshes
- **API rate limiting**: Respects 48 calls/hour quota with intelligent pacing
- **Retry logic**: Exponential backoff for transient failures
- **Comprehensive logging**: Execution metrics persisted to control tables

---

## 📁 Repository Structure

```
├── docs/
│   ├── 01_ARCHITECTURE.md              # Detailed architecture documentation
│   ├── 02_BRONZE_LAYER.md              # Bronze ingestion patterns
│   ├── 03_SILVER_LAYER.md              # Transformation logic
│   ├── 04_GOLD_LAYER.md                # Dimensional modeling
│   ├── 05_CONTROL_TABLES.md            # Metadata framework
│   ├── 06_ORCHESTRATION.md             # Pipeline automation
│   └── diagrams/
│       ├── architecture_overview.md    # Mermaid diagrams
│       ├── data_flow.md                # Layer-by-layer flow
│       └── partition_strategy.md       # Partitioning deep-dive
│
├── notebooks/
│   ├── bronze/
│   │   └── ingest_api_bronze.ipynb    # API → Bronze ingestion
│   ├── silver/
│   │   └── transform_silver.ipynb     # Bronze → Silver transformation
│   ├── gold/
│   │   ├── load_dimensions.ipynb      # Silver → Gold dimensions (MERGE)
│   │   └── load_facts.ipynb           # Silver → Gold facts (incremental)
│   ├── setup/
│   │   ├── setup_control_tables.ipynb # Create metadata framework
│   │   └── populate_catalog.ipynb     # Initialize configurations
│   └── orchestration/
│       └── orchestrator.ipynb         # E2E pipeline runner
│
├── sql/
│   ├── create_bronze_tables.sql       # Bronze DDL
│   ├── create_silver_tables.sql       # Silver DDL
│   ├── create_gold_tables.sql         # Gold DDL (dimensions + facts)
│   └── create_control_tables.sql      # Metadata tables DDL
│
├── config/
│   ├── catalog_dimensions.json        # Dimension configurations
│   ├── catalog_facts.json             # Fact table configurations
│   └── api_config_template.json       # API connection template
│
└── tests/
    ├── test_bronze_ingestion.ipynb    # Bronze layer validation
    ├── test_silver_quality.ipynb      # Data quality checks
    └── test_gold_integrity.ipynb      # Referential integrity tests
```

---

## 🚀 Quick Start

### Prerequisites

- **Microsoft Fabric** workspace with:
  - Lakehouse configured (target for Bronze/Silver/Gold)
  - Azure Key Vault linked (for API credentials)
  - Spark runtime 3.5+ available
- **Source System**: REST API with OAuth2 authentication
- **Permissions**: 
  - Lakehouse Contributor role
  - Key Vault Secret Reader

### Installation

**Step 1: Deploy Control Tables**
```python
# Run setup notebook
notebooks/setup/setup_control_tables.ipynb

# Verify 7 control tables created:
# - ctrl.entity_catalog
# - ctrl.gold_catalog  
# - ctrl.gold_update_columns
# - ctrl.gold_zorder_columns
# - ctrl.ingestion_watermarks
# - ctrl.execution_log
# - ctrl.orchestrator_log
```

**Step 2: Configure Metadata**
```python
# Populate dimension & fact configurations
notebooks/setup/populate_catalog.ipynb

# Customization points:
# - API endpoints & report names
# - Business key mappings
# - Watermark strategies
# - Partition columns
```

**Step 3: Execute Pipeline**
```python
# Option A: Run layers individually
notebooks/bronze/ingest_api_bronze.ipynb    # ~25 min
notebooks/silver/transform_silver.ipynb     # ~18 min
notebooks/gold/load_dimensions.ipynb        # ~4 min
notebooks/gold/load_facts.ipynb             # ~12 min

# Option B: Orchestrated E2E
notebooks/orchestration/orchestrator.ipynb  # ~60 min total
```

---

## 📈 Performance Metrics

| Layer | Process | Volume | Duration | Optimization |
|-------|---------|--------|----------|--------------|
| Bronze | API Ingestion (5 entities, 26 periods) | ~50K records/period | 25 min | Parallel API calls, JSON serialization |
| Silver | JSON → Typed (5 tables) | ~1.3M records total | 18 min | Dynamic schema, predicate pushdown |
| Gold - Dims | MERGE (2 dimensions) | ~10K employees | 4 min | Z-ORDER on business keys |
| Gold - Facts | Incremental load (3 facts) | ~1.3M records | 12 min | Broadcast joins, partition pruning |
| **Total E2E** | **Bronze → Gold** | **~1.3M records** | **59 min** | **SLA: 70 min** |

### Optimization Highlights
- **Z-ORDER Indexing**: 40% query performance improvement on business keys
- **Partition Pruning**: 60% reduction in data scanned for incremental loads
- **Broadcast Joins**: 3x faster FK enrichment in fact tables
- **OPTIMIZE + VACUUM**: 25% storage reduction through file compaction

---

## 🛠️ Technical Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Platform** | Microsoft Fabric | Unified data lakehouse platform |
| **Storage** | OneLake (Delta Lake format) | ACID transactions, time travel, schema evolution |
| **Compute** | Apache Spark 3.5 (PySpark) | Distributed data processing |
| **Orchestration** | Fabric Notebooks | Programmatic pipeline execution |
| **Secrets** | Azure Key Vault | Credential management |
| **Catalogs** | Delta Tables (control schema) | Metadata-driven configuration |
| **API Client** | Python `requests` + OAuth2 | REST API integration |

---

## 💡 Advanced Patterns Demonstrated

### Pattern 1: Schema-on-Read with Column Evolution
```python
# Detect new columns in API response
bronze_columns = set(existing_schema.fieldNames())
json_columns = set(infer_schema(json_payload).fieldNames())
new_columns = json_columns - bronze_columns

if new_columns:
    for col_name in new_columns:
        spark.sql(f"ALTER TABLE silver.{table} ADD COLUMN {col_name} STRING")
```

### Pattern 2: Metadata-DrivenMERGE Generator
```python
# Generate MERGE statement from control table config
def build_merge_query(table_config: Dict) -> str:
    update_cols = fetch_update_columns(table_config['table_name'])
    
    return f"""
    MERGE INTO gold.{table_config['gold_table']} AS target
    USING staging AS source
    ON target.{table_config['business_key']} = source.{table_config['business_key']}
    WHEN MATCHED THEN UPDATE SET {', '.join(update_cols)}
    WHEN NOT MATCHED THEN INSERT *
    """
```

### Pattern 3: Multi-Type Watermark Manager
```python
def get_next_watermark(entity: str, watermark_type: str) -> Any:
    last_value = spark.sql(f"""
        SELECT watermark_value 
        FROM ctrl.ingestion_watermarks 
        WHERE entity = '{entity}'
    """).first()[0]
    
    if watermark_type == 'int':
        return int(last_value) + 1
    elif watermark_type == 'timestamp':
        return datetime.fromisoformat(last_value) + timedelta(hours=1)
    elif watermark_type == 'date':
        return (datetime.fromisoformat(last_value) + timedelta(days=1)).date()
```

---

## 🔐 Security & Governance

### Implemented Controls
- ✅ **Secrets Management**: All credentials stored in Azure Key Vault (no hardcoded passwords)
- ✅ **Service Principal Authentication**: Automated pipelines use managed identities
- ✅ **Data Encryption**: At rest (Delta Lake) and in transit (TLS 1.2+)
- ✅ **Row-Level Security (RLS)**: Filter data access based on user context
- ✅ **Column-Level Security (CLS)**: Hide/mask sensitive columns (SSN, salary)
- ✅ **Workspace Isolation**: Separate environments (dev/prod) with RBAC
- ✅ **Audit Logging**: Complete activity tracking for compliance
- ✅ **Data Classification**: Automated PII detection and tagging

### Multi-Layer Security Model
1. **Workspace-Level**: Admin, Member, Contributor, Viewer roles
2. **Item-Level**: Lakehouse permissions (Read, ReadAll, Write)
3. **Data-Level**: RLS, CLS, dynamic data masking
4. **Network-Level**: VNet integration, private endpoints

### Compliance Features
- **GDPR Compliance**: Right to access, right to erasure procedures
- **SOX Compliance**: Segregation of duties, change audit trail
- **Data Retention**: Automated lifecycle management (Bronze: 90d, Silver: 365d, Gold: 7y)
- **Incident Response**: Documented procedures for credential compromise and unauthorized access

📖 **[Complete Security Documentation](docs/07_SECURITY_GOVERNANCE.md)** - Comprehensive guide covering:
- Azure Key Vault integration
- Workspace, item, and lakehouse security
- Row-level and column-level security implementation
- Data masking and encryption
- Compliance reporting (GDPR, SOX)
- Security best practices and incident response

---

## 📊 Sample Output

### Gold Fact Table (fact_payroll_earnings)
```
+-------------+-------------+--------------+-------------+--------+---------------+
|EMPLOYEE_KEY |LOCATION_KEY |PAY_PERIOD_ID |EARNING_CODE |AMOUNT  |INGESTION_DATE |
+-------------+-------------+--------------+-------------+--------+---------------+
|195          |42           |26            |REG          |3200.00 |2026-04-15     |
|196          |42           |26            |OT           |450.75  |2026-04-15     |
|197          |51           |26            |REG          |2850.00 |2026-04-15     |
+-------------+-------------+--------------+-------------+--------+---------------+
```

---

## 🧪 Testing Strategy

### Implemented Tests
1. **Bronze Layer**: API response validation, partition integrity
2. **Silver Layer**: Schema conformance, duplicate detection
3. **Gold Layer**: 
   - Referential integrity (FK → PK validation)
   - Surrogate key uniqueness
   - Watermark consistency
   - Fact table grain validation

### CI/CD Readiness
- Notebooks structured for parameterization
- Idempotent design (safe to re-run)
- Comprehensive logging for troubleshooting
- Execution time tracking for SLA monitoring

---

## 📚 Documentation

Detailed documentation available in `/docs`:

1. [Architecture Design Decisions](docs/01_ARCHITECTURE.md)
2. [Bronze Layer Implementation](docs/02_BRONZE_LAYER.md)
3. [Silver Layer Transformations](docs/03_SILVER_LAYER.md)
4. [Gold Layer Dimensional Modeling](docs/04_GOLD_LAYER.md)
5. [Control Tables Framework](docs/05_CONTROL_TABLES.md)
6. [Orchestration & Scheduling](docs/06_ORCHESTRATION.md)
7. [Security & Governance](docs/07_SECURITY_GOVERNANCE.md)

---

## 🎓 Key Learnings & Best Practices

### What Worked Well
✅ **Metadata-driven design**: Made the pipeline extensible without code changes  
✅ **Dual partition strategy**: Solved the mixed workload problem elegantly  
✅ **Surrogate key preservation**: MERGE pattern prevented FK breakage  
✅ **Comprehensive logging**: Enabled quick troubleshooting in production  

### Production Challenges Solved
⚠️ **API rate limiting**: Implemented intelligent pacing to stay under quota  
⚠️ **Token expiration**: Built auto-renewal logic to handle long-running jobs  
⚠️ **Schema drift**: Dynamic column detection prevented ingestion failures  
⚠️ **Partition explosion**: Carefully designed partition strategy to avoid small files  

### If I Were to Do It Again
🔄 **Add streaming ingestion** for real-time facts (current: batch bi-weekly)  
🔄 **Implement SCD Type 2** for critical dimensions (current: Type 1 overwrite)  
🔄 **Add dbt for Gold transformations** (current: pure PySpark)  
🔄 **Enhance data quality framework** with Great Expectations integration  

---

## 🤝 Contributing

This is a portfolio project demonstrating production-grade patterns. For questions or discussions about the architecture:

- **Architecture Deep-Dives**: Review `/docs` folder
- **Code Walkthroughs**: See inline comments in notebooks
- **Pattern Reuse**: All code is structured for adaptation to other domains

---

## 📄 License

This project is provided as a **learning resource and portfolio piece**. The patterns and architectures demonstrated are based on industry best practices and can be adapted for commercial use.

---

## 🙏 Acknowledgments

**Technologies & Frameworks:**
- Microsoft Fabric team for the unified data platform
- Delta Lake contributors for ACID transactions on data lakes
- Apache Spark community for distributed processing excellence

**Architectural Influences:**
- Databricks Medallion Architecture pattern
- Kimball dimensional modeling methodology
- Azure Well-Architected Framework principles

---

## 📞 Contact
#### Idia Herrera
#### idia.herrera@gmail.com

**For architectural or implementation questions**, please review the extensive documentation in this repository. The design decisions and patterns are thoroughly documented to serve as a reference implementation.

---

**⭐ If you find these patterns valuable for your own data engineering work, consider starring this repository!**

Built with ❤️ using Microsoft Fabric & Delta Lake
