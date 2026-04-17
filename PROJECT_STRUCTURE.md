# Project Structure Overview

```
📦 HCM-Medallion-Architecture-Portfolio/
│
├── 📄 README.md                          # Main portfolio overview
├── 📄 SETUP_GUIDE.md                     # Step-by-step deployment guide
├── 📄 BEST_PRACTICES.md                  # Learnings & design patterns
├── 📄 .gitignore                         # Git ignore patterns
│
├── 📁 docs/                              # Technical documentation
│   ├── 01_ARCHITECTURE.md               # Architecture design decisions
│   ├── 02_BRONZE_LAYER.md               # Bronze layer deep-dive
│   ├── 03_SILVER_LAYER.md               # Silver layer deep-dive
│   ├── 04_GOLD_LAYER.md                 # Gold layer deep-dive
│   ├── 05_CONTROL_TABLES.md             # Metadata framework
│   ├── 06_ORCHESTRATION.md              # Pipeline automation
│   ├── 07_SECURITY_GOVERNANCE.md        # Security & compliance
│   └── diagrams/
│       └── architecture_overview.md     # Mermaid diagrams
│
├── 📁 config/                            # Configuration templates
│   ├── api_config_template.json         # API connection settings
│   ├── catalog_dimensions.json          # Dimension configurations
│   └── catalog_facts.json               # Fact table configurations
│
├── 📁 sql/                               # DDL scripts
│   ├── create_control_tables.sql        # Control tables DDL
│   ├── create_bronze_tables.sql         # Bronze layer DDL
│   ├── create_silver_tables.sql         # Silver layer DDL
│   └── create_gold_tables.sql           # Gold layer DDL
│
├── 📁 notebooks/                         # Fabric notebooks (samples)
│   ├── bronze/
│   │   └── ingest_api_bronze.ipynb     # API → Bronze ingestion
│   ├── silver/
│   │   └── transform_silver.ipynb      # Bronze → Silver transformation
│   ├── gold/
│   │   ├── load_dimensions.ipynb       # MERGE-based dimension load
│   │   └── load_facts.ipynb            # Incremental fact load
│   ├── setup/
│   │   ├── setup_control_tables.ipynb  # Create metadata tables
│   │   └── populate_catalog.ipynb      # Initialize configurations
│   └── orchestration/
│       └── orchestrator.ipynb          # E2E pipeline runner
│
└── 📁 tests/                             # Data quality tests
    ├── test_bronze_ingestion.ipynb      # Bronze validation
    ├── test_silver_quality.ipynb        # Silver quality checks
    └── test_gold_integrity.ipynb        # FK integrity tests
```

## Quick Navigation

### For Recruiters / Technical Managers
Start here: [README.md](README.md) → [Architecture Design](docs/01_ARCHITECTURE.md) → [Diagrams](docs/diagrams/architecture_overview.md)

### For Data Engineers
Start here: [Setup Guide](SETUP_GUIDE.md) → Layer docs ([Bronze](docs/02_BRONZE_LAYER.md), [Silver](docs/03_SILVER_LAYER.md), [Gold](docs/04_GOLD_LAYER.md)) → [Best Practices](BEST_PRACTICES.md)

### For Hands-On Implementation
1. [Setup Guide](SETUP_GUIDE.md) - Infrastructure provisioning
2. [SQL DDL Scripts](sql/) - Create all tables
3. [Config Templates](config/) - Customize for your environment
4. [Notebooks](notebooks/) - Execute pipeline

## What Makes This Portfolio Stand Out

### 1. Production-Grade Patterns
- ✅ Metadata-driven configuration (zero hardcoded values)
- ✅ Watermark-based incremental processing
- ✅ Surrogate key preservation (MERGE pattern)
- ✅ Comprehensive error handling & retry logic

### 2. Complete Documentation
- ✅ Architecture design decisions explained
- ✅ Trade-offs and alternatives discussed
- ✅ Mermaid diagrams for visual clarity
- ✅ Troubleshooting guides included

### 3. Real-World Complexity
- ✅ Mixed incremental/full-load patterns
- ✅ API rate limiting & token renewal
- ✅ Schema evolution handling
- ✅ Multi-type watermark support

### 4. Operational Excellence
- ✅ Execution logging & auditability
- ✅ Performance optimizations (Z-ORDER, broadcast joins)
- ✅ Data quality validations
- ✅ Scalability planning (3x growth projections)

## Technologies Demonstrated

| Category | Technologies |
|----------|-------------|
| **Platform** | Microsoft Fabric, Azure Key Vault |
| **Storage** | Delta Lake, OneLake (Parquet) |
| **Compute** | Apache Spark 3.5 (PySpark) |
| **Architecture** | Medallion (Bronze/Silver/Gold) |
| **Patterns** | Star schema, SCD Type 1, metadata-driven config |
| **Integration** | REST API, OAuth2, JSON parsing |
| **Optimization** | Z-ORDER, OPTIMIZE, broadcast joins, partition pruning |

## Key Metrics

- **Processing Speed**: 59 min E2E (within 70 min SLA)
- **Data Volume**: ~1.3M records across 5 entities
- **Performance**: 2.5x query improvement with Z-ORDER
- **Storage Efficiency**: 18% reduction via OPTIMIZE + VACUUM
- **Code Reusability**: 100% metadata-driven (add entities without code changes)

## Contact & Questions

This repository serves as a **comprehensive reference implementation** of modern data engineering patterns. All code and documentation are designed to be:
- **Adaptable**: Generalizable to other domains (not HCM-specific)
- **Educational**: Explains WHY, not just WHAT
- **Production-Ready**: Includes error handling, logging, optimization

**For technical questions**, please review the extensive documentation in [docs/](docs/).

---


Built with ❤️ by a Senior Data Engineer specializing in Medallion Architectures on Microsoft Fabric
