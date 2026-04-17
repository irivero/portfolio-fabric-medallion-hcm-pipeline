# Architecture Diagrams

## End-to-End Data Flow

```mermaid
graph TB
    subgraph "Source System"
        API[HCM REST API<br/>OAuth2 Protected<br/>Rate Limited: 48 calls/hour]
    end
    
    subgraph "Bronze Layer - Raw Persistence"
        BRONZE[(Delta Table<br/>hcm_api_extracts<br/>Schema: JSON strings<br/>Partitions: FULL_LOAD + 01-26)]
        WM1[Watermarks<br/>Control Table]
    end
    
    subgraph "Silver Layer - Typed Data"
        SILVER_DIM[(dim_employee<br/>dim_location<br/>Full Load)]
        SILVER_FACT[(fact_earnings<br/>fact_deductions<br/>fact_taxes<br/>Incremental)]
    end
    
    subgraph "Gold Layer - Star Schema"
        GOLD_DIM[(dwh_dim_employee<br/>dwh_dim_location<br/>Surrogate Keys)]
        GOLD_FACT[(fact_payroll_earnings<br/>fact_payroll_deductions<br/>fact_payroll_taxes<br/>FK Enriched)]
    end
    
    subgraph "Control Layer"
        CATALOG[entity_catalog<br/>gold_catalog]
        EXEC_LOG[execution_log]
        WM[watermarks]
    end
    
    API -->|REST GET<br/>JSON Payload| BRONZE
    BRONZE -->|Update| WM1
    BRONZE -->|Parse JSON<br/>Type Inference| SILVER_DIM
    BRONZE -->|Parse JSON<br/>Type Inference| SILVER_FACT
    
    SILVER_DIM -->|MERGE<br/>Preserve SK| GOLD_DIM
    SILVER_FACT -->|JOIN + Enrich<br/>Watermark Based| GOLD_FACT
    
    GOLD_DIM -.->|Lookup| GOLD_FACT
    
    CATALOG -.->|Config| BRONZE
    CATALOG -.->|Config| GOLD_DIM
    CATALOG -.->|Config| GOLD_FACT
    WM -.->|Track| EXEC_LOG
    
    classDef source fill:#e1f5ff
    classDef bronze fill:#cd7f32,color:#fff
    classDef silver fill:#c0c0c0
    classDef gold fill:#ffd700
    classDef control fill:#90ee90
    
    class API source
    class BRONZE,WM1 bronze
    class SILVER_DIM,SILVER_FACT silver
    class GOLD_DIM,GOLD_FACT gold
    class CATALOG,EXEC_LOG,WM control
```

---

## Dual Partition Strategy (Bronze Layer)

```mermaid
graph LR
    subgraph "Bronze Table Structure"
        ROOT[bronze.hcm_api_extracts]
        
        ROOT --> PF[partition_column=FULL_LOAD]
        ROOT --> P1[partition_column=01]
        ROOT --> P25[partition_column=25]
        ROOT --> P26[partition_column=26]
        
        PF --> EMP[entity=dim_employee]
        PF --> LOC[entity=dim_location]
        
        P26 --> EARN[entity=fact_earnings]
        P26 --> DED[entity=fact_deductions]
        P26 --> TAX[entity=fact_taxes]
    end
    
    subgraph "Load Patterns"
        FULL[Full Load<br/>partition=FULL_LOAD<br/>Auto-replace on each run]
        INC[Incremental<br/>partition=01-26<br/>Retain historical]
    end
    
    EMP -.->|Pattern| FULL
    LOC -.->|Pattern| FULL
    EARN -.->|Pattern| INC
    DED -.->|Pattern| INC
    TAX -.->|Pattern| INC
    
    classDef full fill:#ffcccc
    classDef inc fill:#ccffcc
    
    class EMP,LOC,FULL full
    class EARN,DED,TAX,INC inc
```

**Benefits**:
- ✅ Single unified table (no *_full vs *_incremental split)
- ✅ Self-documenting (partition value = load type)
- ✅ Automatic cleanup (FULL_LOAD overwrites)
- ✅ Historical retention (periods 01-26 preserved)

---

## Surrogate Key Preservation (Gold Dimensions)

```mermaid
sequenceDiagram
    participant Silver as Silver Layer
    participant Staging as Staging View
    participant Gold as Gold Dimension
    
    Note over Silver: Daily Full Load<br/>NEW employees added<br/>EXISTING employees updated
    
    Silver->>Staging: Read all current employees
    Staging->>Staging: Assign temporary surrogate keys
    
    Note over Gold: Existing table with<br/>STABLE surrogate keys
    
    Staging->>Gold: MERGE INTO
    
    alt Employee Exists (MATCHED)
        Gold->>Gold: UPDATE attributes ONLY<br/>PRESERVE existing surrogate key
        Note right of Gold: EMPLOYEE_KEY unchanged<br/>FK relationships intact
    else Employee New (NOT MATCHED)
        Gold->>Gold: INSERT with NEW surrogate key
        Note right of Gold: EMPLOYEE_KEY = MAX + 1<br/>New FK available for facts
    end
    
    Note over Gold: Result: Stable keys<br/>No FK breakage
```

**Critical Pattern**: Without MERGE, full overwrite would regenerate ALL keys → facts lose referential integrity

---

## Incremental Fact Loading with Watermarks

```mermaid
stateDiagram-v2
    [*] --> CheckWatermark
    
    CheckWatermark --> ReadIncremental: last_period = 25
    ReadIncremental --> EnrichFKs: Filter period > 25
    EnrichFKs --> ValidateQuality: Join dimensions
    ValidateQuality --> WriteGold: Check nulls, orphans
    WriteGold --> UpdateWatermark: APPEND mode
    UpdateWatermark --> LogSuccess: watermark = 26
    LogSuccess --> [*]: COMMIT
    
    CheckWatermark --> Skip: No new data
    Skip --> [*]
    
    ValidateQuality --> LogFailure: Quality checks fail
    LogFailure --> [*]: ROLLBACK
    
    note right of CheckWatermark
        Query: ctrl.execution_log
        Get: MAX(watermark_value)
        WHERE status = 'SUCCESS'
    end note
    
    note right of EnrichFKs
        Broadcast Join:
        - dim_employee → EMPLOYEE_KEY
        - dim_location → LOCATION_KEY
    end note
```

**Idempotency**: Running period 26 multiple times → same result (watermark doesn't advance past 26)

---

## Control Table Dependencies

```mermaid
erDiagram
    ENTITY_CATALOG ||--o{ BRONZE_TABLE : configures
    ENTITY_CATALOG {
        string entity_name PK
        string api_endpoint
        string load_type
        boolean is_active
    }
    
    BRONZE_TABLE ||--o{ INGESTION_WATERMARKS : tracks
    BRONZE_TABLE {
        string partition_column
        string entity_name
        string payload_json
    }
    
    INGESTION_WATERMARKS {
        string entity_name PK
        string last_processed_value
        timestamp last_ingestion_time
    }
    
    GOLD_CATALOG ||--o{ GOLD_DIMENSION : defines
    GOLD_CATALOG ||--o{ GOLD_FACT : defines
    GOLD_CATALOG {
        string table_name PK
        string table_type
        string business_key
        string surrogate_key_name
        string watermark_column
    }
    
    GOLD_CATALOG ||--o{ GOLD_UPDATE_COLUMNS : has
    GOLD_UPDATE_COLUMNS {
        string table_name FK
        string column_name
        int sort_order
    }
    
    GOLD_CATALOG ||--o{ GOLD_ZORDER_COLUMNS : defines
    GOLD_ZORDER_COLUMNS {
        string table_name FK
        string column_name
    }
    
    GOLD_FACT ||--o{ EXECUTION_LOG : logs
    EXECUTION_LOG {
        string execution_id PK
        string table_name
        string watermark_value
        string status
    }
```

**Pattern**: All pipeline behavior driven by control tables, not hardcoded configurations

---

## Orchestration Flow

```mermaid
flowchart TD
    START([Start Pipeline]) --> LOAD_CATALOG[Load Entity Catalog]
    
    LOAD_CATALOG --> BRONZE{Bronze Layer<br/>Ingestion}
    
    BRONZE --> |For each entity| API_CALL[Call REST API]
    API_CALL --> RATE_LIMIT{Rate Limit<br/>Exceeded?}
    RATE_LIMIT -->|Yes| WAIT[Sleep until quota resets]
    WAIT --> API_CALL
    RATE_LIMIT -->|No| PARSE[Serialize to JSON]
    PARSE --> WRITE_BRONZE[Write to Delta<br/>replaceWhere partition]
    WRITE_BRONZE --> UPDATE_WM1[Update Watermark]
    UPDATE_WM1 --> CHECK_MORE{More<br/>entities?}
    CHECK_MORE -->|Yes| API_CALL
    CHECK_MORE -->|No| SILVER
    
    SILVER{Silver Layer<br/>Transformation} --> PARSE_JSON[Parse JSON to Typed]
    PARSE_JSON --> EVOLVE[Detect New Columns]
    EVOLVE --> QUALITY[Apply Quality Rules]
    QUALITY --> BIZ_LOGIC[Business Logic]
    BIZ_LOGIC --> WRITE_SILVER[Write to Silver]
    
    WRITE_SILVER --> GOLD
    
    GOLD{Gold Layer<br/>Load} --> DIMS[Load Dimensions<br/>MERGE Strategy]
    DIMS --> FACTS[Load Facts<br/>Incremental + FK Enrich]
    FACTS --> OPTIMIZE[OPTIMIZE + Z-ORDER]
    OPTIMIZE --> VALIDATE[Validate FK Integrity]
    
    VALIDATE -->|Pass| SUCCESS([Success])
    VALIDATE -->|Fail| ERROR([Error - Rollback])
    
    API_CALL -.->|Retry on 401| TOKEN[Refresh OAuth Token]
    TOKEN -.-> API_CALL
    
    style BRONZE fill:#cd7f32,color:#fff
    style SILVER fill:#c0c0c0
    style GOLD fill:#ffd700
    style SUCCESS fill:#90ee90
    style ERROR fill:#ff6b6b,color:#fff
```

---

## Storage Layout

```
📦 OneLake (Delta Lake Format)
│
├── 🥉 bronze/
│   └── sens/
│       └── hcm_api_extracts/
│           ├── partition_column=FULL_LOAD/
│           │   ├── entity_name=dim_employee/
│           │   │   └── part-00000-*.snappy.parquet  (latest snapshot)
│           │   └── entity_name=dim_location/
│           │       └── part-00000-*.snappy.parquet
│           ├── partition_column=25/
│           │   ├── entity_name=fact_earnings/
│           │   ├── entity_name=fact_deductions/
│           │   └── entity_name=fact_taxes/
│           └── partition_column=26/  (current period)
│               └── ...
│
├── 🥈 silver/
│   └── sens/
│       ├── dim_employee/  (no partitions)
│       ├── dim_location/  (no partitions)
│       ├── fact_earnings/
│       │   ├── pay_period_id=25/
│       │   └── pay_period_id=26/
│       ├── fact_deductions/
│       └── fact_taxes/
│
└── 🥇 gold/
    └── dbo/
        ├── dwh_dim_employee/  (optimized, Z-ORDERed)
        ├── dwh_dim_location/
        ├── fact_payroll_earnings/  (Z-ORDER: EMPLOYEE_KEY, PAY_PERIOD_ID)
        ├── fact_payroll_deductions/
        └── fact_payroll_taxes/
```

---

## Technology Stack

```mermaid
graph BT
    subgraph "Consumption Layer"
        POWERBI[Power BI]
        EXCEL[Excel]
        PYTHON[Python/Pandas]
    end
    
    subgraph "Data Platform"
        GOLD[Gold Layer<br/>Star Schema]
        SILVER[Silver Layer<br/>Typed Data]
        BRONZE[Bronze Layer<br/>Raw JSON]
    end
    
    subgraph "Compute & Storage"
        SPARK[Apache Spark 3.5<br/>PySpark]
        DELTA[Delta Lake<br/>ACID Transactions]
        ONELAKE[OneLake Storage<br/>Parquet Format]
    end
    
    subgraph "Infrastructure"
        FABRIC[Microsoft Fabric<br/>Unified Platform]
        KEYVAULT[Azure Key Vault<br/>Secrets Management]
    end
    
    BRONZE --> DELTA
    SILVER --> DELTA
    GOLD --> DELTA
    
    DELTA --> ONELAKE
    DELTA --> SPARK
    
    SPARK --> FABRIC
    ONELAKE --> FABRIC
    
    FABRIC --> KEYVAULT
    
    GOLD --> POWERBI
    GOLD --> EXCEL
    GOLD --> PYTHON
    
    classDef layer fill:#4a90e2,color:#fff
    classDef tech fill:#50e3c2
    classDef infra fill:#f5a623
    
    class BRONZE,SILVER,GOLD layer
    class SPARK,DELTA,ONELAKE tech
    class FABRIC,KEYVAULT infra
```

---

## Query Optimization - Z-ORDER Impact

```mermaid
graph LR
    subgraph "Before Z-ORDER"
        Q1[Query: EMPLOYEE_KEY = 195] --> SCAN1[Full Table Scan<br/>1.3M records<br/>450 files read<br/>Duration: 12 sec]
    end
    
    subgraph "After Z-ORDER"
        Q2[Query: EMPLOYEE_KEY = 195] --> SKIP[Data Skipping<br/>18 files read<br/>~50K records scanned<br/>Duration: 3 sec]
    end
    
    subgraph "Z-ORDER Clustering"
        direction TB
        FILES[Delta Files] --> CLUSTER[Co-locate rows<br/>with same EMPLOYEE_KEY]
        CLUSTER --> META[File-level stats<br/>min/max per column]
    end
    
    META -.->|Enable| SKIP
    
    style SCAN1 fill:#ff6b6b,color:#fff
    style SKIP fill:#90ee90
```

**Command**: `OPTIMIZE gold.fact_payroll_earnings ZORDER BY (EMPLOYEE_KEY, PAY_PERIOD_ID)`

**Result**: 4x query performance improvement on filtered queries

---

These diagrams illustrate:
1. **Data Flow**: End-to-end architecture
2. **Partitioning**: Dual partition strategy benefits
3. **MERGE Pattern**: Surrogate key preservation
4. **Watermarks**: Incremental processing logic
5. **Control Tables**: Metadata-driven configuration
6. **Orchestration**: Complete pipeline workflow
7. **Storage**: Physical layout on OneLake
8. **Technology**: Complete stack visualization
9. **Optimization**: Z-ORDER performance impact

For interactive diagrams, copy markdown code blocks to [Mermaid Live Editor](https://mermaid.live/).
