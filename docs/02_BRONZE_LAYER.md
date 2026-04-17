# Bronze Layer - Raw Data Ingestion

## Overview

The Bronze layer serves as the **raw data landing zone**, implementing an **immutable audit log** pattern. All source data is persisted exactly as received from the HCM REST API without transformation, enabling replay and audit capabilities.

---

## Table of Contents
1. [Architecture](#architecture)
2. [Data Model](#data-model)
3. [Ingestion Process](#ingestion-process)
4. [API Integration](#api-integration)
5. [Partition Strategy](#partition-strategy)
6. [Watermark Management](#watermark-management)
7. [Error Handling](#error-handling)
8. [Code Examples](#code-examples)

---

## Architecture

```
┌──────────────────────────────────────────────┐
│          HCM REST API                        │
│  - OAuth2 Authentication                     │
│  - Rate Limit: 48 calls/hour                 │
│  - Pagination: 500 records/page              │
└────────────────┬─────────────────────────────┘
                 │
                 │ HTTPS GET Requests
                 │ Bearer Token Authentication
                 ▼
┌──────────────────────────────────────────────┐
│     Bronze Ingestion Notebook                │
│                                              │
│  1. Load entity catalog from control table  │
│  2. Get/refresh OAuth2 token                │
│  3. Determine incremental range (watermark) │
│  4. Fetch paginated API data                │
│  5. Serialize to JSON strings               │
│  6. Write to Delta Lake (partitioned)       │
│  7. Update watermarks                       │
└────────────────┬─────────────────────────────┘
                 │
                 │ Delta Lake ACID Write
                 ▼
┌──────────────────────────────────────────────┐
│   Delta Table: bronze.hcm_api_extracts       │
│   Partitions: [partition_column, entity]     │
│   Format: JSON strings (schema-agnostic)     │
│   Retention: 90 days                         │
└──────────────────────────────────────────────┘
```

---

## Data Model

### Table Schema

```sql
CREATE TABLE bronze.sens.hcm_api_extracts (
    -- Identifiers
    ingestion_id           STRING NOT NULL,    -- UUID per ingestion run
    entity_name            STRING NOT NULL,    -- Entity type (e.g., 'fact_earnings')
    partition_column       STRING NOT NULL,    -- 'FULL_LOAD' or period ID ('01'-'26')
    
    -- Raw Payload
    payload_json           STRING,             -- Raw API response (JSON string)
    record_count           INT,                -- Number of records in this payload
    
    -- Technical Metadata
    api_call_timestamp     TIMESTAMP,          -- When API request was made
    ingestion_datetime     TIMESTAMP,          -- When data landed in Bronze
    source_system          STRING,             -- Always 'hcm_api'
    api_endpoint           STRING,             -- API URL called
    api_http_status        INT                 -- HTTP response code (200, 401, etc.)
)
PARTITIONED BY (partition_column, entity_name)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
```

### Partition Strategy Explained

**Challenge**: Mixed workload with different retention needs

| Entity Type | Load Pattern | Retention Requirement |
|------------|--------------|----------------------|
| **Dimensions** (Employee, Location) | Full snapshot daily | Only latest needed (auto-cleanup) |
| **Facts** (Earnings, Deductions, Taxes) | Incremental by pay period | All historical periods (archive) |

**Solution**: Dual Partition Pattern

```python
# Dimension entities → "FULL_LOAD" partition
partition_value = "FULL_LOAD"  # Constant sentinel value
# Each write replaces this partition entirely

# Fact entities → Period ID partition
partition_value = str(pay_period_id).zfill(2)  # "01", "02", ..., "26"
# Each period is separate, historical periods retained
```

**Benefits**:
- ✅ **Single unified table** (no need for `*_full` + `*_incremental` tables)
- ✅ **Self-documenting** structure (partition value indicates load type)
- ✅ **Automatic cleanup** (full-load overwrites previous snapshot)
- ✅ **Historical retention** (incremental partitions preserved)

**Storage Layout Example**:
```
bronze/sens/hcm_api_extracts/
├── partition_column=FULL_LOAD/
│   ├── entity_name=dim_employee/         ← Always latest snapshot
│   └── entity_name=dim_location/         ← Auto-replaces on each run
├── partition_column=01/
│   ├── entity_name=fact_earnings/        ← Historical period 1
│   ├── entity_name=fact_deductions/
│   └── entity_name=fact_taxes/
├── partition_column=25/                   ← Historical period 25
│   └── ...
└── partition_column=26/                   ← Current period
    └── ...
```

---

## Ingestion Process

### Workflow Steps

```
┌─────────────────────────────────────────────────────────────┐
│ Step 1: Load Entity Catalog                                 │
│  • Read ctrl.entity_catalog                                 │
│  • Filter is_active = TRUE                                  │
│  • Get: entity_name, api_endpoint, load_type, periods       │
└────────────┬────────────────────────────────────────────────┘
             │
┌────────────▼────────────────────────────────────────────────┐
│ Step 2: OAuth2 Token Acquisition                            │
│  • Read credentials from Azure Key Vault                    │
│  • Request token from OAuth2 server                         │
│  • Store token + expiry time in memory                      │
│  • Implement auto-renewal (detect 401, refresh, retry)      │
└────────────┬────────────────────────────────────────────────┘
             │
┌────────────▼───────────────────────────────────────────────┐
│ Step 3: Watermark Lookup                                   │
│  • Query ctrl.ingestion_watermarks                         │
│  • Get last processed value per entity                     │
│  • Determine incremental range:                            │
│    - FULL_LOAD: No range (fetch all)                       │
│    - INCREMENTAL: last_watermark + 1 to current_period     │
└────────────┬───────────────────────────────────────────────┘
             │
┌────────────▼───────────────────────────────────────────────┐
│ Step 4: API Data Extraction (Paginated)                    │
│  FOR EACH entity IN catalog:                               │
│    FOR EACH period IN range:                               │
│      page_number = 1                                       │
│      WHILE has_more_data:                                  │
│        • Call API (rate limiter check)                     │
│        • Collect JSON response                             │
│        • Append to batch buffer                            │
│        • page_number += 1                                  │
│      END WHILE                                             │
│    END FOR                                                 │
│  END FOR                                                   │
└────────────┬───────────────────────────────────────────────┘
             │
┌────────────▼───────────────────────────────────────────────┐
│ Step 5: Delta Lake Write                                   │
│  • Create DataFrame with JSON strings                      │
│  • Mode: OVERWRITE with replaceWhere predicate             │
│  • Partition: by partition_column + entity_name            │
│  • Atomic write (ACID guarantee)                           │
└────────────┬───────────────────────────────────────────────┘
             │
┌────────────▼───────────────────────────────────────────────┐
│ Step 6: Watermark Update                                   │
│  • MERGE INTO ctrl.ingestion_watermarks                    │
│  • Update last_processed_value for entity                  │
│  • Record ingestion metadata (timestamp, record_count)     │
└────────────────────────────────────────────────────────────┘
```

---

## API Integration

### OAuth2 Token Management

**Pattern**: Token caching with automatic renewal

```python
class OAuth2TokenManager:
    """Manages OAuth2 token lifecycle with auto-renewal"""
    
    def __init__(self, token_url: str, client_id: str):
        self.token_url = token_url
        self.client_id = client_id
        self.access_token = None
        self.token_expiry = None
    
    def get_valid_token(self) -> str:
        """Returns valid token, refreshing if expired"""
        if self._is_token_expired():
            self._refresh_token()
        return self.access_token
    
    def _is_token_expired(self) -> bool:
        """Check if token is expired or about to expire (60s buffer)"""
        if self.access_token is None:
            return True
        
        buffer_seconds = 60
        return datetime.now() >= (self.token_expiry - timedelta(seconds=buffer_seconds))
    
    def _refresh_token(self):
        """Fetch new token from OAuth2 server"""
        # Retrieve credentials from Azure Key Vault
        username = notebookutils.credentials.getSecret(
            'https://keyvault.vault.azure.net',
            'hcm-api-username'
        )
        password = notebookutils.credentials.getSecret(
            'https://keyvault.vault.azure.net',
            'hcm-api-password'
        )
        
        # Request token
        response = requests.post(
            self.token_url,
            data={
                'grant_type': 'password',
                'client_id': self.client_id,
                'username': username,
                'password': password
            },
            timeout=30
        )
        
        response.raise_for_status()
        token_data = response.json()
        
        self.access_token = token_data['access_token']
        expires_in = token_data.get('expires_in', 3600)
        self.token_expiry = datetime.now() + timedelta(seconds=expires_in)
        
        logger.info(f"✅ OAuth2 token refreshed (expires in {expires_in}s)")
```

**Auto-Renewal on 401 Errors**:
```python
def call_api_with_retry(url: str, token_manager: OAuth2TokenManager) -> dict:
    """API call with automatic token renewal on auth failure"""
    
    max_retries = 3
    for attempt in range(max_retries):
        token = token_manager.get_valid_token()
        headers = {'Authorization': f'Bearer {token}'}
        
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 401:  # Unauthorized
            logger.warning(f"Token expired, refreshing... (attempt {attempt + 1})")
            token_manager._refresh_token()  # Force refresh
            continue  # Retry with new token
        
        response.raise_for_status()
        return response.json()
    
    raise RuntimeError("Failed to authenticate after 3 retries")
```

### Rate Limiting

**Pattern**: Sliding window with sleep-based throttling

```python
class APIRateLimiter:
    """Enforces API rate limits to prevent quota exhaustion"""
    
    def __init__(self, max_calls_per_hour: int = 48):
        self.max_calls = max_calls_per_hour
        self.call_timestamps = []
    
    def wait_if_needed(self):
        """Sleep if rate limit would be exceeded"""
        now = datetime.now()
        
        # Remove calls older than 1 hour
        cutoff = now - timedelta(hours=1)
        self.call_timestamps = [ts for ts in self.call_timestamps if ts > cutoff]
        
        # Check if limit reached
        if len(self.call_timestamps) >= self.max_calls:
            oldest_call = min(self.call_timestamps)
            wait_until = oldest_call + timedelta(hours=1)
            sleep_seconds = (wait_until - now).total_seconds()
            
            if sleep_seconds > 0:
                logger.warning(f"⏱️ Rate limit reached. Sleeping {sleep_seconds:.0f}s...")
                time.sleep(sleep_seconds)
                
                # Clear old timestamps after sleep
                now = datetime.now()
                cutoff = now - timedelta(hours=1)
                self.call_timestamps = [ts for ts in self.call_timestamps if ts > cutoff]
        
        # Record this call
        self.call_timestamps.append(now)

# Usage
rate_limiter = APIRateLimiter(max_calls_per_hour=48)

for entity in entities:
    for period in periods:
        rate_limiter.wait_if_needed()  # Blocks if needed
        data = call_api(entity, period)
```

### Pagination

**Pattern**: Cursor-based iteration with empty page detection

```python
def fetch_paginated_data(
    api_url: str,
    headers: dict,
    entity_name: str,
    period_id: int,
    page_size: int = 500
) -> List[Dict]:
    """Fetch all pages for entity-period combination"""
    
    all_records = []
    page_number = 1
    
    while True:
        params = {
            'entity': entity_name,
            'periodId': period_id,
            'pageNumber': page_number,
            'pageSize': page_size
        }
        
        logger.debug(f"Fetching {entity_name} period {period_id} page {page_number}")
        
        response = requests.get(api_url, headers=headers, params=params, timeout=60)
        response.raise_for_status()
        
        page_data = response.json()
        
        # Check if empty (no more data)
        if not page_data or len(page_data) == 0:
            logger.info(f"✅ {entity_name} period {period_id}: {len(all_records)} total records")
            break
        
        all_records.extend(page_data)
        page_number += 1
        
        # Safety check: limit to 1000 pages (500 × 1000 = 500K records max)
        if page_number > 1000:
            logger.error(f"⚠️ Exceeded max pages (1000) for {entity_name} period {period_id}")
            break
    
    return all_records
```

---

## Partition Strategy

### Partition Routing Logic

```python
def determine_partition_value(entity_config: Dict, current_period: int) -> str:
    """
    Route entity to appropriate partition based on load type
    
    Args:
        entity_config: Entity configuration from control table
        current_period: Current pay period ID (1-26)
    
    Returns:
        Partition value: "FULL_LOAD" or zero-padded period ID
    """
    load_type = entity_config['load_type']
    
    if load_type == 'FULL_LOAD':
        return "FULL_LOAD"  # Sentinel value for dimensions
    elif load_type == 'INCREMENTAL':
        return str(current_period).zfill(2)  # "01", "02", ..., "26"
    else:
        raise ValueError(f"Unknown load_type: {load_type}")

# Example usage
entities = [
    {'name': 'dim_employee', 'load_type': 'FULL_LOAD'},
    {'name': 'fact_earnings', 'load_type': 'INCREMENTAL'},
]

for entity in entities:
    partition_val = determine_partition_value(entity, current_period=26)
    print(f"{entity['name']}: partition={partition_val}")

# Output:
# dim_employee: partition=FULL_LOAD
# fact_earnings: partition=26
```

### Atomic Partition Replacement

```python
def write_to_bronze(
    df: DataFrame,
    entity_name: str,
    partition_value: str,
    table_name: str = "bronze.sens.hcm_api_extracts"
):
    """
    Write data to Bronze with atomic partition replacement
    
    Delta Lake guarantees:
    - All-or-nothing write (no partial states)
    - Partition-level atomicity with replaceWhere
    - ACID compliance (concurrent readers see consistent snapshot)
    """
    
    # Build replaceWhere predicate for atomic partition swap
    replace_predicate = (
        f"partition_column = '{partition_value}' "
        f"AND entity_name = '{entity_name}'"
    )
    
    logger.info(f"Writing to Bronze: {entity_name} (partition={partition_value})")
    logger.info(f"ReplaceWhere: {replace_predicate}")
    
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .option("replaceWhere", replace_predicate) \
      .partitionBy("partition_column", "entity_name") \
      .saveAsTable(table_name)
    
    logger.info(f"✅ Successfully wrote {df.count()} records")
```

**Idempotency Guarantee**:
```python
# Run 1: Loads period 26 earnings data
write_to_bronze(data_period_26, 'fact_earnings', '26')

# Run 2: Same period (e.g., reprocessing after failure)
# Result: Exact same partition replaced (no duplicates)
write_to_bronze(data_period_26, 'fact_earnings', '26')

# Run 3: Again (maybe testing)
# Result: Still no duplicates (idempotent)
write_to_bronze(data_period_26, 'fact_earnings', '26')
```

---

## Watermark Management

### Watermark Table Schema

```sql
CREATE TABLE ctrl.ingestion_watermarks (
    source_system         STRING,      -- 'hcm_api'
    entity_name           STRING,      -- 'fact_earnings', 'dim_employee', etc.
    watermark_column      STRING,      -- 'pay_period_id', 'effective_date', etc.
    watermark_type        STRING,      -- 'int', 'timestamp', 'date'
    last_processed_value  STRING,      -- Stored as STRING (supports all types)
    last_ingestion_time   TIMESTAMP,   -- When last successful ingestion completed
    records_ingested      BIGINT,      -- Total records in last run
    is_active             BOOLEAN,     -- Enable/disable watermark tracking
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP,
    
    PRIMARY KEY (source_system, entity_name)
);
```

### Watermark Read Logic

```python
def get_last_watermark(entity_name: str, watermark_type: str) -> Optional[Any]:
    """
    Retrieve last successfully processed watermark value
    
    Returns:
        - For 'int': Integer value
        - For 'timestamp': datetime object
        - For 'date': date object
        - None if first run
    """
    result = spark.sql(f"""
        SELECT last_processed_value
        FROM ctrl.ingestion_watermarks
        WHERE entity_name = '{entity_name}'
          AND is_active = TRUE
    """).first()
    
    if result is None:
        logger.info(f"No watermark found for {entity_name} (first run)")
        return None
    
    value_str = result['last_processed_value']
    
    # Type conversion
    if watermark_type == 'int':
        return int(value_str)
    elif watermark_type == 'timestamp':
        return datetime.fromisoformat(value_str)
    elif watermark_type == 'date':
        return datetime.fromisoformat(value_str).date()
    else:
        raise ValueError(f"Unknown watermark_type: {watermark_type}")
```

### Watermark Update Logic

```python
def update_watermark(
    entity_name: str,
    new_watermark_value: Any,
    records_ingested: int
):
    """
    Update watermark after successful ingestion using MERGE
    
    Handles:
    - First run (INSERT)
    - Subsequent runs (UPDATE)
    """
    
    # Convert value to string for storage
    if isinstance(new_watermark_value, (datetime, date)):
        value_str = new_watermark_value.isoformat()
    else:
        value_str = str(new_watermark_value)
    
    merge_sql = f"""
    MERGE INTO ctrl.ingestion_watermarks AS target
    USING (
        SELECT 
            'hcm_api' AS source_system,
            '{entity_name}' AS entity_name,
            '{value_str}' AS last_processed_value,
            CURRENT_TIMESTAMP() AS last_ingestion_time,
            {records_ingested} AS records_ingested,
            CURRENT_TIMESTAMP() AS updated_at
    ) AS source
    ON target.source_system = source.source_system
       AND target.entity_name = source.entity_name
    
    WHEN MATCHED THEN UPDATE SET
        last_processed_value = source.last_processed_value,
        last_ingestion_time = source.last_ingestion_time,
        records_ingested = source.records_ingested,
        updated_at = source.updated_at
    
    WHEN NOT MATCHED THEN INSERT *
    """
    
    spark.sql(merge_sql)
    logger.info(f"✅ Watermark updated: {entity_name} = {value_str}")
```

---

## Error Handling

### Comprehensive Error Strategy

```python
class BronzeIngestionError(Exception):
    """Base exception for Bronze layer errors"""
    pass

class APIAuthenticationError(BronzeIngestionError):
    """OAuth2 token acquisition failed"""
    pass

class APIRateLimitError(BronzeIngestionError):
    """Rate limit exceeded and cannot wait"""
    pass

class APIDataError(BronzeIngestionError):
    """API returned malformed or unexpected data"""
    pass

def ingest_entity_with_error_handling(entity_config: Dict):
    """
    Ingest single entity with comprehensive error handling
    
    Error categories:
    1. Transient (retry): Network timeouts, 503 Service Unavailable
    2. Auth (refresh token): 401 Unauthorized
    3. Fatal (abort): 404 Not Found, 500 Internal Server Error
    """
    
    max_retries = 3
    backoff_seconds = 60
    
    for attempt in range(max_retries):
        try:
            # Main ingestion logic
            data = fetch_api_data(entity_config)
            df = create_bronze_dataframe(data)
            write_to_bronze(df, entity_config['name'], partition_value)
            update_watermark(entity_config['name'], max_watermark)
            
            logger.info(f"✅ Successfully ingested {entity_config['name']}")
            return  # Success
        
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout on attempt {attempt + 1}/{max_retries}")
            if attempt < max_retries - 1:
                time.sleep(backoff_seconds * (attempt + 1))  # Exponential backoff
                continue
            else:
                raise BronzeIngestionError(f"Failed after {max_retries} retries")
        
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logger.warning("401 Unauthorized - refreshing token")
                token_manager._refresh_token()
                continue  # Retry with new token
            
            elif e.response.status_code == 429:
                logger.error("429 Too Many Requests - rate limit exceeded")
                raise APIRateLimitError("Exceeded API quota")
            
            elif e.response.status_code in [404, 500, 502, 503]:
                logger.error(f"HTTP {e.response.status_code} - fatal error")
                raise BronzeIngestionError(f"API returned {e.response.status_code}")
        
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise BronzeIngestionError(f"Ingestion failed: {str(e)}")
```

### Partial Failure Recovery

```python
def ingest_all_entities_with_resilience(entity_catalog: List[Dict]):
    """
    Ingest multiple entities with per-entity error isolation
    
    Strategy:
    - Continue processing other entities if one fails
    - Log all failures for review
    - Return summary of successes/failures
    """
    
    results = {
        'success': [],
        'failed': []
    }
    
    for entity in entity_catalog:
        try:
            ingest_entity_with_error_handling(entity)
            results['success'].append(entity['name'])
        
        except BronzeIngestionError as e:
            logger.error(f"❌ Failed to ingest {entity['name']}: {str(e)}")
            results['failed'].append({
                'entity': entity['name'],
                'error': str(e)
            })
            # Continue with next entity (don't abort entire job)
    
    # Summary logging
    logger.info(f"""
    ╔═══════════════════════════════════════════╗
    ║       BRONZE INGESTION SUMMARY            ║
    ╠═══════════════════════════════════════════╣
    ║ ✅ Successful:  {len(results['success']):2d}                      ║
    ║ ❌ Failed:      {len(results['failed']):2d}                      ║
    ╚═══════════════════════════════════════════╝
    """)
    
    if results['failed']:
        logger.error("Failed entities:")
        for failure in results['failed']:
            logger.error(f"  - {failure['entity']}: {failure['error']}")
    
    return results
```

---

## Code Examples

### Complete Ingestion Notebook (Simplified)

```python
# ========================================
# BRONZE LAYER - API INGESTION NOTEBOOK
# ========================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import requests
import json
import time

# ========================================
# CONFIGURATION
# ========================================

# API endpoints
API_BASE_URL = "https://api.hcm-system.example.com"
OAUTH_TOKEN_URL = "https://auth.hcm-system.example.com/connect/token"

# Azure Key Vault
KEYVAULT_URL = "https://your-keyvault.vault.azure.net"

# Delta tables
BRONZE_TABLE = "lh_bronze.sens.hcm_api_extracts"
CONTROL_CATALOG = "lh_control.ctrl.entity_catalog"
CONTROL_WATERMARKS = "lh_control.ctrl.ingestion_watermarks"

# API configuration
API_PAGE_SIZE = 500
MAX_CALLS_PER_HOUR = 48
CURRENT_PAY_PERIOD = 26  # Bi-weekly period (1-26)

# ========================================
# STEP 1: LOAD ENTITY CATALOG
# ========================================

print("📖 Loading entity catalog...")

catalog_df = spark.sql(f"""
    SELECT 
        entity_name,
        api_endpoint,
        load_type,
        is_active,
        start_period,
        end_period
    FROM {CONTROL_CATALOG}
    WHERE is_active = TRUE
    ORDER BY entity_name
""")

entity_catalog = catalog_df.collect()
print(f"✅ Loaded {len(entity_catalog)} active entities\n")

# ========================================
# STEP 2: INITIALIZE API CLIENTS
# ========================================

# OAuth2 Token Manager
token_manager = OAuth2TokenManager(OAUTH_TOKEN_URL, "hcm_client_id")
token_manager._refresh_token()  # Initial token fetch

# Rate Limiter
rate_limiter = APIRateLimiter(max_calls_per_hour=MAX_CALLS_PER_HOUR)

# ========================================
# STEP 3: INGEST ENTITIES
# ========================================

for entity_row in entity_catalog:
    entity_name = entity_row['entity_name']
    load_type = entity_row['load_type']
    
    print(f"\n{'='*60}")
    print(f"Processing: {entity_name} ({load_type})")
    print(f"{'='*60}")
    
    # Determine period range
    if load_type == 'FULL_LOAD':
        periods = [None]  # Full snapshot ( no period filter)
        partition_value = "FULL_LOAD"
    else:  # INCREMENTAL
        last_watermark = get_last_watermark(entity_name, 'int')
        start_period = (last_watermark + 1) if last_watermark else 1
        periods = range(start_period, CURRENT_PAY_PERIOD + 1)
    
    # Fetch API data
    for period in periods:
        if period is not None:
            partition_value = str(period).zfill(2)
        
        print(f"  Fetching period {period or 'ALL'}...")
        
        # Rate limiting check
        rate_limiter.wait_if_needed()
        
        # API call
        api_data = fetch_paginated_data(
            api_url=f"{API_BASE_URL}/{entity_row['api_endpoint']}",
            headers={'Authorization': f'Bearer {token_manager.get_valid_token()}'},
            entity_name=entity_name,
            period_id=period,
            page_size=API_PAGE_SIZE
        )
        
        # Create Bronze DataFrame
        bronze_data = [{
            'ingestion_id': str(uuid.uuid4()),
            'entity_name': entity_name,
            'partition_column': partition_value,
            'payload_json': json.dumps(api_data),  # Serialize to JSON string
            'record_count': len(api_data),
            'api_call_timestamp': datetime.now(),
            'ingestion_datetime': datetime.now(),
            'source_system': 'hcm_api'
        }]
        
        df = spark.createDataFrame(bronze_data)
        
        # Write to Bronze (atomic partition replacement)
        write_to_bronze(df, entity_name, partition_value, BRONZE_TABLE)
        
        # Update watermark (if incremental)
        if load_type == 'INCREMENTAL' and period is not None:
            update_watermark(entity_name, period, len(api_data))
        
        print(f"  ✅ {len(api_data)} records written (partition={partition_value})")

print("\n🎉 Bronze ingestion complete!")
```

---

## Performance Metrics

### Typical Execution Times

| Entity | Load Type | Periods | API Calls | Records | Duration |
|--------|-----------|---------|-----------|---------|----------|
| fact_earnings | INCREMENTAL | 1 period | 12 | 15,000 | 4 min |
| fact_deductions | INCREMENTAL | 1 period | 8 | 8,500 | 3 min |
| fact_taxes | INCREMENTAL | 1 period | 10 | 12,000 | 3.5 min |
| dim_employee | FULL_LOAD | N/A | 20 | 10,000 | 6 min |
| dim_location | FULL_LOAD | N/A | 2 | 150 | 1 min |
| **TOTAL (all entities, 1 period)** | | | **52** | **45,650** | **17.5 min** |
| **TOTAL (all entities, 26 periods)** | | | **~330** | **~1.3M** | **~25 min** |

### Optimization Opportunities

1. **Parallel API calls**: Currently sequential; could parallelize non-dependent entities (requires careful rate limit coordination)
2. **Compression**: Implement JSON compression before storage (reduce storage cost ~40%)
3. **Incremental-only runs**: Skip full-load dimensions when not needed (save ~7 min)

---

## Troubleshooting Guide

### Common Issues

**Issue 1: "401 Unauthorized" repeating**
- **Cause**: Invalid credentials in Key Vault
- **Solution**: Verify secrets with `notebookutils.credentials.getSecret()`

**Issue 2: "429 Too Many Requests"**
- **Cause**: Rate limiter not enforcing correctly
- **Solution**: Check `call_timestamps` list; may need to reduce `max_calls_per_hour`

**Issue 3: Partition file explosion**
- **Cause**: Too many small writes per partition
- **Solution**: Run `OPTIMIZE` command weekly:
  ```sql
  OPTIMIZE bronze.sens.hcm_api_extracts
  WHERE partition_column IN ('25', '26');
  ```

**Issue 4: Watermark not updating**
- **Cause**: Exception thrown before `update_watermark()` call
- **Solution**: Check execution logs; add try-finally block

---

## Next Steps

After Bronze ingestion completes:
1. **Verify data**: Query Bronze table to check record counts
2. **Proceed to Silver**: Run transformation notebook (Bronze → Silver)
3. **Monitor watermarks**: Ensure all entities show latest period

**See Also**:
- [Silver Layer Documentation](03_SILVER_LAYER.md)
- [Control Tables Reference](05_CONTROL_TABLES.md)
- [Orchestration Guide](06_ORCHESTRATION.md)
