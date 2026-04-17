# Security & Governance Framework

## Overview

This document outlines the comprehensive security implementation for the Medallion Architecture pipeline, covering authentication, authorization, data protection, and governance across Microsoft Fabric workspaces, items, and lakehouses.

---

## Table of Contents
1. [Security Architecture](#security-architecture)
2. [Implemented Security Controls](#implemented-security-controls)
3. [Workspace-Level Security](#workspace-level-security)
4. [Item-Level Security](#item-level-security)
5. [Lakehouse Security](#lakehouse-security)
6. [Data Protection](#data-protection)
7. [Compliance & Auditing](#compliance--auditing)
8. [Best Practices](#best-practices)

---

## Security Architecture

### Platform Prerequisites vs. Pipeline Implementation

> **Note**: This section distinguishes between **organizational platform security** (managed by Azure AD/IT teams) and **pipeline-specific security controls** (directly implemented in this project).

```
┌────────────────────────────────────────────────────────────────────┐
│           PLATFORM PREREQUISITES (Organizational Level)            │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  ⚙️ Azure Active Directory (AAD) / Entra ID                       │
│     • Multi-Factor Authentication (MFA) - Tenant Policy            │
│     • Conditional Access Policies                                  │
│     • Identity Governance                                          │
│                                                                    │
│  ⚙️ Microsoft Fabric Platform                                     │
│     • TLS 1.2+ Encryption in Transit                               │
│     • Encryption at Rest (Microsoft-managed keys)                  │
│     • Network Security (VNets, Private Endpoints)                  │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
                                  ▼
┌────────────────────────────────────────────────────────────────────┐
│         PIPELINE SECURITY IMPLEMENTATION (This Project) ✅        │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  Layer 1: AUTHENTICATION & CONFIGURATION                           │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ ✅ Azure Key Vault Integration (API credentials)             │  │
│  │ ✅ Variable Library (environment configuration)              │  │
│  │ ✅ OAuth2 Password Grant Flow (Dayforce API)                 │  │
│  │ ✅ OAuth2 Token Auto-Renewal (HTTP 401 detection)            │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                          ▼                                         │
│  Layer 2: WORKSPACE AUTHORIZATION                                  │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ ✅ Role-Based Access (Admin/Member/Contributor/Viewer)       │  │
│  │ ✅ Group-Based Access via Azure AD Groups                    │  │
│  │ ✅ Environment Isolation (Dev/Prod workspaces)               │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                          ▼                                         │
│  Layer 3: ITEM-LEVEL PERMISSIONS                                   │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ ✅ Lakehouse Permissions (Read/ReadAll/Write)                │  │
│  │ ✅ Notebook Execution Controls                               │  │
│  │ ✅ Pipeline Execution Restrictions                           │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                          ▼                                         │
│  Layer 4: DATA-LEVEL SECURITY                                      │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ ✅ RLS-style Filtering (via secure views)                   │  │
│  │ ✅ CLS-style Column Hiding (via selective views)            │  │
│  │ ✅ Data Masking Patterns (via CASE statements)              │  │
│  │ ✅ Schema-Level Isolation (sens vs. dbo) - NATIVE           │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                          ▼                                         │
│  Layer 5: AUDIT & COMPLIANCE                                       │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ ✅ Query Audit Logging (ctrl.audit_log table)                │  │
│  │ ✅ Data Retention Policies                                   │  │
│  │ ✅ GDPR Compliance Procedures                                │  │
│  └──────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────┘
```

**Legend**:
- ⚙️ **Platform Prerequisites**: Foundational security provided by Azure/Fabric (not configured in this pipeline)
- ✅ **Implemented Controls**: Security features directly built and configured in this project

---

## Implemented Security Controls

> **Focus**: This section details security controls **directly implemented and configured** in this pipeline project. Platform-level security (MFA, tenant encryption, network policies) is assumed as prerequisite infrastructure.

### 1. **Secrets Management with Azure Key Vault**

**Problem**: API credentials cannot be hardcoded in notebooks or configuration files.

**Solution**: Azure Key Vault integration with Fabric

**Implementation**:
```python
# Fetch credentials securely from Azure Key Vault
vault_url = "https://your-keyvault.vault.azure.net"

# Fabric automatically authenticates using workspace identity
api_username = notebookutils.credentials.getSecret(vault_url, "hcm-api-username")
api_password = notebookutils.credentials.getSecret(vault_url, "hcm-api-password")

# Credentials NEVER logged or displayed
logger.info("✅ Credentials retrieved from Key Vault")  # Safe
# logger.info(f"Username: {api_username}")  # ❌ NEVER do this
```

**Key Vault Configuration**:
```bash
# 1. Create Key Vault
az keyvault create \
  --name myproject-keyvault \
  --resource-group myproject-rg \
  --location eastus

# 2. Store secrets
az keyvault secret set \
  --vault-name myproject-keyvault \
  --name hcm-api-username \
  --value "service_account_user"

# 3. Grant Fabric workspace access
az keyvault set-policy \
  --name myproject-keyvault \
  --object-id <FABRIC_WORKSPACE_IDENTITY_ID> \
  --secret-permissions get list
```

**Benefits**:
- ✅ Centralized secret management
- ✅ Automatic secret rotation support
- ✅ Audit logging of secret access
- ✅ No credentials in code or notebooks

---

### 2. **OAuth2 Password Grant Authentication**

**Context**: External API authentication for Dayforce HCM

> **Important**: This implementation uses OAuth2 Password Grant Flow because **Dayforce API requires username/password authentication** (not Azure AD-based). Service Principal authentication (Azure AD) only applies to Azure-native APIs (e.g., Azure Storage, Key Vault, SQL Database).

**Implementation**:
```python
# OAuth2 Password Grant Flow for Dayforce API
import requests

# Retrieve Dayforce credentials from Key Vault (NOT Azure AD credentials)
oauth_username = notebookutils.credentials.getSecret(azure_keyvault_url, keyvault_secret_username)
oauth_password = notebookutils.credentials.getSecret(azure_keyvault_url, keyvault_secret_password)

def get_oauth2_token() -> str:
    """Acquire OAuth2 access token from Dayforce."""
    response = requests.post(
        dayforce_oauth_token_url,  # Dayforce token endpoint
        data={
            "grant_type": "password",  # Required by Dayforce API
            "companyId": oauth2_company_id,
            "username": oauth_username,  # From Key Vault
            "password": oauth_password,  # From Key Vault
            "client_id": oauth2_client_id
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30
    )
    
    if response.status_code != 200:
        raise RuntimeError(f"Token acquisition failed: HTTP {response.status_code}")
    
    token = response.json().get("access_token")
    if not token:
        raise RuntimeError("No access_token in OAuth2 response")
    
    logger.info("✅ OAuth2 token acquired successfully")
    return token

# Acquire token
access_token = get_oauth2_token()
```

**Automatic Token Renewal** (implemented in pipeline):
```python
def api_call_with_retry(url: str, headers: Dict) -> requests.Response:
    """Execute API call with automatic token renewal on expiration."""
    response = requests.get(url, headers=headers, timeout=timeout_config)
    
    if response.status_code == 401:  # Token expired
        logger.warning("HTTP 401 Unauthorized - Attempting token renewal...")
        
        # Renew token automatically
        new_token = get_oauth2_token()
        headers["Authorization"] = f"Bearer {new_token}"
        
        # Retry with new token
        response = requests.get(url, headers=headers, timeout=timeout_config)
        
        if response.status_code == 200:
            logger.info("✓ Success after token renewal")
            return response
        else:
            raise RuntimeError("Authentication failed after token renewal")
    
    return response
```

**Benefits**:
- ✅ Credentials stored in Azure Key Vault (not hardcoded)
- ✅ Automatic token renewal on expiration (resilient to token timeouts)
- ✅ Structured logging without exposing credentials
- ✅ Correct authentication method for Dayforce API

**Why Not Service Principal?**
- ❌ Dayforce API is NOT Azure AD-integrated
- ❌ Service Principal (ClientSecretCredential) only works with Azure APIs
- ✅ Password Grant is the **only supported** authentication method for Dayforce OAuth2

**Alternative Consideration**:
- For **Azure-native APIs** (Storage, SQL, Key Vault), use **Managed Identity** or **Service Principal**
- For **external APIs** (Dayforce, Salesforce, etc.), use their required auth flow (often Password Grant or Client Credentials)

---

### 3. **Environment Configuration Management with Variable Library**

**Problem**: Environment-specific configurations (URLs, connection strings, environment flags) should not be hardcoded in notebooks or pipelines. Different values are required for Development, Staging, and Production environments.

**Solution**: Microsoft Fabric Variable Library for centralized, environment-aware configuration management.

> **Critical for Release Management**: During production deployments, the **Prod column** must be activated in Variable Library with production-specific values. This ensures seamless environment transitions without code modifications.

**Implementation**:

**Step 1: Create Variable Library in Fabric Workspace**
```python
# Navigate to Workspace → Settings → Variable Library
# Create environment-specific variables with columns: Dev | Staging | Prod

Variable Library Configuration:
┌─────────────────────────┬──────────────────────────┬──────────────────────────┬──────────────────────────┐
│ Variable Name           │ Dev                      │ Staging                  │ Prod                     │
├─────────────────────────┼──────────────────────────┼──────────────────────────┼──────────────────────────┤
│ api_base_url            │ https://dev-api.com      │ https://stg-api.com      │ https://api.company.com  │
│ lakehouse_bronze_table  │ dev_bronze.api_extracts  │ stg_bronze.api_extracts  │ bronze.api_extracts      │
│ lakehouse_silver_table  │ dev_silver.transformed   │ stg_silver.transformed   │ silver.transformed       │
│ lakehouse_gold_table    │ dev_gold.fact_earnings   │ stg_gold.fact_earnings   │ gold.fact_earnings       │
│ max_retries             │ 3                        │ 5                        │ 5                        │
│ enable_debug_logging    │ true                     │ true                     │ false                    │
│ data_retention_days     │ 30                       │ 90                       │ 2555                     │
│ environment_name        │ DEVELOPMENT              │ STAGING                  │ PRODUCTION               │
└─────────────────────────┴──────────────────────────┴──────────────────────────┴──────────────────────────┘
```

**Step 2: Access Variables in Notebooks**
```python
# Fabric automatically loads variables based on active environment
# No need to specify which column - Fabric handles it via workspace settings

from notebookutils import fabric

# Retrieve environment-aware configuration values
api_base_url = fabric.variable.get("api_base_url")
lakehouse_bronze_table = fabric.variable.get("lakehouse_bronze_table")
max_retries = int(fabric.variable.get("max_retries"))
enable_debug_logging = fabric.variable.get("enable_debug_logging").lower() == "true"
environment_name = fabric.variable.get("environment_name")

# Log configuration (safe - no secrets)
logger.info(f"Environment: {environment_name}")
logger.info(f"API Base URL: {api_base_url}")
logger.info(f"Bronze Table: {lakehouse_bronze_table}")
logger.info(f"Debug Logging: {enable_debug_logging}")

# Use in pipeline logic
if enable_debug_logging:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)
```

**Step 3: Release Process - Environment Activation**
```bash
# Production Release Checklist:
# 1. Navigate to Production Workspace → Settings → Variable Library
# 2. Click "Activate" on the "Prod" column
# 3. Verify all production values are correct
# 4. Save changes
# 5. Run smoke tests to validate configuration

# Result: All notebooks/pipelines automatically use Prod values
# No code changes required ✅
```

**Benefits**:
- ✅ **Single codebase** across all environments (Dev/Staging/Prod)
- ✅ **Zero code changes** during deployment - just activate target environment
- ✅ **Audit trail** of configuration changes in Fabric
- ✅ **Version control** for environment configs (export/import capability)
- ✅ **Separation of concerns**: Configs separated from secrets (Key Vault for secrets)
- ✅ **Future-proof**: Includes variables that may change later, even if currently identical across environments

**Security Considerations**:
- ✅ Variable Library is for **non-sensitive configuration** (URLs, table names, retry counts)
- ❌ **NEVER store secrets** in Variable Library (passwords, API keys, tokens)
- ✅ Secrets belong in **Azure Key Vault** (see Section 1)
- ✅ Access to Variable Library controlled by workspace roles (Admin/Member can edit)
- ✅ Production Variable Library should have restricted edit access (Admin only)

**Disaster Recovery**:
```python
# Export Variable Library for backup
fabric.variable.export("./config_backup/variable_library_backup.json")

# Import Variable Library in new workspace
fabric.variable.import_from_file("./config_backup/variable_library_backup.json")
```

**Real-World Example - Release Day Scenario**:
```
📅 Release Day: Promoting pipeline from Staging to Production

❌ OLD WAY (manual code changes):
1. Open each notebook
2. Find all hardcoded URLs, table names, etc.
3. Replace staging values with production values
4. Risk: Human error, missed values, inconsistencies
5. Time: 2-4 hours across 10+ notebooks

✅ NEW WAY (Variable Library):
1. Navigate to Production Workspace → Variable Library
2. Click "Activate" on Prod column
3. Save changes
4. Run validation notebook
5. Time: 5 minutes total
6. Risk: Minimal - all configs centralized and reviewed in advance
```

**Best Practices**:
1. **Include future variables early**: Add variables that might differ across environments later, even if values are currently identical
2. **Document variable purpose**: Add descriptions/comments for each variable
3. **Test environment switches**: Practice activating different environments in Dev workspace first
4. **Validate after activation**: Always run smoke tests after switching environments
5. **Limit production access**: Only Admins should modify Production Variable Library

---

### 4. **Data Encryption**

> **Note**: Encryption at rest and in transit are **platform-provided features** (not configured in this pipeline). This section documents the encryption context and optional customer-managed key configuration.

**At Rest** (Platform Feature):
- All Delta Lake tables automatically encrypted with Microsoft-managed keys
- Optional enhancement: Customer-managed keys (CMK) via Azure Key Vault

**In Transit** (Platform Feature):
- All API calls over HTTPS/TLS 1.2+ (enforced by Azure)
- Fabric internal traffic encrypted

---

#### 4.1 Customer-Managed Keys (CMK) - Advanced Option

> **Important**: CMK in Fabric is configured at the **Capacity level**, not per table or lakehouse. This means all workspaces in a capacity use the same encryption key.

**When to Consider CMK**:
- Highly regulated industries (banking, healthcare, government)
- Compliance requirements mandating key ownership (PCI-DSS Level 1, HIPAA)
- Need to revoke data access immediately by disabling the key
- Audit requirements for encryption key usage

**Configuration Approach** (Capacity-Wide):

```bash
# 1. Create encryption key in Azure Key Vault
az keyvault key create \
  --vault-name myproject-keyvault \
  --name fabric-capacity-encryption-key \
  --kty RSA \
  --size 2048

# 2. Get Key URI
KEY_URI=$(az keyvault key show \
  --vault-name myproject-keyvault \
  --name fabric-capacity-encryption-key \
  --query key.kid -o tsv)

# 3. Grant Fabric permission to use the key
az keyvault set-policy \
  --name myproject-keyvault \
  --object-id <FABRIC_SERVICE_PRINCIPAL_ID> \
  --key-permissions get wrapKey unwrapKey

# 4. Enable CMK via Fabric Admin Portal
# Admin Portal → Capacities → [Your Capacity] → Settings → Encryption
# - Enable "Customer-Managed Keys"
# - Provide Key Vault URI: https://myproject-keyvault.vault.azure.net
# - Provide Key Name: fabric-capacity-encryption-key
# - ALL workspaces in this capacity now use CMK
```


---

## Workspace-Level Security

### Microsoft Fabric Workspace Roles

| Role | Permissions | Use Case |
|------|-------------|----------|
| **Admin** | Full control: create/delete items, manage permissions, configure workspace | Workspace owners, senior architects |
| **Member** | Create/edit/delete items, manage workspace settings, full item control | Data engineers, senior developers |
| **Contributor** | Create and edit items (notebooks, pipelines, datasets); cannot  manage workspace | Junior engineers, analysts with authoring needs |
| **Viewer** | Read-only access: view items and data, cannot modify or execute | Business users, auditors, QA testers |

### Implementation Example

**Assigning Workspace Roles**:
```bash
# Via Fabric UI
1. Navigate to Workspace Settings
2. Select "Manage Access"
3. Add users/groups with appropriate roles


### Best Practice: Group-Based Access

**Recommended Approach**:
```
Azure AD Groups → Fabric Workspace Roles

Example Structure:
├── AAD-DataEngineers-Senior → Workspace Admin
├── AAD-DataEngineers-Junior → Workspace Member
├── AAD-DataAnalysts → Workspace Contributor
└── AAD-BusinessUsers → Workspace Viewer
```

**Benefits**:
- ✅ Centralized access management in Azure AD
- ✅ Easy onboarding/offboarding (just group membership)
- ✅ Consistent permissions across multiple workspaces
- ✅ Audit trail in Azure AD logs

---

### Workspace Isolation Strategy

**Pattern**: Separate workspaces by environment and data sensitivity

**Implementation**:
```
Development Workspace (dev-hcm-pipeline)
  - Roles: Data Engineers (Admin/Member)
  - Data: Synthetic/anonymized test data
  - Access: Open for experimentation

Production Workspace (prod-hcm-pipeline)
  - Roles: Senior Engineers (Admin), Automated Notebooks (Member)
  - Data: Real production data
  - Access: Restricted, requires approval

Sensitive Workspace (prod-hcm-pii)
  - Roles: Compliance team (Admin), Limited engineers (Viewer)
  - Data: PII, SSN, salary data
  - Access: Logged and audited
```

---

## Item-Level Security

### Lakehouse Permissions

**Granular Permissions per Lakehouse**:

| Permission | Description | Typical Users |
|------------|-------------|---------------|
| **Read** | Query tables via SQL endpoint | Analysts, BI developers |
| **ReadAll** | Read all data + metadata | Data scientists, senior analysts |
| **Write** | Insert/update/delete data | ETL processes, data engineers |
| **ReadWrite** | Full data access | Automated pipelines |

**Implementation**:
```python
# Grant read-only access to specific lakehouse
from notebookutils import fabric

fabric.lakehouse.grant_permission(
    lakehouse_name="lh_gold",
    principal="group:AAD-Analysts",
    permission="Read"
)

# Revoke write access
fabric.lakehouse.revoke_permission(
    lakehouse_name="lh_gold",
    principal="user:junior@company.com",
    permission="Write"
)
```

---

### Notebook Execution Permissions

**Pattern**: Separate notebook authoring from execution

**Use Case**: Analysts can view notebooks but not modify them

**Implementation**:
```python
# Notebook permissions matrix
Notebook: nb_load_dayforce_gold.ipynb

├── engineer@company.com → Full Control (edit + execute)
├── analyst@company.com → Execute Only (run, view results)
└── auditor@company.com → View Only (read code, cannot run)
```

**Enforcement**:
1. Workspace Admin sets notebook permissions
2. Notebooks can be shared with specific users
3. Execution requires lakehouse permissions

---

## Lakehouse Security

> ⚠️ **Technical Reality Check**: Microsoft Fabric Lakehouses (Delta Lake) do **not** support native Row-Level Security (RLS), Column-Level Security (CLS), or Dynamic Data Masking (DDM) as database features like Azure SQL Database. This implementation uses **view-based security patterns** to achieve equivalent functionality. Access control is enforced through:
> - Secure views with embedded WHERE filters (RLS-style)
> - Column exclusion/masking in views (CLS/DDM-style)
> - Base table access restricted via Fabric RBAC
> - Schema-level isolation (native Delta Lake feature)

### 1. **Schema-Level Security**

**Pattern**: Separate sensitive data into dedicated schemas

**Implementation**:
```sql
-- Sensitive schema (restricted access)
CREATE SCHEMA IF NOT EXISTS sens 
COMMENT 'Sensitive data - PII, financial';

-- Non-sensitive schema (broader access)
CREATE SCHEMA IF NOT EXISTS dbo
COMMENT 'Non-sensitive analytics data';

-- Grant schema-level access
GRANT SELECT ON SCHEMA sens TO GROUP 'AAD-ComplianceTeam';
GRANT SELECT ON SCHEMA dbo TO GROUP 'AAD-AllAnalysts';
```

---

### Comparison: Native Database Security vs. Fabric Lakehouse Patterns

**Understanding the Technical Constraints:**

| Security Feature | Azure SQL Database (Native) | Fabric Lakehouse (Delta Lake) | This Project Implementation |
|------------------|----------------------------|-------------------------------|----------------------------|
| **Row-Level Security** | ✅ `ALTER TABLE ADD ROW FILTER` | ❌ Not supported | ✅ Secure views with WHERE filters |
| **Column-Level Security** | ✅ `GRANT SELECT ON COLUMN` | ❌ Not supported | ✅ Views with selective projection |
| **Dynamic Data Masking** | ✅ `MASKED WITH (FUNCTION = ...)` | ❌ Not supported | ✅ CASE statements in views |
| **Schema-Level Security** | ✅ Native | ✅ Native | ✅ `sens` vs. `dbo` schemas |
| **Encryption at Rest** | ✅ Native (TDE) | ✅ Native (automatic) | ✅ Platform-managed |
| **Performance Impact** | Minimal (storage layer) | Moderate (query layer) | Optimized with Z-ORDER/partitioning |
| **Access Enforcement** | Automatic (all queries) | Manual (users must use views) | RBAC revokes base table access |

**Key Insight**: View-based patterns require **discipline** — users must be prevented from accessing base tables via Fabric RBAC. Native RLS/CLS would enforce security at the storage layer regardless of query path.

---

### 2. **RLS-Style Row Filtering (View-Based Pattern)**

> **Implementation Note**: Fabric Lakehouses do not support native `ALTER TABLE ... ADD ROW FILTER` syntax. This implementation uses **secure views** with embedded WHERE clauses.

**Pattern**: Filter rows based on user context via views

**Use Case**: Employees can only see their own payroll data

**Implementation**:
```sql

-- ✅ CORRECT APPROACH: Use secure view with WHERE clause
CREATE OR REPLACE VIEW gold.dbo.vw_payroll_earnings_secure AS
SELECT 
    employee_id,
    employee_email,
    period_id,
    earnings_type
FROM gold.dbo.fact_payroll_earnings
WHERE 
    employee_email = current_user()  -- Filter at query time
    OR current_user() IN (SELECT email FROM ctrl.authorized_admins);

-- Grant access ONLY to view, revoke from base table
GRANT SELECT ON gold.dbo.vw_payroll_earnings_secure TO GROUP 'AAD-AllEmployees';
REVOKE SELECT ON gold.dbo.fact_payroll_earnings FROM GROUP 'AAD-AllEmployees';

-- Result: User john@company.com sees only their own records
SELECT * FROM gold.dbo.vw_payroll_earnings_secure;
```

**Advanced Pattern with Role-Based Access**:
```sql
-- Security view with manager access logic
CREATE OR REPLACE VIEW gold.dbo.vw_payroll_secure_advanced AS
SELECT 
    employee_id,
    employee_email,
    department,
    period_id
FROM gold.dbo.fact_payroll_earnings
WHERE 
    employee_email = current_user()  -- Employee sees own data
    OR is_member('AAD-PayrollManagers')  -- Managers see all
    OR (
        is_member('AAD-DepartmentManagers') 
        AND department IN (
            SELECT dept FROM ctrl.manager_departments 
            WHERE manager_email = current_user()
        )
    );  -- Department managers see their team

-- Users query the view, never the base table
```

**Key Differences from Native RLS**:
- ✅ **Pros**: Works in Fabric Lakehouses, uses standard SQL
- ⚠️ **Cons**: Filters applied at query time (not storage scan); requires RBAC discipline to prevent base table access
- 💡 **Optimization**: Use Z-ORDER and partition pruning to minimize query-time filter cost

---

### 3. **CLS-Style Column Hiding (View-Based Pattern)**

> **Implementation Note**: This approach uses **selective column projection** in views to achieve column-level security.

**Pattern**: Hide sensitive columns from unauthorized users

**Use Case**: Hide SSN and salary from most users

**Implementation**:
```sql
-- Create view with column filtering and conditional masking
CREATE OR REPLACE VIEW gold.dbo.vw_employee_public AS
SELECT 
    employee_id,
    employee_name,
    department,
    hire_date,
    -- Conditional column exposure based on role
    CASE 
        WHEN is_member('AAD-PayrollAdmins') THEN ssn
        ELSE '***-**-****'  -- Masked for non-admins
    END as ssn,
    CASE 
        WHEN is_member('AAD-PayrollAdmins') THEN salary
        ELSE NULL  -- Hidden for non-admins
    END as salary
FROM gold.dbo.dim_employee;

-- Grant access to view ONLY, revoke base table access
GRANT SELECT ON gold.dbo.vw_employee_public TO GROUP 'AAD-AllUsers';
REVOKE SELECT ON gold.dbo.dim_employee FROM GROUP 'AAD-AllUsers';

-- Admins get direct table access if needed
GRANT SELECT ON gold.dbo.dim_employee TO GROUP 'AAD-PayrollAdmins';
```

**Alternative: Pure Column Exclusion**:
```sql
-- For simpler cases, just exclude columns entirely
CREATE OR REPLACE VIEW gold.dbo.vw_employee_nonsensitive AS
SELECT 
    employee_id,
    employee_name,
    department,
    hire_date
    -- ssn and salary columns completely excluded
FROM gold.dbo.dim_employee;
```

---

### 4. **Data Masking Patterns (CASE-Based Implementation)**

> **Implementation Note**: Fabric Lakehouses do not support `MASKED WITH (FUNCTION = ...)` syntax. This implementation uses **CASE statements** within views to achieve masking.

**Pattern**: Mask sensitive data based on user permissions

**Implementation**:
```sql

-- ✅ CORRECT APPROACH: Use CASE statements in views
CREATE OR REPLACE VIEW gold.dbo.vw_employee_masked AS
SELECT 
    employee_id,
    employee_name,
    -- SSN masking: show only last 4 digits
    CASE 
        WHEN is_member('AAD-PayrollAdmins') THEN ssn
        ELSE CONCAT('***-**-', RIGHT(ssn, 4))
    END as ssn,
    -- Email masking: show only domain
    CASE 
        WHEN is_member('AAD-PayrollAdmins') THEN email
        ELSE CONCAT('****@', SPLIT(email, '@')[1])
    END as email,
    -- Salary masking: NULL for non-admins
    CASE 
        WHEN is_member('AAD-PayrollAdmins') THEN salary
        ELSE NULL
    END as salary,
    -- Phone masking: show only area code
    CASE 
        WHEN is_member('AAD-PayrollAdmins') THEN phone
        ELSE CONCAT('(', SUBSTRING(phone, 1, 3), ') ***-****')
    END as phone
FROM gold.dbo.dim_employee;

-- Grant access to masked view
GRANT SELECT ON gold.dbo.vw_employee_masked TO GROUP 'AAD-AllUsers';

-- Result for non-privileged users:
-- ssn: ***-**-1234
-- email: ****@company.com
-- salary: NULL
-- phone: (555) ***-****
```

**Reusable Masking Functions**:
```sql
-- Create UDFs for common masking patterns
CREATE OR REPLACE FUNCTION dbo.mask_ssn(ssn STRING, show_all BOOLEAN)
RETURNS STRING
RETURN CASE 
    WHEN show_all THEN ssn
    ELSE CONCAT('***-**-', RIGHT(ssn, 4))
END;

CREATE OR REPLACE FUNCTION dbo.mask_email(email STRING, show_all BOOLEAN)
RETURNS STRING
RETURN CASE
    WHEN show_all THEN email
    ELSE CONCAT('****@', SPLIT(email, '@')[1])
END;

-- Use in views
CREATE OR REPLACE VIEW gold.dbo.vw_employee_masked_v2 AS
SELECT 
    employee_id,
    dbo.mask_ssn(ssn, is_member('AAD-PayrollAdmins')) as ssn,
    dbo.mask_email(email, is_member('AAD-PayrollAdmins')) as email
FROM gold.dbo.dim_employee;
```

---

## Data Protection

### 1. **Data Classification & Tagging**

**Pattern**: Tag tables and columns by sensitivity level

**Implementation**:
```sql
-- Tag table with sensitivity classification
ALTER TABLE gold.dbo.fact_payroll_earnings
SET TBLPROPERTIES (
    'data_classification' = 'Confidential',
    'contains_pii' = 'true',
    'retention_period' = '7_years',
    'compliance_tags' = 'SOX,GDPR'
);

-- Tag individual columns
ALTER TABLE gold.dbo.dim_employee
ALTER COLUMN ssn SET TBLPROPERTIES (
    'sensitivity' = 'HighlyConfidential',
    'pii_type' = 'SSN'
);
```

---

### 2. **Data Retention & Purging**

**Pattern**: Automated data lifecycle management

**Implementation**:
```python
# Retention policy configuration
RETENTION_POLICIES = {
    'bronze': {
        'retention_days': 90,
        'vacuum_enabled': True,
        'archive_before_delete': False
    },
    'silver': {
        'retention_days': 365,
        'vacuum_enabled': True,
        'archive_before_delete': True  # Archive to cold storage
    },
    'gold': {
        'retention_days': 2555,  # 7 years (compliance)
        'vacuum_enabled': True,
        'archive_before_delete': True
    }
}

# Automated purging
def apply_retention_policy(table_name, layer):
    policy = RETENTION_POLICIES[layer]
    
    if policy['archive_before_delete']:
        # Archive old data to Azure Blob Cold Storage
        archive_old_data(table_name, policy['retention_days'])
    
    # Delete data older than retention period
    spark.sql(f"""
        DELETE FROM {table_name}
        WHERE ingestion_date < CURRENT_DATE() - INTERVAL {policy['retention_days']} DAYS
    """)
    
    # VACUUM to remove deleted files
    if policy['vacuum_enabled']:
        spark.sql(f"VACUUM {table_name} RETAIN 168 HOURS")
```

---


## Best Practices

### 1. **Principle of Least Privilege**

✅ **Grant minimum necessary permissions**
```python
# Bad: Grant Admin to all engineers
# Good: Grant Member to engineers, Admin only to leads

# Bad: Grant ReadWrite to all analysts
# Good: Grant Read to analysts, Write only to ETL processes
```

### 2. **Separation of Duties**

✅ **Separate development and production**
```
Development:
  - Engineers: Admin (full access for testing)
  - Automated Notebooks: Member (run pipelines)

Production:
  - Engineers: Viewer (read-only, for troubleshooting)
  - Automated Notebooks: Member (run pipelines)
  - Senior Engineers: Admin (emergency access only)
```

### 3. **Regular Access Reviews**

```sql
-- Quarterly access review report
SELECT 
    workspace_name,
    user_email,
    role,
    granted_date,
    last_access_date,
    DATEDIFF(DAY, last_access_date, CURRENT_DATE()) as days_since_access
FROM ctrl.workspace_access_log
WHERE role IN ('Admin', 'Member')
  AND days_since_access > 90  -- Flag inactive users
ORDER BY days_since_access DESC;
```

### 4. **Secrets Rotation**

> **Note**: Secret rotation policies are typically managed by Security/Platform teams. Data Engineers can recommend rotation schedules and verify compliance.

**Recommended Approach** (Security Team Implementation):
```bash
# Configure secret expiration (Security Team)
az keyvault secret set \
  --vault-name myproject-keyvault \
  --name hcm-api-password \
  --value "NEW_PASSWORD_HERE" \
  --expires $(date -d "+90 days" +%Y-%m-%dT%H:%M:%SZ)

# Set alert for expiring secrets (Platform Team)
az monitor metrics alert create \
  --name "KeyVault-Secret-Expiring" \
  --resource-group myproject-rg \
  --condition "secretNearExpiry > 0"
```

**Data Engineer Responsibility**:
- Request rotation schedule from Security team
- Update pipeline code to handle secret updates gracefully
- Monitor for secret expiration warnings
- Document which secrets are used by which pipelines

### 5. **Network Security**

> **Note**: Network security rules are configured by Infrastructure/Network teams. Data Engineers work with these constraints and request necessary access.

**Infrastructure Configuration** (Network Team Implementation):
```bash
# Restrict Key Vault access to specific VNets (Network Team)
az keyvault network-rule add \
  --name myproject-keyvault \
  --resource-group myproject-rg \
  --vnet-name fabric-vnet \
  --subnet fabric-subnet

# Enable firewall (Security Team)
az keyvault update \
  --name myproject-keyvault \
  --default-action Deny \
  --bypass AzureServices
```

**Data Engineer Responsibility**:
- Identify required network access for pipelines
- Submit change requests for firewall rules if needed
- Verify connectivity after network changes
- Document network dependencies in pipeline documentation

---

## Security Checklist

### Platform Prerequisites (Verify with IT/Cloud Team)

- [ ] Azure AD/Entra ID tenant configured with MFA policies
- [ ] Conditional Access policies enabled for Fabric access
- [ ] Microsoft Fabric workspace created with appropriate capacity
- [ ] VNet/Private Endpoint configuration (if required)
- [ ] Base encryption at rest enabled (default Microsoft-managed keys)
- [ ] TLS 1.2+ enforced for all connections

### Pre-Production Deployment (Pipeline Implementation)

- [ ] All credentials stored in Azure Key Vault (no hardcoded secrets) ✅
- [ ] OAuth2 authentication implemented with token auto-renewal ✅
- [ ] Variable Library configured with Dev/Staging/Prod environment values ✅
- [ ] Workspace roles assigned via Azure AD groups ✅
- [ ] Sensitive data in `sens` schema with restricted access ✅
- [ ] RLS-style secure views configured for sensitive tables ✅
- [ ] CLS-style column hiding/masking views for PII columns ✅
- [ ] Audit logging enabled for all data access (ctrl.audit_log) ✅
- [ ] Data retention policies configured ✅
- [ ] GDPR compliance procedures documented ✅
- [ ] Secret rotation policies in place
- [ ] Regular access reviews scheduled (quarterly)

### Production Release Checklist

- [ ] Variable Library **Prod column activated** in Production workspace ✅
- [ ] All production configuration values verified (URLs, table names, timeouts)
- [ ] Smoke tests executed in Production environment
- [ ] Variable Library backup exported for disaster recovery
- [ ] Production Key Vault secrets validated and up-to-date
- [ ] Production workspace access restricted (Admin roles only for edits)
- [ ] Rollback plan documented (includes Variable Library deactivation steps)

### Monthly Security Review

- [ ] Review audit logs for suspicious activity
- [ ] Check for inactive user accounts (last access > 90 days)
- [ ] Verify Key Vault secret expiration dates
- [ ] Validate Variable Library configuration consistency across environments
- [ ] Confirm correct environment (Dev/Staging/Prod) is activated in each workspace
- [ ] Validate RLS/CLS policies still correct
- [ ] Review and update data classification tags
- [ ] Test data recovery procedures
- [ ] Validate backup integrity
- [ ] Verify platform security updates applied (Azure/Fabric)
- [ ] Export Variable Library backups for all environments

---

## Security Incident Response

**Suspected Credential Compromise**:
1. Immediately rotate compromised secrets in Key Vault
2. Review audit logs for unauthorized access
3. Revoke permissions for affected accounts
4. Notify security team and stakeholders
5. Document incident and remediation

**Unauthorized Data Access**:
1. Query audit logs to identify scope of access
2. Revoke user permissions immediately
3. Review and tighten RLS/CLS policies
4. Notify compliance team if PII accessed
5. Conduct post-incident review

---

## Conclusion

This security framework demonstrates **defense in depth** through a combination of **platform security** and **pipeline-specific implementation**:

### Platform Prerequisites (Organizational Infrastructure):
1. **Authentication**: Azure AD/Entra ID with MFA policies
2. **Encryption**: TLS 1.2+ in transit, Microsoft-managed keys at rest
3. **Network**: VNets, private endpoints, firewall rules

### Pipeline Implementation (This Project):
1. **Secrets Management**: Azure Key Vault integration for API credentials ✅
2. **OAuth2 Authentication**: Password Grant Flow with automatic token renewal ✅
3. **Configuration Management**: Variable Library for environment-aware deployments (Dev/Staging/Prod) ✅
4. **Authorization**: RBAC via Azure AD groups, workspace/item permissions ✅
5. **Data Security**: View-based RLS/CLS patterns, CASE-based masking, schema isolation ✅
6. **Auditing**: Comprehensive query logging (ctrl.audit_log) ✅
7. **Compliance**: GDPR/SOX procedures with data retention policies ✅

**Result**: Enterprise-grade security leveraging Azure platform capabilities while implementing granular pipeline-specific controls. **Configuration management via Variable Library** enables zero-code deployments across environments, reducing human error and deployment time from hours to minutes. Data-level security uses **view-based patterns** (not native RLS/CLS/DDM features) suitable for Fabric Lakehouse architecture.

---

**Key Takeaway**: Effective security architecture requires understanding the boundary between **platform-provided features** (MFA, base encryption), **native database capabilities** (schema isolation), and **workaround patterns** (view-based RLS/CLS). This project demonstrates how to achieve robust data security even when native row/column-level features are unavailable.

---

**Security is not a one-time setup—it's an ongoing process of monitoring, reviewing, and adapting to new threats.**
