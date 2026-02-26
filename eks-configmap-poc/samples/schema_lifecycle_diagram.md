# Schema Management Lifecycle Diagrams

This document details the CI/CD sequence for how schemas are validated, flattened, and dynamically deployed into AWS Glue/Athena using the Schema Management frameworks.

## CI/CD Schema Evolution Flow

This sequence diagram illustrates the lifecycle of a schema change: from a developer proposing a change via a Pull Request, through local CI linting (validating constraints, checking for drift, and truncating fields at depth limit 5), down to CD deployment and the actual infrastructure updates executed by `cd_deployer.py`.

```mermaid
sequenceDiagram
    participant DEV as Developer
    participant GH as GitHub PR (CI Linter)
    participant ADO as Azure DevOps (CD Deployer)
    participant S3 as AWS S3 (Target Paths)
    participant GLUE as AWS Glue / Athena

    Note over DEV, GLUE: Stage 1: Continuous Integration (CI) - Validation and Preview

    DEV->>GH: Open Pull Request<br/>(Modifies swagger_v2.yaml & metadata.json)
    GH->>GH: Run `ci_linter.py` (Local Execution)
    
    GH->>GH: Resolve $refs & Extract Properties
    GH->>GH: Apply Flattening logic (Max Depth: 5)
    Note right of GH: Nested objects beyond level 5 are truncated <br/>and stored as STRING (JSON blobs)
    
    GH->>GH: Read metadata.json targets
    Note right of GH: Identify duplicate raw fields for <br/>columns mapped as "sensitive_columns"
    
    GH-->>DEV: Outputs generated CREATE/ALTER DDL queries
    DEV->>GH: Review DDL Preview, Approve, and Merge PR

    Note over DEV, GLUE: Stage 2: Continuous Deployment (CD) - Infrastructure Execution

    GH->>ADO: Trigger Main Branch Pipeline
    ADO->>ADO: Run `cd_deployer.py` (Production Credentials)

    ADO->>S3: Upload schemas to `s3://datalake/schemas/...`
    
    ADO->>GLUE: Query Information Schema
    GLUE-->>ADO: Return Existing Table Schema (if any)
    
    alt Table Does Not Exist
        ADO->>ADO: Generate CREATE TABLE DDLs (Standardized & Curated)
        ADO->>GLUE: Execute Athena Query: CREATE TABLE
        GLUE-->>ADO: Success
    else Table Exists (Schema Drift)
        ADO->>ADO: Compare Existing vs New Schema Properties
        ADO->>ADO: Generate ALTER TABLE ADD COLUMNS DDL
        ADO->>GLUE: Execute Athena Query: ALTER TABLE
        GLUE-->>ADO: Success
    end
    
    Note right of GLUE: Iceberg V2 tables are now provisioned<br/>matching the flattened OpenAPI spec!
```

### Key Mechanisms Detailed
1. **Depth Limits**: Notice how `ci_linter.py` and `cd_deployer.py` both enforce the `max_depth: 5` flattening rule locally without needing to query AWS. This prevents runaway recursion before runtime.
2. **Sensitive Data Forking**: The linter identifies columns tagged as PII in the `metadata.json` and duplicates them under the hood during the DDL generation phase (masking the primary field with Iceberg comments, and appending an unmasked `_raw` column to the Standardized target only).
3. **Idempotency via Athena**: The deployer specifically queries the `Information Schema` of AWS Glue to determine if an `ALTER` or `CREATE` is required, ensuring that the CD pipeline can safely be run repeatedly without breaking existing tables.
