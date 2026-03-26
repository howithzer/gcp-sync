# Data Factory — Developer Guide
## From Swagger to Live Topic: How the Code Works and How to Test It

---

## 1. What you are looking at

This codebase implements a **Metadata-Driven Resource Factory**. The core idea is:

> You (the developer) own the schema and config. The platform owns all the infrastructure and processing logic.

You never write Terraform for a new topic. You never write Glue PySpark. You never implement HWM/offset logic. You commit **two files**, pass a PR, and the platform provisions everything and starts processing data.

The four source files in this repo correspond to four layers of the system:

| File | What it is | When it runs |
|---|---|---|
| `infra_stamper_lambda.py` | Resource Factory — provisions ALL AWS resources for one topic | On PR merge, triggered by ADO deploy pipeline |
| `golden_glue_transform.py` | The single shared PySpark script all Glue jobs point to | At runtime, when a Glue job executes |
| `step_function_template.json` | ASL blueprint — one copy is instantiated per topic | At deploy time (Stamper injects topic name) |
| `support_lambdas.py` | Four helper Lambdas called by the Step Function | At runtime, during each pipeline execution |

---

## 2. The two files you commit as a developer

### `swagger.yaml` (or `.json`) — the Physical Schema

This is a standard OpenAPI/Swagger file describing your topic's message payload. The ADO lint pipeline reads this and generates the Athena/Iceberg DDL (`std_ddl.sql`, `cur_ddl.sql`, `quarantine_ddl.sql`). You do not write SQL.

**Minimum required fields:**
```yaml
openapi: "3.0.0"
info:
  title: "payments_v2"
  version: "1.0.0"
components:
  schemas:
    PaymentsV2:
      type: object
      required:
        - transaction_id
        - amount
        - event_timestamp
      properties:
        transaction_id:
          type: string
          description: "Natural primary key — maps to _record_id in Iceberg"
        amount:
          type: number
          format: double
        event_timestamp:
          type: string
          format: date-time
        card_number:
          type: string
          x-cde: true          # <-- tag CDE columns here, not in code
```

### `file_metadata.json` — the Operational Config

This tells the platform how to run your topic — partitioning, DPU, custom scripts, tags.

```json
{
  "topic_name":          "payments_v2",
  "partition_columns":   ["event_date"],
  "cde_columns":         ["card_number", "ssn"],
  "primary_key":         "transaction_id",
  "database_name":       "data_factory_db",
  "glue_worker_type":    "G.1X",
  "glue_num_workers":    5,
  "glue_timeout_minutes": 60,
  "custom_std_script_path": null,
  "custom_cur_script_path": null,
  "tags": {
    "Department": "payments",
    "DataClass":  "PII"
  }
}
```

**Custom script override:** If your topic needs non-standard transformation logic, place a `.py` file alongside these two files in the repo and set:
```json
"custom_std_script_path": "s3://glue-scripts/custom/payments_v2_std_custom.py"
```
The Stamper will point the Glue job at your script instead of the golden image. The golden image handles ~90% of topics; the override handles the rest.

---

## 3. What happens when your PR is approved

### Phase A — ADO Lint Pipeline (before PR is raised)

1. The pipeline runs the **linter** against your swagger + metadata.
2. The **DDL generator** synthesises three SQL files and uploads them to S3:
   - `s3://artifacts/payments_v2/ddl/std_ddl.sql`
   - `s3://artifacts/payments_v2/ddl/cur_ddl.sql`
   - `s3://artifacts/payments_v2/ddl/quarantine_ddl.sql`
3. If the linter fails, the PR is blocked. Fix the swagger and push again.

### Phase B — ADO Deploy Pipeline (on PR merge)

The deploy pipeline calls the **Stamper Lambda** (`infra_stamper_lambda.py`) with a payload like:
```json
{
  "TopicName":       "payments_v2",
  "BatchId":         "deploy-uuid-1234",
  "DDLArtifactPath": "s3://artifacts/payments_v2/ddl/",
  "Metadata": { ...contents of your file_metadata.json... }
}
```

The Stamper runs four steps **in sequence**:

```
Step 1 — Execute DDL via Athena
  Fetches std_ddl.sql, cur_ddl.sql, quarantine_ddl.sql from S3
  Runs each as CREATE TABLE IF NOT EXISTS in Athena
  Creates: payments_v2_std, payments_v2_cur, payments_v2_quarantine

Step 2 — Create two Glue jobs
  payments_v2_std_glue_job  → points to golden_glue_transform.py  (or custom)
  payments_v2_cur_glue_job  → points to golden_glue_transform.py  (or custom)
  Both jobs receive --target_layer STD / CUR at runtime

Step 3 — Create the Step Function
  Fetches step_function_template.json from S3
  Replaces [TOPIC_NAME_PLACEHOLDER] with "payments_v2" throughout the ASL
  Creates state machine: payments_v2_pipeline

Step 4 — Write DynamoDB config
  Writes CONFIG item: PK=TOPIC#payments_v2  SK=CONFIG
  HWM starts at "0" — meaning "no data processed yet"
  nuclear_option = false
  staging_rerun_count = 0
```

After the Stamper completes, the ADO pipeline logs the Step Function ARN and all resource names. **Your topic is live.**

---

## 4. What happens at runtime (every processing cycle)

The **EKS Orchestrator pod** runs on a schedule. It:
1. Scans DynamoDB for all `is_active = "true"` topics
2. Checks current Glue DPU usage (capacity governor)
3. For each topic with headroom, calls `StartExecution` on `payments_v2_pipeline`
4. Passes `{ "topic_name": "payments_v2", "batch_id": "<new-uuid>" }`

The Step Function then executes these states:

```
State 1: GetTopicConfig
  Lambda: df_get_topic_config
  Fetches CONFIG + HWM items from DynamoDB in one call
  Returns the stored staging_hwm_snapshot_id (last processed snapshot)

State 2: ValidateWork
  Lambda: df_validate_work
  Queries payments_v2_raw$snapshots in Athena to find latest snapshot ID
  Compares it to the stored HWM
  → If same: no new data

State 3: CheckNewData (Choice state)
  If no new data AND nuclear_option = false → NoNewDataEnd (clean success)
  If new data OR nuclear_option = true     → RunSTDGlueJob

State 4a: RunSTDGlueJob
  Triggers: payments_v2_std_glue_job
  Passes: --starting_snapshot <hwm>  --ending_snapshot <latest>  --target_layer STD
  The Glue job reads only the delta (Iceberg incremental read)
  Writes typed records to payments_v2_std
  Quarantine records go to payments_v2_quarantine

State 4b: RunCURGlueJob
  Triggers: payments_v2_cur_glue_job
  Same snapshot range, reads from payments_v2_std, writes to payments_v2_cur
  Applies deduplication + business curation logic

State 5: CommitOffsets
  Lambda: df_commit_offsets
  Writes NEW HWM snapshot ID to DynamoDB HWM item
  Resets nuclear_option = false
  This is the ONLY place HWM advances — atomically, after both jobs succeed

Catch (any state): OnFailureNotify
  Lambda: df_on_failure
  Increments staging_rerun_count in DynamoDB
  Publishes SNS alert with full error context + batch_id
  Does NOT touch HWM — safe to re-trigger
```

---

## 5. How the batch_id correlation ID works

Every execution starts with a `batch_id` (UUID). This ID flows through **every component**:

```
ADO Pipeline   → passes batch_id to Stamper Lambda
Stamper Lambda → logs all operations with batch_id
EKS Pod        → generates new batch_id per SF execution
Step Function  → passes batch_id in every Lambda and Glue job payload
Glue Job       → logs every line as JSON with batch_id
                 emits CloudWatch metrics with BatchId as a dimension
CommitOffsets  → stores last_successful_batch_id in DynamoDB
OnFailure      → stores last_failure_batch in DynamoDB, publishes in SNS
```

**To trace a specific run:**

In CloudWatch Logs Insights, run against the Glue log group `/aws-glue/jobs/output`:
```
fields @timestamp, event, records_processed, quarantine_count, error
| filter batch_id = "your-uuid-here"
| sort @timestamp asc
```

In DynamoDB, check the CONFIG item:
- `last_failure_batch` — batch_id of the last failure
- `staging_rerun_count` — total failure count for this topic
- `last_commit_batch_id` — batch_id of the last success

---

## 6. The DynamoDB schema — what each field means

### CONFIG item (`SK = "CONFIG"`)

| Field | Type | Purpose |
|---|---|---|
| `PK` | String | `TOPIC#payments_v2` — partition key |
| `SK` | String | `CONFIG` — sort key |
| `topic_name` | String | Human-readable topic name |
| `raw_table_name` | String | `payments_v2_raw` — Firehose target (read-only for Glue) |
| `std_table_name` | String | `payments_v2_std` — Staging Iceberg table |
| `cur_table_name` | String | `payments_v2_cur` — Curated Iceberg table |
| `quarantine_table_name` | String | `payments_v2_quarantine` — malformed records |
| `partition_columns` | List | From your `file_metadata.json` |
| `cde_columns` | List | Columns subject to data classification / masking |
| `primary_key` | String | Natural dedup key (`_record_id` by default) |
| `std_glue_job_name` | String | `payments_v2_std_glue_job` |
| `cur_glue_job_name` | String | `payments_v2_cur_glue_job` |
| `step_function_arn` | String | ARN of this topic's dedicated state machine |
| **`nuclear_option`** | Boolean | **Set to `true` to force a full reload on next run** |
| `staging_rerun_count` | Number | Incremented on every pipeline failure |
| `is_active` | String | `"true"` / `"false"` — EKS orchestrator GSI key |

### HWM item (`SK = "HWM"`)

| Field | Type | Purpose |
|---|---|---|
| `PK` | String | `TOPIC#payments_v2` |
| `SK` | String | `HWM` |
| `staging_hwm_snapshot_id` | String | Last Iceberg snapshot ID successfully written to STD |
| `target_hwm_snapshot_id` | String | Last Iceberg snapshot ID successfully written to CUR |
| `last_successful_batch_id` | String | UUID of the last clean run — use to trace in CloudWatch |
| `last_updated_at` | String | ISO timestamp of last commit |
| `std_glue_run_id` | String | Glue job run ID for the last STD job — link to Glue console |
| `cur_glue_run_id` | String | Glue job run ID for the last CUR job — link to Glue console |

---

## 7. How to test your topic end-to-end

### Step 1 — Verify the Stamper ran correctly

After PR merge, check the ADO pipeline logs for the Stamper response. It should contain:
```json
{
  "status": "SUCCESS",
  "std_glue_job": "payments_v2_std_glue_job",
  "cur_glue_job": "payments_v2_cur_glue_job",
  "step_function_arn": "arn:aws:states:..."
}
```

Then verify in AWS Console:
- **Athena**: `SHOW TABLES LIKE 'payments_v2%'` — should return 3 tables (std, cur, quarantine)
- **Glue**: Jobs tab — search `payments_v2` — should see two jobs
- **Step Functions**: State machines — search `payments_v2_pipeline`
- **DynamoDB**: `topic_metadata` table → query `PK = TOPIC#payments_v2` — should see CONFIG item

### Step 2 — Trigger a manual Step Function execution

In the AWS Step Functions console, open `payments_v2_pipeline` → Start Execution:
```json
{
  "topic_name": "payments_v2",
  "batch_id":   "test-run-001"
}
```

Watch the execution graph. Expected paths:
- **No data yet** → `GetTopicConfig → ValidateWork → CheckNewData → NoNewDataEnd` (green, SUCCEEDED)
- **Data present** → all states green through to `PipelineSuccess`

### Step 3 — Verify incremental processing

After a successful run:
1. Check DynamoDB HWM item — `staging_hwm_snapshot_id` should be non-zero
2. Query STD table: `SELECT COUNT(*), MIN(_ingested_at), MAX(_ingested_at) FROM payments_v2_std`
3. Check CUR table similarly
4. Check quarantine: `SELECT failure_reason, COUNT(*) FROM payments_v2_quarantine GROUP BY failure_reason`

### Step 4 — Test the quarantine path

Insert a malformed record directly into the RAW Iceberg table (a row where `payload` is not valid JSON). Trigger a manual execution. The job should succeed, the bad record should appear in `payments_v2_quarantine`, and the CloudWatch metric `QuarantineRecords` should be non-zero.

### Step 5 — Test the nuclear option (full reload)

In DynamoDB, update the CONFIG item:
```python
table.update_item(
    Key={"PK": "TOPIC#payments_v2", "SK": "CONFIG"},
    UpdateExpression="SET nuclear_option = :t",
    ExpressionAttributeValues={":t": True}
)
```
Trigger a manual SF execution. The `CheckNewData` Choice state will bypass the HWM check and route directly to the Glue jobs with `--full_reload true`. After completion, `nuclear_option` is automatically reset to `false` by `CommitOffsets`.

### Step 6 — Test a failure and recovery

Stop one of the Glue jobs mid-run (or introduce a deliberate error in a custom script). The Step Function will exhaust retries, route to `OnFailureNotify`, increment `staging_rerun_count`, and publish an SNS alert. Verify:
- DynamoDB `staging_rerun_count` incremented
- SNS message received with `batch_id` and `error_detail`
- HWM **unchanged** — next execution will re-process the same snapshot range

---

## 8. Common operational tasks for the support team

**Topic is failing repeatedly:**
1. Note the `batch_id` from the SNS alert
2. Search CloudWatch Logs Insights for that `batch_id` across the Glue log groups
3. Check `staging_rerun_count` in DynamoDB — if >3, consider a full reload
4. Set `nuclear_option = true` and re-trigger

**Topic appears stuck (not processing new data):**
1. Check the HWM item — is `staging_hwm_snapshot_id` advancing?
2. Check the EKS orchestrator pod logs — is the topic being scheduled?
3. Check `is_active` in the CONFIG item — should be `"true"`
4. Manually trigger the SF and watch `ValidateWork` — is it finding the latest snapshot?

**Need to re-deploy a schema change:**
1. Update swagger + metadata, push a new PR
2. The Stamper's DDL uses `CREATE TABLE IF NOT EXISTS` — it won't drop and recreate
3. For column additions: Iceberg supports schema evolution automatically
4. For breaking changes (column rename, type change): coordinate with the data team — this requires a `nuclear_option` full reload after the DDL change

**Finding logs for a specific topic in CloudWatch:**
- Glue STD job logs: `/aws-glue/jobs/output` — filter by `payments_v2_std_glue_job`
- Glue CUR job logs: `/aws-glue/jobs/output` — filter by `payments_v2_cur_glue_job`
- Step Function execution logs: `/aws/states/data-factory/payments_v2`
- Support Lambda logs: `/aws/lambda/df_get_topic_config`, `/aws/lambda/df_validate_work`, etc.
