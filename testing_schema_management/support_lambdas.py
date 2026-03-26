"""
support_lambdas.py
==================
Four Lambda functions referenced by the per-topic Step Function ASL.
Deploy each as a separate Lambda function in your environment.

Functions:
  1. df_get_topic_config   — Stage 1: fetch topic config from DynamoDB
  2. df_validate_work      — Stage 2: compare RAW snapshot vs HWM, detect new data
  3. df_commit_offsets     — Stage 4: advance HWM, reset nuclear_option
  4. df_on_failure         — Catch-all: increment rerun counter, publish SNS alert

DynamoDB Table Design (single table, two item types):
─────────────────────────────────────────────────────
Table name : topic_metadata
Billing    : PAY_PER_REQUEST (on-demand)

Primary key:
  PK  (String)  — e.g. "TOPIC#payments_v2"
  SK  (String)  — "CONFIG" | "HWM"

GSI (optional, for EKS orchestrator list scan):
  GSI name   : is_active-index
  PK         : is_active  (String "true"/"false")
  SK         : topic_name (String)

CONFIG item (written by Stamper Lambda at deploy time):
  PK                      : "TOPIC#{topic}"
  SK                      : "CONFIG"
  topic_name              : "payments_v2"
  deploy_batch_id         : "uuid-of-deploy-run"
  raw_table_name          : "payments_v2_raw"
  std_table_name          : "payments_v2_std"
  cur_table_name          : "payments_v2_cur"
  quarantine_table_name   : "payments_v2_quarantine"
  database_name           : "data_factory_db"
  partition_columns       : ["event_date"]           (List<String>)
  cde_columns             : ["card_number", "ssn"]   (List<String>)
  primary_key             : "_record_id"
  std_glue_job_name       : "payments_v2_std_glue_job"
  cur_glue_job_name       : "payments_v2_cur_glue_job"
  step_function_arn       : "arn:aws:states:..."
  nuclear_option          : false                    (Boolean)
  staging_rerun_count     : 0                        (Number)
  is_active               : "true"                   (String — GSI key)
  tags                    : { "Department": "payments" }

HWM item (written by CommitOffsets Lambda after each successful run):
  PK                      : "TOPIC#{topic}"
  SK                      : "HWM"
  topic_name              : "payments_v2"
  staging_hwm_snapshot_id : "8274638291"             (String — Iceberg snapshot ID)
  target_hwm_snapshot_id  : "8274638291"
  last_successful_batch_id: "uuid-of-last-run"
  last_updated_at         : "2026-03-25T14:22:00Z"
  std_glue_run_id         : "jr_abc123"
  cur_glue_run_id         : "jr_def456"
"""

import os
import json
import logging
import datetime
import boto3
from boto3.dynamodb.conditions import Key

# ─── Shared config ────────────────────────────────────────────────────────────
REGION         = os.environ.get("AWS_REGION",      "us-east-1")
DYNAMO_TABLE   = os.environ.get("DYNAMODB_TABLE",  "topic_metadata")
SNS_TOPIC_ARN  = os.environ.get("SNS_TOPIC_ARN",   "arn:aws:sns:REGION:ACCOUNT_ID:df_pipeline_failures")
ATHENA_WG      = os.environ.get("ATHENA_WORKGROUP", "DataFactory")
ATHENA_OUT     = os.environ.get("ATHENA_RESULTS_BUCKET", "s3://athena-results-bucket/")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb", region_name=REGION)
athena   = boto3.client("athena",    region_name=REGION)
sns      = boto3.client("sns",       region_name=REGION)


def _log(level: str, event: str, **kw):
    logger.info(json.dumps({"level": level, "event": event, **kw}))


# ══════════════════════════════════════════════════════════════════════════════
# Lambda 1 — df_get_topic_config
# Step Function Stage 1: GetTopicConfig
#
# Reads both CONFIG and HWM items for the topic in a single BatchGetItem call.
# Merges them into one flat response payload that the rest of the SF uses.
# ══════════════════════════════════════════════════════════════════════════════

def get_topic_config(event: dict, context) -> dict:
    """
    Input event:
      { "topic_name": "payments_v2", "batch_id": "uuid", "caller": "StepFunction" }

    Output (returned as $.stage1.config in the SF):
      All CONFIG fields + HWM snapshot IDs merged into one flat dict.
    """
    topic    = event["topic_name"]
    batch_id = event["batch_id"]
    table    = dynamodb.Table(DYNAMO_TABLE)

    _log("INFO", "get_config_start", topic=topic, batch_id=batch_id)

    # BatchGetItem: fetch CONFIG and HWM in one round-trip
    response = dynamodb.batch_get_item(
        RequestItems={
            DYNAMO_TABLE: {
                "Keys": [
                    {"PK": f"TOPIC#{topic}", "SK": "CONFIG"},
                    {"PK": f"TOPIC#{topic}", "SK": "HWM"},
                ]
            }
        }
    )

    items = {
        item["SK"]: item
        for item in response["Responses"].get(DYNAMO_TABLE, [])
    }

    if "CONFIG" not in items:
        raise ValueError(
            f"No CONFIG item found for topic '{topic}'. "
            "Ensure the Stamper Lambda has been run for this topic."
        )

    config = dict(items["CONFIG"])

    # Merge HWM into config — use "0" as default if HWM item doesn't exist yet
    # (first run of a newly deployed topic)
    hwm = items.get("HWM", {})
    config["staging_hwm_snapshot_id"] = hwm.get("staging_hwm_snapshot_id", "0")
    config["target_hwm_snapshot_id"]  = hwm.get("target_hwm_snapshot_id",  "0")
    config["last_successful_batch_id"] = hwm.get("last_successful_batch_id", None)

    # Convert DynamoDB Decimal types to int/str for JSON serialisation
    config = _deserialise_decimals(config)

    _log("INFO", "get_config_success",
         topic=topic, batch_id=batch_id,
         staging_hwm=config["staging_hwm_snapshot_id"],
         nuclear_option=config.get("nuclear_option", False))

    return config


# ══════════════════════════════════════════════════════════════════════════════
# Lambda 2 — df_validate_work
# Step Function Stage 2: ValidateWork
#
# Queries the RAW Iceberg table's snapshot history via Athena to find the
# current maximum snapshot ID.  Compares it against the stored HWM.
# Returns has_new_data, starting_snapshot, and ending_snapshot.
# ══════════════════════════════════════════════════════════════════════════════

def validate_work(event: dict, context) -> dict:
    """
    Input event:
      {
        "topic_name":       "payments_v2",
        "batch_id":         "uuid",
        "topic_config":     { ...CONFIG item... },
        "raw_table_name":   "payments_v2_raw",
        "staging_hwm_snap": "8274638291"
      }

    Output (returned as $.stage2.validation in the SF):
      {
        "has_new_data":       true,
        "starting_snapshot":  "8274638291",   # exclusive — the stored HWM
        "ending_snapshot":    "9384729382",   # inclusive — latest RAW snapshot
        "snapshot_gap":       1               # number of new snapshots
      }
    """
    topic        = event["topic_name"]
    batch_id     = event["batch_id"]
    config       = event["topic_config"]
    raw_table    = event["raw_table_name"]
    stored_hwm   = str(event.get("staging_hwm_snap", "0"))
    database     = config.get("database_name", "data_factory_db")

    _log("INFO", "validate_work_start",
         topic=topic, batch_id=batch_id,
         raw_table=raw_table, stored_hwm=stored_hwm)

    # Query Iceberg snapshot history to find the latest committed snapshot
    # snapshots table is a metadata table — very fast, no full scan
    sql = f"""
        SELECT MAX(snapshot_id) AS latest_snapshot_id,
               COUNT(*)         AS total_snapshots
        FROM   "{database}"."{raw_table}$snapshots"
        WHERE  operation IN ('append', 'overwrite', 'replace')
    """

    qid   = _run_athena_query(sql, batch_id, label=f"{topic}_hwm_check")
    rows  = _fetch_athena_results(qid)

    if not rows or rows[0].get("latest_snapshot_id") in (None, "", "None"):
        _log("WARN", "validate_work_no_snapshots",
             topic=topic, batch_id=batch_id,
             raw_table=raw_table)
        return {
            "has_new_data":      False,
            "starting_snapshot": stored_hwm,
            "ending_snapshot":   stored_hwm,
            "snapshot_gap":      0,
        }

    latest_snapshot = str(rows[0]["latest_snapshot_id"])
    total_snapshots = int(rows[0].get("total_snapshots", 0))

    has_new_data = latest_snapshot != stored_hwm

    _log("INFO", "validate_work_complete",
         topic=topic, batch_id=batch_id,
         stored_hwm=stored_hwm,
         latest_snapshot=latest_snapshot,
         has_new_data=has_new_data)

    return {
        "has_new_data":      has_new_data,
        "starting_snapshot": stored_hwm,        # exclusive lower bound for Glue
        "ending_snapshot":   latest_snapshot,   # inclusive upper bound for Glue
        "snapshot_gap":      total_snapshots,   # informational — for metrics
    }


# ══════════════════════════════════════════════════════════════════════════════
# Lambda 3 — df_commit_offsets
# Step Function Stage 4: CommitOffsets
#
# This is the ONLY function that advances the HWM.
# Writes a new HWM item and resets nuclear_option to False.
# Uses a conditional write to prevent a stale Step Function
# execution from overwriting a newer HWM.
# ══════════════════════════════════════════════════════════════════════════════

def commit_offsets(event: dict, context) -> dict:
    """
    Input event:
      {
        "topic_name":            "payments_v2",
        "batch_id":              "uuid",
        "new_staging_snapshot":  "9384729382",
        "new_target_snapshot":   "9384729382",
        "std_glue_run_id":       "jr_abc123",
        "cur_glue_run_id":       "jr_def456",
        "reset_nuclear_option":  true
      }
    """
    topic          = event["topic_name"]
    batch_id       = event["batch_id"]
    new_staging    = str(event["new_staging_snapshot"])
    new_target     = str(event["new_target_snapshot"])
    std_run_id     = event.get("std_glue_run_id", "unknown")
    cur_run_id     = event.get("cur_glue_run_id", "unknown")
    reset_nuclear  = event.get("reset_nuclear_option", True)
    now            = datetime.datetime.utcnow().isoformat() + "Z"

    table = dynamodb.Table(DYNAMO_TABLE)

    _log("INFO", "commit_offsets_start",
         topic=topic, batch_id=batch_id,
         new_staging=new_staging, new_target=new_target)

    # ── Write new HWM item ──
    # Condition: only write if stored batch_id != this batch_id
    # (prevents re-execution of a completed SF from rewinding the HWM)
    try:
        table.put_item(
            Item={
                "PK":                       f"TOPIC#{topic}",
                "SK":                       "HWM",
                "topic_name":               topic,
                "staging_hwm_snapshot_id":  new_staging,
                "target_hwm_snapshot_id":   new_target,
                "last_successful_batch_id": batch_id,
                "last_updated_at":          now,
                "std_glue_run_id":          std_run_id,
                "cur_glue_run_id":          cur_run_id,
            },
            ConditionExpression=(
                "attribute_not_exists(last_successful_batch_id) OR "
                "last_successful_batch_id <> :bid"
            ),
            ExpressionAttributeValues={":bid": batch_id},
        )
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        # This batch_id was already committed — idempotent success
        _log("WARN", "commit_offsets_already_committed",
             topic=topic, batch_id=batch_id,
             message="HWM already committed for this batch_id — skipping.")
        return {"status": "ALREADY_COMMITTED", "topic": topic, "batch_id": batch_id}

    # ── Reset nuclear_option on CONFIG item ──
    if reset_nuclear:
        table.update_item(
            Key={"PK": f"TOPIC#{topic}", "SK": "CONFIG"},
            UpdateExpression=(
                "SET nuclear_option = :f, "
                "    last_commit_at = :now, "
                "    last_commit_batch_id = :bid"
            ),
            ExpressionAttributeValues={
                ":f":   False,
                ":now": now,
                ":bid": batch_id,
            },
        )

    _log("INFO", "commit_offsets_success",
         topic=topic, batch_id=batch_id,
         staging_hwm=new_staging, target_hwm=new_target,
         nuclear_reset=reset_nuclear)

    return {
        "status":           "COMMITTED",
        "topic":            topic,
        "batch_id":         batch_id,
        "staging_hwm":      new_staging,
        "target_hwm":       new_target,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Lambda 4 — df_on_failure
# Step Function Catch-all: OnFailureNotify
#
# Called when ANY state in the SF throws an unrecoverable error.
# Increments staging_rerun_count, publishes structured SNS alert.
# Does NOT touch the HWM — safe to retry the full pipeline.
# ══════════════════════════════════════════════════════════════════════════════

def on_failure(event: dict, context) -> dict:
    """
    Input event:
      {
        "topic_name":   "payments_v2",
        "batch_id":     "uuid",
        "error_detail": { "Error": "...", "Cause": "..." },
        "failed_state": "PIPELINE_FAILURE",
        "sns_topic_arn": "arn:aws:sns:..."
      }
    """
    topic        = event["topic_name"]
    batch_id     = event["batch_id"]
    error_detail = event.get("error_detail", {})
    failed_state = event.get("failed_state", "UNKNOWN")
    sns_arn      = event.get("sns_topic_arn", SNS_TOPIC_ARN)
    now          = datetime.datetime.utcnow().isoformat() + "Z"

    _log("ERROR", "pipeline_failure",
         topic=topic, batch_id=batch_id,
         failed_state=failed_state,
         error=error_detail)

    table = dynamodb.Table(DYNAMO_TABLE)

    # ── Increment rerun counter and record the failure ──
    try:
        resp = table.update_item(
            Key={"PK": f"TOPIC#{topic}", "SK": "CONFIG"},
            UpdateExpression=(
                "SET staging_rerun_count = if_not_exists(staging_rerun_count, :zero) + :one, "
                "    last_failure_at     = :now, "
                "    last_failure_batch  = :bid, "
                "    last_failure_reason = :reason"
            ),
            ExpressionAttributeValues={
                ":zero":   0,
                ":one":    1,
                ":now":    now,
                ":bid":    batch_id,
                ":reason": json.dumps(error_detail)[:1000],  # DDB attribute limit
            },
            ReturnValues="UPDATED_NEW",
        )
        rerun_count = int(
            resp.get("Attributes", {}).get("staging_rerun_count", 1)
        )
    except Exception as exc:
        # Don't let a DynamoDB write failure prevent the SNS alert
        _log("WARN", "on_failure_dynamo_write_error", topic=topic, error=str(exc))
        rerun_count = -1  # unknown

    # ── Publish SNS alert ──
    alert_message = {
        "alert_type":    "DataFactory Pipeline Failure",
        "topic":         topic,
        "batch_id":      batch_id,
        "failed_state":  failed_state,
        "rerun_count":   rerun_count,
        "failed_at":     now,
        "error":         error_detail,
        "action_required": (
            "Check CloudWatch Logs for batch_id above. "
            "Set nuclear_option=true in DynamoDB CONFIG item to force full reload, "
            "or fix and re-trigger from EKS orchestrator."
        ),
    }

    try:
        sns.publish(
            TopicArn=sns_arn,
            Subject=f"[DataFactory FAILURE] {topic} | batch={batch_id}",
            Message=json.dumps(alert_message, indent=2),
            MessageAttributes={
                "topic_name": {
                    "DataType":    "String",
                    "StringValue": topic,
                },
                "rerun_count": {
                    "DataType":    "Number",
                    "StringValue": str(rerun_count),
                },
            },
        )
        _log("INFO", "on_failure_sns_published",
             topic=topic, batch_id=batch_id, rerun_count=rerun_count)
    except Exception as exc:
        _log("ERROR", "on_failure_sns_publish_error",
             topic=topic, batch_id=batch_id, error=str(exc))

    return {
        "status":      "FAILURE_RECORDED",
        "topic":       topic,
        "batch_id":    batch_id,
        "rerun_count": rerun_count,
    }


# ─────────────────────────────────────────────
# Shared Athena helpers
# ─────────────────────────────────────────────

import time

def _run_athena_query(sql: str, batch_id: str, label: str) -> str:
    resp = athena.start_query_execution(
        QueryString=sql,
        WorkGroup=ATHENA_WG,
        ResultConfiguration={"OutputLocation": ATHENA_OUT},
    )
    qid = resp["QueryExecutionId"]
    for _ in range(20):
        time.sleep(2)
        state = athena.get_query_execution(
            QueryExecutionId=qid
        )["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            return qid
        if state in ("FAILED", "CANCELLED"):
            reason = athena.get_query_execution(
                QueryExecutionId=qid
            )["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
            raise RuntimeError(f"Athena [{label}] failed: {reason} (qid={qid})")
    raise TimeoutError(f"Athena [{label}] timed out (qid={qid})")


def _fetch_athena_results(qid: str) -> list:
    resp     = athena.get_query_results(QueryExecutionId=qid)
    columns  = [c["Label"] for c in resp["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    rows     = resp["ResultSet"]["Rows"][1:]  # skip header row
    return [
        {columns[i]: row["Data"][i].get("VarCharValue") for i in range(len(columns))}
        for row in rows
    ]


def _deserialise_decimals(obj):
    """Recursively convert DynamoDB Decimal to int/float for JSON safety."""
    from decimal import Decimal
    if isinstance(obj, dict):
        return {k: _deserialise_decimals(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_deserialise_decimals(i) for i in obj]
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    return obj


# ─────────────────────────────────────────────
# Lambda dispatch — each function is deployed
# as its own Lambda with its own handler entry point
# ─────────────────────────────────────────────

def handler_get_config(event, context):
    return get_topic_config(event, context)

def handler_validate_work(event, context):
    return validate_work(event, context)

def handler_commit_offsets(event, context):
    return commit_offsets(event, context)

def handler_on_failure(event, context):
    return on_failure(event, context)
