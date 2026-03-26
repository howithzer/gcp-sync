"""
infra_stamper_lambda.py
=======================
Resource Factory Lambda — "The Stamper"
Triggered by the ADO deployment pipeline (via API Gateway or CodePipeline invoke)
on PR merge. Provisions all AWS resources for one topic in a single atomic pass.

Resources created per topic (naming convention: [TopicName]_[Resource]):
  Athena / Iceberg:
    - {topic}_raw          (already exists — Firehose target, created at ingest onboarding)
    - {topic}_std          (Staging: typed, lineage-enriched)
    - {topic}_cur          (Target: curated, business-logic applied)
    - {topic}_quarantine   (Side-table for malformed records)
  DynamoDB:
    - topic_metadata table: CONFIG#{topic} item  (config + HWM + run state)
  Glue:
    - {topic}_std_glue_job (cloned from golden STD script, or custom_script_path)
    - {topic}_cur_glue_job (cloned from golden CUR script, or custom_cur_script_path)
  Step Functions:
    - {topic}_pipeline     (ASL template with topic name injected)

Input event schema:
{
  "TopicName":         "payments_v2",
  "BatchId":           "uuid-for-this-deploy-run",        // correlation ID
  "DDLArtifactPath":   "s3://artifacts/payments_v2/ddl/", // output of ADO linter
  "Metadata": {
    "partition_columns":       ["event_date"],
    "cde_columns":             ["card_number", "ssn"],
    "primary_key":             "_record_id",
    "glue_worker_type":        "G.1X",
    "glue_num_workers":        5,
    "glue_timeout_minutes":    60,
    "custom_std_script_path":  null,   // null = use golden image
    "custom_cur_script_path":  null,
    "database_name":           "data_factory_db",
    "tags": {
      "Department": "payments",
      "DataClass":  "PII"
    }
  }
}
"""

import os
import json
import time
import logging
import boto3
import botocore

# ─────────────────────────────────────────────
# Configuration — all tunable via Lambda env vars
# ─────────────────────────────────────────────

REGION                   = os.environ.get("AWS_REGION", "us-east-1")
DYNAMODB_TABLE           = os.environ.get("DYNAMODB_TABLE",   "topic_metadata")
ATHENA_WORKGROUP         = os.environ.get("ATHENA_WORKGROUP",  "DataFactory")
ATHENA_RESULTS_BUCKET    = os.environ.get("ATHENA_RESULTS_BUCKET", "s3://athena-results-bucket/")
GOLDEN_STD_SCRIPT_S3     = os.environ.get("GOLDEN_STD_SCRIPT_S3", "s3://glue-scripts/golden/golden_glue_transform.py")
GOLDEN_CUR_SCRIPT_S3     = os.environ.get("GOLDEN_CUR_SCRIPT_S3", "s3://glue-scripts/golden/golden_glue_transform.py")
GLUE_IAM_ROLE_ARN        = os.environ.get("GLUE_IAM_ROLE_ARN",   "arn:aws:iam::ACCOUNT_ID:role/GlueDataFactoryRole")
SF_IAM_ROLE_ARN          = os.environ.get("SF_IAM_ROLE_ARN",     "arn:aws:iam::ACCOUNT_ID:role/StepFunctionsDataFactoryRole")
SF_ASL_TEMPLATE_S3       = os.environ.get("SF_ASL_TEMPLATE_S3",  "s3://infra-templates/step_function_template.json")
SF_ASL_INJECT_MARKER     = "[TOPIC_NAME_PLACEHOLDER]"

# Governor: max concurrent Glue job creations to avoid DPU limit thrashing
GLUE_DPU_GOVERNOR_TABLE  = os.environ.get("GLUE_DPU_GOVERNOR_TABLE", "glue_dpu_governor")
GLUE_MAX_DPU_PER_DEPLOY  = int(os.environ.get("GLUE_MAX_DPU_PER_DEPLOY", "200"))

# ─────────────────────────────────────────────
# Logging — structured JSON for CloudWatch Insights
# ─────────────────────────────────────────────

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def log(level: str, event: str, batch_id: str = "", topic: str = "", **extra):
    record = {
        "level":    level,
        "event":    event,
        "topic":    topic,
        "batch_id": batch_id,
        **extra,
    }
    getattr(logger, level.lower(), logger.info)(json.dumps(record))


# ─────────────────────────────────────────────
# AWS Clients
# ─────────────────────────────────────────────

athena   = boto3.client("athena",         region_name=REGION)
glue     = boto3.client("glue",           region_name=REGION)
sfn      = boto3.client("stepfunctions",  region_name=REGION)
dynamodb = boto3.resource("dynamodb",     region_name=REGION)
s3       = boto3.client("s3",             region_name=REGION)


# ─────────────────────────────────────────────
# Helper: parse S3 URI into bucket + key
# ─────────────────────────────────────────────

def parse_s3_uri(uri: str):
    parts = uri.replace("s3://", "").split("/", 1)
    return parts[0], parts[1] if len(parts) > 1 else ""


# ─────────────────────────────────────────────
# Helper: run Athena DDL and wait for completion
# ─────────────────────────────────────────────

def run_athena_ddl(sql: str, topic: str, batch_id: str, label: str) -> str:
    """
    Submit DDL to Athena and poll until SUCCEEDED or FAILED.
    Returns the QueryExecutionId for tracing.
    """
    log("INFO", "athena_ddl_submit", topic=topic, batch_id=batch_id, label=label)
    resp = athena.start_query_execution(
        QueryString=sql,
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_RESULTS_BUCKET},
        QueryExecutionContext={"Catalog": "AwsDataCatalog"},
    )
    qid = resp["QueryExecutionId"]

    # Poll — DDL usually completes in <10 s
    for attempt in range(30):
        time.sleep(3)
        status = athena.get_query_execution(QueryExecutionId=qid)
        state  = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            log("INFO", "athena_ddl_succeeded",
                topic=topic, batch_id=batch_id,
                label=label, query_id=qid)
            return qid
        if state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
            raise RuntimeError(
                f"Athena DDL failed [{label}] QueryId={qid} Reason={reason}"
            )
    raise TimeoutError(f"Athena DDL timed out [{label}] QueryId={qid}")


# ─────────────────────────────────────────────
# 1. DDL Execution
#    The ADO linter generates three DDL files and
#    commits them to S3 under DDLArtifactPath.
#    Convention:
#      {ddl_path}/std_ddl.sql
#      {ddl_path}/cur_ddl.sql
#      {ddl_path}/quarantine_ddl.sql
#    RAW table DDL is managed by the ingest onboarding
#    pipeline and is NOT re-executed here.
# ─────────────────────────────────────────────

def execute_ddl(topic: str, ddl_artifact_path: str, batch_id: str):
    """
    Fetch each DDL file from S3 (linter output) and execute via Athena.
    Uses CREATE TABLE IF NOT EXISTS — safe to re-run on re-deploy.
    """
    ddl_files = {
        "std":        "std_ddl.sql",
        "cur":        "cur_ddl.sql",
        "quarantine": "quarantine_ddl.sql",
    }
    executed = {}
    base_path = ddl_artifact_path.rstrip("/")

    for layer, filename in ddl_files.items():
        s3_uri = f"{base_path}/{filename}"
        bucket, key = parse_s3_uri(s3_uri)
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            ddl_sql = obj["Body"].read().decode("utf-8")
        except s3.exceptions.NoSuchKey:
            raise FileNotFoundError(
                f"DDL artifact not found at {s3_uri}. "
                "Ensure the ADO lint pipeline completed successfully before deployment."
            )

        qid = run_athena_ddl(
            sql=ddl_sql,
            topic=topic,
            batch_id=batch_id,
            label=f"{topic}_{layer}_ddl",
        )
        executed[layer] = qid
        log("INFO", "ddl_executed",
            topic=topic, batch_id=batch_id,
            layer=layer, query_id=qid)

    return executed


# ─────────────────────────────────────────────
# 2. DynamoDB: initialize topic config item
#    Single table, two item types via sort key:
#      PK=TOPIC#{topic}  SK=CONFIG  → topic config + run state
#      PK=TOPIC#{topic}  SK=HWM     → snapshot offsets (managed separately)
# ─────────────────────────────────────────────

def initialize_dynamodb(topic: str, metadata: dict, batch_id: str,
                        std_job_name: str, cur_job_name: str, sf_arn: str):
    """
    Put the initial config item into DynamoDB.
    Condition ensures we don't overwrite an existing live topic's HWM.
    """
    table = dynamodb.Table(DYNAMODB_TABLE)

    config_item = {
        # Primary key
        "PK":                       f"TOPIC#{topic}",
        "SK":                       "CONFIG",
        # Identity
        "topic_name":               topic,
        "deploy_batch_id":          batch_id,
        # Table names (naming convention: [topic]_[resource])
        "raw_table_name":           f"{topic}_raw",
        "std_table_name":           f"{topic}_std",
        "cur_table_name":           f"{topic}_cur",
        "quarantine_table_name":    f"{topic}_quarantine",
        # Schema metadata from ADO linter output
        "partition_columns":        metadata.get("partition_columns", []),
        "cde_columns":              metadata.get("cde_columns", []),
        "primary_key":              metadata.get("primary_key", "_record_id"),
        "database_name":            metadata.get("database_name", "data_factory_db"),
        # Glue resources
        "std_glue_job_name":        std_job_name,
        "cur_glue_job_name":        cur_job_name,
        # Step Function
        "step_function_arn":        sf_arn,
        # Runtime state — HWM offsets start at 0
        "staging_hwm_snapshot_id":  "0",
        "target_hwm_snapshot_id":   "0",
        # Control flags
        "nuclear_option":           False,
        "staging_rerun_count":      0,
        "is_active":                True,
        # Tags passthrough
        "tags":                     metadata.get("tags", {}),
    }

    try:
        table.put_item(
            Item=config_item,
            # Only write if this item does not already exist, OR if HWM is still 0
            # (safe to overwrite a freshly-deployed topic that has never processed data)
            ConditionExpression=(
                "attribute_not_exists(PK) OR "
                "(staging_hwm_snapshot_id = :zero AND target_hwm_snapshot_id = :zero)"
            ),
            ExpressionAttributeValues={":zero": "0"},
        )
        log("INFO", "dynamodb_config_written",
            topic=topic, batch_id=batch_id)
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        log("WARN", "dynamodb_config_skip_existing_live_topic",
            topic=topic, batch_id=batch_id,
            message="Topic has active HWM — config not overwritten. Manual intervention required.")
        raise ValueError(
            f"Topic {topic} already has an active HWM in DynamoDB. "
            "Use a targeted update operation, not a full re-deploy, "
            "to avoid resetting offset state on a live topic."
        )


# ─────────────────────────────────────────────
# 3. Governor: DPU capacity check
#    Queries the governor table (managed separately
#    by the EKS orchestrator) to ensure we have
#    headroom before creating new Glue jobs.
#    This is a PLACEHOLDER — replace with your
#    actual DPU accounting logic.
# ─────────────────────────────────────────────

def check_glue_dpu_governor(topic: str, requested_dpu: int, batch_id: str) -> bool:
    """
    Returns True if DPU headroom exists for deployment.
    In production: query GLUE_DPU_GOVERNOR_TABLE for current reserved DPU,
    compare against account limit, and reject if over threshold.

    TODO: Implement actual DPU accounting.
    Current behaviour: log a warning and proceed (non-blocking placeholder).
    """
    log("WARN", "dpu_governor_placeholder_check",
        topic=topic, batch_id=batch_id,
        requested_dpu=requested_dpu,
        max_dpu_per_deploy=GLUE_MAX_DPU_PER_DEPLOY,
        message="Governor check is a placeholder — implement DPU accounting before production.")

    # Example real implementation sketch:
    # governor_table = dynamodb.Table(GLUE_DPU_GOVERNOR_TABLE)
    # resp = governor_table.get_item(Key={"PK": "GLOBAL", "SK": "DPU_RESERVED"})
    # reserved = int(resp.get("Item", {}).get("reserved_dpu", 0))
    # if reserved + requested_dpu > GLUE_MAX_DPU_PER_DEPLOY:
    #     raise RuntimeError(f"DPU governor rejected deploy: {reserved} + {requested_dpu} > {GLUE_MAX_DPU_PER_DEPLOY}")
    # governor_table.update_item(...)  # increment reserved
    return True


# ─────────────────────────────────────────────
# 4. Glue job creation
#    Clones the golden image for STD and CUR.
#    If custom_*_script_path provided in metadata,
#    uses that instead of the golden image S3 path.
# ─────────────────────────────────────────────

def create_glue_job(
    job_name: str,
    script_s3_path: str,
    topic: str,
    layer: str,
    metadata: dict,
    batch_id: str,
) -> str:
    """
    Creates a Glue job for one layer (STD or CUR).
    All default arguments point the job back to its own name
    so CloudWatch Logs groups are per-job.
    Returns the job name on success.
    """
    worker_type  = metadata.get("glue_worker_type",       "G.1X")
    num_workers  = metadata.get("glue_num_workers",        5)
    timeout_mins = metadata.get("glue_timeout_minutes",    60)
    tags         = metadata.get("tags", {})

    # Merge DataFactory-standard tags with topic tags
    glue_tags = {
        "DataFactory": "true",
        "Topic":        topic,
        "Layer":        layer,
        "DeployBatchId": batch_id,
        **tags,
    }

    try:
        glue.create_job(
            Name=job_name,
            Role=GLUE_IAM_ROLE_ARN,
            Command={
                "Name":           "glueetl",
                "ScriptLocation": script_s3_path,
                "PythonVersion":  "3",
            },
            DefaultArguments={
                "--topic_name":       topic,
                "--target_layer":     layer,
                "--enable-metrics":   "",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-job-insights":              "true",
                "--job-language":                     "python",
                # TempDir and Spark event log — adjust bucket to your env
                "--TempDir":          f"s3://glue-temp/{topic}/{layer}/",
                "--spark-event-logs-path": f"s3://glue-spark-logs/{topic}/{layer}/",
            },
            GlueVersion="4.0",
            WorkerType=worker_type,
            NumberOfWorkers=num_workers,
            Timeout=timeout_mins,
            MaxRetries=0,   # Retries handled by Step Function — not Glue
            Tags=glue_tags,
            Description=(
                f"Data Factory [{layer}] job for topic [{topic}]. "
                f"Script: {script_s3_path}. "
                f"Deploy batch: {batch_id}."
            ),
        )
        log("INFO", "glue_job_created",
            topic=topic, batch_id=batch_id,
            job_name=job_name, layer=layer,
            script=script_s3_path)
        return job_name

    except glue.exceptions.AlreadyExistsException:
        log("WARN", "glue_job_already_exists",
            topic=topic, batch_id=batch_id,
            job_name=job_name,
            message="Job already exists — skipping creation. Use update_job for changes.")
        return job_name


# ─────────────────────────────────────────────
# 5. Step Function creation
#    Fetches the ASL template from S3 (version-
#    controlled alongside this Lambda's deployment),
#    injects the topic name, and creates a dedicated
#    state machine for the topic.
# ─────────────────────────────────────────────

def create_step_function(topic: str, metadata: dict, batch_id: str) -> str:
    """
    Loads the ASL template from S3, injects the topic name,
    and creates a dedicated per-topic state machine.
    Returns the new state machine ARN.
    """
    # Fetch ASL template from S3
    bucket, key = parse_s3_uri(SF_ASL_TEMPLATE_S3)
    obj = s3.get_object(Bucket=bucket, Key=key)
    asl_template = obj["Body"].read().decode("utf-8")

    # Inject topic name into every placeholder
    asl_definition = asl_template.replace(SF_ASL_INJECT_MARKER, topic)

    sf_name = f"{topic}_pipeline"
    tags     = metadata.get("tags", {})

    sf_tags = [
        {"key": "DataFactory",    "value": "true"},
        {"key": "Topic",          "value": topic},
        {"key": "DeployBatchId",  "value": batch_id},
        *[{"key": k, "value": v} for k, v in tags.items()],
    ]

    try:
        resp = sfn.create_state_machine(
            name=sf_name,
            definition=asl_definition,
            roleArn=SF_IAM_ROLE_ARN,
            type="STANDARD",
            loggingConfiguration={
                "level": "ERROR",
                "includeExecutionData": True,
                "destinations": [
                    {
                        "cloudWatchLogsLogGroup": {
                            "logGroupArn": (
                                f"arn:aws:logs:{REGION}:ACCOUNT_ID:"
                                f"log-group:/aws/states/data-factory/{topic}:*"
                            )
                        }
                    }
                ],
            },
            tracingConfiguration={"enabled": True},
            tags=sf_tags,
        )
        sf_arn = resp["stateMachineArn"]
        log("INFO", "step_function_created",
            topic=topic, batch_id=batch_id,
            sf_name=sf_name, sf_arn=sf_arn)
        return sf_arn

    except sfn.exceptions.StateMachineAlreadyExists:
        # Idempotent: fetch the existing ARN and return it
        existing = sfn.describe_state_machine(
            stateMachineArn=(
                f"arn:aws:states:{REGION}:ACCOUNT_ID:stateMachine:{sf_name}"
            )
        )
        sf_arn = existing["stateMachineArn"]
        log("WARN", "step_function_already_exists",
            topic=topic, batch_id=batch_id,
            sf_arn=sf_arn,
            message="State machine already exists — returning existing ARN.")
        return sf_arn


# ─────────────────────────────────────────────
# Lambda Handler
# ─────────────────────────────────────────────

def lambda_handler(event: dict, context) -> dict:
    """
    Entry point. Orchestrates the full resource provisioning sequence.
    All exceptions are caught at the top level and returned as a structured
    failure response so the ADO pipeline can surface them clearly.
    """
    topic    = event.get("TopicName", "").strip().lower()
    batch_id = event.get("BatchId",   "unknown")
    metadata = event.get("Metadata",  {})
    ddl_path = event.get("DDLArtifactPath", "")

    # ── Input validation ──
    if not topic:
        return _failure("Missing required field: TopicName", batch_id=batch_id)
    if not ddl_path:
        return _failure("Missing required field: DDLArtifactPath", topic=topic, batch_id=batch_id)

    log("INFO", "stamper_start",
        topic=topic, batch_id=batch_id,
        ddl_artifact_path=ddl_path)

    # ── Resolve Glue script paths ──
    std_script = metadata.get("custom_std_script_path") or GOLDEN_STD_SCRIPT_S3
    cur_script = metadata.get("custom_cur_script_path") or GOLDEN_CUR_SCRIPT_S3
    is_custom_std = bool(metadata.get("custom_std_script_path"))
    is_custom_cur = bool(metadata.get("custom_cur_script_path"))

    if is_custom_std:
        log("INFO", "custom_std_script_detected",
            topic=topic, batch_id=batch_id, path=std_script)
    if is_custom_cur:
        log("INFO", "custom_cur_script_detected",
            topic=topic, batch_id=batch_id, path=cur_script)

    # ── DPU Governor check ──
    total_dpu = (
        metadata.get("glue_num_workers", 5) * 2  # STD + CUR jobs
    )
    try:
        check_glue_dpu_governor(topic, total_dpu, batch_id)
    except RuntimeError as exc:
        return _failure(str(exc), topic=topic, batch_id=batch_id, stage="dpu_governor")

    # ── STEP 1: Execute DDL via Athena ──
    try:
        ddl_result = execute_ddl(topic, ddl_path, batch_id)
    except Exception as exc:
        return _failure(str(exc), topic=topic, batch_id=batch_id, stage="ddl_execution")

    # ── STEP 2: Create Glue jobs ──
    std_job_name = f"{topic}_std_glue_job"
    cur_job_name = f"{topic}_cur_glue_job"
    try:
        create_glue_job(std_job_name, std_script, topic, "STD", metadata, batch_id)
        create_glue_job(cur_job_name, cur_script, topic, "CUR", metadata, batch_id)
    except Exception as exc:
        return _failure(str(exc), topic=topic, batch_id=batch_id, stage="glue_creation")

    # ── STEP 3: Create Step Function ──
    try:
        sf_arn = create_step_function(topic, metadata, batch_id)
    except Exception as exc:
        return _failure(str(exc), topic=topic, batch_id=batch_id, stage="sf_creation")

    # ── STEP 4: Initialize DynamoDB config ──
    try:
        initialize_dynamodb(
            topic=topic,
            metadata=metadata,
            batch_id=batch_id,
            std_job_name=std_job_name,
            cur_job_name=cur_job_name,
            sf_arn=sf_arn,
        )
    except ValueError as exc:
        # ConditionalCheck failure — live topic, don't overwrite
        return _failure(str(exc), topic=topic, batch_id=batch_id, stage="dynamodb_init")
    except Exception as exc:
        return _failure(str(exc), topic=topic, batch_id=batch_id, stage="dynamodb_init")

    # ── All done ──
    result = {
        "status":           "SUCCESS",
        "topic":            topic,
        "batch_id":         batch_id,
        "std_glue_job":     std_job_name,
        "cur_glue_job":     cur_job_name,
        "step_function_arn": sf_arn,
        "ddl_query_ids":    ddl_result,
        "custom_std_script": is_custom_std,
        "custom_cur_script": is_custom_cur,
    }

    log("INFO", "stamper_complete",
        topic=topic, batch_id=batch_id,
        result=result)

    return result


def _failure(message: str, topic: str = "", batch_id: str = "", stage: str = "") -> dict:
    log("ERROR", "stamper_failed",
        topic=topic, batch_id=batch_id,
        stage=stage, error=message)
    return {
        "status":   "FAILURE",
        "topic":    topic,
        "batch_id": batch_id,
        "stage":    stage,
        "error":    message,
    }
