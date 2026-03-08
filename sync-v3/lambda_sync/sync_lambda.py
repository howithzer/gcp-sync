"""
GCP Sync — Discovery Service (v3)
===================================

Single responsibility: keep the Iceberg subscription registry in sync with
GCP Pub/Sub. No Kubernetes changes ever happen here.

Schedule: runs hourly via EventBridge (configurable in Terraform).

Flow
----
  1. List all subscriptions from GCP Pub/Sub
  2. MERGE into Iceberg registry:
       New subscriptions   → INSERT  (usage_group='baseline', status='PENDING')
       Known subscriptions → UPDATE  last_seen_ts only
                             usage_group is NEVER overwritten — ops owns it
  3. Detect removals:
       Subscriptions gone from GCP entirely → status='REMOVED'
       Subscriptions whose backing topic was deleted (topic='_deleted-topic_')
       → status='REMOVED' (orphan detection)
  4. On-demand patching trigger (with concurrent execution guard):
       If any group has PENDING topics AND no execution is already RUNNING
       for that group → start the Patching Step Function immediately.
       If an execution IS already RUNNING → skip to avoid conflicting patches.

Changes from v2
---------------
  - Concurrent patch guard: before triggering the Step Function, paginate
    all RUNNING executions and inspect their input JSON for a group match.
    Skips the trigger if a matching execution is found.
  - Athena polling timeout: all polling loops now enforce a configurable
    deadline (ATHENA_POLL_TIMEOUT, default 120 s). A timed-out query is
    cancelled before the exception is raised so it does not hold resources.
  - SQL sanitisation: all dynamic queries now use ExecutionParameters.

Output payload
--------------
  {
    "total_gcp":        8,
    "removed":          2,
    "groups_triggered": ["baseline"],
    "groups_skipped":   ["group1"]    <- already being patched, trigger skipped
  }
"""

import os
import time
import json
import boto3
from google.cloud import pubsub_v1

GCP_PROJECT_ID      = os.environ.get("GCP_PROJECT_ID", "wired-sign-858")
ATHENA_DATABASE     = os.environ.get("ATHENA_DATABASE", "gcp_sync_db")
ATHENA_TABLE        = os.environ.get("ATHENA_TABLE", "subscription_registry")
ATHENA_OUTPUT_LOC   = os.environ.get("ATHENA_OUTPUT_LOC", "s3://YOUR-BUCKET/sync/")
ICEBERG_DATA_BUCKET = os.environ.get("ICEBERG_DATA_BUCKET", "YOUR-BUCKET")
PATCHING_SF_ARN     = os.environ.get("PATCHING_SF_ARN", "")
ATHENA_POLL_TIMEOUT = int(os.environ.get("ATHENA_POLL_TIMEOUT", "120"))

athena = boto3.client('athena')
sfn    = boto3.client('stepfunctions')


# ---------------------------------------------------------------------------
# Athena helpers
# ---------------------------------------------------------------------------

def _run_query(sql, params=None, timeout=None):
    """
    Executes a single Athena statement and polls until a terminal state.
    Raises RuntimeError on FAILED/CANCELLED.
    Raises TimeoutError if the deadline is exceeded; cancels the query first
    so it does not continue consuming Athena DPU capacity.
    """
    if timeout is None:
        timeout = ATHENA_POLL_TIMEOUT

    kwargs = {
        'QueryString': sql,
        'QueryExecutionContext': {'Database': ATHENA_DATABASE},
        'ResultConfiguration': {'OutputLocation': ATHENA_OUTPUT_LOC}
    }
    if params:
        kwargs['ExecutionParameters'] = params

    resp = athena.start_query_execution(**kwargs)
    exec_id  = resp['QueryExecutionId']
    deadline = time.time() + timeout

    while time.time() < deadline:
        result = athena.get_query_execution(QueryExecutionId=exec_id)
        status = result['QueryExecution']['Status']
        state  = status['State']

        if state == 'SUCCEEDED':
            return exec_id
        if state in ('FAILED', 'CANCELLED'):
            reason = status.get('StateChangeReason', 'no reason returned')
            raise RuntimeError(
                f"Athena query {exec_id} {state}: {reason} | SQL: {sql[:120]}"
            )
        time.sleep(2)

    # Deadline exceeded — cancel the orphaned query before raising
    try:
        athena.stop_query_execution(QueryExecutionId=exec_id)
        print(f"Cancelled timed-out Athena query {exec_id}.")
    except Exception as cancel_err:
        print(f"Warning: could not cancel query {exec_id}: {cancel_err}")

    raise TimeoutError(
        f"Athena query {exec_id} did not complete within {timeout}s | SQL: {sql[:120]}"
    )


def _fetch_column(exec_id, col=0):
    """Returns all values from one column in the result set (skips header row)."""
    rows = athena.get_query_results(QueryExecutionId=exec_id)['ResultSet']['Rows']
    return [r['Data'][col]['VarCharValue'] for r in rows[1:]]


# ---------------------------------------------------------------------------
# Phase 1: GCP Discovery
# ---------------------------------------------------------------------------

def _discover_gcp_subscriptions():
    """
    Lists all Pub/Sub subscriptions in the GCP project.
    Returns [{"subscription": str, "topic": str}, ...]
    topic = '_deleted-topic_' when the backing topic has been deleted.
    """
    print(f"Discovering GCP subscriptions in project: {GCP_PROJECT_ID}")
    subscriber = pubsub_v1.SubscriberClient()
    result = [
        {"subscription": s.name, "topic": s.topic}
        for s in subscriber.list_subscriptions(
            request={"project": f"projects/{GCP_PROJECT_ID}"}
        )
    ]
    print(f"Found {len(result)} subscription(s) on GCP.")
    return result


# ---------------------------------------------------------------------------
# Phase 2: Registry Sync
# ---------------------------------------------------------------------------

def _ensure_table():
    """Creates the Iceberg registry table if it does not exist."""
    _run_query(f"""
    CREATE TABLE IF NOT EXISTS {ATHENA_DATABASE}.{ATHENA_TABLE} (
        subscription_name string,
        topic_name        string,
        last_seen_ts      timestamp,
        status            string,
        usage_group       string
    )
    LOCATION 's3://{ICEBERG_DATA_BUCKET}/{ATHENA_TABLE}/'
    TBLPROPERTIES (
        'table_type'='ICEBERG',
        'format'='parquet',
        'write_compression'='snappy',
        'optimize_rewrite_delete_file_threshold'='10'
    )
    """, timeout=60)

    # Safe migration for tables created before topic_name was added
    try:
        _run_query(
            f"ALTER TABLE {ATHENA_DATABASE}.{ATHENA_TABLE} ADD COLUMNS (topic_name string)",
            timeout=60
        )
    except Exception:
        pass  # Column already exists


def _upsert(gcp_subs):
    """
    MERGE all GCP-discovered subscriptions into the registry.
    - WHEN MATCHED:     update last_seen_ts and topic_name only
                        usage_group is NEVER touched — ops owns that column
                        if previously REMOVED and now back → reset to PENDING
    - WHEN NOT MATCHED: INSERT with usage_group='baseline', status='PENDING'
    """
    if not gcp_subs:
        print("No GCP subscriptions to upsert.")
        return

    # Use parameterized strings for the array builder
    values_placeholders = ",\n        ".join("(?, ?, current_timestamp)" for _ in gcp_subs)
    
    # Flatten the parameters array sequentially
    flat_params = []
    for s in gcp_subs:
        flat_params.extend([s['subscription'], s['topic']])

    resp = athena.start_query_execution(
        QueryString=f"""
        MERGE INTO {ATHENA_DATABASE}.{ATHENA_TABLE} target
        USING (
            SELECT * FROM (VALUES {values_placeholders}) AS t(sub_name, topic_nm, seen_ts)
        ) source
        ON target.subscription_name = source.sub_name
        WHEN MATCHED THEN
            UPDATE SET
                topic_name   = source.topic_nm,
                last_seen_ts = source.seen_ts,
                status = CASE WHEN target.status = 'REMOVED' THEN 'PENDING' ELSE target.status END
        WHEN NOT MATCHED THEN
            INSERT (subscription_name, topic_name, last_seen_ts, status, usage_group)
            VALUES (source.sub_name, source.topic_nm, source.seen_ts, 'PENDING', 'baseline')
        """,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT_LOC},
        ExecutionParameters=flat_params
    )

    exec_id = resp['QueryExecutionId']
    deadline = time.time() + ATHENA_POLL_TIMEOUT

    while time.time() < deadline:
        result = athena.get_query_execution(QueryExecutionId=exec_id)
        status = result['QueryExecution']['Status']
        state  = status['State']

        if state == 'SUCCEEDED':
            print("MERGE complete.")
            return
        if state in ('FAILED', 'CANCELLED'):
            reason = status.get('StateChangeReason', 'no reason returned')
            raise RuntimeError(f"Athena MERGE {exec_id} {state}: {reason}")
        time.sleep(2)

    # Deadline exceeded — cancel the orphaned MERGE query before raising
    try:
        athena.stop_query_execution(QueryExecutionId=exec_id)
        print(f"Cancelled timed-out Athena query {exec_id}.")
    except Exception as cancel_err:
        print(f"Warning: could not cancel query {exec_id}: {cancel_err}")

    raise TimeoutError(f"Athena MERGE {exec_id} timed out.")


def _mark_removed(gcp_subs):
    """
    Marks subscriptions REMOVED in two cases:
      1. Subscription deleted from GCP entirely
      2. Orphaned subscription — backing topic was deleted (topic='_deleted-topic_')
    Returns the count marked REMOVED.
    """
    live_names = {s['subscription'] for s in gcp_subs}
    orphaned   = {s['subscription'] for s in gcp_subs if s['topic'] == '_deleted-topic_'}

    exec_id     = _run_query(
        f"SELECT subscription_name FROM {ATHENA_DATABASE}.{ATHENA_TABLE} "
        f"WHERE status IN ('ACTIVE', 'PENDING')"
    )
    in_registry = set(_fetch_column(exec_id))

    to_remove = (in_registry - live_names) | orphaned
    if not to_remove:
        print("No removed or orphaned subscriptions.")
        return 0

    names_csv = ", ".join("?" for _ in to_remove)
    _run_query(f"""
    UPDATE {ATHENA_DATABASE}.{ATHENA_TABLE}
    SET status = 'REMOVED', last_seen_ts = current_timestamp
    WHERE subscription_name IN ({names_csv})
    """, params=list(to_remove))
    print(f"Marked {len(to_remove)} subscription(s) REMOVED.")
    return len(to_remove)


# ---------------------------------------------------------------------------
# Phase 3: Concurrent-safe on-demand patch trigger
# ---------------------------------------------------------------------------

def _is_patching_running_for_group(group):
    """
    Returns True if the Patching Step Function has a RUNNING execution whose
    input targets the given group.

    Paginates list_executions(statusFilter=RUNNING) and calls
    describe_execution on each to read its input JSON.  The number of
    concurrent executions is expected to be very small (one per group at
    most), so the extra describe calls are negligible.

    Required IAM permissions (on the state machine ARN):
      states:ListExecutions
      states:DescribeExecution
    """
    paginator = sfn.get_paginator('list_executions')
    for page in paginator.paginate(
        stateMachineArn=PATCHING_SF_ARN,
        statusFilter='RUNNING'
    ):
        for exe in page['executions']:
            try:
                detail = sfn.describe_execution(executionArn=exe['executionArn'])
                if json.loads(detail.get('input', '{}')).get('group') == group:
                    return True
            except Exception as err:
                # A single describe failure should not abort the entire run
                print(f"  Warning: could not inspect execution {exe['executionArn']}: {err}")
    return False


def _trigger_patching_for_groups_with_pending():
    """
    Finds all distinct groups with PENDING subscriptions, then triggers the
    Patching Step Function for each group that is NOT already being patched.
    Returns (triggered_list, skipped_list).
    """
    if not PATCHING_SF_ARN:
        print("PATCHING_SF_ARN not set — skipping on-demand trigger.")
        return [], []

    exec_id = _run_query(f"""
    SELECT DISTINCT usage_group FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
    WHERE status = 'PENDING'
    """)
    groups_with_pending = _fetch_column(exec_id)

    triggered = []
    skipped   = []

    for group in groups_with_pending:
        if _is_patching_running_for_group(group):
            print(
                f"Step Function already RUNNING for group '{group}' "
                f"— skipping on-demand trigger to prevent conflicting patches."
            )
            skipped.append(group)
            continue

        exec_name = f"ondemand-{group}-{int(time.time())}"
        print(f"PENDING topics found in group '{group}' — starting execution '{exec_name}'.")
        sfn.start_execution(
            stateMachineArn=PATCHING_SF_ARN,
            name=exec_name,
            input=json.dumps({"group": group, "trigger": "on-demand"})
        )
        triggered.append(group)

    if skipped:
        print(f"Groups skipped (already patching): {skipped}")

    return triggered, skipped


# ---------------------------------------------------------------------------
# Lambda Handler
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    """Runs on the hourly EventBridge schedule."""
    print("=== GCP Sync Discovery Service (v3) ===")

    _ensure_table()

    gcp_subs           = _discover_gcp_subscriptions()
    _upsert(gcp_subs)
    removed            = _mark_removed(gcp_subs)
    triggered, skipped = _trigger_patching_for_groups_with_pending()

    return {
        "status":           "SUCCESS",
        "total_gcp":        len(gcp_subs),
        "removed":          removed,
        "groups_triggered": triggered,
        "groups_skipped":   skipped,
    }


if __name__ == "__main__":
    os.environ["AWS_PROFILE"] = "terraform-firehose"
    print(lambda_handler({}, None))
