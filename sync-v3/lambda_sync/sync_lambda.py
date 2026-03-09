"""
GCP Sync — Discovery Service (v3)
===================================

Single responsibility: keep the Iceberg subscription registry in sync with
GCP Pub/Sub. No Kubernetes changes ever happen here.

Schedule: runs every 5 minutes via EventBridge (configurable in Terraform).

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
  4. Log execution summary to discovery_execution_log Iceberg table.

Patching is NOT triggered here.
  All KEDA / ConfigMap / Deployment patching is driven exclusively by
  per-group EventBridge cron rules in Terraform (every 4 hours, staggered
  15 minutes apart). This ensures that at most one group is ever restarting
  at a time — critical for safe operation at 700-800 topics across many groups.

  Ops workflow for reassigning topics to groups:
    UPDATE gcp_sync_db.subscription_registry
    SET usage_group = 'group2'
    WHERE subscription_name IN (...);
  The next scheduled patch for 'group2' will pick them up automatically.

Changes from v2
---------------
  - Pure registry-sync service. The on-demand patching trigger has been
    removed. Patching is driven solely by scheduled EventBridge rules.
  - Unified _run_query() helper supports ExecutionParameters for
    parameterized queries; _upsert() and _mark_removed() both use it.
  - Athena polling timeout: all polling loops enforce ATHENA_POLL_TIMEOUT
    (default 120s). Timed-out queries are cancelled before raising.
  - Execution audit log: each run writes a row to discovery_execution_log
    for observability (total topics seen, removals, timestamp).

Output payload
--------------
  {
    "total_gcp": 8,
    "removed":   2
  }
"""

import os
import time
import boto3
from google.cloud import pubsub_v1

GCP_PROJECT_ID      = os.environ.get("GCP_PROJECT_ID", "wired-sign-858")
ATHENA_DATABASE     = os.environ.get("ATHENA_DATABASE", "gcp_sync_db")
ATHENA_TABLE        = os.environ.get("ATHENA_TABLE", "subscription_registry")
ATHENA_OUTPUT_LOC   = os.environ.get("ATHENA_OUTPUT_LOC", "s3://YOUR-BUCKET/sync/")
ICEBERG_DATA_BUCKET = os.environ.get("ICEBERG_DATA_BUCKET", "YOUR-BUCKET")
ATHENA_POLL_TIMEOUT = int(os.environ.get("ATHENA_POLL_TIMEOUT", "120"))

athena = boto3.client('athena')


# ---------------------------------------------------------------------------
# Athena helpers
# ---------------------------------------------------------------------------

def _run_query(sql, params=None, timeout=None):
    """
    Executes a single Athena statement and polls until a terminal state.
    params: optional list of strings passed as ExecutionParameters (?).
    Raises RuntimeError on FAILED/CANCELLED.
    Raises TimeoutError if the deadline is exceeded; cancels the query first
    so it does not continue consuming Athena DPU capacity.
    """
    if timeout is None:
        timeout = ATHENA_POLL_TIMEOUT

    kwargs = {
        'QueryString':            sql,
        'QueryExecutionContext':  {'Database': ATHENA_DATABASE},
        'ResultConfiguration':    {'OutputLocation': ATHENA_OUTPUT_LOC},
    }
    if params:
        kwargs['ExecutionParameters'] = params

    resp     = athena.start_query_execution(**kwargs)
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

def _ensure_tables():
    """Creates the subscription registry and execution log tables if they do not exist."""
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

    _run_query(f"""
    CREATE TABLE IF NOT EXISTS {ATHENA_DATABASE}.discovery_execution_log (
        execution_ts     timestamp,
        total_gcp_topics int,
        removed_topics   int
    )
    LOCATION 's3://{ICEBERG_DATA_BUCKET}/discovery_execution_log/'
    TBLPROPERTIES (
        'table_type'='ICEBERG',
        'format'='parquet',
        'write_compression'='snappy'
    )
    """, timeout=60)


def _upsert(gcp_subs):
    """
    MERGE all GCP-discovered subscriptions into the registry.
    - WHEN MATCHED:     update last_seen_ts and topic_name only
                        usage_group is NEVER touched — ops owns that column
                        if previously REMOVED and now back → reset to PENDING
    - WHEN NOT MATCHED: INSERT with usage_group='baseline', status='PENDING'
    Uses ExecutionParameters (?) to prevent SQL injection from GCP names.
    """
    if not gcp_subs:
        print("No GCP subscriptions to upsert.")
        return

    values_placeholders = ",\n        ".join("(?, ?, current_timestamp)" for _ in gcp_subs)

    flat_params = []
    for s in gcp_subs:
        flat_params.extend([s['subscription'], s['topic']])

    _run_query(f"""
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
    """, params=flat_params)
    print("MERGE complete.")


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


def _log_execution(total_gcp, removed):
    """Appends a row to the execution audit log for observability."""
    _run_query(
        f"""
        INSERT INTO {ATHENA_DATABASE}.discovery_execution_log
        (execution_ts, total_gcp_topics, removed_topics)
        VALUES (current_timestamp, ?, ?)
        """,
        params=[str(total_gcp), str(removed)],
        timeout=60
    )


# ---------------------------------------------------------------------------
# Lambda Handler
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    """Runs on the EventBridge schedule (every 5 minutes)."""
    print("=== GCP Sync Discovery Service (v3) ===")

    _ensure_tables()

    gcp_subs = _discover_gcp_subscriptions()
    _upsert(gcp_subs)
    removed  = _mark_removed(gcp_subs)
    _log_execution(len(gcp_subs), removed)

    return {
        "status":    "SUCCESS",
        "total_gcp": len(gcp_subs),
        "removed":   removed,
    }


if __name__ == "__main__":
    os.environ["AWS_PROFILE"] = "terraform-firehose"
    print(lambda_handler({}, None))
