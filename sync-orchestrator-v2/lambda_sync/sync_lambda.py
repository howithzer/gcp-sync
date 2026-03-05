"""
GCP Sync — Discovery Service (v2)
===================================

Single responsibility: keep the Iceberg subscription registry in sync with
GCP Pub/Sub. No Kubernetes changes ever happen here.

Schedule: runs hourly via EventBridge (configurable in Terraform).

Flow
----
  1. List all subscriptions from GCP Pub/Sub
  2. MERGE into Iceberg registry:
       New subscriptions  → INSERT  (usage_group='baseline', status='PENDING')
       Known subscriptions → UPDATE  last_seen_ts only
                             usage_group is NEVER overwritten — ops owns it
  3. Detect removals:
       Subscriptions gone from GCP entirely → status='REMOVED'
       Subscriptions whose backing topic was deleted (topic='_deleted-topic_')
       → status='REMOVED' (orphan detection)
  4. On-demand patching trigger:
       If any group has new PENDING topics, immediately start the Patching
       Step Function for that group — don't wait for the 4-hour patch window.

Output payload
--------------
  {
    "total_gcp":        8,
    "removed":          2,
    "groups_triggered": ["baseline"]   ← groups where patching was triggered early
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
PATCHING_SF_ARN     = os.environ.get("PATCHING_SF_ARN", "")  # Patching Step Function ARN

athena = boto3.client('athena')
sfn    = boto3.client('stepfunctions')


# ---------------------------------------------------------------------------
# Athena helpers
# ---------------------------------------------------------------------------

def _run_query(sql):
    """Executes a single Athena statement and waits. Returns execution ID."""
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT_LOC}
    )
    exec_id = resp['QueryExecutionId']
    while True:
        state = athena.get_query_execution(
            QueryExecutionId=exec_id
        )['QueryExecution']['Status']['State']
        if state == 'SUCCEEDED':
            return exec_id
        if state in ('FAILED', 'CANCELLED'):
            raise Exception(f"Athena query failed ({state}): {sql[:120]}")
        time.sleep(1)


def _fetch_column(exec_id, col=0):
    """Returns all values from a column in the result set (skips header row)."""
    rows = athena.get_query_results(QueryExecutionId=exec_id)['ResultSet']['Rows']
    return [r['Data'][col]['VarCharValue'] for r in rows[1:]]


# ---------------------------------------------------------------------------
# Phase 1: GCP Discovery
# ---------------------------------------------------------------------------

def _discover_gcp_subscriptions():
    """
    Lists all Pub/Sub subscriptions in the GCP project.
    Returns a list of dicts: [{"subscription": str, "topic": str}, ...]
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
    """)
    # Safe migration for tables created before topic_name was added
    try:
        _run_query(
            f"ALTER TABLE {ATHENA_DATABASE}.{ATHENA_TABLE} ADD COLUMNS (topic_name string)"
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

    values = ",\n        ".join(
        [f"('{s['subscription']}', '{s['topic']}', current_timestamp)" for s in gcp_subs]
    )
    _run_query(f"""
    MERGE INTO {ATHENA_DATABASE}.{ATHENA_TABLE} target
    USING (
        SELECT * FROM (VALUES {values}) AS t(sub_name, topic_nm, seen_ts)
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
    """)
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

    exec_id    = _run_query(
        f"SELECT subscription_name FROM {ATHENA_DATABASE}.{ATHENA_TABLE} "
        f"WHERE status IN ('ACTIVE', 'PENDING')"
    )
    in_registry = set(_fetch_column(exec_id))

    to_remove = (in_registry - live_names) | orphaned
    if not to_remove:
        print("No removed or orphaned subscriptions.")
        return 0

    names_csv = ", ".join([f"'{s}'" for s in to_remove])
    _run_query(f"""
    UPDATE {ATHENA_DATABASE}.{ATHENA_TABLE}
    SET status = 'REMOVED', last_seen_ts = current_timestamp
    WHERE subscription_name IN ({names_csv})
    """)
    print(f"Marked {len(to_remove)} subscription(s) REMOVED.")
    return len(to_remove)


# ---------------------------------------------------------------------------
# Phase 3: On-demand patch trigger
# ---------------------------------------------------------------------------

def _trigger_patching_for_groups_with_pending():
    """
    Finds all distinct groups that have PENDING subscriptions and immediately
    starts the Patching Step Function for each. This gives new topics a fast
    path to production without waiting for the scheduled patch window.
    """
    if not PATCHING_SF_ARN:
        print("PATCHING_SF_ARN not set — skipping on-demand trigger.")
        return []

    exec_id = _run_query(f"""
    SELECT DISTINCT usage_group FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
    WHERE status = 'PENDING'
    """)
    groups_with_pending = _fetch_column(exec_id)

    triggered = []
    for group in groups_with_pending:
        print(f"New PENDING topics in group '{group}' — triggering patching immediately.")
        sfn.start_execution(
            stateMachineArn=PATCHING_SF_ARN,
            input=json.dumps({"group": group, "trigger": "on-demand"})
        )
        triggered.append(group)

    return triggered


# ---------------------------------------------------------------------------
# Lambda Handler
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    """
    Runs on the hourly EventBridge schedule.
    Syncs GCP → registry; optionally triggers patching for groups with new topics.
    """
    print("=== GCP Sync Discovery Service (v2) ===")

    _ensure_table()

    gcp_subs = _discover_gcp_subscriptions()
    _upsert(gcp_subs)
    removed = _mark_removed(gcp_subs)
    triggered = _trigger_patching_for_groups_with_pending()

    return {
        "status":           "SUCCESS",
        "total_gcp":        len(gcp_subs),
        "removed":          removed,
        "groups_triggered": triggered
    }


if __name__ == "__main__":
    os.environ["AWS_PROFILE"] = "terraform-firehose"
    print(lambda_handler({}, None))
