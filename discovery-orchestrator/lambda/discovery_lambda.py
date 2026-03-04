import os
import time
import boto3
from google.cloud import pubsub_v1

GCP_PROJECT_ID      = os.environ.get("GCP_PROJECT_ID", "wired-sign-858")
ATHENA_DATABASE     = os.environ.get("ATHENA_DATABASE", "gcp_sync_db")
ATHENA_TABLE        = os.environ.get("ATHENA_TABLE", "subscription_registry")
ATHENA_OUTPUT_LOC   = os.environ.get("ATHENA_OUTPUT_LOC", "s3://YOUR-ATHENA-RESULTS-BUCKET/discovery/")
ICEBERG_DATA_BUCKET = os.environ.get("ICEBERG_DATA_BUCKET", "YOUR-DATA-BUCKET")

athena_client = boto3.client('athena')

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def execute_athena_query(query_string):
    """Executes a single Athena SQL statement and waits for completion.
    Returns the query execution ID on success."""
    print(f"Executing Athena Query:\n{query_string}")
    response = athena_client.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT_LOC}
    )
    execution_id = response['QueryExecutionId']
    while True:
        status_resp = athena_client.get_query_execution(QueryExecutionId=execution_id)
        state = status_resp['QueryExecution']['Status']['State']
        if state == 'SUCCEEDED':
            return execution_id
        if state in ['FAILED', 'CANCELLED']:
            reason = status_resp['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
            raise Exception(f"Athena query failed: {state} - {reason}")
        time.sleep(1)


def athena_fetch_column(execution_id, col_index=0):
    """Returns all values in a specific column of the Athena result (skips header row)."""
    results = athena_client.get_query_results(QueryExecutionId=execution_id)
    return [row['Data'][col_index]['VarCharValue'] for row in results['ResultSet']['Rows'][1:]]


# ---------------------------------------------------------------------------
# Phase 1: GCP Discovery
# ---------------------------------------------------------------------------

def discover_gcp_subscriptions():
    """
    Lists all Pub/Sub subscriptions in the GCP project.

    Returns a list of dicts:
      [
        { "subscription": "projects/.../subscriptions/foo-sub",
          "topic":        "projects/.../topics/foo-topic"   },
        ...
      ]

    If a subscription's backing topic has been deleted, GCP sets topic="_deleted-topic_".
    We capture this so we can auto-detect orphaned subscriptions without needing the
    operator to manually delete the subscription.
    """
    print(f"Discovering GCP Subscriptions in project: {GCP_PROJECT_ID}...")
    subscriber = pubsub_v1.SubscriberClient()
    result = []
    for sub in subscriber.list_subscriptions(request={"project": f"projects/{GCP_PROJECT_ID}"}):
        result.append({
            "subscription": sub.name,
            "topic": sub.topic   # "_deleted-topic_" when backing topic is gone
        })
    print(f"Found {len(result)} subscription(s) on GCP.")
    return result


# ---------------------------------------------------------------------------
# Phase 2: Registry Operations
# ---------------------------------------------------------------------------

def ensure_iceberg_table_exists():
    """
    Creates the Iceberg subscription_registry table if it does not exist.
    Includes topic_name so we can track which topic backs each subscription.
    """
    ddl = f"""
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
    """
    execute_athena_query(ddl)

    # Safe migration: add topic_name if table already existed without it
    try:
        execute_athena_query(
            f"ALTER TABLE {ATHENA_DATABASE}.{ATHENA_TABLE} ADD COLUMNS (topic_name string)"
        )
        print("Migration: added topic_name column to existing table.")
    except Exception as e:
        if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
            pass  # Column already exists, safe to ignore
        else:
            print(f"ALTER TABLE note (non-fatal): {e}")


def upsert_subscriptions_to_iceberg(live_subs):
    """
    MERGE all currently-live GCP subscriptions into the registry.

    live_subs: list of { "subscription": str, "topic": str }

    Rules:
      WHEN MATCHED  → update last_seen_ts and topic_name only.
                       NEVER touch usage_group (ops-owned).
                       Restore status REMOVED→PENDING if topic reappears.
      WHEN NOT MATCHED → INSERT with usage_group='baseline', status='PENDING'.
    """
    if not live_subs:
        print("No live GCP subscriptions. Skipping upsert.")
        return

    values_str = ",\n        ".join([
        f"('{s['subscription']}', '{s['topic']}', current_timestamp)"
        for s in live_subs
    ])

    merge_query = f"""
    MERGE INTO {ATHENA_DATABASE}.{ATHENA_TABLE} target
    USING (
        SELECT * FROM (
            VALUES
            {values_str}
        ) AS t(sub_name, topic_nm, seen_ts)
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
    """
    print("Executing MERGE UPSERT (topic_name tracked, usage_group never overwritten)...")
    execute_athena_query(merge_query)
    print("MERGE complete.")


def mark_removed_subscriptions(live_subs):
    """
    Marks subscriptions as REMOVED in two cases:
    1. Subscription deleted from GCP entirely (not in list_subscriptions results)
    2. Subscription is orphaned — backing topic was deleted (topic = '_deleted-topic_')

    Returns count of topics newly marked REMOVED.
    """
    gcp_sub_names = set(s['subscription'] for s in live_subs)
    orphaned      = set(s['subscription'] for s in live_subs if s['topic'] == '_deleted-topic_')

    # Find subscriptions that exist in registry but are gone from GCP entirely
    exec_id = execute_athena_query(
        f"SELECT subscription_name FROM {ATHENA_DATABASE}.{ATHENA_TABLE} "
        f"WHERE status IN ('ACTIVE', 'PENDING')"
    )
    in_registry = set(athena_fetch_column(exec_id))

    deleted    = in_registry - gcp_sub_names   # completely gone from GCP
    to_remove  = deleted | orphaned             # union: gone OR orphaned

    if not to_remove:
        print("No removed or orphaned subscriptions detected.")
        return 0

    if orphaned:
        print(f"Orphaned subscriptions (topic deleted, sub still exists): {orphaned}")
    if deleted:
        print(f"Deleted subscriptions (gone from GCP entirely): {deleted}")

    names_csv = ", ".join([f"'{s}'" for s in to_remove])
    execute_athena_query(
        f"""UPDATE {ATHENA_DATABASE}.{ATHENA_TABLE}
        SET status = 'REMOVED', last_seen_ts = current_timestamp
        WHERE subscription_name IN ({names_csv})"""
    )
    return len(to_remove)


# ---------------------------------------------------------------------------
# Phase 3: Group-scoped Query
# ---------------------------------------------------------------------------

def get_group_subscriptions(group):
    """
    Returns the current non-REMOVED subscription names for this group.
    Used to rebuild the KEDA triggers array and ConfigMap.
    drift_count = len(result): 0 = skip patching, >0 = always rebuild.
    """
    exec_id = execute_athena_query(
        f"""SELECT subscription_name FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
        WHERE usage_group = '{group}'
        AND status != 'REMOVED'"""
    )
    subs = athena_fetch_column(exec_id)
    print(f"Group '{group}': {len(subs)} subscription(s) for EKS patching.")
    return subs


# ---------------------------------------------------------------------------
# Lambda Handler
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    """
    Entry point. EventBridge passes {"group": "baseline"} / {"group": "group1"} etc.

    Output payload handed to the Step Function:
      {
        "status":        "SUCCESS",
        "group":         "baseline",
        "total_gcp":     5,
        "drift_count":   5,     ← 0 = skip K8s patching
        "removed":       4,     ← topics marked REMOVED this run
        "subscriptions": [...]  ← non-REMOVED topics for this group → EKS patcher
      }
    """
    print("=== Discovery Orchestrator: Phase 4 ===")
    group = event.get('group', 'baseline')
    print(f"Invoked for group: '{group}'")

    # Step 0: ensure table (with topic_name column)
    ensure_iceberg_table_exists()

    # Step 1: discover all GCP subscriptions with their backing topic
    gcp_subs = discover_gcp_subscriptions()

    # Step 2: global upsert — new topics land in baseline/PENDING, topic_name always refreshed
    upsert_subscriptions_to_iceberg(gcp_subs)

    # Step 3: mark deleted subs + orphaned subs (topic="_deleted-topic_") as REMOVED
    removed_count = mark_removed_subscriptions(gcp_subs)

    # Step 4: group-scoped query — what does THIS group's K8s infra need?
    group_subs = get_group_subscriptions(group)

    return {
        "status":        "SUCCESS",
        "group":         group,
        "total_gcp":     len(gcp_subs),
        "drift_count":   len(group_subs),   # 0 = group empty, skip patching
        "removed":       removed_count,
        "subscriptions": group_subs
    }


if __name__ == "__main__":
    os.environ["AWS_PROFILE"] = "terraform-firehose"
    result = lambda_handler({"group": "baseline"}, None)
    print(result)
