import os
import time
import json
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
    """Executes a single Athena query and waits for completion."""
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


def athena_query_results(execution_id):
    """Returns all data rows (skipping header) from a completed Athena query."""
    results = athena_client.get_query_results(QueryExecutionId=execution_id)
    return [row['Data'][0]['VarCharValue'] for row in results['ResultSet']['Rows'][1:]]


def discover_gcp_subscriptions():
    """Lists all Pub/Sub subscriptions in the GCP project."""
    print(f"Discovering GCP Subscriptions in project: {GCP_PROJECT_ID}...")
    subscriber = pubsub_v1.SubscriberClient()
    subs = [s.name for s in subscriber.list_subscriptions(request={"project": f"projects/{GCP_PROJECT_ID}"})]
    print(f"Found {len(subs)} subscriptions on GCP.")
    return subs


# ---------------------------------------------------------------------------
# Iceberg Registry Operations
# ---------------------------------------------------------------------------

def ensure_iceberg_table_exists():
    """Creates the Iceberg subscription_registry table if it doesn't already exist."""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {ATHENA_DATABASE}.{ATHENA_TABLE} (
        subscription_name string,
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


def upsert_subscriptions_to_iceberg(gcp_subs):
    """
    MERGE all currently-discovered GCP subscriptions into the registry.
    - New subscriptions  → INSERT  status='PENDING', usage_group='group1'
    - Known subscriptions → UPDATE  last_seen_ts, keep existing status/group
    """
    if not gcp_subs:
        print("No GCP subscriptions discovered. Skipping upsert.")
        return

    values_str = ",\n        ".join([f"('{s}', current_timestamp)" for s in gcp_subs])

    merge_query = f"""
    MERGE INTO {ATHENA_DATABASE}.{ATHENA_TABLE} target
    USING (
        SELECT * FROM (
            VALUES
            {values_str}
        ) AS t(sub_name, seen_ts)
    ) source
    ON target.subscription_name = source.sub_name
    WHEN MATCHED THEN
        UPDATE SET last_seen_ts = source.seen_ts
    WHEN NOT MATCHED THEN
        INSERT (subscription_name, last_seen_ts, status, usage_group)
        VALUES (source.sub_name, source.seen_ts, 'PENDING', 'group1')
    """
    print("Executing MERGE UPSERT into Iceberg...")
    execute_athena_query(merge_query)
    print("MERGE complete.")


def mark_removed_subscriptions(gcp_subs):
    """
    Finds ACTIVE registry topics that are NO LONGER in GCP and marks them REMOVED.
    This handles the negative drift case (topic deleted from GCP).
    Returns the count of topics marked REMOVED.
    """
    # Fetch all currently ACTIVE subscriptions from the registry
    exec_id = execute_athena_query(
        f"SELECT subscription_name FROM {ATHENA_DATABASE}.{ATHENA_TABLE} WHERE status = 'ACTIVE'"
    )
    active_in_registry = set(athena_query_results(exec_id))
    gcp_set = set(gcp_subs)

    removed = active_in_registry - gcp_set
    if not removed:
        print("Removal check: no removed subscriptions detected.")
        return 0

    print(f"Removal check: {len(removed)} topic(s) deleted from GCP → marking REMOVED: {removed}")
    names_csv = ", ".join([f"'{s}'" for s in removed])
    execute_athena_query(
        f"""UPDATE {ATHENA_DATABASE}.{ATHENA_TABLE}
        SET status = 'REMOVED', last_seen_ts = current_timestamp
        WHERE subscription_name IN ({names_csv})"""
    )
    return len(removed)


def get_group_drift_count(group):
    """
    Group-scoped drift gate.
    Returns count of topics in this group needing EKS patching (PENDING or REMOVED).
    PENDING = new topic added to this group
    REMOVED = topic deleted from GCP (needs to be dropped from KEDA/ConfigMap)
    """
    exec_id = execute_athena_query(
        f"""SELECT COUNT(*) FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
        WHERE usage_group = '{group}'
        AND status IN ('PENDING', 'REMOVED')"""
    )
    rows = athena_query_results(exec_id)
    count = int(rows[0]) if rows else 0
    print(f"Group '{group}' drift count: {count}")
    return count


def get_active_subscriptions_for_group(group):
    """
    Returns current ACTIVE subscriptions for the given group.
    These are passed to EKS patching to rebuild KEDA triggers and ConfigMap.
    REMOVED/PENDING are excluded — KEDA/ConfigMap are rebuilt from ACTIVE only.
    """
    exec_id = execute_athena_query(
        f"""SELECT subscription_name FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
        WHERE usage_group = '{group}'
        AND status = 'ACTIVE'"""
    )
    subs = athena_query_results(exec_id)
    print(f"Group '{group}' has {len(subs)} ACTIVE subscriptions for EKS patching.")
    return subs


# ---------------------------------------------------------------------------
# Lambda Handler
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    print("=== Phase 4: Discovery Orchestrator ===")

    # Which group is this invocation responsible for?
    # EventBridge passes {"group": "group1"} as the execution input.
    group = event.get('group', 'group1')
    print(f"Processing group: '{group}'")

    # 0. Ensure registry table exists
    ensure_iceberg_table_exists()

    # 1. Discover all current GCP subscriptions
    gcp_subs = discover_gcp_subscriptions()

    # 2. Upsert ALL discovered subscriptions (new → PENDING/group1, known → update ts)
    upsert_subscriptions_to_iceberg(gcp_subs)

    # 3. Mark topics that disappeared from GCP as REMOVED (negative drift)
    removed_count = mark_removed_subscriptions(gcp_subs)

    # 4. Group-scoped drift gate: count PENDING + REMOVED in this group
    drift_count = get_group_drift_count(group)

    # 5. Get the current ACTIVE subscriptions for this group (for EKS patching)
    #    NOTE: We send the ACTIVE list even if drift_count > 0 from REMOVED topics,
    #    so EKS rebuilds triggers/ConfigMap without the deleted topics.
    group_subs = get_active_subscriptions_for_group(group)

    return {
        "status":      "SUCCESS",
        "group":       group,
        "total_gcp":   len(gcp_subs),
        "drift_count": drift_count,
        "removed":     removed_count,
        "subscriptions": group_subs    # ACTIVE topics for this group only → EKS patching
    }


if __name__ == "__main__":
    os.environ["AWS_PROFILE"] = "terraform-firehose"
    lambda_handler({"group": "group1"}, None)
