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


def athena_fetch_column(execution_id):
    """Returns all values in the first column of the Athena result (skipping header row)."""
    results = athena_client.get_query_results(QueryExecutionId=execution_id)
    return [row['Data'][0]['VarCharValue'] for row in results['ResultSet']['Rows'][1:]]


# ---------------------------------------------------------------------------
# Phase 1: GCP Discovery
# ---------------------------------------------------------------------------

def discover_gcp_subscriptions():
    """Lists all Pub/Sub subscriptions currently live in the GCP project."""
    print(f"Discovering GCP Subscriptions in project: {GCP_PROJECT_ID}...")
    subscriber = pubsub_v1.SubscriberClient()
    subs = [s.name for s in subscriber.list_subscriptions(
        request={"project": f"projects/{GCP_PROJECT_ID}"}
    )]
    print(f"Found {len(subs)} subscriptions on GCP.")
    return subs


# ---------------------------------------------------------------------------
# Phase 2: Registry Operations
# ---------------------------------------------------------------------------

def ensure_iceberg_table_exists():
    """Creates the Iceberg subscription_registry table if it does not exist."""
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
    MERGE all GCP-discovered subscriptions into the registry.

    Rules:
      WHEN MATCHED  → only update last_seen_ts.
                       NEVER touch usage_group — that column is owned by ops.
                       NEVER touch status if already ACTIVE — only reset REMOVED→PENDING.
      WHEN NOT MATCHED → INSERT with usage_group='baseline', status='PENDING'.
                          All new topics land in baseline; ops promotes them later.
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
        UPDATE SET
            last_seen_ts = source.seen_ts,
            status = CASE WHEN target.status = 'REMOVED' THEN 'PENDING' ELSE target.status END
    WHEN NOT MATCHED THEN
        INSERT (subscription_name, last_seen_ts, status, usage_group)
        VALUES (source.sub_name, source.seen_ts, 'PENDING', 'baseline')
    """
    print("Executing MERGE UPSERT into Iceberg (usage_group is ops-owned, never overwritten)...")
    execute_athena_query(merge_query)
    print("MERGE complete.")


def mark_removed_subscriptions(gcp_subs):
    """
    Finds ACTIVE registry topics that are no longer in GCP and marks them REMOVED.
    This is the negative drift path — handles topics deleted from GCP.
    Returns the count of topics marked REMOVED.
    """
    exec_id = execute_athena_query(
        f"SELECT subscription_name FROM {ATHENA_DATABASE}.{ATHENA_TABLE} "
        f"WHERE status IN ('ACTIVE', 'PENDING')"
    )
    in_registry = set(athena_fetch_column(exec_id))
    disappeared = in_registry - set(gcp_subs)

    if not disappeared:
        print("No removed subscriptions detected.")
        return 0

    print(f"Marking {len(disappeared)} topic(s) REMOVED (deleted from GCP): {disappeared}")
    names_csv = ", ".join([f"'{s}'" for s in disappeared])
    execute_athena_query(
        f"""UPDATE {ATHENA_DATABASE}.{ATHENA_TABLE}
        SET status = 'REMOVED', last_seen_ts = current_timestamp
        WHERE subscription_name IN ({names_csv})"""
    )
    return len(disappeared)


# ---------------------------------------------------------------------------
# Phase 3: Group-scoped Query
# ---------------------------------------------------------------------------

def get_group_subscriptions(group):
    """
    Returns the current non-REMOVED subscriptions for this group.
    This is the list used to rebuild KEDA triggers and ConfigMap.
    REMOVED topics are excluded — the rebuild drops them automatically.

    drift_count = len(result):
      0  → group has no topics at all → skip K8s patching entirely
      >0 → always rebuild (idempotent, ensures group stays in sync with registry)
    """
    exec_id = execute_athena_query(
        f"""SELECT subscription_name FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
        WHERE usage_group = '{group}'
        AND status != 'REMOVED'"""
    )
    subs = athena_fetch_column(exec_id)
    print(f"Group '{group}': {len(subs)} active subscription(s).")
    return subs


# ---------------------------------------------------------------------------
# Lambda Handler
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    """
    Entry point for the Step Function DiscoverGCPSubscriptions state.

    EventBridge passes   {"group": "baseline"}  or  {"group": "group1"}  etc.
    The group determines which K8s resources to patch; discovery is always global.

    Output payload:
      {
        "status":        "SUCCESS",
        "group":         "baseline",
        "total_gcp":     8,
        "drift_count":   3,       ← topics in this group (0 = skip patching)
        "removed":       0,
        "subscriptions": [...]    ← topics for this group handed to EKS patcher
      }
    """
    print("=== Discovery Orchestrator: Phase 4 ===")

    # Which group is this invocation responsible for?
    group = event.get('group', 'baseline')
    print(f"Invoked for group: '{group}'")

    # Step 0: ensure registry table exists
    ensure_iceberg_table_exists()

    # Step 1: discover all current GCP subscriptions
    gcp_subs = discover_gcp_subscriptions()

    # Step 2: global upsert — new topics land in baseline; usage_group never overwritten
    upsert_subscriptions_to_iceberg(gcp_subs)

    # Step 3: negative drift — mark topics deleted from GCP as REMOVED
    removed_count = mark_removed_subscriptions(gcp_subs)

    # Step 4: get the current subscription list for THIS group (excludes REMOVED)
    group_subs = get_group_subscriptions(group)

    # drift_count = number of topics in this group
    # 0  → group is empty → Step Function will skip K8s patching
    # >0 → always rebuild this group's KEDA/ConfigMap (idempotent)
    drift_count = len(group_subs)

    return {
        "status":        "SUCCESS",
        "group":         group,
        "total_gcp":     len(gcp_subs),
        "drift_count":   drift_count,
        "removed":       removed_count,
        "subscriptions": group_subs
    }


if __name__ == "__main__":
    os.environ["AWS_PROFILE"] = "terraform-firehose"
    # Test baseline group
    result = lambda_handler({"group": "baseline"}, None)
    print(result)
