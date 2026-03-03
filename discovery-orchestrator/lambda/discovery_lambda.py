import os
import time
import json
import boto3
from google.cloud import pubsub_v1

# Ensure you map GCP credentials via env var or secure AWS Secrets Manager in production
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "wired-sign-858")
ATHENA_DATABASE = os.environ.get("ATHENA_DATABASE", "gcp_sync_db")
ATHENA_TABLE = os.environ.get("ATHENA_TABLE", "subscription_registry")
ATHENA_OUTPUT_LOC = os.environ.get("ATHENA_OUTPUT_LOC", "s3://YOUR-ATHENA-RESULTS-BUCKET/discovery/")
ICEBERG_DATA_BUCKET = os.environ.get("ICEBERG_DATA_BUCKET", "YOUR-DATA-BUCKET")

athena_client = boto3.client('athena')

def execute_athena_query(query_string):
    """Executes a query in Athena and waits for completion."""
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
        if state in ['SUCCEEDED']:
            return status_resp
        if state in ['FAILED', 'CANCELLED']:
            error_reason = status_resp['QueryExecution']['Status'].get('StateChangeReason', 'Unknown Error')
            raise Exception(f"Athena query failed: {state} - {error_reason}")
        time.sleep(1)

def discover_gcp_subscriptions():
    """Uses GCP Pub/Sub client to list all subscriptions accessible in the project."""
    print(f"Discovering GCP Subscriptions in project: {GCP_PROJECT_ID}...")
    subscriber = pubsub_v1.SubscriberClient()
    project_path = f"projects/{GCP_PROJECT_ID}"
    
    subscriptions_discovered = []
    # This lists all subscriptions in the project
    for subscription in subscriber.list_subscriptions(request={"project": project_path}):
        subscriptions_discovered.append(subscription.name)
        
    print(f"Found {len(subscriptions_discovered)} subscriptions on GCP.")
    return subscriptions_discovered

def ensure_iceberg_table_exists():
    """Creates the Iceberg Registry table if it doesn't already exist."""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {ATHENA_DATABASE}.{ATHENA_TABLE} (
        subscription_name string,
        last_seen_ts timestamp,
        status string,
        usage_group string
    ) 
    LOCATION 's3://{ICEBERG_DATA_BUCKET}/{ATHENA_TABLE}/'
    TBLPROPERTIES (
        'table_type'='ICEBERG',
        'format'='parquet',
        'write_compression'='snappy',
        'optimize_rewrite_delete_file_threshold'='10'
    );
    """
    execute_athena_query(ddl)

def upsert_subscriptions_to_iceberg(subscriptions):
    """
    Constructs a MERGE INTO statement to upsert the discovered subscriptions 
    into the Iceberg table. Unseen subscriptions shouldn't be deleted immediately, 
    but for this POC we will simply update the last_seen_ts for all discovered ones.
    """
    if not subscriptions:
        print("No subscriptions discovered. Skipping UPSERT.")
        return

    # Build an inline values table query to act as the source for the MERGE
    # Format: ( 'projects/.../sub-1', current_timestamp, 'ACTIVE', 'baseline' )
    values_raw = []
    for sub in subscriptions:
        # Note: Athena Iceberg requires strict timestamp formats, current_timestamp handles this
        values_raw.append(f"('{sub}', current_timestamp, 'ACTIVE', 'baseline')")
        
    values_str = ",\n        ".join(values_raw)
    
    merge_query = f"""
    MERGE INTO {ATHENA_DATABASE}.{ATHENA_TABLE} target
    USING (
        SELECT * FROM (
            VALUES 
            {values_str}
        ) AS t(sub_name, seen_ts, stat, u_group)
    ) source
    ON target.subscription_name = source.sub_name
    WHEN MATCHED THEN
        UPDATE SET 
            last_seen_ts = source.seen_ts,
            status = 'ACTIVE'
    WHEN NOT MATCHED THEN
        INSERT (subscription_name, last_seen_ts, status, usage_group)
        VALUES (source.sub_name, source.seen_ts, source.stat, source.u_group);
    """
    
    print("Executing MERGE UPSERT into Iceberg...")
    execute_athena_query(merge_query)
    print("Successfully Upserted GCP topology into Iceberg Registry.")

def calculate_drift(discovered_subs):
    """
    Compares newly discovered subscriptions against the Iceberg Registry.
    Returns the list of subscriptions that are entirely net-new.
    """
    if not discovered_subs:
        return []
        
    query = f"SELECT subscription_name FROM {ATHENA_DATABASE}.{ATHENA_TABLE}"
    try:
        # Start the query ourselves to capture the execution_id from the INITIAL response
        print(f"Drift check: querying {ATHENA_DATABASE}.{ATHENA_TABLE}...")
        start_resp = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': ATHENA_DATABASE},
            ResultConfiguration={'OutputLocation': ATHENA_OUTPUT_LOC}
        )
        execution_id = start_resp['QueryExecutionId']
        
        # Wait for it to complete
        while True:
            status_resp = athena_client.get_query_execution(QueryExecutionId=execution_id)
            state = status_resp['QueryExecution']['Status']['State']
            if state == 'SUCCEEDED':
                break
            if state in ['FAILED', 'CANCELLED']:
                raise Exception(f"Drift query failed: {state}")
            time.sleep(1)
        
        # Read results
        known_subs = set()
        results = athena_client.get_query_results(QueryExecutionId=execution_id)
        for row in results['ResultSet']['Rows'][1:]:  # skip header
            known_subs.add(row['Data'][0]['VarCharValue'])
            
        new_subs = [sub for sub in discovered_subs if sub not in known_subs]
        print(f"Drift: {len(known_subs)} already registered, {len(new_subs)} net-new.")
        return new_subs
        
    except Exception as e:
        print(f"Failed to calculate drift (treating all as new): {e}")
        return discovered_subs

def lambda_handler(event, context):
    print("Beginning Phase 4 Discovery Orchestrator...")
    
    # 0. Setup Table (Usually done by Terraform, but safe to run IF NOT EXISTS)
    ensure_iceberg_table_exists()
    
    # 1. Ask GCP what's available
    active_subs = discover_gcp_subscriptions()
    
    # 2. Evaluate Drift BEFORE Upserting
    new_subs = calculate_drift(active_subs)
    
    # 3. Write findings to Iceberg
    upsert_subscriptions_to_iceberg(active_subs)
    
    # 4. Step Function Output Payload
    # We output the total list for EKS, but pass the net-new count specifically for the Step Function Choice gate
    return {
        "status": "SUCCESS",
        "total_count": len(active_subs),
        "drift_count": len(new_subs),
        "subscriptions": active_subs
    }

if __name__ == "__main__":
    # Local dry-run testing block
    os.environ["AWS_PROFILE"] = "terraform-firehose"
    lambda_handler({}, None)
