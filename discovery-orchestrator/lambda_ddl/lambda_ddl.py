import os
import time
import boto3

ATHENA_DATABASE = os.environ.get("ATHENA_DATABASE", "gcp_sync_db")
ATHENA_OUTPUT_LOC = os.environ.get("ATHENA_OUTPUT_LOC", "s3://YOUR-ATHENA-RESULTS-BUCKET/ddl/")
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

def format_table_name(subscription_name):
    """
    Converts a GCP subscription string into a valid Athena table name.
    Example: projects/wired-sign-858/subscriptions/my-topic-sub 
    Becomes: raw_my_topic_sub
    """
    # Extract just the subscription name part
    base_name = subscription_name.split('/')[-1]
    # Replace hyphens with underscores for SQL compatibility
    clean_name = base_name.replace('-', '_')
    return f"raw_{clean_name}"

def provision_iceberg_table(table_name):
    """
    Generates and executes the DDL to create the RAW Iceberg table
    for a specific GCP Topic.
    """
    print(f"Provisioning RAW Iceberg Table: {ATHENA_DATABASE}.{table_name}")
    
    # Example Schema matching the flattened event pattern
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {ATHENA_DATABASE}.{table_name} (
        message_id string,
        publish_time timestamp,
        payload string,
        attributes map<string, string>,
        ingestion_ts timestamp
    ) 
    LOCATION 's3://{ICEBERG_DATA_BUCKET}/{table_name}/'
    TBLPROPERTIES (
        'table_type'='ICEBERG',
        'format'='parquet',
        'write_compression'='snappy'
    );
    """
    execute_athena_query(ddl)

def lambda_handler(event, context):
    print("Beginning Phase 4: Auto-DDL Orchestrator...")
    
    # The Step Function wraps Lambda output under a 'Payload' key when passing between states.
    # Defensively unwrap it so this Lambda works whether invoked directly or via Step Functions.
    effective_event = event.get('Payload', event)
    
    if 'subscriptions' not in effective_event or not effective_event['subscriptions']:
        print("No subscriptions passed to DDL generator. Exiting.")
        return {"status": "SKIPPED", "tables_provisioned": [], "subscriptions": []}
    
    subscriptions = effective_event['subscriptions']
    tables_provisioned = []
    
    # 2. Iterate through discovered subscriptions and provision matching destination tables
    for sub in subscriptions:
        table_name = format_table_name(sub)
        provision_iceberg_table(table_name)
        tables_provisioned.append(table_name)
        
    print(f"Successfully ensured DDL exists for {len(tables_provisioned)} routing tables.")
    
    # 3. Output for the final EKS Patcher Step
    return {
        "status": "SUCCESS",
        "tables_provisioned": tables_provisioned,
        # Pass the original subscriptions forward so the EKS Patcher knows what to configure KEDA for
        "subscriptions": subscriptions
    }

if __name__ == "__main__":
    # Local dry-run testing block
    os.environ["AWS_PROFILE"] = "terraform-firehose"
    # Mocking Step Function Input
    mock_event = {
        "discovered_count": 2,
        "subscriptions": [
            "projects/wired-sign-858/subscriptions/test-payment-sub",
            "projects/wired-sign-858/subscriptions/test-streaming-sub"
        ]
    }
    lambda_handler(mock_event, None)
