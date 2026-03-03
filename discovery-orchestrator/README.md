# Phase 4: Discovery Orchestrator

This directory contains the foundational architecture for the Phase 4 fully-automated GCP Pub/Sub Auto-Subscription process.

## Architecture
- **AWS EventBridge**: Triggers the Step Function every 5 minutes (for testing).
- **AWS Step Function**: The master state machine orchestrating the discovery and rollout.
- **Python Lambda**: Authenticates to GCP, calls the `list_subscriptions` API, and executes an Athena `MERGE INTO` statement to UPSERT the results into an Administrative **Iceberg Registry Table**.

## How to Deploy (Terraform)

### 1. Vendor Python Dependencies
AWS Lambda requires external packages (like the Google Cloud SDK) to be zipped alongside the code. 
Run this to install them directly into the `/lambda/` directory before building the zip:
```bash
cd lambda
python3 -m pip install -r requirements.txt -t .
```

### 2. Inject GCP Credentials
For this Lambda to read from GCP, it needs the `wired-sign-858` JSON Service Account Key. 
*Note: In production, this should be stored in AWS Secrets Manager and retrieved via boto3. For this POC, we will simply copy the key you downloaded earlier into the lambda folder so it is zipped with the code.*

```bash
# Copy your existing key into the lambda package
cp /Users/sagarm/Downloads/wired-sign-858-b289e61406fa.json .

# Tell the python script where to find it when executing in the cloud
export GOOGLE_APPLICATION_CREDENTIALS="wired-sign-858-b289e61406fa.json"
```
*(You will need to manually add `GOOGLE_APPLICATION_CREDENTIALS` to the `environment.variables` block in `terraform/main.tf` pointing to the exact filename you copied).*

### 3. Terraform Apply
Ensure your `terraform-firehose` AWS profile is active.
```bash
cd ../terraform
terraform init
terraform apply
```

### 4. Verification
Once applied:
1. Go to the AWS Step Functions Console. You will see `GCPSync-Topic-Onboarding` executing every 5 minutes.
2. Go to AWS Athena. You will see the new `gcp_sync_db` database and the `subscription_registry` Iceberg table created automatically!
3. Run `SELECT * FROM gcp_sync_db.subscription_registry` to view the live topology AWS discovered from GCP!
