# -----------------------------------------------------------------------------
# PHASE 4: Automated Discovery & Orchestration
# -----------------------------------------------------------------------------

provider "aws" {
  region = "us-east-1"
}

# -----------------------------------------------------------------------------
# 1. Package the Discovery Lambda
# -----------------------------------------------------------------------------
data "archive_file" "discovery_lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda"
  output_path = "${path.module}/discovery_lambda.zip"
}

resource "aws_lambda_function" "gcp_discovery" {
  filename         = data.archive_file.discovery_lambda_zip.output_path
  source_code_hash = data.archive_file.discovery_lambda_zip.output_base64sha256
  function_name    = "gcp-sync-discovery-orchestrator"
  role             = aws_iam_role.lambda_exec.arn
  handler          = "discovery_lambda.lambda_handler"
  runtime          = "python3.9"
  timeout          = 300 # 5 minutes max to query GCP and Athena

  environment {
    variables = {
      # In production, securely pull this from AWS Secrets Manager
      GCP_PROJECT_ID                 = "wired-sign-858"
      ATHENA_DATABASE                = "gcp_sync_db"
      ATHENA_TABLE                   = "subscription_registry"
      ATHENA_OUTPUT_LOC              = "s3://${aws_s3_bucket.athena_results.bucket}/discovery/"
      ICEBERG_DATA_BUCKET            = aws_s3_bucket.iceberg_data.bucket
      GOOGLE_APPLICATION_CREDENTIALS = "wired-sign-858-b289e61406fa.json"
    }
  }
}

# -----------------------------------------------------------------------------
# 2. Athena Setup (S3 Buckets)
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "athena_results" {
  bucket_prefix = "gcp-sync-athena-results-"
}

resource "aws_s3_bucket" "iceberg_data" {
  bucket_prefix = "gcp-sync-iceberg-data-"
}

resource "aws_athena_database" "gcp_sync" {
  name   = "gcp_sync_db"
  bucket = aws_s3_bucket.athena_results.id
}

# -----------------------------------------------------------------------------
# 1.5 Package the DDL Lambda
# -----------------------------------------------------------------------------
data "archive_file" "ddl_lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda_ddl"
  output_path = "${path.module}/lambda_ddl.zip"
}

resource "aws_lambda_function" "gcp_ddl" {
  filename         = data.archive_file.ddl_lambda_zip.output_path
  source_code_hash = data.archive_file.ddl_lambda_zip.output_base64sha256
  function_name    = "gcp-sync-ddl-orchestrator"
  role             = aws_iam_role.lambda_exec.arn
  handler          = "lambda_ddl.lambda_handler"
  runtime          = "python3.9"
  timeout          = 300 # 5 minutes max to execute Athena queries

  environment {
    variables = {
      ATHENA_DATABASE     = "gcp_sync_db"
      ATHENA_OUTPUT_LOC   = "s3://${aws_s3_bucket.athena_results.bucket}/ddl/"
      ICEBERG_DATA_BUCKET = aws_s3_bucket.iceberg_data.bucket
    }
  }
}

# -----------------------------------------------------------------------------
# 1.6 Package the EKS Patcher Lambda
# -----------------------------------------------------------------------------
data "archive_file" "eks_patcher_lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda_eks_patcher"
  output_path = "${path.module}/lambda_eks_patcher.zip"
}

resource "aws_lambda_function" "gcp_eks_patcher" {
  filename         = data.archive_file.eks_patcher_lambda_zip.output_path
  source_code_hash = data.archive_file.eks_patcher_lambda_zip.output_base64sha256
  function_name    = "gcp-sync-eks-patch-orchestrator"
  role             = aws_iam_role.lambda_exec.arn
  handler          = "lambda_autoscaler.lambda_handler"
  runtime          = "python3.9"
  timeout          = 60

  environment {
    variables = {
      EKS_CLUSTER_NAME = "gcp-sync-poc-test"
      EKS_REGION       = "us-east-1"
      NAMESPACE        = "default"
    }
  }
}

# -----------------------------------------------------------------------------
# 3. AWS Step Function (The Orchestrator)
# -----------------------------------------------------------------------------
resource "aws_sfn_state_machine" "onboarding_orchestrator" {
  name     = "GCPSync-Topic-Onboarding"
  role_arn = aws_iam_role.step_function_exec.arn

  definition = <<EOF
{
  "Comment": "Phase 4: GCP Subscription Discovery & Iceberg Pipeline Orchestrator",
  "StartAt": "DiscoverGCPSubscriptions",
  "States": {
    "DiscoverGCPSubscriptions": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "${aws_lambda_function.gcp_discovery.arn}",
        "Payload.$": "$"
      },
      "Retry": [
        {
          "ErrorEquals": [ "States.ALL" ],
          "IntervalSeconds": 10,
          "MaxAttempts": 3,
          "BackoffRate": 2.0
        }
      ],
      "Next": "EvaluateDrift"
    },
    "EvaluateDrift": {
      "Type": "Choice",
      "Choices": [
        {
          "Not": {
            "Variable": "$.Payload.drift_count",
            "NumericEquals": 0
          },
          "Next": "ProvisionIcebergTables"
        }
      ],
      "Default": "OrchestrationComplete"
    },
    "ProvisionIcebergTables": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "${aws_lambda_function.gcp_ddl.arn}",
        "Payload.$": "$.Payload"
      },
      "Retry": [
        {
          "ErrorEquals": [ "States.ALL" ],
          "IntervalSeconds": 10,
          "MaxAttempts": 3,
          "BackoffRate": 2.0
        }
      ],
      "Next": "PatchEKSKEDA"
    },
    "PatchEKSKEDA": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "${aws_lambda_function.gcp_eks_patcher.arn}",
        "Payload.$": "$.Payload"
      },
      "Retry": [
        {
          "ErrorEquals": [ "States.ALL" ],
          "IntervalSeconds": 10,
          "MaxAttempts": 3,
          "BackoffRate": 2.0
        }
      ],
      "Next": "OrchestrationComplete"
    },
    "OrchestrationComplete": {
      "Type": "Succeed"
    }
  }
}
EOF
}

# -----------------------------------------------------------------------------
# 4. Amazon EventBridge (Cron Trigger - Every 5 Minutes for Testing)
# -----------------------------------------------------------------------------
resource "aws_cloudwatch_event_rule" "every_five_minutes" {
  name                = "trigger-discovery-every-5-mins"
  description         = "Triggers the GCP Sync Step Function every 5 minutes"
  schedule_expression = "rate(5 minutes)"
}

resource "aws_cloudwatch_event_target" "step_function" {
  rule      = aws_cloudwatch_event_rule.every_five_minutes.name
  target_id = "TriggerGCPDiscoveryStepFunction"
  arn       = aws_sfn_state_machine.onboarding_orchestrator.arn
  role_arn  = aws_iam_role.eventbridge_exec.arn
}
