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
  function_name    = "gcp-sync-eks-keda-patcher"
  role             = aws_iam_role.lambda_exec.arn
  handler          = "lambda_autoscaler.keda_handler"
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

# Second Lambda entry point — same zip, different handler
# Invoked AFTER a 15s Wait state so KEDA has time to reconcile
resource "aws_lambda_function" "gcp_configmap_patcher" {
  filename         = data.archive_file.eks_patcher_lambda_zip.output_path
  source_code_hash = data.archive_file.eks_patcher_lambda_zip.output_base64sha256
  function_name    = "gcp-sync-eks-configmap-patcher"
  role             = aws_iam_role.lambda_exec.arn
  handler          = "lambda_autoscaler.configmap_handler"
  runtime          = "python3.9"
  timeout          = 60

  environment {
    variables = {
      EKS_CLUSTER_NAME  = "gcp-sync-poc-test"
      EKS_REGION        = "us-east-1"
      NAMESPACE         = "default"
      ATHENA_DATABASE   = "gcp_sync_db"
      ATHENA_TABLE      = "subscription_registry"
      ATHENA_OUTPUT_LOC = "s3://${aws_s3_bucket.athena_results.bucket}/ddl/"
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
      "Next": "WaitForKEDAReconcile"
    },
    "WaitForKEDAReconcile": {
      "Type": "Wait",
      "Seconds": 15,
      "Comment": "Give KEDA 15s to process the new ScaledObject before restarting pods",
      "Next": "PatchConfigMap"
    },
    "PatchConfigMap": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "${aws_lambda_function.gcp_configmap_patcher.arn}",
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

# -----------------------------------------------------------------------------
# 5. Alerting Stack: SNS + CloudWatch Alarm + EventBridge failure events
# -----------------------------------------------------------------------------

variable "alert_email" {
  description = "Email address to receive pipeline failure alerts"
  type        = string
  default     = "ops-team@example.com"  # Override with: terraform apply -var="alert_email=you@example.com"
}

# SNS topic — single target for all pipeline alerts
resource "aws_sns_topic" "pipeline_alerts" {
  name = "gcp-sync-pipeline-alerts"
}

resource "aws_sns_topic_subscription" "alert_email" {
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# --- Alert Path 1: EventBridge → SNS (immediate, event-driven) ---
# Fires within seconds of any Step Function execution failure.
# Covers: FAILED, TIMED_OUT, ABORTED states.
resource "aws_cloudwatch_event_rule" "step_function_failure" {
  name        = "gcp-sync-pipeline-failure"
  description = "Routes Step Function failures to SNS immediately"
  event_pattern = jsonencode({
    source      = ["aws.states"]
    detail-type = ["Step Functions Execution Status Change"]
    detail = {
      stateMachineArn = [aws_sfn_state_machine.onboarding_orchestrator.arn]
      status          = ["FAILED", "TIMED_OUT", "ABORTED"]
    }
  })
}

resource "aws_cloudwatch_event_target" "notify_on_failure" {
  rule      = aws_cloudwatch_event_rule.step_function_failure.name
  target_id = "SendFailureAlertToSNS"
  arn       = aws_sns_topic.pipeline_alerts.arn

  input_transformer {
    input_paths = {
      execution = "$.detail.executionArn"
      status    = "$.detail.status"
      reason    = "$.detail.cause"
    }
    input_template = "\"GCP Sync Pipeline FAILED\\nStatus: <status>\\nExecution: <execution>\\nReason: <reason>\""
  }
}

# Allow EventBridge to publish to the SNS topic
resource "aws_sns_topic_policy" "allow_eventbridge" {
  arn = aws_sns_topic.pipeline_alerts.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sns:Publish"
      Resource  = aws_sns_topic.pipeline_alerts.arn
    }]
  })
}

# --- Alert Path 2: CloudWatch Alarm → SNS (metric-based safety net) ---
# Triggers if any Lambda errors accumulate even within a successful execution.
resource "aws_cloudwatch_metric_alarm" "step_function_failures" {
  alarm_name          = "gcp-sync-pipeline-executions-failed"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 300  # 5-minute window
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Fires when any GCP Sync Step Function execution fails"
  treat_missing_data  = "notBreaching"

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.onboarding_orchestrator.arn
  }

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
  ok_actions    = [aws_sns_topic.pipeline_alerts.arn]  # Also notify when it recovers
}

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "gcp-sync-lambda-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 3    # Allow up to 3 retries before alerting
  alarm_description   = "Fires when Lambda errors exceed retry budget across all pipeline Lambdas"
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
}
