"""
Terraform — GCP Sync Orchestrator v2 (Decoupled Architecture)
================================================================

Two independent services with separate schedules:

  Discovery Service  (rate = 1 hour)
  ─────────────────
  EventBridge → Lambda: gcp-sync-discovery-v2
    - Discovers all GCP subscriptions
    - Upserts into Iceberg registry (new → baseline/PENDING)
    - Marks REMOVED (deleted or orphaned subscriptions)
    - Triggers Patching Step Function immediately if new PENDING topics found

  Patching Service  (rate = 4 hours, one rule per group)
  ───────────────────────────────────────────────────────
  EventBridge → Step Function: GCPSync-Patcher-v2
    State 1: PatchKEDA      → Lambda: gcp-sync-patcher-keda-v2
    State 2: WaitForKEDA    → 15 seconds
    State 3: PatchConfigMap → Lambda: gcp-sync-patcher-configmap-v2
"""

terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

provider "aws" {
  region = "us-east-1"
}

data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = "us-east-1"

  # ── lambda source zips ──────────────────────────────────────────────────
  sync_lambda_source_dir    = "${path.module}/../lambda_sync"
  patcher_lambda_source_dir = "${path.module}/../lambda_patcher"

  # ── environment shared by both lambdas ──────────────────────────────────
  common_env = {
    ATHENA_DATABASE    = "gcp_sync_db"
    ATHENA_TABLE       = "subscription_registry"
    ATHENA_OUTPUT_LOC  = "s3://${aws_s3_bucket.athena_results.bucket}/patcher/"
    ICEBERG_DATA_BUCKET = aws_s3_bucket.iceberg_data.bucket
    EKS_CLUSTER_NAME   = "gcp-sync-cluster"
    EKS_REGION         = "us-east-1"
    NAMESPACE          = "default"
    GCP_PROJECT_ID     = "wired-sign-858"
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# S3 Buckets
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_s3_bucket" "iceberg_data" {
  bucket_prefix = "gcp-sync-iceberg-data-"
  force_destroy = true
}

resource "aws_s3_bucket" "athena_results" {
  bucket_prefix = "gcp-sync-athena-results-"
  force_destroy = true
}

# ─────────────────────────────────────────────────────────────────────────────
# IAM — Shared Lambda Role
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_iam_role" "lambda_exec" {
  name = "gcp_sync_v2_lambda_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_permissions" {
  name = "GCPSyncV2LambdaPermissions"
  role = aws_iam_role.lambda_exec.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AthenaAndS3"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution", "athena:GetQueryExecution",
          "athena:GetQueryResults",     "athena:StopQueryExecution"
        ]
        Resource = "*"
      },
      {
        Sid    = "S3DataAndResults"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.iceberg_data.arn,    "${aws_s3_bucket.iceberg_data.arn}/*",
          aws_s3_bucket.athena_results.arn,  "${aws_s3_bucket.athena_results.arn}/*"
        ]
      },
      {
        Sid    = "GlueForIceberg"
        Effect = "Allow"
        Action = ["glue:GetDatabase", "glue:GetTable", "glue:CreateTable",
                  "glue:UpdateTable", "glue:DeleteTable", "glue:GetPartitions"]
        Resource = "*"
      },
      {
        Sid    = "EKSAndSTS"
        Effect = "Allow"
        Action = ["eks:DescribeCluster", "sts:GetCallerIdentity"]
        Resource = "*"
      },
      {
        Sid    = "SecretForGCP"
        Effect = "Allow"
        Action = ["secretsmanager:GetSecretValue"]
        Resource = "arn:aws:secretsmanager:${local.region}:${local.account_id}:secret:gcp-service-account*"
      },
      {
        Sid      = "TriggerPatchingSF"
        Effect   = "Allow"
        Action   = ["states:StartExecution"]
        Resource = aws_sfn_state_machine.patcher.arn
      }
    ]
  })
}

# ─────────────────────────────────────────────────────────────────────────────
# Lambda — Discovery (Sync Only)
# ─────────────────────────────────────────────────────────────────────────────

data "archive_file" "sync_lambda_zip" {
  type        = "zip"
  source_dir  = local.sync_lambda_source_dir
  output_path = "${path.module}/sync_lambda.zip"
}

resource "aws_lambda_function" "discovery" {
  function_name    = "gcp-sync-discovery-v2"
  description      = "Discovers GCP Pub/Sub subscriptions and syncs to Iceberg registry. No K8s changes."
  role             = aws_iam_role.lambda_exec.arn
  filename         = data.archive_file.sync_lambda_zip.output_path
  source_code_hash = data.archive_file.sync_lambda_zip.output_base64sha256
  runtime          = "python3.9"
  handler          = "sync_lambda.lambda_handler"
  timeout          = 300
  memory_size      = 256

  environment {
    variables = merge(local.common_env, {
      PATCHING_SF_ARN = aws_sfn_state_machine.patcher.arn
    })
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# Lambda — Patcher (K8s Only)
# Two handlers in one zip: keda_handler and configmap_handler
# ─────────────────────────────────────────────────────────────────────────────

data "archive_file" "patcher_lambda_zip" {
  type        = "zip"
  source_dir  = local.patcher_lambda_source_dir
  output_path = "${path.module}/patcher_lambda.zip"
}

resource "aws_lambda_function" "patcher_keda" {
  function_name    = "gcp-sync-patcher-keda-v2"
  description      = "Patches KEDA ScaledObject for a group. Reads desired state from Iceberg registry."
  role             = aws_iam_role.lambda_exec.arn
  filename         = data.archive_file.patcher_lambda_zip.output_path
  source_code_hash = data.archive_file.patcher_lambda_zip.output_base64sha256
  runtime          = "python3.9"
  handler          = "patcher_lambda.keda_handler"
  timeout          = 120
  memory_size      = 256

  environment { variables = local.common_env }

  # ── EKS Private Cluster Networking ──────────────────────────────────────────
  # Uncomment the block below if the EKS cluster uses a private-only API endpoint.
  # Note: The Lambda VPC must have VPC Endpoints (or a NAT Gateway) for Athena, S3, and STS.
  #
  # vpc_config {
  #   subnet_ids         = ["subnet-xxxxxxxxx", "subnet-yyyyyyyyy"] # Private subnets in EKS VPC
  #   security_group_ids = ["sg-zzzzzzzzzzzz"]                      # Must allow outbound HTTPS (443)
  # }
}

resource "aws_lambda_function" "patcher_configmap" {
  function_name    = "gcp-sync-patcher-configmap-v2"
  description      = "Patches ConfigMap + rolling restart for a group. Marks PENDING → ACTIVE in registry."
  role             = aws_iam_role.lambda_exec.arn
  filename         = data.archive_file.patcher_lambda_zip.output_path
  source_code_hash = data.archive_file.patcher_lambda_zip.output_base64sha256
  runtime          = "python3.9"
  handler          = "patcher_lambda.configmap_handler"
  timeout          = 120
  memory_size      = 256

  environment { variables = local.common_env }

  # ── EKS Private Cluster Networking ──────────────────────────────────────────
  # Uncomment the block below if the EKS cluster uses a private-only API endpoint.
  # Note: The Lambda VPC must have VPC Endpoints (or a NAT Gateway) for Athena, S3, and STS.
  #
  # vpc_config {
  #   subnet_ids         = ["subnet-xxxxxxxxx", "subnet-yyyyyyyyy"] # Private subnets in EKS VPC
  #   security_group_ids = ["sg-zzzzzzzzzzzz"]                      # Must allow outbound HTTPS (443)
  # }
}

# ─────────────────────────────────────────────────────────────────────────────
# Step Function — Patching Pipeline
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_iam_role" "sfn_exec" {
  name = "gcp_sync_v2_sfn_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "sfn_invoke_lambda" {
  name = "InvokePatcherLambdas"
  role = aws_iam_role.sfn_exec.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "lambda:InvokeFunction"
      Resource = [
        aws_lambda_function.patcher_keda.arn,
        aws_lambda_function.patcher_configmap.arn
      ]
    }]
  })
}

resource "aws_sfn_state_machine" "patcher" {
  name     = "GCPSync-Patcher-v2"
  role_arn = aws_iam_role.sfn_exec.arn

  definition = jsonencode({
    Comment : "Patches KEDA and ConfigMap for a single group. Input: {group: 'baseline'}"
    StartAt : "PatchKEDA"
    States : {
      PatchKEDA : {
        Type     : "Task"
        Resource : aws_lambda_function.patcher_keda.arn
        Next     : "EvaluateDrift"
        ResultPath : "$.Payload"
      }
      EvaluateDrift : {
        Type : "Choice"
        Choices : [{
          Variable      : "$.Payload.status"
          StringEquals  : "SKIPPED"
          Next          : "OrchestrationComplete"
        }]
        Default : "WaitForKEDAReconcile"
      }
      WaitForKEDAReconcile : {
        Type    : "Wait"
        Seconds : 15
        Next    : "PatchConfigMap"
      }
      PatchConfigMap : {
        Type     : "Task"
        Resource : aws_lambda_function.patcher_configmap.arn
        Next     : "OrchestrationComplete"
        Parameters : {
          "group.$"         : "$.Payload.group"
          "subscriptions.$" : "$.Payload.subscriptions"
        }
      }
      OrchestrationComplete : {
        Type : "Succeed"
      }
    }
  })
}

# ─────────────────────────────────────────────────────────────────────────────
# EventBridge — Discovery Schedule (hourly)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_iam_role" "eventbridge_exec" {
  name = "gcp_sync_v2_eventbridge_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "events.amazonaws.com" }
        Action    = "sts:AssumeRole"
      },
      {
        # Also allowed to start Step Function executions (for patching schedule)
        Effect    = "Allow"
        Principal = { Service = "scheduler.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "eventbridge_permissions" {
  name = "EventBridgeV2Permissions"
  role = aws_iam_role.eventbridge_exec.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "lambda:InvokeFunction"
        Resource = aws_lambda_function.discovery.arn
      },
      {
        Effect   = "Allow"
        Action   = "states:StartExecution"
        Resource = aws_sfn_state_machine.patcher.arn
      }
    ]
  })
}

# Discovery: runs hourly, globally (no group parameter needed)
resource "aws_cloudwatch_event_rule" "discovery_hourly" {
  name                = "gcp-sync-discovery-hourly"
  description         = "Syncs GCP subscriptions to Iceberg registry every hour"
  schedule_expression = "rate(1 hour)"
}

resource "aws_cloudwatch_event_target" "discovery" {
  rule      = aws_cloudwatch_event_rule.discovery_hourly.name
  target_id = "TriggerDiscovery"
  arn       = aws_lambda_function.discovery.arn
  role_arn  = aws_iam_role.eventbridge_exec.arn
}

resource "aws_lambda_permission" "allow_eventbridge_discovery" {
  statement_id  = "AllowEventBridgeInvokeDiscovery"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.discovery.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.discovery_hourly.arn
}

# ─────────────────────────────────────────────────────────────────────────────
# EventBridge — Patching Schedules (per group, every 4 hours)
# Add more groups by duplicating the pattern below.
# ─────────────────────────────────────────────────────────────────────────────

locals {
  patch_groups = {
    # Staggered cron schedules to prevent simultaneous pod restarts across groups.
    # Format: cron(Minutes Hours Day-of-month Month Day-of-week Year)
    baseline = "cron(0 0,4,8,12,16,20 * * ? *)"  # Top of the hour: 00:00, 04:00...
    group1   = "cron(15 0,4,8,12,16,20 * * ? *)" # 15 mins later:   00:15, 04:15...
    group2   = "cron(30 0,8,16 * * ? *)"         # Every 8 hours, at the half-hour
  }
}

resource "aws_cloudwatch_event_rule" "patching" {
  for_each            = local.patch_groups
  name                = "gcp-sync-patch-${each.key}"
  description         = "Triggers K8s patching for group '${each.key}' on schedule: ${each.value}"
  schedule_expression = each.value
}

resource "aws_cloudwatch_event_target" "patching" {
  for_each  = local.patch_groups
  rule      = aws_cloudwatch_event_rule.patching[each.key].name
  target_id = "TriggerPatchFor${title(each.key)}"
  arn       = aws_sfn_state_machine.patcher.arn
  role_arn  = aws_iam_role.eventbridge_exec.arn
  input     = jsonencode({ group = each.key, trigger = "scheduled" })
}

# ─────────────────────────────────────────────────────────────────────────────
# Athena Database
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_athena_database" "gcp_sync" {
  name   = "gcp_sync_db"
  bucket = aws_s3_bucket.athena_results.bucket
}
