/*
Terraform — GCP Sync Orchestrator v3
======================================

Two independent services with cleanly separated responsibilities:

  Discovery Service  (every 5 minutes)
  ─────────────────
  EventBridge → Lambda: gcp-sync-discovery-v3
    - Discovers all GCP Pub/Sub subscriptions from GCP
    - Upserts into Iceberg registry (new → baseline/PENDING)
    - Marks REMOVED (deleted or orphaned subscriptions)
    - Writes execution summary to discovery_execution_log
    - Does NOT trigger patching — registry sync only

  Patching Service  (every 4 hours, one staggered rule per group)
  ───────────────────────────────────────────────────────────────
  EventBridge → Step Function: GCPSync-Topic-Onboarding
    State 1: PatchKEDA      → Lambda: gcp-sync-patcher-keda-v3
    State 2: WaitForKEDA    → 15 seconds
    State 3: PatchConfigMap → Lambda: gcp-sync-patcher-configmap-v3
               [v3] Creates KEDA/ConfigMap if missing (bootstrap-safe)
               [v3] Verifies Deployment exists before restarting
               [v3] Polls Deployment rollout until all pods are Ready
               [v3] Only marks ACTIVE after rollout confirms healthy pods

  Scheduling design
  -----------------
  Groups are staggered 15 minutes apart so at most one group is patching
  at any given time. With ~100-150 topics per group this keeps each rolling
  restart blast radius bounded and predictable.

  To add a new group: add an entry to local.patch_groups and run terraform apply.
  To reassign topics between groups, run an Athena UPDATE on usage_group;
  the next scheduled patch for each affected group reconciles automatically.

v3 Lambda changes vs v2:
  - patcher_configmap timeout raised to 360s (accommodates rollout polling)
  - New env vars: KEDA_SUBSCRIPTION_SIZE, KEDA_MIN_REPLICAS,
    KEDA_MAX_REPLICAS, KEDA_POLLING_INTERVAL, ROLLOUT_TIMEOUT_SECONDS,
    ATHENA_POLL_TIMEOUT
*/

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

  sync_lambda_source_dir    = "${path.module}/../lambda_sync"
  patcher_lambda_source_dir = "${path.module}/../lambda_patcher"

  # ── environment shared by both lambdas ──────────────────────────────────
  common_env = {
    ATHENA_DATABASE      = "gcp_sync_db_v3"
    ATHENA_TABLE         = "subscription_registry"
    ATHENA_OUTPUT_LOC    = "s3://${aws_s3_bucket.athena_results.bucket}/patcher/"
    ICEBERG_DATA_BUCKET  = aws_s3_bucket.iceberg_data.bucket
    EKS_CLUSTER_NAME     = "gcp-sync-poc-test"
    EKS_REGION           = "us-east-1"
    NAMESPACE            = "default"
    GCP_PROJECT_ID       = "wired-sign-858"
    ATHENA_POLL_TIMEOUT  = "120"
  }

  # ── KEDA tuning shared by all patcher lambdas ───────────────────────────
  keda_env = {
    KEDA_SUBSCRIPTION_SIZE = "5"   # message lag threshold per trigger
    KEDA_MIN_REPLICAS      = "0"   # scale to zero when idle
    KEDA_MAX_REPLICAS      = "10"
    KEDA_POLLING_INTERVAL  = "30"  # seconds between KEDA lag checks
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
  name = "gcp_discovery_lambda_role"
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
  name = "GCPSyncV3LambdaPermissions"
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
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [
          aws_s3_bucket.iceberg_data.arn,   "${aws_s3_bucket.iceberg_data.arn}/*",
          aws_s3_bucket.athena_results.arn, "${aws_s3_bucket.athena_results.arn}/*"
        ]
      },
      {
        Sid    = "GlueForIceberg"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetTable", "glue:CreateTable",
          "glue:UpdateTable", "glue:DeleteTable", "glue:GetPartitions"
        ]
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
      # Note: no states:* permissions for the Lambda role.
      # The Discovery Lambda is a pure registry-sync service.
      # All patching is triggered by EventBridge scheduled rules (see below).
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
  function_name    = "gcp-sync-discovery-v3"
  description      = "Discovers GCP Pub/Sub subscriptions and syncs to Iceberg registry. No K8s changes."
  role             = aws_iam_role.lambda_exec.arn
  filename         = data.archive_file.sync_lambda_zip.output_path
  source_code_hash = data.archive_file.sync_lambda_zip.output_base64sha256
  runtime          = "python3.12"
  handler          = "sync_lambda.lambda_handler"
  timeout          = 300
  memory_size      = 256

  environment {
    variables = merge(local.common_env, {
      GOOGLE_APPLICATION_CREDENTIALS = "gcp-service-account.json"
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
  function_name    = "gcp-sync-patcher-keda-v3"
  description      = "Creates or updates KEDA ScaledObject for a group. Reads desired state from Iceberg registry."
  role             = aws_iam_role.lambda_exec.arn
  filename         = data.archive_file.patcher_lambda_zip.output_path
  source_code_hash = data.archive_file.patcher_lambda_zip.output_base64sha256
  runtime          = "python3.12"
  handler          = "patcher_lambda.keda_handler"
  timeout          = 120
  memory_size      = 256

  environment {
    variables = merge(local.common_env, local.keda_env)
  }

  # ── EKS Private Cluster Networking ──────────────────────────────────────────
  # Uncomment if the EKS cluster uses a private-only API endpoint.
  # The Lambda VPC must have VPC Endpoints (or a NAT Gateway) for Athena, S3, STS.
  #
  # vpc_config {
  #   subnet_ids         = ["subnet-xxxxxxxxx", "subnet-yyyyyyyyy"]
  #   security_group_ids = ["sg-zzzzzzzzzzzz"]  # Must allow outbound HTTPS (443)
  # }
}

resource "aws_lambda_function" "patcher_configmap" {
  function_name    = "gcp-sync-patcher-configmap-v3"
  description      = "Patches ConfigMap, triggers rolling restart, confirms rollout, then marks PENDING → ACTIVE."
  role             = aws_iam_role.lambda_exec.arn
  filename         = data.archive_file.patcher_lambda_zip.output_path
  source_code_hash = data.archive_file.patcher_lambda_zip.output_base64sha256
  runtime          = "python3.12"
  handler          = "patcher_lambda.configmap_handler"

  # Timeout raised from 120s (v2) to 360s to accommodate rollout polling.
  # ROLLOUT_TIMEOUT_SECONDS (default 240s) + ~60s buffer for other operations.
  timeout     = 360
  memory_size = 256

  environment {
    variables = merge(local.common_env, local.keda_env, {
      ROLLOUT_TIMEOUT_SECONDS = "240"
    })
  }

  # vpc_config {
  #   subnet_ids         = ["subnet-xxxxxxxxx", "subnet-yyyyyyyyy"]
  #   security_group_ids = ["sg-zzzzzzzzzzzz"]
  # }
}

# ─────────────────────────────────────────────────────────────────────────────
# Step Function — Patching Pipeline
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_iam_role" "sfn_exec" {
  name = "gcp_sync_step_function_role"
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
  name     = "GCPSync-Topic-Onboarding"
  role_arn = aws_iam_role.sfn_exec.arn

  definition = jsonencode({
    Comment : "Patches KEDA and ConfigMap for a single group, confirms rollout. Input: {group: 'baseline'}"
    StartAt : "PatchKEDA"
    States : {
      PatchKEDA : {
        Type       : "Task"
        Resource   : aws_lambda_function.patcher_keda.arn
        Next       : "EvaluateDrift"
        ResultPath : "$.Payload"
      }
      EvaluateDrift : {
        Type : "Choice"
        Choices : [{
          Variable     : "$.Payload.status"
          StringEquals : "SKIPPED"
          Next         : "OrchestrationComplete"
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
        # TimeoutSeconds omitted — Lambda function timeout (360s) governs.
        # The Lambda polls rollout internally before returning.
        Next       : "OrchestrationComplete"
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
# EventBridge — Discovery Schedule
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_iam_role" "eventbridge_exec" {
  name = "gcp_sync_eventbridge_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "eventbridge_permissions" {
  name = "EventBridgeV3Permissions"
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

resource "aws_cloudwatch_event_rule" "discovery_hourly" {
  name                = "trigger-discovery-every-5-mins"
  description         = "Syncs GCP subscriptions to Iceberg registry every 5 minutes"
  schedule_expression = "rate(5 minutes)"
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
# EventBridge — Patching Schedules (per group, every 4 hours, staggered)
#
# Groups are offset 15 minutes apart so only one group ever patches at a time.
# Each execution rebuilds the full KEDA + ConfigMap for that group from the
# current registry state — covering new topics, reassigned topics, and drift.
#
# Sizing guidance: aim for ~100-150 topics per group to keep rolling restart
# duration bounded.  Add new groups here and run `terraform apply`.
# ─────────────────────────────────────────────────────────────────────────────

locals {
  patch_groups = {
    # cron(Minutes Hours Day-of-month Month Day-of-week Year)
    baseline = "cron(0  0,4,8,12,16,20 * * ? *)"   # every 4h at :00
    group1   = "cron(15 0,4,8,12,16,20 * * ? *)"   # every 4h at :15  (+15 min)
    group2   = "cron(30 0,4,8,12,16,20 * * ? *)"   # every 4h at :30  (+30 min)
    group3   = "cron(45 0,4,8,12,16,20 * * ? *)"   # every 4h at :45  (+45 min)
    group4   = "cron(0  2,6,10,14,18,22 * * ? *)"  # every 4h at :00, offset +2h
    group5   = "cron(15 2,6,10,14,18,22 * * ? *)"  # every 4h at :15, offset +2h
  }
}

resource "aws_cloudwatch_event_rule" "patching" {
  for_each            = local.patch_groups
  name                = "gcp-sync-patch-${each.key}"
  description         = "Patches KEDA + ConfigMap for group '${each.key}' every 4h"
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
  name   = "gcp_sync_db_v3"
  bucket = aws_s3_bucket.athena_results.bucket
}
