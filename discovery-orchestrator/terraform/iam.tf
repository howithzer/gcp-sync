# -----------------------------------------------------------------------------
# IAM Roles & Policies for Phase 4 Discovery
# -----------------------------------------------------------------------------

data "aws_caller_identity" "current" {}

# Lambda Execution Role
resource "aws_iam_role" "lambda_exec" {
  name = "gcp_discovery_lambda_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_athena_access" {
  name = "AthenaAndS3Access"
  role = aws_iam_role.lambda_exec.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "glue:GetTable",
          "glue:GetDatabase",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:UpdateDatabase"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads",
          "s3:ListMultipartUploadParts",
          "s3:AbortMultipartUpload",
          "s3:CreateBucket",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "arn:aws:s3:::gcp-sync-athena-results-*",
          "arn:aws:s3:::gcp-sync-athena-results-*/*",
          "arn:aws:s3:::gcp-sync-iceberg-data-*",
          "arn:aws:s3:::gcp-sync-iceberg-data-*/*"
        ]
      }
    ]
  })
}

# Step Function Execution Role
resource "aws_iam_role" "step_function_exec" {
  name = "gcp_sync_step_function_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "step_function_lambda_invoke" {
  name = "InvokeDiscoveryLambda"
  role = aws_iam_role.step_function_exec.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = [
          aws_lambda_function.gcp_discovery.arn,
          aws_lambda_function.gcp_ddl.arn,
          aws_lambda_function.gcp_eks_patcher.arn,
          aws_lambda_function.gcp_configmap_patcher.arn
        ]
      }
    ]
  })
}

# EventBridge Execution Role
resource "aws_iam_role" "eventbridge_exec" {
  name = "gcp_sync_eventbridge_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "eventbridge_step_function" {
  name = "StartStepFunction"
  role = aws_iam_role.eventbridge_exec.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = [
          aws_sfn_state_machine.onboarding_orchestrator.arn
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "lambda_eks_access" {
  name = "EKSAndSTSAccess"
  role = aws_iam_role.lambda_exec.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "eks:DescribeCluster",
          "sts:GetCallerIdentity"
        ]
        Resource = "*"
      }
    ]
  })
}

