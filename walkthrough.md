# Walkthrough: V3 Architectural Fixes

We have successfully refined the GCP Sync codebase in the `sync-v3` folder to address the issues you approved. 

## Completed Fixes

### 1. `patcher_lambda.py`
We addressed the critical `401 Unauthorized` token expiration crashing issue that occurs during prolonged Kubernetes deployment states.

*   `_connect_eks()` now returns a lazy-evaluation Lambda function `token_func` instead of a static 60-second string token. 
*   `_call_eks_api` executes `token_func()` dynamically on every single API request, ensuring long-running polls during `_wait_for_rollout` always use a fresh STS token.
*   The `_mark_topics_active` function and the `_read_group_subscriptions` were updated to use AWS Native `ExecutionParameters` parameterized queries to avoid raw string building issues, avoiding SQL syntax/type injection.

### 2. `sync_lambda.py`
We updated the string builder code for merging arrays of topic strings and implemented parameterized batch operations across the board.

*   `_upsert` was updated to iterate over your data and pass a flattened `ExecutionParameters` sequence rather than building raw SQL value strings. This guarantees syntax safety for Athena when merging into the registry table.
*   `_mark_removed` was refactored similarly to `_mark_topics_active` in patcher. The `_run_query` helper abstraction was enhanced to securely pass `ExecutionParameters` to cleanly execute the `IN` array payload check. 
*   `_sql_escape` was effectively retired and removed entirely from the file since parameters negate SQL injection.
*   The timeout polling termination handling within `_upsert` was hardened by wrapping `stop_query_execution` in a `try...except` catch block (mirroring `_run_query`) so that network blips don't mask `TimeoutError` exceptions.

## Terraform and EKS Testing Environment
During testing, we discovered specific requirements for the user's `terraform-firehose` IAM profile:
1.  **Strict IAM Naming**: Terraform's `main.tf` was refactored because the developer's deployment IAM policy explicitly restricted role creation to strictly named spaces. Variables were rewritten across Lambda, Step Functions, and EventBridge execution roles to match (`gcp_discovery_lambda_role`, `gcp_sync_step_function_role`, `gcp_sync_eventbridge_role`). We also aligned the Step Function Resource limits to `GCPSync-Topic-Onboarding` and the EventBridge rules to `trigger-discovery-every-5-mins`.
2.  **Native `kubectl` Application**: We shifted the `KEDA` and base architecture implementation instructions away from `helm` logic and defaulted back closely to the Phase 3 `integrated-scaling-poc` directory YAML execution to support AWS CloudShell environments out of the box.
3.  **Cygrpc Compilation**: Because the user was deploying the AWS Lambdas from a local macOS laptop, running a standard `pip install -t` downloaded the local ARM architecture wheels for `grpcio`. When deployed to AWS Lambda, this crashed with a `cygrpc` ImportModuleError on the Linux x86_64 runtime. We solved this by deleting the local packages and executing `pip install --platform manylinux2014_x86_64 --target=. --implementation cp --python-version 3.12 --only-binary=:all: -r requirements.txt` to inject the correct backend architecture.
4.  **EKS IAM RBAC**: The Python Lambdas use the STS `get_caller_identity` token methodology to dynamically hit the EKS Kubernetes cluster privately. We discovered that standard IAM Permissions (`DescribeCluster`) were insufficient—the specific `gcp_discovery_lambda_role` required an explicit injection into the cluster's internal `kube-system/aws-auth` ConfigMap. We wrote a small `eks-auth-patch.sh` bash script wrapping `eksctl` to cleanly map the Lambda role to the `system:masters` group for full deployment execution permissions.

## Skipped Fixes
As requested, we skipped implementing Athena pagination in both `patcher_lambda` and `sync_lambda` since the data sets are capped under 1,000 strings.

## Validation 
We ran a syntax check using `python3 -m py_compile` locally and both files compile successfully.

If everything looks good, I believe our modifications are complete. Let me know if you would like to run any E2E tests or further validate these changes!
