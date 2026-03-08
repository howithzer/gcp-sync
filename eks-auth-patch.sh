#!/bin/bash
# Patch EKS aws-auth ConfigMap to allow the Lambda IAM role to authenticate
set -e

CLUSTER_NAME="gcp-sync-poc-test"
ROLE_ARN="arn:aws:iam::487500748616:role/gcp_discovery_lambda_role"
REGION="us-east-1"

echo "Current aws-auth mapping:"
kubectl get configmap aws-auth -n kube-system -o yaml > /tmp/aws-auth-backup.yaml

# Download eksctl if not present (CloudShell usually has it)
if ! command -v eksctl &> /dev/null; then
    echo "eksctl could not be found. Installing..."
    curl --silent --location "https://github.com/weaveworks/eksctl/releases/latest/download/eksctl_$(uname -s)_amd64.tar.gz" | tar xz -C /tmp
    sudo mv /tmp/eksctl /usr/local/bin
fi

echo "Mapping Lambda IAM Role to EKS system:masters..."
eksctl create iamidentitymapping \
    --cluster $CLUSTER_NAME \
    --region $REGION \
    --arn $ROLE_ARN \
    --group system:masters \
    --username lambda-patcher

echo "Done! The Lambda now has full permission to describe and patch deployments!"
