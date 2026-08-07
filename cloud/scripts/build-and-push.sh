#!/usr/bin/env bash
# Tags each of Phase 1's already-built `orderflow/<service>:local`
# images with its ECR repository URI (from OrderFlowEcrStack's outputs)
# and pushes them. Does NOT rebuild the images — Phase 1's Dockerfiles
# already produce the exact artifact this pushes; re-running `docker
# build` here would just waste time reproducing something that already
# exists locally.
#
# Requires: OrderFlowEcrStack already deployed (Phase 4), and the 8
# `orderflow/<service>:local` images already built (Phase 1).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$SCRIPT_DIR/../.."

SERVICES=(
  order-service
  inventory-service
  payment-service
  shipment-service
  fraud-detection-service
  analytics-service
  order-saga-orchestrator
  search-indexer-service
)

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=$(aws configure get region)
REGISTRY="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

echo "Logging in to ECR ($REGISTRY)..."
# get-login-password returns a short-lived (12-hour) auth token — this
# is the modern replacement for the old `aws ecr get-login` command,
# which used to print a full `docker login` command (including the
# password) to your shell history, a real credential-leak footgun the
# current two-step (fetch token, pipe straight into `docker login
# --password-stdin`) approach avoids entirely.
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "$REGISTRY"

for service in "${SERVICES[@]}"; do
  local_image="orderflow/${service}:local"
  remote_image="${REGISTRY}/orderflow/${service}:latest"

  echo
  echo "=== $service ==="
  docker tag "$local_image" "$remote_image"
  docker push "$remote_image"
done

echo
echo "All 8 images pushed. Verify:"
echo "  aws ecr describe-images --repository-name orderflow/order-service --query 'imageDetails[].imageTags'"
