#!/usr/bin/env bash
# Tears down ONLY the paid compute (OrderFlowEcsClusterStack — the 3x
# t3.small Kafka instances + 2x t3.medium app-tier instances). Leaves
# OrderFlowBudgetStack and OrderFlowNetworkStack deployed — both cost
# $0 to exist (a Budget alert and two Security Groups), and leaving
# NetworkStack up means the next `up.sh` doesn't need to recreate it.
#
# This is the actual cost lever for a 13-day budget window: running the
# 5 EC2 instances continuously costs roughly $13/day; tearing down
# between work sessions and bringing them back up when actually needed
# is what keeps total spend well under the AWS Budget's $130 threshold.
#
# Safe to run repeatedly — `cdk destroy` on a stack that's already gone
# is a no-op, not an error.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CDK_DIR="$SCRIPT_DIR/../cdk"

echo "Tearing down OrderFlowEcsClusterStack (all 5 EC2 instances)..."
echo "OrderFlowBudgetStack and OrderFlowNetworkStack are left running (both are free)."
echo

cd "$CDK_DIR"
# --force: skips the interactive "are you sure?" confirmation prompt —
# deliberate here, since this script's entire purpose is a fast,
# repeatable, push-button teardown. cdk destroy is NOT a dangerous
# irreversible action for this specific stack: it deletes the ASGs/EC2
# instances/capacity providers/cluster, all of which `up.sh` recreates
# identically from the same CDK source on the next run. Nothing this
# stack owns is stateful data worth protecting (Kafka/Postgres/
# Elasticsearch's actual data volumes don't exist yet at this phase —
# see docs/aws-cloud-deployment.md's note on this for LATER phases,
# once this stack does carry state worth being careful about).
node_modules/.bin/cdk destroy OrderFlowEcsClusterStack --force --no-color

echo
echo "Done. Verify nothing billable is left running:"
echo "  aws ec2 describe-instances --filters Name=instance-state-name,Values=running --query 'Reservations[].Instances[].InstanceId' --output text"
