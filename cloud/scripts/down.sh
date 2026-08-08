#!/usr/bin/env bash
# Tears down ONLY the paid compute (OrderFlowServicesStack — the Kafka
# brokers/Schema Registry ECS services — then OrderFlowEcsClusterStack
# — the 3x t4g.small Kafka instances + 2x t4g.small app-tier
# instances). Leaves OrderFlowBudgetStack and OrderFlowNetworkStack
# deployed — both cost $0 to exist (a Budget alert and two Security
# Groups), and leaving NetworkStack up means the next `up.sh` doesn't
# need to recreate it.
#
# ORDER MATTERS as of Phase 5a: OrderFlowServicesStack's ECS services
# reference OrderFlowEcsClusterStack's cluster/capacity providers via
# CloudFormation Fn::ImportValue — AWS will refuse to delete a stack
# whose exports are still imported elsewhere. Destroying
# ServicesStack FIRST releases those imports before EcsClusterStack's
# own destroy is attempted.
#
# This is the actual cost lever for a 13-day budget window: running the
# 5 EC2 instances continuously costs real money per day; tearing down
# between work sessions and bringing them back up when actually needed
# is what keeps total spend well under the AWS Budget's threshold.
#
# Safe to run repeatedly — `cdk destroy` on a stack that's already gone
# is a no-op, not an error.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CDK_DIR="$SCRIPT_DIR/../cdk"

echo "Tearing down OrderFlowServicesStack (Kafka brokers + Schema Registry)..."
echo "then OrderFlowEcsClusterStack (all 5 EC2 instances)..."
echo "OrderFlowBudgetStack and OrderFlowNetworkStack are left running (both are free)."
echo

cd "$CDK_DIR"
# --force: skips the interactive "are you sure?" confirmation prompt —
# deliberate here, since this script's entire purpose is a fast,
# repeatable, push-button teardown. cdk destroy is NOT a dangerous
# irreversible action for these stacks: everything they own (ECS
# services/task defs, ASGs/EC2 instances/capacity providers/cluster) is
# fully reproducible from the same CDK source on the next `up.sh`.
# Kafka's bind-mounted broker data does NOT survive this (see up.sh's
# own comment) — a deliberate, already-accepted trade-off, not
# something worth protecting here.
node_modules/.bin/cdk destroy OrderFlowServicesStack --force --no-color
node_modules/.bin/cdk destroy OrderFlowEcsClusterStack --force --no-color

echo
echo "Done. Verify nothing billable is left running:"
echo "  aws ec2 describe-instances --filters Name=instance-state-name,Values=running --query 'Reservations[].Instances[].InstanceId' --output text"
