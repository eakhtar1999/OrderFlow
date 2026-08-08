#!/usr/bin/env bash
# Brings the paid compute back up after down.sh tore it down —
# redeploys OrderFlowNetworkStack (usually a fast no-op if nothing
# changed — CDK diffs against what's ACTUALLY deployed and only
# touches what differs) followed by OrderFlowEcsClusterStack (recreates
# the ASGs, which launches 5 fresh EC2 instances from scratch).
#
# "From scratch" is a real, deliberate consequence worth knowing before
# running this: any Kafka broker's local disk state from bind-mounted
# storage (see docs/aws-cloud-deployment.md's Kafka design in a later
# phase) does NOT survive `down.sh` + `up.sh` — a fresh EC2 instance
# means a fresh, empty disk. This is the SAME trade-off this project's
# local docker-compose.yml already made peace with for a genuine broker
# FAILURE (kill one instance, its data is gone, that's the point) — the
# difference here is `down.sh`/`up.sh` intentionally does that to ALL
# 3 brokers at once, as a cost-saving measure between work sessions,
# not as a failure demo. Expect to re-seed/re-run the platform's own
# topic-creation (`@Bean NewTopic`s, already built into every service)
# after bringing compute back up — the same as any true cold start.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CDK_DIR="$SCRIPT_DIR/../cdk"

echo "Bringing up OrderFlowNetworkStack + OrderFlowEcsClusterStack + OrderFlowServicesStack..."
echo "(OrderFlowBudgetStack should already be deployed and is left alone by this script.)"
echo

cd "$CDK_DIR"
node_modules/.bin/cdk deploy OrderFlowNetworkStack OrderFlowEcsClusterStack \
  --no-color --require-approval never

echo
echo "Cluster capacity is up. Confirm before deploying services onto it:"
echo "  aws ecs describe-clusters --clusters orderflow-cluster --query 'clusters[0].{registeredContainerInstances:registeredContainerInstancesCount,activeServicesCount:activeServicesCount}'"
echo

# ServicesStack deploys as a separate `cdk deploy` call, not bundled
# into the multi-stack command above, on purpose: ECS EC2 container
# instances take a few minutes to register with the cluster after
# their ASG launches them, and an Ec2Service's tasks can only place
# once real registered capacity exists to place onto. Deploying it as
# its own step (after the capacity above is confirmed/settled) avoids
# a race where CDK tries to place a Kafka broker task before any
# instance has finished joining the cluster.
node_modules/.bin/cdk deploy OrderFlowServicesStack --no-color --require-approval never

echo
echo "Done. Confirm the Kafka brokers + Schema Registry are running:"
echo "  aws ecs list-tasks --cluster orderflow-cluster --query 'taskArns' --output text"
