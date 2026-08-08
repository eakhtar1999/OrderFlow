#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { BudgetStack } from '../lib/budget-stack';
import { NetworkStack } from '../lib/network-stack';
import { EcsClusterStack } from '../lib/ecs-cluster-stack';
import { EcrStack } from '../lib/ecr-stack';
import { ServicesStack } from '../lib/services-stack';
import { AppServicesStack } from '../lib/app-services-stack';

const app = new cdk.App();

// CDK_DEFAULT_ACCOUNT/CDK_DEFAULT_REGION aren't real environment
// variables you set yourself — the CDK CLI populates them automatically
// from whatever AWS CLI profile/credentials are active when you run
// `cdk deploy`/`cdk synth`. Explicitly setting `env` (rather than
// leaving a stack "environment-agnostic," the scaffolded default) is
// what unlocks account/region-AWARE features — Cost Explorer lookups
// in BudgetStack, and NetworkStack's `Vpc.fromLookup` (a real API call
// that needs to know exactly which account/region to look IN).
const env = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEFAULT_REGION,
};

new BudgetStack(app, 'OrderFlowBudgetStack', { env });

// NetworkStack and EcsClusterStack are two SEPARATE CDK Stack objects,
// but not two separate CloudFormation exports/imports by hand — CDK
// wires the cross-stack reference automatically. Passing
// `network.vpc`/`network.ecsInstanceSecurityGroup` directly as
// constructor props (rather than, say, looking them up again by ARN)
// is what makes CDK generate the actual CloudFormation
// Export/Fn::ImportValue plumbing for you, and — just as importantly —
// what makes CDK understand these two stacks now have a REAL
// dependency: `cdk deploy` will always deploy NetworkStack before
// EcsClusterStack, in that order, without needing to be told to.
const network = new NetworkStack(app, 'OrderFlowNetworkStack', { env });

const ecsCluster = new EcsClusterStack(app, 'OrderFlowEcsClusterStack', {
  env,
  vpc: network.vpc,
  ecsInstanceSecurityGroup: network.ecsInstanceSecurityGroup,
});

// EcrStack has no dependency on the other three — it doesn't need the
// VPC, the cluster, or the Security Groups, just an AWS account/region
// to create repositories in. Independent stacks like this deploy in
// whatever order `cdk deploy --all` decides (or in parallel, when
// nothing forces sequencing) — no explicit ordering needed here the
// way NetworkStack -> EcsClusterStack's real prop-passing dependency
// requires it.
const ecrStack = new EcrStack(app, 'OrderFlowEcrStack', { env });

// Phase 5a/5b: the backing infrastructure — Kafka brokers, Schema
// Registry, Postgres, Redis, Elasticsearch. Depends on both
// NetworkStack (VPC) and EcsClusterStack (the cluster + the two
// capacity providers) — same cross-stack-reference mechanism as
// EcsClusterStack's own props above, so `cdk deploy` again orders this
// correctly on its own.
const servicesStack = new ServicesStack(app, 'OrderFlowServicesStack', {
  env,
  vpc: network.vpc,
  cluster: ecsCluster.cluster,
  kafkaCapacityProviderName: ecsCluster.kafkaCapacityProviderName,
  appCapacityProviderName: ecsCluster.appCapacityProviderName,
});

// Phase 5c: the 8 Spring Boot services + the public ALB. Depends on
// EcrStack (the images to actually run) and ServicesStack (the shared
// internal security group and Cloud Map namespace every backing
// dependency above already registered into) in addition to
// NetworkStack/EcsClusterStack.
new AppServicesStack(app, 'OrderFlowAppServicesStack', {
  env,
  vpc: network.vpc,
  cluster: ecsCluster.cluster,
  appCapacityProviderName: ecsCluster.appCapacityProviderName,
  albSecurityGroup: network.albSecurityGroup,
  internalSecurityGroup: servicesStack.internalSecurityGroup,
  namespace: servicesStack.namespace,
  repositories: ecrStack.repositories,
});
