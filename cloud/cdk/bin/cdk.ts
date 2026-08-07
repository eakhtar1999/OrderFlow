#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { BudgetStack } from '../lib/budget-stack';

const app = new cdk.App();

// CDK_DEFAULT_ACCOUNT/CDK_DEFAULT_REGION aren't real environment
// variables you set yourself — the CDK CLI populates them automatically
// from whatever AWS CLI profile/credentials are active when you run
// `cdk deploy`/`cdk synth`. Explicitly setting `env` (rather than
// leaving a stack "environment-agnostic," the scaffolded default) is
// what unlocks account/region-AWARE features — Cost Explorer lookups
// here, and later, Cloud Map/ECS constructs that need to know which
// VPC/AZs actually exist in this specific account.
const env = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEFAULT_REGION,
};

new BudgetStack(app, 'OrderFlowBudgetStack', { env });
