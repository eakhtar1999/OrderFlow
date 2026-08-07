import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as budgets from 'aws-cdk-lib/aws-budgets';

/**
 * The cost-safety net, deployed BEFORE any billable resource in this
 * project — see docs/aws-cloud-deployment.md's "Phase 2" section for
 * why this comes first, not last.
 *
 * `CfnBudget` is an L1 ("Cfn") construct, not an L2 — CDK ships hand-
 * written, higher-level L2 constructs (like `ecs.Cluster` or
 * `ec2.SecurityGroup`, coming in later phases) for services with rich,
 * commonly-reused patterns; AWS Budgets doesn't have one, so this is a
 * near-literal mapping of the underlying CloudFormation resource's
 * properties. Not every AWS service gets an L2 construct — plenty of
 * real CDK code is L1, and reading CloudFormation's own resource
 * documentation (not just CDK's) is often unavoidable for these.
 *
 * Notably, this does NOT use SNS for the email alert — AWS Budgets
 * supports `EMAIL`-type subscribers natively, with no SNS topic, no
 * subscription-confirmation step, and no extra IAM permissions needed.
 * SNS only becomes necessary here if you want a PROGRAMMATIC reaction
 * to a budget alert (a Lambda that auto-shuts-down resources, a Slack
 * webhook, etc.) — plain email is simpler and sufficient for a solo
 * cost-safety net.
 */
export class BudgetStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Passed via `cdk deploy -c alertEmail=you@example.com`, falling
    // back to a sane default — see this stack's own doc section on WHY
    // context values (not environment variables, not hardcoding) are
    // the idiomatic way to parameterize a CDK app per-deployment.
    const alertEmail: string = this.node.tryGetContext('alertEmail') ?? 'eakhtar1999@gmail.com';

    // AWS Budgets only supports a MONTHLY/QUARTERLY/ANNUALLY timeUnit —
    // there's no "exactly 13 days" option. MONTHLY is used here as the
    // practical proxy for this project's real, shorter window; the
    // dollar threshold below (not the time window) is what actually
    // does the safety-net work.
    new budgets.CfnBudget(this, 'OrderFlowCloudBudget', {
      budget: {
        budgetName: 'orderflow-cloud-deployment',
        budgetType: 'COST',
        timeUnit: 'MONTHLY',
        budgetLimit: {
          amount: 130,
          unit: 'USD',
        },
      },
      notificationsWithSubscribers: [
        // ACTUAL: fires once REAL spend crosses the threshold.
        {
          notification: {
            notificationType: 'ACTUAL',
            comparisonOperator: 'GREATER_THAN',
            threshold: 80,
            thresholdType: 'PERCENTAGE',
          },
          subscribers: [{ subscriptionType: 'EMAIL', address: alertEmail }],
        },
        {
          notification: {
            notificationType: 'ACTUAL',
            comparisonOperator: 'GREATER_THAN',
            threshold: 100,
            thresholdType: 'PERCENTAGE',
          },
          subscribers: [{ subscriptionType: 'EMAIL', address: alertEmail }],
        },
        // FORECASTED: fires earlier — AWS Cost Explorer projects
        // where spend is TRENDING based on the current billing period
        // so far, and alerts before you actually cross the line, not
        // just after. Genuinely different information than the ACTUAL
        // alerts above (e.g. a burst of spend early in a period can
        // trigger this well before ACTUAL spend itself reaches 100%).
        {
          notification: {
            notificationType: 'FORECASTED',
            comparisonOperator: 'GREATER_THAN',
            threshold: 100,
            thresholdType: 'PERCENTAGE',
          },
          subscribers: [{ subscriptionType: 'EMAIL', address: alertEmail }],
        },
      ],
    });
  }
}
