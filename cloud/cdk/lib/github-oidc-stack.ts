import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as ecr from 'aws-cdk-lib/aws-ecr';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as cloudfront from 'aws-cdk-lib/aws-cloudfront';
import { SERVICE_NAMES } from './ecr-stack';

export interface GithubOidcStackProps extends cdk.StackProps {
  repositories: Record<string, ecr.Repository>;
  frontendBucket: s3.Bucket;
  frontendDistribution: cloudfront.Distribution;
}

// Must match this repo's actual GitHub org/name exactly — see
// docs/github-actions-fundamentals.md's OIDC section for what happens if
// this drifts from reality (AssumeRoleWithWebIdentity is denied, not
// silently mismatched).
const GITHUB_REPO = 'eakhtar1999/OrderFlow';
const CLUSTER_NAME = 'orderflow-cluster';

/**
 * Grants GitHub Actions temporary AWS credentials via OIDC federation —
 * no long-lived AWS_ACCESS_KEY_ID/SECRET stored as a GitHub secret
 * anywhere. See docs/github-actions-fundamentals.md's "Why OIDC" section
 * for the full mechanism; short version here: GitHub mints a short-lived
 * signed token per workflow run, this role's trust policy checks that
 * token's claims before handing out real (also short-lived, ~1hr) AWS
 * credentials.
 *
 * This account (027677879393) had NO OIDC provider at all before this
 * stack — unlike some AWS accounts where one might already exist from
 * prior work, `new iam.OpenIdConnectProvider` here is creating it fresh,
 * not importing something that was already there. Only one such provider
 * can exist per account for a given issuer URL — if you ever see a
 * "provider already exists" error deploying this stack, that means
 * something else already created one and this should import it instead
 * (`iam.OpenIdConnectProvider.fromOpenIdConnectProviderArn`).
 */
export class GithubOidcStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: GithubOidcStackProps) {
    super(scope, id, props);

    const provider = new iam.OpenIdConnectProvider(this, 'GithubOidcProvider', {
      url: 'https://token.actions.githubusercontent.com',
      clientIds: ['sts.amazonaws.com'],
    });

    // The trust condition is the ENTIRE security boundary here — it's
    // what stops a workflow run from a fork, a different repo, or a
    // pull_request event (rather than a manually-dispatched run on
    // main) from ever being able to assume this role, even though the
    // OIDC provider itself trusts ANY token GitHub signs. Scoped to
    // `ref:refs/heads/main` specifically (not `repo:.../*`) because this
    // project's deploy workflows are workflow_dispatch-only, always run
    // from main — see docs/github-actions-fundamentals.md for what a
    // broader/narrower condition would look like for a different trigger
    // strategy.
    const deployRole = new iam.Role(this, 'GithubDeployRole', {
      roleName: 'orderflow-github-deploy-role',
      assumedBy: new iam.WebIdentityPrincipal(provider.openIdConnectProviderArn, {
        StringEquals: {
          'token.actions.githubusercontent.com:aud': 'sts.amazonaws.com',
          'token.actions.githubusercontent.com:sub': `repo:${GITHUB_REPO}:ref:refs/heads/main`,
        },
      }),
      description: 'Assumed by GitHub Actions (workflow_dispatch on main) to deploy OrderFlow to AWS.',
    });

    // ---- ECR: build-and-push the 8 backend service images ----
    // GetAuthorizationToken has no resource-level permissions in IAM —
    // AWS requires it be granted on "*" regardless of which specific
    // repositories you intend to push to; scoping happens on the OTHER
    // ECR actions below instead.
    deployRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['ecr:GetAuthorizationToken'],
        resources: ['*'],
      }),
    );
    deployRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          'ecr:BatchCheckLayerAvailability',
          'ecr:PutImage',
          'ecr:InitiateLayerUpload',
          'ecr:UploadLayerPart',
          'ecr:CompleteLayerUpload',
          'ecr:BatchGetImage',
        ],
        resources: Object.values(props.repositories).map((repo) => repo.repositoryArn),
      }),
    );

    // ---- ECS: roll a new image out after pushing it ----
    // Scoped to exactly the 8 named services inside orderflow-cluster —
    // deliberately not ecs:* or a wildcard service ARN, so a leaked/
    // misused token still can't touch any other cluster or service this
    // AWS account might ever host.
    deployRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['ecs:UpdateService', 'ecs:DescribeServices'],
        resources: SERVICE_NAMES.map(
          (name) => `arn:aws:ecs:${this.region}:${this.account}:service/${CLUSTER_NAME}/${name}`,
        ),
      }),
    );

    // ---- S3 + CloudFront: ship the frontend build ----
    props.frontendBucket.grantReadWrite(deployRole);
    deployRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['cloudfront:CreateInvalidation'],
        resources: [
          `arn:aws:cloudfront::${this.account}:distribution/${props.frontendDistribution.distributionId}`,
        ],
      }),
    );

    new cdk.CfnOutput(this, 'GithubDeployRoleArn', {
      value: deployRole.roleArn,
      description: 'Paste into .github/workflows/*.yml\'s role-to-assume input',
    });
  }
}
