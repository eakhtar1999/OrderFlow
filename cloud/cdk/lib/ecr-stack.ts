import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ecr from 'aws-cdk-lib/aws-ecr';

/** The 8 services, matching Phase 1's Dockerfiles exactly by name. */
export const SERVICE_NAMES = [
  'order-service',
  'inventory-service',
  'payment-service',
  'shipment-service',
  'fraud-detection-service',
  'analytics-service',
  'order-saga-orchestrator',
  'search-indexer-service',
] as const;

/**
 * One ECR (Elastic Container Registry) repository per service — the
 * AWS-hosted equivalent of the `orderflow/<service>:local` images
 * Phase 1 already builds locally. ECS can only pull images from a
 * registry it can reach; a laptop's local Docker daemon isn't one.
 *
 * `emptyOnDelete: true` is the one property that matters most here for
 * this project's specific constraints: by default, ECR refuses to
 * delete a repository that still contains images (a real, sensible
 * safety default for a repo you might depend on) — which would make
 * `cdk destroy` FAIL partway through instead of cleanly tearing
 * everything down, exactly the kind of thing this project's tight,
 * cost-conscious teardown/redeploy cycle can't afford. This is the
 * infrastructure-level equivalent of Build Order Step 16's
 * `autoDeleteImages`-style reasoning applied to a different AWS
 * service, not a new idea.
 */
export class EcrStack extends cdk.Stack {
  public readonly repositories: Record<string, ecr.Repository> = {};

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    for (const serviceName of SERVICE_NAMES) {
      const repo = new ecr.Repository(this, `${serviceName}Repo`, {
        repositoryName: `orderflow/${serviceName}`,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
        emptyOnDelete: true,
        // Keeps only the 5 most recent images per repository — without
        // this, every `docker push` (one per code change, potentially
        // many per day during active development) accumulates forever,
        // slowly costing more in ECR storage ($0.10/GB-month) for old
        // images nothing references anymore.
        lifecycleRules: [
          {
            description: 'Keep only the 5 most recent images',
            maxImageCount: 5,
          },
        ],
      });
      this.repositories[serviceName] = repo;

      // CfnOutput: prints the repo URI at the end of `cdk deploy`'s own
      // output, AND makes it queryable later via
      // `aws cloudformation describe-stacks --query Stacks[0].Outputs`
      // without needing to reconstruct the URI by hand (which IS
      // predictable — `<account>.dkr.ecr.<region>.amazonaws.com/<name>`
      // — but an Output is the honest, don't-make-me-guess way to get
      // it, especially useful for the build-and-push script's own use).
      new cdk.CfnOutput(this, `${serviceName}RepoUri`, {
        value: repo.repositoryUri,
        description: `ECR repository URI for ${serviceName}`,
      });
    }
  }
}
