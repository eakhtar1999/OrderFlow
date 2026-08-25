import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as cloudfront from 'aws-cdk-lib/aws-cloudfront';
import * as origins from 'aws-cdk-lib/aws-cloudfront-origins';

/**
 * The frontend's hosting infrastructure — an S3 bucket + a CloudFront
 * distribution in front of it. This stack provisions the INFRASTRUCTURE
 * only; it does not upload `frontend/dist`'s build output itself. That
 * split mirrors this project's own backend pattern exactly:
 * `OrderFlowEcrStack` (lib/ecr-stack.ts) provisions empty ECR repositories
 * and a separate script (cloud/scripts/build-and-push.sh, and now
 * .github/workflows/backend-deploy.yml) pushes the actual images — here,
 * this stack provisions an empty bucket + distribution, and
 * .github/workflows/frontend-deploy.yml does the actual `aws s3 sync` +
 * cache invalidation. Infrastructure-as-code and "ship this build's
 * artifact" are deliberately two different concerns with two different
 * change frequencies (this stack changes rarely; a deploy happens every
 * time you click "Run workflow").
 */
export class FrontendStack extends cdk.Stack {
  public readonly bucket: s3.Bucket;
  public readonly distribution: cloudfront.Distribution;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // No public bucket policy, no static website hosting mode — the
    // bucket stays 100% private. CloudFront reaches it via Origin Access
    // Control (OAC, the current recommended mechanism, superseding the
    // older Origin Access Identity/OAI), which signs every request
    // CloudFront makes to S3 so only CloudFront itself can read objects,
    // never a direct https://<bucket>.s3.amazonaws.com/... URL. This is
    // the same "nothing publicly reachable except through the front
    // door" shape as the backend's ALB — S3 here plays the role ECS
    // instances play there, reachable only via the one public entry
    // point in front of them.
    this.bucket = new s3.Bucket(this, 'FrontendBucket', {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
    });

    this.distribution = new cloudfront.Distribution(this, 'FrontendDistribution', {
      defaultRootObject: 'index.html',
      defaultBehavior: {
        origin: origins.S3BucketOrigin.withOriginAccessControl(this.bucket),
        viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
        cachePolicy: cloudfront.CachePolicy.CACHING_OPTIMIZED,
      },
      // The actual mechanism that makes client-side routing (React
      // Router, routes/routes.tsx) work behind CloudFront: a browser
      // request for e.g. /order/track has NO matching object in the S3
      // bucket (only index.html and /assets/* exist there) — S3 answers
      // that with 403 (via CloudFront's OAC) or 404, and WITHOUT this
      // override CloudFront would just hand that error straight back to
      // the browser as a broken page. Rewriting both error codes to
      // serve /index.html with a 200 hands the request to React Router,
      // which then reads the URL itself and renders the right route
      // client-side — exactly what every SPA behind a CDN needs.
      errorResponses: [
        { httpStatus: 403, responseHttpStatus: 200, responsePagePath: '/index.html' },
        { httpStatus: 404, responseHttpStatus: 200, responsePagePath: '/index.html' },
      ],
    });

    new cdk.CfnOutput(this, 'FrontendBucketName', {
      value: this.bucket.bucketName,
      description: 'S3 bucket the frontend deploy workflow syncs frontend/dist into',
    });
    new cdk.CfnOutput(this, 'FrontendDistributionId', {
      value: this.distribution.distributionId,
      description: 'CloudFront distribution id — needed for cache invalidation after a deploy',
    });
    new cdk.CfnOutput(this, 'FrontendUrl', {
      value: `https://${this.distribution.distributionDomainName}`,
      description: 'Public URL of the deployed frontend',
    });
  }
}
