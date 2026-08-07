import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';

/**
 * References the account's EXISTING default VPC — does NOT create a
 * new one. See docs/aws-cloud-deployment.md's Phase 3 section for the
 * full reasoning, but in one sentence: a new VPC built the "textbook"
 * way (private subnets for compute, a NAT Gateway for their outbound
 * internet access) costs real, ongoing money — a NAT Gateway alone is
 * roughly $0.045/hr PLUS per-GB data processing charges, for a
 * portfolio deployment that doesn't actually need private-subnet
 * isolation from anything. Every EC2 instance this project launches
 * sits in a PUBLIC subnet instead, with Security Groups (not subnet
 * isolation) doing the actual access control — a real, deliberate
 * trade-off, not an oversight, the same way this project already
 * documents `xpack.security.enabled=false` as a dev-only
 * simplification for local Elasticsearch.
 */
export class NetworkStack extends cdk.Stack {
  public readonly vpc: ec2.IVpc;
  public readonly ecsInstanceSecurityGroup: ec2.SecurityGroup;
  public readonly albSecurityGroup: ec2.SecurityGroup;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Vpc.fromLookup performs a REAL AWS API call at synth time (not at
    // deploy time) to find the account's default VPC and read back its
    // subnet IDs/AZs/CIDRs — this is why `env: { account, region }` had
    // to be set explicitly back in bin/cdk.ts (Phase 2's comment on
    // this): a lookup like this is meaningless without knowing exactly
    // which account/region to look IN. The result gets cached in
    // cdk.context.json so repeated synths don't repeat the API call
    // unless the cache is cleared.
    this.vpc = ec2.Vpc.fromLookup(this, 'DefaultVpc', { isDefault: true });

    // The ALB's security group: the ACTUAL internet-facing entry point
    // to this whole deployment. Inbound from anywhere on 80 (plain
    // HTTP — no ACM certificate/custom domain for this budget-
    // constrained deployment, see Phase 3's own notes on why). Outbound
    // defaults to "allow all," which is fine here — an ALB only ever
    // originates traffic TOWARD the services it's routing to, all of
    // which live in this same VPC.
    this.albSecurityGroup = new ec2.SecurityGroup(this, 'AlbSecurityGroup', {
      vpc: this.vpc,
      description: 'OrderFlow ALB - public HTTP entry point',
      allowAllOutbound: true,
    });
    this.albSecurityGroup.addIngressRule(
      ec2.Peer.anyIpv4(),
      ec2.Port.tcp(80),
      'Public HTTP access to the ALB',
    );

    // The EC2 instances' security group — every Kafka broker, Postgres,
    // Redis, Elasticsearch, Schema Registry, and all 8 Spring Boot
    // services eventually run inside instances wearing THIS one group.
    // Two rules, both deliberately narrow:
    this.ecsInstanceSecurityGroup = new ec2.SecurityGroup(this, 'EcsInstanceSecurityGroup', {
      vpc: this.vpc,
      description: 'OrderFlow ECS EC2 instances - Kafka brokers + app tier',
      allowAllOutbound: true,
    });
    // 1. Only the ALB can reach these instances from OUTSIDE the group
    //    — no direct internet access to any app service, Kafka broker,
    //    or database, even though they technically sit in a PUBLIC
    //    subnet (see this class's own doc comment above). The subnet
    //    being "public" only means it HAS a route to an internet
    //    gateway; it says nothing about which traffic is actually
    //    allowed to reach an instance inside it — that's entirely this
    //    Security Group's job.
    this.ecsInstanceSecurityGroup.addIngressRule(
      this.albSecurityGroup,
      ec2.Port.allTraffic(),
      'Inbound from the ALB only',
    );
    // 2. A SELF-referencing rule: any instance wearing this group can
    //    reach any OTHER instance wearing this same group, on any port.
    //    This is what lets the 3 Kafka brokers reach each other (KRaft
    //    controller quorum, inter-broker replication) and lets every
    //    app service reach Kafka/Postgres/Redis/Elasticsearch/Schema
    //    Registry — all of which live on instances in this SAME group.
    //    Real production security practice would scope this down to
    //    the SPECIFIC ports each service actually needs (9092 for
    //    Kafka, 5432 for Postgres, etc.) rather than "all traffic
    //    between group members" — flagged here as the pragmatic choice
    //    for a portfolio deployment with a dozen distinct internal
    //    ports to otherwise enumerate by hand, not a security best
    //    practice being recommended.
    this.ecsInstanceSecurityGroup.addIngressRule(
      this.ecsInstanceSecurityGroup,
      ec2.Port.allTraffic(),
      'Inbound from other instances in this same group (inter-service, inter-broker)',
    );

    // No SSH ingress rule at all, on purpose — see Phase 3's notes on
    // ECS Exec as the modern replacement for "SSH into the box to
    // debug a container."
  }
}
