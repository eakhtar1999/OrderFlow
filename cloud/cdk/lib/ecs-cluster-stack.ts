import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as autoscaling from 'aws-cdk-lib/aws-autoscaling';

export interface EcsClusterStackProps extends cdk.StackProps {
  vpc: ec2.IVpc;
  ecsInstanceSecurityGroup: ec2.SecurityGroup;
}

/**
 * The ECS cluster itself, plus TWO separate EC2 capacity pools — this
 * is the one stack in this project where writing the CDK code is easy
 * and the DESIGN DECISION is the actual hard part. See
 * docs/aws-cloud-deployment.md's Phase 3 section for the full
 * reasoning; in short: the 3 Kafka brokers need genuine placement
 * isolation (killing one EC2 instance must only ever take down ONE
 * broker, for a real failover demo), which is incompatible with
 * letting ECS bin-pack them onto a shared, elastic pool the way the
 * other 12 containers (Postgres/Redis/Elasticsearch/Schema Registry +
 * the 8 Spring Boot services) safely can be.
 *
 * `EcsClusterStack` creates the CAPACITY (the ECS cluster resource
 * itself, plus the two EC2 pools registered as ECS "capacity
 * providers") — it does NOT yet run anything ON that capacity. Actual
 * ECS services (Kafka brokers, Postgres, the 8 app services, etc.) are
 * a later phase's `ServicesStack`, which will target ONE of these two
 * capacity providers per service via `capacityProviderStrategies`.
 */
export class EcsClusterStack extends cdk.Stack {
  public readonly cluster: ecs.Cluster;
  public readonly kafkaCapacityProviderName: string;
  public readonly appCapacityProviderName: string;

  constructor(scope: Construct, id: string, props: EcsClusterStackProps) {
    super(scope, id, props);

    this.cluster = new ecs.Cluster(this, 'OrderFlowCluster', {
      vpc: props.vpc,
      clusterName: 'orderflow-cluster',
      // Container Insights: CloudWatch-level metrics (CPU/memory per
      // service, not just per-instance) — genuinely useful for the
      // throughput-benchmarking work this whole deployment exists
      // partly to do, and low/no cost at this scale (a handful of
      // services, not hundreds).
      containerInsightsV2: ecs.ContainerInsights.ENABLED,
    });

    // The ECS-optimized Amazon Linux 2023 AMI, resolved automatically
    // at DEPLOY time via an SSM parameter lookup (not a hardcoded AMI
    // ID) — the actual AMI ID differs per region and changes over
    // time as AWS patches it; this always resolves to whatever the
    // current recommended one is for the target region.
    const machineImage = ecs.EcsOptimizedImage.amazonLinux2023();

    // ---- Kafka's dedicated capacity: 3x t3.small, FIXED size -------
    // min = max = desired = 3, on purpose — this pool exists for
    // exactly 3 Kafka broker ECS services, one instance each, no more,
    // no less, and no elastic scaling behavior to reason about.
    const kafkaAsg = new autoscaling.AutoScalingGroup(this, 'KafkaCapacityAsg', {
      vpc: props.vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.SMALL),
      machineImage,
      securityGroup: props.ecsInstanceSecurityGroup,
      associatePublicIpAddress: true,
      minCapacity: 3,
      maxCapacity: 3,
      desiredCapacity: 3,
    });
    const kafkaCapacityProvider = new ecs.AsgCapacityProvider(this, 'KafkaCapacityProvider', {
      autoScalingGroup: kafkaAsg,
      capacityProviderName: 'orderflow-kafka-capacity',
      // Both default to true, and both are genuinely useful for a
      // capacity pool that's meant to ELASTICALLY grow/shrink with
      // load. This pool never does either — it's pinned at exactly 3,
      // forever — so both are switched off rather than left as unused
      // complexity this stack doesn't actually need.
      enableManagedScaling: false,
      enableManagedTerminationProtection: false,
    });
    this.cluster.addAsgCapacityProvider(kafkaCapacityProvider);
    this.kafkaCapacityProviderName = kafkaCapacityProvider.capacityProviderName;

    // ---- Everything else's shared capacity: 2x t3.medium -----------
    // Postgres, Redis, Elasticsearch, Schema Registry, and all 8
    // Spring Boot services (12 containers total) bin-pack onto this
    // pool — ECS's own scheduler decides which of the 2 instances each
    // container actually lands on, based on each task's declared
    // CPU/memory reservation.
    const appAsg = new autoscaling.AutoScalingGroup(this, 'AppCapacityAsg', {
      vpc: props.vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
      // TEMPORARILY t3.small, not m7i-flex.large, while this phase is
      // still validating the STACK ITSELF (up/down cycles, ECS
      // registration, capacity provider wiring) rather than actually
      // running the 12-container app tier — deliberately the cheapest
      // free-tier-eligible option that's already proven reliable
      // (matching Kafka's own instance type), to minimize cost during
      // repeated up.sh/down.sh testing cycles. m7i-flex.large (2 vCPU,
      // 8GB RAM — 4x the RAM here) is the right choice once this
      // capacity pool actually needs to bin-pack Postgres, Redis,
      // Elasticsearch, Schema Registry, and all 8 Spring Boot services
      // — see this file's own comment history / git log for exactly
      // when that swap happens. Also confirmed (see Phase 3's "found
      // live" section) that t3.medium is REJECTED entirely in this
      // account by a Free-Tier-only guardrail on ec2:RunInstances — the
      // full eligible set here is c7i-flex.large, m7i-flex.large,
      // t3.micro, t3.small, t4g.micro, t4g.small.
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.SMALL),
      machineImage,
      securityGroup: props.ecsInstanceSecurityGroup,
      associatePublicIpAddress: true,
      minCapacity: 2,
      maxCapacity: 2,
      desiredCapacity: 2,
    });
    const appCapacityProvider = new ecs.AsgCapacityProvider(this, 'AppCapacityProvider', {
      autoScalingGroup: appAsg,
      capacityProviderName: 'orderflow-app-capacity',
      enableManagedScaling: false,
      enableManagedTerminationProtection: false,
    });
    this.cluster.addAsgCapacityProvider(appCapacityProvider);
    this.appCapacityProviderName = appCapacityProvider.capacityProviderName;
  }
}
