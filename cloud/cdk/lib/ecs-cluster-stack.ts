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
    //
    // AmiHardwareType.ARM, not the default Standard (x86_64) — found
    // live, the expensive way: Phase 1's Docker images were built on
    // this project's own Apple Silicon development machine, which
    // means `docker build` produced arm64 images by default (Docker
    // matches the HOST architecture unless told otherwise). A
    // cross-platform rebuild via `docker buildx build --platform
    // linux/amd64` was attempted first, to keep these EC2 instances on
    // the more common x86_64 architecture — and was killed after 92
    // minutes still not finished. QEMU-based emulation (what buildx
    // uses to cross-compile for a different CPU architecture) is a
    // genuinely bad fit for compiling/running a JVM specifically: the
    // JIT compiler's own dynamically-generated machine code changes
    // constantly, which defeats QEMU's translation cache in a way
    // static/AOT-compiled binaries don't suffer from — a widely-cited
    // worst case for this class of emulation, confirmed here first-
    // hand rather than taken on faith. The actual fix is simpler than
    // rebuilding anything: match the EC2 instances' architecture to
    // the images that ALREADY exist, not the other way around.
    const machineImage = ecs.EcsOptimizedImage.amazonLinux2023(ecs.AmiHardwareType.ARM);

    // ---- Kafka's dedicated capacity: 3x t4g.small, FIXED size ------
    // min = max = desired = 3, on purpose — this pool exists for
    // exactly 3 Kafka broker ECS services, one instance each, no more,
    // no less, and no elastic scaling behavior to reason about.
    // t4g (Graviton/ARM), not t3 (x86_64) — matches the arm64 Docker
    // images from Phase 1 natively; see machineImage's own comment
    // above for the full story of why. Same specs as t3.small (2 vCPU,
    // 2GB RAM), still free-tier-eligible in this account.
    const kafkaAsg = new autoscaling.AutoScalingGroup(this, 'KafkaCapacityAsg', {
      vpc: props.vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.T4G, ec2.InstanceSize.SMALL),
      machineImage,
      securityGroup: props.ecsInstanceSecurityGroup,
      associatePublicIpAddress: true,
      minCapacity: 3,
      maxCapacity: 3,
      desiredCapacity: 3,
    });
    // Found live in Phase 5a: apache/kafka:3.8.0 runs as a non-root
    // user (uid 1000, "appuser") for real image-hardening reasons —
    // good practice in general. But ServicesStack's Kafka broker task
    // definitions bind-mount an EC2 HOST path (/data/kafka) into that
    // container for durable local storage, and a host path Docker
    // creates on first mount is owned by root:root by default. Result:
    // appuser couldn't write meta.properties into it and every broker
    // task crash-looped on "Error while writing meta.properties file."
    // The fix has to happen on the HOST, before any container starts
    // — `addUserData` appends shell commands to this ASG's launch
    // template, run once at instance boot, right alongside the
    // ECS-agent bootstrap CDK already injects here automatically.
    kafkaAsg.addUserData(
      'mkdir -p /data/kafka',
      'chown -R 1000:1000 /data/kafka',
    );
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

    // ---- Everything else's shared capacity: 6x t4g.small -----------
    // Postgres, Redis, Elasticsearch, Schema Registry, and (Phase 5c)
    // all 8 Spring Boot services bin-pack onto this pool — ECS's own
    // scheduler decides which instance each container actually lands
    // on, based on each task's declared CPU/memory reservation.
    //
    // Grown 2 -> 4 -> 6 across Phase 5c, for THREE different reasons —
    // the second and third found live, the hard way:
    //
    // 1. MEMORY (drove the 2 -> 4 move): Postgres (512) + Redis (128) +
    //    Elasticsearch (900) + Schema Registry (512) reserve ~2052 MiB;
    //    the 8 Spring Boot services' own memoryReservationMiB values
    //    (see AppServicesStack) add roughly another 3500 MiB — ~5550
    //    MiB needed total, comfortably under 4 instances' 4 × 1846 =
    //    7384 MiB of ECS-reservable memory.
    //
    // 2. ENI COUNT (drove the 4 -> 7 move, found live): `t4g.small`
    //    supports a MAXIMUM of 3 network interfaces total (confirmed
    //    via `aws ec2 describe-instance-types`) — 1 is always the
    //    instance's own primary ENI, leaving only 2 free for
    //    `awsvpc`-mode ECS tasks, REGARDLESS of how much CPU/memory is
    //    still free. With 12 app-tier services needing placement (4
    //    backing + 8 app) and only 4 instances × 2 ENI-limited task
    //    slots = 8 slots, 4 services got stuck permanently retrying
    //    with `RESOURCE:ENI` placement failures — a genuinely different
    //    failure mode from every prior "insufficient memory" case,
    //    confirmed via `aws ecs describe-services --query
    //    'services[0].events'`.
    //
    //    ECS's account-level `awsvpcTrunking` setting (`aws ecs
    //    put-account-setting-default --name awsvpcTrunking --value
    //    enabled`) exists specifically to raise this per-instance
    //    ceiling via "branch" ENIs riding on one "trunk" ENI — tried
    //    live here, and it's the reason `t4g.small` container instances
    //    DO show the `ecs.capability.task-eni-trunking` attribute. It
    //    did NOT, empirically, raise the actual per-instance task
    //    ceiling here: fresh instances launched AFTER enabling it still
    //    hit `RESOURCE:ENI` at exactly 2 tasks each, with CPU/memory
    //    both still plentiful. Left enabled anyway (harmless — it's an
    //    account-wide default, not something THIS stack could
    //    meaningfully "undo" per-instance, and it may matter for a
    //    larger instance type later), but NOT relied on: the number
    //    below assumes the UNTRUNKED 2-tasks-per-instance ceiling
    //    still applies, because that's what was actually observed.
    //
    // 3. ACCOUNT-LEVEL vCPU QUOTA (drove 7 back down to 6, found live):
    //    an attempt at 7 app-tier instances (10 total t4g.small
    //    account-wide, alongside the 3 Kafka brokers) failed to launch
    //    the 7th, repeatedly, with a real AWS error: "You have
    //    requested more vCPU capacity than your current vCPU limit of
    //    16 allows" (confirmed via `aws autoscaling
    //    describe-scaling-activities`) — this account's "Running
    //    On-Demand Standard (A, C, D, H, I, M, R, T, Z) instances"
    //    quota (`aws service-quotas get-service-quota --service-code
    //    ec2 --quota-code L-1216C47A`) is 16 vCPU. Every `t4g` size
    //    reports 2 vCPUs regardless of instance SIZE (confirmed:
    //    `t4g.micro` is ALSO 2 vCPU, not a smaller number — no
    //    escape via a "smaller" instance within this family), and this
    //    quota bucket covers essentially every general-purpose/
    //    compute/memory/burstable family (the "C, M, R, T" letters),
    //    so switching instance families doesn't dodge it either.
    //    Empirically, 9 total t4g.small instances run successfully but
    //    a 10th consistently fails — the account's REAL usable ceiling
    //    sits a little higher than the naive 16 ÷ 2 = 8 math predicts
    //    (T-family burstable instances may count differently toward
    //    this quota than the number implies), but the boundary is real
    //    and reproducible right at 9. With 3 Kafka instances fixed,
    //    app-tier gets AT MOST 6 — which, not by coincidence, is
    //    exactly 6 × 2 = 12 ENI-limited task slots for the 12 services
    //    that actually need one, with zero slack for a rolling
    //    deployment. Requesting a quota increase (`aws service-quotas
    //    request-service-quota-increase`) is the proper long-term fix
    //    if this pool ever needs to grow again; not pursued here given
    //    this project's fixed 13-day budget window and the fact that
    //    exactly-fitting capacity was achievable without waiting on an
    //    AWS support request to process.
    const appAsg = new autoscaling.AutoScalingGroup(this, 'AppCapacityAsg', {
      vpc: props.vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.T4G, ec2.InstanceSize.SMALL),
      machineImage,
      securityGroup: props.ecsInstanceSecurityGroup,
      associatePublicIpAddress: true,
      minCapacity: 6,
      maxCapacity: 6,
      desiredCapacity: 6,
    });
    // Phase 5b: same root-owned-bind-mount problem as Kafka's own fix
    // above, pre-empted here instead of found live a second time.
    // `postgres:16` and `redis:7` both run their entrypoint AS root
    // (confirmed via `docker inspect --format '{{.Config.User}}'` —
    // empty output means no USER directive) and internally chown their
    // own data directory before dropping privileges, so THEY need no
    // host-side fix. `elasticsearch:8.15.0` is the opposite: its image
    // has an explicit `USER 1000:0` Dockerfile directive, so it never
    // runs as root at all — same category of bug as apache/kafka's,
    // pre-empted the same way.
    appAsg.addUserData(
      'mkdir -p /data/elasticsearch',
      'chown -R 1000:0 /data/elasticsearch',
    );
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
