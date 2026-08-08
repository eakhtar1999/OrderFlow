import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as servicediscovery from 'aws-cdk-lib/aws-servicediscovery';
import * as logs from 'aws-cdk-lib/aws-logs';

export interface ServicesStackProps extends cdk.StackProps {
  vpc: ec2.IVpc;
  cluster: ecs.Cluster;
  kafkaCapacityProviderName: string;
  appCapacityProviderName: string;
}

const KAFKA_IMAGE = 'apache/kafka:3.8.0';
const NAMESPACE_NAME = 'orderflow.local';

/**
 * Phase 5a: the actual point of this entire cloud deployment. Three
 * INDEPENDENT ECS services — not one service running 3 tasks — each
 * pinned to a different EC2 instance via a `distinctInstances`
 * placement constraint, each its own KRaft broker with its own node
 * ID, all three wired into the SAME controller quorum via Cloud Map
 * DNS names. This is what makes "kill one EC2 instance, watch ISR/
 * leader-election happen for real" a genuine demo later, rather than a
 * simulation — see docs/aws-cloud-deployment.md's Phase 5a section for
 * why one Ec2Service per broker was the only design that could deliver
 * that.
 */
export class ServicesStack extends cdk.Stack {
  public readonly namespace: servicediscovery.PrivateDnsNamespace;
  // Public (not private, as originally scoped) as of Phase 5c —
  // AppServicesStack's 8 Spring Boot services need this SAME shared
  // security group (not a parallel duplicate) to reach Kafka/Postgres/
  // Redis/Elasticsearch/Schema Registry, and the ALB needs to add its
  // OWN ingress rules to it for the 5 externally-exposed services —
  // one shared internal-traffic boundary is simpler to reason about
  // than two overlapping ones.
  public readonly internalSecurityGroup: ec2.SecurityGroup;

  constructor(scope: Construct, id: string, props: ServicesStackProps) {
    super(scope, id, props);

    // Every awsvpc-mode Ec2Service gets its OWN, freshly-created
    // security group by default if you don't pass one — egress-only,
    // zero ingress rules. Found live via `cdk synth`, before ever
    // deploying: with the default behavior, kafka-1 has no rule
    // allowing kafka-2/kafka-3 to reach it on 9092/9093, and Schema
    // Registry has no rule allowing it to reach ANY broker — a KRaft
    // quorum that could never actually form. One SHARED security
    // group with a self-referencing ingress rule (anything already
    // wearing this security group may talk to anything else wearing
    // it, on these specific ports) is the fix — the same "Security
    // Groups as the access-control boundary" approach NetworkStack
    // already uses for the ALB/instance split, applied here one level
    // down for the services running ON those instances.
    this.internalSecurityGroup = new ec2.SecurityGroup(this, 'InternalSecurityGroup', {
      vpc: props.vpc,
      description: 'OrderFlow internal service-to-service traffic (Kafka brokers, Schema Registry)',
      allowAllOutbound: true,
    });
    this.internalSecurityGroup.addIngressRule(
      this.internalSecurityGroup,
      ec2.Port.tcp(9092),
      'Kafka client/inter-broker traffic',
    );
    this.internalSecurityGroup.addIngressRule(
      this.internalSecurityGroup,
      ec2.Port.tcp(9093),
      'Kafka KRaft controller quorum traffic',
    );
    this.internalSecurityGroup.addIngressRule(
      this.internalSecurityGroup,
      ec2.Port.tcp(8085),
      'Schema Registry HTTP traffic',
    );
    this.internalSecurityGroup.addIngressRule(
      this.internalSecurityGroup,
      ec2.Port.tcp(5432),
      'Postgres traffic',
    );
    this.internalSecurityGroup.addIngressRule(
      this.internalSecurityGroup,
      ec2.Port.tcp(6379),
      'Redis traffic',
    );
    this.internalSecurityGroup.addIngressRule(
      this.internalSecurityGroup,
      ec2.Port.tcp(9200),
      'Elasticsearch HTTP traffic',
    );

    // PrivateDnsNamespace: the actual Cloud Map + Route 53 private
    // hosted zone machinery behind "kafka-1.orderflow.local" resolving
    // to whichever IP that broker's task currently has. Associated
    // with the VPC (not the public internet) — these DNS names only
    // resolve for things INSIDE this VPC, which is exactly right: no
    // outside caller should ever need "kafka-1.orderflow.local" to mean
    // anything.
    this.namespace = new servicediscovery.PrivateDnsNamespace(this, 'Namespace', {
      name: NAMESPACE_NAME,
      vpc: props.vpc,
    });

    const kafkaLogGroup = new logs.LogGroup(this, 'KafkaLogGroup', {
      logGroupName: '/orderflow/kafka',
      retention: logs.RetentionDays.THREE_DAYS,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // All 3 brokers' controller quorum voters list is IDENTICAL and
    // must be known BEFORE any individual broker's task definition is
    // built — this is the one piece of real coupling between the 3
    // otherwise-independent broker services.
    const controllerQuorumVoters = [1, 2, 3]
      .map((nodeId) => `${nodeId}@kafka-${nodeId}.${NAMESPACE_NAME}:9093`)
      .join(',');

    for (const nodeId of [1, 2, 3]) {
      this.createKafkaBroker(nodeId, controllerQuorumVoters, props, kafkaLogGroup);
    }

    this.createSchemaRegistry(props);

    // Phase 5b: the 3 stateful dependencies the 8 Spring Boot services
    // (Phase 5c) will need — Postgres (order-service's outbox source of
    // truth), Redis (cache-aside/dedupe/rate-limit/lock, all sharing
    // one instance the same way the local docker-compose.yml does —
    // see that file's own comment for why), and Elasticsearch
    // (search-indexer-service's target). None of these need
    // `distinctInstances` placement — unlike the Kafka brokers, there's
    // only ONE of each here, so there's no "spread across instances"
    // question to force; they simply bin-pack onto the app-tier pool
    // like Schema Registry already does.
    this.createPostgres(props);
    this.createRedis(props);
    this.createElasticsearch(props);
  }

  private createKafkaBroker(
    nodeId: number,
    controllerQuorumVoters: string,
    props: ServicesStackProps,
    logGroup: logs.LogGroup,
  ) {
    const brokerName = `kafka-${nodeId}`;

    const taskDefinition = new ecs.Ec2TaskDefinition(this, `${brokerName}TaskDef`, {
      networkMode: ecs.NetworkMode.AWS_VPC,
      // Host bind-mount to THIS instance's local disk — see
      // docs/aws-cloud-deployment.md's Phase 5a section for why this
      // is a deliberate choice (survives task restarts, does NOT
      // survive the instance dying, which is the exact failure mode
      // this whole deployment exists to demonstrate) and for the
      // Build Order Step 12 callback: KAFKA_LOG_DIRS below points the
      // broker at this SAME path on purpose — apache/kafka:3.8.0's own
      // default log directory does NOT match this mount point unless
      // told to explicitly, a real bug this project already found and
      // fixed once, locally.
      volumes: [
        {
          name: 'kafka-data',
          host: { sourcePath: '/data/kafka' },
        },
      ],
    });

    const container = taskDefinition.addContainer(`${brokerName}Container`, {
      image: ecs.ContainerImage.fromRegistry(KAFKA_IMAGE),
      memoryReservationMiB: 1400,
      logging: ecs.LogDrivers.awsLogs({ streamPrefix: brokerName, logGroup }),
      environment: {
        KAFKA_NODE_ID: String(nodeId),
        KAFKA_PROCESS_ROLES: 'broker,controller',
        KAFKA_LISTENERS: 'PLAINTEXT://:9092,CONTROLLER://:9093',
        KAFKA_ADVERTISED_LISTENERS: `PLAINTEXT://${brokerName}.${NAMESPACE_NAME}:9092`,
        KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
        KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER',
        KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT',
        KAFKA_CONTROLLER_QUORUM_VOTERS: controllerQuorumVoters,
        // Real replication, for the first time in this project's
        // entire history — the local docker-compose.yml's single
        // broker was structurally incapable of anything above 1. With
        // 3 real brokers, these internal topics can finally have
        // actual redundant copies. MIN_ISR=2 with
        // REPLICATION_FACTOR=3 is the standard "tolerate exactly 1
        // broker being down" configuration — the actual number Phase
        // 5's later failover demo will be verifying live.
        KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: '3',
        KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: '3',
        KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: '2',
        KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'false',
        // See this method's own volumes comment — matching the bind
        // mount's target path exactly.
        KAFKA_LOG_DIRS: '/var/lib/kafka/data',
      },
      portMappings: [
        { containerPort: 9092, protocol: ecs.Protocol.TCP },
        { containerPort: 9093, protocol: ecs.Protocol.TCP },
      ],
    });
    container.addMountPoints({
      sourceVolume: 'kafka-data',
      containerPath: '/var/lib/kafka/data',
      readOnly: false,
    });

    new ecs.Ec2Service(this, `${brokerName}Service`, {
      cluster: props.cluster,
      taskDefinition,
      serviceName: brokerName,
      desiredCount: 1,
      // The one line that makes 3 SEPARATE services into a genuine
      // 3-instance cluster instead of 3 tasks that COULD accidentally
      // land on the same EC2 instance — see this class's own top-level
      // Javadoc for why that distinction is the entire point.
      placementConstraints: [ecs.PlacementConstraint.distinctInstances()],
      capacityProviderStrategies: [
        { capacityProvider: props.kafkaCapacityProviderName, weight: 1 },
      ],
      securityGroups: [this.internalSecurityGroup],
      cloudMapOptions: {
        name: brokerName,
        cloudMapNamespace: this.namespace,
        dnsRecordType: servicediscovery.DnsRecordType.A,
      },
      // ECS Exec — the modern replacement for "SSH into the instance
      // to debug a container." No SSH ingress rule exists anywhere in
      // this project's Security Groups (see NetworkStack) on purpose;
      // this is how `docs/aws-cloud-deployment.md`'s later
      // verification steps will actually run `kafka-topics.sh` INSIDE
      // a running broker container without needing one.
      enableExecuteCommand: true,
    });
  }

  private createSchemaRegistry(props: ServicesStackProps) {
    const taskDefinition = new ecs.Ec2TaskDefinition(this, 'SchemaRegistryTaskDef', {
      networkMode: ecs.NetworkMode.AWS_VPC,
    });

    const bootstrapServers = [1, 2, 3]
      .map((nodeId) => `PLAINTEXT://kafka-${nodeId}.${NAMESPACE_NAME}:9092`)
      .join(',');

    taskDefinition.addContainer('SchemaRegistryContainer', {
      image: ecs.ContainerImage.fromRegistry('confluentinc/cp-schema-registry:7.7.0'),
      memoryReservationMiB: 512,
      logging: ecs.LogDrivers.awsLogs({
        streamPrefix: 'schema-registry',
        logGroup: new logs.LogGroup(this, 'SchemaRegistryLogGroup', {
          logGroupName: '/orderflow/schema-registry',
          retention: logs.RetentionDays.THREE_DAYS,
          removalPolicy: cdk.RemovalPolicy.DESTROY,
        }),
      }),
      environment: {
        SCHEMA_REGISTRY_HOST_NAME: 'schema-registry',
        SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: bootstrapServers,
        SCHEMA_REGISTRY_LISTENERS: 'http://0.0.0.0:8085',
      },
      portMappings: [{ containerPort: 8085, protocol: ecs.Protocol.TCP }],
    });

    new ecs.Ec2Service(this, 'SchemaRegistryService', {
      cluster: props.cluster,
      taskDefinition,
      serviceName: 'schema-registry',
      desiredCount: 1,
      // No placement constraint — bin-packs onto the shared app-tier
      // pool with everything else Phase 5b/5c add, same reasoning as
      // every OTHER non-Kafka service in this deployment.
      capacityProviderStrategies: [{ capacityProvider: props.appCapacityProviderName, weight: 1 }],
      securityGroups: [this.internalSecurityGroup],
      cloudMapOptions: {
        name: 'schema-registry',
        cloudMapNamespace: this.namespace,
        dnsRecordType: servicediscovery.DnsRecordType.A,
      },
      enableExecuteCommand: true,
    });
  }

  private createPostgres(props: ServicesStackProps) {
    const taskDefinition = new ecs.Ec2TaskDefinition(this, 'PostgresTaskDef', {
      networkMode: ecs.NetworkMode.AWS_VPC,
      // No chown needed on the host side — see EcsClusterStack's own
      // UserData comment: postgres:16's entrypoint runs AS root and
      // chowns its own data directory internally before dropping to
      // its unprivileged runtime user, unlike Kafka/Elasticsearch.
      volumes: [{ name: 'postgres-data', host: { sourcePath: '/data/postgres' } }],
    });

    const container = taskDefinition.addContainer('PostgresContainer', {
      image: ecs.ContainerImage.fromRegistry('postgres:16'),
      memoryReservationMiB: 512,
      logging: ecs.LogDrivers.awsLogs({
        streamPrefix: 'postgres',
        logGroup: new logs.LogGroup(this, 'PostgresLogGroup', {
          logGroupName: '/orderflow/postgres',
          retention: logs.RetentionDays.THREE_DAYS,
          removalPolicy: cdk.RemovalPolicy.DESTROY,
        }),
      }),
      // Same credentials as the local docker-compose.yml on purpose —
      // order-service's `spring.datasource.*` properties already
      // default to these locally; matching them here means the 8 app
      // services (Phase 5c) need zero code changes, only an
      // environment variable pointing `spring.datasource.url` at
      // `postgres.orderflow.local` instead of `localhost`.
      environment: {
        POSTGRES_USER: 'orderflow',
        POSTGRES_PASSWORD: 'orderflow',
        POSTGRES_DB: 'orderflow',
      },
      portMappings: [{ containerPort: 5432, protocol: ecs.Protocol.TCP }],
    });
    container.addMountPoints({
      sourceVolume: 'postgres-data',
      containerPath: '/var/lib/postgresql/data',
      readOnly: false,
    });

    new ecs.Ec2Service(this, 'PostgresService', {
      cluster: props.cluster,
      taskDefinition,
      serviceName: 'postgres',
      desiredCount: 1,
      capacityProviderStrategies: [{ capacityProvider: props.appCapacityProviderName, weight: 1 }],
      securityGroups: [this.internalSecurityGroup],
      cloudMapOptions: {
        name: 'postgres',
        cloudMapNamespace: this.namespace,
        dnsRecordType: servicediscovery.DnsRecordType.A,
      },
      enableExecuteCommand: true,
    });
  }

  private createRedis(props: ServicesStackProps) {
    // No bind-mount volume — matching the local docker-compose.yml
    // exactly, which also gives Redis no persistent volume. All 4 of
    // Redis's use cases here (cache-aside, dedupe store, rate limiter,
    // distributed lock — see docker-compose.yml's own comment) are
    // legitimately fine losing their contents on a restart: Postgres
    // remains the source of truth for the cache, dedupe/rate-limit
    // state is meant to be short-TTL anyway, and a lost lock just gets
    // re-acquired.
    const taskDefinition = new ecs.Ec2TaskDefinition(this, 'RedisTaskDef', {
      networkMode: ecs.NetworkMode.AWS_VPC,
    });

    taskDefinition.addContainer('RedisContainer', {
      image: ecs.ContainerImage.fromRegistry('redis:7'),
      memoryReservationMiB: 128,
      logging: ecs.LogDrivers.awsLogs({
        streamPrefix: 'redis',
        logGroup: new logs.LogGroup(this, 'RedisLogGroup', {
          logGroupName: '/orderflow/redis',
          retention: logs.RetentionDays.THREE_DAYS,
          removalPolicy: cdk.RemovalPolicy.DESTROY,
        }),
      }),
      portMappings: [{ containerPort: 6379, protocol: ecs.Protocol.TCP }],
    });

    new ecs.Ec2Service(this, 'RedisService', {
      cluster: props.cluster,
      taskDefinition,
      serviceName: 'redis',
      desiredCount: 1,
      capacityProviderStrategies: [{ capacityProvider: props.appCapacityProviderName, weight: 1 }],
      securityGroups: [this.internalSecurityGroup],
      cloudMapOptions: {
        name: 'redis',
        cloudMapNamespace: this.namespace,
        dnsRecordType: servicediscovery.DnsRecordType.A,
      },
      enableExecuteCommand: true,
    });
  }

  private createElasticsearch(props: ServicesStackProps) {
    const taskDefinition = new ecs.Ec2TaskDefinition(this, 'ElasticsearchTaskDef', {
      networkMode: ecs.NetworkMode.AWS_VPC,
      // Host directory pre-created and chowned to uid 1000 by
      // EcsClusterStack's appAsg UserData — elasticsearch:8.15.0 has
      // an explicit `USER 1000:0` Dockerfile directive (confirmed via
      // `docker inspect`), so it never runs as root the way postgres/
      // redis do, and would hit the exact same crash-loop Phase 5a's
      // Kafka brokers hit without this.
      volumes: [{ name: 'es-data', host: { sourcePath: '/data/elasticsearch' } }],
    });

    const container = taskDefinition.addContainer('ElasticsearchContainer', {
      image: ecs.ContainerImage.fromRegistry('docker.elastic.co/elasticsearch/elasticsearch:8.15.0'),
      // Reservation comfortably above the capped 512m JVM heap —
      // Elasticsearch/Lucene use significant off-heap memory (mmap'd
      // segment files, native buffers) beyond the heap itself, the
      // same reasoning the local docker-compose.yml's own
      // ES_JAVA_OPTS comment already calls out.
      memoryReservationMiB: 900,
      logging: ecs.LogDrivers.awsLogs({
        streamPrefix: 'elasticsearch',
        logGroup: new logs.LogGroup(this, 'ElasticsearchLogGroup', {
          logGroupName: '/orderflow/elasticsearch',
          retention: logs.RetentionDays.THREE_DAYS,
          removalPolicy: cdk.RemovalPolicy.DESTROY,
        }),
      }),
      // Identical to docker-compose.yml's own settings — single-node
      // mode and disabled security are the same deliberate dev-only
      // simplification on AWS as they already are locally; see that
      // file's own comment for the full reasoning.
      environment: {
        'discovery.type': 'single-node',
        'xpack.security.enabled': 'false',
        ES_JAVA_OPTS: '-Xms512m -Xmx512m',
      },
      portMappings: [{ containerPort: 9200, protocol: ecs.Protocol.TCP }],
    });
    container.addMountPoints({
      sourceVolume: 'es-data',
      containerPath: '/usr/share/elasticsearch/data',
      readOnly: false,
    });

    new ecs.Ec2Service(this, 'ElasticsearchService', {
      cluster: props.cluster,
      taskDefinition,
      serviceName: 'elasticsearch',
      desiredCount: 1,
      capacityProviderStrategies: [{ capacityProvider: props.appCapacityProviderName, weight: 1 }],
      securityGroups: [this.internalSecurityGroup],
      cloudMapOptions: {
        name: 'elasticsearch',
        cloudMapNamespace: this.namespace,
        dnsRecordType: servicediscovery.DnsRecordType.A,
      },
      enableExecuteCommand: true,
    });
  }
}
