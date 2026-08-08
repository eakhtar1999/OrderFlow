import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as elbv2 from 'aws-cdk-lib/aws-elasticloadbalancingv2';
import * as servicediscovery from 'aws-cdk-lib/aws-servicediscovery';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as ecr from 'aws-cdk-lib/aws-ecr';

export interface AppServicesStackProps extends cdk.StackProps {
  vpc: ec2.IVpc;
  cluster: ecs.Cluster;
  appCapacityProviderName: string;
  albSecurityGroup: ec2.SecurityGroup;
  internalSecurityGroup: ec2.SecurityGroup;
  namespace: servicediscovery.PrivateDnsNamespace;
  repositories: Record<string, ecr.Repository>;
}

const NAMESPACE_NAME = 'orderflow.local';
const KAFKA_BOOTSTRAP_SERVERS = [1, 2, 3]
  .map((n) => `kafka-${n}.${NAMESPACE_NAME}:9092`)
  .join(',');
const SCHEMA_REGISTRY_URL = `http://schema-registry.${NAMESPACE_NAME}:8085`;
const POSTGRES_URL = `jdbc:postgresql://postgres.${NAMESPACE_NAME}:5432/orderflow`;
const REDIS_HOST = `redis.${NAMESPACE_NAME}`;
const ELASTICSEARCH_URIS = `http://elasticsearch.${NAMESPACE_NAME}:9200`;

interface AppServiceSpec {
  /** Matches the ECR repo name (Phase 4) and the Cloud Map name this service registers under. */
  name: string;
  port: number;
  /** ALB path pattern this service is exposed under, e.g. "/orders/*" — omitted for internal-only services. */
  pathPattern?: string;
  memoryReservationMiB: number;
  /** Passed via JAVA_TOOL_OPTIONS — every Dockerfile ENTRYPOINT is a direct `java -jar`, so the JVM honors this automatically with no image change. */
  jvmXmx: string;
  environment: Record<string, string>;
}

// Every address below is already an externalized Spring Boot property
// with a `localhost` default in each service's application.yml (read
// directly, not guessed, before writing this) — these env vars are the
// ENTIRE cloud-specific configuration each service needs. Zero Java
// code or Dockerfile changes anywhere in this phase.
const APP_SERVICES: AppServiceSpec[] = [
  {
    name: 'order-service',
    port: 8080,
    pathPattern: '/orders/*',
    memoryReservationMiB: 400,
    jvmXmx: '256m',
    environment: {
      SPRING_DATASOURCE_URL: POSTGRES_URL,
      SPRING_DATA_REDIS_HOST: REDIS_HOST,
      SPRING_KAFKA_BOOTSTRAP_SERVERS: KAFKA_BOOTSTRAP_SERVERS,
      SPRING_KAFKA_PRODUCER_PROPERTIES_SCHEMA_REGISTRY_URL: SCHEMA_REGISTRY_URL,
      SPRING_KAFKA_CONSUMER_PROPERTIES_SCHEMA_REGISTRY_URL: SCHEMA_REGISTRY_URL,
    },
  },
  {
    name: 'inventory-service',
    port: 8086,
    memoryReservationMiB: 400,
    jvmXmx: '256m',
    environment: {
      SPRING_DATASOURCE_URL: POSTGRES_URL,
      SPRING_DATA_REDIS_HOST: REDIS_HOST,
      SPRING_KAFKA_BOOTSTRAP_SERVERS: KAFKA_BOOTSTRAP_SERVERS,
      SPRING_KAFKA_CONSUMER_PROPERTIES_SCHEMA_REGISTRY_URL: SCHEMA_REGISTRY_URL,
      SPRING_KAFKA_PRODUCER_PROPERTIES_SCHEMA_REGISTRY_URL: SCHEMA_REGISTRY_URL,
    },
  },
  {
    name: 'payment-service',
    port: 8087,
    memoryReservationMiB: 350,
    jvmXmx: '256m',
    environment: {
      SPRING_KAFKA_BOOTSTRAP_SERVERS: KAFKA_BOOTSTRAP_SERVERS,
      SPRING_KAFKA_CONSUMER_PROPERTIES_SCHEMA_REGISTRY_URL: SCHEMA_REGISTRY_URL,
      SPRING_KAFKA_PRODUCER_PROPERTIES_SCHEMA_REGISTRY_URL: SCHEMA_REGISTRY_URL,
    },
  },
  {
    name: 'shipment-service',
    port: 8088,
    memoryReservationMiB: 350,
    jvmXmx: '256m',
    environment: {
      SPRING_KAFKA_BOOTSTRAP_SERVERS: KAFKA_BOOTSTRAP_SERVERS,
      SPRING_KAFKA_CONSUMER_PROPERTIES_SCHEMA_REGISTRY_URL: SCHEMA_REGISTRY_URL,
      SPRING_KAFKA_PRODUCER_PROPERTIES_SCHEMA_REGISTRY_URL: SCHEMA_REGISTRY_URL,
    },
  },
  {
    name: 'fraud-detection-service',
    port: 8082,
    pathPattern: '/fraud/*',
    // Kafka Streams (stateful aggregation + RocksDB state store) —
    // more headroom than the plain-consumer services above.
    memoryReservationMiB: 550,
    jvmXmx: '384m',
    environment: {
      SPRING_KAFKA_BOOTSTRAP_SERVERS: KAFKA_BOOTSTRAP_SERVERS,
      SPRING_KAFKA_STREAMS_PROPERTIES_SCHEMA_REGISTRY_URL: SCHEMA_REGISTRY_URL,
      SPRING_KAFKA_PRODUCER_PROPERTIES_SCHEMA_REGISTRY_URL: SCHEMA_REGISTRY_URL,
    },
  },
  {
    name: 'analytics-service',
    port: 8084,
    pathPattern: '/analytics/*',
    memoryReservationMiB: 550,
    jvmXmx: '384m',
    environment: {
      SPRING_KAFKA_BOOTSTRAP_SERVERS: KAFKA_BOOTSTRAP_SERVERS,
      SPRING_KAFKA_STREAMS_PROPERTIES_SCHEMA_REGISTRY_URL: SCHEMA_REGISTRY_URL,
    },
  },
  {
    name: 'order-saga-orchestrator',
    port: 8089,
    pathPattern: '/saga/*',
    memoryReservationMiB: 450,
    jvmXmx: '320m',
    environment: {
      SPRING_DATASOURCE_URL: POSTGRES_URL,
      // Orchestration-based saga: no Kafka config at all here (see
      // this service's own application.yml) — it reaches the 3
      // downstream services synchronously, over plain HTTP, by Cloud
      // Map name instead.
      SERVICES_INVENTORY_SERVICE_BASE_URL: `http://inventory-service.${NAMESPACE_NAME}:8086`,
      SERVICES_PAYMENT_SERVICE_BASE_URL: `http://payment-service.${NAMESPACE_NAME}:8087`,
      SERVICES_SHIPMENT_SERVICE_BASE_URL: `http://shipment-service.${NAMESPACE_NAME}:8088`,
    },
  },
  {
    name: 'search-indexer-service',
    port: 8090,
    pathPattern: '/search/*',
    memoryReservationMiB: 450,
    jvmXmx: '320m',
    environment: {
      SPRING_ELASTICSEARCH_URIS: ELASTICSEARCH_URIS,
      SPRING_KAFKA_BOOTSTRAP_SERVERS: KAFKA_BOOTSTRAP_SERVERS,
      SPRING_KAFKA_CONSUMER_PROPERTIES_SCHEMA_REGISTRY_URL: SCHEMA_REGISTRY_URL,
    },
  },
];

/** "order-saga-orchestrator" -> "OrderSagaOrchestrator", for construct IDs. */
function pascalCase(kebab: string): string {
  return kebab
    .split('-')
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join('');
}

/**
 * Phase 5c: the last piece before a real order can complete
 * end-to-end on AWS. All 8 Spring Boot services, bin-packed onto the
 * (Phase 5c-grown, 4-instance) app-tier capacity pool — none of them
 * need `distinctInstances`, unlike Phase 5a's Kafka brokers, since
 * there's only ONE of each here; there's no "spread across instances"
 * question when there's nothing to spread.
 *
 * One shared Application Load Balancer with PATH-BASED routing exposes
 * the 5 services that have their own REST API worth reaching from
 * outside the VPC (order-service, order-saga-orchestrator,
 * fraud-detection-service, analytics-service, search-indexer-service).
 * inventory-service, payment-service, and shipment-service stay
 * internal-only — reached purely via Cloud Map, by the orchestrator
 * and each other, exactly matching how they're used locally (nothing
 * outside this deployment ever calls them directly).
 */
export class AppServicesStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: AppServicesStackProps) {
    super(scope, id, props);

    // Phase 5a's internalSecurityGroup only had ingress from ITSELF —
    // correct for Kafka/Postgres/Redis/Elasticsearch/Schema Registry,
    // which only ever talk to each other. The 5 externally-exposed
    // services also need inbound from the ALB's OWN security group, on
    // each service's specific port — awsvpc mode gives every task its
    // OWN ENI wearing internalSecurityGroup, not the underlying EC2
    // instance's ecsInstanceSecurityGroup (whose own ALB-inbound rule,
    // see NetworkStack, therefore does NOT cover task-level traffic).
    for (const spec of APP_SERVICES) {
      if (spec.pathPattern) {
        props.internalSecurityGroup.addIngressRule(
          props.albSecurityGroup,
          ec2.Port.tcp(spec.port),
          `ALB to ${spec.name}`,
        );
      }
    }

    const alb = new elbv2.ApplicationLoadBalancer(this, 'Alb', {
      vpc: props.vpc,
      internetFacing: true,
      securityGroup: props.albSecurityGroup,
      vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
    });

    const listener = alb.addListener('HttpListener', {
      port: 80,
      // No default target — a request matching no path pattern below
      // gets an explicit 404 rather than silently landing on whichever
      // service happened to be registered first.
      defaultAction: elbv2.ListenerAction.fixedResponse(404, {
        contentType: 'text/plain',
        messageBody: 'No OrderFlow service matches this path.',
      }),
    });

    let priority = 10;
    for (const spec of APP_SERVICES) {
      const constructName = pascalCase(spec.name);

      const taskDefinition = new ecs.Ec2TaskDefinition(this, `${constructName}TaskDef`, {
        networkMode: ecs.NetworkMode.AWS_VPC,
      });

      taskDefinition.addContainer(`${constructName}Container`, {
        image: ecs.ContainerImage.fromEcrRepository(props.repositories[spec.name], 'latest'),
        memoryReservationMiB: spec.memoryReservationMiB,
        logging: ecs.LogDrivers.awsLogs({
          streamPrefix: spec.name,
          logGroup: new logs.LogGroup(this, `${constructName}LogGroup`, {
            logGroupName: `/orderflow/${spec.name}`,
            retention: logs.RetentionDays.THREE_DAYS,
            removalPolicy: cdk.RemovalPolicy.DESTROY,
          }),
        }),
        environment: {
          ...spec.environment,
          // Found live: the ALB's path-based routing rule matches a
          // PATTERN ("/orders/*") to pick which target group gets a
          // request, but does NOT strip that prefix before forwarding
          // — ALB has no path-rewrite feature at all. A request for
          // "/orders/api/orders" arrives at the container as literally
          // "/orders/api/orders", which 404s against
          // OrderController's own `/api/orders` mapping. Every one of
          // the 5 externally-exposed services already follows the
          // SAME "/api/<domain>/..." convention (confirmed by reading
          // each controller directly), which makes
          // `server.servlet.context-path` — a standard, already-
          // externalized Spring Boot property, zero Java changes
          // needed — the exact right fix: it makes the app itself
          // respond AT that prefix, matching what the ALB actually
          // forwards.
          ...(spec.pathPattern
            ? { SERVER_SERVLET_CONTEXT_PATH: spec.pathPattern.replace(/\/\*$/, '') }
            : {}),
          // Constrains actual heap usage so N JVMs can safely bin-pack
          // onto one 2 GiB instance — without this, JVM ergonomics
          // size the default heap off the HOST's total memory (visible
          // to the container, since no hard `memory` limit is set,
          // only the soft `memoryReservationMiB` above), not off the
          // much smaller slice this task is actually meant to use.
          JAVA_TOOL_OPTIONS: `-Xmx${spec.jvmXmx} -Xss512k`,
        },
        portMappings: [{ containerPort: spec.port, protocol: ecs.Protocol.TCP }],
      });

      const service = new ecs.Ec2Service(this, `${constructName}Service`, {
        cluster: props.cluster,
        taskDefinition,
        serviceName: spec.name,
        desiredCount: 1,
        capacityProviderStrategies: [
          { capacityProvider: props.appCapacityProviderName, weight: 1 },
        ],
        securityGroups: [props.internalSecurityGroup],
        cloudMapOptions: {
          name: spec.name,
          cloudMapNamespace: props.namespace,
          dnsRecordType: servicediscovery.DnsRecordType.A,
        },
        enableExecuteCommand: true,
      });

      if (spec.pathPattern) {
        const targetGroup = new elbv2.ApplicationTargetGroup(this, `${constructName}TargetGroup`, {
          vpc: props.vpc,
          port: spec.port,
          protocol: elbv2.ApplicationProtocol.HTTP,
          targetType: elbv2.TargetType.IP,
          healthCheck: {
            // server.servlet.context-path (set above) moves EVERY
            // endpoint under that prefix, actuator included — the
            // health check has to follow, or the ALB marks every
            // target permanently unhealthy against a path that no
            // longer resolves to anything.
            path: `${spec.pathPattern!.replace(/\/\*$/, '')}/actuator/health`,
            interval: cdk.Duration.seconds(15),
            healthyHttpCodes: '200',
          },
        });
        service.attachToApplicationTargetGroup(targetGroup);
        listener.addTargetGroups(`${constructName}Rule`, {
          priority: priority++,
          conditions: [elbv2.ListenerCondition.pathPatterns([spec.pathPattern])],
          targetGroups: [targetGroup],
        });
      }
    }

    new cdk.CfnOutput(this, 'AlbDnsName', {
      value: alb.loadBalancerDnsName,
      description: 'Public DNS name of the OrderFlow ALB',
    });
  }
}
