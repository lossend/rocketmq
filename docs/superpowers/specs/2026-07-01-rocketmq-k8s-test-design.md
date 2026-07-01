# RocketMQ K8s Test & Isolation Design

**Date:** 2026-07-01
**Status:** Approved

---

## Goals

1. Create a standalone Maven multi-module project (`../rocketmq-k8s-test/`) providing local Kubernetes deployment manifests, traffic-label isolation integration tests, and performance/load benchmarks for RocketMQ 5.5.0.
2. Add a `rocketmq-plugin` to `urbanic-sergo-client` that injects `__RMQ_TRAFFIC_LABEL` into RocketMQ v5 messages and suffixes the clientId with the isolation label — mirroring the existing `rabbitmq-plugin`.

---

## Scope

### In scope
- K8s manifests for local OrbStack cluster (NameServer ×2, Controller ×3, Broker ×6, Proxy ×2)
- `rocketmq-isolation-test` module: 4 traffic-label routing scenarios, JUnit 5, Venus 2.0.0-SNAPSHOT + native RocketMQ v5 Java client
- `rocketmq-perf-test` module: throughput, latency, and traffic-label overhead benchmarks
- `rocketmq-plugin` JVM Sandbox plugin in `urbanic-sergo-client`

### Out of scope
- Venus site-context propagation testing
- Multi-topic consumer merging
- CI/CD pipeline integration

---

## Architecture

### Repository layout

```
../rocketmq-k8s-test/                    # new standalone project
  pom.xml                                # parent POM
  k8s/                                   # Kubernetes manifests
  rocketmq-isolation-test/               # integration tests
  rocketmq-perf-test/                    # perf/load tests

/Users/lossend/pro/urbanic-sergo-client/
  sergo-plugins/rocketmq-plugin/         # new JVM Sandbox plugin
```

### Key dependencies (parent POM)

| Artifact | Version |
|---|---|
| `com.mayfair.infra:venus` | `2.0.0-SNAPSHOT` (local install from `/Users/lossend/pro/venus`) |
| `org.apache.rocketmq:rocketmq-v5-client-spring-boot-starter` | `2.3.6` |
| `org.junit.jupiter:junit-jupiter` | `5.10.2` |
| `org.awaitility:awaitility` | `4.2.x` |

---

## Deliverable 1 — `rocketmq-plugin` (urbanic-sergo-client)

### Module location
`sergo-plugins/rocketmq-plugin/` — registered in `sergo-plugins/pom.xml`

### Structure

```
src/main/java/com/urbanic/sergo/plugin/rocketmq/
  RocketMQRoutePlugin.java
  advisor/
    ProducerSendAdvisor.java
    advice/ProducerSendAdvice.java
    ClientIdAdvisor.java
    advice/ClientIdAdvice.java
```

### Plugin registration

```java
@MetaInfServices(InvokePlugin.class)
@EnableOnProperty(
    prefix = PluginConstants.SERVICE_ROUTE_PLUGIN_PREFIX,
    name = "rocketmq.enabled",
    havingValue = "true",
    matchIfMissing = false)
public class RocketMQRoutePlugin extends AbstractInvokePlugin { ... }
```

### ProducerSendAdvice logic

Mirrors `ChannelNBasicPublishAdvice.before()`:

1. Read label: `BaggageManager.getBaggage(SERVICE_TAG)`
2. If empty, fall back to `EnvUtil.getServiceEnv()` and push into `BaggageManager`
3. If label is non-empty, inject `__RMQ_TRAFFIC_LABEL` into the `Message` user properties via reflection
4. `after()`: close the `Result` from step 2 if it was created in this call

PointCut target: `org.apache.rocketmq.client.java.impl.producer.ProducerImpl#send`

### ClientIdAdvice logic

1. Read label from `EnvUtil.getServiceEnv()`
2. If non-empty, append `#<label>` to the return value of the intercepted clientId generation method

PointCut target: `org.apache.rocketmq.client.java.misc.Utilities#genClientId` (or equivalent)

---

## Deliverable 2 — Kubernetes Manifests (`k8s/`)

### Cluster topology

| Component | Kind | Replicas | Notes |
|---|---|---|---|
| NameServer | StatefulSet | 2 | Headless service, port 9876 |
| Controller | StatefulSet | 3 | Headless service, port 9878 |
| Broker Master | StatefulSet | 3 | Named `broker-master-0/1/2`, port 10911 |
| Broker Slave | StatefulSet | 3 | Named `broker-slave-0/1/2`, each paired to a master |
| Proxy | Deployment | 2 | NodePort service, port 8081 → 30081 |

### Image
`rocketmq:5.5.0` (built locally, loaded into OrbStack via `docker image load` or direct OrbStack socket)

### Namespace
`rocketmq-test` — all resources deployed here

### Broker config
`ConfigMap` per broker sets:
- `namesrvAddr` pointing to NameServer headless service
- `brokerRole`: `ASYNC_MASTER` or `SLAVE`
- `brokerId`: 0 for masters, 1+ for slaves
- `enablePropertyFilter=true` — required for traffic-label SQL92 filtering

### Directory layout

```
k8s/
  namespace.yaml
  namesrv/
    statefulset.yaml
    service.yaml
  controller/
    statefulset.yaml
    service.yaml
  broker/
    configmap.yaml
    statefulset-master.yaml
    statefulset-slave.yaml
    service.yaml
  proxy/
    deployment.yaml
    service.yaml
```

### Apply order
`namespace → namesrv → controller → broker → proxy`

A `Makefile` target `make deploy` applies in order using `--context=orbstack`.

---

## Deliverable 3 — Isolation Tests (`rocketmq-isolation-test`)

### Agent injection (Maven Surefire)

```xml
<plugin>
  <artifactId>maven-surefire-plugin</artifactId>
  <configuration>
    <argLine>
      -javaagent:${sandbox.home}/sandbox/lib/sandbox-agent.jar
      -Dservice.tag=${SERVICE_TAG}
      -Dsergo.service.route.plugin.enabled=true
      -Dsergo.service.route.plugin.rocketmq.enabled=true
    </argLine>
  </configuration>
</plugin>
```

- `sandbox.home` defaults to `${user.home}`; override via `-Dsandbox.home=...`
- `SERVICE_TAG` passed via `-DSERVICE_TAG=gray1` at test run time
- If sandbox agent jar is absent: `@BeforeAll` calls `assumeTrue(agentJarExists, "sandbox agent not installed — skipping")`

### Proxy endpoint

Resolved from env var `ROCKETMQ_PROXY_ENDPOINT` (e.g. `localhost:30081`).
Tests fail fast with a clear message if the env var is absent.

### Test class: `TrafficLabelIsolationIT`

Each test method:
1. Creates a unique topic + consumer group
2. Starts the required consumer(s) against the K8s proxy
3. Sends one message from a producer with the appropriate `SERVICE_TAG` (set via system property, picked up by the agent)
4. Asserts delivery within 15s using Awaitility

| Test method | Sender env | Consumers present | Assert |
|---|---|---|---|
| `gray1_message_routed_to_gray1_consumer_when_both_exist` | gray1 | gray1 + standard | gray1 receives; standard does not |
| `gray1_message_falls_back_to_standard_when_only_gray2_exists` | gray1 | gray2 + standard | standard receives; gray2 does not |
| `gray1_message_falls_back_to_standard_when_no_isolated_consumer` | gray1 | standard only | standard receives |
| `standard_message_not_routed_to_isolated_consumer` | standard | standard + gray1 | standard receives; gray1 does not |

**Scenarios 2 and 3** validate the fallback-to-standard behavior.
**Scenario 2** additionally asserts no cross-isolation leakage (gray2 must not receive gray1 messages).

### Test dependencies
- `com.mayfair.infra:venus-starter` (Venus producer/consumer via `IVenusProducerAPI` + `@VenusListener`)
- `org.apache.rocketmq:rocketmq-v5-client-spring-boot-starter` (native v5 client for raw producer in scenario control)
- `org.awaitility:awaitility`
- `org.junit.jupiter:junit-jupiter`

---

## Deliverable 4 — Perf Tests (`rocketmq-perf-test`)

No agent. Native RocketMQ v5 Java client only. No Venus dependency.

### Benchmarks

| Class | What it measures | Duration |
|---|---|---|
| `ThroughputBenchmarkTest` | Max msg/sec with 6 concurrent producers, 2 consumers | 60s |
| `LatencyBenchmarkTest` | p50/p95/p99 end-to-end latency at 1000 msg/s steady load | 60s |
| `TrafficLabelOverheadBenchmarkTest` | Repeat throughput + latency with `__RMQ_TRAFFIC_LABEL` set; report delta vs baseline | 2×60s |

### Output format

Each benchmark prints a JSON result block to stdout:

```json
{
  "benchmark": "throughput",
  "messagesPerSecond": 45230,
  "producerCount": 6,
  "durationSeconds": 60,
  "trafficLabel": null
}
```

### Proxy endpoint
Same `ROCKETMQ_PROXY_ENDPOINT` env var as isolation tests.

---

## Routing Rules (from RocketMQ traffic-label implementation)

| Message label | gray1 consumer | gray2 consumer | standard consumer | Receiver |
|---|---|---|---|---|
| gray1 | ✓ present | — | ✓ present | **gray1** |
| gray1 | — | ✓ present | ✓ present | **standard** (fallback) |
| gray1 | — | — | ✓ present | **standard** (fallback) |
| (none) | — | — | ✓ present | **standard** |
| (none) | — | ✓ present | ✓ present | **standard** |

---

## Test Execution

```bash
# 1. Deploy cluster
cd ../rocketmq-k8s-test
make deploy

# 2. Wait for proxy NodePort
export ROCKETMQ_PROXY_ENDPOINT=localhost:30081

# 3. Run isolation tests (with agent)
mvn test -pl rocketmq-isolation-test \
  -DSERVICE_TAG=gray1 \
  -Dsandbox.home=$HOME

# 4. Run perf tests (no agent)
mvn test -pl rocketmq-perf-test
```

---

## Open Questions

None — all design decisions resolved during brainstorming.
