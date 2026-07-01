# RocketMQ K8s Test & Isolation Plugin Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create a `rocketmq-plugin` JVM Sandbox plugin in `urbanic-sergo-client` and a standalone `rocketmq-k8s-test` project with K8s manifests, traffic-label isolation integration tests, and `hey`-driven perf apps.

**Architecture:** The plugin intercepts RocketMQ v5 `ProducerImpl.send()` to inject `__RMQ_TRAFFIC_LABEL` and suffixes `clientId` with the isolation label, mirroring the existing `rabbitmq-plugin`. The test project deploys a full RocketMQ cluster to OrbStack and exercises it via JUnit 5 isolation tests (using the JVM Sandbox agent) and two Spring Boot perf apps driven by `hey`.

**Tech Stack:** Java 8, Maven, JVM Sandbox (Arthas), RocketMQ v5 (`rocketmq-v5-client-spring-boot-starter:2.3.6`), Venus 2.0.0-SNAPSHOT, JUnit Jupiter 5.10.2, Awaitility 4.2.x, Spring Boot 2.7.x, Kubernetes (OrbStack), `hey`

---

## File Map

### Repo: `/Users/lossend/pro/urbanic-sergo-client`

| Action | File |
|---|---|
| Modify | `sergo-plugins/pom.xml` — add `rocketmq-plugin` module |
| Create | `sergo-plugins/rocketmq-plugin/pom.xml` |
| Create | `sergo-plugins/rocketmq-plugin/src/main/java/com/urbanic/sergo/plugin/rocketmq/RocketMQRoutePlugin.java` |
| Create | `sergo-plugins/rocketmq-plugin/src/main/java/com/urbanic/sergo/plugin/rocketmq/advisor/ProducerSendAdvisor.java` |
| Create | `sergo-plugins/rocketmq-plugin/src/main/java/com/urbanic/sergo/plugin/rocketmq/advisor/advice/ProducerSendAdvice.java` |
| Create | `sergo-plugins/rocketmq-plugin/src/main/java/com/urbanic/sergo/plugin/rocketmq/advisor/ClientIdAdvisor.java` |
| Create | `sergo-plugins/rocketmq-plugin/src/main/java/com/urbanic/sergo/plugin/rocketmq/advisor/advice/ClientIdAdvice.java` |

### Repo: `/Users/lossend/pro/rocketmq-k8s-test` (new)

| Action | File |
|---|---|
| Create | `pom.xml` |
| Create | `Makefile` |
| Create | `k8s/namespace.yaml` |
| Create | `k8s/namesrv/statefulset.yaml` |
| Create | `k8s/namesrv/service.yaml` |
| Create | `k8s/controller/statefulset.yaml` |
| Create | `k8s/controller/service.yaml` |
| Create | `k8s/broker/configmap.yaml` |
| Create | `k8s/broker/statefulset-master.yaml` |
| Create | `k8s/broker/statefulset-slave.yaml` |
| Create | `k8s/broker/service.yaml` |
| Create | `k8s/proxy/deployment.yaml` |
| Create | `k8s/proxy/service.yaml` |
| Create | `rocketmq-isolation-test/pom.xml` |
| Create | `rocketmq-isolation-test/src/test/java/com/mayfair/rocketmq/isolation/TrafficLabelIsolationIT.java` |
| Create | `rocketmq-perf-test/pom.xml` |
| Create | `rocketmq-perf-test/perf-producer-app/pom.xml` |
| Create | `rocketmq-perf-test/perf-producer-app/src/main/java/com/mayfair/rocketmq/perf/producer/PerfProducerApplication.java` |
| Create | `rocketmq-perf-test/perf-producer-app/src/main/java/com/mayfair/rocketmq/perf/producer/SendController.java` |
| Create | `rocketmq-perf-test/perf-producer-app/src/main/java/com/mayfair/rocketmq/perf/producer/MetricsController.java` |
| Create | `rocketmq-perf-test/perf-producer-app/src/main/java/com/mayfair/rocketmq/perf/producer/SendMetrics.java` |
| Create | `rocketmq-perf-test/perf-producer-app/src/main/resources/application.yml` |
| Create | `rocketmq-perf-test/perf-consumer-app/pom.xml` |
| Create | `rocketmq-perf-test/perf-consumer-app/src/main/java/com/mayfair/rocketmq/perf/consumer/PerfConsumerApplication.java` |
| Create | `rocketmq-perf-test/perf-consumer-app/src/main/java/com/mayfair/rocketmq/perf/consumer/PerfMessageListener.java` |
| Create | `rocketmq-perf-test/perf-consumer-app/src/main/java/com/mayfair/rocketmq/perf/consumer/ConsumeMetrics.java` |
| Create | `rocketmq-perf-test/perf-consumer-app/src/main/java/com/mayfair/rocketmq/perf/consumer/MetricsController.java` |
| Create | `rocketmq-perf-test/perf-consumer-app/src/main/resources/application.yml` |
| Create | `rocketmq-perf-test/perf-producer-app/starlink.yaml` — Starlink deployment descriptor for producer app |
| Create | `rocketmq-perf-test/perf-consumer-app/starlink.yaml` — Starlink deployment descriptor for consumer app |

---

## Task 1: rocketmq-plugin — module scaffold + pom files

**Files:**
- Modify: `/Users/lossend/pro/urbanic-sergo-client/sergo-plugins/pom.xml`
- Create: `/Users/lossend/pro/urbanic-sergo-client/sergo-plugins/rocketmq-plugin/pom.xml`

- [ ] **Step 1: Add rocketmq-plugin module to sergo-plugins/pom.xml**

Open `/Users/lossend/pro/urbanic-sergo-client/sergo-plugins/pom.xml` and add `<module>rocketmq-plugin</module>` after `rabbitmq-plugin`:

```xml
<modules>
    <module>spring-mvc-plugin</module>
    <module>apache-kafka-plugin</module>
    <module>apache-dubbo-plugin</module>
    <module>rabbitmq-plugin</module>
    <module>rocketmq-plugin</module>
    <module>execute-service-plugin</module>
    <module>spring-reactive-plugin</module>
    <module>okhttp-plugin</module>
</modules>
```

- [ ] **Step 2: Create rocketmq-plugin/pom.xml**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <parent>
        <artifactId>sergo-plugins</artifactId>
        <groupId>com.urbanic.sergo</groupId>
        <version>0.0.11</version>
    </parent>
    <modelVersion>4.0.0</modelVersion>
    <artifactId>rocketmq-plugin</artifactId>
    <properties>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>
    <dependencies>
        <dependency>
            <groupId>org.apache.rocketmq</groupId>
            <artifactId>rocketmq-client-java</artifactId>
            <version>5.0.5</version>
            <scope>provided</scope>
        </dependency>
    </dependencies>
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-assembly-plugin</artifactId>
                <version>3.4.0</version>
                <executions>
                    <execution>
                        <goals><goal>single</goal></goals>
                        <phase>package</phase>
                        <configuration>
                            <descriptorRefs>
                                <descriptorRef>jar-with-dependencies</descriptorRef>
                            </descriptorRefs>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 3: Verify module is recognized**

```bash
cd /Users/lossend/pro/urbanic-sergo-client
mvn -pl sergo-plugins/rocketmq-plugin validate
```

Expected: `BUILD SUCCESS`

- [ ] **Step 4: Commit**

```bash
cd /Users/lossend/pro/urbanic-sergo-client
git add sergo-plugins/pom.xml sergo-plugins/rocketmq-plugin/pom.xml
git commit -m "feat(rocketmq-plugin): scaffold module"
```

---

## Task 2: rocketmq-plugin — ProducerSendAdvisor + ProducerSendAdvice

**Files:**
- Create: `sergo-plugins/rocketmq-plugin/src/main/java/com/urbanic/sergo/plugin/rocketmq/advisor/ProducerSendAdvisor.java`
- Create: `sergo-plugins/rocketmq-plugin/src/main/java/com/urbanic/sergo/plugin/rocketmq/advisor/advice/ProducerSendAdvice.java`

- [ ] **Step 1: Create ProducerSendAdvisor.java**

`sergo-plugins/rocketmq-plugin/src/main/java/com/urbanic/sergo/plugin/rocketmq/advisor/ProducerSendAdvisor.java`:

```java
package com.urbanic.sergo.plugin.rocketmq.advisor;

import com.urbanic.sergo.plugin.InvokeAdvice;
import com.urbanic.sergo.plugin.InvokeAdvisor;
import com.urbanic.sergo.plugin.domain.enhance.PointCut;
import com.urbanic.sergo.plugin.rocketmq.advisor.advice.ProducerSendAdvice;

import java.util.Collections;
import java.util.List;

public class ProducerSendAdvisor implements InvokeAdvisor {

    @Override
    public List<PointCut> getPointCuts() {
        PointCut.MethodPattern method = PointCut.MethodPattern.builder()
                .methodName("send")
                .parameterType(new String[]{
                        "org.apache.rocketmq.client.apis.message.Message",
                        "java.time.Duration"
                })
                .build();
        PointCut point = PointCut.builder()
                .classPattern("org.apache.rocketmq.client.java.impl.producer.ProducerImpl")
                .methodPatterns(new PointCut.MethodPattern[]{method})
                .build();
        return Collections.singletonList(point);
    }

    @Override
    public InvokeAdvice getInvocationAdvice() {
        return new ProducerSendAdvice();
    }
}
```

- [ ] **Step 2: Create ProducerSendAdvice.java**

`sergo-plugins/rocketmq-plugin/src/main/java/com/urbanic/sergo/plugin/rocketmq/advisor/advice/ProducerSendAdvice.java`:

```java
package com.urbanic.sergo.plugin.rocketmq.advisor.advice;

import com.alibaba.jvm.sandbox.api.listener.ext.Advice;
import com.urbanic.sergo.plugin.InvokeAdvice;
import com.urbanic.sergo.plugin.core.baggage.BaggageManager;
import com.urbanic.sergo.plugin.core.baggage.Result;
import com.urbanic.sergo.plugin.core.util.EnvUtil;
import com.urbanic.sergo.plugin.domain.FlowTag;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static com.urbanic.sergo.plugin.Constants.SERVICE_TAG;

@Slf4j
public class ProducerSendAdvice implements InvokeAdvice {

    private static final String TRAFFIC_LABEL_PROPERTY = "__RMQ_TRAFFIC_LABEL";

    @Override
    public void before(Advice advice) {
        Object[] params = advice.getParameterArray();
        if (params == null || params.length < 1 || params[0] == null) {
            return;
        }
        Object message = params[0];

        String tag = BaggageManager.getBaggage(SERVICE_TAG);
        String env = EnvUtil.getServiceEnv();

        if (StringUtils.isEmpty(tag) && StringUtils.isNotEmpty(env)) {
            List<FlowTag> flowTags = FlowTag.fromEnvStr(env);
            if (CollectionUtils.isNotEmpty(flowTags)) {
                Result r = BaggageManager.putBaggage(SERVICE_TAG, FlowTag.toJson(flowTags));
                advice.attach(r);
            }
            tag = BaggageManager.getBaggage(SERVICE_TAG);
        }

        if (StringUtils.isEmpty(tag)) {
            return;
        }

        try {
            injectUserProperty(message, TRAFFIC_LABEL_PROPERTY, extractFirstTag(tag));
        } catch (Exception e) {
            log.warn("Failed to inject {} into RocketMQ message: {}", TRAFFIC_LABEL_PROPERTY, e.getMessage());
        }
    }

    @Override
    public void after(Advice advice) throws Exception {
        if (advice.attachment() instanceof Result) {
            ((Result) advice.attachment()).close();
        }
    }

    private String extractFirstTag(String tagJson) {
        List<FlowTag> tags = FlowTag.fromJson(tagJson);
        if (CollectionUtils.isEmpty(tags)) {
            return tagJson;
        }
        return tags.get(0).getTag();
    }

    @SuppressWarnings("unchecked")
    private void injectUserProperty(Object message, String key, String value) throws Exception {
        Class<?> clazz = message.getClass();
        Field field = null;
        while (clazz != null && field == null) {
            try {
                field = clazz.getDeclaredField("userProperties");
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        if (field == null) {
            throw new NoSuchFieldException("userProperties not found on " + message.getClass().getName());
        }
        field.setAccessible(true);
        Map<String, String> userProps = (Map<String, String>) field.get(message);
        userProps.put(key, value);
        log.info("Injected {}={} into RocketMQ message", key, value);
    }
}
```

- [ ] **Step 3: Build to verify compilation**

```bash
cd /Users/lossend/pro/urbanic-sergo-client
mvn -pl sergo-plugins/rocketmq-plugin compile -q
```

Expected: `BUILD SUCCESS`

- [ ] **Step 4: Commit**

```bash
git add sergo-plugins/rocketmq-plugin/src/
git commit -m "feat(rocketmq-plugin): add ProducerSendAdvisor and ProducerSendAdvice"
```

---

## Task 3: rocketmq-plugin — ClientIdAdvisor + ClientIdAdvice + RocketMQRoutePlugin

**Files:**
- Create: `sergo-plugins/rocketmq-plugin/src/main/java/com/urbanic/sergo/plugin/rocketmq/advisor/ClientIdAdvisor.java`
- Create: `sergo-plugins/rocketmq-plugin/src/main/java/com/urbanic/sergo/plugin/rocketmq/advisor/advice/ClientIdAdvice.java`
- Create: `sergo-plugins/rocketmq-plugin/src/main/java/com/urbanic/sergo/plugin/rocketmq/RocketMQRoutePlugin.java`

- [ ] **Step 1: Create ClientIdAdvisor.java**

```java
package com.urbanic.sergo.plugin.rocketmq.advisor;

import com.urbanic.sergo.plugin.InvokeAdvice;
import com.urbanic.sergo.plugin.InvokeAdvisor;
import com.urbanic.sergo.plugin.domain.enhance.PointCut;
import com.urbanic.sergo.plugin.rocketmq.advisor.advice.ClientIdAdvice;

import java.util.Collections;
import java.util.List;

public class ClientIdAdvisor implements InvokeAdvisor {

    @Override
    public List<PointCut> getPointCuts() {
        PointCut.MethodPattern method = PointCut.MethodPattern.builder()
                .methodName("genClientId")
                .parameterType(new String[]{})
                .build();
        PointCut point = PointCut.builder()
                .classPattern("org.apache.rocketmq.client.java.misc.Utilities")
                .methodPatterns(new PointCut.MethodPattern[]{method})
                .build();
        return Collections.singletonList(point);
    }

    @Override
    public InvokeAdvice getInvocationAdvice() {
        return new ClientIdAdvice();
    }
}
```

- [ ] **Step 2: Create ClientIdAdvice.java**

```java
package com.urbanic.sergo.plugin.rocketmq.advisor.advice;

import com.alibaba.jvm.sandbox.api.ProcessControlException;
import com.alibaba.jvm.sandbox.api.ProcessController;
import com.alibaba.jvm.sandbox.api.listener.ext.Advice;
import com.urbanic.sergo.plugin.InvokeAdvice;
import com.urbanic.sergo.plugin.core.util.EnvUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class ClientIdAdvice implements InvokeAdvice {

    @Override
    public void before(Advice advice) {
        // no-op: intercept on return
    }

    @Override
    public void after(Advice advice) throws ProcessControlException {
        String env = EnvUtil.getServiceEnv();
        if (StringUtils.isEmpty(env)) {
            return;
        }
        Object returnObj = advice.getReturnObj();
        if (!(returnObj instanceof String)) {
            return;
        }
        String original = (String) returnObj;
        String modified = original + "#" + env;
        log.info("RocketMQ clientId modified: {} -> {}", original, modified);
        ProcessController.returnImmediately(modified);
    }
}
```

- [ ] **Step 3: Create RocketMQRoutePlugin.java**

```java
package com.urbanic.sergo.plugin.rocketmq;

import com.google.common.collect.Lists;
import com.urbanic.sergo.plugin.InvokeAdvisor;
import com.urbanic.sergo.plugin.PluginConstants;
import com.urbanic.sergo.plugin.PluginContext;
import com.urbanic.sergo.plugin.PluginType;
import com.urbanic.sergo.plugin.annotation.EnableOnProperty;
import com.urbanic.sergo.plugin.core.impl.spi.AbstractInvokePlugin;
import com.urbanic.sergo.plugin.rocketmq.advisor.ClientIdAdvisor;
import com.urbanic.sergo.plugin.rocketmq.advisor.ProducerSendAdvisor;
import com.urbanic.sergo.plugin.spi.InvokePlugin;
import org.kohsuke.MetaInfServices;

import java.util.List;

@MetaInfServices(InvokePlugin.class)
@EnableOnProperty(
        prefix = PluginConstants.SERVICE_ROUTE_PLUGIN_PREFIX,
        name = "rocketmq.enabled",
        havingValue = "true",
        matchIfMissing = false)
public class RocketMQRoutePlugin extends AbstractInvokePlugin {

    @Override
    public String name() {
        return "rocketmq-route";
    }

    @Override
    public boolean enable(PluginContext context) {
        return true;
    }

    @Override
    public List<InvokeAdvisor> getAdvisorList() {
        return Lists.newArrayList(new ProducerSendAdvisor(), new ClientIdAdvisor());
    }

    @Override
    public PluginType type() {
        return PluginType.SERVICE_ROUTE;
    }
}
```

- [ ] **Step 4: Build and package**

```bash
cd /Users/lossend/pro/urbanic-sergo-client
mvn -pl sergo-plugins/rocketmq-plugin package -q
```

Expected: `BUILD SUCCESS`, jar produced at `sergo-plugins/rocketmq-plugin/target/rocketmq-plugin-0.0.11-jar-with-dependencies.jar`

- [ ] **Step 5: Commit**

```bash
git add sergo-plugins/rocketmq-plugin/src/
git commit -m "feat(rocketmq-plugin): add ClientIdAdvisor and RocketMQRoutePlugin"
```

---

## Task 4: rocketmq-k8s-test — project scaffold + parent POM

**Files:**
- Create: `/Users/lossend/pro/rocketmq-k8s-test/pom.xml`

- [ ] **Step 1: Create project directory and parent pom.xml**

```bash
mkdir -p /Users/lossend/pro/rocketmq-k8s-test
cd /Users/lossend/pro/rocketmq-k8s-test
git init
```

Create `pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.mayfair.rocketmq</groupId>
    <artifactId>rocketmq-k8s-test</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <packaging>pom</packaging>

    <modules>
        <module>rocketmq-isolation-test</module>
        <module>rocketmq-perf-test</module>
    </modules>

    <properties>
        <java.version>8</java.version>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <spring-boot.version>2.7.18</spring-boot.version>
        <rocketmq-spring.version>2.3.6</rocketmq-spring.version>
        <venus.version>2.0.0-SNAPSHOT</venus.version>
        <junit-jupiter.version>5.10.2</junit-jupiter.version>
        <awaitility.version>4.2.1</awaitility.version>
    </properties>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-dependencies</artifactId>
                <version>${spring-boot.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
            <dependency>
                <groupId>org.apache.rocketmq</groupId>
                <artifactId>rocketmq-v5-client-spring-boot-starter</artifactId>
                <version>${rocketmq-spring.version}</version>
            </dependency>
            <dependency>
                <groupId>com.mayfair.infra</groupId>
                <artifactId>venus-starter</artifactId>
                <version>${venus.version}</version>
            </dependency>
            <dependency>
                <groupId>org.junit.jupiter</groupId>
                <artifactId>junit-jupiter</artifactId>
                <version>${junit-jupiter.version}</version>
            </dependency>
            <dependency>
                <groupId>org.awaitility</groupId>
                <artifactId>awaitility</artifactId>
                <version>${awaitility.version}</version>
            </dependency>
        </dependencies>
    </dependencyManagement>
</project>
```

- [ ] **Step 2: Install Venus SNAPSHOT to local Maven repo**

```bash
cd /Users/lossend/pro/venus
mvn install -DskipTests -q
```

Expected: `BUILD SUCCESS`

- [ ] **Step 3: Verify parent POM parses**

```bash
cd /Users/lossend/pro/rocketmq-k8s-test
mvn validate
```

Expected: `BUILD SUCCESS`

- [ ] **Step 4: Commit**

```bash
git add pom.xml
git commit -m "feat: scaffold rocketmq-k8s-test parent POM"
```

---

## Task 5: K8s manifests — namespace, NameServer, Controller

**Files:** `k8s/namespace.yaml`, `k8s/namesrv/`, `k8s/controller/`

- [ ] **Step 1: Create namespace.yaml**

`k8s/namespace.yaml`:
```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: rocketmq-test
```

- [ ] **Step 2: Create namesrv StatefulSet**

`k8s/namesrv/statefulset.yaml`:
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: namesrv
  namespace: rocketmq-test
spec:
  serviceName: namesrv-headless
  replicas: 2
  selector:
    matchLabels:
      app: namesrv
  template:
    metadata:
      labels:
        app: namesrv
    spec:
      containers:
        - name: namesrv
          image: rocketmq:5.5.0
          imagePullPolicy: Never
          command: ["./mqnamesrv"]
          env:
            - name: JAVA_OPT_EXT
              value: "-Xms256m -Xmx256m -Xmn128m"
          ports:
            - containerPort: 9876
          readinessProbe:
            tcpSocket:
              port: 9876
            initialDelaySeconds: 10
            periodSeconds: 5
```

`k8s/namesrv/service.yaml`:
```yaml
apiVersion: v1
kind: Service
metadata:
  name: namesrv-headless
  namespace: rocketmq-test
spec:
  clusterIP: None
  selector:
    app: namesrv
  ports:
    - port: 9876
      targetPort: 9876
```

- [ ] **Step 3: Create controller StatefulSet**

`k8s/controller/statefulset.yaml`:
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: controller
  namespace: rocketmq-test
spec:
  serviceName: controller-headless
  replicas: 3
  selector:
    matchLabels:
      app: controller
  template:
    metadata:
      labels:
        app: controller
    spec:
      containers:
        - name: controller
          image: rocketmq:5.5.0
          imagePullPolicy: Never
          command: ["./mqcontroller"]
          env:
            - name: JAVA_OPT_EXT
              value: "-Xms256m -Xmx256m -Xmn128m"
          ports:
            - containerPort: 9878
          readinessProbe:
            tcpSocket:
              port: 9878
            initialDelaySeconds: 10
            periodSeconds: 5
```

`k8s/controller/service.yaml`:
```yaml
apiVersion: v1
kind: Service
metadata:
  name: controller-headless
  namespace: rocketmq-test
spec:
  clusterIP: None
  selector:
    app: controller
  ports:
    - port: 9878
      targetPort: 9878
```

- [ ] **Step 4: Commit**

```bash
cd /Users/lossend/pro/rocketmq-k8s-test
git add k8s/namespace.yaml k8s/namesrv/ k8s/controller/
git commit -m "feat(k8s): add namespace, namesrv, controller manifests"
```

---

## Task 6: K8s manifests — Broker (3 masters + 3 slaves) + Proxy

**Files:** `k8s/broker/`, `k8s/proxy/`

- [ ] **Step 1: Create broker ConfigMap**

`k8s/broker/configmap.yaml` — one config for masters, one for slaves:
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: broker-master-config
  namespace: rocketmq-test
data:
  broker.conf: |
    brokerClusterName=DefaultCluster
    brokerRole=ASYNC_MASTER
    brokerId=0
    namesrvAddr=namesrv-headless.rocketmq-test.svc.cluster.local:9876
    enablePropertyFilter=true
    autoCreateTopicEnable=true
    deleteWhen=04
    fileReservedTime=48
    brokerIP1=$(MY_POD_IP)
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: broker-slave-config
  namespace: rocketmq-test
data:
  broker.conf: |
    brokerClusterName=DefaultCluster
    brokerRole=SLAVE
    brokerId=1
    namesrvAddr=namesrv-headless.rocketmq-test.svc.cluster.local:9876
    enablePropertyFilter=true
    autoCreateTopicEnable=true
    deleteWhen=04
    fileReservedTime=48
    brokerIP1=$(MY_POD_IP)
```

- [ ] **Step 2: Create broker master StatefulSet**

`k8s/broker/statefulset-master.yaml`:
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: broker-master
  namespace: rocketmq-test
spec:
  serviceName: broker-master
  replicas: 3
  selector:
    matchLabels:
      app: broker-master
  template:
    metadata:
      labels:
        app: broker-master
    spec:
      containers:
        - name: broker
          image: rocketmq:5.5.0
          imagePullPolicy: Never
          command: ["./mqbroker", "-c", "/etc/rocketmq/broker.conf"]
          env:
            - name: JAVA_OPT_EXT
              value: "-Xms512m -Xmx512m -Xmn256m"
            - name: MY_POD_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP
          ports:
            - containerPort: 10909
            - containerPort: 10911
            - containerPort: 10912
          volumeMounts:
            - name: config
              mountPath: /etc/rocketmq
          readinessProbe:
            tcpSocket:
              port: 10911
            initialDelaySeconds: 20
            periodSeconds: 5
      volumes:
        - name: config
          configMap:
            name: broker-master-config
```

- [ ] **Step 3: Create broker slave StatefulSet**

`k8s/broker/statefulset-slave.yaml`:
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: broker-slave
  namespace: rocketmq-test
spec:
  serviceName: broker-slave
  replicas: 3
  selector:
    matchLabels:
      app: broker-slave
  template:
    metadata:
      labels:
        app: broker-slave
    spec:
      containers:
        - name: broker
          image: rocketmq:5.5.0
          imagePullPolicy: Never
          command: ["./mqbroker", "-c", "/etc/rocketmq/broker.conf"]
          env:
            - name: JAVA_OPT_EXT
              value: "-Xms256m -Xmx256m -Xmn128m"
            - name: MY_POD_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP
          ports:
            - containerPort: 10909
            - containerPort: 10911
            - containerPort: 10912
          volumeMounts:
            - name: config
              mountPath: /etc/rocketmq
          readinessProbe:
            tcpSocket:
              port: 10911
            initialDelaySeconds: 20
            periodSeconds: 5
      volumes:
        - name: config
          configMap:
            name: broker-slave-config
```

`k8s/broker/service.yaml`:
```yaml
apiVersion: v1
kind: Service
metadata:
  name: broker-master
  namespace: rocketmq-test
spec:
  clusterIP: None
  selector:
    app: broker-master
  ports:
    - name: vip
      port: 10909
    - name: main
      port: 10911
    - name: ha
      port: 10912
---
apiVersion: v1
kind: Service
metadata:
  name: broker-slave
  namespace: rocketmq-test
spec:
  clusterIP: None
  selector:
    app: broker-slave
  ports:
    - name: main
      port: 10911
```

- [ ] **Step 4: Create Proxy Deployment**

`k8s/proxy/deployment.yaml`:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: proxy
  namespace: rocketmq-test
spec:
  replicas: 2
  selector:
    matchLabels:
      app: proxy
  template:
    metadata:
      labels:
        app: proxy
    spec:
      containers:
        - name: proxy
          image: rocketmq:5.5.0
          imagePullPolicy: Never
          command: ["./mqproxy"]
          env:
            - name: NAMESRV_ADDR
              value: "namesrv-headless.rocketmq-test.svc.cluster.local:9876"
            - name: JAVA_OPT_EXT
              value: "-Xms256m -Xmx256m -Xmn128m"
          ports:
            - containerPort: 8081
          readinessProbe:
            tcpSocket:
              port: 8081
            initialDelaySeconds: 15
            periodSeconds: 5
```

`k8s/proxy/service.yaml`:
```yaml
apiVersion: v1
kind: Service
metadata:
  name: proxy-nodeport
  namespace: rocketmq-test
spec:
  type: NodePort
  selector:
    app: proxy
  ports:
    - port: 8081
      targetPort: 8081
      nodePort: 30081
```

- [ ] **Step 5: Commit**

```bash
git add k8s/broker/ k8s/proxy/
git commit -m "feat(k8s): add broker (3 master + 3 slave) and proxy manifests"
```

---

## Task 7: Makefile for cluster lifecycle

**Files:** `Makefile`

- [ ] **Step 1: Create Makefile**

```makefile
KUBE_CONTEXT = orbstack
NAMESPACE    = rocketmq-test
NS           = --context=$(KUBE_CONTEXT) -n $(NAMESPACE)

.PHONY: deploy teardown status

deploy:
	kubectl apply --context=$(KUBE_CONTEXT) -f k8s/namespace.yaml
	kubectl apply $(NS) -f k8s/namesrv/
	kubectl apply $(NS) -f k8s/controller/
	kubectl apply $(NS) -f k8s/broker/
	kubectl apply $(NS) -f k8s/proxy/
	@echo "Waiting for proxy to be ready..."
	kubectl rollout status $(NS) deployment/proxy --timeout=120s

teardown:
	kubectl delete namespace $(NAMESPACE) --context=$(KUBE_CONTEXT) --ignore-not-found

status:
	kubectl get all $(NS)

# Perf apps (perf-producer-app, perf-consumer-app) are deployed via Starlink.
# See: rocketmq-perf-test/perf-producer-app/starlink.yaml
#      rocketmq-perf-test/perf-consumer-app/starlink.yaml
```

- [ ] **Step 2: Smoke-test deploy**

```bash
cd /Users/lossend/pro/rocketmq-k8s-test
make deploy
```

Expected: all pods reach `Ready` state. Verify:
```bash
kubectl get pods -n rocketmq-test --context=orbstack
```
Expected: 2 namesrv + 3 controller + 3 broker-master + 3 broker-slave + 2 proxy pods all `Running`.

- [ ] **Step 3: Commit**

```bash
git add Makefile
git commit -m "feat: add Makefile for cluster lifecycle management"
```

---

## Task 8: rocketmq-isolation-test — module + TrafficLabelIsolationIT

**Files:**
- Create: `rocketmq-isolation-test/pom.xml`
- Create: `rocketmq-isolation-test/src/test/java/com/mayfair/rocketmq/isolation/TrafficLabelIsolationIT.java`

- [ ] **Step 1: Create rocketmq-isolation-test/pom.xml**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <parent>
        <groupId>com.mayfair.rocketmq</groupId>
        <artifactId>rocketmq-k8s-test</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>
    <modelVersion>4.0.0</modelVersion>
    <artifactId>rocketmq-isolation-test</artifactId>
    <packaging>jar</packaging>

    <dependencies>
        <dependency>
            <groupId>org.apache.rocketmq</groupId>
            <artifactId>rocketmq-v5-client-spring-boot-starter</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>com.mayfair.infra</groupId>
            <artifactId>venus-starter</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.awaitility</groupId>
            <artifactId>awaitility</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>3.0.0-M9</version>
                <configuration>
                    <argLine>
                        -javaagent:${sandbox.home}/sandbox/lib/sandbox-agent.jar
                        -Dservice.tag=${SERVICE_TAG}
                        -Dsergo.service.route.plugin.enabled=true
                        -Dsergo.service.route.plugin.rocketmq.enabled=true
                    </argLine>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 2: Create TrafficLabelIsolationIT.java**

```java
package com.mayfair.rocketmq.isolation;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests for traffic-label isolation routing on a real K8s RocketMQ cluster.
 *
 * Prerequisites:
 *   1. make deploy (OrbStack cluster running)
 *   2. ROCKETMQ_PROXY_ENDPOINT=localhost:30081
 *   3. sandbox agent installed at ${sandbox.home}/sandbox/lib/sandbox-agent.jar
 *   4. Run with -DSERVICE_TAG=gray1 to simulate isolated producer environment
 */
class TrafficLabelIsolationIT {

    private static final String ENDPOINT =
            System.getenv().getOrDefault("ROCKETMQ_PROXY_ENDPOINT", "localhost:30081");

    private static final String TRAFFIC_LABEL_PROPERTY = "__RMQ_TRAFFIC_LABEL";

    private final List<SimpleConsumer> consumers = new ArrayList<>();
    private final List<Thread> pollers = new ArrayList<>();

    @BeforeAll
    static void checkPrerequisites() {
        String sandboxHome = System.getProperty("sandbox.home", System.getProperty("user.home"));
        File agentJar = new File(sandboxHome + "/sandbox/lib/sandbox-agent.jar");
        assumeTrue(agentJar.exists(),
                "sandbox agent not installed at " + agentJar.getAbsolutePath() + " — skipping isolation tests");
        assumeTrue(System.getenv("ROCKETMQ_PROXY_ENDPOINT") != null || true,
                "ROCKETMQ_PROXY_ENDPOINT not set — using default " + ENDPOINT);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (Thread t : pollers) {
            t.interrupt();
        }
        for (SimpleConsumer c : consumers) {
            c.close();
        }
        pollers.clear();
        consumers.clear();
    }

    /**
     * Scenario 1: gray1 message → gray1 consumer + standard consumer present → gray1 receives.
     */
    @Test
    void gray1_message_routed_to_gray1_consumer_when_both_exist() throws Exception {
        String topic = "test-topic-" + UUID.randomUUID().toString().substring(0, 8);
        String group = "cid-" + UUID.randomUUID().toString().substring(0, 8);

        List<MessageView> gray1Received = new CopyOnWriteArrayList<>();
        List<MessageView> standardReceived = new CopyOnWriteArrayList<>();

        startConsumer(topic, group + "-gray1", "gray1", gray1Received);
        startConsumer(topic, group + "-std", null, standardReceived);

        sendMessage(topic, "gray1", "hello-gray1");

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertFalse(gray1Received.isEmpty(), "gray1 consumer must receive the message"));
        assertTrue(standardReceived.isEmpty(), "standard consumer must not receive the gray1 message");
    }

    /**
     * Scenario 2: gray1 message → gray2 + standard consumers → standard receives; gray2 does not.
     */
    @Test
    void gray1_message_falls_back_to_standard_when_only_gray2_exists() throws Exception {
        String topic = "test-topic-" + UUID.randomUUID().toString().substring(0, 8);
        String group = "cid-" + UUID.randomUUID().toString().substring(0, 8);

        List<MessageView> gray2Received = new CopyOnWriteArrayList<>();
        List<MessageView> standardReceived = new CopyOnWriteArrayList<>();

        startConsumer(topic, group + "-gray2", "gray2", gray2Received);
        startConsumer(topic, group + "-std", null, standardReceived);

        sendMessage(topic, "gray1", "hello-fallback");

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertFalse(standardReceived.isEmpty(), "standard consumer must receive gray1 message via fallback"));
        assertTrue(gray2Received.isEmpty(), "gray2 consumer must not receive gray1 message");
    }

    /**
     * Scenario 3: gray1 message → standard consumer only → standard receives.
     */
    @Test
    void gray1_message_falls_back_to_standard_when_no_isolated_consumer() throws Exception {
        String topic = "test-topic-" + UUID.randomUUID().toString().substring(0, 8);
        String group = "cid-" + UUID.randomUUID().toString().substring(0, 8);

        List<MessageView> standardReceived = new CopyOnWriteArrayList<>();
        startConsumer(topic, group + "-std", null, standardReceived);

        sendMessage(topic, "gray1", "hello-no-isolated");

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertFalse(standardReceived.isEmpty(), "standard consumer must receive gray1 message when no gray1 consumer exists"));
    }

    /**
     * Scenario 4: standard message → standard + gray1 consumers → standard receives; gray1 does not.
     */
    @Test
    void standard_message_not_routed_to_isolated_consumer() throws Exception {
        String topic = "test-topic-" + UUID.randomUUID().toString().substring(0, 8);
        String group = "cid-" + UUID.randomUUID().toString().substring(0, 8);

        List<MessageView> gray1Received = new CopyOnWriteArrayList<>();
        List<MessageView> standardReceived = new CopyOnWriteArrayList<>();

        startConsumer(topic, group + "-gray1", "gray1", gray1Received);
        startConsumer(topic, group + "-std", null, standardReceived);

        sendMessage(topic, null, "hello-standard");

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertFalse(standardReceived.isEmpty(), "standard consumer must receive unlabeled message"));
        assertTrue(gray1Received.isEmpty(), "gray1 consumer must not receive standard message");
    }

    private void sendMessage(String topic, String trafficLabel, String body) throws Exception {
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfiguration config = ClientConfiguration.newBuilder()
                .setEndpoints(ENDPOINT)
                .build();
        try (Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(config)
                .setTopics(topic)
                .build()) {
            Message.Builder builder = provider.newMessageBuilder()
                    .setTopic(topic)
                    .setBody(body.getBytes(StandardCharsets.UTF_8));
            if (trafficLabel != null) {
                builder.addProperty(TRAFFIC_LABEL_PROPERTY, trafficLabel);
            }
            producer.send(builder.build());
        }
    }

    private void startConsumer(String topic, String consumerGroup, String trafficLabel,
            List<MessageView> received) throws Exception {
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfiguration config = ClientConfiguration.newBuilder()
                .setEndpoints(ENDPOINT)
                .build();

        FilterExpression filter = trafficLabel != null
                ? new FilterExpression(TRAFFIC_LABEL_PROPERTY + " = '" + trafficLabel + "'",
                        FilterExpressionType.SQL92)
                : new FilterExpression("*", FilterExpressionType.TAG);

        SimpleConsumer consumer = provider.newSimpleConsumerBuilder()
                .setClientConfiguration(config)
                .setConsumerGroup(consumerGroup)
                .setAwaitDuration(Duration.ofSeconds(3))
                .setSubscriptionExpressions(Collections.singletonMap(topic, filter))
                .build();
        consumers.add(consumer);

        AtomicBoolean running = new AtomicBoolean(true);
        Thread poller = new Thread(() -> {
            while (running.get() && !Thread.currentThread().isInterrupted()) {
                try {
                    List<MessageView> msgs = consumer.receive(10, Duration.ofSeconds(3));
                    for (MessageView msg : msgs) {
                        received.add(msg);
                        consumer.ack(msg);
                    }
                } catch (Exception e) {
                    if (Thread.currentThread().isInterrupted()) break;
                }
            }
        }, "consumer-" + consumerGroup);
        poller.setDaemon(true);
        poller.start();
        pollers.add(poller);
    }
}
```

- [ ] **Step 3: Compile test (without running)**

```bash
cd /Users/lossend/pro/rocketmq-k8s-test
mvn test-compile -pl rocketmq-isolation-test -q
```

Expected: `BUILD SUCCESS`

- [ ] **Step 4: Run isolation tests against live cluster**

Ensure cluster is deployed and `ROCKETMQ_PROXY_ENDPOINT` is set:
```bash
export ROCKETMQ_PROXY_ENDPOINT=localhost:30081
mvn test -pl rocketmq-isolation-test \
  -DSERVICE_TAG=gray1 \
  -Dsandbox.home=$HOME
```

Expected: all 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add rocketmq-isolation-test/
git commit -m "feat(isolation-test): add 4 traffic-label routing scenarios"
```

---

## Task 9: perf-producer-app (Spring Boot)

**Files:** `rocketmq-perf-test/perf-producer-app/`

- [ ] **Step 1: Create rocketmq-perf-test/pom.xml**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <parent>
        <groupId>com.mayfair.rocketmq</groupId>
        <artifactId>rocketmq-k8s-test</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>
    <modelVersion>4.0.0</modelVersion>
    <artifactId>rocketmq-perf-test</artifactId>
    <packaging>pom</packaging>
    <modules>
        <module>perf-producer-app</module>
        <module>perf-consumer-app</module>
    </modules>
</project>
```

- [ ] **Step 2: Create perf-producer-app/pom.xml**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <parent>
        <artifactId>rocketmq-perf-test</artifactId>
        <groupId>com.mayfair.rocketmq</groupId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>
    <modelVersion>4.0.0</modelVersion>
    <artifactId>perf-producer-app</artifactId>
    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.rocketmq</groupId>
            <artifactId>rocketmq-v5-client-spring-boot-starter</artifactId>
        </dependency>
    </dependencies>
    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <version>${spring-boot.version}</version>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 3: Create SendMetrics.java**

`src/main/java/com/mayfair/rocketmq/perf/producer/SendMetrics.java`:
```java
package com.mayfair.rocketmq.perf.producer;

import org.springframework.stereotype.Component;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class SendMetrics {
    private final AtomicLong totalSent = new AtomicLong();
    private final AtomicLong errorCount = new AtomicLong();
    private final long startTimeMs = System.currentTimeMillis();

    public void recordSent() { totalSent.incrementAndGet(); }
    public void recordError() { errorCount.incrementAndGet(); }

    public long totalSent() { return totalSent.get(); }
    public long errorCount() { return errorCount.get(); }
    public double sendRatePerSec() {
        long elapsed = Math.max(1, System.currentTimeMillis() - startTimeMs);
        return totalSent.get() * 1000.0 / elapsed;
    }
}
```

- [ ] **Step 4: Create SendController.java**

`src/main/java/com/mayfair/rocketmq/perf/producer/SendController.java`:
```java
package com.mayfair.rocketmq.perf.producer;

import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;

@RestController
public class SendController {

    @Value("${rocketmq.topic}")
    private String topic;

    @Autowired
    private SendMetrics metrics;

    private Producer producer;

    @PostConstruct
    public void init() throws Exception {
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        producer = provider.newProducerBuilder()
                .setClientConfiguration(
                    org.apache.rocketmq.client.apis.ClientConfiguration.newBuilder()
                        .setEndpoints(System.getenv("ROCKETMQ_PROXY_ENDPOINT"))
                        .build())
                .setTopics(topic)
                .build();
    }

    @PostMapping("/send")
    public String send() {
        try {
            ClientServiceProvider provider = ClientServiceProvider.loadService();
            Message msg = provider.newMessageBuilder()
                    .setTopic(topic)
                    .addProperty("x-send-time", String.valueOf(System.currentTimeMillis()))
                    .setBody("perf-payload".getBytes(StandardCharsets.UTF_8))
                    .build();
            producer.send(msg);
            metrics.recordSent();
            return "ok";
        } catch (Exception e) {
            metrics.recordError();
            return "error: " + e.getMessage();
        }
    }
}
```

- [ ] **Step 5: Create MetricsController.java (producer)**

`src/main/java/com/mayfair/rocketmq/perf/producer/MetricsController.java`:
```java
package com.mayfair.rocketmq.perf.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
public class MetricsController {

    @Autowired
    private SendMetrics metrics;

    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("role", "producer");
        m.put("totalSent", metrics.totalSent());
        m.put("errorCount", metrics.errorCount());
        m.put("sendRatePerSec", Math.round(metrics.sendRatePerSec()));
        return m;
    }
}
```

- [ ] **Step 6: Create PerfProducerApplication.java**

`src/main/java/com/mayfair/rocketmq/perf/producer/PerfProducerApplication.java`:
```java
package com.mayfair.rocketmq.perf.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class PerfProducerApplication {
    public static void main(String[] args) {
        SpringApplication.run(PerfProducerApplication.class, args);
    }
}
```

- [ ] **Step 7: Create application.yml**

`src/main/resources/application.yml`:
```yaml
server:
  port: 8080

rocketmq:
  topic: perf-test-topic
```

- [ ] **Step 8: Build**

```bash
cd /Users/lossend/pro/rocketmq-k8s-test
mvn package -pl rocketmq-perf-test/perf-producer-app -DskipTests -q
```

Expected: `BUILD SUCCESS`, fat jar at `rocketmq-perf-test/perf-producer-app/target/perf-producer-app-1.0.0-SNAPSHOT.jar`

- [ ] **Step 9: Commit**

```bash
git add rocketmq-perf-test/
git commit -m "feat(perf): add perf-producer-app"
```

---

## Task 10: perf-consumer-app (Spring Boot)

**Files:** `rocketmq-perf-test/perf-consumer-app/`

- [ ] **Step 1: Create perf-consumer-app/pom.xml**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <parent>
        <artifactId>rocketmq-perf-test</artifactId>
        <groupId>com.mayfair.rocketmq</groupId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>
    <modelVersion>4.0.0</modelVersion>
    <artifactId>perf-consumer-app</artifactId>
    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.rocketmq</groupId>
            <artifactId>rocketmq-v5-client-spring-boot-starter</artifactId>
        </dependency>
        <dependency>
            <groupId>org.hdrhistogram</groupId>
            <artifactId>HdrHistogram</artifactId>
            <version>2.1.12</version>
        </dependency>
    </dependencies>
    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <version>${spring-boot.version}</version>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 2: Create ConsumeMetrics.java**

`src/main/java/com/mayfair/rocketmq/perf/consumer/ConsumeMetrics.java`:
```java
package com.mayfair.rocketmq.perf.consumer;

import org.HdrHistogram.Histogram;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Component
public class ConsumeMetrics {
    private final Histogram histogram = new Histogram(60_000L, 3);
    private final AtomicLong totalReceived = new AtomicLong();
    private final long startTimeMs = System.currentTimeMillis();

    public void record(long latencyMs) {
        totalReceived.incrementAndGet();
        histogram.recordValue(Math.min(latencyMs, 60_000L));
    }

    public long totalReceived() { return totalReceived.get(); }
    public long p50Ms() { return histogram.getValueAtPercentile(50); }
    public long p95Ms() { return histogram.getValueAtPercentile(95); }
    public long p99Ms() { return histogram.getValueAtPercentile(99); }
    public double throughputPerSec() {
        long elapsed = Math.max(1, System.currentTimeMillis() - startTimeMs);
        return totalReceived.get() * 1000.0 / elapsed;
    }
}
```

- [ ] **Step 3: Create PerfMessageListener.java**

`src/main/java/com/mayfair/rocketmq/perf/consumer/PerfMessageListener.java`:
```java
package com.mayfair.rocketmq.perf.consumer;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Component
public class PerfMessageListener {

    @Value("${rocketmq.topic}")
    private String topic;

    @Value("${rocketmq.consumer-group}")
    private String consumerGroup;

    @Autowired
    private ConsumeMetrics metrics;

    private SimpleConsumer consumer;
    private Thread pollerThread;
    private volatile boolean running = true;

    @PostConstruct
    public void start() throws Exception {
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfiguration config = ClientConfiguration.newBuilder()
                .setEndpoints(System.getenv("ROCKETMQ_PROXY_ENDPOINT"))
                .build();
        consumer = provider.newSimpleConsumerBuilder()
                .setClientConfiguration(config)
                .setConsumerGroup(consumerGroup)
                .setAwaitDuration(Duration.ofSeconds(3))
                .setSubscriptionExpressions(
                        Collections.singletonMap(topic, FilterExpression.SUB_ALL))
                .build();

        pollerThread = new Thread(() -> {
            while (running && !Thread.currentThread().isInterrupted()) {
                try {
                    List<MessageView> msgs = consumer.receive(32, Duration.ofSeconds(3));
                    for (MessageView msg : msgs) {
                        Map<String, String> props = msg.getProperties();
                        String sendTime = props.get("x-send-time");
                        if (sendTime != null) {
                            metrics.record(System.currentTimeMillis() - Long.parseLong(sendTime));
                        }
                        consumer.ack(msg);
                    }
                } catch (Exception e) {
                    if (Thread.currentThread().isInterrupted()) break;
                }
            }
        }, "perf-consumer-poller");
        pollerThread.setDaemon(true);
        pollerThread.start();
    }

    @PreDestroy
    public void stop() throws Exception {
        running = false;
        pollerThread.interrupt();
        consumer.close();
    }
}
```

- [ ] **Step 4: Create MetricsController.java (consumer)**

`src/main/java/com/mayfair/rocketmq/perf/consumer/MetricsController.java`:
```java
package com.mayfair.rocketmq.perf.consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
public class MetricsController {

    @Autowired
    private ConsumeMetrics metrics;

    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("role", "consumer");
        m.put("totalReceived", metrics.totalReceived());
        m.put("p50LatencyMs", metrics.p50Ms());
        m.put("p95LatencyMs", metrics.p95Ms());
        m.put("p99LatencyMs", metrics.p99Ms());
        m.put("throughputPerSec", Math.round(metrics.throughputPerSec()));
        return m;
    }
}
```

- [ ] **Step 5: Create PerfConsumerApplication.java**

`src/main/java/com/mayfair/rocketmq/perf/consumer/PerfConsumerApplication.java`:
```java
package com.mayfair.rocketmq.perf.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class PerfConsumerApplication {
    public static void main(String[] args) {
        SpringApplication.run(PerfConsumerApplication.class, args);
    }
}
```

- [ ] **Step 6: Create application.yml**

`src/main/resources/application.yml`:
```yaml
server:
  port: 8080

rocketmq:
  topic: perf-test-topic
  consumer-group: cid-perf-consumer
```

- [ ] **Step 7: Build**

```bash
cd /Users/lossend/pro/rocketmq-k8s-test
mvn package -pl rocketmq-perf-test/perf-consumer-app -DskipTests -q
```

Expected: `BUILD SUCCESS`

- [ ] **Step 8: Commit**

```bash
git add rocketmq-perf-test/perf-consumer-app/
git commit -m "feat(perf): add perf-consumer-app with HDR histogram latency tracking"
```

---

## Task 11: Starlink deployment for perf apps

**Files:**
- Create: `rocketmq-perf-test/perf-producer-app/starlink.yaml`
- Create: `rocketmq-perf-test/perf-consumer-app/starlink.yaml`

The perf apps are deployed via **Starlink**, the company's internal CD platform, not raw `kubectl`. Each app ships its own `starlink.yaml` deployment descriptor alongside its Maven module.

- [ ] **Step 1: Create Dockerfile for each perf app**

Both apps need a fat-jar Dockerfile. Create `rocketmq-perf-test/perf-producer-app/Dockerfile`:
```dockerfile
FROM public.ecr.aws/docker/library/eclipse-temurin:8-jre
COPY target/perf-producer-app-1.0.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "/app.jar"]
```

Create `rocketmq-perf-test/perf-consumer-app/Dockerfile` (same pattern, different jar name):
```dockerfile
FROM public.ecr.aws/docker/library/eclipse-temurin:8-jre
COPY target/perf-consumer-app-1.0.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "/app.jar"]
```

- [ ] **Step 2: Create starlink.yaml for perf-producer-app**

`rocketmq-perf-test/perf-producer-app/starlink.yaml`:
```yaml
# Starlink deployment descriptor for perf-producer-app.
# Adjust replicas, resources, and env per Starlink team standards.
app: perf-producer-app
version: 1.0.0-SNAPSHOT

envs:
  test:
    replicas: 1
    resources:
      cpu: "500m"
      memory: "512Mi"
    env:
      ROCKETMQ_PROXY_ENDPOINT: "proxy-nodeport.rocketmq-test.svc.cluster.local:8081"
    ports:
      - containerPort: 8080
        nodePort: 30080   # expose for hey HTTP load generator
```

> **Note:** Verify the exact Starlink YAML schema with your team. The structure above is a typical pattern — field names (`replicas`, `envs`, `ports.nodePort`) may differ from actual Starlink DSL.

- [ ] **Step 3: Create starlink.yaml for perf-consumer-app**

`rocketmq-perf-test/perf-consumer-app/starlink.yaml`:
```yaml
app: perf-consumer-app
version: 1.0.0-SNAPSHOT

envs:
  test:
    replicas: 1
    resources:
      cpu: "500m"
      memory: "512Mi"
    env:
      ROCKETMQ_PROXY_ENDPOINT: "proxy-nodeport.rocketmq-test.svc.cluster.local:8081"
    ports:
      - containerPort: 8080
```

- [ ] **Step 4: Build fat jars and deploy via Starlink**

```bash
# Build both apps
cd /Users/lossend/pro/rocketmq-k8s-test
mvn package -pl rocketmq-perf-test/perf-producer-app,rocketmq-perf-test/perf-consumer-app -DskipTests -q

# Deploy producer to Starlink test environment
cd rocketmq-perf-test/perf-producer-app
starlink deploy --env test

# Deploy consumer to Starlink test environment
cd ../perf-consumer-app
starlink deploy --env test
```

> **Note:** Replace `starlink deploy --env test` with the actual Starlink CLI command and flags used in your org (e.g. `starlink apply -f starlink.yaml -e test`). Confirm with the Starlink team.

- [ ] **Step 5: Verify pods healthy in Starlink test environment**

After deploying, confirm both apps reach `Running` state in the Starlink console or via CLI:
```bash
starlink status perf-producer-app --env test
starlink status perf-consumer-app --env test
```
Expected: both report `Running`, no error logs in the startup window.

- [ ] **Step 6: Run hey and collect results**

```bash
# Get producer endpoint from Starlink (NodePort or Starlink-assigned URL)
PRODUCER_URL=$(starlink url perf-producer-app --env test)

# Baseline run (no agent)
hey -n 100000 -c 50 ${PRODUCER_URL}/send

# Collect metrics
curl ${PRODUCER_URL}/metrics
curl $(starlink url perf-consumer-app --env test)/metrics
```

- [ ] **Step 7: Isolated run (with sandbox agent)**

Update `starlink.yaml` for producer app to add agent env vars for the isolated run:
```yaml
# Add to envs.test.env:
JAVA_TOOL_OPTIONS: >-
  -javaagent:/sandbox/lib/sandbox-agent.jar
  -Dservice.tag=gray1
  -Dsergo.service.route.plugin.enabled=true
  -Dsergo.service.route.plugin.rocketmq.enabled=true
```

Then redeploy and re-run `hey`:
```bash
starlink deploy --env test
hey -n 100000 -c 50 ${PRODUCER_URL}/send
curl ${PRODUCER_URL}/metrics
curl $(starlink url perf-consumer-app --env test)/metrics
```

Compare p50/p95/p99 latency and throughput between baseline and isolated runs.

- [ ] **Step 8: Commit**

```bash
cd /Users/lossend/pro/rocketmq-k8s-test
git add rocketmq-perf-test/perf-producer-app/Dockerfile \
        rocketmq-perf-test/perf-producer-app/starlink.yaml \
        rocketmq-perf-test/perf-consumer-app/Dockerfile \
        rocketmq-perf-test/perf-consumer-app/starlink.yaml
git commit -m "feat(perf): add Dockerfiles and Starlink deployment descriptors for perf apps"
```
