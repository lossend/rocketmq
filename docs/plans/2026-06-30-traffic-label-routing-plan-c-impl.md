# Traffic Label Routing (Plan-C, Proxy-Only) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route POP consumption by traffic label entirely in the Proxy — a consumer that declares a label `gray1` transparently consumes from real subscription group `G%gray1` with a SQL92 label filter, while standard consumers consume only `STANDARD`/null — with zero broker code change.

**Architecture:** Three stateless/low-state Proxy components. `LabelRoutingResolver` translates `(group, label, originExp)` → `(effectiveGroup, mergedSql92)`. A rewrite is applied at all four consumer-side gRPC activity entries (receive / ack / changeInvisible / DLQ-forward) so the receipt handle (which carries no group) always resolves back to the same `G%label`. `LabelGroupBootstrapper` lazily creates the real group via a new `AdminService` method. An optional `LabelGroupCleaner` periodically deletes idle `G%label` groups + their retry topics. Everything is gated by a master switch defaulting OFF.

**Tech Stack:** Java, RocketMQ Proxy (gRPC v2 module `proxy/src/main/java/org/apache/rocketmq/proxy/...`), JUnit 4 + Mockito (existing test conventions), broker `enablePropertyFilter` for SQL92.

---

## Design source

This plan implements [[2026-06-30-traffic-label-routing-plan-c-proxy-only]] (`docs/plans/2026-06-30-traffic-label-routing-plan-c-proxy-only.md`). Read §2.1 (config), §4 (components), §5 (SQL92 merge), §6 (path table) before starting.

## Key codebase facts (verified against source)

- `ProxyConfig` — config holder, plain fields + getter/setter pairs. Anchor: `proxy/src/main/java/org/apache/rocketmq/proxy/config/ProxyConfig.java:41`.
- `ReceiptHandle.encode()` does **not** encode the group name; ack/changeInvisible re-read the group from the client request each time. Anchor: `common/src/main/java/org/apache/rocketmq/common/consumer/ReceiptHandle.java:43-47`. → The group rewrite must be applied identically at every consumer-side entry, or ack lands on the wrong group.
- Consumer-side gRPC activity entries that read `request.getGroup().getName()`:
  - `ReceiveMessageActivity.java:105`
  - `AckMessageActivity.java:56`
  - `ChangeInvisibleDurationActivity.java:51`
  - `ForwardMessageToDeadLetterQueueActivity` (same pattern)
- `AdminService` interface currently exposes only topic creation. Anchor: `proxy/src/main/java/org/apache/rocketmq/proxy/service/admin/AdminService.java:23`, impl `DefaultAdminService.java:37` (holds a `MQClientAPIFactory`).
- The create-subscription-group RPC already exists end to end: `client/src/main/java/org/apache/rocketmq/client/impl/MQClientAPIImpl.java:431` (`createSubscriptionGroup`) → broker `UPDATE_AND_CREATE_SUBSCRIPTIONGROUP` handler. `MQClientAPIExt` (the proxy client type returned by `MQClientAPIFactory.getClient()`) extends `MQClientAPIImpl`, so this method is reachable.
- Proxy has no SPI; components must be wired explicitly. Cluster wiring entry: `ServiceManagerFactory.createForClusterMode` → `ClusterServiceManager`. Startup: `ProxyStartup` → `DefaultMessagingProcessor.createForClusterMode()`.
- The label transport (spec D2 "consumer declares label") is pinned in this plan as a gRPC metadata header `__RMQ_TRAFFIC_LABEL` surfaced on `ProxyContext`. No consumer SDK change is required — the consumer sets a client-level metadata header.

## File Structure

| File | Responsibility | New/Modify |
|---|---|---|
| `proxy/.../config/ProxyConfig.java` | 3 toggles + 1 threshold | Modify |
| `proxy/.../grpc/v2/consumer/TrafficLabel.java` | Constants (property key, STANDARD, separator), `effectiveGroup()` helper | New |
| `proxy/.../grpc/v2/consumer/LabelRoutingResolver.java` | `(group,label,originExp)` → `RoutingDecision(effectiveGroup, sql92)` | New |
| `proxy/.../grpc/v2/consumer/TrafficLabelExtractor.java` | Read declared label from `ProxyContext` | New |
| `proxy/.../service/admin/AdminService.java` + `DefaultAdminService.java` | `createSubscriptionGroup` + `deleteSubscriptionGroup` | Modify |
| `proxy/.../grpc/v2/consumer/LabelGroupBootstrapper.java` | Lazy-create `G%label` + dedup cache | New |
| `proxy/.../grpc/v2/consumer/ReceiveMessageActivity.java` | Apply rewrite | Modify |
| `proxy/.../grpc/v2/consumer/AckMessageActivity.java` | Apply rewrite | Modify |
| `proxy/.../grpc/v2/consumer/ChangeInvisibleDurationActivity.java` | Apply rewrite | Modify |
| `proxy/.../grpc/v2/consumer/ForwardMessageToDeadLetterQueueActivity.java` | Apply rewrite | Modify |
| `proxy/.../grpc/v2/consumer/LabelGroupCleaner.java` | Optional periodic cleanup | New |
| `test/.../grpc/v2/TrafficLabelRoutingIT.java` | E2E with real broker+proxy | New |

---

## Task 1: Add ProxyConfig toggles

**Files:**
- Modify: `proxy/src/main/java/org/apache/rocketmq/proxy/config/ProxyConfig.java`
- Test: `proxy/src/test/java/org/apache/rocketmq/proxy/config/ProxyConfigTrafficLabelTest.java`

- [ ] **Step 1: Write the failing test**

```java
package org.apache.rocketmq.proxy.config;

import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;

public class ProxyConfigTrafficLabelTest {

    @Test
    public void should_default_all_traffic_label_toggles_off_for_production_safety() {
        ProxyConfig config = new ProxyConfig();
        assertThat(config.isEnableTrafficLabelRouting()).isFalse();
        assertThat(config.isEnableTrafficLabelGroupCleanup()).isFalse();
        assertThat(config.isEnableTrafficLabelRoutingLog()).isFalse();
    }

    @Test
    public void should_default_cleanup_idle_threshold_to_one_hour() {
        ProxyConfig config = new ProxyConfig();
        assertThat(config.getTrafficLabelGroupCleanupIdleThresholdMs()).isEqualTo(3600_000L);
    }

    @Test
    public void should_allow_enabling_routing() {
        ProxyConfig config = new ProxyConfig();
        config.setEnableTrafficLabelRouting(true);
        assertThat(config.isEnableTrafficLabelRouting()).isTrue();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl proxy -am test -Dtest=ProxyConfigTrafficLabelTest`
Expected: FAIL — `isEnableTrafficLabelRouting()` not defined (compilation error).

- [ ] **Step 3: Add fields + accessors to ProxyConfig**

Add these fields near the other boolean toggles in `ProxyConfig` (after an existing `private boolean ...;` field, e.g. close to `enableBatchAck`):

```java
    /**
     * Master switch for traffic-label consumption routing. Default OFF so the
     * feature is fully bypassed in production unless explicitly enabled.
     */
    private boolean enableTrafficLabelRouting = false;
    /**
     * Periodically delete idle G%label subscription groups and their retry topics.
     * Only effective when enableTrafficLabelRouting is also true. Default OFF.
     */
    private boolean enableTrafficLabelGroupCleanup = false;
    /**
     * Emit traffic-label routing logs (rewrite decisions, lazy group creation,
     * cleanup actions). Default OFF to avoid production log noise.
     */
    private boolean enableTrafficLabelRoutingLog = false;
    /**
     * A G%label group must be offline at least this long before cleanup deletes it.
     * Guards against transient restarts. Default 1 hour.
     */
    private long trafficLabelGroupCleanupIdleThresholdMs = 3600_000L;
```

Add accessors alongside the other getters/setters:

```java
    public boolean isEnableTrafficLabelRouting() {
        return enableTrafficLabelRouting;
    }

    public void setEnableTrafficLabelRouting(boolean enableTrafficLabelRouting) {
        this.enableTrafficLabelRouting = enableTrafficLabelRouting;
    }

    public boolean isEnableTrafficLabelGroupCleanup() {
        return enableTrafficLabelGroupCleanup;
    }

    public void setEnableTrafficLabelGroupCleanup(boolean enableTrafficLabelGroupCleanup) {
        this.enableTrafficLabelGroupCleanup = enableTrafficLabelGroupCleanup;
    }

    public boolean isEnableTrafficLabelRoutingLog() {
        return enableTrafficLabelRoutingLog;
    }

    public void setEnableTrafficLabelRoutingLog(boolean enableTrafficLabelRoutingLog) {
        this.enableTrafficLabelRoutingLog = enableTrafficLabelRoutingLog;
    }

    public long getTrafficLabelGroupCleanupIdleThresholdMs() {
        return trafficLabelGroupCleanupIdleThresholdMs;
    }

    public void setTrafficLabelGroupCleanupIdleThresholdMs(long trafficLabelGroupCleanupIdleThresholdMs) {
        this.trafficLabelGroupCleanupIdleThresholdMs = trafficLabelGroupCleanupIdleThresholdMs;
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl proxy -am test -Dtest=ProxyConfigTrafficLabelTest`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add proxy/src/main/java/org/apache/rocketmq/proxy/config/ProxyConfig.java \
        proxy/src/test/java/org/apache/rocketmq/proxy/config/ProxyConfigTrafficLabelTest.java
git commit -m "feat(proxy): add traffic-label routing config toggles (default off)"
```

---

## Task 2: TrafficLabel constants + effective-group helper

**Files:**
- Create: `proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/consumer/TrafficLabel.java`
- Test: `proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/consumer/TrafficLabelTest.java`

- [ ] **Step 1: Write the failing test**

```java
package org.apache.rocketmq.proxy.grpc.v2.consumer;

import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;

public class TrafficLabelTest {

    @Test
    public void should_build_virtual_group_for_gray_label() {
        assertThat(TrafficLabel.effectiveGroup("G", "gray1")).isEqualTo("G%gray1");
    }

    @Test
    public void should_return_origin_group_when_label_blank() {
        assertThat(TrafficLabel.effectiveGroup("G", null)).isEqualTo("G");
        assertThat(TrafficLabel.effectiveGroup("G", "")).isEqualTo("G");
        assertThat(TrafficLabel.effectiveGroup("G", "   ")).isEqualTo("G");
    }

    @Test
    public void should_treat_standard_label_as_origin_group() {
        assertThat(TrafficLabel.effectiveGroup("G", "STANDARD")).isEqualTo("G");
    }

    @Test
    public void should_detect_gray_label() {
        assertThat(TrafficLabel.isGray("gray1")).isTrue();
        assertThat(TrafficLabel.isGray("STANDARD")).isFalse();
        assertThat(TrafficLabel.isGray(null)).isFalse();
        assertThat(TrafficLabel.isGray("")).isFalse();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl proxy -am test -Dtest=TrafficLabelTest`
Expected: FAIL — `TrafficLabel` not defined.

- [ ] **Step 3: Create TrafficLabel**

```java
package org.apache.rocketmq.proxy.grpc.v2.consumer;

import org.apache.commons.lang3.StringUtils;

/**
 * Shared constants and helpers for traffic-label routing.
 */
public final class TrafficLabel {

    /** Message property carrying the isolation label, also the gRPC metadata key. */
    public static final String PROPERTY_KEY = "__RMQ_TRAFFIC_LABEL";

    /** Label value (or absence) meaning "standard environment". */
    public static final String STANDARD = "STANDARD";

    /** Separator between origin group and label in the virtual real group name. */
    public static final String GROUP_SEPARATOR = "%";

    private TrafficLabel() {
    }

    /**
     * A label denotes a gray (isolated) environment iff it is non-blank and not STANDARD.
     */
    public static boolean isGray(String label) {
        return StringUtils.isNotBlank(label) && !STANDARD.equals(label);
    }

    /**
     * Map (origin group, label) to the effective subscription group.
     * Gray labels map to {@code G%label}; blank or STANDARD map to the origin group.
     */
    public static String effectiveGroup(String originGroup, String label) {
        if (!isGray(label)) {
            return originGroup;
        }
        return originGroup + GROUP_SEPARATOR + label;
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl proxy -am test -Dtest=TrafficLabelTest`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/consumer/TrafficLabel.java \
        proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/consumer/TrafficLabelTest.java
git commit -m "feat(proxy): add TrafficLabel constants and effective-group helper"
```

---

## Task 3: LabelRoutingResolver (group + SQL92 merge)

**Files:**
- Create: `proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/consumer/LabelRoutingResolver.java`
- Test: `proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/consumer/LabelRoutingResolverTest.java`

- [ ] **Step 1: Write the failing test**

```java
package org.apache.rocketmq.proxy.grpc.v2.consumer;

import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;

public class LabelRoutingResolverTest {

    private final LabelRoutingResolver resolver = new LabelRoutingResolver();

    @Test
    public void gray_label_routes_to_virtual_group_with_label_filter() {
        LabelRoutingResolver.RoutingDecision d = resolver.resolve("G", "gray1", null);
        assertThat(d.getEffectiveGroup()).isEqualTo("G%gray1");
        assertThat(d.getSql92()).isEqualTo("__RMQ_TRAFFIC_LABEL = 'gray1'");
    }

    @Test
    public void standard_consumer_gets_complement_filter_on_origin_group() {
        LabelRoutingResolver.RoutingDecision d = resolver.resolve("G", null, null);
        assertThat(d.getEffectiveGroup()).isEqualTo("G");
        assertThat(d.getSql92())
            .isEqualTo("__RMQ_TRAFFIC_LABEL IS NULL OR __RMQ_TRAFFIC_LABEL = 'STANDARD'");
    }

    @Test
    public void gray_label_AND_merges_consumer_origin_expression() {
        LabelRoutingResolver.RoutingDecision d = resolver.resolve("G", "gray1", "a > 1");
        assertThat(d.getEffectiveGroup()).isEqualTo("G%gray1");
        assertThat(d.getSql92()).isEqualTo("( a > 1 ) AND ( __RMQ_TRAFFIC_LABEL = 'gray1' )");
    }

    @Test
    public void standard_AND_merges_consumer_origin_expression() {
        LabelRoutingResolver.RoutingDecision d = resolver.resolve("G", "STANDARD", "a > 1");
        assertThat(d.getEffectiveGroup()).isEqualTo("G");
        assertThat(d.getSql92()).isEqualTo(
            "( a > 1 ) AND ( __RMQ_TRAFFIC_LABEL IS NULL OR __RMQ_TRAFFIC_LABEL = 'STANDARD' )");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl proxy -am test -Dtest=LabelRoutingResolverTest`
Expected: FAIL — `LabelRoutingResolver` not defined.

- [ ] **Step 3: Create LabelRoutingResolver**

```java
package org.apache.rocketmq.proxy.grpc.v2.consumer;

import org.apache.commons.lang3.StringUtils;

/**
 * Translates (origin group, declared label, consumer's own SQL92 expression) into the
 * effective subscription group and the merged SQL92 expression. Stateless.
 *
 * <p>The label condition is AND-merged with the consumer's own expression — never
 * overwritten — so a consumer that already filters on its own properties keeps working.
 */
public class LabelRoutingResolver {

    public RoutingDecision resolve(String originGroup, String label, String originExpression) {
        String effectiveGroup = TrafficLabel.effectiveGroup(originGroup, label);
        String labelCondition = labelCondition(label);
        String merged = merge(originExpression, labelCondition);
        return new RoutingDecision(effectiveGroup, merged);
    }

    private String labelCondition(String label) {
        if (TrafficLabel.isGray(label)) {
            return TrafficLabel.PROPERTY_KEY + " = '" + label + "'";
        }
        return TrafficLabel.PROPERTY_KEY + " IS NULL OR " + TrafficLabel.PROPERTY_KEY
            + " = '" + TrafficLabel.STANDARD + "'";
    }

    private String merge(String originExpression, String labelCondition) {
        if (StringUtils.isBlank(originExpression)) {
            return labelCondition;
        }
        return "( " + originExpression.trim() + " ) AND ( " + labelCondition + " )";
    }

    public static class RoutingDecision {
        private final String effectiveGroup;
        private final String sql92;

        public RoutingDecision(String effectiveGroup, String sql92) {
            this.effectiveGroup = effectiveGroup;
            this.sql92 = sql92;
        }

        public String getEffectiveGroup() {
            return effectiveGroup;
        }

        public String getSql92() {
            return sql92;
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl proxy -am test -Dtest=LabelRoutingResolverTest`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/consumer/LabelRoutingResolver.java \
        proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/consumer/LabelRoutingResolverTest.java
git commit -m "feat(proxy): add LabelRoutingResolver with AND-merged SQL92"
```

---

## Task 4: TrafficLabelExtractor (read declared label from ProxyContext)

**Files:**
- Create: `proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/consumer/TrafficLabelExtractor.java`
- Test: `proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/consumer/TrafficLabelExtractorTest.java`

> The consumer declares its label as a gRPC metadata header `__RMQ_TRAFFIC_LABEL`, which the proxy's context interceptor places into `ProxyContext`. This task reads it back. `ProxyContext` exposes a generic value map via `withVal`/`getVal` (see `ProxyContext` usage `setLocalAddress → withVal`). We store/read under the same key.

- [ ] **Step 1: Write the failing test**

```java
package org.apache.rocketmq.proxy.grpc.v2.consumer;

import org.apache.rocketmq.proxy.common.ProxyContext;
import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;

public class TrafficLabelExtractorTest {

    @Test
    public void returns_label_when_present_in_context() {
        ProxyContext ctx = ProxyContext.create();
        ctx.withVal(TrafficLabel.PROPERTY_KEY, "gray1");
        assertThat(TrafficLabelExtractor.extract(ctx)).isEqualTo("gray1");
    }

    @Test
    public void returns_null_when_absent() {
        ProxyContext ctx = ProxyContext.create();
        assertThat(TrafficLabelExtractor.extract(ctx)).isNull();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl proxy -am test -Dtest=TrafficLabelExtractorTest`
Expected: FAIL — `TrafficLabelExtractor` not defined.

- [ ] **Step 3: Create TrafficLabelExtractor**

```java
package org.apache.rocketmq.proxy.grpc.v2.consumer;

import org.apache.rocketmq.proxy.common.ProxyContext;

/**
 * Reads the consumer-declared traffic label from the request context.
 * The label arrives as a gRPC metadata header and is stored on ProxyContext
 * under {@link TrafficLabel#PROPERTY_KEY}.
 */
public final class TrafficLabelExtractor {

    private TrafficLabelExtractor() {
    }

    public static String extract(ProxyContext ctx) {
        if (ctx == null) {
            return null;
        }
        Object val = ctx.getVal(TrafficLabel.PROPERTY_KEY);
        return val == null ? null : val.toString();
    }
}
```

> If `ProxyContext` does not expose `getVal(String)`/`withVal(String, Object)` with these exact signatures, check `proxy/src/main/java/org/apache/rocketmq/proxy/common/ProxyContext.java` and adapt to the actual accessor (it backs a `Map<String,Object>`). Do not change `ProxyContext`'s API.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl proxy -am test -Dtest=TrafficLabelExtractorTest`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/consumer/TrafficLabelExtractor.java \
        proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/consumer/TrafficLabelExtractorTest.java
git commit -m "feat(proxy): add TrafficLabelExtractor reading label from ProxyContext"
```

---

## Task 5: AdminService.createSubscriptionGroup / deleteSubscriptionGroup

**Files:**
- Modify: `proxy/src/main/java/org/apache/rocketmq/proxy/service/admin/AdminService.java`
- Modify: `proxy/src/main/java/org/apache/rocketmq/proxy/service/admin/DefaultAdminService.java`
- Test: `proxy/src/test/java/org/apache/rocketmq/proxy/service/admin/DefaultAdminServiceTrafficLabelTest.java`

> `MQClientAPIFactory.getClient()` returns `MQClientAPIExt extends MQClientAPIImpl`, so `createSubscriptionGroup(addr, config, timeout)` (`MQClientAPIImpl.java:431`) and `deleteSubscriptionGroup(addr, groupName, removeOffset, timeout)` are available. The broker master address is resolved via `getTopicRouteDataDirectlyFromNameServer(sampleTopic).getBrokerDatas()`, matching the existing `createTopicOnBroker` pattern.

- [ ] **Step 1: Write the failing test**

```java
package org.apache.rocketmq.proxy.service.admin;

import java.util.Collections;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIExt;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultAdminServiceTrafficLabelTest {

    private MQClientAPIExt client;
    private DefaultAdminService adminService;

    @Before
    public void setUp() throws Exception {
        client = mock(MQClientAPIExt.class);
        MQClientAPIFactory factory = mock(MQClientAPIFactory.class);
        when(factory.getClient()).thenReturn(client);

        TopicRouteData route = new TopicRouteData();
        BrokerData broker = new BrokerData();
        broker.setBrokerName("broker-a");
        HashMap<Long, String> addrs = new HashMap<>();
        addrs.put(0L, "127.0.0.1:10911");
        broker.setBrokerAddrs(addrs);
        route.setBrokerDatas(Collections.singletonList(broker));
        when(client.getTopicRouteInfoFromNameServer(eq("sample-topic"), anyLong())).thenReturn(route);

        adminService = new DefaultAdminService(factory);
    }

    @Test
    public void creates_subscription_group_on_each_broker() throws Exception {
        SubscriptionGroupConfig config = new SubscriptionGroupConfig();
        config.setGroupName("G%gray1");

        boolean ok = adminService.createSubscriptionGroup("sample-topic", config);

        assertThat(ok).isTrue();
        ArgumentCaptor<SubscriptionGroupConfig> captor =
            ArgumentCaptor.forClass(SubscriptionGroupConfig.class);
        verify(client, times(1))
            .createSubscriptionGroup(eq("127.0.0.1:10911"), captor.capture(), anyLong());
        assertThat(captor.getValue().getGroupName()).isEqualTo("G%gray1");
    }

    @Test
    public void deletes_subscription_group_on_each_broker() throws Exception {
        boolean ok = adminService.deleteSubscriptionGroup("sample-topic", "G%gray1");

        assertThat(ok).isTrue();
        verify(client, times(1))
            .deleteSubscriptionGroup(eq("127.0.0.1:10911"), eq("G%gray1"), eq(true), anyLong());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl proxy -am test -Dtest=DefaultAdminServiceTrafficLabelTest`
Expected: FAIL — `createSubscriptionGroup` not defined on `AdminService`.

- [ ] **Step 3: Add methods to AdminService interface**

Append to `AdminService` interface body:

```java
    /**
     * Create (or update) a subscription group on every broker that hosts sampleTopic.
     * @return true if the create RPC was issued without error to at least one broker.
     */
    boolean createSubscriptionGroup(String sampleTopic,
        org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig config);

    /**
     * Delete a subscription group (and reset its offset) on every broker that hosts sampleTopic.
     */
    boolean deleteSubscriptionGroup(String sampleTopic, String groupName);
```

- [ ] **Step 4: Implement in DefaultAdminService**

Add to `DefaultAdminService` (it already imports `BrokerData`, `MixAll`, `Duration`, `List`):

```java
    @Override
    public boolean createSubscriptionGroup(String sampleTopic,
        org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig config) {
        return forEachBrokerMaster(sampleTopic, addr -> {
            this.getClient().createSubscriptionGroup(addr, config, Duration.ofSeconds(3).toMillis());
            return null;
        });
    }

    @Override
    public boolean deleteSubscriptionGroup(String sampleTopic, String groupName) {
        return forEachBrokerMaster(sampleTopic, addr -> {
            this.getClient().deleteSubscriptionGroup(addr, groupName, true, Duration.ofSeconds(3).toMillis());
            return null;
        });
    }

    private interface BrokerAction {
        void run(String brokerAddr) throws Exception;
    }

    private boolean forEachBrokerMaster(String sampleTopic, java.util.function.Function<String, Void> action) {
        TopicRouteData route;
        try {
            route = this.getTopicRouteDataDirectlyFromNameServer(sampleTopic);
        } catch (Exception e) {
            log.error("traffic-label admin: get route for {} failed.", sampleTopic, e);
            return false;
        }
        if (route == null || route.getBrokerDatas().isEmpty()) {
            return false;
        }
        boolean any = false;
        for (BrokerData brokerData : route.getBrokerDatas()) {
            String addr = brokerData.getBrokerAddrs() == null ? null
                : brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
            if (addr == null) {
                continue;
            }
            try {
                action.apply(addr);
                any = true;
            } catch (Exception e) {
                log.error("traffic-label admin: action on broker {} failed.", addr, e);
            }
        }
        return any;
    }
```

> Remove the unused `BrokerAction` interface if you keep the `Function`-based form (it is included only to document intent). Keep one form. The `Function<String,Void>` lambdas above return `null`.

- [ ] **Step 5: Run test to verify it passes**

Run: `mvn -pl proxy -am test -Dtest=DefaultAdminServiceTrafficLabelTest`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
git add proxy/src/main/java/org/apache/rocketmq/proxy/service/admin/AdminService.java \
        proxy/src/main/java/org/apache/rocketmq/proxy/service/admin/DefaultAdminService.java \
        proxy/src/test/java/org/apache/rocketmq/proxy/service/admin/DefaultAdminServiceTrafficLabelTest.java
git commit -m "feat(proxy): AdminService create/delete subscription group for traffic-label groups"
```

---

## Task 6: LabelGroupBootstrapper (lazy create + dedup)

**Files:**
- Create: `proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/consumer/LabelGroupBootstrapper.java`
- Test: `proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/consumer/LabelGroupBootstrapperTest.java`

- [ ] **Step 1: Write the failing test**

```java
package org.apache.rocketmq.proxy.grpc.v2.consumer;

import org.apache.rocketmq.proxy.service.admin.AdminService;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.assertj.core.api.Assertions.assertThat;

public class LabelGroupBootstrapperTest {

    @Test
    public void creates_group_once_then_caches() {
        AdminService admin = mock(AdminService.class);
        when(admin.createSubscriptionGroup(any(), any())).thenReturn(true);
        LabelGroupBootstrapper boot = new LabelGroupBootstrapper(admin);

        boot.ensureGroup("test-topic", "G%gray1");
        boot.ensureGroup("test-topic", "G%gray1");

        verify(admin, times(1)).createSubscriptionGroup(eq("test-topic"), any());
    }

    @Test
    public void created_group_name_matches_requested() {
        AdminService admin = mock(AdminService.class);
        when(admin.createSubscriptionGroup(any(), any())).thenReturn(true);
        LabelGroupBootstrapper boot = new LabelGroupBootstrapper(admin);

        boot.ensureGroup("test-topic", "G%gray1");

        ArgumentCaptor<SubscriptionGroupConfig> captor =
            ArgumentCaptor.forClass(SubscriptionGroupConfig.class);
        verify(admin).createSubscriptionGroup(eq("test-topic"), captor.capture());
        assertThat(captor.getValue().getGroupName()).isEqualTo("G%gray1");
    }

    @Test
    public void failed_creation_is_not_cached_and_retries() {
        AdminService admin = mock(AdminService.class);
        when(admin.createSubscriptionGroup(any(), any())).thenReturn(false, true);
        LabelGroupBootstrapper boot = new LabelGroupBootstrapper(admin);

        boot.ensureGroup("test-topic", "G%gray1");
        boot.ensureGroup("test-topic", "G%gray1");

        verify(admin, times(2)).createSubscriptionGroup(eq("test-topic"), any());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl proxy -am test -Dtest=LabelGroupBootstrapperTest`
Expected: FAIL — `LabelGroupBootstrapper` not defined.

- [ ] **Step 3: Create LabelGroupBootstrapper**

```java
package org.apache.rocketmq.proxy.grpc.v2.consumer;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.proxy.service.admin.AdminService;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

/**
 * Lazily creates the real subscription group G%label the first time a gray consumer
 * is routed to it. Successful creations are cached so repeated POPs do not re-issue
 * the admin RPC. The cache is in-memory and may be lost on restart — losing it only
 * causes one redundant (idempotent) create RPC.
 */
public class LabelGroupBootstrapper {

    private final AdminService adminService;
    private final Set<String> createdGroups = ConcurrentHashMap.newKeySet();

    public LabelGroupBootstrapper(AdminService adminService) {
        this.adminService = adminService;
    }

    /**
     * Ensure G%label exists. The group config is created fresh (broker fills defaults);
     * G%label inherits effective behavior from broker-side defaults plus the standard
     * group's retry policy via the broker create handler.
     *
     * @param sampleTopic a topic hosted on the brokers where the group must exist
     * @param effectiveGroup the G%label group name
     */
    public void ensureGroup(String sampleTopic, String effectiveGroup) {
        if (createdGroups.contains(effectiveGroup)) {
            return;
        }
        SubscriptionGroupConfig config = new SubscriptionGroupConfig();
        config.setGroupName(effectiveGroup);
        boolean ok = adminService.createSubscriptionGroup(sampleTopic, config);
        if (ok) {
            createdGroups.add(effectiveGroup);
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl proxy -am test -Dtest=LabelGroupBootstrapperTest`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/consumer/LabelGroupBootstrapper.java \
        proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/consumer/LabelGroupBootstrapperTest.java
git commit -m "feat(proxy): add LabelGroupBootstrapper for lazy G%label creation"
```

---

## Task 7: Routing facade tying resolver + extractor + bootstrapper + gating

**Files:**
- Create: `proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/consumer/TrafficLabelRouter.java`
- Test: `proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/consumer/TrafficLabelRouterTest.java`

> This is the single chokepoint the four activities call, so the gating + rewrite logic lives in exactly one place (avoids the "four-places-inconsistent" risk in spec §8). It returns the rewritten group for ack/changeInvisible/DLQ (which need only the group) and a full decision for receive (which needs group + SQL92 + lazy create).

- [ ] **Step 1: Write the failing test**

```java
package org.apache.rocketmq.proxy.grpc.v2.consumer;

import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class TrafficLabelRouterTest {

    private LabelGroupBootstrapper bootstrapper;
    private TrafficLabelRouter router;

    @Before
    public void setUp() {
        bootstrapper = mock(LabelGroupBootstrapper.class);
        router = new TrafficLabelRouter(new LabelRoutingResolver(), bootstrapper);
        // ensure a real ProxyConfig is installed for ConfigurationManager
        ConfigurationManager.setProxyConfig(new ProxyConfig());
    }

    @Test
    public void disabled_master_switch_returns_origin_group_untouched() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(false);
        ProxyContext ctx = ProxyContext.create();
        ctx.withVal(TrafficLabel.PROPERTY_KEY, "gray1");

        assertThat(router.rewriteGroup(ctx, "test-topic", "G")).isEqualTo("G");
        verify(bootstrapper, never()).ensureGroup(org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any());
    }

    @Test
    public void enabled_gray_rewrites_group_and_creates_group() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);
        ProxyContext ctx = ProxyContext.create();
        ctx.withVal(TrafficLabel.PROPERTY_KEY, "gray1");

        assertThat(router.rewriteGroup(ctx, "test-topic", "G")).isEqualTo("G%gray1");
        verify(bootstrapper).ensureGroup("test-topic", "G%gray1");
    }

    @Test
    public void enabled_standard_keeps_origin_group_no_create() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);
        ProxyContext ctx = ProxyContext.create();

        assertThat(router.rewriteGroup(ctx, "test-topic", "G")).isEqualTo("G");
        verify(bootstrapper, never()).ensureGroup(org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any());
    }

    @Test
    public void resolve_for_receive_returns_decision_when_enabled() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);
        ProxyContext ctx = ProxyContext.create();
        ctx.withVal(TrafficLabel.PROPERTY_KEY, "gray1");

        LabelRoutingResolver.RoutingDecision d = router.resolveForReceive(ctx, "test-topic", "G", null);
        assertThat(d.getEffectiveGroup()).isEqualTo("G%gray1");
        assertThat(d.getSql92()).isEqualTo("__RMQ_TRAFFIC_LABEL = 'gray1'");
    }

    @Test
    public void resolve_for_receive_returns_null_when_disabled() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(false);
        ProxyContext ctx = ProxyContext.create();
        ctx.withVal(TrafficLabel.PROPERTY_KEY, "gray1");

        assertThat(router.resolveForReceive(ctx, "test-topic", "G", null)).isNull();
    }
}
```

> If `ConfigurationManager.setProxyConfig` does not exist, install config via the existing test pattern used by other proxy tests (e.g. `ConfigurationManager.initEnv()` + reflection, or a static setter present in `Configuration`). Check `proxy/src/test/java/org/apache/rocketmq/proxy/config/` for the established helper and mirror it.

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl proxy -am test -Dtest=TrafficLabelRouterTest`
Expected: FAIL — `TrafficLabelRouter` not defined.

- [ ] **Step 3: Create TrafficLabelRouter**

```java
package org.apache.rocketmq.proxy.grpc.v2.consumer;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;

/**
 * Single entry point for traffic-label routing, called by all four consumer-side
 * activities. Centralizes the master-switch gate and the rewrite so the four call
 * sites stay consistent (the receipt handle carries no group, so ack must rewrite
 * to the same group as receive).
 */
public class TrafficLabelRouter {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final LabelRoutingResolver resolver;
    private final LabelGroupBootstrapper bootstrapper;

    public TrafficLabelRouter(LabelRoutingResolver resolver, LabelGroupBootstrapper bootstrapper) {
        this.resolver = resolver;
        this.bootstrapper = bootstrapper;
    }

    private boolean enabled() {
        return ConfigurationManager.getProxyConfig().isEnableTrafficLabelRouting();
    }

    private boolean logEnabled() {
        return ConfigurationManager.getProxyConfig().isEnableTrafficLabelRoutingLog();
    }

    /**
     * Rewrite the group for ack / changeInvisible / DLQ. When disabled, or for a
     * standard/blank label, returns the origin group unchanged.
     */
    public String rewriteGroup(ProxyContext ctx, String topic, String originGroup) {
        if (!enabled()) {
            return originGroup;
        }
        String label = TrafficLabelExtractor.extract(ctx);
        if (!TrafficLabel.isGray(label)) {
            return originGroup;
        }
        String effectiveGroup = TrafficLabel.effectiveGroup(originGroup, label);
        bootstrapper.ensureGroup(topic, effectiveGroup);
        if (logEnabled()) {
            log.info("traffic-label rewrite group {} -> {} (label={})", originGroup, effectiveGroup, label);
        }
        return effectiveGroup;
    }

    /**
     * Resolve the full decision (group + SQL92) for receive. Returns null when disabled,
     * signalling the caller to keep its native behavior.
     */
    public LabelRoutingResolver.RoutingDecision resolveForReceive(ProxyContext ctx, String topic,
        String originGroup, String originExpression) {
        if (!enabled()) {
            return null;
        }
        String label = TrafficLabelExtractor.extract(ctx);
        LabelRoutingResolver.RoutingDecision decision = resolver.resolve(originGroup, label, originExpression);
        if (TrafficLabel.isGray(label)) {
            bootstrapper.ensureGroup(topic, decision.getEffectiveGroup());
        }
        if (logEnabled()) {
            log.info("traffic-label receive group {} -> {} sql92={}", originGroup,
                decision.getEffectiveGroup(), decision.getSql92());
        }
        return decision;
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl proxy -am test -Dtest=TrafficLabelRouterTest`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/consumer/TrafficLabelRouter.java \
        proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/consumer/TrafficLabelRouterTest.java
git commit -m "feat(proxy): add TrafficLabelRouter facade with master-switch gating"
```

---

## Task 8: Wire TrafficLabelRouter into the four consumer-side activities

**Files:**
- Modify: `proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/consumer/ReceiveMessageActivity.java`
- Modify: `proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/consumer/AckMessageActivity.java`
- Modify: `proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/consumer/ChangeInvisibleDurationActivity.java`
- Modify: `proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/consumer/ForwardMessageToDeadLetterQueueActivity.java`
- Modify: `proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/AbstractMessagingActivity.java` (hold the router)
- Modify: `proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/DefaultGrpcMessagingActivity.java` (construct + inject router)
- Test: extend `proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/consumer/ReceiveMessageActivityTest.java`

> The router is injected through `AbstractMessagingActivity` so all four subclasses share one instance. When the master switch is off, `rewriteGroup`/`resolveForReceive` return the origin group / null, so behavior is byte-for-byte identical to today.

- [ ] **Step 1: Write the failing test (receive rewrites group + filter when enabled)**

Add to `ReceiveMessageActivityTest`:

```java
@Test
public void receive_routes_gray_label_to_virtual_group() throws Throwable {
    ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);
    // ctx carries label
    ProxyContext ctx = createContext();
    ctx.withVal(TrafficLabel.PROPERTY_KEY, "gray1");

    ArgumentCaptor<String> groupCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<SubscriptionData> subCaptor = ArgumentCaptor.forClass(SubscriptionData.class);
    when(this.messagingProcessor.popMessage(any(), any(), groupCaptor.capture(), any(),
        anyInt(), anyLong(), anyLong(), anyInt(), subCaptor.capture(), anyBoolean(), any(), any(), anyLong()))
        .thenReturn(CompletableFuture.completedFuture(createPopResult()));

    this.receiveMessageActivity.receiveMessage(ctx, createReceiveRequest("G", "test-topic"),
        receiveStreamObserver);

    assertThat(groupCaptor.getValue()).isEqualTo("G%gray1");
    assertThat(subCaptor.getValue().getSubString()).contains("__RMQ_TRAFFIC_LABEL = 'gray1'");
}
```

> Mirror the existing helpers in `ReceiveMessageActivityTest` for `createContext()`, `createReceiveRequest(...)`, `createPopResult()`, and the mock field names. If their names differ, adapt to the file's actual conventions — do not invent new test infrastructure.

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl proxy -am test -Dtest=ReceiveMessageActivityTest#receive_routes_gray_label_to_virtual_group`
Expected: FAIL — group passed to `popMessage` is still `G`.

- [ ] **Step 3: Hold the router in AbstractMessagingActivity**

In `AbstractMessagingActivity`, add a protected field and initialize it (default instance so existing constructors keep working):

```java
    protected TrafficLabelRouter trafficLabelRouter;

    public void setTrafficLabelRouter(TrafficLabelRouter trafficLabelRouter) {
        this.trafficLabelRouter = trafficLabelRouter;
    }
```

> Use a setter (not a constructor param) to avoid touching every subclass constructor and its callers. `DefaultGrpcMessagingActivity` will call the setter after constructing each activity.

- [ ] **Step 4: Apply rewrite in ReceiveMessageActivity**

In `receiveMessage`, after `String group = request.getGroup().getName();` (line ~105) and after `subscriptionData` is built (line ~119-124), insert:

```java
            if (trafficLabelRouter != null) {
                LabelRoutingResolver.RoutingDecision decision =
                    trafficLabelRouter.resolveForReceive(ctx, topic, group, filterExpression.getExpression());
                if (decision != null) {
                    group = decision.getEffectiveGroup();
                    subscriptionData = FilterAPI.build(topic, decision.getSql92(),
                        org.apache.rocketmq.common.filter.ExpressionType.SQL92);
                }
            }
```

> Place this AFTER the original `subscriptionData` assignment so the SQL92 from the decision replaces it. `group` is used by both `popMessage` and `popLiteMessage` below, so a single rewrite covers both branches.

- [ ] **Step 5: Apply rewrite in AckMessageActivity**

In `ackMessage`, replace `String group = request.getGroup().getName();` (line 56) with:

```java
            String group = request.getGroup().getName();
            String topic = request.getTopic().getName();
            if (trafficLabelRouter != null) {
                group = trafficLabelRouter.rewriteGroup(ctx, topic, group);
            }
```

> `topic` is already declared just below at line 57 in the original — remove the now-duplicate `String topic = request.getTopic().getName();` on the original line 57 to avoid a double declaration.

- [ ] **Step 6: Apply rewrite in ChangeInvisibleDurationActivity**

In `changeInvisibleDuration`, after `String group = request.getGroup().getName();` (line 51) insert:

```java
            if (trafficLabelRouter != null) {
                group = trafficLabelRouter.rewriteGroup(ctx, request.getTopic().getName(), group);
            }
```

- [ ] **Step 7: Apply rewrite in ForwardMessageToDeadLetterQueueActivity**

Open the file and locate where it reads `request.getGroup().getName()`. Immediately after that local `group` assignment, insert the same guard:

```java
            if (trafficLabelRouter != null) {
                group = trafficLabelRouter.rewriteGroup(ctx, request.getTopic().getName(), group);
            }
```

> If the variable is named differently (e.g. `groupName`), match the local name. The forward-to-DLQ RPC must target `G%label` so the message's checkpoint resolves on the same group it was popped from.

- [ ] **Step 8: Construct + inject the router in DefaultGrpcMessagingActivity**

In `DefaultGrpcMessagingActivity` constructor, after the four activities are created, build the router and inject it into each:

```java
        LabelGroupBootstrapper bootstrapper =
            new LabelGroupBootstrapper(messagingProcessor.getServiceManager().getAdminService());
        TrafficLabelRouter trafficLabelRouter =
            new TrafficLabelRouter(new LabelRoutingResolver(), bootstrapper);
        this.receiveMessageActivity.setTrafficLabelRouter(trafficLabelRouter);
        this.ackMessageActivity.setTrafficLabelRouter(trafficLabelRouter);
        this.changeInvisibleDurationActivity.setTrafficLabelRouter(trafficLabelRouter);
        this.forwardMessageToDeadLetterQueueActivity.setTrafficLabelRouter(trafficLabelRouter);
```

> If `MessagingProcessor` does not expose `getServiceManager().getAdminService()`, add a narrow accessor `AdminService getAdminService()` to `MessagingProcessor` + `DefaultMessagingProcessor` (delegating to `serviceManager.getAdminService()`), mirroring the existing `getSubscriptionGroupConfig` delegate at `DefaultMessagingProcessor.java:148`. Match the actual activity field names in `DefaultGrpcMessagingActivity`.

- [ ] **Step 9: Run all four activities' tests**

Run: `mvn -pl proxy -am test -Dtest=ReceiveMessageActivityTest,AckMessageActivityTest,ChangeInvisibleDurationActivityTest`
Expected: PASS — including the new gray-routing test; all pre-existing tests still green (master switch defaults off in their setup).

- [ ] **Step 10: Commit**

```bash
git add proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/
git add proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/consumer/ReceiveMessageActivityTest.java
git commit -m "feat(proxy): apply traffic-label group rewrite at all four consumer entries"
```

---

## Task 9: LabelGroupCleaner (optional periodic cleanup)

**Files:**
- Create: `proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/consumer/LabelGroupCleaner.java`
- Test: `proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/consumer/LabelGroupCleanerTest.java`

> The cleaner deletes a `G%label` group + its `%RETRY%G%label` topic only when: cleanup is enabled, the label has no online consumer, origin & retry offsets are both at max, and it has been offline beyond the idle threshold. Per spec §2.1 there is NO third "revive in-flight" condition (no standard harvest exists). The offset/online/retry queries are abstracted behind a `CleanupProbe` interface so this unit is testable without a live broker; the production probe is wired in Task 10.

- [ ] **Step 1: Write the failing test**

```java
package org.apache.rocketmq.proxy.grpc.v2.consumer;

import org.apache.rocketmq.proxy.service.admin.AdminService;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LabelGroupCleanerTest {

    private final long threshold = 1000L;

    private LabelGroupCleaner.CleanupProbe probe(boolean online, boolean drained, long offlineMs) {
        LabelGroupCleaner.CleanupProbe p = mock(LabelGroupCleaner.CleanupProbe.class);
        when(p.hasOnlineConsumer("G%gray1")).thenReturn(online);
        when(p.isDrained("test-topic", "G%gray1")).thenReturn(drained);
        when(p.offlineDurationMs("G%gray1")).thenReturn(offlineMs);
        return p;
    }

    @Test
    public void deletes_idle_drained_offline_group() {
        AdminService admin = mock(AdminService.class);
        LabelGroupCleaner cleaner = new LabelGroupCleaner(admin, threshold);
        cleaner.cleanupOnce("test-topic", java.util.Collections.singletonList("G%gray1"),
            probe(false, true, 2000L));
        verify(admin).deleteSubscriptionGroup("test-topic", "G%gray1");
    }

    @Test
    public void keeps_group_with_online_consumer() {
        AdminService admin = mock(AdminService.class);
        LabelGroupCleaner cleaner = new LabelGroupCleaner(admin, threshold);
        cleaner.cleanupOnce("test-topic", java.util.Collections.singletonList("G%gray1"),
            probe(true, true, 2000L));
        verify(admin, never()).deleteSubscriptionGroup(any(), any());
    }

    @Test
    public void keeps_group_offline_within_threshold() {
        AdminService admin = mock(AdminService.class);
        LabelGroupCleaner cleaner = new LabelGroupCleaner(admin, threshold);
        cleaner.cleanupOnce("test-topic", java.util.Collections.singletonList("G%gray1"),
            probe(false, true, 500L));
        verify(admin, never()).deleteSubscriptionGroup(any(), any());
    }

    @Test
    public void keeps_group_not_yet_drained() {
        AdminService admin = mock(AdminService.class);
        LabelGroupCleaner cleaner = new LabelGroupCleaner(admin, threshold);
        cleaner.cleanupOnce("test-topic", java.util.Collections.singletonList("G%gray1"),
            probe(false, false, 2000L));
        verify(admin, never()).deleteSubscriptionGroup(any(), any());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl proxy -am test -Dtest=LabelGroupCleanerTest`
Expected: FAIL — `LabelGroupCleaner` not defined.

- [ ] **Step 3: Create LabelGroupCleaner**

```java
package org.apache.rocketmq.proxy.grpc.v2.consumer;

import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.service.admin.AdminService;

/**
 * Optional periodic cleanup of idle G%label groups and their retry topics.
 * Deletion requires: no online consumer, origin+retry drained to max, and offline
 * beyond the idle threshold. No "revive in-flight" check is needed — this design has
 * no standard harvest, so a gray's failed messages are handled within its own group.
 */
public class LabelGroupCleaner {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final AdminService adminService;
    private final long idleThresholdMs;

    public LabelGroupCleaner(AdminService adminService, long idleThresholdMs) {
        this.adminService = adminService;
        this.idleThresholdMs = idleThresholdMs;
    }

    public void cleanupOnce(String sampleTopic, List<String> candidateGroups, CleanupProbe probe) {
        for (String group : candidateGroups) {
            if (probe.hasOnlineConsumer(group)) {
                continue;
            }
            if (!probe.isDrained(sampleTopic, group)) {
                continue;
            }
            if (probe.offlineDurationMs(group) < idleThresholdMs) {
                continue;
            }
            boolean ok = adminService.deleteSubscriptionGroup(sampleTopic, group);
            log.info("traffic-label cleanup delete group {} result={}", group, ok);
        }
    }

    /**
     * Abstraction over broker-state queries so the cleanup policy is unit-testable.
     */
    public interface CleanupProbe {
        boolean hasOnlineConsumer(String group);

        /** True iff origin offset == max AND retry topic offset == max for the group. */
        boolean isDrained(String sampleTopic, String group);

        long offlineDurationMs(String group);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl proxy -am test -Dtest=LabelGroupCleanerTest`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/consumer/LabelGroupCleaner.java \
        proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/consumer/LabelGroupCleanerTest.java
git commit -m "feat(proxy): add optional LabelGroupCleaner with idle/drained policy"
```

---

## Task 10: E2E integration test (real broker + proxy)

**Files:**
- Create: `test/src/test/java/org/apache/rocketmq/test/grpc/v2/TrafficLabelRoutingIT.java`

> Follow the existing integration pattern in `test/src/test/java/org/apache/rocketmq/test/grpc/v2/ClusterGrpcIT.java` and `IntegrationTestBase` (real broker + nameserver + proxy startup, no mocks). Enable `enableTrafficLabelRouting=true` and broker `enablePropertyFilter=true` in the started configs. The consumer declares its label via the gRPC metadata header `__RMQ_TRAFFIC_LABEL`.

- [ ] **Step 1: Write the E2E test (isolation + no-fallback)**

```java
package org.apache.rocketmq.test.grpc.v2;

import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E for traffic-label proxy-only routing. Real broker + proxy, no mocks.
 * Covers spec §10: isolation (E2E-01), no-fallback (E2E-06), ack consistency (E2E-07).
 */
public class TrafficLabelRoutingIT extends GrpcBaseIT {  // extend the existing base used by ClusterGrpcIT

    @Test
    public void gray_consumer_only_receives_its_label_and_no_fallback_to_standard() throws Exception {
        // Arrange: topic with 4 queues; broker enablePropertyFilter=true; proxy enableTrafficLabelRouting=true
        String topic = initTopicOnSampleBroker(4);

        // gray1 consumer declares label gray1 (metadata header __RMQ_TRAFFIC_LABEL=gray1), group=G
        GrayConsumer gray1 = startConsumer(topic, "G", /*label*/ "gray1");
        StandardConsumer std = startConsumer(topic, "G", /*label*/ null);

        // Act: send 10 label=gray1 + 10 label=STANDARD + 10 no-label
        sendMessages(topic, 10, /*label*/ "gray1");
        sendMessages(topic, 10, /*label*/ "STANDARD");
        sendMessages(topic, 10, /*label*/ null);

        // Assert: isolation
        assertThat(gray1.poll(10)).hasSize(10);
        assertThat(gray1.allLabels()).containsOnly("gray1");
        assertThat(std.poll(20)).hasSize(20);
        assertThat(std.consumedLabel("gray1")).isZero();

        // Act: stop gray1, send 5 more label=gray1
        gray1.stop();
        sendMessages(topic, 5, "gray1");

        // Assert: NO fallback — standard never consumes the gray backlog
        Thread.sleep(5000);
        assertThat(std.consumedLabel("gray1")).isZero();
    }
}
```

> The helper names (`GrpcBaseIT`, `initTopicOnSampleBroker`, `startConsumer`, `sendMessages`, `GrayConsumer`) are illustrative — bind them to the real base class and utilities used by `ClusterGrpcIT`. The label must be transmitted as a client-level gRPC metadata header so it reaches `ProxyContext`. Verify the proxy context interceptor copies header `__RMQ_TRAFFIC_LABEL` into `ProxyContext`; if not, add that copy in the gRPC context-init interceptor (a one-line `context.withVal(...)`), which is part of the label transport pinned by this plan.

- [ ] **Step 2: Run it to verify it fails (or errors on missing wiring)**

Run: `mvn -pl test -am test -Dtest=TrafficLabelRoutingIT`
Expected: FAIL — until the header→ProxyContext copy + config flags are in place.

- [ ] **Step 3: Add header→ProxyContext copy in the gRPC context interceptor**

Locate the gRPC context-init interceptor/pipeline that builds `ProxyContext` from request metadata (search `withVal` usages near the gRPC interceptor). Copy the label header:

```java
        String trafficLabel = metadata.get(
            io.grpc.Metadata.Key.of(TrafficLabel.PROPERTY_KEY, io.grpc.Metadata.ASCII_STRING_MARSHALLER));
        if (org.apache.commons.lang3.StringUtils.isNotBlank(trafficLabel)) {
            context.withVal(TrafficLabel.PROPERTY_KEY, trafficLabel);
        }
```

- [ ] **Step 4: Run it to verify it passes**

Run: `mvn -pl test -am test -Dtest=TrafficLabelRoutingIT`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add test/src/test/java/org/apache/rocketmq/test/grpc/v2/TrafficLabelRoutingIT.java
git add proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/  # interceptor change
git commit -m "test(proxy): E2E traffic-label isolation + no-fallback; wire label header into ProxyContext"
```

---

## Task 11: Full build + verification gate

- [ ] **Step 1: Full module build**

Run: `mvn -pl proxy -am clean test`
Expected: BUILD SUCCESS, all proxy unit tests green.

- [ ] **Step 2: Run the E2E module**

Run: `mvn -pl test -am test -Dtest=TrafficLabelRoutingIT`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Verify default-off bypass on existing suites**

Run: `mvn -pl proxy -am test -Dtest=ReceiveMessageActivityTest,AckMessageActivityTest,ChangeInvisibleDurationActivityTest`
Expected: PASS — confirms zero behavior change when the master switch is off.

- [ ] **Step 4: Commit any fixups, then run the verify skill**

Use the `verify` skill (per user rules, mandatory plan-completion gate). Address any high/medium findings before reporting done.

---

## Self-Review notes (filled by author)

- **Spec coverage:** §2.1 toggles → Task 1; §4 components LabelRoutingResolver/Bootstrapper → Tasks 3/6; group via admin (D1) → Task 5; consumer declares label (D2) → Tasks 4/10; cleanup (D3 optional) → Task 9; transparent rewrite at four entries (D4, §3 receipt-handle fact) → Tasks 7/8; SQL92 AND-merge (§5) → Task 3; E2E §10 (E2E-01/06/07) → Task 10. E2E-08 (master-off bypass) → Task 7 unit + Task 11 step 3. E2E-09 (cleanup) → Task 9 unit.
- **Deferred to execution discovery (explicitly flagged inline):** exact `ProxyContext` get/set accessor names; `ConfigurationManager` test-config installation helper; `DefaultGrpcMessagingActivity` field names; `MessagingProcessor.getServiceManager()/getAdminService()` accessor; gRPC interceptor location for the header copy; existing IT base class name. Each carries an inline instruction to match the real symbol rather than invent one.
- **Type consistency:** `RoutingDecision.getEffectiveGroup()/getSql92()`, `TrafficLabel.effectiveGroup/isGray/PROPERTY_KEY/STANDARD`, `AdminService.createSubscriptionGroup(sampleTopic, config)/deleteSubscriptionGroup(sampleTopic, group)`, `LabelGroupBootstrapper.ensureGroup(topic, group)`, `TrafficLabelRouter.rewriteGroup/resolveForReceive`, `LabelGroupCleaner.cleanupOnce/CleanupProbe` — used consistently across tasks.
