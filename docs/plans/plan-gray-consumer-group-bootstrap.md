# Gray Consumer Group Bootstrap Implementation Plan

> **For Codex:** Execute this plan task-by-task with test-first verification.

**Goal:** Explicitly create a traffic-label virtual subscription group before Proxy validates and registers a gray consumer.

**Architecture:** Registration already rewrites a gray consumer group to `origin%label`. Pass a subscribed topic to the traffic-label router and use the existing `LabelGroupBootstrapper` to ensure that virtual group exists before `ClientProcessor.validateLiteMode` queries its configuration. Standard consumers and disabled routing keep their current behavior.

**Tech Stack:** Java, JUnit 4, Mockito, Maven.

---

### Task 1: Add a regression test

**Files:**

- Modify: `proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/client/ClientActivityTrafficLabelTest.java`

1. Make the test bootstrapper observable.
2. Add a gray registration test with a subscription topic and assert it ensures `G%gray1` using that topic.
3. Run the focused test and confirm it fails before implementation.

### Task 2: Bootstrap the rewritten gray group at registration

**Files:**

- Modify: `proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/client/ClientActivity.java`
- Modify: `proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/consumer/TrafficLabelRouter.java`

1. Add a registration-specific router method that takes the first subscription topic.
2. With traffic-label routing enabled and a gray label, rewrite the group and call `LabelGroupBootstrapper.ensureGroup` before returning it.
3. Preserve existing behavior for disabled routing, standard labels, and registrations without a topic.
4. Update the misleading Javadoc that says Broker auto-creates the group.

### Task 3: Verify

1. Re-run the focused regression test.
2. Run the affected Proxy test class.
3. Run the Proxy module test compile/check appropriate to the repository configuration.
4. Inspect the final diff and report test evidence.
