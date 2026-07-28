# Proxy Graceful Lifecycle Correctness Fixes

## Goal

Close the correctness gaps found in the implemented Proxy lifecycle so a send can only reach an application success after a real Broker-facing completion, shutdown cannot leave admission open, and forced drain is visible to automation as failure.

## Implementation Tasks

### 1. Production startup and forced drain

**Files**

- `proxy/src/main/java/org/apache/rocketmq/proxy/lifecycle/ProxyLifecycleCoordinator.java`
- `proxy/src/main/java/org/apache/rocketmq/proxy/ProxyStartup.java`
- corresponding lifecycle/startup tests under `proxy/src/test/java`

**Changes**

- Add one public startup-complete transition and invoke it only after all Proxy owners start successfully.
- Close the shared send gate immediately after winning any force-drain transition.
- Preserve adapter force failures in the terminal drain result.
- Dispatch transport-termination continuation away from the permit/tracer completion thread.

### 2. gRPC terminal-before-bind race

**Files**

- `proxy/src/main/java/org/apache/rocketmq/proxy/lifecycle/grpc/GrpcSendLifecycleHolder.java`
- `proxy/src/test/java/org/apache/rocketmq/proxy/lifecycle/grpc/GrpcSendLifecycleHolderTest.java`

**Changes**

- Latch the first stream terminal independently from permit binding.
- Deliver the latched terminal once when both pieces are present, regardless of ordering.
- Add deterministic close-before-bind and concurrent exactly-once tests.

### 3. Send success causality

**Files**

- `proxy/src/main/java/org/apache/rocketmq/proxy/lifecycle/grpc/SendLifecycleMessagingActivity.java`
- `proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/common/ResponseBuilder.java`
- focused tests for both classes

**Changes**

- Return the future stage that records `backendTerminal`, rather than an unchained Broker future.
- Convert skipped, synchronous-throw, and null-future dispatches into explicit exceptional completions.
- Sanitize all throwable mappings so an exception can never produce `Code.OK`.
- Rename the permit completion concept to release completion while retaining a compatibility alias.

### 4. CLI and documentation

**Files**

- `distribution/bin/mqproxyctl`
- `docker/tests/test-mqproxyctl.sh`
- `docs/plans/proxy-graceful-lifecycle/send-permit.md`

**Changes**

- Return non-zero from `drain --wait` for `FORCE_DRAINING` and for a sticky forced result after `STOPPED`.
- Add a launcher regression test.
- Update the lifecycle document to match the terminal latch and response-visible backend completion chain.

### 5. Concurrency closure and optional send drain

**Files**

- `proxy/src/main/java/org/apache/rocketmq/proxy/lifecycle/ProxyLifecycleCoordinator.java`
- `proxy/src/main/java/org/apache/rocketmq/proxy/lifecycle/grpc/GrpcActiveCallRegistry.java`
- `proxy/src/main/java/org/apache/rocketmq/proxy/lifecycle/grpc/GrpcActiveCallInterceptor.java`
- `proxy/src/main/java/org/apache/rocketmq/proxy/config/ProxyConfig.java`
- `proxy/src/main/java/org/apache/rocketmq/proxy/grpc/GrpcServerBuilder.java`
- `proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/GrpcMessagingApplication.java`
- focused concurrency and toggle tests

**Changes**

- Retry the force-state CAS when a normal lifecycle transition wins the first attempt, so a one-shot hard deadline cannot be lost.
- Linearize active-call count admission and publication into the live-call registry with `closeAll`, and reject post-close calls before the business handler.
- Add `enableProxySendDrain` as an explicit, lifecycle-dependent opt-in. When off, retain transport migration and active-call draining while skipping every send-permit component.
- Preserve the original six-argument `GrpcServerBuilder.configLifecycle` behavior by delegating it to send drain enabled.
- Document that strict production profiles must explicitly enable send drain to claim accepted-send dual-terminal draining.

## Verification

1. Run focused Proxy lifecycle, active-call, gRPC holder, response builder, messaging activity, and send-drain toggle tests.
2. Run `docker/tests/test-mqproxyctl.sh`.
3. Run `mvn clean compile` for the affected Maven reactor.
4. Run `git diff --check` and inspect the full final diff for unrelated changes.
