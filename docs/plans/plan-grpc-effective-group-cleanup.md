# gRPC Traffic-Label Activity Decorator Implementation Plan

**Goal:** Establish one typed boundary that converts client-visible consumer groups into the effective groups used by Proxy and Broker, so registration, consumption, acknowledgement, cleanup, and termination stay paired.

**Architecture:** `GrpcMessagingApplication` runs the existing request pipeline against the original protobuf request, then dispatches through a new `TrafficLabelGrpcMessagingActivity` decorator. The decorator rewrites only group-bearing consumer/client requests and delegates all other calls unchanged. Registration stores the effective group directly in `CLIENT_SETTINGS_MAP`; later requests prefer that client binding. The wrapped telemetry response restores the logical group before it is sent to the SDK.

**Tech Stack:** Java 8, RocketMQ Proxy, protobuf/gRPC, JUnit 4, Mockito, AssertJ.

---

### Task 1: Lock the routing boundary with failing tests

1. Add `@DisplayName` tests for decorator request transformation and passthrough behavior.
2. Cover Receive group + SQL92 filter transformation and telemetry logical/effective round-trip.
3. Add the missing-settings Receive regression test.
4. Run focused tests and record the expected RED failures before production changes.

### Task 2: Add the decorator and effective-group resolver

1. Add `TrafficLabelGrpcMessagingActivity`, explicitly implementing every `GrpcMessagingActivity` method.
2. Add a resolver that uses the registered Settings group first and falls back to the traffic-label header before registration.
3. Rewrite QueryAssignment, Receive, Ack, ChangeInvisibleDuration, ForwardDLQ, Heartbeat, Termination, SyncLiteSubscription, and telemetry Settings.
4. For Receive, rewrite both group and filter; for telemetry, restore the original group on outbound Settings.
5. Delegate non-consumer requests and lifecycle methods unchanged.

### Task 3: Make Default activity group-agnostic and fix lifecycle cleanup

1. Wire the decorator in `GrpcMessagingApplication.create` after the request pipeline.
2. Move traffic-label router construction to the decorator factory and remove per-Activity router injection.
3. Remove group rewrites from child Activities and `ClientActivity` so downstream code consumes only effective requests.
4. Let transformed telemetry Settings populate `CLIENT_SETTINGS_MAP` with the effective group; Cleaner then checks the registered group without a second map.
5. Add the Receive missing-settings response and ensure downstream POP is skipped.

### Task 4: Verify behavior and compatibility

1. Verify the pipeline sees the logical group while the delegate sees the effective group.
2. Verify all group-bearing methods rewrite exactly once, binding survives missing/changed headers, and feature-off calls pass through unchanged.
3. Verify Cleaner retention/removal, remote Settings sync, telemetry response restoration, Receive NACK/renew paths, and client termination.
4. Run focused Proxy tests, `mvn clean compile`, `ur-format`, focused tests again, and `git diff --check`.
5. Review the final diff; do not commit, push, or deploy.

