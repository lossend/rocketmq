# Plan: Dynamic Traffic-Label Routing via ConsumerIdsChangeListener Index

## Context

Branch `traffic-label-5.5.0` already ships the **"old" Plan-C** (per-request label header + fixed
`STANDARD/null` complement filter). The plan docs on disk describe a **revised** design that is
**not yet implemented**:

- `docs/plans/2026-06-30-traffic-label-routing-plan-c-proxy-only.md` — revised Plan-C: standard
  consumers dynamically **exclude the isolated labels that are currently online** for the same
  `(topic, logicalGroup)`, driven by an event index.
- `docs/plans/2026-06-30-consumer-clientinfo-index-by-topic.md` — the `ConsumerIdsChangeListener`
  index that feeds that decision. No `TopicClientInfoIndex` / listener registration exists anywhere.

**Problem with the current fixed filter:** a standard consumer always excludes *only* `null`/`default`.
If a gray lane `gray3` exists but the standard filter was hard-coded for `gray1`, standard would wrongly
consume `gray3`, or wrongly exclude a gray lane that has gone offline. The exclusion must reflect the
**live** set of online isolated labels per `(topic, logicalGroup)`.

**Outcome:** keep zero broker code changes and the existing gray path unchanged; make the standard-side
filter dynamic and driven by an event index. Master switch stays OFF by default.

### Locked decisions (confirmed with user)
1. **Include registration rewrite** — gray consumers register at the broker under `G%label` so the
   `CLIENT_REGISTER`/`CLIENT_UNREGISTER` events expose gray topology per `(topic, logicalGroup)`.
2. **Gray runtime resolution stays per-request header** (`__SERVICE_TAG__` on `ProxyContext`). No
   `clientId` reverse-lookup. The existing gray receive/ack/changeInvisible/DLQ path is unchanged.
3. **Standard filter = dynamic exclude, empty ⇒ no filter.** When gray labels are online for
   `(topic, G)`: `__SERVICE_TAG__ IS NULL OR (__SERVICE_TAG__ <> 'g1' AND ...)`, AND-merged with the
   origin expression. When none online: emit no label filter (origin expression passes through unchanged).

---

## Source anchors (verified in this checkout)

| Fact | Anchor |
|---|---|
| gRPC consumer registration reads `group` and passes it down; `ctx` available | `ClientActivity.registerConsumer` `proxy/.../grpc/v2/client/ClientActivity.java:428-447`; caller `heartbeat` `:98-140` |
| unregister path | `ClientActivity` `unRegisterConsumer` (`:176` termination) → `ClientProcessor.java:150` → `ClusterConsumerManager.unregisterConsumer:54` |
| `ctx` is dropped below `ClientProcessor.registerConsumer` | `ClientProcessor.java:89` (broker `ConsumerManager` has no `ctx`) → rewrite must be at/above `ClientActivity` |
| listener registration hook | `MessagingProcessor.registerConsumerListener` `:412`; impl `DefaultMessagingProcessor.java:361`; terminus `ConsumerManager.appendConsumerIdsChangeListener` `broker/.../ConsumerManager.java:393` |
| `CLIENT_REGISTER` args: `[0]=ClientChannelInfo`, `[1]=Set<String> topics` | `broker/.../ConsumerManager.java:252-253` |
| `CLIENT_UNREGISTER` args (same shape) | `broker/.../ConsumerManager.java:315`, `:373` |
| interface | `ConsumerIdsChangeListener.handle(event, group, args...)` + `shutdown()`; enum `ConsumerGroupEvent{CHANGE,UNREGISTER,REGISTER,CLIENT_REGISTER,CLIENT_UNREGISTER}` |
| listener idiom to copy | `DefaultReceiptHandleManager.java:94-117`, `HeartbeatSyncer.java:76` |
| router wiring point | `DefaultGrpcMessagingActivity.init()` `proxy/.../grpc/v2/DefaultGrpcMessagingActivity.java:86-107` (has `messagingProcessor`) |
| standard/gray receive integration | `ReceiveMessageActivity.java:125-140` (`trafficLabelRouter.resolveForReceive`) |
| existing helpers to reuse | `TrafficLabel.effectiveGroup/isGray/GROUP_SEPARATOR`; `LabelRoutingResolver.toSql92Clause`/`mergeExpressions` (private today) |
| header→ctx copy | `ContextInitPipeline.java:43-47`, header `GrpcConstants.TRAFFIC_LABEL="__SERVICE_TAG__"` (grpc-java normalizes to `__service_tag__` on the wire), key `TrafficLabel.PROPERTY_KEY="__SERVICE_TAG__"` |
| IT harness | `test/.../grpc/v2/TrafficLabelRoutingIT.java`, base `GrpcBaseIT.java`; `createLabeledBlockingStub`, `createStandardBlockingStub`, `buildSendMessageRequestWithLabel`, `enablePropertyFilter=true` |

---

## Design

Reuse the existing gray path. Add three components and rewrite only the **standard** receive branch.

```
gray consumer  --(header __SERVICE_TAG__=gray1)--> register rewritten to G%gray1
standard consumer --------------------------------> register stays G
                                                      |
   broker ConsumerManager emits CLIENT_REGISTER/UNREGISTER(group=G%gray1|G, topics)
                                                      v
   TopicClientInfoIndex (ConsumerIdsChangeListener)
     activeIsolatedLabelTable: Map<topic, Map<logicalGroup, Set<label>>>
                                                      |
   receive:  gray  -> existing resolver (unchanged): group=G%gray1, __SERVICE_TAG__='gray1'
             standard -> StandardFilterAssembler(index.activeLabels(topic,G), originExp)
                         -> exclusion SQL92 or null (origin passes through)
```

### New / changed files

| File | Change |
|---|---|
| `proxy/.../grpc/v2/consumer/Sql92Filters.java` | **New.** Extract `toSql92Clause(expr,type)` + `merge(a,b)` (moved out of `LabelRoutingResolver` privates) so both resolver and assembler share one implementation. |
| `proxy/.../grpc/v2/consumer/LabelRoutingResolver.java` | **Modify.** Delegate its two private helpers to `Sql92Filters` (behavior identical; keeps gray path unchanged). Add `TrafficLabel.parseLabel/parseLogicalGroup` usage only if needed. |
| `proxy/.../grpc/v2/consumer/TrafficLabel.java` | **Modify.** Add `parseLogicalGroup(effectiveGroup)` and `parseLabel(effectiveGroup)` (split on `GROUP_SEPARATOR`), for the index to derive `(logicalGroup,label)` from `G%gray1`. |
| `proxy/.../grpc/v2/consumer/TopicClientInfoIndex.java` | **New.** `implements ConsumerIdsChangeListener`. Maintains `activeIsolatedLabelTable`. `getActiveIsolatedLabels(topic, logicalGroup): Set<String>`. Handles `CLIENT_REGISTER`/`CLIENT_UNREGISTER` only; ignores others. Null/length-guards `args` per `DefaultReceiptHandleManager` idiom. |
| `proxy/.../grpc/v2/consumer/StandardFilterAssembler.java` | **New.** `assemble(topic, logicalGroup, originExpr, originType, Set<label> activeLabels): String or null`. Builds `__SERVICE_TAG__ IS NULL OR (__SERVICE_TAG__ <> 'g1' AND ...)`, AND-merges origin via `Sql92Filters`; returns `null` when no active labels **and** no origin clause. |
| `proxy/.../grpc/v2/consumer/TrafficLabelRouter.java` | **Modify.** Inject `TopicClientInfoIndex`. Add `rewriteRegistrationGroup(ctx, group)` (rename only, no bootstrap). In `resolveForReceive`: gray branch unchanged; **standard branch** now consults `StandardFilterAssembler` with `index.getActiveIsolatedLabels(topic, originGroup)`; returns `null` when the assembler yields no change. |
| `proxy/.../grpc/v2/client/ClientActivity.java` | **Modify.** In `registerConsumer` (`:428`) rewrite `consumerGroup` via `router.rewriteRegistrationGroup(ctx, consumerGroup)` before building `ClientChannelInfo`/registering; mirror in the unregister path. Gated by master switch (router returns input unchanged when OFF). |
| `proxy/.../grpc/v2/DefaultGrpcMessagingActivity.java` | **Modify.** Build one `TopicClientInfoIndex`, `messagingProcessor.registerConsumerListener(index)`, inject into `TrafficLabelRouter`, and pass the router into `ClientActivity` (new setter, mirroring the existing `setTrafficLabelRouter` on the four activities). |
| `proxy/.../grpc/v2/AbstractMessagingActivity.java` | **Modify (if ClientActivity extends it)** — otherwise add `setTrafficLabelRouter` to `ClientActivity` directly. |

> **Wiring note:** listener is appended in `init()` which runs before `PROXY_START_AND_SHUTDOWN.start()`
> in prod and before any client registers in the IT — both safe. No `ProxyStartup.java` change required.

---

## Tasks (test-first)

### Task 1 — Extract `Sql92Filters`, add `TrafficLabel` parse helpers
- **Test:** `Sql92FiltersTest` (new, pure JUnit4/AssertJ) — TAG→`TAGS in (...)`, SUB_ALL/blank→null, SQL92 passthrough, `merge` AND-wrapping. `TrafficLabelTest` — add `parseLogicalGroup("G%gray1")=="G"`, `parseLabel("G%gray1")=="gray1"`, plain `"G"`→(`"G"`, null).
- **Impl:** move the two private helpers from `LabelRoutingResolver` into `Sql92Filters`; resolver delegates. Add parse helpers to `TrafficLabel`.
- **Regression guard:** existing `LabelRoutingResolverTest` must still pass unchanged.

### Task 2 — `TopicClientInfoIndex`
- **Test:** `TopicClientInfoIndexTest` (new) — construct listener; feed a fake `ClientChannelInfo` + topic set:
  - CLIENT_REGISTER `G%gray1` on `T` ⇒ `getActiveIsolatedLabels(T,"G")=={"gray1"}`.
  - CLIENT_REGISTER `G` (standard) on `T` ⇒ no labels added.
  - Two gray labels online ⇒ set of both; CLIENT_UNREGISTER one ⇒ removed; last removed ⇒ empty set.
  - `getActiveIsolatedLabels` of unknown `(topic,group)` ⇒ empty set (never null).
  - Non-CLIENT events (`CHANGE`,`REGISTER`,`UNREGISTER`) are ignored; malformed `args` don't throw.
- **Impl:** `ConcurrentHashMap` nesting + `newKeySet`; parse via `TrafficLabel.parseLogicalGroup/parseLabel`; unmodifiable defensive copy on read.

### Task 3 — `StandardFilterAssembler`
- **Test:** `StandardFilterAssemblerTest` (new):
  - active `{gray1,gray2}`, origin null ⇒ `__SERVICE_TAG__ IS NULL OR (__SERVICE_TAG__ <> 'gray1' AND __SERVICE_TAG__ <> 'gray2')` (assert stable ordering — sort labels).
  - active `{gray1}`, origin TAG `TagA` ⇒ `( TAGS in ('TagA') ) AND ( <exclusion> )`.
  - active empty, origin null ⇒ `null` (no change).
  - active empty, origin SQL92 `a>1` ⇒ `a>1` (origin only, no label clause).
  - single-quote escaping in labels.
- **Impl:** sort labels for determinism; build exclusion; merge via `Sql92Filters.merge`.

### Task 4 — Wire index + standard branch into `TrafficLabelRouter`
- **Test:** extend `TrafficLabelRouterTest` (hand-spy style already in file):
  - switch OFF ⇒ `resolveForReceive` returns null, `rewriteRegistrationGroup` returns input.
  - gray label present ⇒ unchanged behavior (group `G%gray1`, `__SERVICE_TAG__='gray1'`, bootstrap called once).
  - standard, index has `{gray1}` for `(T,G)` ⇒ decision keeps group `G`, sql92 excludes `gray1`.
  - standard, index empty ⇒ returns null (caller keeps origin subscription).
  - `rewriteRegistrationGroup` with gray header ⇒ `G%gray1` (no bootstrap call); standard ⇒ `G`.
- **Impl:** inject `TopicClientInfoIndex` + `StandardFilterAssembler`; branch on `TrafficLabel.isGray`.

### Task 5 — Apply registration rewrite in `ClientActivity`
- **Test:** `ClientActivityTrafficLabelTest` (new, Mockito) — verify `messagingProcessor.registerConsumer` is invoked with `G%gray1` when header present + switch ON; with `G` when standard or switch OFF; unregister path mirrors.
- **Impl:** call `router.rewriteRegistrationGroup(ctx, consumerGroup)` at `:428` before building `ClientChannelInfo`; mirror in unregister. Wire router into `ClientActivity` in `DefaultGrpcMessagingActivity.init()`.

### Task 6 — Startup wiring
- **Impl:** in `DefaultGrpcMessagingActivity.init()`: create `TopicClientInfoIndex`, `messagingProcessor.registerConsumerListener(index)`, build `StandardFilterAssembler`, construct `TrafficLabelRouter(resolver, bootstrapper, index, assembler)`, inject into the four consumer activities **and** `ClientActivity`.
- **Test:** covered by Task 7 E2E (integration-level; no unit test for wiring).

### Task 7 — E2E in `TrafficLabelRoutingIT`
Add scenarios to the existing IT (register the index listener in `setUp()` between `createForLocalMode` and `start()`; mirror prod wiring):
- **E2E-A gray online ⇒ standard excludes gray:** gray consumer (`createLabeledBlockingStub("gray1")`) issues a receive to register under `G%gray1`; produce one `gray1`-labeled + one standard message; assert standard consumer (`createStandardBlockingStub`) receives only the standard message (Awaitility poll, per-message `getUserPropertiesMap().get(PROPERTY_KEY)` assertion).
- **E2E-B gray offline ⇒ exclusion removed:** after gray channel closes / `scanNotActiveChannel` expiry, produce a new `gray1` message; assert standard now can match it (poll until index empties). *(If deterministic offline is hard in-process, drive CLIENT_UNREGISTER via the termination RPC used by the base harness; note any timing reliance in the test comment.)*
- **E2E-C gray path regression:** existing gray/standard tests still pass unchanged.

---

## Verification

1. **Unit:** `mvn -pl proxy -am test -Dtest='Sql92FiltersTest,TrafficLabelTest,TopicClientInfoIndexTest,StandardFilterAssemblerTest,TrafficLabelRouterTest,LabelRoutingResolverTest,ClientActivityTrafficLabelTest'`
2. **E2E (real broker+proxy, no mocks):** `mvn -pl test -am test -Dtest=TrafficLabelRoutingIT` — broker runs with `enablePropertyFilter=true` via `IntegrationTestBase`.
3. **Full build:** `mvn -q -pl proxy -am -DskipTests package` to confirm compilation.
4. **Switch-off safety:** confirm with `enableTrafficLabelRouting=false` (default) that registration group, receive filter, and index are all bypassed (Task 4/5 OFF cases + IT `@After` resets the flag).
5. **`verify` skill** run as the mandatory plan-completion gate.
6. On completion, copy this plan to `docs/plans/2026-07-03-traffic-label-dynamic-index.md` per project planning rules, then commit on the feature branch.

## Risks
- Registration rewrite affects both `heartbeatSyncer.onConsumerRegister` and `super.registerConsumer` (same `group` arg) — cross-proxy sync will broadcast `G%label`, which is intended.
- Index reflects **current/future** traffic only — no historical backlog harvesting (explicit Plan-C boundary; E2E-B asserts only new messages).
- Standard filter must recompute per receive from the live index; determinism enforced by sorting labels.
