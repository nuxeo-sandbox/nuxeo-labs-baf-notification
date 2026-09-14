# AGENTS.md - Nuxeo Labs BAF Notification

Nuxeo LTS 2025 plugin: consumes the Nuxeo Stream `bulk/done` and fires a synchronous `bulkActionDone` Nuxeo event for each completed/aborted BAF command.

Standard Nuxeo plugin conventions apply. Notes below are repo-specific only. User-facing usage docs live in `README.md` — don't duplicate them here.

Parent: `org.nuxeo:nuxeo-parent:2025.18`. Target platform: `2025.*`. Project version: `2025.3.0-SNAPSHOT`. Released so far: `2025.1.0`, `2025.2.0`. Existing code uses `@since 2025.1` and `@since 2025.2`; new public API should use `@since 2025.3` (track `<version>` in the parent POM, not the parent's `2025.18`). Do NOT use `2025.18` or the current LTS dot version. After each release, bump this line to the new snapshot.

## Modules

- `nuxeo-labs-baf-notification-core` — the bundle (two components: stream processor + filter service).
- `nuxeo-labs-baf-notification-package` — Marketplace package wrapper.

Java package is `nuxeo.labs.bafnotification` (no dot between `baf` and `notification`); OSGi component names use `nuxeo.labs.baf.notification[.service]`. Don't unify them.

Core bundle SymbolicName (from `src/main/resources/META-INF/MANIFEST.MF`): `nuxeo.labs.baf.notification.nuxeo-labs-baf-notification-core`. Test bundle: same with `.tests` suffix. These exact strings are the prefix of every `@Deploy` annotation — if you rename them, update every test.

## Architecture

Single `StreamProcessorTopology` (`BulkActionDoneProcessor`) wires the existing `bulk/done` stream into `BulkActionDoneComputation`. The computation decodes `BulkStatus` with `BulkCodecs.getStatusCodec()`, asks `BAFNotificationService.shouldNotify(action)`, and fires the event if allowed — no output stream.

- Stream-processor component: `nuxeo.labs.baf.notification` (`OSGI-INF/baf-notification-processor.xml`).
- Service component: `nuxeo.labs.baf.notification.service` (`OSGI-INF/baf-notification-service.xml`) — this is the `target` used by integrators in `<extension>` contributions.
- The processor component declares `<require>org.nuxeo.ecm.core.bulk.config</require>` (so `bulk/done` is initialized first) and `<require>nuxeo.labs.baf.notification.service</require>` (so the filter service is ready before the computation starts). Do not remove either.
- `<streamProcessor>` attributes: `name="bulkActionDoneNotifier"` (same string as `BulkActionDoneComputation.COMPUTATION_NAME` — rename both together), `defaultConcurrency="1" defaultPartitions="1"`.
- `bulk/done` is declared `<stream name="bulk/done" external="true" />`: the stream is owned and created by `org.nuxeo.ecm.core.bulk.config`, we only consume it. Do not drop the `external="true"` — without it `LogStreamManager.initStream` tries to own a platform stream and correctness then depends on `createIfNotExists` internals.
- `bulk/done` is declared with `size="1"` by the platform, which is why concurrency/partitions are 1. That is the ceiling, not a tuning choice.

### THE critical invariant: the event MUST be fired inside a transaction

`TransactionHelper.runInTransaction(() -> eventService.fireEvent(event))` in `BulkActionDoneComputation.processRecord` is **mandatory**. A stream computation thread carries no ambient transaction (`nuxeo-runtime-stream` contains zero `TransactionHelper` references; `ComputationRunner` opens nothing). Firing a non-inline, non-commit event without one causes three separate defects at once:

1. `EventServiceImpl#recordEvent` pushes a `ShallowEvent` into a `ThreadLocal<CompositeEventBundle>` and then takes neither the `isTransactionActive()` branch nor the `isCommitEvent()` branch — so the bundle is never drained. `EventBundleImpl.events` is a plain `ArrayList` on a JVM-lifetime thread: **unbounded memory leak**.
2. Any listener opening a `CoreSession` fails with `NuxeoException: Cannot use a session outside a transaction` (`RepositoryService:309`). That breaks the Studio automation pattern documented in the README.
3. `handleTxCommited()` is unreachable, so **post-commit and async listeners never run**, silently.

`TestBulkActionDoneEvent.testListenerRunsInTransactionAndCanUseCoreSession` and `testPostCommitListenerIsNotified` are the regression guards. Both fail if the wrapper is removed — verified.

Do **not** "simplify" this to a bare `fireEvent`, and do not substitute `Event.FLAG_INLINE` (stops the leak but permanently kills post-commit listeners) or `FLAG_COMMIT` (dispatches every bundle synchronously on the computation thread with a null repository name).

### Error handling policy

Deliberate, and the three layers interact — do not change one in isolation:

- `ConcurrentUpdateException` is **re-thrown** (the platform re-throws it past its own swallowing logic precisely so the caller can retry). Re-throwing skips `askForCheckpoint()`, so the record is replayed.
- Every other `RuntimeException` from `fireEvent` is caught and logged. A misbehaving listener cannot block the pipeline and is never retried.
- `maxRetries="3"` therefore covers only: Avro decode failures, the `BulkService.getCommand` KV lookup, and re-thrown `ConcurrentUpdateException`. `continueOnFailure="true"` skips the record once the budget is exhausted, logging via the overridden `processFailure` (which includes `currentCommandId` — an offset alone is useless for support).

### Delivery semantics

**At-least-once, not exactly-once.** Do not reintroduce any "exactly once" claim in the README or javadoc — it was there and it was wrong. Four duplication paths exist: asynchronous checkpointing, stream retries re-running `processRecord` from the top, Kafka rebalance on a slow listener, and the upstream `BulkStatusComputation` itself producing a second `bulk/done` record when a late `bulk/status` delta arrives for an already-COMPLETED command. Listeners must dedupe on `commandId`.

Also note `auto.offset.reset` is always `earliest` in Nuxeo: a first install on an existing cluster replays the whole retained `bulk/done` history. The README documents the `stream.sh position --to-end` mitigation.

## Action-name filter (`BAFNotificationService`)

Extension point: `nuxeo.labs.baf.notification.service#configuration`. Descriptor `BAFNotificationConfigDescriptor` (`@XObject("actions")`) wraps a `@XNodeList` of `<action>` elements.

Contribution shape:

```xml
<extension target="nuxeo.labs.baf.notification.service" point="configuration">
  <actions>
    <action>setProperties</action>
  </actions>
</extension>
```

Semantics (implemented in `BAFNotificationServiceImpl`):
- No contribution registered → `shouldNotify` returns `true` for everything (fire-all default). Do NOT introduce a `*` wildcard — "no contribution" already covers it.
- Any contribution registered → fire only when `status.getAction()` is in the **union** of all contributed action names. Multiple contributions intentionally merge so a Studio project and a custom plugin can both contribute.
- Filtered-out actions: DEBUG log only, no event.
- Match is case-sensitive, exact string.
- A contribution whose effective action list is empty (`<actions/>`, or only blank `<action>` entries) registers as a contribution with an empty union and therefore **blocks every event**. This is a known footgun, so `rebuildActions()` logs a WARNING and calls `addRuntimeMessage(Level.WARNING, ...)`. Keep that warning.

### Concurrency — do not revert to a plain concurrent set

`shouldNotify` is called from the stream computation thread while contributions register/unregister on the runtime thread. The filter is held in a **single `volatile FilterState` record** (`hasContributions` + immutable `actions`) that `rebuildActions()` replaces atomically.

The earlier implementation used `Collections.newSetFromMap(new ConcurrentHashMap<>())` plus `clear()` / `addAll()`. That is element-safe but **not snapshot-safe**: between `clear()` and `addAll()` a reader saw `hasContributions == true` with an empty action set and **silently dropped events**. `shouldNotify` also read two independent fields, which was a second TOCTOU. Both are fixed by taking one volatile read. Do not "simplify" this back to a concurrent collection.

`TestBAFNotificationServiceFilterState` drives `registerContribution`/`unregisterContribution` directly and covers the transitions the XML-based tests cannot reach.

`XNodeList.trim()` defaults to `true`, so XMap already trims `<action>` text content. No `strip()` is needed in `rebuildActions()` — the `isBlank()` guard is sufficient.

## Event contract (`bulkActionDone`)

Constants live on `BulkActionDoneComputation`:
- `EVENT_NAME = "bulkActionDone"`
- `COMPUTATION_NAME = "bulkActionDoneNotifier"`

Event context properties (keep this list in sync with `BulkActionDoneComputation.processRecord`):
`commandId`, `action`, `username`, `state` (enum name: `COMPLETED` / `ABORTED`), `processed`, `skipCount`, `total`,
`queryLimitReached`, `errorCount`, `errorCode`, `errorMessage`, `processingDurationMillis`, `repository`, `query`,
`actionParams`, `result`.

`skipCount`, `queryLimitReached` and `result` are `@since 2025.3`.

`repository`, `query` and `actionParams` are pulled from the originating `BulkCommand` via `BulkService.getCommand(status.getId())`. If the command record was already evicted from the bulk KV store, `repository` and `query` are `null` and `actionParams` is `Map.of()` — the event is still fired. `actionParams` is the raw `Map<String, Serializable>` from `BulkCommand.getParams()`; cast it back in listeners.

`errorCount` is a count only. `BulkStatus` carries no list of failed document IDs and stock BAF actions do not expose one; per-doc failures land in `server.log` only. Do not add code that tries to enumerate failed docs for stock actions — there is no API. A custom action surfaces them by calling `status.setResult(...)`, which **is** forwarded to the event as the `result` property.

`result` is safe to forward unguarded: `BulkStatus.getResult()` wraps in `Collections.unmodifiableMap` (Serializable), and the codec's `MapAsJsonAsStringEncoding.read` returns `emptyMap()` — never `null` — for a missing value. But it is a **JSON** round-trip, so listeners get Jackson's default bindings, not the types the action stored. `TestBulkActionDoneEvent#testActionResultIsForwardedToTheEvent` injects a synthetic record to prove the round-trip (no stock action populates `result`).

Not forwarded, deliberately, to keep the payload small — it is serialised into the WorkManager stream for every async post-commit listener: `submitTime`, `completedTime`, `scrollStartTime`, `scrollEndTime`. Listeners read them from `BulkService.getStatus(commandId)`.

Two payload caveats that must stay in the README: `total` is `0` when the scroll never completed, and `processed` excludes `skipCount`, so `processed + errorCount != total` is not an invariant.

The event context carries **no principal and no `CoreSession`** (`new EventContextImpl()` binds to the public varargs constructor, so both are `null`, and `repositoryName` is `null` too). Listeners must open their own session. Do not "fix" this by passing a session into the context — the computation has none to give, and the context is copied into a `ShallowEvent` anyway.

The `null` principal has a documented consequence, do not lose it: a Studio Event Handler with a **user** filter (`filters/group`, `filters/isAdministrator`) NPEs in `EventHandler.isEnabled`, and `EventServiceImpl.fireEvent` swallows it — the chain silently never runs. Document filters merely skip silently. Both are documented in `README.md` and `README-MANUAL-TESTING.md` §4.5. Do **not** "fix" this by setting a `SystemPrincipal`: that would make an `isAdministrator` filter pass, turning a crash into a privilege check that always succeeds. Also do not set `repositoryName` without a principal — `ReconnectedEventBundleImpl` calls `ctx.getPrincipal().getActingUser()` whenever the repository name is non-null.

## Testing

Five test classes (24 tests). Every regression guard below was verified to **fail** against the pre-fix code — if you change one, re-verify the same way rather than trusting a green run.

- `TestBulkActionDoneEvent` — submits a real BAF command and asserts on the event. Holds **three regression guards for the transaction invariant** (see Architecture): `testListenerRunsInTransactionAndCanUseCoreSession`, `testPostCommitListenerIsNotified`, and `testNoEventBundleLeakOnComputationThread`. Do not weaken them. `testEventFiredOnCompletion` asserts the **whole** payload; it deliberately compares counter *relationships* (`processed == total`) rather than absolute numbers so it does not depend on how many documents the repository holds. `testActionResultIsForwardedToTheEvent` injects a synthetic record because no stock action populates `result`.
- `TestBAFNotificationFiltering` — action-name filter end to end. `test-filter-nomatch-contrib.xml` (action `noSuchActionEverFired`) is deployed at the **class level** so every test sees `hasContributions() == true` and `shouldNotify("setProperties") == false`. `test-filter-match-contrib.xml` (action `setProperties`) is deployed per-test on the methods that need the union to match. Never deploy either on `TestBulkActionDoneEvent` — it relies on the fire-all default.
- `TestBAFNotificationServiceFilterState` — drives `registerContribution`/`unregisterContribution` programmatically (no XML): unregister, partial unregister, the empty-contribution footgun, blank-name stripping, and the M4 WARNING + runtime message. Builds descriptors by assigning the package-visible `BAFNotificationConfigDescriptor.actions` field directly — that is why no public setter exists. The service is a singleton shared with the other classes, so every test must restore the fire-all default in a `finally`.
- `TestBulkActionDoneStreamSemantics` — at-least-once delivery, the seek-to-end mitigation, and consumer lag. **Stops, rewinds and restarts the shared computation**, hence its own class and an unconditional `@After` that restores position-to-end + restarted. `FeaturesRunner` reuses the runtime across classes with an identical feature/deploy set, so a half-finished test would otherwise leave a stopped consumer for the next class.
- `TestBulkActionDoneErrorHandling` — `ConcurrentUpdateException` retry, null `BulkStatus` state, and an undecodable record. Uses `LogCaptureFeature`.

### The leak probe

`TestBulkActionDoneListener.probePendingBundleSize()` reflects into `EventServiceImpl.threadBundles` (a `protected static final ThreadLocal`) **from inside the listener**, which runs on the computation thread — the thread that leaked. Since the inline-listener loop runs before `recordEvent`, what it observes is the residue of previous events, which must always be 0. Against the pre-fix code it reports `[3, 4, 5, 6, 7]`.

It returns `BUNDLE_SIZE_UNKNOWN` instead of throwing if the internals move, and the test `Assume`s out — a platform refactor degrades coverage rather than breaking the build. Keep that behaviour.

### Injecting records is not mocking

`TestBulkActionDoneErrorHandling` appends synthetic records straight to `bulk/done` via `StreamManager.append(...)`. This does **not** contradict the "do not mock `BulkService`" rule: the real pipeline never produces a malformed or null-state record, so injection is the only way to reach those branches. Every other test still drives the unmodified bulk pipeline.

`bulk/done` is appendable from tests because `bulkServiceProcessor` declares it `external="false"` and so registers a filter for it in the singleton `LogStreamManager`; our own processor declaring it `external="true"` does not remove that.

### Conventions

- Test bundle `Nuxeo-Component` declares ONLY `test-listener-contrib.xml`. The filter contribs and `test-failing-listener-contrib.xml` are deployed exclusively via `@Deploy`. Adding any of them to the manifest would silently activate a filter or a throwing listener at startup and break the other classes.
- `test-listener-contrib.xml` registers an inline listener (`TestBulkActionDoneListener`) and an **async post-commit** one (`TestBulkActionDonePostCommitListener`, `postCommit="true" async="true"`). Keep `async="true"`: the sync post-commit form is deprecated (NXP-26911) and floods the build log with a WARNING on every commit.
- `TestBulkActionDoneListener` captures into a `Map<String, Capture>` **keyed by `commandId`** plus a separate delivery counter (a capture map alone cannot detect a redelivery — the second capture overwrites the first). Its `handleEvent` deliberately opens a `CoreSession`; that is what makes it a real regression guard rather than a passive recorder.
- `FailingOnceListener` must be **armed** explicitly (`arm()` / `disarm()` in a `finally`). Otherwise a replay triggered by another test would trip it.
- **Waiting: use `StreamService.await`, never `Thread.sleep`.** `bulkService.await(commandId, ...)` only waits for the KV status to flip; `BulkStatusComputation` produces the `bulk/done` record after that. The correct wait is:
  ```java
  Framework.getService(StreamService.class)
           .await(Name.ofUrn("bulk/done"), Name.ofUrn(BulkActionDoneComputation.COMPUTATION_NAME), duration);
  ```
  ⚠️ **Argument order trap:** the `StreamService` interface declares `await(Name computation, Name stream, ...)` but the implementation is `await(Name stream, Name computation, ...)` and calls `getLag(stream, group)`. **Stream first** is what actually works; the interface parameter names are wrong. Same for `getLag(Name stream, Name group)`.
  (Earlier versions used `Thread.sleep(2000)` and an `assertNotNull` on a primitive `boolean`, which asserted nothing — do not reintroduce either.)
- Absence assertions ("no event was fired") must be preceded by **positive controls**: the command reached `COMPLETED` and the computation drained `bulk/done`. Otherwise they pass even when the plugin is entirely broken.
- Tests rely on the actual bulk pipeline running; do not mock `BulkService`.
- Test logging is configured in `src/test/resources/log4j2-test.xml`: package `nuxeo.labs.bafnotification` at DEBUG, root at WARN. Flip the root to DEBUG if you need to see the bulk framework's chatter.

### What is NOT automated

Only these need a live server, and they are documented in `README-MANUAL-TESTING.md` at the repo root: the Studio/Web UI wiring, Kafka rebalance duplication, `auto.offset.reset=earliest` on a real first install, the `stream.sh` CLI procedure, transaction timeout under production load, and multi-node behaviour. Keep that file in sync when you add or remove automated coverage — its first section maps behaviours to test methods. Do not add manual test instructions for anything that `FeaturesRunner` can reach — nearly all of it can.

## Build & release

- No CI: presales projects do not use GitHub Actions. Builds run locally — always run `mvn clean install` before committing code changes. Do not add a workflow without asking.
- Local build: `mvn clean install` at the repo root. Marketplace zip lands at `nuxeo-labs-baf-notification-package/target/nuxeo-labs-baf-notification-package-<version>.zip`.
- Single test: `mvn -pl nuxeo-labs-baf-notification-core test -Dtest=TestBulkActionDoneEvent#testEventFiredOnCompletion` (etc).
- The Marketplace package declares `<installer restart="true"/>` and `<uninstaller restart="true"/>` — installing/removing the package requires a server restart. `production-state="testing"` and `supported="false"` are intentional; don't flip them.
- Release: run the `nuxeo-release-plugin` command (do not hand-edit versions in the three POMs).
