# Manual Testing — `nuxeo-labs-baf-notification`

The plugin's logic is covered by the automated test suite (`mvn test`). This document covers only what a test
harness cannot reach: the Nuxeo Studio / Web UI wiring, and anything that depends on a real Kafka broker.

**Applies to:** Nuxeo LTS 2025.
**You need:** a running Nuxeo server, an Administrator account, Nuxeo Studio access, and `curl`.

---

## 1. Start with the automated suite

```bash
mvn clean install
```

24 tests across 5 classes. They cover the whole notification pipeline, including several defects that are easy to
reintroduce:

| Behaviour verified | Test |
|---|---|
| The event is fired inside a transaction, so listeners can open a `CoreSession` | `TestBulkActionDoneEvent#testListenerRunsInTransactionAndCanUseCoreSession` |
| Events are not left to accumulate on the stream computation thread (memory leak) | `TestBulkActionDoneEvent#testNoEventBundleLeakOnComputationThread` |
| Post-commit and asynchronous listeners are actually notified | `TestBulkActionDoneEvent#testPostCommitListenerIsNotified` |
| The full event payload is delivered, not just a subset | `TestBulkActionDoneEvent#testEventFiredOnCompletion` |
| A custom action's `BulkStatus.setResult(...)` map reaches the listener | `TestBulkActionDoneEvent#testActionResultIsForwardedToTheEvent` |
| The same command can be delivered more than once (at-least-once delivery) | `TestBulkActionDoneStreamSemantics#testReplayRedeliversSameCommandId` |
| Moving the consumer to the end of the stream skips a backlog | `TestBulkActionDoneStreamSemantics#testPositionToEndSkipsBacklog` |
| Lag builds up behind a stopped or slow consumer, then drains | `TestBulkActionDoneStreamSemantics#testLagGrowsWhileComputationStopped` |
| `ConcurrentUpdateException` is propagated so the stream retries the record | `TestBulkActionDoneErrorHandling#testConcurrentUpdateExceptionIsRetried` |
| A `BulkStatus` with no state does not break the event | `TestBulkActionDoneErrorHandling#testNullStateDoesNotBreakTheEvent` |
| An undecodable record is skipped and logged instead of wedging the stream | `TestBulkActionDoneErrorHandling#testPoisonRecordIsSkippedAndLogged` |
| Action-name filtering: fire-all default, union of contributions, unregister | `TestBAFNotificationServiceFilterState` (6 tests) |
| An empty `<actions/>` contribution blocks every event, loudly | `TestBAFNotificationServiceFilterState#testEmptyContributionLogsWarningAndRuntimeMessage` |
| Filtering end to end against a real bulk command | `TestBAFNotificationFiltering` (7 tests) |

Each of these was checked to **fail** when the corresponding fix is reverted, not merely to pass against the current
code. If you change one, re-verify it the same way rather than trusting a green run.

### What remains manual

| Section | Why it cannot be automated |
|---|---|
| [§4 Studio / Web UI smoke test](#4-studio--web-ui-smoke-test) | Requires a Studio project and a browser |
| [§5.1 First install on an existing cluster](#51-first-install-on-an-existing-cluster) | Kafka-specific (`auto.offset.reset`); the harness uses in-memory logs |
| [§5.2 Rebalance duplication](#52-rebalance-duplication) | Needs a real Kafka consumer-group coordinator |
| [§5.3 Transaction timeout](#53-transaction-timeout) | Needs production-like listener load |
| [§5.4 Multi-node](#54-multi-node) | Needs a cluster |

---

## 2. Environment note: streams without Kafka

On a server with `kafka.enabled=false` (the default), Nuxeo keeps its streams **in memory inside the server JVM**.
LTS 2025 supports only `kafka` and `mem` — Chronicle Queue is gone. Consequences:

- Streams are wiped on restart.
- **`stream.sh` cannot see your streams at all**, because it runs in a separate JVM. The `stream.sh` procedure in
  `README.md` applies to Kafka deployments only.
- Use the management REST API instead — it runs in-process and works either way.

```bash
export NX="http://localhost:8080/nuxeo"
export NX_USER="Administrator"
export NX_PASSWORD="<your-admin-password>"     # set this; do not commit it
export AUTH="-u ${NX_USER}:${NX_PASSWORD}"
export NXLOG="$NUXEO_HOME/log/server.log"

nxlag()   { curl -s $AUTH "$NX/api/v1/management/stream/consumer/position?consumer=bulkActionDoneNotifier&stream=bulk/done"; echo; }
nxtoend() { curl -s $AUTH -X PUT "$NX/api/v1/management/stream/consumer/position/end?consumer=bulkActionDoneNotifier&stream=bulk/done"; echo; }
```

Smoke-test it:

```bash
nxlag     # -> {"stream":"bulk/done","consumer":"bulkActionDoneNotifier","lag":0,...}
```

The management API requires an Administrator (or the user named by `nuxeo.management.api.user`) and must be called on
the port given by `nuxeo.management.api.http.port`, which defaults to the normal HTTP port. A `404` on an otherwise
correct URL means the wrong port; a `403` means insufficient rights.

---

## 3. Before writing any handler

Nuxeo uses the Bulk Action Framework for its own housekeeping — indexing, renditions, fulltext extraction. Without a
filter, the plugin fires `bulkActionDone` for all of them.

A handler that **creates or modifies documents** therefore triggers new bulk commands, which fire new events, which
run the handler again: an unbounded loop that will saturate the server.

Two rules for everything below:

1. **Test handlers only call `Log`.** No `Document.Create`, no `Document.Update`.
2. **Deploy the action filter first** (§4.1), before enabling any handler.

---

## 4. Studio / Web UI smoke test

**Goal:** confirm the event reaches a Studio automation chain with a usable `CoreSession`. This is the one path the
automated suite cannot cover, because it goes through Nuxeo's `OperationEventListener` rather than a Java listener.

**Duration:** about 10 minutes.

### 4.1 Action filter — XML Extension

**Studio Modeler → Advanced Settings → XML Extensions → New →** `baf-notification-filter`

```xml
<extension target="nuxeo.labs.baf.notification.service" point="configuration">
  <actions>
    <action>setProperties</action>
  </actions>
</extension>
```

Deploy, then confirm:

```bash
grep "BAF notification filter updated" "$NXLOG" | tail -1
# -> "...fired only for actions: [setProperties]"
```

### 4.2 Register the event — Registries

**Studio Modeler → Settings → Registries → Events → Add**

- **ID:** `bulkActionDone`
- **Label:** `Bulk Action Done`

Without this the event will not appear in the Event Handler dropdown.

### 4.3 Handler chain — Automation Scripting

**Studio Modeler → Automation → Automation Scripting → New →** `BAFNotifSmoke`

```javascript
/* Smoke test for bulkActionDone. Logs one line and probes whether a CoreSession
   can be opened. Only calls Log - never create/update documents here. */
function run(input, params) {

  // The event context carries no principal, so we must log in.
  Auth.LoginAs(null, {});

  var ec = ctx.Event.getContext();
  var commandId = ec.getProperty("commandId");
  var action = ec.getProperty("action");
  var state = ec.getProperty("state");
  var repo = ec.getProperty("repository");

  var marker = "none";
  try {
    var ap = ec.getProperty("actionParams");
    if (ap) {
      var v = ap.get("dc:source");
      if (v) { marker = v; }
    }
  } catch (e) {
    marker = "unreadable";
  }

  /* THE PROBE: Repository.Query needs a CoreSession, which needs an active
     transaction. If the event were fired outside a transaction this would throw
     "Cannot use a session outside a transaction". */
  var queryOk = "no";
  try {
    Repository.Query(null, { 'query': "SELECT * FROM Document WHERE ecm:primaryType = 'Domain'" });
    queryOk = "yes";
  } catch (e) {
    queryOk = "FAILED: " + e;
  }

  Log(null, {
    'category': 'BAFNOTIF',
    'level': 'warn',
    'message': 'BAFNOTIF|commandId=' + commandId + '|action=' + action
             + '|state=' + state + '|repo=' + repo
             + '|marker=' + marker + '|queryOk=' + queryOk
  });
}
```

### 4.4 Load generator — Automation Scripting

**Studio Modeler → Automation → Automation Scripting → New →** `BAFNotifLoad`

```javascript
/* Submits N setProperties bulk commands, each with a unique marker.
   NEVER call this from inside a bulkActionDone handler. */
function run(input, params) {
  var count = params.count ? parseInt(params.count, 10) : 1;
  var tag = params.tag ? params.tag : "run";
  var query = "SELECT * FROM Document WHERE ecm:primaryType = 'Domain' AND ecm:isVersion = 0";

  for (var i = 0; i < count; i++) {
    Bulk.RunAction(null, {
      'query': query,
      'action': 'setProperties',
      'parameters': '{"dc:source":"bafnotif-' + tag + '-' + i + '"}'
    });
  }
}
```

### 4.5 Event Handler

**Studio Modeler → Automation → Event Handlers → New**

| Field | Value |
|---|---|
| ID | `onBulkActionDoneSmoke` |
| Chain to call | `BAFNotifSmoke` |
| Events | `bulkActionDone` |
| Document filters | *(leave all empty)* |
| User filters | *(leave all empty)* |

> **Leave every filter empty — document filters *and* user filters.** `bulkActionDone` carries no source document
> and no principal, and the two families fail differently:
>
> - **Document filters** (doctype, facet, lifecycle, path, attribute) evaluate against `null` and **silently
>   discard** the event.
> - **User filters** (*is member of group*, *is administrator*) evaluate against a `null` principal and throw a
>   `NullPointerException` that Nuxeo swallows — the chain **never runs** and the only trace is a stack trace in
>   `server.log`.
>
> If your handler never fires, check this first. Filter inside the chain instead, on `action` / `username`.

Deploy the Studio project.

### 4.6 Run it

```bash
WC=$(wc -l < "$NXLOG")

curl -s $AUTH -X POST "$NX/api/v1/automation/javascript.BAFNotifLoad" \
     -H "Content-Type: application/json" \
     -d '{"params":{"count":1,"tag":"smoke"}}'

sleep 5
tail -n +$WC "$NXLOG" | grep "BAFNOTIF|"
```

**Expected** — a single line ending in `queryOk=yes`:

```
WARN  BAFNOTIF - BAFNOTIF|commandId=3f2a...|action=setProperties|state=COMPLETED|repo=default|marker=bafnotif-smoke-0|queryOk=yes
```

| Field | Expected | Meaning |
|---|---|---|
| `queryOk` | `yes` | A `CoreSession` was opened successfully from the handler |
| `action` | `setProperties` | The filter let the event through |
| `state` | `COMPLETED` | Final state decoded correctly |
| `repo` | your repository name | The originating `BulkCommand` was resolved |
| `marker` | `bafnotif-smoke-0` | `actionParams` round-tripped correctly |

**If you see this instead**, the event was fired without a transaction:

```
BAFNOTIF|...|queryOk=FAILED: org.nuxeo.ecm.core.api.NuxeoException: Cannot use a session outside a transaction
```

**If nothing is logged at all**, check in order: `nxlag` (a non-zero lag means the consumer is stopped), the filter
line from §4.1, that the Event Handler has no document filters, and the Registries entry from §4.2.

### 4.7 Optional — post-commit handler

Duplicate the handler as `onBulkActionDonePostCommit` with **Execute asynchronously / post-commit** checked, pointing
at the same chain. Re-run §4.6 and confirm you now get **two** log lines. This exercises Nuxeo's
`PostCommitOperationEventListener`, the Studio counterpart of the automated post-commit test.

---

## 5. Kafka and cluster checklist

Run these on a Kafka-backed, ideally multi-node environment before going to production. None can be reproduced on a
single dev server with in-memory streams.

### 5.1 First install on an existing cluster

1. Take a Nuxeo cluster on Kafka that has been running long enough to accumulate `bulk/done` history.
2. Install the plugin **with no listener enabled**.
3. Restart and count:
   ```bash
   grep -c "Firing bulkActionDone event" server.log
   ```
   **Expect a burst** — one per retained record. Nuxeo always sets `auto.offset.reset=earliest`, so a brand-new
   consumer group starts at the oldest retained offset, not at the tail.
4. Verify the mitigation documented in `README.md` works on real Kafka:
   ```bash
   ./bin/stream.sh position --log-name bulk/done --group bulkActionDoneNotifier --to-end
   ```
   Confirm the burst does not recur on the next restart.
5. Confirm replayed events older than one hour carry `repository=null` and `actionParams={}` — that is the bulk
   command record expiring from the key-value store (`COMPLETED_TTL_SECONDS`).

### 5.2 Rebalance duplication

1. Deploy a listener that deliberately blocks for longer than `kafka.max.poll.interval.ms`.
2. Submit bulk commands.
3. Confirm the consumer is evicted from the group, the record is redelivered, and a **duplicate** `bulkActionDone`
   fires for the same `commandId`.

This validates the idempotency guidance in `README.md`. The automated suite proves the same property by rewinding the
consumer explicitly; only a real broker can prove it via eviction.

### 5.3 Transaction timeout

The plugin opens a transaction around the event. Confirm the default transaction timeout is adequate for your real
listener workload, and raise `nuxeo.transaction.timeout` if your listeners legitimately need longer.

### 5.4 Multi-node

With two or more nodes, confirm each `bulk/done` record is handled by exactly one node — the consumer group should
partition the work, not duplicate it across nodes.

---

## 6. Cleanup

**Studio** — delete `onBulkActionDoneSmoke`, `onBulkActionDonePostCommit`, `BAFNotifSmoke` and `BAFNotifLoad`. Keep
the `bulkActionDone` Registries entry, and set `baf-notification-filter` to the action list you actually want.

**Server:**

```bash
nxtoend    # skip any backlog left by testing
nxlag      # -> lag: 0
```

With in-memory streams, restarting the server also clears everything.

---

## Appendix — Quick reference

```bash
# Lag, and skip to the tail
curl -s $AUTH "$NX/api/v1/management/stream/consumer/position?consumer=bulkActionDoneNotifier&stream=bulk/done"
curl -s $AUTH -X PUT "$NX/api/v1/management/stream/consumer/position/end?consumer=bulkActionDoneNotifier&stream=bulk/done"

# Stop and start (a position change requires the consumer to be stopped first)
curl -s $AUTH -X PUT "$NX/api/v1/management/stream/consumer/stop?consumer=bulkActionDoneNotifier"
curl -s $AUTH -X PUT "$NX/api/v1/management/stream/consumer/start?consumer=bulkActionDoneNotifier"

# Dump raw records
curl -s $AUTH "$NX/api/v1/management/stream/cat?stream=bulk/done&rewind=10&limit=10&timeout=5000"
```

```bash
grep "BAF notification filter updated" "$NXLOG" | tail -1   # effective action filter
grep "effective action list is empty" "$NXLOG"              # empty-contribution misconfiguration
grep "Cannot use a session outside a transaction" "$NXLOG"  # listener could not open a session
grep "dropped the bulk/done record" "$NXLOG"                # record abandoned after retries
grep -A5 "EventHandler.isEnabled" "$NXLOG"                  # handler has a user filter -> NPE, chain never ran
```

| Thing | Value |
|---|---|
| Event name | `bulkActionDone` |
| Computation / consumer group | `bulkActionDoneNotifier` |
| Stream | `bulk/done` (single partition) |
| Extension target | `nuxeo.labs.baf.notification.service`, point `configuration` |
