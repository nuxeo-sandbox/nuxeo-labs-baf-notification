# Nuxeo Labs BAF Notification

A Nuxeo plugin that fires an event (`bulkActionDone`) when a Bulk Action Framework (BAF) command completes or aborts.

> [!IMPORTANT]
> By default, it sends the event for all and every action, and Nuxeo spreads _a lot_ of BAF actions, because it uses the BAF even for a single document: Index a document saved by a user, generate a picture views (For example, 10 users uploading 10 images => 100 "recomputeViews" actions), etc. So, in production, without any filter, you may be notified a lot every second, while you very likely need to be notified only for _some_ actions: You may want to avoid running a listener (and/or an automation chain) for no reason.
> 
> So, consider filtering the actions your are listening to, see below, [Filtering Which Actions Trigger the Event](#filtering-which-actions-trigger-the-event).


## The `bulkActionDone` Event

### Event Name

`bulkActionDone`

### Event Properties

The `EventContext` carries the following properties (basically, the `BulkStatus`, plus a few fields pulled from the originating `BulkCommand`):

| Property | Type | Description |
|----------|------|-------------|
| `commandId` | `String` | The unique identifier of the bulk command |
| `action` | `String` | The bulk action name (e.g. `"setProperties"`, `"csvExport"`) |
| `username` | `String` | The user who submitted the command |
| `state` | `String` | The final state: `"COMPLETED"` or `"ABORTED"` |
| `processed` | `long` | Number of documents processed |
| `total` | `long` | Total number of documents in the command |
| `errorCount` | `long` | Number of errors encountered during processing |
| `errorCode` | `int` | Error code, or `0` if none |
| `errorMessage` | `String` | Error message, or `null` if none |
| `processingDurationMillis` | `long` | Total processing duration in milliseconds |
| `repository` | `String` | The repository the command ran against (may be `null` if the command record was already evicted) |
| `query` | `String` | The NXQL query used to scroll documents (may be `null` for non-query scrollers or if evicted) |
| `actionParams` | `Map<String, Serializable>` | The raw params map passed to `BulkCommand.Builder.param(...)`. Empty map if the command record was already evicted. |

`repository`, `query` and `actionParams` are looked up via `BulkService.getCommand(commandId)` at event-firing time. The event is still fired even if the command record has been evicted from the bulk KV store — those three properties will simply be `null` / empty.

> [!WARNING]
> **Do not log `actionParams` or `query` wholesale.**
>
> Depending on the action, `actionParams` may contain sensitive values: for the `automation` action it holds the full operation parameter map, for a custom action it holds whatever the action author put there. `query` may embed document identifiers or values used in NXQL predicates. Log `commandId` and `action`, and extract only the specific parameters you actually need.

### Reading `actionParams` in a listener

```java
// In this example, we act after an "automation" action has finished
// and it is one of our custom plugin operation (MyCustomOp)
var action = (String) event.getContext().getProperty("action");
// org.nuxeo.ecm.automation.core.operations.services.bulk.BulkRunAction
if(AutomationBulkAction.ACTION_NAME.equals(action)) {
  var actionParams = (Map<String, Serializable>) event.getContext().getProperty("actionParams");
  var operationId = (String) actionParams.get("operationId");
  if(MyCustomOp.ID.equals(operationId)) {
    . . .
  }
}
```

> [!IMPORTANT]
> **Per-Document Failures**
> 
> `errorCount` is just a counter — a `long` incremented by the action's computation each time it catches an error while processing a document or a batch. `errorCode` and `errorMessage` carry only one representative error (typically the last one seen), not a list. The event does not include the IDs of the documents that failed.
> 
> `BulkStatus` is a stream record and must stay small and bounded, so the Bulk Action Framework intentionally does not keep per-document failure detail anywhere addressable by `commandId`. For stock actions (`setProperties`, `trash`, `reindex`, `deletion`, `removeProxy`, …) the only place failed document IDs land is `server.log`, with the `commandId` in MDC. There is no programmatic API to enumerate them after the fact.
> 
> If you need the list, the only API-grade path is to author a custom BAF action whose computation collects failed document IDs and calls `status.setResult(Map.of("failedDocIds", List.of(...)))` before publishing the status. That map is round-tripped through the bulk codec, so a custom action can surface failure detail to listeners via `BulkStatus.getResult()` — but it requires owning the action.

## How it Works

The Nuxeo Bulk Action Framework (BAF) processes documents using stream-based computations. When a bulk command finishes (whether successfully or by being aborted), the framework does not fire any Nuxeo event. This means there is no built-in and simple way to reactively listen for bulk action completion.

This plugin bridges that gap by consuming the `bulk/done` stream and firing a standard Nuxeo event, allowing any code to react to bulk action completion using the familiar `EventListener` pattern:

1. The plugin registers a **stream computation** (`BulkActionDoneComputation`) that consumes the `bulk/done` stream
2. The `bulk/done` stream already receives the final `BulkStatus` for every bulk command that completes or aborts — this is built into Nuxeo's `BulkStatusComputation`
3. For each record on that stream, the computation decodes the `BulkStatus` and fires a **`bulkActionDone`** Nuxeo event via `EventService`, inside a transaction it opens itself (a stream computation thread has none by default — see [Threading, Transaction and Delivery Guarantees](#threading-transaction-and-delivery-guarantees))
4. By default the event is fired for **all** bulk actions (setProperties, csvExport, trash, reindex, or any custom action). You can restrict it to a specific subset by contributing to the `nuxeo.labs.baf.notification.service` extension point — see [Filtering Which Actions Trigger the Event](#filtering-which-actions-trigger-the-event).

## Threading, Transaction and Delivery Guarantees

Read this section before writing a listener. It describes constraints that are not obvious and that will bite you in production.

### The event is fired inside a transaction

A Nuxeo stream computation thread carries no transaction. The plugin therefore opens one explicitly around `EventService.fireEvent`. This means your listener:

- runs with an **active transaction**, so it can open a `CoreSession` normally (`CoreInstance.doPrivileged(...)`);
- runs with **no principal and no `CoreSession`** in the event context — `EventContext.getPrincipal()` and `getCoreSession()` are both `null`. Open your own session, or call `Auth.LoginAs` in an automation chain;
- can be a **post-commit or asynchronous** listener as well as an inline one. Both are supported.

### Delivery is at-least-once, not exactly-once

The event is fired from a Nuxeo stream computation, and stream computations checkpoint asynchronously. The same `bulk/done` record can be reprocessed — and the event fired again for the same `commandId` — after a node failure, a restart, or a Kafka consumer rebalance. In addition, the upstream `bulk/done` stream can itself carry more than one record for a single command.

> [!IMPORTANT]
> **Your listener must be idempotent.** Deduplicate on the `commandId` property, which is stable across redeliveries. If your listener has an external side effect — sending an email, calling a webhook, incrementing a counter, appending a report row — keep a short-lived record of the `commandId`s you have already handled (a `KeyValueStore` entry with a TTL is enough) and skip repeats.

If a listener throws, the exception is logged and swallowed, the record is checkpointed, and the event is not replayed for that failure. The one exception is `ConcurrentUpdateException`, which is deliberately propagated so the stream retries the record — another reason listeners must be idempotent.

### Keep listeners fast

`bulk/done` has a **single partition**, so all `bulkActionDone` notifications for the whole cluster are processed sequentially on one thread, and an inline listener runs on that thread.

- Target a few milliseconds. Do not perform long-running work, remote HTTP calls, or `WorkManager.awaitCompletion()` inline.
- A slow listener builds lag on `bulk/done` (a core platform stream, which will trigger monitoring alerts) and can exceed Kafka's `max.poll.interval.ms`, causing a consumer rebalance — which redelivers the record and fires the event **again**.
- **Prefer an asynchronous post-commit listener** (`<listener postCommit="true" async="true">`) for anything non-trivial: it is dispatched through the `WorkManager` and takes the work off the stream thread.

### Installing on a cluster that has already been running

The plugin registers a new stream consumer group (`bulkActionDoneNotifier`). Nuxeo always configures Kafka consumers with `auto.offset.reset=earliest`, so on first start the group begins at the **oldest retained** `bulk/done` offset — not at the tail. On an existing cluster this replays every bulk command still inside the Kafka retention window, firing `bulkActionDone` for each one.

Those replayed events are also degraded: the bulk command record has a TTL of one hour after completion, so `repository`, `query` and `actionParams` will usually be `null` / empty for them.

Before enabling your listeners, move the consumer group to the end of the stream:

```bash
./bin/stream.sh position --log-name bulk/done --group bulkActionDoneNotifier --to-end
```

Or, programmatically:

```java
Framework.getService(StreamService.class)
         .setComputationPositionToEnd(Name.ofUrn("bulkActionDoneNotifier"), Name.ofUrn("bulk/done"));
```

This is only needed on the **first** install. Afterwards the group has committed offsets and resumes from where it left off.

## Filtering Which Actions Trigger the Event

By default the plugin fires the `bulkActionDone` event for every BAF command that completes or aborts. You can narrow it down to a specific subset of actions by contributing to the `configuration` extension point of `nuxeo.labs.baf.notification.service`.

### Semantics

- **No contribution at all** &rarr; event is fired for every action (default).
- **At least one contribution exists** &rarr; event is fired only when the action name is in the union of all contributions.
- **Multiple contributions are merged (union)**. This is intentional: a Studio project and a custom plugin can each contribute their own list and both sets are taken into account.
- Action names are matched **case-sensitively**, exactly as they appear in `BulkStatus.getAction()`.
- Filtered-out actions are silently ignored (a single `DEBUG` log is emitted in `BulkActionDoneComputation`).
- The effective filter is logged at `INFO` every time it changes, so `server.log` always tells you which actions are currently active.

> [!WARNING]
> **An empty contribution blocks everything.**
>
> Contributing `<actions/>`, or a block whose `<action>` entries are all blank, registers a contribution with an empty union — which means **no event is ever fired**. To go back to the fire-all default you must remove the contribution entirely, not empty it. The plugin logs a `WARNING` and raises a runtime message (visible in the admin console) when this happens.

> [!TIP]
> Filtering is also a **security control**, not just a noise control. It restricts which user-submitted bulk commands can trigger your handlers — see the security note in [Event Handler in Nuxeo Studio](#event-handler-in-nuxeo-studio).

### Example: restrict to a single action

```xml
<extension target="nuxeo.labs.baf.notification.service" point="configuration">
  <actions>
    <action>setProperties</action>
  </actions>
</extension>
```

### Example: union of two contributions

Plugin A contributes:

```xml
<extension target="nuxeo.labs.baf.notification.service" point="configuration">
  <actions>
    <action>setProperties</action>
  </actions>
</extension>
```

Plugin B contributes:

```xml
<extension target="nuxeo.labs.baf.notification.service" point="configuration">
  <actions>
    <action>csvExport</action>
  </actions>
</extension>
```

Effective filter: `{setProperties, csvExport}`. The event is fired for both.

## How to Listen for the Event

### Event Handler in Nuxeo Studio

1. Add the `bulkActionDone` event to the [Nuxeo Studio Registries](https://doc.nuxeo.com/studio/registries)
2. Create a new [EventHandler](https://doc.nuxeo.com/studio/event-handlers) for this event.
3. Link it to a [JavaScript automation chain](https://doc.nuxeo.com/nxdoc/automation-scripting).

In this chain, you can access the misc. properties of the even using `ctx.Event.getContext().getProperty()`.

> [!TIP]
> Reminder: As the event is trigger without an explicit user context, do not forget to start your script with a call to `Auth.LoginAs()`.

> [!CAUTION]
> **`Auth.LoginAs(null, {})` performs a system login, with no permission checks.**
>
> The `action`, `query`, `actionParams` and `username` properties are supplied by **whoever submitted the bulk command**. The BAF is exposed over REST, so any authenticated user who can submit a bulk command controls those values. A chain that runs as system and then feeds them into a query, a script, a document update or an outbound request is performing a privileged operation on untrusted input.
>
> - Validate `action` against an allow-list before doing anything.
> - Never interpolate `query` or `actionParams` values into NXQL, into a script, or into an outbound request without validating them first.
> - Restrict the plugin to the actions you own via [Filtering Which Actions Trigger the Event](#filtering-which-actions-trigger-the-event), so your handlers cannot be reached by arbitrary user-submitted commands.
> - Do not treat `username` as an authorization decision: it tells you who submitted the command, not what they are allowed to cause.

```javascript
function run(input, params) {
  
  // Login as Admin/System
  Auth.LoginAs(null, {});

  // Get properties (for the example we get them all
  var eventContext = ctx.Event.getContext();

  var commandId = eventContext.getProperty("commandId");
  var action = eventContext.getProperty("action");
  var state = eventContext.getProperty("state");
  var processed = eventContext.getProperty("processed");
  var total = eventContext.getProperty("total");
  var errorCount = eventContext.getProperty("errorCount");
  var errorCode = eventContext.getProperty("errorCode");
  var errorMessage = eventContext.getProperty("errorMessage");
  var query = eventContext.getProperty("query");
  var repository = eventContext.getProperty("repository");
  var actionParams = eventContext.getProperty("actionParams");

  /* Delivery is at-least-once: this chain may run more than once for the same
     commandId. Guard any non-idempotent side effect (mail, webhook, counter). */

  switch(action) {
    case "setProperties":
      if (state === "COMPLETED") {
        . . .
      } else {
        . . .
      }
      break;

    case "automation":
      if (state === "COMPLETED") {
        . . .
      } else {
        . . .
      }
      break;
  }

}
```



### Java Listener

#### 1. Create the listener class

Register an `EventListener` in your own plugin:

```java
package com.example;

import java.io.Serializable;
import java.util.Map;

import org.nuxeo.ecm.core.api.CoreInstance;
import org.nuxeo.ecm.core.event.Event;
import org.nuxeo.ecm.core.event.EventListener;

public class MyBulkActionDoneListener implements EventListener {

    @Override
    public void handleEvent(Event event) {
        var ctx = event.getContext();
        var commandId = (String) ctx.getProperty("commandId");
        var action = (String) ctx.getProperty("action");
        var state = (String) ctx.getProperty("state");
        var processed = (long) ctx.getProperty("processed");
        var total = (long) ctx.getProperty("total");
        var errorCount = (long) ctx.getProperty("errorCount");
        var errorCode = (int) ctx.getProperty("errorCode");
        var errorMessage = (String) ctx.getProperty("errorMessage");
        var query = (String) ctx.getProperty("query");
        var repository = (String) ctx.getProperty("repository");
        @SuppressWarnings("unchecked")
        var actionParams = (Map<String, Serializable>) ctx.getProperty("actionParams");

        if (!"COMPLETED".equals(state) || !"setProperties".equals(action)) {
            return;
        }

        // Delivery is at-least-once: make sure this is a no-op if we already handled commandId.
        if (alreadyHandled(commandId)) {
            return;
        }

        // There is an active transaction, so a CoreSession can be opened normally.
        // Note the event context carries no principal and no session: open your own.
        if (repository != null) {
            CoreInstance.doPrivileged(repository, session -> {
                // do something
            });
        }

        markHandled(commandId);
    }
}
```

> [!NOTE]
> The event context carries **no principal and no `CoreSession`** — `ctx.getPrincipal()` and `ctx.getCoreSession()` are both `null`. Open your own session as shown above.

#### 2. Register the listener via XML contribution

For anything non-trivial, prefer an **asynchronous post-commit** listener so the work does not run on the stream thread (see [Keep listeners fast](#keep-listeners-fast)):

```xml
<?xml version="1.0"?>
<component name="com.example.my-bulk-listener">

  <extension target="org.nuxeo.ecm.core.event.EventServiceComponent" point="listener">
    <listener name="myBulkActionDoneListener"
        class="com.example.MyBulkActionDoneListener"
        postCommit="true" async="true">
      <event>bulkActionDone</event>
    </listener>
  </extension>

</component>
```

A post-commit listener implements `PostCommitEventListener` (it receives an `EventBundle`, not a single `Event`). If you want a simple inline listener instead — acceptable only when the handler is genuinely fast — drop the `postCommit` and `async` attributes and implement `EventListener` as shown above.

## Important Notes

- The event is fired **asynchronously relative to the bulk command** — it is triggered when the stream computation processes the `bulk/done` record, which may be slightly after the command status transitions to `COMPLETED`/`ABORTED` in the key-value store
- The event is fired **inside a transaction** opened by the plugin, so listeners can use a `CoreSession`. Both inline and post-commit/async listeners are supported
- Delivery is **at-least-once**, not exactly-once — your listener must be idempotent. See [Threading, Transaction and Delivery Guarantees](#threading-transaction-and-delivery-guarantees)
- `bulk/done` has a single partition, so notifications are processed **sequentially on one thread**. Keep inline listeners fast, or use an async post-commit listener
- On a **first install on an existing cluster**, the plugin replays the retained `bulk/done` history unless you move the consumer group to the end of the stream first
- The event is fired for **every** bulk action by default. To restrict to specific action names, contribute to the `nuxeo.labs.baf.notification.service` extension point — see [Filtering Which Actions Trigger the Event](#filtering-which-actions-trigger-the-event). You can still filter further in your listener by reading the `action` property if needed


## How to Build and Deploy

### Build and Deploy Locally

```bash
git clone https://github.com/nuxeo-sandbox/nuxeo-labs-baf-notification
cd nuxeo-labs-baf-notification
mvn clean install
```

To skip unit testing, add `-DskipTests`.

The Marketplace package is generated at:

```
nuxeo-labs-baf-notification-package/target/nuxeo-labs-baf-notification-package-{VERSION}.zip
```

Install it via `nuxeoctl`:

```bash
nuxeoctl mp-install nuxeo-labs-baf-notification-package-{VERSION}.zip
```

### Deploy from Nuxeo Marketplace

This plugin is available as a package on the [Nuxeo Marketplace](https://connect.nuxeo.com/nuxeo/site/marketplace), you can just:

```bash
nuxeoctl mp-install nuxeo-labs-baf-notification

```

## Testing

The plugin ships an automated test suite covering the notification pipeline — transaction handling, delivery
semantics, error handling and the action-name filter:

```bash
mvn test
```

A few things cannot be exercised from a test harness: the Nuxeo Studio / Web UI wiring, and anything that needs a
real Kafka broker (first-install replay, consumer rebalance, multi-node). Those are covered by a step-by-step
runbook: **[README-MANUAL-TESTING.md](README-MANUAL-TESTING.md)**.

## Support

**These features are not part of the Nuxeo Production platform.**

These solutions are provided for inspiration and we encourage customers to use them as code samples and learning resources.

This is a moving project (no API maintenance, no deprecation process, etc.) If any of these solutions are found to be useful for the Nuxeo Platform in general, they will be integrated directly into the platform, not maintained here.

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)

## About Nuxeo

Nuxeo Platform is an open source highly scalable, cloud-native, enterprise content management product with rich multimedia support, written in Java. Data can be stored in both SQL & NoSQL databases.

The development of the Nuxeo Platform is mostly done by Nuxeo employees with an open development model.

The source code, documentation, roadmap, issue tracker, testing, benchmarks are all public.

More information is available at [Hyland/Nuxeo](https://www.hyland.com/en/solutions/products/nuxeo-platform).

