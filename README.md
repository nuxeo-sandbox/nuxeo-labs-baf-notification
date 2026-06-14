# Nuxeo Labs BAF Notification

A Nuxeo plugin that fires an event (`bulkActionDone`) when a Bulk Action Framework (BAF) command completes or aborts. By default, it sends the event for all and every action, and this [can be configured](#filtering-which-actions-trigger-the-event).

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
3. For each record on that stream, the computation decodes the `BulkStatus` and fires a synchronous **`bulkActionDone`** Nuxeo event via `EventService`
4. By default the event is fired for **all** bulk actions (setProperties, csvExport, trash, reindex, or any custom action). You can restrict it to a specific subset by contributing to the `nuxeo.labs.baf.notification.service` extension point — see [Filtering Which Actions Trigger the Event](#filtering-which-actions-trigger-the-event).

## Filtering Which Actions Trigger the Event

By default the plugin fires the `bulkActionDone` event for every BAF command that completes or aborts. You can narrow it down to a specific subset of actions by contributing to the `configuration` extension point of `nuxeo.labs.baf.notification.service`.

### Semantics

- **No contribution at all** &rarr; event is fired for every action (default).
- **At least one contribution exists** &rarr; event is fired only when the action name is in the union of all contributions.
- **Multiple contributions are merged (union)**. This is intentional: a Studio project and a custom plugin can each contribute their own list and both sets are taken into account.
- Action names are matched **case-sensitively**, exactly as they appear in `BulkStatus.getAction()`.
- Filtered-out actions are silently ignored (a single `DEBUG` log is emitted in `BulkActionDoneComputation`).

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
        var actionParams = (Map<String, Serializable>) event.getContext().getProperty("actionParams");

        // React to the bulk action completion
        if ("COMPLETED".equals(state) && "setProperties".equals(action)) {
            // do something
        }
    }
}
```

#### 2. Register the listener via XML contribution

```xml
<?xml version="1.0"?>
<component name="com.example.my-bulk-listener">

  <extension target="org.nuxeo.ecm.core.event.EventServiceComponent" point="listener">
    <listener name="myBulkActionDoneListener"
        class="com.example.MyBulkActionDoneListener">
      <event>bulkActionDone</event>
    </listener>
  </extension>

</component>
```

## Important Notes

- The event is fired **asynchronously relative to the bulk command** — it is triggered when the stream computation processes the `bulk/done` record, which may be slightly after the command status transitions to `COMPLETED`/`ABORTED` in the key-value store
- The event itself is fired **synchronously** within the computation — your listener runs inline
- The event is fired **exactly once** per bulk command completion. If a listener throws an exception, the error is caught and logged, but the event is **not** retried. This guarantees that listeners will not receive duplicate events, and a misbehaving listener cannot block the stream processing
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

