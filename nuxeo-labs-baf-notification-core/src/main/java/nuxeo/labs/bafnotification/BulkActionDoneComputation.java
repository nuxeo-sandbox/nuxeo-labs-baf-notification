/*
 * (C) Copyright 2026 Nuxeo (http://nuxeo.com/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Thibaud Arguillere
 *     (Code initially generated with the help of OpenCode / Claude Opus)
 */
package nuxeo.labs.bafnotification;

import java.io.Serializable;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.nuxeo.ecm.core.bulk.BulkCodecs;
import org.nuxeo.ecm.core.bulk.BulkService;
import org.nuxeo.ecm.core.event.EventService;
import org.nuxeo.ecm.core.event.impl.EventContextImpl;
import org.nuxeo.ecm.core.event.impl.EventImpl;
import org.nuxeo.lib.stream.computation.AbstractComputation;
import org.nuxeo.lib.stream.computation.ComputationContext;
import org.nuxeo.lib.stream.computation.Record;
import org.nuxeo.runtime.api.Framework;

/**
 * Stream computation that consumes the {@code bulk/done} stream and fires a synchronous
 * {@value #EVENT_NAME} Nuxeo event for each completed or aborted bulk command.
 * <p>
 * The event context carries the following properties:
 * <ul>
 * <li>{@code commandId} - the bulk command identifier</li>
 * <li>{@code action} - the bulk action name (e.g. "setProperties", "csvExport")</li>
 * <li>{@code username} - the user who submitted the command</li>
 * <li>{@code state} - the final state: "COMPLETED" or "ABORTED"</li>
 * <li>{@code processed} - number of documents processed</li>
 * <li>{@code total} - total number of documents in the command</li>
 * <li>{@code errorCount} - number of errors encountered</li>
 * <li>{@code errorCode} - the error code, or 0 if none</li>
 * <li>{@code errorMessage} - the error message, or null if none</li>
 * <li>{@code processingDurationMillis} - total processing duration in ms</li>
 * <li>{@code repository} - the repository the command ran against (null if the command record was already evicted)</li>
 * <li>{@code query} - the NXQL query used to scroll documents (null for non-query scrollers or if evicted)</li>
 * <li>{@code actionParams} - the raw {@code Map<String, Serializable>} from {@link
 *     org.nuxeo.ecm.core.bulk.message.BulkCommand#getParams()} (empty map if the command record was already evicted)</li>
 * </ul>
 *
 * @since 2025.1
 */
public class BulkActionDoneComputation extends AbstractComputation {

    private static final Logger log = LogManager.getLogger(BulkActionDoneComputation.class);

    public static final String COMPUTATION_NAME = "bulkActionDoneNotifier";

    /**
     * The name of the Nuxeo event fired when a bulk action completes or aborts.
     */
    public static final String EVENT_NAME = "bulkActionDone";

    public BulkActionDoneComputation() {
        super(COMPUTATION_NAME, 1, 0);
    }

    @Override
    public void processRecord(ComputationContext context, String inputStreamName, Record record) {
        var codec = BulkCodecs.getStatusCodec();
        var status = codec.decode(record.getData());

        // Filter: only fire the event for action names allowed by BAFNotificationService.
        // When no contribution is registered, the service returns true for every action.
        var notificationService = Framework.getService(BAFNotificationService.class);
        if (notificationService != null && !notificationService.shouldNotify(status.getAction())) {
            log.debug("Skipping {} event (filtered out) for command: {}, action: {}",
                    EVENT_NAME, status.getId(), status.getAction());
            context.askForCheckpoint();
            return;
        }

        log.debug("Firing {} event for command: {}, action: {}, state: {}",
                EVENT_NAME, status.getId(), status.getAction(), status.getState());

        // Look up the originating BulkCommand to expose its repository, query and params.
        // The command record is normally still in the bulk KV store when bulk/done fires,
        // but we guard against eviction so the event is always emitted.
        var cmd = Framework.getService(BulkService.class).getCommand(status.getId());
        String repository = cmd != null ? cmd.getRepository() : null;
        String query = cmd != null ? cmd.getQuery() : null;
        Map<String, Serializable> actionParams = (cmd != null && cmd.getParams() != null)
                ? cmd.getParams() : Map.of();
        if (cmd == null) {
            log.debug("BulkCommand {} no longer available; firing event without command fields", status.getId());
        }

        var eventCtx = new EventContextImpl();
        eventCtx.setProperty("commandId", status.getId());
        eventCtx.setProperty("action", status.getAction());
        eventCtx.setProperty("username", status.getUsername());
        eventCtx.setProperty("state", status.getState().name());
        eventCtx.setProperty("processed", status.getProcessed());
        eventCtx.setProperty("total", status.getTotal());
        eventCtx.setProperty("errorCount", status.getErrorCount());
        eventCtx.setProperty("errorCode", status.getErrorCode());
        eventCtx.setProperty("errorMessage", status.getErrorMessage());
        eventCtx.setProperty("processingDurationMillis", status.getProcessingDurationMillis());
        eventCtx.setProperty("repository", repository);
        eventCtx.setProperty("query", query);
        eventCtx.setProperty("actionParams", (Serializable) actionParams);

        var event = new EventImpl(EVENT_NAME, eventCtx);
        try {
            Framework.getService(EventService.class).fireEvent(event);
        } catch (RuntimeException e) {
            log.error("Error firing {} event for command: {}, action: {}", EVENT_NAME, status.getId(),
                    status.getAction(), e);
        }

        context.askForCheckpoint();
    }
}
