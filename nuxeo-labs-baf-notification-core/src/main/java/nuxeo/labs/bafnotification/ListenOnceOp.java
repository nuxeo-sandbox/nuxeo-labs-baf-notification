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

import org.nuxeo.ecm.automation.core.Constants;
import org.nuxeo.ecm.automation.core.annotations.Context;
import org.nuxeo.ecm.automation.core.annotations.Operation;
import org.nuxeo.ecm.automation.core.annotations.OperationMethod;
import org.nuxeo.ecm.automation.core.annotations.Param;

/**
 * Registers a one-shot {@code bulkActionDone} subscription for a specific bulk command.
 * <p>
 * When the targeted command completes (or aborts), the {@code bulkActionDone} event is fired
 * regardless of the static action-name filter, and the subscription is consumed atomically.
 * The event is delivered exactly once per registration.
 * <p>
 * Typical chain pattern (Studio):
 *
 * <pre>
 * Bulk.RunAction(...)                                  &rarr; outputs a BulkStatus
 * Context.SetVar(name="cmdId", value=@{ChangeableDocument.id})  // capture status.id
 * BAFNotification.ListenOnce(commandId=@{cmdId})
 * </pre>
 *
 * @since 2025.2
 */
@Operation(id = ListenOnceOp.ID, category = Constants.CAT_SERVICES, //
        label = "BAF Notification: Listen Once", //
        addToStudio = true, //
        description = "Register a one-shot bulkActionDone subscription for a specific bulk command. "
                + "The event will fire exactly once when the command completes or aborts, regardless "
                + "of the static action-name filter, and the subscription is then consumed. "
                + "Idempotent: registering the same commandId twice yields a single fire. "
                + "If the command's action is already covered by the static filter, this is a no-op "
                + "(a WARN is logged).")
public class ListenOnceOp {

    public static final String ID = "BAFNotification.ListenOnce";

    @Context
    protected BAFNotificationService service;

    @Param(name = "commandId", required = true, //
            description = "The bulk command id returned by BulkService.submit(...) "
                    + "(or by Bulk.RunAction as BulkStatus.id).")
    protected String commandId;

    @OperationMethod
    public void run() {
        service.registerOneShotForCommand(commandId);
    }
}
