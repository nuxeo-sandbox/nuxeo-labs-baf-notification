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

import java.util.Set;

/**
 * Service deciding which BAF action completions should trigger the
 * {@code bulkActionDone} Nuxeo event.
 * <p>
 * Filtering semantics:
 * <ul>
 * <li>If no contribution has been registered on the {@code configuration} extension
 * point, {@link #shouldNotify(String)} returns {@code true} for every action name
 * (default behavior — fire for all bulk actions).</li>
 * <li>If at least one contribution exists, only the action names present in the union
 * of all contributions trigger the event.</li>
 * </ul>
 *
 * @since 2025.1
 */
public interface BAFNotificationService {

    /**
     * @param actionName the BAF action name (e.g. {@code "setProperties"}); may be {@code null}.
     * @return {@code true} if the {@code bulkActionDone} event must be fired for this action.
     */
    boolean shouldNotify(String actionName);

    /**
     * @return the union of all contributed action names; empty when no contribution has been
     *         registered.
     */
    Set<String> getConfiguredActions();

    /**
     * @return {@code true} if at least one {@code <actions>} contribution has been registered.
     */
    boolean hasContributions();

    /**
     * Registers a one-shot subscription for a specific bulk command. When {@code bulk/done} ships
     * the {@link org.nuxeo.ecm.core.bulk.message.BulkStatus status} for {@code commandId}, the
     * {@code bulkActionDone} event is fired regardless of the static action-name filter, and the
     * subscription is consumed atomically (i.e. removed) so the event fires exactly once.
     * <p>
     * Idempotent: registering the same {@code commandId} twice is a no-op (single subscription,
     * single fire).
     * <p>
     * If {@code commandId} maps to a command whose action is already covered by the static filter
     * (see {@link #shouldNotify(String)}), this method logs WARN and does NOT write a one-shot
     * entry — the event will be fired anyway by the static path, and writing a redundant entry
     * would only count against the capacity limits.
     * <p>
     * The subscription has no TTL. If {@code commandId} never appears on {@code bulk/done}
     * (e.g. typo, command never submitted, originating node crashed before publishing), the
     * entry persists until the cleanup threshold is reached or it is manually removed from
     * the KV store named {@code bafNotificationOneShotCommands}.
     * <p>
     * Capacity thresholds are enforced inside this call (WARN / cleanup / hard cap). When the
     * hard cap is reached this method throws {@link org.nuxeo.ecm.core.api.NuxeoException}.
     *
     * @param commandId the bulk command identifier returned by
     *        {@link org.nuxeo.ecm.core.bulk.BulkService#submit}
     * @throws IllegalArgumentException if {@code commandId} is {@code null} or blank
     * @throws org.nuxeo.ecm.core.api.NuxeoException if the registry has reached its hard cap
     * @since 2025.2
     */
    void registerOneShotForCommand(String commandId);

    /**
     * Atomically checks whether {@code commandId} has a one-shot subscription and removes it.
     *
     * @param commandId the bulk command identifier
     * @return {@code true} if a subscription was present and has been consumed, {@code false}
     *         otherwise (including for {@code null} or blank input)
     * @since 2025.2
     */
    boolean consumeOneShotForCommand(String commandId);

    /**
     * @param commandId the bulk command identifier
     * @return {@code true} if a one-shot subscription is currently registered for this command.
     *         Intended for testing and observability; the consume operation does NOT go through
     *         this method.
     * @since 2025.2
     */
    boolean hasOneShotForCommand(String commandId);

    /**
     * @return the current number of one-shot subscriptions in the registry. Iterates the
     *         underlying KV store; intended for management and tests, not for hot paths.
     * @since 2025.2
     */
    long oneShotRegistrySize();
}
