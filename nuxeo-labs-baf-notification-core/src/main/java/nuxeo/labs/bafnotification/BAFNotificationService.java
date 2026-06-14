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
}
