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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.nuxeo.runtime.model.ComponentInstance;
import org.nuxeo.runtime.model.DefaultComponent;

/**
 * Default {@link BAFNotificationService} implementation.
 * <p>
 * Aggregates every contributed {@link BAFNotificationConfigDescriptor} into a single
 * set of action names (union). Tracks whether at least one contribution was registered
 * so the service can distinguish the "no contribution = fire all" default from an
 * explicit empty filter.
 *
 * @since 2025.1
 */
public class BAFNotificationServiceImpl extends DefaultComponent implements BAFNotificationService {

    private static final Logger log = LogManager.getLogger(BAFNotificationServiceImpl.class);

    public static final String XP_CONFIGURATION = "configuration";

    // Use a concurrent set so reads from the stream computation thread are safe while
    // contributions register/unregister on the runtime thread.
    protected final Set<String> configuredActions = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // Tracks distinct contributions so unregister can flip hasContributions back to false
    // when the last one is removed.
    protected final Set<BAFNotificationConfigDescriptor> contributions = ConcurrentHashMap.newKeySet();

    @Override
    public void registerContribution(Object contribution, String extensionPoint, ComponentInstance contributor) {
        if (XP_CONFIGURATION.equals(extensionPoint) && contribution instanceof BAFNotificationConfigDescriptor desc) {
            contributions.add(desc);
            rebuildActions();
            log.debug("Registered BAF notification configuration: actions={}", desc.getActions());
        }
    }

    @Override
    public void unregisterContribution(Object contribution, String extensionPoint, ComponentInstance contributor) {
        if (XP_CONFIGURATION.equals(extensionPoint) && contribution instanceof BAFNotificationConfigDescriptor desc) {
            contributions.remove(desc);
            rebuildActions();
        }
    }

    protected void rebuildActions() {
        var union = new LinkedHashSet<String>();
        for (var c : contributions) {
            if (c.getActions() != null) {
                for (var name : c.getActions()) {
                    if (name != null && !name.isBlank()) {
                        union.add(name);
                    }
                }
            }
        }
        configuredActions.clear();
        configuredActions.addAll(union);
    }

    @Override
    public boolean shouldNotify(String actionName) {
        // No contribution at all -> default behavior: fire for every action.
        if (contributions.isEmpty()) {
            return true;
        }
        if (actionName == null) {
            return false;
        }
        return configuredActions.contains(actionName);
    }

    @Override
    public Set<String> getConfiguredActions() {
        return Set.copyOf(configuredActions);
    }

    @Override
    public boolean hasContributions() {
        return !contributions.isEmpty();
    }
}
