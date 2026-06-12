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
import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.ecm.core.bulk.BulkService;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.kv.KeyValueService;
import org.nuxeo.runtime.kv.KeyValueStore;
import org.nuxeo.runtime.kv.KeyValueStoreProvider;
import org.nuxeo.runtime.model.ComponentContext;
import org.nuxeo.runtime.model.ComponentInstance;
import org.nuxeo.runtime.model.DefaultComponent;

/**
 * Default {@link BAFNotificationService} implementation.
 * <p>
 * Aggregates every contributed {@link BAFNotificationConfigDescriptor} into a single
 * set of action names (union). Tracks whether at least one contribution was registered
 * so the service can distinguish the "no contribution = fire all" default from an
 * explicit empty filter.
 * <p>
 * One-shot per-{@code commandId} subscriptions are stored in the cluster-safe KV store
 * named {@value #KV_STORE_NAME}. See {@link BAFNotificationService} for the contract.
 *
 * @since 2025.1
 */
public class BAFNotificationServiceImpl extends DefaultComponent implements BAFNotificationService {

    private static final Logger log = LogManager.getLogger(BAFNotificationServiceImpl.class);

    public static final String XP_CONFIGURATION = "configuration";

    /** KV store namespace holding per-commandId one-shot subscriptions. @since 2025.2 */
    public static final String KV_STORE_NAME = "bafNotificationOneShotCommands";

    /** Sentinel value for a registered one-shot. We only care about presence. @since 2025.2 */
    public static final String KV_SENTINEL = "1";

    // ===== Capacity thresholds (configurable via nuxeo.conf) =====

    /** @since 2025.2 */
    public static final String PROP_WARN_THRESHOLD = "nuxeo.labs.baf.notification.oneshot.warnThreshold";

    /** @since 2025.2 */
    public static final String PROP_CLEANUP_THRESHOLD = "nuxeo.labs.baf.notification.oneshot.cleanupThreshold";

    /** @since 2025.2 */
    public static final String PROP_HARD_CAP_THRESHOLD = "nuxeo.labs.baf.notification.oneshot.hardCapThreshold";

    public static final int DEFAULT_WARN_THRESHOLD = 20;

    public static final int DEFAULT_CLEANUP_THRESHOLD = 100;

    public static final int DEFAULT_HARD_CAP_THRESHOLD = 500;

    // Use a concurrent set so reads from the stream computation thread are safe while
    // contributions register/unregister on the runtime thread.
    protected final Set<String> configuredActions = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // Tracks distinct contributions so unregister can flip hasContributions back to false
    // when the last one is removed.
    protected final Set<BAFNotificationConfigDescriptor> contributions = ConcurrentHashMap.newKeySet();

    // Throttle flags so we don't spam the log on every register call once a threshold is crossed.
    // Reset to false when the count drops back below the warn threshold.
    protected volatile boolean warnLogged = false;

    protected volatile boolean errorLogged = false;

    // ===== DefaultComponent lifecycle =====

    @Override
    public void start(ComponentContext context) {
        super.start(context);
        // Read thresholds once at startup, log the effective config so operators can sanity-check.
        log.info("BAF one-shot capacity thresholds: warn={}, cleanup={}, hardCap={}",
                getWarnThreshold(), getCleanupThreshold(), getHardCapThreshold());
    }

    // ===== Filter (action-name) contributions =====

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

    // ===== One-shot per-commandId subscriptions =====

    @Override
    public void registerOneShotForCommand(String commandId) {
        if (commandId == null || commandId.isBlank()) {
            throw new IllegalArgumentException("commandId must not be null or blank");
        }

        // If the command's action is already covered by the static filter, the event will fire
        // anyway via shouldNotify(). Writing a one-shot entry would only count against the
        // capacity thresholds, so log and skip.
        // Note: the lookup may return null if the command was already evicted from the bulk KV
        // store (e.g. registration happens long after submission). In that case we proceed with
        // the registration — better to honor the caller's intent than to silently drop it.
        if (hasContributions()) {
            var bulkService = Framework.getService(BulkService.class);
            var cmd = bulkService != null ? bulkService.getCommand(commandId) : null;
            if (cmd != null && configuredActions.contains(cmd.getAction())) {
                log.warn("Command {} (action={}) is already covered by the static filter; "
                        + "one-shot registration is a no-op", commandId, cmd.getAction());
                return;
            }
        }

        enforceCapacityLimits();

        var kv = getKVStore();
        kv.put(commandId, KV_SENTINEL);
        log.debug("Registered one-shot bulkActionDone subscription for command: {}", commandId);
    }

    @Override
    public boolean consumeOneShotForCommand(String commandId) {
        if (commandId == null || commandId.isBlank()) {
            return false;
        }
        // Atomic compare-and-set: only one consumer (across the cluster) succeeds.
        return getKVStore().compareAndSet(commandId, KV_SENTINEL, null);
    }

    @Override
    public boolean hasOneShotForCommand(String commandId) {
        if (commandId == null || commandId.isBlank()) {
            return false;
        }
        return getKVStore().getString(commandId) != null;
    }

    @Override
    public long oneShotRegistrySize() {
        return getKVStoreProvider().keyStream().count();
    }

    // ===== Capacity enforcement =====

    /**
     * Enforces the three-tier capacity ladder before a new one-shot entry is written.
     * Called from {@link #registerOneShotForCommand(String)}; not from the hot consume path.
     */
    protected void enforceCapacityLimits() {
        long count = oneShotRegistrySize();
        int warn = getWarnThreshold();
        int cleanup = getCleanupThreshold();
        int hardCap = getHardCapThreshold();

        // Self-heal: if entries have drained below the warn threshold, reset the throttles
        // so future incidents are logged again.
        if (count < warn) {
            warnLogged = false;
            errorLogged = false;
        }

        if (count >= hardCap) {
            throw new NuxeoException(String.format(
                    "BAF one-shot registry full (%d entries, hard cap %d). "
                            + "Consume or clear existing entries before registering more, "
                            + "or raise %s.",
                    count, hardCap, PROP_HARD_CAP_THRESHOLD));
        }

        if (count >= cleanup) {
            int removed = cleanupUnknownCommandIds();
            long remaining = oneShotRegistrySize();
            if (!errorLogged) {
                log.error("BAF one-shot registry reached cleanup threshold ({} >= {}). "
                        + "Removed {} entries whose commandId is no longer known to BulkService; "
                        + "{} entries remain.", count, cleanup, removed, remaining);
                errorLogged = true;
            }
            return;
        }

        if (count >= warn && !warnLogged) {
            log.warn("BAF one-shot registry has {} entries (warn threshold {}). "
                    + "Check for forgotten subscriptions, typos in commandId, or commands "
                    + "that never reached bulk/done.", count, warn);
            warnLogged = true;
        }
    }

    /**
     * Iterates the one-shot registry and removes any entry whose commandId is unknown to
     * {@link BulkService}. Safe: entries that still correspond to a live command are kept.
     *
     * @return the number of entries removed
     */
    protected int cleanupUnknownCommandIds() {
        var bulkService = Framework.getService(BulkService.class);
        if (bulkService == null) {
            // Without BulkService we can't tell which IDs are still alive — refuse to drop anything.
            return 0;
        }
        var kv = getKVStore();
        var provider = getKVStoreProvider();
        // Snapshot the keys before mutating, to avoid surprises from lazy streams over live state.
        var keys = provider.keyStream().toList();
        int removed = 0;
        for (var key : keys) {
            if (bulkService.getCommand(key) == null) {
                // compareAndSet(key, sentinel, null) → atomic delete-if-still-sentinel.
                if (kv.compareAndSet(key, KV_SENTINEL, null)) {
                    removed++;
                }
            }
        }
        return removed;
    }

    // ===== KV store helpers =====

    protected KeyValueStore getKVStore() {
        return Framework.getService(KeyValueService.class).getKeyValueStore(KV_STORE_NAME);
    }

    /**
     * @return the same store as {@link #getKVStore()}, cast to the SPI that exposes
     *         {@code keyStream()}. Every Nuxeo-shipped KV backend (memory, MongoDB, Redis, SQL)
     *         implements {@link KeyValueStoreProvider}, this cast is used throughout the platform.
     */
    protected KeyValueStoreProvider getKVStoreProvider() {
        return (KeyValueStoreProvider) getKVStore();
    }

    // ===== Threshold accessors (read from Framework properties with defaults) =====

    protected int getWarnThreshold() {
        return getIntProperty(PROP_WARN_THRESHOLD, DEFAULT_WARN_THRESHOLD);
    }

    protected int getCleanupThreshold() {
        return getIntProperty(PROP_CLEANUP_THRESHOLD, DEFAULT_CLEANUP_THRESHOLD);
    }

    protected int getHardCapThreshold() {
        return getIntProperty(PROP_HARD_CAP_THRESHOLD, DEFAULT_HARD_CAP_THRESHOLD);
    }

    protected int getIntProperty(String key, int defaultValue) {
        var value = Framework.getProperty(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            log.warn("Invalid integer for {} ('{}'), falling back to default {}", key, value, defaultValue);
            return defaultValue;
        }
    }
}
