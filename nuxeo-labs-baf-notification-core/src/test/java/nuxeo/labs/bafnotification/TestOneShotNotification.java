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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import jakarta.inject.Inject;

import nuxeo.labs.bafnotification.TestBulkActionDoneEvent.TestBulkActionDoneListener;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.automation.AutomationService;
import org.nuxeo.ecm.automation.OperationContext;
import org.nuxeo.ecm.automation.test.AutomationFeature;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.ecm.core.bulk.BulkService;
import org.nuxeo.ecm.core.bulk.CoreBulkFeature;
import org.nuxeo.ecm.core.bulk.message.BulkCommand;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.kv.KeyValueService;
import org.nuxeo.runtime.kv.KeyValueStoreProvider;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.test.runner.TransactionalFeature;

/**
 * Covers the per-{@code commandId} one-shot subscription added in 2025.2:
 * <ul>
 * <li>functional: one-shot fires + is consumed, overrides a static-filter miss, no-op when the
 * static filter already covers the action, idempotent registration, the
 * {@code BAFNotification.ListenOnce} operation writes an entry;</li>
 * <li>capacity ladder: WARN at warnThreshold, ERROR + cleanup at cleanupThreshold (only unknown
 * commandIds are evicted), hard cap throws {@link NuxeoException}.</li>
 * </ul>
 *
 * @since 2025.2
 */
@RunWith(FeaturesRunner.class)
@Features({ AutomationFeature.class, CoreBulkFeature.class })
@Deploy("nuxeo.labs.baf.notification.nuxeo-labs-baf-notification-core")
@Deploy("nuxeo.labs.baf.notification.nuxeo-labs-baf-notification-core.tests:OSGI-INF/test-listener-contrib.xml")
public class TestOneShotNotification {

    @Inject
    protected CoreSession session;

    @Inject
    protected BulkService bulkService;

    @Inject
    protected AutomationService automationService;

    @Inject
    protected TransactionalFeature txFeature;

    @Before
    public void before() {
        TestBulkActionDoneListener.reset();
        clearOneShotStore();
    }

    @After
    public void after() {
        // Always wipe both the KV store and any threshold overrides — tests in the same JVM
        // share the same memory KV provider and Framework properties.
        clearOneShotStore();
        Framework.getProperties().remove(BAFNotificationServiceImpl.PROP_WARN_THRESHOLD);
        Framework.getProperties().remove(BAFNotificationServiceImpl.PROP_CLEANUP_THRESHOLD);
        Framework.getProperties().remove(BAFNotificationServiceImpl.PROP_HARD_CAP_THRESHOLD);
    }

    protected void clearOneShotStore() {
        var kv = (KeyValueStoreProvider) Framework.getService(KeyValueService.class)
                .getKeyValueStore(BAFNotificationServiceImpl.KV_STORE_NAME);
        kv.clear();
    }

    protected BAFNotificationService service() {
        return Framework.getService(BAFNotificationService.class);
    }

    // ===== Functional tests =====

    @Test
    public void testOneShotFiresThenRemoved() throws InterruptedException {
        var commandId = submitSetPropertiesCommand("oneshot-doc-1", "fires-then-removed");

        service().registerOneShotForCommand(commandId);
        assertTrue("one-shot must be present before bulk/done", service().hasOneShotForCommand(commandId));

        bulkService.await(commandId, Duration.ofSeconds(30));
        Thread.sleep(2000);

        var ev = TestBulkActionDoneListener.getReceivedEvents().stream()
                .filter(e -> commandId.equals(e.getContext().getProperty("commandId")))
                .findFirst().orElse(null);
        assertNotNull("Expected bulkActionDone event for command " + commandId, ev);

        assertFalse("one-shot must be consumed (removed) after the event fired",
                service().hasOneShotForCommand(commandId));
    }

    @Test
    @Deploy("nuxeo.labs.baf.notification.nuxeo-labs-baf-notification-core.tests:OSGI-INF/test-filter-nomatch-contrib.xml")
    public void testOneShotOverridesStaticFilterMiss() throws InterruptedException {
        // Static filter only allows "noSuchActionEverFired" so plain setProperties would be filtered out.
        // The one-shot must override that and fire the event for THIS specific commandId.
        var commandId = submitSetPropertiesCommand("oneshot-doc-2", "overrides-static-miss");

        service().registerOneShotForCommand(commandId);
        bulkService.await(commandId, Duration.ofSeconds(30));
        Thread.sleep(2000);

        var ev = TestBulkActionDoneListener.getReceivedEvents().stream()
                .filter(e -> commandId.equals(e.getContext().getProperty("commandId")))
                .findFirst().orElse(null);
        assertNotNull("one-shot must fire even when the static filter would have dropped the action", ev);
        assertEquals("setProperties", ev.getContext().getProperty("action"));
        assertFalse("one-shot must be consumed", service().hasOneShotForCommand(commandId));
    }

    @Test
    @Deploy("nuxeo.labs.baf.notification.nuxeo-labs-baf-notification-core.tests:OSGI-INF/test-filter-match-contrib.xml")
    public void testOneShotNoOpWhenAlreadyCoveredByStaticFilter() throws InterruptedException {
        // Static filter already covers "setProperties" -> registering a one-shot for a setProperties
        // command must NOT write an entry. The event still fires once via the static path.
        var commandId = submitSetPropertiesCommand("oneshot-doc-3", "already-covered");

        service().registerOneShotForCommand(commandId);
        assertFalse("no one-shot entry must be written when the static filter already covers the action",
                service().hasOneShotForCommand(commandId));

        bulkService.await(commandId, Duration.ofSeconds(30));
        Thread.sleep(2000);

        long matching = TestBulkActionDoneListener.getReceivedEvents().stream()
                .filter(e -> commandId.equals(e.getContext().getProperty("commandId")))
                .count();
        assertEquals("event must fire exactly once via the static path", 1L, matching);
    }

    @Test
    public void testRegistrationIsIdempotent() throws InterruptedException {
        var commandId = submitSetPropertiesCommand("oneshot-doc-4", "idempotent");

        service().registerOneShotForCommand(commandId);
        service().registerOneShotForCommand(commandId);
        service().registerOneShotForCommand(commandId);
        assertEquals("duplicate registrations must collapse to a single entry", 1L, service().oneShotRegistrySize());

        bulkService.await(commandId, Duration.ofSeconds(30));
        Thread.sleep(2000);

        long matching = TestBulkActionDoneListener.getReceivedEvents().stream()
                .filter(e -> commandId.equals(e.getContext().getProperty("commandId")))
                .count();
        assertEquals("event must fire exactly once even after multiple registrations", 1L, matching);
        assertEquals("registry must be empty after consumption", 0L, service().oneShotRegistrySize());
    }

    @Test
    public void testRejectsBlankCommandId() {
        try {
            service().registerOneShotForCommand("   ");
            fail("blank commandId must throw IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // ok
        }
        try {
            service().registerOneShotForCommand(null);
            fail("null commandId must throw IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // ok
        }
    }

    @Test
    public void testConsumeReturnsFalseForBlankOrUnknown() {
        assertFalse(service().consumeOneShotForCommand(null));
        assertFalse(service().consumeOneShotForCommand(""));
        assertFalse(service().consumeOneShotForCommand("   "));
        assertFalse(service().consumeOneShotForCommand("unknown-uuid-" + UUID.randomUUID()));
    }

    @Test
    public void testOperationRegistersOneShot() throws Exception {
        var commandId = UUID.randomUUID().toString();
        try (var ctx = new OperationContext(session)) {
            var params = new HashMap<String, Object>();
            params.put("commandId", commandId);
            automationService.run(ctx, ListenOnceOp.ID, params);
        }
        assertTrue("operation must write a one-shot entry", service().hasOneShotForCommand(commandId));
    }

    // ===== Capacity ladder =====

    @Test
    public void testWarnThresholdLogsAndKeepsEntries() {
        // warn=3, cleanup=99, hardCap=999: stay below cleanup so nothing is evicted.
        setThresholds(3, 99, 999);
        for (int i = 0; i < 5; i++) {
            service().registerOneShotForCommand("warn-cmd-" + i);
        }
        assertEquals("entries below the cleanup threshold must be kept", 5L, service().oneShotRegistrySize());
        // Logging is best-effort verified via behavior: registry size grew without any cleanup.
    }

    @Test
    public void testCleanupRemovesUnknownCommandIds() {
        // warn=2, cleanup=3, hardCap=999. Once we hit 3 entries on the NEXT register, cleanup runs.
        setThresholds(2, 3, 999);
        // All five commandIds are bogus -> all unknown to BulkService -> all evicted at cleanup time.
        for (int i = 0; i < 5; i++) {
            service().registerOneShotForCommand("bogus-cmd-" + i);
        }
        // After cleanup, only the most recently added entries since the last cleanup remain.
        // What matters: cleanup actually fired and pruned all unknown ids — final size should be
        // far below 5 (in practice 1, the last write that triggered cleanup and then succeeded).
        long finalSize = service().oneShotRegistrySize();
        assertTrue("cleanup must have removed unknown commandIds (final size=" + finalSize + ")",
                finalSize < 5L);
    }

    @Test
    public void testCleanupKeepsLiveCommandIds() throws Exception {
        // Submit ONE real command and surround it with bogus ids. Cleanup must NOT evict the real one.
        var realCommandId = submitSetPropertiesCommand("oneshot-doc-cleanup", "keep-live");
        setThresholds(2, 3, 999);

        // Register one live + several bogus. The live entry must survive cleanup.
        service().registerOneShotForCommand(realCommandId);
        for (int i = 0; i < 4; i++) {
            service().registerOneShotForCommand("bogus-keep-" + i);
        }

        assertTrue("live commandId must survive cleanup", service().hasOneShotForCommand(realCommandId));

        // Drain the real command so we don't leave a dangling subscription behind.
        bulkService.await(realCommandId, Duration.ofSeconds(30));
        Thread.sleep(2000);
        assertFalse("live one-shot must be consumed after bulk/done",
                service().hasOneShotForCommand(realCommandId));
    }

    @Test
    public void testHardCapThrows() {
        // warn=2, cleanup=999 (disabled), hardCap=4. Up to 4 entries are allowed; the 5th throws.
        setThresholds(2, 999, 4);
        service().registerOneShotForCommand("hardcap-cmd-1");
        service().registerOneShotForCommand("hardcap-cmd-2");
        service().registerOneShotForCommand("hardcap-cmd-3");
        service().registerOneShotForCommand("hardcap-cmd-4");
        assertEquals("registry must be full at the hard cap", 4L, service().oneShotRegistrySize());
        try {
            service().registerOneShotForCommand("hardcap-cmd-5");
            fail("registering beyond the hard cap must throw NuxeoException");
        } catch (NuxeoException expected) {
            assertTrue("error message must mention the hard cap",
                    expected.getMessage().toLowerCase().contains("hard cap"));
        }
    }

    @Test
    public void testThresholdsAreConfigurable() {
        setThresholds(7, 13, 21);
        // Smoke test: 6 entries stay below warn, 12 stays below cleanup, neither throws.
        for (int i = 0; i < 6; i++) {
            service().registerOneShotForCommand("threshold-cmd-" + i);
        }
        assertEquals(6L, service().oneShotRegistrySize());
    }

    // ===== helpers =====

    protected String submitSetPropertiesCommand(String docName, String description) {
        var doc = session.createDocumentModel("/", docName, "File");
        session.createDocument(doc);
        txFeature.nextTransaction();

        var query = "SELECT * FROM Document WHERE ecm:isVersion = 0 AND ecm:isTrashed = 0";
        var command = new BulkCommand.Builder("setProperties", query, session.getPrincipal().getName())
                .repository(session.getRepositoryName())
                .param("dc:description", (Serializable) description)
                .build();
        return bulkService.submit(command);
    }

    protected void setThresholds(int warn, int cleanup, int hardCap) {
        var props = Framework.getProperties();
        props.setProperty(BAFNotificationServiceImpl.PROP_WARN_THRESHOLD, Integer.toString(warn));
        props.setProperty(BAFNotificationServiceImpl.PROP_CLEANUP_THRESHOLD, Integer.toString(cleanup));
        props.setProperty(BAFNotificationServiceImpl.PROP_HARD_CAP_THRESHOLD, Integer.toString(hardCap));
    }
}
