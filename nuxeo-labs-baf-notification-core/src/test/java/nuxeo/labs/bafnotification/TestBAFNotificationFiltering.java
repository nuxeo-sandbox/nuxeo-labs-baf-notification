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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.time.Duration;

import jakarta.inject.Inject;

import nuxeo.labs.bafnotification.TestBulkActionDoneEvent.TestBulkActionDoneListener;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.bulk.BulkService;
import org.nuxeo.ecm.core.bulk.CoreBulkFeature;
import org.nuxeo.ecm.core.bulk.message.BulkCommand;
import org.nuxeo.ecm.core.bulk.message.BulkStatus;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.lib.stream.log.Name;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.stream.StreamService;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.test.runner.TransactionalFeature;

/**
 * Verifies the action-name filtering performed by {@link BAFNotificationService}.
 * <p>
 * Deploys a contribution that lists an action name that is NOT used by the test command, then a second contribution
 * that DOES match, and asserts the event is only fired when an action name from the union is matched.
 * <p>
 * The "no event fired" assertions are guarded by positive controls (the bulk command really completed, and the
 * notification computation really drained {@code bulk/done}). Without them an absence assertion would pass for any
 * reason at all, including the plugin being completely broken.
 *
 * @since 2025.1
 */
@RunWith(FeaturesRunner.class)
@Features({ CoreFeature.class, CoreBulkFeature.class })
@Deploy("nuxeo.labs.baf.notification.nuxeo-labs-baf-notification-core")
@Deploy("nuxeo.labs.baf.notification.nuxeo-labs-baf-notification-core.tests:OSGI-INF/test-listener-contrib.xml")
@Deploy("nuxeo.labs.baf.notification.nuxeo-labs-baf-notification-core.tests:OSGI-INF/test-filter-nomatch-contrib.xml")
public class TestBAFNotificationFiltering {

    protected static final String ALL_DOCS_QUERY =
            "SELECT * FROM Document WHERE ecm:isVersion = 0 AND ecm:isTrashed = 0";

    @Inject
    protected CoreSession session;

    @Inject
    protected BulkService bulkService;

    @Inject
    protected TransactionalFeature txFeature;

    /** @see TestBulkActionDoneEvent#awaitBulkActionDone(String) */
    protected void awaitBulkActionDone(String commandId) throws InterruptedException {
        assertTrue("Bulk command %s did not reach a final state within 30s".formatted(commandId),
                bulkService.await(commandId, Duration.ofSeconds(30)));
        assertTrue("Computation %s did not drain bulk/done within 30s".formatted(
                BulkActionDoneComputation.COMPUTATION_NAME),
                Framework.getService(StreamService.class)
                         .await(Name.ofUrn("bulk/done"),
                                 Name.ofUrn(BulkActionDoneComputation.COMPUTATION_NAME), Duration.ofSeconds(30)));
        // Positive control: the command really ran, so an absence assertion below means the filter worked.
        assertEquals(BulkStatus.State.COMPLETED, bulkService.getStatus(commandId).getState());
    }

    protected String submitSetProperties(String docName, String description) {
        var doc = session.createDocumentModel("/", docName, "File");
        session.createDocument(doc);
        txFeature.nextTransaction();

        var command = new BulkCommand.Builder("setProperties", ALL_DOCS_QUERY, session.getPrincipal().getName())
                                     .repository(session.getRepositoryName())
                                     .param("dc:description", (Serializable) description)
                                     .build();
        return bulkService.submit(command);
    }

    @Test
    public void testServiceHasContributions() {
        var service = Framework.getService(BAFNotificationService.class);
        assertNotNull(service);
        assertTrue(service.hasContributions());
        assertFalse(service.shouldNotify("setProperties"));
        assertTrue(service.shouldNotify("noSuchActionEverFired"));
    }

    @Test
    public void testShouldNotifyRejectsNullWhenFiltering() {
        var service = Framework.getService(BAFNotificationService.class);
        assertTrue(service.hasContributions());
        assertFalse("A null action name cannot match a configured filter", service.shouldNotify(null));
    }

    @Test
    public void testMatchIsCaseSensitive() {
        var service = Framework.getService(BAFNotificationService.class);
        assertTrue(service.shouldNotify("noSuchActionEverFired"));
        assertFalse("Action names are matched case-sensitively", service.shouldNotify("NOSUCHACTIONEVERFIRED"));
        assertFalse("Action names are matched case-sensitively", service.shouldNotify("nosuchactioneverfired"));
    }

    @Test
    public void testGetConfiguredActionsIsImmutable() {
        var service = Framework.getService(BAFNotificationService.class);
        var actions = service.getConfiguredActions();
        assertEquals(1, actions.size());
        assertTrue(actions.contains("noSuchActionEverFired"));
        assertThrows(UnsupportedOperationException.class, () -> actions.add("somethingElse"));
    }

    @Test
    @Deploy("nuxeo.labs.baf.notification.nuxeo-labs-baf-notification-core.tests:OSGI-INF/test-filter-match-contrib.xml")
    public void testContributionsAreMergedAsUnion() {
        var service = Framework.getService(BAFNotificationService.class);
        assertEquals(2, service.getConfiguredActions().size());
        assertTrue(service.shouldNotify("setProperties"));
        assertTrue(service.shouldNotify("noSuchActionEverFired"));
        assertFalse(service.shouldNotify("csvExport"));
    }

    @Test
    public void testEventNotFiredWhenActionFilteredOut() throws InterruptedException {
        TestBulkActionDoneListener.reset();

        // setProperties is NOT in the deployed contribution -> no event must fire.
        var commandId = submitSetProperties("filterDoc1", "filtered out");
        awaitBulkActionDone(commandId);

        assertTrue("No bulkActionDone event must be fired for a filtered-out action",
                TestBulkActionDoneListener.forCommand(commandId).isEmpty());
    }

    @Test
    @Deploy("nuxeo.labs.baf.notification.nuxeo-labs-baf-notification-core.tests:OSGI-INF/test-filter-match-contrib.xml")
    public void testEventFiredWhenActionMatchesUnion() throws InterruptedException {
        TestBulkActionDoneListener.reset();

        var commandId = submitSetProperties("filterDoc2", "matched");
        awaitBulkActionDone(commandId);

        var capture = TestBulkActionDoneListener.forCommand(commandId)
                                                .orElseThrow(() -> new AssertionError(
                                                        "Expected a bulkActionDone event for command " + commandId));
        assertEquals("setProperties", capture.event().getContext().getProperty("action"));
    }
}
