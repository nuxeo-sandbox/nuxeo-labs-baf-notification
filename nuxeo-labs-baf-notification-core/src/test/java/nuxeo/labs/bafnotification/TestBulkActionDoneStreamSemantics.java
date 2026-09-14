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
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.time.Duration;

import jakarta.inject.Inject;

import nuxeo.labs.bafnotification.TestBulkActionDoneEvent.TestBulkActionDoneListener;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.bulk.BulkService;
import org.nuxeo.ecm.core.bulk.CoreBulkFeature;
import org.nuxeo.ecm.core.bulk.message.BulkCommand;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.lib.stream.log.Name;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.stream.StreamService;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.test.runner.TransactionalFeature;

/**
 * Verifies the stream-level delivery semantics of the {@code bulkActionDone} notification: at-least-once delivery,
 * the "seek to end" mitigation documented for a first install on an existing cluster, and the consumer lag that
 * builds up behind a stopped (or slow) listener.
 * <p>
 * These tests stop, rewind and restart the shared {@code bulkActionDoneNotifier} computation, so they live in their
 * own class and every test restores the computation in {@link #restoreComputation()}. A half-finished test must never
 * leave a stopped consumer or a rewound position behind: {@code FeaturesRunner} reuses the runtime across test
 * classes with an identical feature/deploy set, so the damage would leak into the next class.
 *
 * @since 2025.2
 */
@RunWith(FeaturesRunner.class)
@Features({ CoreFeature.class, CoreBulkFeature.class })
@Deploy("nuxeo.labs.baf.notification.nuxeo-labs-baf-notification-core")
@Deploy("nuxeo.labs.baf.notification.nuxeo-labs-baf-notification-core.tests:OSGI-INF/test-listener-contrib.xml")
public class TestBulkActionDoneStreamSemantics {

    protected static final String ALL_DOCS_QUERY =
            "SELECT * FROM Document WHERE ecm:isVersion = 0 AND ecm:isTrashed = 0";

    protected static final Name COMPUTATION = Name.ofUrn(BulkActionDoneComputation.COMPUTATION_NAME);

    protected static final Name DONE_STREAM = Name.ofUrn("bulk/done");

    @Inject
    protected CoreSession session;

    @Inject
    protected BulkService bulkService;

    @Inject
    protected TransactionalFeature txFeature;

    protected StreamService streamService() {
        return Framework.getService(StreamService.class);
    }

    /**
     * Always leave the computation running and positioned at the tail, whatever the test did or how it failed.
     */
    @After
    public void restoreComputation() {
        var streamService = streamService();
        streamService.stopComputation(COMPUTATION);
        streamService.setComputationPositionToEnd(COMPUTATION, DONE_STREAM);
        streamService.restartComputation(COMPUTATION);
    }

    /**
     * Note the argument order: {@code StreamService} declares this as {@code await(computation, stream, ...)} but the
     * implementation is {@code await(stream, computation, ...)} and calls {@code getLag(stream, group)}. Stream first
     * is what actually works.
     */
    protected boolean awaitDrained(Duration duration) throws InterruptedException {
        return streamService().await(DONE_STREAM, COMPUTATION, duration);
    }

    protected long currentLag() {
        return streamService().getLogManager().getLag(DONE_STREAM, COMPUTATION).lag();
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

    protected void awaitBulkActionDone(String commandId) throws InterruptedException {
        assertTrue("Bulk command %s did not reach a final state within 30s".formatted(commandId),
                bulkService.await(commandId, Duration.ofSeconds(30)));
        assertTrue("Computation did not drain bulk/done within 30s", awaitDrained(Duration.ofSeconds(30)));
    }

    /**
     * Delivery is at-least-once, not exactly-once. Rewinding the consumer to the beginning of {@code bulk/done}
     * reproduces exactly what happens on a Kafka rebalance, on a crash before checkpoint, and on a first install
     * against an existing cluster (where {@code auto.offset.reset} is always {@code earliest}).
     * <p>
     * Seeing the same {@code commandId} delivered twice is the expected outcome, and the reason the README tells
     * integrators to make listeners idempotent.
     */
    @Test
    public void testReplayRedeliversSameCommandId() throws InterruptedException {
        TestBulkActionDoneListener.reset();

        var commandId = submitSetProperties("replayDoc", "replay me");
        awaitBulkActionDone(commandId);
        assertEquals("Expected exactly one delivery before the replay", 1,
                TestBulkActionDoneListener.deliveryCount(commandId));

        var streamService = streamService();
        // The position cannot be changed while the consumer is running.
        assertTrue("Could not stop the computation", streamService.stopComputation(COMPUTATION));
        assertTrue("Could not rewind the consumer",
                streamService.setComputationPositionToBeginning(COMPUTATION, DONE_STREAM));
        assertTrue("Could not restart the computation", streamService.restartComputation(COMPUTATION));

        assertTrue("Computation did not drain bulk/done after the replay", awaitDrained(Duration.ofSeconds(30)));

        assertTrue("After a rewind the same commandId must be delivered again (at-least-once delivery)",
                TestBulkActionDoneListener.deliveryCount(commandId) >= 2);
    }

    /**
     * The mitigation documented in the README for installing on a cluster that has already been running: move the
     * consumer group to the tail so the retained history is not replayed.
     */
    @Test
    public void testPositionToEndSkipsBacklog() throws InterruptedException {
        TestBulkActionDoneListener.reset();

        var commandId = submitSetProperties("skipDoc", "skip me");
        awaitBulkActionDone(commandId);
        assertEquals(1, TestBulkActionDoneListener.deliveryCount(commandId));

        var streamService = streamService();
        assertTrue(streamService.stopComputation(COMPUTATION));
        // Rewind to create a backlog, then skip it - this is the install-time procedure.
        assertTrue(streamService.setComputationPositionToBeginning(COMPUTATION, DONE_STREAM));
        assertTrue("Rewinding must create a backlog, otherwise this test proves nothing", currentLag() > 0);

        assertTrue(streamService.setComputationPositionToEnd(COMPUTATION, DONE_STREAM));
        assertEquals("Seeking to the end must clear the backlog", 0, currentLag());

        assertTrue(streamService.restartComputation(COMPUTATION));
        assertTrue(awaitDrained(Duration.ofSeconds(30)));

        assertEquals("No redelivery must happen after seeking to the end", 1,
                TestBulkActionDoneListener.deliveryCount(commandId));
    }

    /**
     * {@code bulk/done} has a single partition and the plugin consumes it with a concurrency of 1, so a listener that
     * blocks the computation thread makes lag build up on a core platform stream. Stopping the computation is a clean
     * stand-in for a listener that is simply too slow.
     */
    @Test
    public void testLagGrowsWhileComputationStopped() throws InterruptedException {
        TestBulkActionDoneListener.reset();

        var streamService = streamService();
        assertTrue(awaitDrained(Duration.ofSeconds(30)));
        assertEquals("Precondition: the consumer must be up to date", 0, currentLag());

        assertTrue(streamService.stopComputation(COMPUTATION));

        var commandId = submitSetProperties("lagDoc", "lag me");
        assertTrue("Bulk command did not complete", bulkService.await(commandId, Duration.ofSeconds(30)));

        // The bulk pipeline finished, but our consumer is stopped: the record is waiting on the stream.
        assertTrue("Lag must build up while the consumer is stopped", currentLag() > 0);
        assertEquals("No event may be delivered while the consumer is stopped", 0,
                TestBulkActionDoneListener.deliveryCount(commandId));

        assertTrue(streamService.restartComputation(COMPUTATION));
        assertTrue("Computation did not catch up after restart", awaitDrained(Duration.ofSeconds(30)));

        assertEquals("The backlog must be consumed once the consumer restarts", 0, currentLag());
        assertTrue("The pending event must be delivered after the restart",
                TestBulkActionDoneListener.deliveryCount(commandId) >= 1);
    }
}
