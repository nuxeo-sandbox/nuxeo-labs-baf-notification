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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.inject.Inject;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.api.CoreInstance;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.bulk.BulkService;
import org.nuxeo.ecm.core.bulk.CoreBulkFeature;
import org.nuxeo.ecm.core.bulk.message.BulkCommand;
import org.nuxeo.ecm.core.bulk.message.BulkStatus;
import org.nuxeo.ecm.core.event.Event;
import org.nuxeo.ecm.core.event.EventBundle;
import org.nuxeo.ecm.core.event.EventListener;
import org.nuxeo.ecm.core.event.PostCommitEventListener;
import org.nuxeo.ecm.core.event.impl.EventServiceImpl;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.lib.stream.log.Name;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.stream.StreamService;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.test.runner.TransactionalFeature;
import org.nuxeo.runtime.transaction.TransactionHelper;

/**
 * Tests that a {@code bulkActionDone} event is fired when a bulk action completes, and that the event is fired in a
 * usable context: inside a transaction, with a working {@code CoreSession}, and reaching post-commit listeners.
 *
 * @since 2025.1
 */
@RunWith(FeaturesRunner.class)
@Features({ CoreFeature.class, CoreBulkFeature.class })
@Deploy("nuxeo.labs.baf.notification.nuxeo-labs-baf-notification-core")
@Deploy("nuxeo.labs.baf.notification.nuxeo-labs-baf-notification-core.tests:OSGI-INF/test-listener-contrib.xml")
public class TestBulkActionDoneEvent {

    protected static final String ALL_DOCS_QUERY =
            "SELECT * FROM Document WHERE ecm:isVersion = 0 AND ecm:isTrashed = 0";

    @Inject
    protected CoreSession session;

    @Inject
    protected BulkService bulkService;

    @Inject
    protected TransactionalFeature txFeature;

    /**
     * Waits for the bulk command to reach a final state <i>and</i> for the plugin's own computation to have consumed
     * the resulting {@code bulk/done} record.
     * <p>
     * {@code BulkService#await} is not sufficient on its own: the bulk status flips to COMPLETED/ABORTED in
     * {@code BulkStatusComputation} <i>before</i> that computation produces the {@code bulk/done} record, so the
     * notification has not necessarily been fired yet. {@code StreamService#await} closes that gap deterministically
     * by waiting for zero lag on our consumer group - no {@code Thread.sleep} guesswork.
     */
    protected void awaitBulkActionDone(String commandId) throws InterruptedException {
        assertTrue("Bulk command %s did not reach a final state within 30s".formatted(commandId),
                bulkService.await(commandId, Duration.ofSeconds(30)));
        assertTrue("Computation %s did not drain bulk/done within 30s".formatted(
                BulkActionDoneComputation.COMPUTATION_NAME),
                Framework.getService(StreamService.class)
                         .await(Name.ofUrn("bulk/done"),
                                 Name.ofUrn(BulkActionDoneComputation.COMPUTATION_NAME), Duration.ofSeconds(30)));
    }

    protected String submitSetProperties(String description) {
        var command = new BulkCommand.Builder("setProperties", ALL_DOCS_QUERY, session.getPrincipal().getName())
                                     .repository(session.getRepositoryName())
                                     .param("dc:description", (Serializable) description)
                                     .param("dc:source", (Serializable) "demo")
                                     .build();
        return bulkService.submit(command);
    }

    @Test
    public void testEventFiredOnCompletion() throws InterruptedException {
        TestBulkActionDoneListener.reset();

        // Create a document so the bulk command has something to process
        var doc = session.createDocumentModel("/", "testDoc", "File");
        doc.setPropertyValue("dc:title", "Test");
        session.createDocument(doc);
        txFeature.nextTransaction();

        var commandId = submitSetProperties("Updated by bulk");
        awaitBulkActionDone(commandId);

        assertEquals(BulkStatus.State.COMPLETED, bulkService.getStatus(commandId).getState());

        var capture = TestBulkActionDoneListener.forCommand(commandId)
                                                .orElseThrow(() -> new AssertionError(
                                                        "Expected a bulkActionDone event for command " + commandId));

        var ctx = capture.event().getContext();
        assertEquals("setProperties", ctx.getProperty("action"));
        assertEquals("COMPLETED", ctx.getProperty("state"));
        assertEquals(session.getRepositoryName(), ctx.getProperty("repository"));
        assertEquals(ALL_DOCS_QUERY, ctx.getProperty("query"));

        @SuppressWarnings("unchecked")
        var actionParams = (Map<String, Serializable>) ctx.getProperty("actionParams");
        assertNotNull("actionParams must be set", actionParams);
        assertEquals("Updated by bulk", actionParams.get("dc:description"));
        assertEquals("demo", actionParams.get("dc:source"));
    }

    /**
     * Regression guard for the defect where the event was fired from the stream computation thread with no ambient
     * transaction. Without a transaction, {@code EventServiceImpl} leaks the event into a thread-local bundle that is
     * never drained, and any listener opening a {@code CoreSession} fails with
     * {@code "Cannot use a session outside a transaction"}.
     */
    @Test
    public void testListenerRunsInTransactionAndCanUseCoreSession() throws InterruptedException {
        TestBulkActionDoneListener.reset();

        var doc = session.createDocumentModel("/", "txDoc", "File");
        session.createDocument(doc);
        txFeature.nextTransaction();

        var commandId = submitSetProperties("tx check");
        awaitBulkActionDone(commandId);

        var capture = TestBulkActionDoneListener.forCommand(commandId)
                                                .orElseThrow(() -> new AssertionError(
                                                        "Expected a bulkActionDone event for command " + commandId));

        assertTrue("The bulkActionDone event must be fired inside an active transaction",
                capture.transactionActive());
        assertNull("A listener must be able to open a CoreSession, but it failed with: " + capture.failure(),
                capture.failure());
        assertEquals("A listener must get a usable CoreSession", session.getRepositoryName(),
                capture.repositoryFromSession());
    }

    /**
     * Regression guard for the defect where post-commit listeners were registered but never invoked, because the
     * event was recorded into a thread-local bundle that no transaction ever committed.
     */
    @Test
    public void testPostCommitListenerIsNotified() throws InterruptedException {
        TestBulkActionDoneListener.reset();
        TestBulkActionDonePostCommitListener.reset();

        var doc = session.createDocumentModel("/", "postCommitDoc", "File");
        session.createDocument(doc);
        txFeature.nextTransaction();

        var commandId = submitSetProperties("post-commit check");
        awaitBulkActionDone(commandId);
        // The listener is asynchronous: let the WorkManager drain before asserting.
        txFeature.nextTransaction();

        assertTrue("An async post-commit listener registered on bulkActionDone must be notified",
                TestBulkActionDonePostCommitListener.sawCommand(commandId));
    }

    /**
     * Regression guard for the memory leak: without a transaction, {@code EventServiceImpl#recordEvent} parks every
     * fired event in a {@code ThreadLocal} bundle on the computation thread that is never drained.
     * <p>
     * The listener probes that thread-local directly (see
     * {@link TestBulkActionDoneListener#probePendingBundleSize()}). Because the inline-listener loop runs before
     * {@code recordEvent}, what it sees is the residue of previous events - which must always be zero. Before the fix
     * this grew by one per bulk command and never came back down.
     */
    @Test
    public void testNoEventBundleLeakOnComputationThread() throws InterruptedException {
        TestBulkActionDoneListener.reset();

        var doc = session.createDocumentModel("/", "leakDoc", "File");
        session.createDocument(doc);
        txFeature.nextTransaction();

        var observations = new ArrayList<Integer>();
        for (int i = 0; i < 5; i++) {
            var commandId = submitSetProperties("leak check " + i);
            awaitBulkActionDone(commandId);
            var capture = TestBulkActionDoneListener.forCommand(commandId)
                                                    .orElseThrow(() -> new AssertionError(
                                                            "Expected a bulkActionDone event for command "
                                                                    + commandId));
            observations.add(capture.pendingBundleSize());
        }

        // Degrade to a skip, not a failure, if the platform internals moved.
        assumeTrue("Could not read EventServiceImpl.threadBundles; skipping the leak probe",
                observations.stream().noneMatch(size -> size == TestBulkActionDoneListener.BUNDLE_SIZE_UNKNOWN));

        assertEquals("Events must not accumulate in the computation thread's event bundle. Observed sizes: "
                + observations, List.of(0, 0, 0, 0, 0), observations);
    }

    /**
     * Inline test listener capturing {@code bulkActionDone} events, keyed by {@code commandId} so that concurrent or
     * parallel tests never observe each other's events.
     *
     * @since 2025.1
     */
    public static class TestBulkActionDoneListener implements EventListener {

        /**
         * What the listener was able to observe and do while handling one event.
         *
         * @param pendingBundleSize number of events already parked in {@code EventServiceImpl.threadBundles} for the
         *            current thread when this event was handled, or {@link #BUNDLE_SIZE_UNKNOWN} if the probe could
         *            not read it.
         * @since 2025.2
         */
        public record Capture(Event event, boolean transactionActive, String repositoryFromSession, String failure,
                int pendingBundleSize) {
        }

        /** Returned by the leak probe when platform internals could not be read. */
        public static final int BUNDLE_SIZE_UNKNOWN = -1;

        private static final Map<String, Capture> capturesByCommandId = new ConcurrentHashMap<>();

        // Counts deliveries per commandId. A Map of captures alone cannot detect a redelivery, since the second
        // capture simply overwrites the first. Used by TestBulkActionDoneStreamSemantics to prove at-least-once.
        private static final Map<String, AtomicInteger> deliveriesByCommandId = new ConcurrentHashMap<>();

        @Override
        public void handleEvent(Event event) {
            if (!BulkActionDoneComputation.EVENT_NAME.equals(event.getName())) {
                return;
            }
            var ctx = event.getContext();
            var commandId = (String) ctx.getProperty("commandId");
            if (commandId == null) {
                return;
            }
            deliveriesByCommandId.computeIfAbsent(commandId, k -> new AtomicInteger()).incrementAndGet();
            boolean transactionActive = TransactionHelper.isTransactionActive();
            int pendingBundleSize = probePendingBundleSize();
            String repositoryFromSession = null;
            String failure = null;
            /*
             * Behave like a real listener: open a CoreSession. This is what fails with
             * "Cannot use a session outside a transaction" when the event is fired outside a transaction, and it is
             * precisely what the previous test listener never exercised.
             */
            try {
                var repositoryName = (String) ctx.getProperty("repository");
                if (repositoryName != null) {
                    repositoryFromSession = CoreInstance.doPrivileged(repositoryName, CoreSession::getRepositoryName);
                }
            } catch (RuntimeException e) {
                failure = e.getMessage();
            }
            capturesByCommandId.put(commandId,
                    new Capture(event, transactionActive, repositoryFromSession, failure, pendingBundleSize));
        }

        /**
         * Reads how many events are currently parked in {@code EventServiceImpl.threadBundles} for <b>this</b> thread.
         * <p>
         * This listener runs on the {@code bulkActionDoneNotifier} stream computation thread, which is exactly the
         * thread that leaked before the fix. {@code recordEvent} pushes a {@code ShallowEvent} into that thread-local
         * bundle and, without a transaction, nothing ever drains it. Because the inline-listener loop runs
         * <i>before</i> {@code recordEvent}, what we observe here is the residue left by previous events: it must
         * always be 0.
         * <p>
         * Uses reflection on platform internals on purpose - there is no public API for this. Returns
         * {@link #BUNDLE_SIZE_UNKNOWN} rather than throwing if the internals move, so a platform refactor degrades
         * this into a skipped test instead of a broken build.
         */
        protected static int probePendingBundleSize() {
            try {
                var field = EventServiceImpl.class.getDeclaredField("threadBundles");
                field.setAccessible(true);
                var threadLocal = (ThreadLocal<?>) field.get(null);
                var composite = threadLocal.get(); // value for the current (computation) thread
                if (composite == null) {
                    return 0;
                }
                var byRepository = composite.getClass().getDeclaredField("byRepository");
                byRepository.setAccessible(true);
                var map = (Map<?, ?>) byRepository.get(composite);
                int total = 0;
                for (var bundle : map.values()) {
                    if (bundle instanceof EventBundle eventBundle) {
                        for (@SuppressWarnings("unused") Event ignored : eventBundle) {
                            total++;
                        }
                    }
                }
                return total;
            } catch (ReflectiveOperationException | RuntimeException e) {
                return BUNDLE_SIZE_UNKNOWN;
            }
        }

        public static Optional<Capture> forCommand(String commandId) {
            return Optional.ofNullable(capturesByCommandId.get(commandId));
        }

        /** Number of times {@code bulkActionDone} was delivered for this command. */
        public static int deliveryCount(String commandId) {
            var counter = deliveriesByCommandId.get(commandId);
            return counter == null ? 0 : counter.get();
        }

        public static void reset() {
            capturesByCommandId.clear();
            deliveriesByCommandId.clear();
        }
    }

    /**
     * Asynchronous post-commit test listener, used to verify that {@code bulkActionDone} reaches post-commit
     * listeners.
     *
     * @since 2025.2
     */
    public static class TestBulkActionDonePostCommitListener implements PostCommitEventListener {

        private static final List<String> commandIds = new CopyOnWriteArrayList<>();

        @Override
        public void handleEvent(EventBundle events) {
            for (Event event : events) {
                if (BulkActionDoneComputation.EVENT_NAME.equals(event.getName())) {
                    var commandId = (String) event.getContext().getProperty("commandId");
                    if (commandId != null) {
                        commandIds.add(commandId);
                    }
                }
            }
        }

        public static boolean sawCommand(String commandId) {
            return commandIds.contains(commandId);
        }

        public static void reset() {
            commandIds.clear();
        }
    }
}
