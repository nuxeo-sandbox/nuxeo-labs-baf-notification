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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.inject.Inject;

import nuxeo.labs.bafnotification.TestBulkActionDoneEvent.TestBulkActionDoneListener;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.api.ConcurrentUpdateException;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.bulk.BulkCodecs;
import org.nuxeo.ecm.core.bulk.BulkService;
import org.nuxeo.ecm.core.bulk.CoreBulkFeature;
import org.nuxeo.ecm.core.bulk.message.BulkCommand;
import org.nuxeo.ecm.core.bulk.message.BulkStatus;
import org.nuxeo.ecm.core.event.Event;
import org.nuxeo.ecm.core.event.EventListener;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.lib.stream.computation.Record;
import org.nuxeo.lib.stream.log.Name;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.stream.StreamService;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.test.runner.LogCaptureFeature;
import org.nuxeo.runtime.test.runner.TransactionalFeature;

/**
 * Covers the error-handling paths of {@link BulkActionDoneComputation}: the deliberate re-throw of
 * {@link ConcurrentUpdateException}, the guard against a {@code null} {@code BulkStatus} state, and the handling of a
 * record that cannot be decoded at all.
 * <p>
 * The last two are reached by appending a synthetic record directly to {@code bulk/done} through
 * {@link org.nuxeo.lib.stream.computation.StreamManager#append}. That is <b>not</b> mocking the bulk service: the
 * real pipeline never produces a malformed record, so injecting one is the only way to exercise these branches. The
 * bulk pipeline itself is still used unmodified for every other test.
 *
 * @since 2025.2
 */
@RunWith(FeaturesRunner.class)
@Features({ CoreFeature.class, CoreBulkFeature.class, LogCaptureFeature.class })
@Deploy("nuxeo.labs.baf.notification.nuxeo-labs-baf-notification-core")
@Deploy("nuxeo.labs.baf.notification.nuxeo-labs-baf-notification-core.tests:OSGI-INF/test-listener-contrib.xml")
public class TestBulkActionDoneErrorHandling {

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

    @Inject
    protected LogCaptureFeature.Result logResult;

    @Before
    public void resetListeners() {
        TestBulkActionDoneListener.reset();
        FailingOnceListener.reset();
    }

    protected StreamService streamService() {
        return Framework.getService(StreamService.class);
    }

    /** Stream first, computation second - see the note in TestBulkActionDoneStreamSemantics. */
    protected boolean awaitDrained(Duration duration) throws InterruptedException {
        return streamService().await(DONE_STREAM, COMPUTATION, duration);
    }

    /** Appends a raw record to {@code bulk/done}, bypassing the bulk pipeline. */
    protected void appendToDoneStream(String key, byte[] data) {
        streamService().getStreamManager().append(DONE_STREAM.getUrn(), Record.of(key, data));
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

    /**
     * {@code EventServiceImpl} re-throws {@link ConcurrentUpdateException} past its own swallowing logic precisely so
     * the caller can retry. The computation must propagate it to the stream policy instead of catching it with the
     * other runtime exceptions - otherwise the record is checkpointed and the notification is lost.
     */
    @Test
    @Deploy("nuxeo.labs.baf.notification.nuxeo-labs-baf-notification-core.tests:"
            + "OSGI-INF/test-failing-listener-contrib.xml")
    public void testConcurrentUpdateExceptionIsRetried() throws InterruptedException {
        FailingOnceListener.arm();
        try {
            var commandId = submitSetProperties("cueDoc", "concurrent update");
            assertTrue("Bulk command did not complete", bulkService.await(commandId, Duration.ofSeconds(30)));
            assertTrue("Computation did not drain bulk/done", awaitDrained(Duration.ofSeconds(30)));

            assertEquals("The listener must be invoked twice: once failing with ConcurrentUpdateException, then once"
                    + " more after the stream policy retried the record", 2,
                    FailingOnceListener.invocationCount(commandId));
            assertTrue("The event must be delivered successfully on the retry",
                    TestBulkActionDoneListener.forCommand(commandId).isPresent());
        } finally {
            FailingOnceListener.disarm();
        }
    }

    /**
     * {@code BulkStatus#state} is {@code @Nullable} and carries no {@code @NotNull} contract. A null state must not
     * blow up before the event is built - that would burn the retry budget and drop the notification entirely.
     */
    @Test
    public void testNullStateDoesNotBreakTheEvent() throws InterruptedException {
        var commandId = UUID.randomUUID().toString();
        var status = new BulkStatus(commandId); // state is left null on purpose
        status.setAction("setProperties");

        appendToDoneStream(commandId, BulkCodecs.getStatusCodec().encode(status));
        assertTrue("Computation did not drain bulk/done", awaitDrained(Duration.ofSeconds(30)));

        var capture = TestBulkActionDoneListener.forCommand(commandId)
                                                .orElseThrow(() -> new AssertionError(
                                                        "The event must still be fired when the status state is null"));
        assertNull("A null state must be exposed as a null property, not an exception",
                capture.event().getContext().getProperty("state"));
        assertEquals("setProperties", capture.event().getContext().getProperty("action"));

        // The command was never submitted through the bulk service, so these are absent - and that is fine.
        assertNull(capture.event().getContext().getProperty("repository"));
        assertEquals(Map.of(), capture.event().getContext().getProperty("actionParams"));
    }

    /**
     * A record that cannot be decoded must not wedge the stream. After the retry budget is exhausted,
     * {@code processFailure} logs it with enough context to be actionable, and {@code continueOnFailure} lets the
     * pipeline move on.
     */
    @Test
    @LogCaptureFeature.FilterOn(loggerName = "nuxeo.labs.bafnotification.BulkActionDoneComputation", logLevel = "ERROR")
    public void testPoisonRecordIsSkippedAndLogged() throws InterruptedException {
        appendToDoneStream("poison", new byte[] { 0x42, 0x13, 0x37, 0x00, 0x7F });

        // 3 retries with 500ms/1s/2s backoff, so allow generous time.
        assertTrue("The poison record must be skipped so the stream keeps flowing",
                awaitDrained(Duration.ofSeconds(60)));

        logResult.assertHasEvent();
        assertTrue("processFailure must name the computation and the stream",
                logResult.getCaughtEventMessages()
                         .stream()
                         .anyMatch(m -> m.contains(BulkActionDoneComputation.COMPUTATION_NAME)
                                 && m.contains("bulk/done")));

        // The pipeline still works afterwards.
        var commandId = submitSetProperties("afterPoisonDoc", "after poison");
        assertTrue(bulkService.await(commandId, Duration.ofSeconds(30)));
        assertTrue(awaitDrained(Duration.ofSeconds(30)));
        assertTrue("A normal command must still be notified after a poison record was skipped",
                TestBulkActionDoneListener.forCommand(commandId).isPresent());
    }

    /**
     * Inline listener that throws {@link ConcurrentUpdateException} the first time it sees a given {@code commandId},
     * then succeeds. Only active while {@link #arm()} has been called, so replays triggered by other tests cannot
     * accidentally trip it.
     *
     * @since 2025.2
     */
    public static class FailingOnceListener implements EventListener {

        private static final Map<String, AtomicInteger> invocations = new ConcurrentHashMap<>();

        private static final Map<String, Boolean> alreadyFailed = new ConcurrentHashMap<>();

        private static volatile boolean armed;

        @Override
        public void handleEvent(Event event) {
            if (!BulkActionDoneComputation.EVENT_NAME.equals(event.getName())) {
                return;
            }
            var commandId = (String) event.getContext().getProperty("commandId");
            if (commandId == null) {
                return;
            }
            invocations.computeIfAbsent(commandId, k -> new AtomicInteger()).incrementAndGet();
            if (armed && alreadyFailed.putIfAbsent(commandId, Boolean.TRUE) == null) {
                throw new ConcurrentUpdateException("Simulated concurrent update for command " + commandId);
            }
        }

        public static int invocationCount(String commandId) {
            var counter = invocations.get(commandId);
            return counter == null ? 0 : counter.get();
        }

        public static void arm() {
            armed = true;
        }

        public static void disarm() {
            armed = false;
        }

        public static void reset() {
            armed = false;
            invocations.clear();
            alreadyFailed.clear();
        }
    }
}
