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
import static org.junit.Assert.assertTrue;

import java.util.List;

import jakarta.inject.Inject;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.bulk.CoreBulkFeature;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.test.runner.LogCaptureFeature;

/**
 * Drives {@link BAFNotificationServiceImpl} registration and unregistration directly, covering the filter state
 * transitions that the XML-contribution tests cannot reach: removing the last contribution, and a contribution whose
 * effective action list is empty.
 * <p>
 * No filter contribution is deployed on this class, so the service starts from the "no contribution = fire all"
 * default. Every test restores that default in a {@code finally} block because the service is a singleton shared with
 * the other test classes.
 *
 * @since 2025.2
 */
@RunWith(FeaturesRunner.class)
@Features({ CoreFeature.class, CoreBulkFeature.class, LogCaptureFeature.class })
@Deploy("nuxeo.labs.baf.notification.nuxeo-labs-baf-notification-core")
public class TestBAFNotificationServiceFilterState {

    @Inject
    protected LogCaptureFeature.Result logResult;

    protected BAFNotificationServiceImpl service() {
        return (BAFNotificationServiceImpl) Framework.getService(BAFNotificationService.class);
    }

    /** Builds a descriptor without going through XMap; {@code actions} is package-visible. */
    protected static BAFNotificationConfigDescriptor descriptorFor(String... actions) {
        var desc = new BAFNotificationConfigDescriptor();
        desc.actions = List.of(actions);
        return desc;
    }

    protected void register(BAFNotificationConfigDescriptor desc) {
        service().registerContribution(desc, BAFNotificationServiceImpl.XP_CONFIGURATION, null);
    }

    protected void unregister(BAFNotificationConfigDescriptor desc) {
        service().unregisterContribution(desc, BAFNotificationServiceImpl.XP_CONFIGURATION, null);
    }

    @Test
    public void testFireAllWhenNoContribution() {
        var service = service();
        assertFalse(service.hasContributions());
        assertTrue(service.getConfiguredActions().isEmpty());
        assertTrue(service.shouldNotify("setProperties"));
        assertTrue(service.shouldNotify("anythingAtAll"));
        // The fire-all default short-circuits before the null check.
        assertTrue(service.shouldNotify(null));
    }

    @Test
    public void testUnregisteringLastContributionRestoresFireAll() {
        var desc = descriptorFor("setProperties");
        register(desc);
        try {
            assertTrue(service().hasContributions());
            assertTrue(service().shouldNotify("setProperties"));
            assertFalse(service().shouldNotify("csvExport"));
        } finally {
            unregister(desc);
        }
        assertFalse(service().hasContributions());
        assertTrue(service().getConfiguredActions().isEmpty());
        assertTrue("Removing the last contribution must restore the fire-all default",
                service().shouldNotify("csvExport"));
    }

    @Test
    public void testUnionThenPartialUnregister() {
        var first = descriptorFor("setProperties");
        var second = descriptorFor("csvExport", "trash");
        register(first);
        register(second);
        try {
            assertEquals(3, service().getConfiguredActions().size());
            assertTrue(service().shouldNotify("setProperties"));
            assertTrue(service().shouldNotify("csvExport"));
            assertTrue(service().shouldNotify("trash"));

            unregister(second);

            assertTrue("The surviving contribution must still apply", service().shouldNotify("setProperties"));
            assertFalse("Actions of the removed contribution must no longer match",
                    service().shouldNotify("csvExport"));
            assertFalse(service().shouldNotify("trash"));
            assertEquals(1, service().getConfiguredActions().size());
        } finally {
            unregister(first);
            unregister(second);
        }
        assertFalse(service().hasContributions());
    }

    /**
     * A contribution whose effective action list is empty blocks every event. This is a documented footgun rather
     * than a feature: the service logs a WARNING and raises a runtime message when it happens.
     */
    @Test
    public void testEmptyContributionBlocksEverything() {
        var desc = descriptorFor();
        register(desc);
        try {
            assertTrue("An empty <actions/> block still counts as a contribution", service().hasContributions());
            assertTrue(service().getConfiguredActions().isEmpty());
            assertFalse(service().shouldNotify("setProperties"));
            assertFalse(service().shouldNotify("anythingAtAll"));
        } finally {
            unregister(desc);
        }
        assertTrue(service().shouldNotify("setProperties"));
    }

    /** Blank and null action names are stripped, so a contribution made only of them behaves as an empty one. */
    @Test
    public void testBlankActionNamesAreIgnored() {
        var desc = descriptorFor("  ", "", "setProperties");
        register(desc);
        try {
            assertEquals(1, service().getConfiguredActions().size());
            assertTrue(service().shouldNotify("setProperties"));
            assertFalse(service().shouldNotify("  "));
        } finally {
            unregister(desc);
        }
    }

    /**
     * The empty-contribution footgun must be loud: a WARNING in the log and a runtime message surfaced in the
     * admin console. Silently disabling every notification is exactly the failure mode this warning exists to
     * prevent.
     */
    @Test
    @LogCaptureFeature.FilterOn(loggerName = "nuxeo.labs.bafnotification.BAFNotificationServiceImpl",
            logLevel = "WARN")
    public void testEmptyContributionLogsWarningAndRuntimeMessage() {
        var desc = descriptorFor();
        register(desc);
        try {
            logResult.assertHasEvent();
            assertTrue("The warning must explain that no event will be fired",
                    logResult.getCaughtEventMessages()
                             .stream()
                             .anyMatch(m -> m.contains("effective action list is empty")
                                     && m.contains("NO bulkActionDone event will be fired")));

            assertTrue("The misconfiguration must also surface as a runtime message in the admin console",
                    Framework.getRuntime()
                             .getMessageHandler()
                             .getWarnings()
                             .stream()
                             .anyMatch(m -> m.contains("effective action list is empty")));
        } finally {
            unregister(desc);
        }
    }
}
