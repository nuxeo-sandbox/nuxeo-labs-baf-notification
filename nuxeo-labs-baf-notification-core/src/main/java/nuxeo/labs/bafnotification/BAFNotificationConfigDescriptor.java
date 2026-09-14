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

import java.util.ArrayList;
import java.util.List;

import org.nuxeo.common.xmap.annotation.XNodeList;
import org.nuxeo.common.xmap.annotation.XObject;

/**
 * Descriptor for the {@code configuration} extension point of
 * {@link BAFNotificationService}. Each contributed {@code <actions>} block lists the BAF
 * action names that should trigger the {@code bulkActionDone} event. Multiple
 * contributions are merged (union) by the service.
 * <p>
 * This class intentionally does <b>not</b> implement {@link org.nuxeo.runtime.model.Descriptor} and defines neither
 * {@code equals} nor {@code hashCode}: the service unions every contribution instead of merging or replacing them by
 * id, and therefore tracks descriptors by identity through the legacy
 * {@link BAFNotificationServiceImpl#registerContribution} path. Migrating this to the modern
 * {@code DescriptorRegistry} would key contributions on {@code getId()} and silently replace same-id contributions,
 * breaking the documented union semantics. Do not "modernize" it without changing the contract first.
 *
 * @since 2025.1
 */
@XObject("actions")
public class BAFNotificationConfigDescriptor {

    @XNodeList(value = "action", type = ArrayList.class, componentType = String.class)
    protected List<String> actions = new ArrayList<>();

    public List<String> getActions() {
        return actions;
    }
}
