/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.metadata.oxia.v2.retention;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.metadata.oxia.v2.testing.DeterministicOxiaConditionalClient;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.read.control.M4ReadControlKeysV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5TaskSelectionCoordinatorV2;
import com.nereusstream.storage.object.retention.M5TaskSelectionDecisionV2;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class OxiaTaskSelectionRouteV2Test {
    private static final BindingIdentity BINDING =
            new BindingIdentity(new TopicBindingId(digest("binding")), digest("incarnation"), digest("storage"));
    private static final String SELECTOR = new M4ReadControlKeysV1(7, BINDING).selector();
    private static final BindingReadSelector INITIAL = new BindingReadSelector(
            BINDING,
            digest("view"),
            1,
            1,
            1,
            SelectorMode.PREFERRED_ONLY,
            AdmissionState.ADMITTING,
            Optional.empty(),
            new CapabilityBinding(1, digest("capability")),
            List.of(),
            List.of());

    @Test
    void archiveRequiresItsDurableAnchorAndClearingRequiresItsExactArchive() {
        var client = new DeterministicOxiaConditionalClient();
        var route = initialized(client);
        var raw = route.rawControlMetadata();
        var decision = decision("task");
        int writes = client.createCount();
        assertThatThrownBy(() -> route.compareAndSet(Optional.empty(), decision.key(), decision.encode())
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("task decision archive lacks its exact durable selector anchor");
        assertThat(client.createCount()).isEqualTo(writes);
        var tasks = new M5TaskSelectionCoordinatorV2(raw, 7, BINDING);
        assertThat(tasks.prepare(decision.taskId()).orElseThrow().cancelSelection(INITIAL))
                .contains(decision);
        var before = raw.get(SELECTOR).orElseThrow();
        var cleared = M5BindingAuthorityCodecV1.encodeAuthority(M5BindingAuthorityCodecV1.clearArchivedTaskSelection(
                M5BindingAuthorityCodecV1.decodeAuthority(before), decision.encode()));
        int cas = client.casCount();
        assertThatThrownBy(() -> route.compareAndSet(
                                route.read(SELECTOR).toCompletableFuture().join(), SELECTOR, cleared)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("inline task decision cannot be cleared without its exact archive");
        assertThat(client.casCount()).isEqualTo(cas);
        assertThat(tasks.archiveCurrentDecision(decision.taskId())).isTrue();
        assertThat(raw.compareAndSet(SELECTOR, Optional.of(before), cleared)).isEqualTo(ControlMutationOutcome.APPLIED);
        assertThat(tasks.readDecision(decision.taskId())).contains(decision);
        assertThat(tasks.prepare(decision.taskId())).isEmpty();
    }

    @Test
    void archivedTaskCannotAttachAgainEvenIfItsProjectedSelectorIsIdentical() {
        var client = new DeterministicOxiaConditionalClient();
        var route = initialized(client);
        var raw = route.rawControlMetadata();
        var tasks = new M5TaskSelectionCoordinatorV2(raw, 7, BINDING);
        var decision = decision("task");
        tasks.prepare(decision.taskId()).orElseThrow().cancelSelection(INITIAL);
        assertThat(tasks.prepare(digest("next-task"))).isPresent();
        var before = raw.get(SELECTOR).orElseThrow();
        var current = M5BindingAuthorityCodecV1.decodeAuthority(before);
        var candidate = M5BindingAuthorityCodecV1.encodeAuthority(M5BindingAuthorityCodecV1.anchorTaskSelection(
                current, M5BindingAuthorityCodecV1.selectorSuccessor(current, INITIAL), decision));
        int cas = client.casCount();
        assertThatThrownBy(() -> route.compareAndSet(
                                route.read(SELECTOR).toCompletableFuture().join(), SELECTOR, candidate)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("a task with an archived selection decision cannot select again");
        assertThat(client.casCount()).isEqualTo(cas);
    }

    @Test
    void decisionArchivesAreImmutableBindingBoundAndKeyBound() {
        var client = new DeterministicOxiaConditionalClient();
        var route = initialized(client);
        var tasks = new M5TaskSelectionCoordinatorV2(route.rawControlMetadata(), 7, BINDING);
        var decision = tasks.prepare(digest("task"))
                .orElseThrow()
                .cancelSelection(INITIAL)
                .orElseThrow();
        assertThat(tasks.archiveCurrentDecision(decision.taskId())).isTrue();
        var exact = route.read(decision.key()).toCompletableFuture().join().orElseThrow();
        int writes = client.createCount() + client.casCount();
        assertThatThrownBy(() -> route.compareAndSet(Optional.of(exact), decision.key(), decision.encode()))
                .hasMessageContaining("create-only");
        assertThatThrownBy(() ->
                        route.compareAndSet(Optional.empty(), decision("other").key(), decision.encode()))
                .hasMessageContaining("Binding or key differs");
        var foreign = new BindingIdentity(BINDING.bindingId(), BINDING.incarnationSha256(), digest("foreign"));
        assertThatThrownBy(() -> route.read(M5TaskSelectionDecisionV2.key(foreign, decision.taskId())))
                .hasMessageContaining("outside");
        assertThat(client.createCount() + client.casCount()).isEqualTo(writes);
    }

    private static OxiaBindingLifecycleMetadataStoreV2 initialized(DeterministicOxiaConditionalClient client) {
        var route = new OxiaBindingLifecycleMetadataStoreV2(client, "/nereus/cells/task-select", 7, BINDING);
        assertThat(route.rawControlMetadata()
                        .putIfAbsent(
                                SELECTOR,
                                M5BindingAuthorityCodecV1.encodeAuthority(M5BindingAuthorityCodecV1.initial(INITIAL))))
                .isEqualTo(ControlMutationOutcome.APPLIED);
        return route;
    }

    private static M5TaskSelectionDecisionV2 decision(String task) {
        return M5TaskSelectionDecisionV2.of(
                digest(task), M5TaskSelectionDecisionV2.Outcome.SELECTION_CANCELLED, INITIAL, INITIAL);
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalUtf8.fromString(value).bytes());
    }
}
