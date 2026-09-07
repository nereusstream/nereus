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

package com.nereusstream.storage.object.retention;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlKeysV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class M5TaskSelectionCoordinatorV2Test {
    private static final BindingIdentity BINDING =
            new BindingIdentity(new TopicBindingId(digest("binding")), digest("incarnation"), digest("storage"));
    private static final String SELECTOR = new M4ReadControlKeysV1(7, BINDING).selector();
    private static final BindingReadSelector INITIAL = selector(1, digest("old-view"));

    @Test
    void cancelWinsEvenWhenArchiveClearRestoresExactlyTheSameProjectedSelectorBeforeLatePublish() {
        var store = new Store();
        var tasks = new M5TaskSelectionCoordinatorV2(store, 7, BINDING);
        var task = digest("task");
        var late = tasks.prepare(task).orElseThrow();
        var cancel = tasks.prepare(task).orElseThrow();
        assertThat(cancel.cancelSelection(INITIAL).orElseThrow().outcome())
                .isEqualTo(M5TaskSelectionDecisionV2.Outcome.SELECTION_CANCELLED);
        assertThat(tasks.archiveCurrentDecision(task)).isTrue();
        assertThat(tasks.prepare(digest("next"))).isPresent();
        assertThat(M5BindingAuthorityCodecV1.projectSelector(store.get(SELECTOR).orElseThrow()))
                .isEqualTo(INITIAL);
        assertThat(select(late, selector(2, digest("new")))).isEqualTo(ControlMutationOutcome.DEFINITIVE_CONFLICT);
        assertThat(tasks.prepare(task)).isEmpty();
        assertThat(tasks.readDecision(task).orElseThrow().outcome())
                .isEqualTo(M5TaskSelectionDecisionV2.Outcome.SELECTION_CANCELLED);
    }

    @Test
    void selectedTaskCannotBeReclassifiedAsUnpublishedByCompetingCancelOrFreshCoordinator() {
        var store = new Store();
        var tasks = new M5TaskSelectionCoordinatorV2(store, 7, BINDING);
        var task = digest("task");
        var publish = tasks.prepare(task).orElseThrow();
        var cancel = tasks.prepare(task).orElseThrow();
        assertThat(select(publish, selector(2, digest("new")))).isEqualTo(ControlMutationOutcome.APPLIED);
        assertThat(cancel.cancelSelection(INITIAL).orElseThrow().outcome())
                .isEqualTo(M5TaskSelectionDecisionV2.Outcome.SELECTED);
        assertThat(tasks.archiveCurrentDecision(task)).isTrue();
        var restarted = new M5TaskSelectionCoordinatorV2(store, 7, BINDING);
        assertThat(restarted.prepare(digest("next"))).isPresent();
        assertThat(restarted.readDecision(task).orElseThrow().selectedOutput()).contains(digest("new"));
        assertThat(restarted.prepare(task)).isEmpty();
    }

    @Test
    void lostAppliedSelectorAndArchiveResponsesReconcileFromExactDurableBytes() {
        var store = new Store();
        var tasks = new M5TaskSelectionCoordinatorV2(store, 7, BINDING);
        var task = digest("task");
        var cancel = tasks.prepare(task).orElseThrow();
        store.loseCas = true;
        var decision = cancel.cancelSelection(INITIAL).orElseThrow();
        store.loseCreate = true;
        assertThat(tasks.archiveCurrentDecision(task)).isTrue();
        assertThat(store.get(decision.key())).contains(decision.encode());
        assertThat(tasks.prepare(digest("next"))).isPresent();
        assertThat(tasks.readDecision(task)).contains(decision);
    }

    @Test
    void missingOrUnreadableArchiveRetainsInlineDecisionAndBlocksNextTaskAdmission() {
        var store = new Store();
        var tasks = new M5TaskSelectionCoordinatorV2(store, 7, BINDING);
        var task = digest("task");
        var decision =
                tasks.prepare(task).orElseThrow().cancelSelection(INITIAL).orElseThrow();
        store.dropCreate = true;
        assertThat(tasks.prepare(digest("next"))).isEmpty();
        assertThat(tasks.readDecision(task)).contains(decision);
        store.failRead = decision.key();
        assertThatThrownBy(() -> tasks.archiveCurrentDecision(task)).hasMessage("native read unknown");
        store.failRead = null;
        store.dropCreate = false;
        assertThat(tasks.prepare(digest("next"))).isPresent();
    }

    @Test
    void moreThanFormerLifetimeCapKeepsExactlyOneFixedSizeInlineAnchor() {
        var store = new Store();
        var tasks = new M5TaskSelectionCoordinatorV2(store, 7, BINDING);
        int maximum = 0;
        for (int i = 0; i < 1026; i++) {
            var task = digest("task-" + i);
            var decision =
                    tasks.prepare(task).orElseThrow().cancelSelection(INITIAL).orElseThrow();
            assertThat(tasks.archiveCurrentDecision(task)).isTrue();
            int length = store.get(SELECTOR).orElseThrow().length();
            if (i == 0) {
                maximum = length;
            }
            assertThat(length).isEqualTo(maximum);
            assertThat(M5BindingAuthorityCodecV1.decodeAuthority(
                                    store.get(SELECTOR).orElseThrow())
                            .taskSelectionDecision())
                    .contains(decision);
        }
        assertThat(tasks.prepare(digest("task-0"))).isEmpty();
        assertThat(tasks.readDecision(digest("task-513")).orElseThrow().outcome())
                .isEqualTo(M5TaskSelectionDecisionV2.Outcome.SELECTION_CANCELLED);
        assertThat(maximum).isLessThan(1024);
    }

    @Test
    void legacyWireRoundTripAndOrdinarySelectorMutationPreserveDecisionWithoutDowngrade() {
        var store = new Store();
        var original = store.get(SELECTOR).orElseThrow();
        assertThat(M5BindingAuthorityCodecV1.decodeAuthority(original).wireVersion())
                .isEqualTo(2);
        assertThat(M5BindingAuthorityCodecV1.encodeAuthority(M5BindingAuthorityCodecV1.decodeAuthority(original)))
                .isEqualTo(original);
        var tasks = new M5TaskSelectionCoordinatorV2(store, 7, BINDING);
        var decision = tasks.prepare(digest("task"))
                .orElseThrow()
                .cancelSelection(INITIAL)
                .orElseThrow();
        var before =
                M5BindingAuthorityCodecV1.decodeAuthority(store.get(SELECTOR).orElseThrow());
        var successor = M5BindingAuthorityCodecV1.selectorSuccessor(before, selector(2, digest("view2")));
        assertThat(successor.wireVersion()).isEqualTo(3);
        assertThat(successor.taskSelectionDecision()).contains(decision);
        assertThatThrownBy(() -> M5BindingAuthorityCodecV1.clearArchivedTaskSelection(before, original))
                .hasMessageContaining("archive differs");
        assertThatThrownBy(() -> M5BindingAuthorityCodecV1.requireTaskSelectionTransition(
                        before, M5BindingAuthorityCodecV1.decodeAuthority(original)))
                .hasMessageContaining("downgrade");
    }

    @Test
    void malformedConflictingAndWrongTaskArchiveNeverBecomesAbsence() {
        var store = new Store();
        var tasks = new M5TaskSelectionCoordinatorV2(store, 7, BINDING);
        var task = digest("task");
        var decision =
                tasks.prepare(task).orElseThrow().cancelSelection(INITIAL).orElseThrow();
        byte[] bytes = decision.encode().toByteArray();
        for (int length = 0; length < bytes.length; length++) {
            var truncated = CanonicalBytes.copyOf(Arrays.copyOf(bytes, length));
            assertThatThrownBy(() -> M5TaskSelectionDecisionV2.decode(truncated))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        store.values.put(decision.key(), CanonicalBytes.copyOf(new byte[] {1}));
        assertThatThrownBy(() -> tasks.prepare(task)).isInstanceOf(IllegalArgumentException.class);
        var foreign = M5TaskSelectionDecisionV2.of(digest("other"), decision.outcome(), INITIAL, INITIAL);
        store.values.put(decision.key(), foreign.encode());
        assertThatThrownBy(() -> tasks.readDecision(task)).hasMessageContaining("another Binding or task");
    }

    private static ControlMutationOutcome select(
            M5TaskSelectionCoordinatorV2.Attempt attempt, BindingReadSelector successor) {
        var facade = new M5BindingAuthorityControlMetadataStoreV1(
                attempt.selectingControl(successor.selectedViewSha256()), SELECTOR);
        return facade.compareAndSet(
                SELECTOR,
                Optional.of(M4ReadControlCodecV1.encodeSelector(attempt.selector())),
                M4ReadControlCodecV1.encodeSelector(successor));
    }

    private static BindingReadSelector selector(long generation, Sha256Digest view) {
        return new BindingReadSelector(
                BINDING,
                view,
                1,
                1,
                generation,
                SelectorMode.PREFERRED_ONLY,
                AdmissionState.ADMITTING,
                Optional.empty(),
                new CapabilityBinding(1, digest("capability")),
                List.of(),
                List.of());
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalUtf8.fromString(value).bytes());
    }

    private static final class Store implements CanonicalControlMetadataStore {
        final HashMap<String, CanonicalBytes> values = new HashMap<>();
        boolean loseCas;
        boolean loseCreate;
        boolean dropCreate;
        String failRead;

        Store() {
            values.put(SELECTOR, M5BindingAuthorityCodecV1.encodeAuthority(M5BindingAuthorityCodecV1.initial(INITIAL)));
        }

        public Optional<CanonicalBytes> get(String key) {
            if (key.equals(failRead)) {
                throw new IllegalStateException("native read unknown");
            }
            return Optional.ofNullable(values.get(key));
        }

        public ControlMutationOutcome putIfAbsent(String key, CanonicalBytes value) {
            if (dropCreate) {
                return ControlMutationOutcome.RESPONSE_UNKNOWN;
            }
            values.putIfAbsent(key, value);
            return loseCreate ? ControlMutationOutcome.RESPONSE_UNKNOWN : ControlMutationOutcome.APPLIED;
        }

        public ControlMutationOutcome compareAndSet(
                String key, Optional<CanonicalBytes> expected, CanonicalBytes value) {
            if (!get(key).equals(expected)) {
                return ControlMutationOutcome.DEFINITIVE_CONFLICT;
            }
            values.put(key, value);
            return loseCas ? ControlMutationOutcome.RESPONSE_UNKNOWN : ControlMutationOutcome.APPLIED;
        }
    }
}
