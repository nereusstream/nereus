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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.read.control.M4ReadControlKeysV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BindingRetirementAuthorityV1;
import java.util.Objects;
import java.util.Optional;

/** Bounded inline selection decisions plus immutable archives, all linearized by the existing selector authority. */
public final class M5TaskSelectionCoordinatorV2 {
    private final CanonicalControlMetadataStore metadata;
    private final BindingIdentity binding;
    private final String selectorKey;

    public M5TaskSelectionCoordinatorV2(
            CanonicalControlMetadataStore rawMetadata, int shardId, BindingIdentity binding) {
        this.metadata = Objects.requireNonNull(rawMetadata, "rawMetadata");
        this.binding = Objects.requireNonNull(binding, "binding");
        selectorKey = new M4ReadControlKeysV1(shardId, binding).selector();
    }

    public Optional<M5TaskSelectionDecisionV2> readDecision(Sha256Digest taskId) {
        var archived = readArchive(taskId);
        var anchored =
                current().taskSelectionDecision().filter(value -> value.taskId().equals(taskId));
        if (archived.isPresent() && anchored.isPresent() && !archived.equals(anchored)) {
            throw new IllegalStateException("task selection archive conflicts with its durable anchor");
        }
        return archived.isPresent() ? archived : anchored;
    }

    /** Captures raw authority before checking absence; a concurrent decision or archive invalidates the exact CAS. */
    public Optional<Attempt> prepare(Sha256Digest taskId) {
        var current = current();
        if (readArchive(taskId).isPresent()
                || current.taskSelectionDecision()
                        .filter(value -> value.taskId().equals(taskId))
                        .isPresent()) {
            return Optional.empty();
        }
        if (current.taskSelectionDecision().isPresent()) {
            var previous = current.taskSelectionDecision().orElseThrow();
            metadata.putIfAbsent(previous.key(), previous.encode());
            if (!readArchive(previous.taskId()).equals(Optional.of(previous))) {
                return Optional.empty();
            }
            var released = M5BindingAuthorityCodecV1.clearArchivedTaskSelection(current, previous.encode());
            metadata.compareAndSet(
                    selectorKey,
                    Optional.of(M5BindingAuthorityCodecV1.encodeAuthority(current)),
                    M5BindingAuthorityCodecV1.encodeAuthority(released));
            current = current();
            if (current.taskSelectionDecision().isPresent()) {
                return Optional.empty();
            }
        }
        // A raw selector captured before this read prevents an absent archive from racing a cancel/clear cycle.
        if (readArchive(taskId).isPresent()) {
            return Optional.empty();
        }
        return Optional.of(new Attempt(taskId, current));
    }

    /** Copies the durable decision, retaining its inline anchor. Release is deferred to the next task admission. */
    public boolean archiveCurrentDecision(Sha256Digest taskId) {
        var anchor =
                current().taskSelectionDecision().filter(value -> value.taskId().equals(taskId));
        if (anchor.isEmpty()) {
            return readArchive(taskId).isPresent();
        }
        var decision = anchor.orElseThrow();
        metadata.putIfAbsent(decision.key(), decision.encode());
        return readArchive(taskId).equals(Optional.of(decision));
    }

    private Optional<M5TaskSelectionDecisionV2> readArchive(Sha256Digest taskId) {
        return metadata.get(M5TaskSelectionDecisionV2.key(binding, taskId)).map(bytes -> {
            var decision = M5TaskSelectionDecisionV2.decode(bytes);
            if (!decision.binding().equals(binding) || !decision.taskId().equals(taskId)) {
                throw new IllegalStateException("task selection archive belongs to another Binding or task");
            }
            return decision;
        });
    }

    private BindingRetirementAuthorityV1 current() {
        var bytes = metadata.get(selectorKey)
                .orElseThrow(() -> new IllegalStateException("task selection lacks authority"));
        if (!M5BindingAuthorityCodecV1.isAuthorityValue(bytes)) {
            throw new IllegalStateException("task selection requires explicit M5 authority migration first");
        }
        var authority = M5BindingAuthorityCodecV1.decodeAuthority(bytes);
        if (!authority.binding().equals(binding)) {
            throw new IllegalStateException("task selection authority belongs to another Binding");
        }
        return authority;
    }

    public final class Attempt {
        private final Sha256Digest taskId;
        private final BindingRetirementAuthorityV1 captured;
        private final CanonicalBytes expected;

        private Attempt(Sha256Digest taskId, BindingRetirementAuthorityV1 captured) {
            this.taskId = taskId;
            this.captured = captured;
            this.expected = M5BindingAuthorityCodecV1.encodeAuthority(captured);
        }

        public BindingReadSelector selector() {
            return captured.selectorProjection();
        }

        /** The read selector remains byte-identical; only this task's immutable selection decision changes. */
        public Optional<M5TaskSelectionDecisionV2> cancelSelection(BindingReadSelector exactTaskPredecessor) {
            if (!captured.selectorProjection().equals(exactTaskPredecessor)) {
                return Optional.empty();
            }
            var successor = M5BindingAuthorityCodecV1.selectorSuccessor(captured, exactTaskPredecessor);
            var decision = M5TaskSelectionDecisionV2.of(
                    taskId,
                    M5TaskSelectionDecisionV2.Outcome.SELECTION_CANCELLED,
                    exactTaskPredecessor,
                    exactTaskPredecessor);
            var anchored = M5BindingAuthorityCodecV1.anchorTaskSelection(captured, successor, decision);
            metadata.compareAndSet(
                    selectorKey, Optional.of(expected), M5BindingAuthorityCodecV1.encodeAuthority(anchored));
            return readDecision(taskId);
        }

        /** M4 still validates view, epoch and membership. This wrapper adds the decision to that same exact CAS. */
        public CanonicalControlMetadataStore selectingControl(Sha256Digest output) {
            Objects.requireNonNull(output, "output");
            return new CanonicalControlMetadataStore() {
                public Optional<CanonicalBytes> get(String key) {
                    return metadata.get(key);
                }

                public ControlMutationOutcome putIfAbsent(String key, CanonicalBytes bytes) {
                    if (key.equals(selectorKey)) {
                        throw new IllegalArgumentException("task selection cannot create a missing authority");
                    }
                    return metadata.putIfAbsent(key, bytes);
                }

                public ControlMutationOutcome compareAndSet(
                        String key, Optional<CanonicalBytes> before, CanonicalBytes after) {
                    if (!key.equals(selectorKey)) {
                        return metadata.compareAndSet(key, before, after);
                    }
                    if (!before.equals(Optional.of(expected))) {
                        return ControlMutationOutcome.DEFINITIVE_CONFLICT;
                    }
                    var successor = M5BindingAuthorityCodecV1.decodeAuthority(after);
                    if (!successor.selectorProjection().selectedViewSha256().equals(output)) {
                        throw new IllegalArgumentException("task selected another output descriptor");
                    }
                    var decision = M5TaskSelectionDecisionV2.of(
                            taskId,
                            M5TaskSelectionDecisionV2.Outcome.SELECTED,
                            captured.selectorProjection(),
                            successor.selectorProjection());
                    var anchored = M5BindingAuthorityCodecV1.anchorTaskSelection(captured, successor, decision);
                    return metadata.compareAndSet(
                            selectorKey, before, M5BindingAuthorityCodecV1.encodeAuthority(anchored));
                }
            };
        }
    }
}
