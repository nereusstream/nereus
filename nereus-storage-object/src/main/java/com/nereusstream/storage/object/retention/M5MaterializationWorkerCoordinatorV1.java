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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.materialization.M5MaterializationAdmissionV1;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationKeysV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IdentityEnvelope;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.TaskState;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlKeysV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BindingAuthorityStateV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BindingRetirementAuthorityV1;
import java.util.Objects;
import java.util.Optional;

/** Durable worker epochs on the selector authority. Native callers supply qualified owner, storage and scope facts. */
public final class M5MaterializationWorkerCoordinatorV1 {
    private final CanonicalControlMetadataStore metadata;
    private final BindingIdentity binding;
    private final String selectorKey;
    private final M5MaterializationKeysV1 materializationKeys;

    public M5MaterializationWorkerCoordinatorV1(
            CanonicalControlMetadataStore rawMetadata, int shardId, BindingIdentity binding) {
        this.metadata = Objects.requireNonNull(rawMetadata, "rawMetadata");
        this.binding = Objects.requireNonNull(binding, "binding");
        selectorKey = new M4ReadControlKeysV1(shardId, binding).selector();
        materializationKeys = new M5MaterializationKeysV1(shardId, binding);
    }

    /** Migration preserves the exact M4 projection and cannot import a grant. UNKNOWN remains unresolved. */
    public boolean migrateLegacyAuthority() {
        var current = metadata.get(selectorKey)
                .orElseThrow(() -> new IllegalStateException("worker lacks selector authority"));
        if (M5BindingAuthorityCodecV1.isAuthorityValue(current)) {
            requireBinding(M5BindingAuthorityCodecV1.decodeAuthority(current));
            return true;
        }
        var migrated = M5BindingAuthorityCodecV1.migrateLegacy(current);
        requireBinding(migrated);
        var candidate = M5BindingAuthorityCodecV1.encodeAuthority(migrated);
        metadata.compareAndSet(selectorKey, Optional.of(current), candidate);
        return metadata.get(selectorKey).equals(Optional.of(candidate));
    }

    /** Predicts the next epoch without granting work. The full plan/task is bound by the later exact CAS. */
    public Optional<Proposal> propose(
            BindingReadSelector predecessor,
            Sha256Digest cell,
            Sha256Digest scope,
            long storageFence,
            Sha256Digest nativeWorkerIdentity,
            Sha256Digest nativeNamespaceBinding) {
        var current = current();
        if (current.state() != BindingAuthorityStateV1.OPEN_V1
                || !current.selectorProjection().equals(predecessor)
                || predecessor.admissionState() != AdmissionState.ADMITTING) {
            return Optional.empty();
        }
        long previous = current.materializationWorker()
                .map(value -> value.identity().workerEpoch())
                .orElse(0L);
        var identity = new IdentityEnvelope(
                cell,
                scope,
                binding,
                predecessor.ownerEpoch(),
                Math.addExact(previous, 1),
                storageFence,
                predecessor.capability());
        return Optional.of(new Proposal(current, identity, nativeWorkerIdentity, nativeNamespaceBinding));
    }

    public void requireCurrent(M5MaterializationWorkerV1 worker) {
        if (!isCurrent(worker)) {
            throw new IllegalStateException("materialization worker is stale or its native selector changed");
        }
    }

    /** Reopens the original grant after an uncertain registration without allocating another worker epoch. */
    public Optional<Admission> resumePlan(
            MaterializationPlan plan, Sha256Digest nativeWorkerIdentity, Sha256Digest nativeNamespaceBinding) {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(nativeWorkerIdentity, "nativeWorkerIdentity");
        Objects.requireNonNull(nativeNamespaceBinding, "nativeNamespaceBinding");
        var worker = current()
                .materializationWorker()
                .filter(value -> value.identity().equals(plan.sourceCut().identity())
                        && value.taskIdSha256().equals(plan.taskIdSha256())
                        && value.nativeWorkerIdentitySha256().equals(nativeWorkerIdentity)
                        && value.nativeNamespaceBindingSha256().equals(nativeNamespaceBinding));
        if (worker.isEmpty()) {
            return Optional.empty();
        }
        var admission = new Admission(worker.orElseThrow());
        admission.requireCurrentPlan(plan);
        return Optional.of(admission);
    }

    private boolean isCurrent(M5MaterializationWorkerV1 worker) {
        var current = current();
        return current.state() == BindingAuthorityStateV1.OPEN_V1
                && current.materializationWorker().equals(Optional.of(worker))
                && current.selectorProjection().admissionState() == AdmissionState.ADMITTING
                && worker.predecessorSelectorSha256()
                        .equals(Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(current.selectorProjection())));
    }

    private BindingRetirementAuthorityV1 current() {
        var bytes = metadata.get(selectorKey)
                .orElseThrow(() -> new IllegalStateException("worker lacks selector authority"));
        if (!M5BindingAuthorityCodecV1.isAuthorityValue(bytes)) {
            throw new IllegalStateException("worker admission requires explicit M5 authority migration first");
        }
        var value = M5BindingAuthorityCodecV1.decodeAuthority(bytes);
        requireBinding(value);
        return value;
    }

    private void requireBinding(BindingRetirementAuthorityV1 authority) {
        if (!authority.binding().equals(binding)) {
            throw new IllegalStateException("worker authority belongs to another Binding");
        }
    }

    public final class Proposal {
        private final BindingRetirementAuthorityV1 predecessor;
        private final IdentityEnvelope identity;
        private final Sha256Digest nativeWorker;
        private final Sha256Digest nativeNamespace;

        private Proposal(
                BindingRetirementAuthorityV1 predecessor,
                IdentityEnvelope identity,
                Sha256Digest nativeWorker,
                Sha256Digest nativeNamespace) {
            this.predecessor = predecessor;
            this.identity = identity;
            this.nativeWorker = Objects.requireNonNull(nativeWorker, "nativeWorker");
            this.nativeNamespace = Objects.requireNonNull(nativeNamespace, "nativeNamespace");
        }

        public IdentityEnvelope identity() {
            return identity;
        }

        /** Native installation uses complete persisted inputs and the existing shared Cell charge, never opaque IDs. */
        public Optional<Admission> admitPlan(MaterializationPlan plan) {
            requireStoredPlan(plan, identity, true);
            return admit(
                    M5MaterializationCodecV1.sourceCutSha256(plan.sourceCut()),
                    Sha256Digest.hash(M5MaterializationCodecV1.encodePlan(plan)),
                    plan.taskIdSha256());
        }

        /** A changed predecessor, failed response/reread or competing worker never yields an admission token. */
        public Optional<Admission> admit(Sha256Digest sourceCut, Sha256Digest planRoot, Sha256Digest taskId) {
            var worker = new M5MaterializationWorkerV1(
                    identity,
                    nativeWorker,
                    nativeNamespace,
                    Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(predecessor.selectorProjection())),
                    sourceCut,
                    planRoot,
                    taskId);
            var successor = M5BindingAuthorityCodecV1.admitWorker(predecessor, worker);
            var candidate = M5BindingAuthorityCodecV1.encodeAuthority(successor);
            metadata.compareAndSet(
                    selectorKey, Optional.of(M5BindingAuthorityCodecV1.encodeAuthority(predecessor)), candidate);
            var observed = current();
            if (!observed.materializationWorker().equals(Optional.of(worker))
                    || !observed.selectorProjection().equals(predecessor.selectorProjection())
                    || observed.state() != BindingAuthorityStateV1.OPEN_V1) {
                return Optional.empty();
            }
            return Optional.of(new Admission(worker));
        }
    }

    /** Constructed only after exact durable admission. It cannot publish a selector or authorize cleanup. */
    public final class Admission {
        private final M5MaterializationWorkerV1 worker;

        private Admission(M5MaterializationWorkerV1 worker) {
            this.worker = worker;
        }

        public M5MaterializationWorkerV1 worker() {
            return worker;
        }

        public void requireCurrent() {
            M5MaterializationWorkerCoordinatorV1.this.requireCurrent(worker);
        }

        public void requireCurrentPlan(MaterializationPlan plan) {
            requireStoredPlan(plan, worker.identity(), false);
            if (!worker.taskIdSha256().equals(plan.taskIdSha256())
                    || !worker.sourceCutSha256().equals(M5MaterializationCodecV1.sourceCutSha256(plan.sourceCut()))
                    || !worker.planRootSha256().equals(Sha256Digest.hash(M5MaterializationCodecV1.encodePlan(plan)))) {
                throw new IllegalStateException("worker grant differs from the complete original plan");
            }
            requireCurrent();
        }

        public boolean isCurrent() {
            return M5MaterializationWorkerCoordinatorV1.this.isCurrent(worker);
        }
    }

    private void requireStoredPlan(MaterializationPlan plan, IdentityEnvelope identity, boolean plannedOnly) {
        Objects.requireNonNull(plan, "plan");
        if (!plan.sourceCut().identity().equals(identity)
                || !metadata.get(materializationKeys.plan(plan.taskIdSha256()))
                        .equals(Optional.of(M5MaterializationCodecV1.encodePlan(plan)))
                || !metadata.get(materializationKeys.sourceCut(plan.taskIdSha256()))
                        .equals(Optional.of(M5MaterializationCodecV1.encodeSourceCut(plan.sourceCut())))) {
            throw new IllegalStateException("worker admission lacks its exact complete stored plan/source cut");
        }
        var task = metadata.get(materializationKeys.task(plan.taskIdSha256()))
                .map(M5MaterializationCodecV1::decodeTask)
                .orElseThrow(() -> new IllegalStateException("worker admission lacks the original persisted task"));
        if ((plannedOnly && task.state() != TaskState.PLANNED)
                || task.state() == TaskState.CANCELLED_STALE
                || task.state() == TaskState.QUARANTINED
                || !task.taskIdSha256().equals(plan.taskIdSha256())
                || !task.outputIdentitySha256().equals(plan.outputIdentitySha256())
                || !task.sourceCutSha256().equals(M5MaterializationCodecV1.sourceCutSha256(plan.sourceCut()))) {
            throw new IllegalStateException("worker admission task differs from the complete original plan");
        }
        long sourceBytes = plan.sourceCut().sources().stream()
                .mapToLong(value -> value.canonicalLength())
                .reduce(0L, Math::addExact);
        var charge = new M5MaterializationAdmissionV1(metadata, identity.protocolCellSha256())
                .read()
                .flatMap(state -> state.reservations().stream()
                        .filter(value -> value.taskIdSha256().equals(plan.taskIdSha256()))
                        .findFirst())
                .orElseThrow(() -> new IllegalStateException("worker admission lacks the original shared Cell charge"));
        if (charge.sourceBytes() != sourceBytes
                || charge.sourceMembers() != plan.sourceCut().sources().size()
                || charge.outputParts() != plan.outputParts().size()
                || charge.indexes() != plan.indexes().size()) {
            throw new IllegalStateException("worker admission shared Cell charge differs from its complete plan");
        }
    }
}
