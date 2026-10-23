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

package com.nereusstream.pulsar.offload;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPulsarMaterializationInputV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2.BirthResult;
import com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2;
import com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3;
import com.nereusstream.storage.object.materialization.M5LookupIndexV1;
import com.nereusstream.storage.object.materialization.M5MaterializationAdmissionV1;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationCoordinatorV1;
import com.nereusstream.storage.object.materialization.M5MaterializationObjectSessionV1;
import com.nereusstream.storage.object.materialization.M5MaterializationPlannerV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.GenerationObject;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationSourceCut;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PayloadKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PublicationOutcome;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.RepresentationMode;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.TaskState;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1.ValidatedGeneration;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1.VerifiedPulsarOutputs;
import com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2;
import com.nereusstream.storage.object.materialization.M5PulsarCoverageV2;
import com.nereusstream.storage.object.materialization.M5PulsarEntriesV2;
import com.nereusstream.storage.object.materialization.M5PulsarFrontiersV2;
import com.nereusstream.storage.object.materialization.M5PulsarStagedPartV2;
import com.nereusstream.storage.object.materialization.Nms1CodecV1;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.ProviderObjectOutcome;
import com.nereusstream.storage.object.provider.ProviderObjectResult;
import com.nereusstream.storage.object.provider.RepeatableObjectBody;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProtectionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtection;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1;
import com.nereusstream.storage.object.retention.M5MaterializationWorkerCoordinatorV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.ReferenceAdapterResultV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceDispositionV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceObservationV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceScanSummaryV1;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.CapturedReadLimits;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.CapturedReadOperation;
import org.apache.pulsar.common.protocol.Commands;

/** Original Native Task owns its complete plan and accepted SDK output attempts through durable handoff. */
public final class PulsarNativeMaterializationTaskV2 {
    /** Exact materialization subgraph only; terminal task state does not erase its retained source/output roots. */
    public static final class RetentionRoots implements AutoCloseable {
        private Map<String, VersionedValue> facts;
        private List<MaterializationPlan> plans;
        private List<OxiaPulsarMaterializationInputV2> nativeInputs;
        private List<com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.NamespaceSnapshot>
                physicalNamespaces;
        private Optional<
                        com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1
                                .BindingRetirementAuthorityV1>
                workerAuthority;
        private Optional<M5MaterializationAdmissionV1.State> cellAdmission;
        private Map<java.util.UUID, PulsarNativeReadCellBudgetV1.Usage> otherCellReservations;
        private final PulsarNativeReadCellBudgetV1.Reservation workspace;
        private VersionedValue sourceScan;
        private CompletableFuture<Void> closing;
        private CompletableFuture<Void> referenceRefresh = CompletableFuture.completedFuture(null);
        private boolean closed;

        private RetentionRoots(
                Map<String, VersionedValue> facts,
                List<MaterializationPlan> plans,
                List<OxiaPulsarMaterializationInputV2> nativeInputs,
                List<com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.NamespaceSnapshot>
                        physicalNamespaces,
                Optional<
                                com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1
                                        .BindingRetirementAuthorityV1>
                        workerAuthority,
                Optional<M5MaterializationAdmissionV1.State> cellAdmission,
                Map<java.util.UUID, PulsarNativeReadCellBudgetV1.Usage> otherCellReservations,
                PulsarNativeReadCellBudgetV1.Reservation workspace,
                VersionedValue sourceScan) {
            this.sourceScan = sourceScan;
            this.facts = Map.copyOf(facts);
            this.plans = List.copyOf(plans);
            this.nativeInputs = List.copyOf(nativeInputs);
            this.physicalNamespaces = List.copyOf(physicalNamespaces);
            this.workerAuthority = workerAuthority;
            this.cellAdmission = cellAdmission;
            this.otherCellReservations = Map.copyOf(otherCellReservations);
            this.workspace = workspace;
        }

        public synchronized Map<String, VersionedValue> facts() {
            requireOpen();
            return facts;
        }

        public synchronized List<MaterializationPlan> plans() {
            requireOpen();
            return plans;
        }

        public synchronized List<OxiaPulsarMaterializationInputV2> nativeInputs() {
            requireOpen();
            return nativeInputs;
        }

        /** Observed physical metadata only; completeness still requires every applicable writer's scan fence. */
        public synchronized List<
                        com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.NamespaceSnapshot>
                physicalNamespaces() {
            requireOpen();
            return physicalNamespaces;
        }

        public synchronized Optional<
                        com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1
                                .BindingRetirementAuthorityV1>
                workerAuthority() {
            requireOpen();
            return workerAuthority;
        }

        /** Shared Cell state; a foreign task ID is not silently attributed to this Binding. */
        public synchronized Optional<M5MaterializationAdmissionV1.State> cellAdmission() {
            requireOpen();
            return cellAdmission;
        }

        /** Other actual reservations of this Binding, excluding only this exact observation's own workspace. */
        public synchronized Map<java.util.UUID, PulsarNativeReadCellBudgetV1.Usage> otherCellReservations() {
            requireOpen();
            return otherCellReservations;
        }

        /** Positive Task references only. An unmatched target is unresolved, never an absence observation. */
        public synchronized List<ReferenceObservationV1> presentTaskReferences(Sha256Digest target) {
            requireOpen();
            Objects.requireNonNull(target, "target");
            var rows = new java.util.TreeMap<String, ReferenceObservationV1>();
            for (var plan : plans) {
                if (plan.sourceCut().sources().stream()
                        .anyMatch(source -> source.sourceIdentitySha256().equals(target)
                                || source.ledgerIdentitySha256()
                                        .filter(target::equals)
                                        .isPresent())) {
                    addTaskReferenceRows(rows, plan, target);
                }
            }
            for (var namespace : physicalNamespaces) {
                for (var birth : namespace.births()) {
                    if (birth.resource()
                            .filter(resource -> resource.sha256().equals(target))
                            .isEmpty()) {
                        continue;
                    }
                    var plan = plans.stream()
                            .filter(value ->
                                    value.taskIdSha256().equals(birth.worker().taskIdSha256()))
                            .findFirst()
                            .orElseThrow(() -> new IllegalStateException("Native output Task plan is unresolved"));
                    if (!birth.worker().identity().equals(plan.sourceCut().identity())
                            || !birth.worker()
                                    .planRootSha256()
                                    .equals(Sha256Digest.hash(M5MaterializationCodecV1.encodePlan(plan)))
                            || !birth.worker()
                                    .sourceCutSha256()
                                    .equals(M5MaterializationCodecV1.sourceCutSha256(plan.sourceCut()))) {
                        throw new IllegalStateException("Native output credit differs from its original Task plan");
                    }
                    var fact = namespace.facts().values().stream()
                            .filter(value -> value.canonicalStoredBytes().equals(birth.encode()))
                            .findFirst()
                            .orElseThrow(() -> new IllegalStateException("Native output credit fact is missing"));
                    addTaskReferenceRows(rows, plan, target);
                    rows.put(fact.key(), taskReferenceRow(fact, plan, target));
                }
            }
            if (rows.isEmpty()) {
                throw new IllegalStateException(
                        "Native Task reference absence requires closed lifecycle/root handoff evidence");
            }
            if (rows.size() > com.nereusstream.storage.object.retention.M5RetentionRecordsV1.MAX_REFERENCE_ROWS) {
                throw new IllegalStateException("Native Task reference rows exceed their admitted bound");
            }
            return List.copyOf(rows.values());
        }

        /** Closed Native Task actors only; recovery/selected roots and every other reference kind remain separate. */
        synchronized java.util.UUID referenceScope(
                VersionedValue expectedScan, PulsarNativeReadCellBudgetV1.Reservation parent) {
            requireOpen();
            if (!sourceScan.equals(expectedScan) || !workspace.sameAllocator(parent)) {
                throw new IllegalStateException("Native Task capture belongs to another source scan or Cell");
            }
            return workspace.operationId();
        }

        synchronized CompletionStage<Void> refreshReferenceReservations() {
            requireOpen();
            if (!referenceRefresh.isDone()) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Native Task reference refresh is already in progress"));
            }
            // A classified observation may have reserved and drained since capture. Recheck the actual
            // original hold/native owner and exact remaining reservations before refreshing only allocator facts.
            referenceRefresh = workspace
                    .captureReservations()
                    .thenAccept(current -> {
                        synchronized (this) {
                            requireOpen();
                            if (!current.otherReservations().equals(otherCellReservations)) {
                                throw new IllegalStateException(
                                        "Native Task reservation inventory changed before reference consumption");
                            }
                            var currentFacts = new java.util.LinkedHashMap<>(facts);
                            currentFacts.putAll(current.facts());
                            facts = Map.copyOf(currentFacts);
                        }
                    })
                    .toCompletableFuture();
            return referenceRefresh.copy();
        }

        public synchronized ReferenceAdapterResultV1 closedTaskReferences(
                M5ClosedWriterRegistryV1 registry, PhysicalResourceIdV2 target, M5PulsarCoverageV2 coverage) {
            return closedTaskReferences(registry, target, coverage, Optional.empty());
        }

        public synchronized ReferenceAdapterResultV1 closedTaskReferences(
                M5ClosedWriterRegistryV1 registry, PhysicalResourceIdV2 target, PulsarNativeCursorFloorScanV3 cursors) {
            return closedTaskReferences(
                    registry, target, cursors.coverage(), Optional.of(cursors.referenceScope(sourceScan, workspace)));
        }

        /** Terminal Task graph and actual original Cell drain, rather than an empty local work queue. */
        synchronized com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.FloorAdapterResultV1
                closedTaskFloor(
                        M5ClosedWriterRegistryV1 registry,
                        PhysicalResourceIdV2 target,
                        PulsarNativeCursorFloorScanV3 cursors) {
            var references = closedTaskReferences(registry, target, cursors);
            var kind = com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1.LIFECYCLE_TASK;
            var rows = references.observations().stream()
                    .map(row ->
                            new com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                    .RetentionFloorObservationV1(
                                    kind,
                                    row.authority(),
                                    com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1
                                            .PositionDomain.PULSAR_ENTRY,
                                    -1,
                                    false,
                                    true,
                                    Optional.empty()))
                    .toList();
            return new com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.FloorAdapterResultV1(
                    kind, references.summary().pageCount(), references.summary().scannedBytes(), rows);
        }

        private ReferenceAdapterResultV1 closedTaskReferences(
                M5ClosedWriterRegistryV1 registry,
                PhysicalResourceIdV2 target,
                M5PulsarCoverageV2 coverage,
                Optional<java.util.UUID> knownCursorObservation) {
            requireOpen();
            PulsarNativeWriterRegistryV2.requireNativeOwners(registry);
            var source = M5PulsarAggregateAuthorityCodecV1.decodeAuthority(sourceScan.canonicalStoredBytes());
            var authority = workerAuthority.orElseThrow(
                    () -> new IllegalStateException("Native Task worker authority is missing"));
            if (source.logicalTrimSourceScan().isEmpty()
                    || source.retentionPolicy().isEmpty()
                    || source.firstNativeFactoryScope().isEmpty()
                    || !source.writerEnrollment().equals(Optional.of(registry.enrollment()))) {
                throw new IllegalStateException("Native Task closure lacks its original enrolled Binding/source scan");
            }
            // Native mutations use the P1 registry; M4 retains the actual worker/capability.
            if (authority
                            .writerEnrollment()
                            .filter(value -> !value.equals(registry.enrollment()))
                            .isPresent()
                    || plans.stream()
                            .anyMatch(plan ->
                                    !plan.sourceCut().identity().capability().equals(authority.capability())
                                            || !plan.sourceCut()
                                                    .predecessorSelector()
                                                    .capability()
                                                    .equals(authority.capability()))) {
                throw new IllegalStateException("Native Task worker capability or explicit registry differs");
            }
            if (!coverage.binding().equals(authority.binding())
                    || knownCursorObservation.isEmpty()
                            && nativeInputs.stream()
                                    .noneMatch(input -> input.coverage().contains(coverage))) {
                throw new IllegalStateException("Native Task request coverage differs from its actual input chain");
            }
            if (otherCellReservations.keySet().stream()
                            .anyMatch(operation -> !knownCursorObservation.equals(Optional.of(operation)))
                    || cellAdmission.isEmpty()
                    || !cellAdmission.orElseThrow().reservations().isEmpty()
                    || !authority.referenceMutationTickets().isEmpty()
                    || !authority.trimMutationTickets().isEmpty()) {
                throw new IllegalStateException(
                        "Native Task actors, reservations or metadata mutations remain unresolved");
            }
            var observed = physicalNamespaces.stream()
                    .flatMap(namespace -> namespace.resources().stream())
                    .filter(value -> value.resourceSha256().equals(target.sha256()))
                    .findFirst()
                    .orElseThrow(
                            () -> new IllegalStateException("Native Task target has no actual physical inventory"));
            if (observed.storedAuthority()
                            .filter(value -> value.resource().equals(target))
                            .isEmpty()
                    || observed.reservation().isEmpty()
                    || observed.members().isEmpty()
                    || observed.members().orElseThrow().inventory().members().stream()
                            .anyMatch(member -> member.domain()
                                            != com.nereusstream.storage.object.materialization
                                                    .M5MaterializationRecordsV1.PositionDomain.PULSAR_ENTRY
                                    || !member.binding().equals(authority.binding()))) {
                throw new IllegalStateException("Native Task target has unresolved or foreign physical members");
            }
            for (var plan : plans) {
                requireClosedPlan(plan);
            }
            var kind = ReferenceKindV1.MATERIALIZATION_OR_COMPACTION_TASK;
            var rows = facts.values().stream()
                    .filter(value -> !value.key().contains("/v2/physical-delete-m5-v2/")
                            && !value.key().contains("/" + M5GcQuotaRecordsV2.ENTRY_PREFIX)
                            && !value.key().endsWith("/" + M5GcQuotaRecordsV2.HEAD_KEY))
                    .sorted(Comparator.comparing(VersionedValue::key))
                    .map(value -> new ReferenceObservationV1(
                            kind,
                            new AuthorityFactV1(value.key(), value.metadataVersion(), value.canonicalStoredSha256()),
                            target.sha256(),
                            ProtocolCoverage.pulsar(coverage),
                            ReferenceDispositionV1.ABSENT,
                            true))
                    .toList();
            long scanned = facts.values().stream()
                    .mapToLong(value -> value.canonicalStoredBytes().length())
                    .sum();
            return new ReferenceAdapterResultV1(
                    kind, new ReferenceScanSummaryV1(kind, rows.size(), 1, scanned, true), rows);
        }

        record UnpublishedOutput(
                MaterializationPlan plan,
                com.nereusstream.storage.object.provider.ObjectIdentity object,
                VersionedValue terminal,
                VersionedValue pending) {}

        synchronized UnpublishedOutput unpublishedOutput(
                M5ClosedWriterRegistryV1 registry,
                PhysicalResourceIdV2.ObjectVersion target,
                PulsarNativeCursorFloorScanV3 cursors) {
            closedTaskReferences(registry, target, cursors);
            var births = physicalNamespaces.stream()
                    .flatMap(value -> value.births().stream())
                    .filter(value -> value.resource().equals(Optional.of(target)))
                    .toList();
            if (births.size() != 1 || births.get(0).state() != M5ObjectBirthCreditV3.State.ASSIGNED) {
                throw new IllegalStateException("Native unpublished target lacks one original assigned output credit");
            }
            var birth = births.get(0);
            var plan = plans.stream()
                    .filter(value -> value.taskIdSha256().equals(birth.worker().taskIdSha256()))
                    .findFirst()
                    .orElseThrow();
            var terminal = requireRoot("tasks", plan.taskIdSha256().toHex());
            if (M5MaterializationCodecV1.decodeTask(terminal.canonicalStoredBytes())
                            .state()
                    == TaskState.PUBLISHED) {
                throw new IllegalStateException("Published Native output requires replacement or expiry qualification");
            }
            return new UnpublishedOutput(
                    plan,
                    birth.output().orElseThrow(),
                    terminal,
                    requireRoot("pending-outputs", plan.taskIdSha256().toHex()));
        }

        private void requireClosedPlan(MaterializationPlan plan) {
            String id = plan.taskIdSha256().toHex();
            var task =
                    M5MaterializationCodecV1.decodeTask(requireRoot("tasks", id).canonicalStoredBytes());
            if (task.state() != TaskState.PUBLISHED
                    && task.state() != TaskState.CANCELLED_STALE
                    && task.state() != TaskState.QUARANTINED) {
                throw new IllegalStateException("Native Task is not terminal");
            }
            var pending = M5PendingOutputInventoryV2.decode(
                    requireRoot("pending-outputs", id).canonicalStoredBytes());
            pending.requirePlan(plan);
            var namespace = physicalNamespaces.stream()
                    .filter(value -> value.binding().equals(pending.profile().physicalBinding()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Native Task output namespace is unresolved"));
            var credits = new ArrayList<M5ObjectBirthCreditV3>();
            for (var slot : pending.outputs()) {
                var credit = namespace.births().stream()
                        .filter(value -> value.worker().equals(pending.worker())
                                && value.slot().equals(slot))
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException("Native Task output credit is missing"));
                if (credit.state() != M5ObjectBirthCreditV3.State.ASSIGNED
                                && credit.state() != M5ObjectBirthCreditV3.State.CANCELLED_NO_DISPATCH
                        || task.state() == TaskState.PUBLISHED && credit.state() != M5ObjectBirthCreditV3.State.ASSIGNED
                        || task.state() == TaskState.CANCELLED_STALE
                                && credit.state() != M5ObjectBirthCreditV3.State.CANCELLED_NO_DISPATCH) {
                    throw new IllegalStateException("Native Task output dispatch/assignment is unresolved");
                }
                credits.add(credit);
            }
            if (task.state() != TaskState.PUBLISHED) {
                return;
            }
            var generation = M5MaterializationCodecV1.decodeGeneration(requireRoot(
                            "generations", task.generationSha256().orElseThrow().toHex())
                    .canonicalStoredBytes());
            var manifest = M5MaterializationCodecV1.decodeManifest(requireRoot(
                            "manifests", task.manifestViewSha256().orElseThrow().toHex())
                    .canonicalStoredBytes());
            var validation = M5MaterializationCodecV1.decodeValidationRoot(requireRoot(
                            "validations",
                            task.validationRootSha256().orElseThrow().toHex())
                    .canonicalStoredBytes());
            var cut = plan.sourceCut();
            if (!generation.identity().equals(cut.identity())
                    || !manifest.identity().equals(cut.identity())
                    || !generation.taskIdSha256().equals(plan.taskIdSha256())
                    || generation.representationMode() != plan.representationMode()
                    || generation.payloadKind() != plan.payloadKind()
                    || generation.sourceGeneration()
                            != Math.addExact(cut.predecessorSelector().sourceGeneration(), 1)
                    || !generation.outputIdentitySha256().equals(plan.outputIdentitySha256())
                    || !generation.sourceSetSha256().equals(cut.sourceSetSha256())
                    || !generation.coverage().equals(cut.coverage())
                    || !manifest.coverage().equals(cut.coverage())
                    || !generation.protocolStateRootSha256().equals(cut.protocolStateRootSha256())
                    || !generation.predecessorSelectedViewSha256().equals(cut.predecessorViewSha256())
                    || !manifest.exactPredecessorViewSha256().equals(cut.predecessorViewSha256())
                    || !manifest.exactPredecessorSelectorValueSha256().equals(cut.predecessorSelectorValueSha256())
                    || !manifest.preferredGenerationSha256()
                            .equals(task.generationSha256().orElseThrow())
                    || !generation
                            .validationRootSha256()
                            .equals(task.validationRootSha256().orElseThrow())
                    || !validation.taskIdSha256().equals(plan.taskIdSha256())
                    || !validation.outputIdentitySha256().equals(plan.outputIdentitySha256())
                    || !validation.sourceSetSha256().equals(cut.sourceSetSha256())) {
                throw new IllegalStateException("Native published Task graph differs from its complete original plan");
            }
            var objects = java.util.stream.Stream.concat(
                            generation.payloadObjects().stream(), generation.indexObjects().stream())
                    .toList();
            if (objects.size() != credits.size()
                    || objects.stream().anyMatch(object -> credits.stream()
                            .noneMatch(credit -> credit.output().equals(Optional.of(object.identity()))
                                    && object.immutableProviderVersionToken()
                                            .equals(credit.resource().map(resource -> CanonicalUtf8.fromString(
                                                            resource.immutableIdentity()
                                                                    .value())
                                                    .bytes()))))) {
                throw new IllegalStateException("Native published outputs differ from their actual birth identities");
            }
        }

        private VersionedValue requireRoot(String kind, String id) {
            return facts.values().stream()
                    .filter(value -> value.key().endsWith("/" + kind + "/" + id))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Native Task root is missing: " + kind));
        }

        private void addTaskReferenceRows(
                Map<String, ReferenceObservationV1> rows, MaterializationPlan plan, Sha256Digest target) {
            String id = plan.taskIdSha256().toHex();
            for (String suffix : List.of("/plans/" + id, "/tasks/" + id)) {
                var fact = facts.values().stream()
                        .filter(value -> value.key().endsWith(suffix))
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException("Native Task reference root is missing"));
                rows.put(fact.key(), taskReferenceRow(fact, plan, target));
            }
        }

        private static ReferenceObservationV1 taskReferenceRow(
                VersionedValue fact, MaterializationPlan plan, Sha256Digest target) {
            return new ReferenceObservationV1(
                    ReferenceKindV1.MATERIALIZATION_OR_COMPACTION_TASK,
                    new AuthorityFactV1(fact.key(), fact.metadataVersion(), fact.canonicalStoredSha256()),
                    target,
                    plan.sourceCut().coverage(),
                    ReferenceDispositionV1.PRESENT,
                    false);
        }

        private void requireOpen() {
            if (closed) {
                throw new IllegalStateException("Native materialization roots are closed");
            }
        }

        public synchronized CompletableFuture<Void> closeAsync() {
            closed = true;
            if (closing == null || closing.isCompletedExceptionally()) {
                closing = referenceRefresh
                        .handle((unused, failure) -> null)
                        .thenCompose(unused -> workspace.releaseAfterDrain())
                        .thenRun(() -> {
                            synchronized (this) {
                                facts = null;
                                plans = null;
                                nativeInputs = null;
                                physicalNamespaces = null;
                                workerAuthority = null;
                                cellAdmission = null;
                                otherCellReservations = null;
                                sourceScan = null;
                            }
                        });
                closing.whenComplete((unused, failure) -> {
                    if (failure != null) {
                        workspace.retainUndeliveredSdk(this::closeAsync, false);
                    }
                });
            }
            return closing.copy();
        }

        @Override
        public void close() {
            closeAsync().join();
        }
    }

    static RetentionRoots captureRetentionRoots(
            PulsarNativeBindingV1 binding,
            PulsarNativeM4ControlV1 control,
            VersionedValue exactScan,
            PulsarNativeReadCellBudgetV1.Reservation workspace,
            List<com.nereusstream.storage.api.lifecycle.NativePhysicalNamespaceAuthorityV2> physicalBackends) {
        var trim = binding.logicalTrimCoordinator();
        trim.requireSourceScanCurrent(exactScan).toCompletableFuture().join();
        binding.revalidate().toCompletableFuture().join();
        var controls = new LinkedHashMap<>(control.captureMaterializationControls());
        var reservations = workspace.captureReservations().join();
        controls.putAll(reservations.facts());
        var authorityBytes = controls.get(control.selectorKey()).canonicalStoredBytes();
        var workerAuthority = com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1.isAuthorityValue(
                        authorityBytes)
                ? Optional.of(com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1.decodeAuthority(
                        authorityBytes))
                : Optional
                        .<com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1
                                        .BindingRetirementAuthorityV1>
                                empty();
        var cell = Sha256Digest.hash(com.nereusstream.domain.codec.ProtocolCellIdentityCodecV1.encode(
                binding.authority().aggregate().binding().cellIdentity()));
        var admission = Optional.ofNullable(controls.get(control.materializationAdmissionKey(cell)))
                .map(fact -> M5MaterializationAdmissionV1.decode(fact.canonicalStoredBytes()));
        var captured = control.scanMaterializationNamespace();
        var keys = new com.nereusstream.storage.object.materialization.M5MaterializationKeysV1(
                0, binding.bindingIdentity());
        String root = control.materializationNamespaceRoot();
        var plans = new java.util.TreeMap<String, MaterializationPlan>();
        var inputs = new ArrayList<OxiaPulsarMaterializationInputV2>();
        long bytes = exactScan.canonicalStoredBytes().length();
        for (var fact : controls.values()) {
            bytes = Math.addExact(bytes, fact.canonicalStoredBytes().length());
        }
        for (var fact : captured.values()) {
            bytes = Math.addExact(bytes, fact.canonicalStoredBytes().length());
            if (bytes > PulsarNativeCursorMetadataV2.MAX_BYTES) {
                throw new IllegalArgumentException("Native materialization roots exceed admitted workspace");
            }
            String suffix = fact.key().substring(root.length() + 1);
            int slash = suffix.indexOf('/');
            if (slash <= 0 || !suffix.startsWith("retention/") && suffix.indexOf('/', slash + 1) >= 0) {
                throw new IllegalArgumentException("Native materialization root key is not canonical");
            }
            var raw = fact.canonicalStoredBytes();
            String expected;
            switch (suffix.substring(0, slash)) {
                case "source-cuts" -> {
                    var cut = M5MaterializationCodecV1.decodeSourceCut(raw);
                    requireRetentionBinding(binding, cut.identity().binding());
                    expected = keys.sourceCut(M5MaterializationCodecV1.calculateTaskId(cut));
                }
                case "plans" -> {
                    var plan = M5MaterializationCodecV1.decodePlan(raw);
                    requireRetentionBinding(binding, plan.sourceCut().identity().binding());
                    plans.put(plan.taskIdSha256().toHex(), plan);
                    expected = keys.plan(plan.taskIdSha256());
                }
                case "tasks" ->
                    expected =
                            keys.task(M5MaterializationCodecV1.decodeTask(raw).taskIdSha256());
                case "pending-outputs" -> {
                    var pending = M5PendingOutputInventoryV2.decode(raw);
                    requireRetentionBinding(binding, pending.worker().identity().binding());
                    expected = keys.pendingOutputs(pending.worker().taskIdSha256());
                }
                case "native-inputs" -> {
                    var input = OxiaPulsarMaterializationInputV2.decode(raw);
                    requireRetentionBinding(binding, input.binding());
                    inputs.add(input);
                    expected = keys.nativeInput(input.protocolStateRootSha256());
                }
                case "validations" -> {
                    var validation = M5MaterializationCodecV1.decodeValidationRoot(raw);
                    expected = keys.validation(M5MaterializationCodecV1.validationRootSha256(validation));
                }
                case "generations" -> {
                    var generation = M5MaterializationCodecV1.decodeGeneration(raw);
                    requireRetentionBinding(binding, generation.identity().binding());
                    expected = keys.generation(M5MaterializationCodecV1.generationSha256(generation));
                }
                case "manifests" -> {
                    var manifest = M5MaterializationCodecV1.decodeManifest(raw);
                    requireRetentionBinding(binding, manifest.identity().binding());
                    expected = keys.manifest(M5MaterializationCodecV1.manifestSha256(manifest));
                }
                case "retention" -> {
                    var retention = new com.nereusstream.storage.object.retention.M5RetentionKeysV1(
                            0, binding.bindingIdentity());
                    if (suffix.equals("retention/trim-frontier")) {
                        var frontier =
                                com.nereusstream.storage.object.retention.M5RetentionCodecV1.decodeTrimFrontier(raw);
                        requireRetentionBinding(binding, frontier.identity().binding());
                        expected = retention.trimFrontier();
                    } else if (suffix.startsWith("retention/floor-snapshots/")) {
                        var snapshot = com.nereusstream.storage.object.retention.M5RetentionCodecV1.decodeSnapshot(raw);
                        requireRetentionBinding(binding, snapshot.identity().binding());
                        expected = retention.floorSnapshot(snapshot.snapshotRootSha256());
                    } else if (suffix.startsWith("retention/reference-free-proofs/")) {
                        var proof =
                                com.nereusstream.storage.object.retention.M5RetentionCodecV1.decodeReferenceFreeProof(
                                        raw);
                        requireRetentionBinding(binding, proof.identity().binding());
                        expected = retention.referenceFreeProof(proof.proofSha256());
                    } else {
                        throw new IllegalArgumentException("unknown Native retention root kind");
                    }
                }
                default -> throw new IllegalArgumentException("unknown Native materialization root kind");
            }
            // The physical route prepends its namespace; the complete Binding-scoped key must still match.
            if (!fact.key().equals(root + expected.substring(keys.root().length()))) {
                throw new IllegalArgumentException("Native materialization root identity/key differs");
            }
        }
        for (var plan : plans.values()) {
            var taskFact = captured.get(control.materializationTaskKey(plan.taskIdSha256()));
            String cutKey = root
                    + keys.sourceCut(plan.taskIdSha256()).substring(keys.root().length());
            var cutFact = captured.get(cutKey);
            if (taskFact == null
                    || cutFact == null
                    || !cutFact.canonicalStoredBytes()
                            .equals(M5MaterializationCodecV1.encodeSourceCut(plan.sourceCut()))) {
                throw new IllegalStateException("Native materialization plan has an unresolved cut/task admission");
            }
            var task = M5MaterializationCodecV1.decodeTask(taskFact.canonicalStoredBytes());
            if (!task.sourceCutSha256().equals(M5MaterializationCodecV1.sourceCutSha256(plan.sourceCut()))
                    || !task.outputIdentitySha256().equals(plan.outputIdentitySha256())) {
                throw new IllegalStateException("Native materialization task differs from its full original plan");
            }
            var pending = captured.get(control.materializationPendingOutputsKey(plan.taskIdSha256()));
            if (pending != null) {
                M5PendingOutputInventoryV2.decode(pending.canonicalStoredBytes())
                        .requirePlan(plan);
            }
        }
        // A partial register, or an orphaned task/output, cannot be treated as an empty directory.
        for (var fact : captured.values()) {
            String suffix = fact.key().substring(root.length() + 1);
            if (suffix.startsWith("validations/")
                            && !plans.containsKey(
                                    M5MaterializationCodecV1.decodeValidationRoot(fact.canonicalStoredBytes())
                                            .taskIdSha256()
                                            .toHex())
                    || suffix.startsWith("generations/")
                            && !plans.containsKey(M5MaterializationCodecV1.decodeGeneration(fact.canonicalStoredBytes())
                                    .taskIdSha256()
                                    .toHex())) {
                throw new IllegalStateException("Native materialization output lacks its complete original plan");
            }
            if (suffix.startsWith("manifests/")) {
                var manifest = M5MaterializationCodecV1.decodeManifest(fact.canonicalStoredBytes());
                if (!captured.containsKey(control.materializationGenerationKey(manifest.preferredGenerationSha256()))) {
                    throw new IllegalStateException("Native materialization manifest has no retained generation");
                }
            }
            if (suffix.startsWith("tasks/")
                    || suffix.startsWith("source-cuts/")
                    || suffix.startsWith("pending-outputs/")) {
                if (!plans.containsKey(suffix.substring(suffix.indexOf('/') + 1))) {
                    throw new IllegalStateException("Native materialization root lacks its complete original plan");
                }
            }
        }
        if (!plans.isEmpty() && (workerAuthority.isEmpty() || admission.isEmpty())) {
            throw new IllegalStateException(
                    "Native materialization roots lack their actual worker/Cell admission authority");
        }
        var physicalNamespaces = new ArrayList<
                com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.NamespaceSnapshot>();
        var physicalRoutes = new ArrayList<com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2>();
        var facts = new LinkedHashMap<>(captured);
        facts.putAll(controls);
        facts.put(exactScan.key(), exactScan);
        for (var backend : physicalBackends) {
            var route = binding.materializationQuota(backend).join().members();
            var snapshot = route.captureNamespace(
                            com.nereusstream.storage.object.retention.M5RetentionRecordsV1.MAX_REFERENCE_ROWS
                                    - facts.size(),
                            PulsarNativeCursorMetadataV2.MAX_BYTES - bytes)
                    .toCompletableFuture()
                    .join();
            for (var fact : snapshot.facts().values()) {
                if (facts.putIfAbsent(fact.key(), fact) != null) {
                    throw new IllegalArgumentException("Native physical namespace routes overlap");
                }
                bytes = Math.addExact(bytes, fact.canonicalStoredBytes().length());
            }
            physicalNamespaces.add(snapshot);
            physicalRoutes.add(route);
        }
        for (int index = 0; index < physicalNamespaces.size(); index++) {
            var original = physicalNamespaces.get(index);
            var current = physicalRoutes
                    .get(index)
                    .captureNamespace(
                            original.facts().size(),
                            original.facts().values().stream()
                                    .mapToLong(value ->
                                            value.canonicalStoredBytes().length())
                                    .sum())
                    .toCompletableFuture()
                    .join();
            if (!current.binding().equals(original.binding())
                    || !current.facts().equals(original.facts())) {
                throw new IllegalStateException("Native physical namespace changed during root capture");
            }
        }
        control.requireMaterializationNamespaceCurrent(captured);
        var currentReservations = workspace.captureReservations().join();
        if (!currentReservations.equals(reservations)) {
            throw new IllegalStateException("Native Cell reservations changed during root capture");
        }
        var controlFacts = new LinkedHashMap<>(controls);
        reservations.facts().keySet().forEach(controlFacts::remove);
        control.requireMaterializationControlsCurrent(controlFacts);
        trim.requireSourceScanCurrent(exactScan).toCompletableFuture().join();
        binding.revalidate().toCompletableFuture().join();
        return new RetentionRoots(
                facts,
                List.copyOf(plans.values()),
                inputs,
                physicalNamespaces,
                workerAuthority,
                admission,
                reservations.otherReservations(),
                workspace,
                exactScan);
    }

    private static void requireRetentionBinding(
            PulsarNativeBindingV1 binding,
            com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity actual) {
        if (!binding.bindingIdentity().equals(actual)) {
            throw new IllegalArgumentException("Native materialization root belongs to another Binding");
        }
    }

    private final PulsarNativeMaterializationSourceV2 source;
    private final PulsarNativeM4ReadOwnerV1 owner;
    private final PulsarNativeBindingReadViewV1 view;
    private final S3PulsarOffloadObjectStoreV1.MaterializationConnection connection;
    private final CanonicalBytes nativeOwner;
    private final M5MaterializationAdmissionV1 admission;
    private final M5MaterializationAdmissionV1.Caps caps;
    private final M5MaterializationCoordinatorV1 tasks;
    private final M5MaterializationWorkerCoordinatorV1 workers;
    private final PulsarNativeM4ControlV1 control;
    private final boolean forceRewrite;
    private final List<IndexKind> indexes;
    private final CompletableFuture<Void> preparationSettled = new CompletableFuture<>();
    private final Set<CompletableFuture<?>> acceptedMetadata = new HashSet<>();
    private final Set<CompletableFuture<Void>> currentChecks = new HashSet<>();
    private M5MaterializationWorkerCoordinatorV1.Proposal proposal;
    private MaterializationPlan plan;
    private M5MaterializationWorkerCoordinatorV1.Admission worker;
    private M5PendingOutputInventoryV2 pendingOutputs;
    private OxiaQuotaTargetDeleteStoreV2 outputQuota;
    private final List<M5PendingOutputInventoryV2.Slot> birthAttempts = new ArrayList<>();
    private OutputRun outputRun;
    private PulsarNativeMaterializationReadMapV2 readMap;
    private MappedRead mappedRead;
    private VerifiedPulsarOutputs verifiedOutputs;
    private PublicationRun publication;
    private long outputBytes;
    private int unknownSlots;
    private boolean reservationAttempted;
    private volatile boolean stopped;
    private boolean released;

    PulsarNativeMaterializationTaskV2(
            PulsarNativeMaterializationSourceV2 source,
            PulsarNativeM4ReadOwnerV1 owner,
            S3PulsarOffloadObjectStoreV1.MaterializationConnection connection,
            boolean forceRewrite,
            List<IndexKind> indexes,
            M5MaterializationAdmissionV1.Caps caps) {
        this.source = source;
        this.owner = owner;
        view = owner.materializationView();
        this.connection = connection;
        connection.qualifiedProvider();
        this.forceRewrite = forceRewrite;
        this.indexes = List.copyOf(indexes);
        this.caps = Objects.requireNonNull(caps, "configured shared Cell caps");
        nativeOwner = owner.cellBudget().nativeOwnerIdentity(view.binding());
        control = owner.materializationControl();
        admission = control.materializationAdmission(source.nativeInput().protocolCellSha256());
        tasks = control.materializationTasks(this::beforeSelectionDispatch);
        workers = control.materializationWorkers();
    }

    void prepare() throws Exception {
        source.verifyPulsarSource(source.sources().get(0));
        requireRunning();
        await(connection.requireProfileCurrent());
        await(owner.cellBudget().requireNativeOwner(view.binding(), nativeOwner));
        await(view.binding().revalidate());
        requireRunning();
        if (!workers.migrateLegacyAuthority()) {
            throw new IllegalStateException("Native original worker authority migration remains unresolved");
        }
        var profile = connection.profile();
        var input = source.nativeInput();
        proposal = workers.propose(
                        owner.selector(),
                        input.protocolCellSha256(),
                        profile.scopeId().digest(),
                        input.storageFence(),
                        commitment("NEREUS_V2_M5_NATIVE_PULSAR_WORKER_V2", nativeOwner),
                        commitment(
                                "NEREUS_V2_M5_NATIVE_PULSAR_NAMESPACE_V2",
                                profile.physicalBinding().encode()))
                .orElseThrow(() -> new IllegalStateException("Native original worker predecessor changed"));
        var policy = new M5MaterializationPlannerV1.PlanningPolicy(
                PayloadKind.PULSAR_ENTRY_PRESERVING_V1,
                forceRewrite,
                Sha256Digest.hash(profile.encode()),
                commitment("NEREUS_V2_M5_NMS1_NO_COMPRESSION_V2", CanonicalBytes.empty()),
                commitment("NEREUS_V2_M5_NMS1_SHA256_V2", CanonicalBytes.empty()),
                indexes,
                profile.exclusiveNamespacePrefix());
        var confirmed = view.confirmedPosition();
        var cut = new MaterializationSourceCut(
                proposal.identity(),
                owner.selector(),
                Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(owner.selector())),
                view.sha256(),
                ProtocolCoverage.pulsar(source.coverage()),
                -1,
                -1,
                -1,
                -1,
                -1,
                input.protocolStateRootSha256(),
                input.recoveryInputSha256(),
                policyRoot(policy),
                commitment("NEREUS_V2_M5_PULSAR_NMS1_OUTPUT_FORMAT_V2", CanonicalBytes.empty()),
                M5MaterializationCodecV1.calculateSourceSetSha256(source.sources()),
                source.sources(),
                Optional.of(new M5PulsarFrontiersV2(
                        new M5PulsarFrontiersV2.LedgerBoundary(
                                confirmed.getLedgerId(), Math.addExact(confirmed.getEntryId(), 1)),
                        Optional.empty(),
                        Optional.empty())));
        plan = new M5MaterializationPlannerV1().plan(cut, policy); // Retain before admission/plan mutations.
        outputBytes = Math.multiplyExact(
                profile.maximumObjectBytes(),
                Math.addExact(plan.outputParts().size(), plan.indexes().size()));
        unknownSlots = Math.max(
                1, Math.addExact(plan.outputParts().size(), plan.indexes().size()));
        var state = admission.read();
        if (state.isEmpty()) {
            requireAdmission(admission.install(caps));
        } else if (!state.orElseThrow().caps().equals(caps)) {
            throw new IllegalStateException("Native Task configured caps differ from the existing shared Cell caps");
        }
        requireRunning();
        reservationAttempted = true;
        var reservation = admission.reserve(plan, outputBytes, unknownSlots);
        if (reservation == M5MaterializationAdmissionV1.Outcome.REJECTED_CAP
                || reservation == M5MaterializationAdmissionV1.Outcome.CONFLICT) {
            reservationAttempted = false; // No dispatch or a changed predecessor excludes the original charge.
        }
        requireAdmission(reservation);
        requirePublication(tasks.register(plan));
        requireRunning();
        await(owner.cellBudget().requireNativeOwner(view.binding(), nativeOwner));
        await(connection.requireProfileCurrent());
        requireRunning();
        worker = proposal.admitPlan(plan)
                .orElseThrow(
                        () -> new IllegalStateException("Native original complete-plan worker grant is unresolved"));
        requireCurrent();
        pendingOutputs = M5PendingOutputInventoryV2.prepare(plan, connection.profile(), worker.worker());
        requirePublication(tasks.registerPendingOutputs(plan, pendingOutputs));
        requireCurrent();
        outputQuota = await(view.binding().materializationQuota(connection.physical()));
        for (var slot : pendingOutputs.outputs()) {
            requireRunning();
            birthAttempts.add(slot); // Own the same chosen-key claim before any accepted head/credit mutation.
            var result = await(outputQuota.quota().reserveBirth(worker.worker(), slot));
            if (result == BirthResult.EXHAUSTED) {
                birthAttempts.remove(birthAttempts.size() - 1); // Definitive exclusion before any grant dispatch.
            }
            requireBirth(result, BirthResult.RESERVED);
        }
        requireCurrent();
    }

    public synchronized MaterializationPlan plan() {
        requireReady();
        return plan;
    }

    public void requireCurrent() {
        final CompletableFuture<Void> original;
        synchronized (this) {
            requireReady();
            if (!currentChecks.isEmpty()) {
                throw new IllegalStateException("Native Task current-read slot remains occupied");
            }
            original = new CompletableFuture<>();
            currentChecks.add(original); // Before any accepted current-selector/owner/profile/plan I/O.
        }
        try {
            source.requireTaskSourceOpen();
            owner.requireMaterializationCurrent();
            await(owner.cellBudget().requireNativeOwner(view.binding(), nativeOwner));
            await(view.binding().revalidate());
            await(connection.requireProfileCurrent());
            var expected = new M5MaterializationAdmissionV1.Reservation(
                    plan.taskIdSha256(),
                    plan.sourceCut().sources().stream()
                            .mapToLong(value -> value.canonicalLength())
                            .reduce(0L, Math::addExact),
                    outputBytes,
                    plan.sourceCut().sources().size(),
                    plan.outputParts().size(),
                    plan.indexes().size(),
                    unknownSlots);
            var state = admission.read().orElseThrow(() -> new IllegalStateException("Native Cell charge is missing"));
            if (!state.caps().equals(caps) || !state.reservations().contains(expected)) {
                throw new IllegalStateException("Native original complete Cell reservation changed");
            }
            worker.requireCurrentPlan(plan);
            if (pendingOutputs != null
                    && !tasks.readPendingOutputs(plan.taskIdSha256()).equals(Optional.of(pendingOutputs))) {
                throw new IllegalStateException("Native original pending-output inventory changed or is unresolved");
            }
            if (outputQuota != null) {
                for (var slot : birthAttempts) {
                    var credit = await(outputQuota.quota().readBirth(birthKey(slot)))
                            .orElseThrow(() -> new IllegalStateException("Native original birth credit is missing"));
                    if (!(outputRun == null
                                    ? credit.state() == M5ObjectBirthCreditV3.State.RESERVED
                                    : outputRun.matches(credit))
                            || !credit.worker().equals(worker.worker())
                            || !credit.slot().equals(slot)) {
                        throw new IllegalStateException("Native original birth credit is changed or terminal");
                    }
                }
            }
            requireRunning(); // Close/fence during the accepted read prevents a usable result.
        } catch (Exception failure) {
            throw new IllegalStateException("Native original Task current qualification remains unresolved", failure);
        } finally {
            acceptedMetadataDrain().thenRun(() -> {
                synchronized (this) {
                    currentChecks.remove(original);
                }
                original.complete(null);
            });
        }
    }

    void preparationSettled() {
        preparationSettled.complete(null); // Precedes observer delivery, including failure delivery.
    }

    synchronized void stop() {
        stopped = true;
    }

    /** Stop and drain original work before unstarted cancellation or unpublished quarantine. No delete authority. */
    CompletableFuture<Void> closeUnstarted() {
        stop();
        return preparationSettled
                .thenCompose(ignored -> currentChecksDrain())
                .thenCompose(ignored -> acceptedMetadataDrain())
                .thenCompose(
                        ignored -> outputRun == null ? CompletableFuture.completedFuture(null) : outputRun.finished)
                .thenCompose(
                        ignored -> mappedRead == null ? CompletableFuture.completedFuture(null) : mappedRead.workEnded)
                .thenCompose(ignored ->
                        publication == null ? CompletableFuture.completedFuture(null) : publication.workEnded)
                .thenCompose(ignored -> acceptedMetadataDrain())
                .thenRun(() -> {
                    if (released || !reservationAttempted) {
                        return;
                    }
                    if (outputRun != null && outputRun.hasDispatch()) {
                        if (mappedRead != null) {
                            mappedRead.closeOriginal();
                        }
                        outputRun.closeDispatched();
                        if (publication == null || !publication.resolveForClose()) {
                            requirePublication(tasks.quarantineUnpublished(plan));
                        }
                        outputRun.releaseWorkspace();
                        requireAdmission(admission.release(plan.taskIdSha256()));
                        released = true;
                        return;
                    }
                    var original = tasks.readTask(plan.taskIdSha256());
                    if (original.isEmpty()) {
                        requirePublication(
                                tasks.register(plan)); // Resolve the same possible late immutable creates first.
                        original = tasks.readTask(plan.taskIdSha256());
                    }
                    if (original.isPresent()) {
                        if (original.orElseThrow().state() != TaskState.PLANNED
                                && original.orElseThrow().state() != TaskState.CANCELLED_STALE) {
                            throw new IllegalStateException(
                                    "Native unstarted Task has unexpected output/terminal state");
                        }
                        if (original.orElseThrow().state() == TaskState.PLANNED
                                && worker == null
                                && owner.materializationLocalCurrent()
                                && owner.materializationControl()
                                        .coordinator()
                                        .readSelector()
                                        .filter(owner.selector()::equals)
                                        .isPresent()) {
                            // Settle the original complete-plan grant before terminal cancellation. This consumes
                            // its original epoch even when the preparation reply was lost before local admission,
                            // so the same live native owner can start its next epoch without a cancelled task-ID
                            // collision. The stopped actor never delivers this token or dispatches output.
                            try {
                                await(owner.cellBudget().requireNativeOwner(view.binding(), nativeOwner));
                                await(view.binding().revalidate());
                                await(connection.requireProfileCurrent());
                            } catch (Exception failure) {
                                throw new IllegalStateException(
                                        "Original unstarted worker settlement is unresolved", failure);
                            }
                            worker = proposal.admitPlan(plan).orElse(null);
                        }
                        if (pendingOutputs != null) {
                            var inventory = tasks.readPendingOutputs(plan.taskIdSha256());
                            if (!inventory.equals(Optional.of(pendingOutputs.cancelUnstarted()))) {
                                // Confirm any late original create-only write before installing its terminal.
                                requirePublication(tasks.registerPendingOutputs(plan, pendingOutputs));
                                requirePublication(tasks.cancelUnstartedOutputs(plan, pendingOutputs));
                            }
                        }
                        // This actor exposes no output dispatch. The private preparation/current callbacks have
                        // drained and its original inventory is terminal before any future-capacity refund.
                        if (outputQuota != null) {
                            try {
                                if (!await(outputQuota.quota().recover())) {
                                    throw new IllegalStateException(
                                            "Native original birth head recovery is unresolved");
                                }
                            } catch (Exception failure) {
                                throw new IllegalStateException(
                                        "Native original birth head recovery is unresolved", failure);
                            }
                        }
                        for (var slot : birthAttempts) {
                            try {
                                var result = await(outputQuota.quota().reserveBirth(worker.worker(), slot));
                                if (result != BirthResult.CANCELLED_NO_DISPATCH) {
                                    if (result != BirthResult.WRITE_ARMED) {
                                        requireBirth(result, BirthResult.RESERVED);
                                    }
                                    requireBirth(
                                            await(outputQuota.quota().cancelNoDispatchBirth(birthKey(slot))),
                                            BirthResult.CANCELLED_NO_DISPATCH);
                                }
                            } catch (Exception failure) {
                                throw new IllegalStateException(
                                        "Native original no-dispatch birth settlement unresolved", failure);
                            }
                        }
                        requirePublication(tasks.cancelUnstarted(plan));
                    }
                    if (outputRun != null) {
                        outputRun.releaseWorkspace();
                    }
                    // Exact reread/retry reserves the original charge before removing it. An old UNKNOWN CAS cannot
                    // install a free late charge after this successor; it uses its captured predecessor generation.
                    requireAdmission(admission.reserve(plan, outputBytes, unknownSlots));
                    requireAdmission(admission.release(plan.taskIdSha256()));
                    released = true;
                });
    }

    /** One original output actor. Public completion/cancellation cannot settle its private SDK or metadata work. */
    public CompletableFuture<VerifiedPulsarOutputs> writeAndVerify(
            Path stagingRoot, long maximumWorkingBytes, Executor executor) {
        final OutputRun original;
        synchronized (this) {
            requireReady();
            if (outputRun != null
                    || plan.representationMode() != RepresentationMode.REWRITE_GENERATION
                    || maximumWorkingBytes <= 0) {
                throw new IllegalStateException("Native Task requires its sole bounded original rewrite attempt");
            }
            original = new OutputRun(stagingRoot, maximumWorkingBytes);
            outputRun = original; // Before native workspace reservation, local files, write intent or SDK dispatch.
        }
        var observer = new CompletableFuture<VerifiedPulsarOutputs>();
        try {
            CompletableFuture.runAsync(
                    () -> {
                        VerifiedPulsarOutputs verified = null;
                        Throwable rejected = null;
                        try {
                            if (observer.isDone()) {
                                throw new java.util.concurrent.CancellationException(
                                        "output observer ended before dispatch");
                            }
                            verified = original.run();
                        } catch (Throwable failure) {
                            rejected = failure;
                        } finally {
                            original.finished.complete(null);
                        }
                        if (rejected == null) {
                            observer.complete(verified);
                        } else {
                            observer.completeExceptionally(rejected);
                        }
                    },
                    executor);
        } catch (Throwable rejection) {
            original.finished.complete(null);
            observer.completeExceptionally(rejection);
        }
        return observer;
    }

    /** Complete routing data from this original verified attempt. This is not a selected M4 read grant. */
    public synchronized PulsarNativeMaterializationReadMapV2 readMap() {
        requireReady();
        return Objects.requireNonNull(readMap, "original complete rewrite map is not verified");
    }

    /** One original publication candidate; retries never rebuild outputs or issue another PUT. */
    public CompletableFuture<Publication> publish(Executor executor) {
        final PublicationRun original;
        final CompletableFuture<Void> ended;
        synchronized (this) {
            requireReady();
            if (verifiedOutputs == null
                    || readMap == null
                    || !outputRun.finished.isDone()
                    || !currentChecks.isEmpty()
                    || mappedRead != null && (!mappedRead.workEnded.isDone() || !mappedRead.released)) {
                throw new IllegalStateException("Native publication requires its original verified/drained output");
            }
            if (publication == null) {
                publication = new PublicationRun();
            }
            original = publication;
            if (!original.workEnded.isDone()) {
                throw new IllegalStateException("Native original publication attempt remains pending");
            }
            ended = new CompletableFuture<>();
            original.workEnded = ended;
        }
        var observer = new CompletableFuture<Publication>();
        try {
            CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    if (observer.isDone() && !original.selectorAttempted) {
                                        throw new java.util.concurrent.CancellationException(
                                                "publication observer ended before dispatch");
                                    }
                                    return original.prepareDrain();
                                } catch (Exception failure) {
                                    throw new java.util.concurrent.CompletionException(failure);
                                }
                            },
                            executor)
                    .thenCompose(drain -> drain)
                    .thenComposeAsync(
                            ignored -> {
                                try {
                                    return original.publishSelected(executor);
                                } catch (Exception failure) {
                                    return CompletableFuture.failedFuture(failure);
                                }
                            },
                            executor)
                    .whenComplete((result, failure) -> {
                        ended.complete(null); // Covers the original asynchronous drain and metadata handoff.
                        if (failure == null) {
                            observer.complete(result);
                        } else {
                            observer.completeExceptionally(failure);
                        }
                    });
        } catch (Throwable rejected) {
            ended.complete(null);
            observer.completeExceptionally(rejected);
        }
        return observer;
    }

    /** Exact native publication receipt/data. This does not install a reader or grant recovery/deletion. */
    public static final class Publication {
        private final ValidatedGeneration validated;
        private final BindingReadSelector selector;
        private final VersionedValue selectorFact;
        private final PulsarNativeMaterializationReadMapV2 map;
        private final List<SourceProtectionIdentity> sources;

        private Publication(
                ValidatedGeneration validated,
                BindingReadSelector selector,
                VersionedValue selectorFact,
                PulsarNativeMaterializationReadMapV2 map,
                List<SourceProtectionIdentity> sources) {
            this.validated = validated;
            this.selector = selector;
            this.selectorFact = selectorFact;
            this.map = map;
            this.sources = List.copyOf(sources);
        }

        public ValidatedGeneration validated() {
            return validated;
        }

        public BindingReadSelector selector() {
            return selector;
        }

        public VersionedValue selectorFact() {
            return selectorFact;
        }

        public PulsarNativeMaterializationReadMapV2 readMap() {
            return map;
        }

        public List<SourceProtectionIdentity> protectedSources() {
            return sources;
        }
    }

    private void beforeSelectionDispatch() {
        synchronized (this) {
            if (publication == null
                    || !publication.ownerDrained
                    || !publication.qualified
                    || stopped && !publication.selectorAttempted) {
                throw new IllegalStateException("Native original publication is stopped or lacks confirmed drain");
            }
            publication.selectorAttempted = true; // Before the original native selector mutation; no I/O under monitor.
        }
    }

    /** One original source import, owned by the publication and executed only under its original C writer. */
    final class SourcePhysicalAdmission implements PulsarNativeControlWriterV2.PhysicalWriter {
        private final List<com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityV1>
                candidates = new ArrayList<>();
        private final Map<PhysicalResourceIdV2, OxiaQuotaTargetDeleteStoreV2> routes = new java.util.TreeMap<>();

        private record Header(org.apache.bookkeeper.versioning.Version version, Sha256Digest bytes) {}

        private final Map<PhysicalResourceIdV2, Header> headers = new java.util.TreeMap<>();
        private final Sha256Digest root;
        private CompletableFuture<Void> settled = CompletableFuture.completedFuture(null);
        private boolean attempted;
        private boolean complete;

        SourcePhysicalAdmission(List<PhysicalResourceIdV2> resources) throws Exception {
            var profile = connection.profile();
            OxiaQuotaTargetDeleteStoreV2 bkRoute = null;
            final Sha256Digest implementation;
            try (var bytes = PulsarNativeOffloadedReferenceV2.class.getResourceAsStream(
                    "PulsarNativeOffloadedReferenceV2.class")) {
                implementation = Sha256Digest.hash(
                        CanonicalBytes.copyOf(Objects.requireNonNull(bytes).readAllBytes()));
            }
            var encoded = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(encoded)) {
                out.writeInt(0x4e505349); // NPSI
                out.writeInt(1);
                out.write(Sha256Digest.hash(nativeOwner).bytes().toByteArray());
                out.write(Sha256Digest.hash(M5MaterializationCodecV1.encodePlan(plan))
                        .bytes()
                        .toByteArray());
                for (var resource : resources) {
                    var object = resource instanceof PhysicalResourceIdV2.ObjectVersion;
                    var capability = object
                            ? profile.providerConfigurationSha256()
                            : source.bookKeeper().capability().configurationDigest();
                    if (!object && bkRoute == null) {
                        bkRoute = await(view.binding().materializationQuota(source.bookKeeper()));
                    }
                    var route = object ? outputQuota : bkRoute;
                    routes.put(resource, route);
                    Sha256Digest witness;
                    Sha256Digest policy;
                    boolean legacy = false;
                    if (object) {
                        var slot = view.materializationDirectory().stream()
                                .filter(value -> value.members().contains(resource))
                                .findFirst();
                        if (slot.isPresent()) {
                            witness = Sha256Digest.hash(slot.orElseThrow().encode());
                        } else {
                            var credit = view.materializedOutput(resource)
                                    .or(() -> readMap.readableOutputs().stream()
                                            .filter(value -> value.resource().equals(Optional.of(resource)))
                                            .findFirst())
                                    .orElseThrow(() -> new IllegalStateException(
                                            "original Native source has no captured output descriptor"));
                            witness = Sha256Digest.hash(credit.encode());
                        }
                        policy = Sha256Digest.hash(profile.encode());
                    } else {
                        var id = ((PhysicalResourceIdV2.BookKeeperLedger) resource).ledgerId();
                        var header = await(source.bookKeeper()
                                .borrowClient()
                                .getLedgerManager()
                                .readLedgerMetadata(id));
                        var metadata = header.getValue();
                        legacy = !metadata.getCustomMetadata().containsKey("nereus/m5-pulsar-create-scope");
                        if (!metadata.isClosed()
                                || !java.util.Arrays.equals(
                                        metadata.getCustomMetadata().get("pulsar/managed-ledger"),
                                        view.binding()
                                                .managedLedgerName()
                                                .getBytes(java.nio.charset.StandardCharsets.UTF_8))
                                || !java.util.Arrays.equals(
                                        metadata.getCustomMetadata().get("application"),
                                        "pulsar".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                                || !java.util.Arrays.equals(
                                        metadata.getCustomMetadata().get("component"),
                                        "managed-ledger".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                                || metadata.getCustomMetadata().containsKey("pulsar/cursor")) {
                            throw new IllegalStateException(
                                    "Native original source is not its actual CLOSED data ledger");
                        }
                        com.nereusstream.storage.bookkeeper.M5PulsarNativeCreateSpecV1.requireBoundedMetadata(metadata);
                        var raw = CanonicalBytes.copyOf(
                                new org.apache.bookkeeper.meta.LedgerMetadataSerDe().serialize(metadata));
                        headers.put(resource, new Header(header.getVersion(), Sha256Digest.hash(raw)));
                        policy = Sha256Digest.hash(CanonicalBytes.copyOf(java.nio.ByteBuffer.allocate(132)
                                .putInt(0x4e505342)
                                .put(resource.sha256().bytes().toByteArray())
                                .put(Sha256Digest.hash(view.bookKeeperNamespaceBinding()
                                                .encode())
                                        .bytes()
                                        .toByteArray())
                                .put(Sha256Digest.hash(raw).bytes().toByteArray())
                                .put(view.binding()
                                        .authority()
                                        .aggregate()
                                        .canonicalStoredDigest()
                                        .bytes()
                                        .toByteArray())
                                .array()));
                        witness = policy;
                        var after = await(source.bookKeeper()
                                .borrowClient()
                                .getLedgerManager()
                                .readLedgerMetadata(id));
                        if (!after.getVersion().equals(header.getVersion())
                                || !java.util.Arrays.equals(
                                        new org.apache.bookkeeper.meta.LedgerMetadataSerDe()
                                                .serialize(after.getValue()),
                                        raw.toByteArray())) {
                            throw new IllegalStateException("Native original source SDK header changed during capture");
                        }
                    }
                    var found = await(new com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1(route)
                            .inspect(resource.authorityKey()));
                    var enrollment = new com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                            .ProofBoundWriterEnrollmentV1(
                            List.of(com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                    .ProofBoundWriterClassV1.values()),
                            capability,
                            implementation,
                            policy);
                    if (found.isPresent()) {
                        var actual = found.orElseThrow();
                        var full = actual.fullAuthority().orElseThrow();
                        if (actual.state()
                                        != com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                                .TargetDeleteAuthorityStateV1.OPEN_V1
                                || !full.writerEnrollment()
                                        .capabilitySetRootSha256()
                                        .equals(capability)
                                || (object || legacy)
                                        && !full.writerEnrollment()
                                                .policyRootSha256()
                                                .equals(policy)) {
                            throw new IllegalStateException(
                                    "Native original source authority is fenced or has another profile");
                        }
                        candidates.add(com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityStateMachineV1.open(
                                full.target(), full.writerEnrollment(), witness));
                    } else {
                        if (!object) {
                            var header = await(source.bookKeeper()
                                    .borrowClient()
                                    .getLedgerManager()
                                    .readLedgerMetadata(((PhysicalResourceIdV2.BookKeeperLedger) resource).ledgerId()));
                            if (header.getValue().getCustomMetadata().containsKey("nereus/m5-pulsar-create-scope")) {
                                throw new IllegalStateException(
                                        "tagged Native source has no original physical authority");
                            }
                        }
                        candidates.add(com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityStateMachineV1.open(
                                com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                        .PhysicalDeleteTargetV1.create(resource),
                                enrollment,
                                witness));
                    }
                    out.write(resource.sha256().bytes().toByteArray());
                    out.write(witness.bytes().toByteArray());
                    var admitted = candidates.get(candidates.size() - 1).writerEnrollment();
                    out.write(admitted.capabilitySetRootSha256().bytes().toByteArray());
                    out.write(admitted.implementationRootSha256().bytes().toByteArray());
                    out.write(admitted.policyRootSha256().bytes().toByteArray());
                }
            }
            root = Sha256Digest.hash(CanonicalBytes.copyOf(encoded.toByteArray()));
        }

        public Sha256Digest root() {
            return root;
        }

        public long writerDescriptorBytes() {
            return Math.multiplyExact(routes.size(), 512L);
        }

        public synchronized CompletableFuture<Void> admit() {
            if (complete) {
                return verify();
            }
            if (attempted) {
                return reconcile();
            }
            attempted = true;
            CompletableFuture<Void> work = CompletableFuture.completedFuture(null);
            for (var candidate : candidates) {
                var resource = candidate.target().resourceId();
                var route = routes.get(resource);
                work = work.thenCompose(unused -> owner.cellBudget().requireNativeOwner(view.binding(), nativeOwner))
                        .thenCompose(unused -> view.binding().revalidate())
                        .thenCompose(unused -> connection.requireProfileCurrent())
                        .thenCompose(unused ->
                                new com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1(route)
                                        .inspect(resource.authorityKey()))
                        .thenCompose(found -> {
                            if (found.isPresent()) {
                                requireEnrollment(
                                        candidate,
                                        found.orElseThrow().fullAuthority().orElseThrow());
                                return CompletableFuture.<Void>completedFuture(null);
                            }
                            return new com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1(route)
                                    .create(candidate)
                                    .thenAccept(value -> {
                                        if (!value.exactCandidateIsAuthoritative()) {
                                            throw new IllegalStateException(
                                                    "original source physical admission remains unknown");
                                        }
                                    });
                        })
                        .thenCompose(unused -> route.members()
                                .register(
                                        resource,
                                        connection.profile().protocolCellSha256(),
                                        new com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2
                                                .Member(
                                                com.nereusstream.storage.object.materialization
                                                        .M5MaterializationRecordsV1.PositionDomain.PULSAR_ENTRY,
                                                resource instanceof PhysicalResourceIdV2.ObjectVersion
                                                        ? connection
                                                                .profile()
                                                                .scopeId()
                                                                .digest()
                                                        : source.bookKeeper()
                                                                .capability()
                                                                .providerScopeId()
                                                                .digest(),
                                                view.bindingIdentity()),
                                        candidate.writerEnrollment().capabilitySetRootSha256(),
                                        Sha256Digest.hash(nativeOwner)))
                        .thenApply(unused -> null);
            }
            settled = work;
            return work.thenCompose(unused -> verify()).thenRun(() -> complete = true);
        }

        public synchronized CompletableFuture<Void> reconcile() {
            if (!attempted) {
                return CompletableFuture.completedFuture(null);
            }
            return settled.handle((unused, failure) -> null)
                    .thenCompose(unused -> verify())
                    .thenCompose(unused -> reconcileMembers())
                    .thenRun(() -> complete = true);
        }

        private com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.Member member(
                PhysicalResourceIdV2 resource) {
            return new com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.Member(
                    com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain
                            .PULSAR_ENTRY,
                    resource instanceof PhysicalResourceIdV2.ObjectVersion
                            ? connection.profile().scopeId().digest()
                            : source.bookKeeper().capability().providerScopeId().digest(),
                    view.bindingIdentity());
        }

        private void requireEnrollment(
                com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityV1 expected,
                com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityV1 actual) {
            if (actual.state()
                            != com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                    .TargetDeleteAuthorityStateV1.OPEN_V1
                    || !actual.target().equals(expected.target())
                    || !actual.writerEnrollment().equals(expected.writerEnrollment())) {
                throw new IllegalStateException("original source physical enrollment changed or is fenced");
            }
        }

        public com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2 writerGuard() {
            var nativeRoutes = routes.entrySet().stream()
                    .collect(java.util.stream.Collectors.toUnmodifiableMap(
                            value -> value.getKey().authorityKey(), Map.Entry::getValue));
            return new com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2(
                    new com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1(
                            new PulsarNativeMetadataWriterV2.PhysicalRoutes(nativeRoutes)));
        }

        public Map<PhysicalResourceIdV2, com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Context>
                writerContexts(
                        Sha256Digest mutationRoot,
                        com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1 kind) {
            return candidates.stream()
                    .collect(java.util.stream.Collectors.toUnmodifiableMap(
                            value -> value.target().resourceId(),
                            value -> new com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Context(
                                    com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                            .ProofBoundWriterClassV1.M4_SOURCE_PROTECTION_RELEASE_V1,
                                    value.writerEnrollment().capabilitySetRootSha256(),
                                    Sha256Digest.hash(nativeOwner),
                                    mutationRoot)));
        }

        private CompletableFuture<Void> reconcileMembers() {
            CompletableFuture<Void> work = CompletableFuture.completedFuture(null);
            for (var candidate : candidates) {
                var resource = candidate.target().resourceId();
                work = work.thenCompose(unused -> routes.get(resource)
                                .members()
                                .reconcileRegistration(
                                        resource,
                                        connection.profile().protocolCellSha256(),
                                        member(resource),
                                        candidate.writerEnrollment().capabilitySetRootSha256(),
                                        Sha256Digest.hash(nativeOwner)))
                        .thenApply(unused -> null);
            }
            return work;
        }

        private CompletableFuture<Void> requireHeader(PhysicalResourceIdV2 resource) {
            var expected = headers.get(resource);
            if (expected == null) {
                return CompletableFuture.completedFuture(null);
            }
            return source.bookKeeper()
                    .borrowClient()
                    .getLedgerManager()
                    .readLedgerMetadata(((PhysicalResourceIdV2.BookKeeperLedger) resource).ledgerId())
                    .thenAccept(value -> {
                        try {
                            com.nereusstream.storage.bookkeeper.M5PulsarNativeCreateSpecV1.requireBoundedMetadata(
                                    value.getValue());
                            var raw = CanonicalBytes.copyOf(
                                    new org.apache.bookkeeper.meta.LedgerMetadataSerDe().serialize(value.getValue()));
                            if (!value.getVersion().equals(expected.version())
                                    || !Sha256Digest.hash(raw).equals(expected.bytes())) {
                                throw new IllegalStateException(
                                        "original Native source SDK header changed before dispatch");
                            }
                        } catch (java.io.IOException failure) {
                            throw new java.util.concurrent.CompletionException(failure);
                        }
                    });
        }

        private CompletableFuture<Void> verify() {
            CompletableFuture<Void> work = CompletableFuture.completedFuture(null);
            for (var candidate : candidates) {
                var resource = candidate.target().resourceId();
                var route = routes.get(resource);
                work = work.thenCompose(unused -> owner.cellBudget().requireNativeOwner(view.binding(), nativeOwner))
                        .thenCompose(unused -> view.binding().revalidate())
                        .thenCompose(unused -> connection.requireProfileCurrent())
                        .thenCompose(unused -> requireHeader(resource))
                        .thenCompose(unused -> route.readResourceFact(resource))
                        .thenCompose(fact -> {
                            requireEnrollment(
                                    candidate,
                                    com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                                            fact.authority().canonicalStoredBytes()));
                            return route.members().readFact(resource);
                        })
                        .thenAccept(member -> {
                            if (!member.inventory()
                                            .protocolCell()
                                            .equals(connection.profile().protocolCellSha256())
                                    || member.inventory().members().stream()
                                            .noneMatch(value -> value.equals(member(resource)))) {
                                throw new IllegalStateException("original source physical member is unresolved");
                            }
                        });
            }
            return work;
        }
    }

    /** Original SDK work must hand off even unselected/quarantined outputs before releasing its charge. */
    private void registerOriginalOutput(M5ObjectBirthCreditV3 credit) throws Exception {
        var profile = connection.profile();
        final Sha256Digest implementation;
        try (var bytes = PulsarNativeMaterializationTaskV2.class.getResourceAsStream(
                "PulsarNativeMaterializationTaskV2.class")) {
            implementation = Sha256Digest.hash(
                    CanonicalBytes.copyOf(Objects.requireNonNull(bytes).readAllBytes()));
        }
        var enrollment =
                new com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterEnrollmentV1(
                        List.of(com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                .ProofBoundWriterClassV1.values()),
                        profile.providerConfigurationSha256(),
                        implementation,
                        Sha256Digest.hash(profile.encode()));
        var targets = new com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1(outputQuota);
        var resource = credit.resource().orElseThrow();
        if (credit.state() != M5ObjectBirthCreditV3.State.ASSIGNED
                || !resource.namespace().equals(profile.physicalBinding().physicalNamespace())) {
            throw new IllegalStateException("Native verified output physical identity differs");
        }
        var found = await(targets.inspect(resource.authorityKey()));
        if (found.isEmpty()) {
            var candidate = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityStateMachineV1.open(
                    com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.PhysicalDeleteTargetV1.create(
                            resource),
                    enrollment,
                    Sha256Digest.hash(credit.encode()));
            if (!await(targets.create(candidate)).exactCandidateIsAuthoritative()) {
                throw new IllegalStateException("Native verified output authority remains unresolved");
            }
        } else if (found.orElseThrow().state()
                        != com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                .TargetDeleteAuthorityStateV1.OPEN_V1
                || found.orElseThrow()
                        .fullAuthority()
                        .filter(value -> value.writerEnrollment()
                                        .capabilitySetRootSha256()
                                        .equals(profile.providerConfigurationSha256())
                                && value.writerEnrollment()
                                        .policyRootSha256()
                                        .equals(Sha256Digest.hash(profile.encode())))
                        .isEmpty()) {
            throw new IllegalStateException("Native verified output is fenced or has another physical scope");
        }
        await(outputQuota
                .members()
                .register(
                        resource,
                        profile.protocolCellSha256(),
                        new com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.Member(
                                com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1
                                        .PositionDomain.PULSAR_ENTRY,
                                profile.scopeId().digest(),
                                view.bindingIdentity()),
                        profile.providerConfigurationSha256(),
                        Sha256Digest.hash(nativeOwner)));
        await(outputQuota.readResourceFact(resource));
    }

    private final class PublicationRun {
        private CompletableFuture<Void> workEnded = CompletableFuture.completedFuture(null);
        private ValidatedGeneration validated;
        private List<SourceProtectionIdentity> sources;
        private Publication result;
        private boolean ownerDrained;
        private boolean qualified;
        private boolean selectorAttempted;
        private boolean physicalReferencesAdmitted;
        private boolean physicalReferenceAdmissionAttempted;
        private Map<PhysicalResourceIdV2, OxiaQuotaTargetDeleteStoreV2> originalReferenceRoutes;
        private List<PhysicalResourceIdV2> originalSourceReferences;
        private SourcePhysicalAdmission originalSourceAdmission;
        private com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2 physicalPublicationGuard;
        private Map<PhysicalResourceIdV2, com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Context>
                physicalPublicationContexts;
        private com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Result<Optional<Publication>>
                physicalPublicationResult;
        private Sha256Digest physicalPublicationRoot;
        private boolean physicalPublicationAttempted;
        private boolean physicalMetadataDispatched;

        private void admitVerifiedPhysicalReferences() throws Exception {
            if (physicalReferencesAdmitted) {
                return;
            }
            physicalReferenceAdmissionAttempted = true;
            for (var credit : readMap.readableOutputs()) {
                await(owner.cellBudget().requireNativeOwner(view.binding(), nativeOwner));
                await(view.binding().revalidate());
                await(connection.requireProfileCurrent());
                var resource = credit.resource().orElseThrow();
                await(outputQuota.readResourceFact(resource));
                await(outputQuota.members().readFact(resource));
            }
            physicalReferencesAdmitted = true;
        }

        private void requireOriginalPhysicalReferences() throws Exception {
            var references = new java.util.TreeSet<PhysicalResourceIdV2>();
            readMap.readableOutputs()
                    .forEach(value -> references.add(value.resource().orElseThrow()));
            references.addAll(Objects.requireNonNull(originalSourceReferences, "original source references"));
            if (references.size() > com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.MAX_TARGETS) {
                throw new IllegalStateException("Native publication physical references exceed the original bound");
            }
            var routes = new java.util.TreeMap<PhysicalResourceIdV2, OxiaQuotaTargetDeleteStoreV2>();
            OxiaQuotaTargetDeleteStoreV2 bookKeeperRoute = null;
            for (var resource : references) {
                var object = resource instanceof PhysicalResourceIdV2.ObjectVersion;
                if (!object && !(resource instanceof PhysicalResourceIdV2.BookKeeperLedger)) {
                    throw new IllegalArgumentException("Native publication has an unsupported physical reference");
                }
                if (!object && bookKeeperRoute == null) {
                    bookKeeperRoute = await(view.binding().materializationQuota(source.bookKeeper()));
                }
                var route = object ? outputQuota : bookKeeperRoute;
                try {
                    await(route.requireActiveResource(resource));
                } catch (Exception failure) {
                    throw new IllegalStateException(
                            "Native original source physical admission is missing: "
                                    + resource.getClass().getSimpleName() + " "
                                    + resource.sha256().toHex(),
                            failure);
                }
                var member = await(route.members().readFact(resource));
                var provider = object
                        ? connection.profile().scopeId().digest()
                        : source.bookKeeper().capability().providerScopeId().digest();
                if (!member.inventory()
                                .protocolCell()
                                .equals(connection.profile().protocolCellSha256())
                        || !member.inventory()
                                .members()
                                .contains(
                                        new com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2
                                                .Member(
                                                com.nereusstream.storage.object.materialization
                                                        .M5MaterializationRecordsV1.PositionDomain.PULSAR_ENTRY,
                                                provider,
                                                view.bindingIdentity()))) {
                    throw new IllegalStateException("Native publication has no original physical source member");
                }
                routes.put(resource, route);
            }
            originalReferenceRoutes = Map.copyOf(routes);
        }

        private CompletableFuture<Void> prepareDrain() throws Exception {
            if (result != null || ownerDrained) {
                return CompletableFuture.completedFuture(null);
            }
            if (validated == null) {
                requireCurrent();
                var originalReferences = new java.util.TreeSet<PhysicalResourceIdV2>();
                for (var range : source.coverage().ranges()) {
                    for (var slice : view.materializationSlices(List.of(range))) {
                        originalReferences.addAll(view.physicalDataReferences(slice));
                        if (originalReferences.size()
                                > com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.MAX_TARGETS) {
                            throw new IllegalStateException("Native original source references exceed their bound");
                        }
                    }
                }
                originalSourceReferences = List.copyOf(originalReferences);
                originalSourceAdmission = new SourcePhysicalAdmission(originalSourceReferences);
                var predecessor = owner.selector();
                sources = predecessor.mode() == SelectorMode.PREFERRED_ONLY
                        ? plan.sourceCut().sources().stream()
                                .map(source -> new SourceProtectionIdentity(
                                        source.sourceIdentitySha256(),
                                        plan.sourceCut().identity().workerEpoch(),
                                        Math.addExact(predecessor.readAdmissionEpoch(), 1),
                                        Math.addExact(predecessor.sourceGeneration(), 1),
                                        predecessor.capability()))
                                .sorted(Comparator.comparing(
                                        source -> source.sourceIdentitySha256().toHex()))
                                .toList()
                        : owner.fallbackSources();
                // A mixed cut cannot silently substitute the old fallback set. Existing fused retirement precedes
                // capture when its membership differs; every actual source identity must be protected exactly.
                validated =
                        new M5MaterializationValidatorV1().finalizePulsarOutputs(verifiedOutputs, predecessor, sources);
            }
            if (!ownerDrained) {
                requireCurrent();
                for (var identity : sources) {
                    var outcome = control.coordinator(originalSourceAdmission)
                            .createProtection(new SourceProtection(
                                    view.bindingIdentity(),
                                    identity,
                                    ProtectionState.PROTECTED,
                                    Optional.empty(),
                                    Optional.empty()));
                    if (outcome != M4ReadControlCoordinatorV1.Outcome.APPLIED
                            && outcome != M4ReadControlCoordinatorV1.Outcome.EXISTING_EXACT) {
                        throw new IllegalStateException("Native original source protection is unresolved: " + outcome);
                    }
                }
                requireCurrent();
                return owner.fence(); // Never block the SDK/control executor that must finish this drain.
            }
            return CompletableFuture.completedFuture(null);
        }

        private CompletableFuture<Publication> publishSelected(Executor executor) throws Exception {
            if (result != null) {
                reconcilePhysicalPublication();
                return CompletableFuture.completedFuture(result);
            }
            if (!ownerDrained) {
                owner.requireConfirmedDrain(control);
                owner.persistMaterializationDrain(); // Exact durable predecessor evidence before selector publication.
                ownerDrained = true;
            }
            await(owner.cellBudget().requireNativeOwner(view.binding(), nativeOwner));
            await(view.binding().revalidate());
            await(connection.requireProfileCurrent());
            admitVerifiedPhysicalReferences();
            requireOriginalPhysicalReferences();
            qualified = true;
            if (physicalPublicationAttempted
                    && !physicalMetadataDispatched
                    && physicalPublicationResult != null
                    && physicalPublicationResult.terminalCleanup().isPresent()) {
                reconcilePhysicalPublication();
                // The original nonce is conclusively no-dispatch and drained. A retry gets new tickets;
                // it retains this actor's verified outputs and never resumes the old nonce.
                physicalPublicationAttempted = false;
                physicalPublicationResult = null;
            }
            if (physicalPublicationAttempted) {
                result = observeOriginalPublication();
                reconcilePhysicalPublication();
                return CompletableFuture.completedFuture(result);
            }
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(0x4e505057); // NPPW
                out.writeInt(2);
                out.write(Sha256Digest.hash(nativeOwner).bytes().toByteArray());
                out.write(Sha256Digest.hash(M5MaterializationCodecV1.encodePlan(plan))
                        .bytes()
                        .toByteArray());
                out.write(M5MaterializationCodecV1.manifestSha256(validated.manifestView())
                        .bytes()
                        .toByteArray());
                out.write(readMap.sha256().bytes().toByteArray());
                var credits = readMap.readableOutputs().stream()
                        .sorted(Comparator.comparing(
                                value -> value.resource().orElseThrow().sha256().toHex()))
                        .toList();
                out.writeInt(credits.size());
                for (var credit : credits) {
                    out.write(credit.resource().orElseThrow().sha256().bytes().toByteArray());
                    out.write(Sha256Digest.hash(credit.encode()).bytes().toByteArray());
                }
                out.writeInt(originalReferenceRoutes.size());
                for (var resource : new java.util.TreeSet<>(originalReferenceRoutes.keySet())) {
                    out.write(resource.sha256().bytes().toByteArray());
                }
            }
            physicalPublicationRoot = Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
            var context = new com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Context(
                    com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1
                            .MANIFEST_SELECTOR_GENERATION_REPRESENTATION_V1,
                    connection.profile().providerConfigurationSha256(),
                    Sha256Digest.hash(nativeOwner),
                    physicalPublicationRoot);
            physicalPublicationContexts = originalReferenceRoutes.keySet().stream()
                    .collect(java.util.stream.Collectors.toUnmodifiableMap(
                            resource -> resource,
                            resource -> resource instanceof PhysicalResourceIdV2.ObjectVersion
                                    ? context
                                    : new com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Context(
                                            context.writerClass(),
                                                    source.bookKeeper()
                                                            .capability()
                                                            .configurationDigest(),
                                            context.ownerFenceSha256(), context.externalFactsRootSha256())));
            var routes = originalReferenceRoutes.entrySet().stream()
                    .collect(java.util.stream.Collectors.toUnmodifiableMap(
                            value -> value.getKey().authorityKey(), Map.Entry::getValue));
            physicalPublicationGuard = new com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2(
                    new com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1(
                            new PulsarNativeMetadataWriterV2.PhysicalRoutes(routes)));
            var physicalWork = physicalPublicationGuard
                    .executeWithContexts(
                            physicalPublicationContexts,
                            () -> CompletableFuture.supplyAsync(
                                    () -> {
                                        try {
                                            await(owner.cellBudget().requireNativeOwner(view.binding(), nativeOwner));
                                            await(view.binding().revalidate());
                                            await(connection.requireProfileCurrent());
                                            worker.requireCurrentPlan(plan);
                                            physicalMetadataDispatched = true;
                                            result = dispatchOriginalPublication();
                                            return new com.nereusstream.storage.object.gc
                                                    .M5TargetDeleteMultiWriterGuardV2.Completion<>(
                                                    Optional.of(result), Optional.of(physicalPublicationRoot));
                                        } catch (Exception failure) {
                                            if (!physicalMetadataDispatched) {
                                                return new com.nereusstream.storage.object.gc
                                                        .M5TargetDeleteMultiWriterGuardV2.Completion<>(
                                                        Optional.<Publication>empty(),
                                                        Optional.of(physicalPublicationRoot));
                                            }
                                            throw new java.util.concurrent.CompletionException(failure);
                                        }
                                    },
                                    executor))
                    .toCompletableFuture();
            physicalPublicationAttempted = true;
            return physicalWork
                    .thenApply(value -> {
                        physicalPublicationResult = value;
                        if (!value.unresolvedTargets().isEmpty()
                                || value.value().flatMap(original -> original).isEmpty()) {
                            throw new IllegalStateException(
                                    "Native original physical publication ticket remains unresolved");
                        }
                        return value.value().orElseThrow().orElseThrow();
                    })
                    .toCompletableFuture();
        }

        private Publication dispatchOriginalPublication() throws Exception {
            requirePublication(tasks.publish(plan, validated, sources));
            return observeOriginalPublication();
        }

        private Publication observeOriginalPublication() throws Exception {
            var selected = control.coordinator().readSelector().orElseThrow();
            var manifest = M5MaterializationCodecV1.manifestSha256(validated.manifestView());
            if (!selected.selectedViewSha256().equals(manifest)
                    || selected.sourceGeneration() != validated.generation().sourceGeneration()
                    || !selected.fallbackSetSha256()
                            .equals(validated.generation().fallbackSetSha256())) {
                throw new IllegalStateException("Native original manifest selection differs from its exact candidate");
            }
            var task = tasks.readTask(plan.taskIdSha256()).orElseThrow();
            if (task.state() != TaskState.PUBLISHED) {
                // Exact selected candidate is already authoritative. This completes only its original Task row;
                // the coordinator recognizes that selector and cannot dispatch the old selector again.
                requirePublication(tasks.publish(plan, validated, sources));
                task = tasks.readTask(plan.taskIdSha256()).orElseThrow();
            }
            if (task.state() != TaskState.PUBLISHED
                    || !task.manifestViewSha256().equals(Optional.of(manifest))) {
                throw new IllegalStateException("Native original Task publication terminal remains unresolved");
            }
            var fact = await(control.confirmMaterializationSelector(selected));
            return new Publication(validated, selected, fact, readMap, sources);
        }

        private void reconcilePhysicalPublication() throws Exception {
            if (physicalPublicationResult == null
                    || physicalPublicationResult.unresolvedTargets().isEmpty()) {
                return;
            }
            var original = physicalPublicationResult.terminalCleanup().isPresent()
                    ? physicalPublicationResult.terminalCleanup().orElseThrow().reconcile()
                    : physicalPublicationGuard.reconcileOperationWithContexts(
                            physicalPublicationContexts,
                            physicalPublicationResult.operationId(),
                            physicalPublicationRoot);
            if (!await(original).isEmpty()) {
                throw new IllegalStateException("Native original physical publication cleanup remains unresolved");
            }
        }

        private boolean resolveForClose() {
            if (physicalReferenceAdmissionAttempted && !physicalReferencesAdmitted) {
                try {
                    admitVerifiedPhysicalReferences(); // Original assigned credits only; no SDK PUT or new Task.
                } catch (Exception unresolved) {
                    throw new IllegalStateException(
                            "Native original physical output references remain unresolved", unresolved);
                }
            }
            if (result != null) {
                try {
                    reconcilePhysicalPublication();
                } catch (Exception unresolved) {
                    throw new IllegalStateException(
                            "Native original physical publication cleanup remains unresolved", unresolved);
                }
                return true; // This original actor owns its exact native publication receipt.
            }
            if (!selectorAttempted) {
                try {
                    if (physicalPublicationResult != null) {
                        if (physicalMetadataDispatched) {
                            try {
                                // This callback has ended without dispatching a selector. Fence its original Task
                                // row before clearing D tickets so late immutable/verified writes stay harmless.
                                requirePublication(tasks.quarantineUnpublished(plan));
                            } catch (Exception notQuarantined) {
                                result = observeOriginalPublication();
                                reconcilePhysicalPublication();
                                return true;
                            }
                        }
                        reconcilePhysicalPublication();
                    } else if (physicalPublicationAttempted) {
                        throw new IllegalStateException("Native original physical publication dispatch is unresolved");
                    }
                } catch (Exception unresolved) {
                    throw new IllegalStateException(
                            "Native original physical publication cleanup remains unresolved", unresolved);
                }
                return false; // Stopped before native selector dispatch; no late original selection can arrive.
            }
            try {
                result = observeOriginalPublication(); // Metadata only; never dispatch another selector or output.
                reconcilePhysicalPublication();
                return true;
            } catch (Exception unresolved) {
                throw new IllegalStateException("Native original publication handoff remains unresolved", unresolved);
            }
        }
    }

    /** Actual same-version located read under this original Task/SDK lifetime; not a published M4 reader grant. */
    public CompletableFuture<M5PulsarEntriesV2.Entry> readMappedEntry(
            M5PulsarCoverageV2.Position position, long maximumEntryBytes, Executor executor) {
        final MappedRead original;
        synchronized (this) {
            requireReady();
            var map = Objects.requireNonNull(readMap, "original complete rewrite map is not verified");
            map.locate(position); // Exact position and caller cap reject before metadata, allocation or SDK dispatch.
            if (maximumEntryBytes <= 0
                    || maximumEntryBytes > owner.cellBudget().readLimits().maximumOwnedEntryBytes()
                    || publication != null
                    || mappedRead != null && (!mappedRead.workEnded.isDone() || !mappedRead.released)) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Native mapped read slot/cap is unavailable"));
            }
            original = new MappedRead(position, maximumEntryBytes, map);
            mappedRead = original;
        }
        var observer = new CompletableFuture<M5PulsarEntriesV2.Entry>();
        try {
            CompletableFuture.runAsync(
                    () -> {
                        M5PulsarEntriesV2.Entry result = null;
                        Throwable failure = null;
                        try {
                            if (observer.isDone()) {
                                throw new java.util.concurrent.CancellationException(
                                        "mapped read observer ended before dispatch");
                            }
                            result = original.read();
                        } catch (Throwable rejected) {
                            failure = rejected;
                        }
                        try {
                            original.closeOriginal();
                        } catch (Throwable unresolved) {
                            if (failure == null) {
                                failure = unresolved;
                            } else {
                                failure.addSuppressed(unresolved);
                            }
                        } finally {
                            original.workEnded.complete(
                                    null); // No observer can settle SDK/native drain or its workspace.
                        }
                        if (failure == null) {
                            observer.complete(result);
                        } else {
                            observer.completeExceptionally(failure);
                        }
                    },
                    executor);
        } catch (Throwable rejected) {
            original.released = true; // No work/workspace/admission was dispatched.
            original.workEnded.complete(null);
            observer.completeExceptionally(rejected);
        }
        return observer;
    }

    private final class MappedRead {
        private final M5PulsarCoverageV2.Position position;
        private final long maximumBytes;
        private final PulsarNativeMaterializationReadMapV2 map;
        private final CompletableFuture<Void> workEnded = new CompletableFuture<>();
        private final CompletableFuture<Void> actualCleanup = new CompletableFuture<>();
        private PulsarNativeReadCellBudgetV1.Reservation workspace;
        private CapturedReadOperation nativeOperation;
        private C1ObjectProviderSession.VerifiedReadCleanupException uncertainResponse;
        private boolean released;

        private MappedRead(
                M5PulsarCoverageV2.Position position, long maximumBytes, PulsarNativeMaterializationReadMapV2 map) {
            this.position = position;
            this.maximumBytes = maximumBytes;
            this.map = map;
        }

        private M5PulsarEntriesV2.Entry read() throws Exception {
            requireCurrent();
            long bytes = Math.addExact(16L * 1024 * 1024, Math.multiplyExact(4, maximumBytes));
            workspace = owner.cellBudget().reserveMaterializationOutput(view.binding(), bytes);
            await(workspace.ready());
            requireCurrent();
            workspace.actorStarting();
            var route = map.locate(position);
            final M5PulsarEntriesV2.Entry result;
            if (route.rewrittenPayload().isPresent()) {
                await(owner.admitOwnedRead(
                        position.ledgerId(),
                        position.entryId(),
                        position.entryId(),
                        maximumBytes,
                        maximumBytes,
                        actualCleanup));
                var resource = route.rewrittenPayload().orElseThrow();
                var credit = map.readableOutputs().stream()
                        .filter(value -> value.resource().equals(Optional.of(resource)))
                        .findFirst()
                        .orElseThrow();
                try {
                    var consumed = connection
                            .qualifiedProvider()
                            .consumeVerifiedObject(
                                    credit.output().orElseThrow(),
                                    Optional.of(CanonicalUtf8.fromString(
                                                    resource.immutableIdentity().value())
                                            .bytes()),
                                    input -> Nms1CodecV1.readPulsarEntry(
                                            map.planForOutput(credit),
                                            credit.slot().ordinal(),
                                            input,
                                            position,
                                            maximumBytes));
                    result = consumed.value().entry();
                } catch (C1ObjectProviderSession.VerifiedReadCleanupException unresolved) {
                    uncertainResponse = unresolved;
                    throw unresolved;
                }
            } else {
                nativeOperation = view.readOwnedOperation(
                        position.ledgerId(),
                        position.entryId(),
                        position.entryId(),
                        new CapturedReadLimits(1, maximumBytes, maximumBytes),
                        () -> !stopped && owner.sourceUseOpen());
                var batch = await(nativeOperation.result());
                await(nativeOperation.drained());
                if (batch.ledgerId() != position.ledgerId()
                        || batch.entries().size() != 1
                        || batch.entries().get(0).entryId() != position.entryId()) {
                    throw new IOException("Native retained source returned another mapped entry");
                }
                var payload = CanonicalBytes.copyOf(batch.entries().get(0).payload());
                var buffer = Unpooled.wrappedBuffer(payload.toByteArray());
                try {
                    var metadata = Commands.parseMessageMetadata(buffer);
                    if (!metadata.hasPublishTime()) {
                        throw new IOException("Native retained mapped entry timestamp is unavailable");
                    }
                    result = new M5PulsarEntriesV2.Entry(position, payload, metadata.getPublishTime());
                } finally {
                    buffer.release();
                }
            }
            requireCurrent(); // Stop/fence after accepted SDK work cannot deliver a usable entry.
            return result;
        }

        private void closeOriginal() {
            if (released) {
                return;
            }
            try {
                if (uncertainResponse != null) {
                    uncertainResponse.reconcileCleanup(); // Original response only; never another GET.
                }
                if (nativeOperation != null) {
                    await(nativeOperation.drained());
                }
                actualCleanup.complete(null);
                if (workspace != null) {
                    await(workspace.releaseAfterDrain());
                }
                released = true;
            } catch (Exception unresolved) {
                throw new IllegalStateException("Native original mapped read cleanup remains unresolved", unresolved);
            }
        }
    }

    private final class OutputRun {
        private final Path requestedRoot;
        private final long maximumBytes;
        private final Map<String, OriginalObject> objects = new LinkedHashMap<>();
        private final List<C1ObjectProviderSession.VerifiedReadCleanupException> readCleanup = new ArrayList<>();
        private final CompletableFuture<Void> finished = new CompletableFuture<>();
        private PulsarNativeReadCellBudgetV1.Reservation workspace;
        private Path ownedRoot;
        private long stagedBytes;

        private OutputRun(Path root, long bytes) {
            requestedRoot = Objects.requireNonNull(root, "configured local staging root");
            maximumBytes = bytes;
            long entries =
                    plan.sourceCut().coverage().pulsarCoverage().orElseThrow().entryCount();
            long rowAllowance = Math.multiplyExact(
                    entries,
                    Math.multiplyExact(
                            4096L,
                            Math.addExact(
                                    4, Math.multiplyExact(4, plan.indexes().size()))));
            long mapAllowance = 16L * PulsarNativeMaterializationReadMapV2.MAX_BYTES;
            if (entries > M5LookupIndexV1.MAX_ROWS || Math.addExact(rowAllowance, mapAllowance) > bytes / 2) {
                throw new IllegalArgumentException(
                        "Native output entry/index metadata exceeds its workspace allowance");
            }
        }

        private VerifiedPulsarOutputs run() throws Exception {
            requireCurrent();
            workspace = owner.cellBudget().reserveMaterializationOutput(view.binding(), maximumBytes);
            await(workspace.ready());
            requireCurrent();
            workspace.actorStarting();
            Files.createDirectories(requestedRoot);
            ownedRoot = Files.createTempDirectory(
                    requestedRoot, "m5-native-" + plan.taskIdSha256().toHex() + "-");
            var payloads = new ArrayList<GenerationObject>();
            var indexes = new ArrayList<GenerationObject>();
            var rows = new ArrayList<M5LookupIndexV1.Row>();
            for (int ordinal = 0; ordinal < plan.outputParts().size(); ordinal++) {
                requireCurrent();
                var part = M5PulsarStagedPartV2.stage(
                        plan, ordinal, source, ownedRoot, Math.subtractExact(maximumBytes, stagedBytes));
                var original = new OriginalObject(part.descriptor(), part, part);
                synchronized (PulsarNativeMaterializationTaskV2.this) {
                    objects.put(original.descriptor.identity().key(), original);
                }
                stagedBytes = Math.addExact(stagedBytes, part.identity().bodyLength());
                rows.addAll(part.locators());
                payloads.add(create(original));
            }
            int indexCap = Math.toIntExact(Math.min(
                    M5LookupIndexV1.MAX_BYTES,
                    maximumBytes
                            / Math.multiplyExact(
                                    16L, Math.addExact(plan.indexes().size(), 1))));
            for (int ordinal = 0; ordinal < plan.indexes().size(); ordinal++) {
                requireCurrent();
                var index = plan.indexes().get(ordinal);
                var body = new M5LookupIndexV1(
                                index.kind(), index.coverage(), plan.taskIdSha256(), plan.outputIdentitySha256(), rows)
                        .encodeBounded(indexCap);
                var descriptor = new GenerationObject(
                        ordinal,
                        index.kind(),
                        index.coverage(),
                        new ObjectIdentity(index.objectKey(), body.length(), Sha256Digest.hash(body)),
                        Optional.empty());
                var original = new OriginalObject(descriptor, new IndexBody(descriptor.identity(), body), null);
                synchronized (PulsarNativeMaterializationTaskV2.this) {
                    objects.put(index.objectKey(), original);
                }
                indexes.add(create(original));
            }
            requireCurrent();
            var profile = connection.profile();
            var session = new M5MaterializationObjectSessionV1(
                    connection.qualifiedProvider(),
                    2,
                    profile.maximumListPageKeys(),
                    1024L * profile.maximumListPageKeys(),
                    1024);
            try {
                var verified = new M5MaterializationValidatorV1()
                        .verifyPulsarOutputs(plan, payloads, indexes, session.dataReaderForPulsarRewrite(plan, source));
                var assigned = new ArrayList<M5ObjectBirthCreditV3>(birthAttempts.size());
                for (var slot : birthAttempts) {
                    assigned.add(
                            await(outputQuota.quota().readBirth(birthKey(slot))).orElseThrow());
                }
                var complete = PulsarNativeMaterializationReadMapV2.capture(view, verified, assigned);
                requireCurrent(); // Fence/close prevents stale verification delivery.
                synchronized (PulsarNativeMaterializationTaskV2.this) {
                    requireReady();
                    readMap = complete;
                    verifiedOutputs = verified;
                }
                return verified;
            } catch (C1ObjectProviderSession.VerifiedReadCleanupException failure) {
                readCleanup.add(failure);
                throw failure;
            }
        }

        private GenerationObject create(OriginalObject original) throws Exception {
            var identity = original.descriptor.identity();
            if (identity.bodyLength() > connection.profile().maximumObjectBytes()) {
                throw new IllegalArgumentException("Native output exceeds the frozen Provider object cap");
            }
            requireCurrent();
            var slot = slot(identity.key());
            requireBirth(await(outputQuota.quota().armBirth(birthKey(slot), identity)), BirthResult.WRITE_ARMED);
            requireCurrent();
            synchronized (PulsarNativeMaterializationTaskV2.this) {
                requireReady();
                owner.requireMaterializationLocal();
                source.requireTaskSourceOpen();
                original.dispatched = true; // Linearizes accepted SDK work against stop; SDK runs outside this lock.
            }
            var result = connection.qualifiedProvider().conditionalCreate(original.body);
            if (result.outcome() == ProviderObjectOutcome.OUTCOME_UNKNOWN) {
                result = reconcile(original);
            }
            return qualify(original, result);
        }

        private ProviderObjectResult reconcile(OriginalObject original) throws IOException {
            var profile = connection.profile();
            String key = original.descriptor.identity().key();
            return connection
                    .provider()
                    .reconcileUnknown(
                            original.descriptor.identity(),
                            key.substring(0, key.lastIndexOf('/') + 1),
                            2,
                            profile.maximumListPageKeys(),
                            1024L * profile.maximumListPageKeys(),
                            1024)
                    .objectResult();
        }

        private GenerationObject qualify(OriginalObject original, ProviderObjectResult result) throws Exception {
            if ((result.outcome() != ProviderObjectOutcome.APPLIED_EXACT
                            && result.outcome() != ProviderObjectOutcome.EXISTING_EXACT)
                    || result.persistenceEvidence().isEmpty()
                    || result.versionToken().isEmpty()) {
                throw new IllegalStateException("Native original output persistence/version remains unresolved");
            }
            var version = result.versionToken().orElseThrow();
            // Actual same-response full GET; an SDK token or deterministic body hash alone is insufficient.
            try {
                connection
                        .provider()
                        .consumeVerifiedObject(original.descriptor.identity(), Optional.of(version), input -> {
                            var buffer = new byte[65536];
                            long bytes = 0;
                            for (int read = input.read(buffer); read != -1; read = input.read(buffer)) {
                                bytes = Math.addExact(bytes, read);
                            }
                            return bytes;
                        });
            } catch (C1ObjectProviderSession.VerifiedReadCleanupException failure) {
                readCleanup.add(failure);
                throw failure;
            }
            var physical = new PhysicalResourceIdV2.ObjectVersion(
                    connection.binding().physicalNamespace(),
                    CanonicalUtf8.fromString(original.descriptor.identity().key()),
                    PhysicalResourceIdV2.ObjectIdentityKind.IMMUTABLE_VERSION,
                    CanonicalUtf8.fromBytes(version.toByteArray()));
            requireBirth(
                    await(outputQuota
                            .quota()
                            .assignBirth(birthKey(slot(physical.objectKey().value())), physical)),
                    BirthResult.ASSIGNED);
            registerOriginalOutput(await(outputQuota
                            .quota()
                            .readBirth(birthKey(slot(physical.objectKey().value()))))
                    .orElseThrow());
            original.exact = new GenerationObject(
                    original.descriptor.ordinal(),
                    original.descriptor.indexKind(),
                    original.descriptor.coverage(),
                    original.descriptor.identity(),
                    Optional.of(version));
            return original.exact;
        }

        private boolean matches(M5ObjectBirthCreditV3 credit) {
            synchronized (PulsarNativeMaterializationTaskV2.this) {
                var original = objects.get(credit.slot().objectKey());
                if (credit.state() == M5ObjectBirthCreditV3.State.RESERVED) {
                    return original == null || !original.dispatched;
                }
                return original != null
                        && credit.output().equals(Optional.of(original.descriptor.identity()))
                        && (credit.state() == M5ObjectBirthCreditV3.State.WRITE_ARMED
                                || credit.state() == M5ObjectBirthCreditV3.State.ASSIGNED);
            }
        }

        private boolean hasDispatch() {
            return objects.values().stream().anyMatch(value -> value.dispatched);
        }

        private void closeDispatched() {
            try {
                for (var cleanup : readCleanup) {
                    cleanup.reconcileCleanup();
                }
                requireQuotaRecovery();
                for (var slot : birthAttempts) {
                    var original = objects.get(slot.objectKey());
                    var credit =
                            await(outputQuota.quota().readBirth(birthKey(slot))).orElseThrow();
                    if (original != null && original.dispatched) {
                        if (!matches(credit)) {
                            throw new IllegalStateException("Native original dispatched body commitment changed");
                        }
                        if (credit.state() != M5ObjectBirthCreditV3.State.ASSIGNED) {
                            qualify(original, reconcile(original)); // Reconcile original work; no new PUT after stop.
                        } else {
                            registerOriginalOutput(credit); // A lost member reply cannot release the original charge.
                        }
                    } else if (credit.state() != M5ObjectBirthCreditV3.State.CANCELLED_NO_DISPATCH) {
                        requireBirth(
                                await(outputQuota.quota().cancelNoDispatchBirth(birthKey(slot))),
                                BirthResult.CANCELLED_NO_DISPATCH);
                    }
                }
            } catch (Exception unresolved) {
                throw new IllegalStateException(
                        "Native original dispatched output handoff remains unresolved", unresolved);
            }
        }

        private void requireQuotaRecovery() throws Exception {
            if (!await(outputQuota.quota().recover())) {
                throw new IllegalStateException("Native original output quota recovery remains unresolved");
            }
        }

        private void releaseWorkspace() {
            try {
                for (var original : objects.values()) {
                    if (original.staged != null) {
                        original.staged.close();
                    }
                }
                if (ownedRoot != null && Files.exists(ownedRoot)) {
                    try (var files = Files.walk(ownedRoot)) {
                        for (var owned : files.sorted(Comparator.reverseOrder()).toList()) {
                            Files.delete(owned);
                        }
                    }
                }
                if (workspace != null) {
                    await(workspace.releaseAfterDrain());
                }
                for (var original : objects.values()) {
                    original.body = null;
                    original.staged = null;
                }
                synchronized (PulsarNativeMaterializationTaskV2.this) {
                    readMap = null;
                    verifiedOutputs = null;
                }
            } catch (Exception unresolved) {
                throw new IllegalStateException(
                        "Native original output workspace cleanup remains unresolved", unresolved);
            }
        }

        private M5PendingOutputInventoryV2.Slot slot(String key) {
            return pendingOutputs.outputs().stream()
                    .filter(value -> value.objectKey().equals(key))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("output is outside original chosen-key inventory"));
        }
    }

    private static final class OriginalObject {
        private final GenerationObject descriptor;
        private RepeatableObjectBody body;
        private M5PulsarStagedPartV2 staged;
        private boolean dispatched;
        private GenerationObject exact;

        private OriginalObject(GenerationObject descriptor, RepeatableObjectBody body, M5PulsarStagedPartV2 staged) {
            this.descriptor = descriptor;
            this.body = body;
            this.staged = staged;
        }
    }

    private record IndexBody(ObjectIdentity identity, CanonicalBytes bytes) implements RepeatableObjectBody {
        @Override
        public InputStream openStream() {
            return new ByteArrayInputStream(bytes.toByteArray());
        }
    }

    private <T> T await(CompletionStage<T> stage) throws Exception {
        var original = stage.toCompletableFuture();
        synchronized (this) {
            acceptedMetadata.add(original);
        }
        original.whenComplete((value, failure) -> {
            synchronized (this) {
                acceptedMetadata.remove(original);
            }
        });
        return original.get(); // Interrupted observers cannot remove the original accepted callback.
    }

    private synchronized CompletableFuture<Void> acceptedMetadataDrain() {
        return CompletableFuture.allOf(acceptedMetadata.stream()
                .map(original -> original.handle((value, failure) -> null))
                .toArray(CompletableFuture[]::new));
    }

    private synchronized CompletableFuture<Void> currentChecksDrain() {
        return CompletableFuture.allOf(currentChecks.toArray(CompletableFuture[]::new));
    }

    private void requireRunning() {
        if (stopped) {
            throw new IllegalStateException("Native original Task is stopped");
        }
        source.requireTaskSourceOpen();
        owner.requireMaterializationCurrent();
    }

    private void requireReady() {
        if (stopped || released || worker == null) {
            throw new IllegalStateException("Native original Task has no usable complete-plan worker grant");
        }
    }

    private static void requireAdmission(M5MaterializationAdmissionV1.Outcome outcome) {
        if (outcome != M5MaterializationAdmissionV1.Outcome.APPLIED_EXACT
                && outcome != M5MaterializationAdmissionV1.Outcome.EXISTING_EXACT) {
            throw new IllegalStateException("Native original Task Cell reservation remains unresolved: " + outcome);
        }
    }

    private String birthKey(M5PendingOutputInventoryV2.Slot slot) {
        var binding = connection.profile().physicalBinding();
        var layout =
                M5GcQuotaRecordsV2.Layout.withPhysicalMembers(binding.authorityRoot(), binding.physicalNamespace());
        return M5ObjectBirthCreditV3.key(layout.scope(), worker.worker(), slot);
    }

    private static void requireBirth(BirthResult actual, BirthResult expected) {
        if (actual != expected) {
            throw new IllegalStateException("Native original Object birth capacity remains unresolved: " + actual);
        }
    }

    private static void requirePublication(PublicationOutcome outcome) {
        if (outcome != PublicationOutcome.APPLIED_EXACT && outcome != PublicationOutcome.EXISTING_EXACT) {
            throw new IllegalStateException(
                    "Native original full-plan task persistence remains unresolved: " + outcome);
        }
    }

    private static Sha256Digest policyRoot(M5MaterializationPlannerV1.PlanningPolicy policy) {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(policy.rewritePayloadKind().ordinal());
                out.writeBoolean(policy.forceRewrite());
                out.write(policy.encryptionGenerationSha256().bytes().toByteArray());
                out.write(policy.compressionPolicySha256().bytes().toByteArray());
                out.write(policy.checksumPolicySha256().bytes().toByteArray());
                out.writeInt(policy.requiredIndexes().size());
                for (var index : policy.requiredIndexes()) {
                    out.writeInt(index.ordinal());
                }
                out.writeUTF(policy.exclusiveCellNamespace());
            }
            return commitment(
                    "NEREUS_V2_M5_NATIVE_PULSAR_MATERIALIZATION_POLICY_V2", CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (IOException impossible) {
            throw new IllegalStateException("Cannot commit original Native planning policy", impossible);
        }
    }

    private static Sha256Digest commitment(String domain, CanonicalBytes body) {
        var tag = CanonicalUtf8.fromString(domain).bytes().toByteArray();
        return Sha256Digest.hash(CanonicalBytes.copyOf(java.nio.ByteBuffer.allocate(8 + tag.length + body.length())
                .putInt(tag.length)
                .put(tag)
                .putInt(body.length())
                .put(body.toByteArray())
                .array()));
    }
}
