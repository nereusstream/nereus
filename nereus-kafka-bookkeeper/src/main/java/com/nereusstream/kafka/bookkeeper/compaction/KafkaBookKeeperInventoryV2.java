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

package com.nereusstream.kafka.bookkeeper.compaction;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerOpenOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.BookKeeperLedger;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * Immutable BK compaction task and native-ID inventory, written before any ledger creation.
 *
 * <p>This component owns no task termination, selection or deletion authority. Its caller must retain the task's
 * writer authority until every accepted provider operation drains. Inventory records are permanent in this version;
 * a terminal task must never be passed to {@link #createPart}. In particular, raw explicit-ID creation after deletion
 * is not made safe by ID allocation. Native admission and task fencing belong to the enclosing lifecycle coordinator.
 */
public final class KafkaBookKeeperInventoryV2 {
    public static final int MAX_PARTS = 256;
    public static final int MAX_TASK_BYTES = 1_048_576;
    public static final long MAX_PART_BYTES = 67_108_864;
    public static final int MAX_PART_ENTRIES = 65_536;

    public enum PartKind {
        DATA,
        INDEX
    }

    /** Root binds the ordered exact entry envelopes, including segment/chunk coordinates and body checksums. */
    public record PartPlan(PartKind kind, int entryCount, long length, Sha256Digest entriesRootSha256) {
        public PartPlan {
            Objects.requireNonNull(kind, "kind");
            requireDigest(entriesRootSha256);
            if (entryCount <= 0 || entryCount > MAX_PART_ENTRIES || length < entryCount || length > MAX_PART_BYTES) {
                throw new IllegalArgumentException("BK compaction part exceeds its non-empty bounds");
            }
        }
    }

    /** The source cut bytes contain the exact Binding, predecessor, owner, capability and protocol frontiers. */
    public record Task(
            CanonicalBytes sourceCut,
            Sha256Digest compactionPlanRootSha256,
            Sha256Digest semanticOutputSha256,
            Sha256Digest semanticValidationRootSha256,
            com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace namespace,
            BookKeeperCapabilitySnapshotV1 capability,
            long attempt,
            List<PartPlan> parts) {
        public Task {
            Objects.requireNonNull(sourceCut, "sourceCut");
            requireDigest(compactionPlanRootSha256);
            requireDigest(semanticOutputSha256);
            requireDigest(semanticValidationRootSha256);
            Objects.requireNonNull(namespace, "namespace");
            Objects.requireNonNull(capability, "capability");
            parts = List.copyOf(Objects.requireNonNull(parts, "parts"));
            if (sourceCut.length() == 0
                    || sourceCut.length() > MAX_TASK_BYTES / 2
                    || namespace.providerKind()
                            != com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.ProviderKind.BOOKKEEPER
                    || attempt < 0
                    || parts.isEmpty()
                    || parts.size() > MAX_PARTS) {
                throw new IllegalArgumentException("BK compaction task identity or bound differs");
            }
            var cut =
                    com.nereusstream.storage.object.materialization.M5MaterializationCodecV1.decodeSourceCut(sourceCut);
            if (cut.coverage().domain()
                            != com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain
                                    .KAFKA_OFFSET
                    || !cut.identity()
                            .providerScopeSha256()
                            .equals(capability.providerScopeId().digest())) {
                throw new IllegalArgumentException("BK task differs from the Kafka source cut provider scope");
            }
        }

        public Sha256Digest taskIdSha256() {
            return Sha256Digest.hash(KafkaBookKeeperInventoryCodecV2.encodeTask(this));
        }

        public RunLedgerConfigurationV1 configuration(int ordinal) {
            requireOrdinal(this, ordinal);
            CanonicalBytes identity = KafkaBookKeeperInventoryCodecV2.encodeRunIdentity(taskIdSha256(), ordinal);
            byte[] hash = Sha256Digest.hash(identity).bytes().toByteArray();
            return RunLedgerConfigurationV1.from(
                    capability, new StorageRunId(Id128.fromBytes(Arrays.copyOf(hash, 16))));
        }
    }

    public record Part(Sha256Digest taskIdSha256, int ordinal, BookKeeperLedger resource, RunLedgerHandleV1 handle) {
        public Part {
            requireDigest(taskIdSha256);
            Objects.requireNonNull(resource, "resource");
            Objects.requireNonNull(handle, "handle");
            if (ordinal < 0
                    || ordinal >= MAX_PARTS
                    || resource.ledgerId() != handle.ledgerIdentity().ledgerId()) {
                throw new IllegalArgumentException("BK inventory part identity differs");
            }
        }
    }

    public enum CreateOutcome {
        CREATED_WRITABLE,
        EXISTING_EXACT_REQUIRES_RECOVERY,
        OUTCOME_UNKNOWN,
        CONFLICT
    }

    private final CanonicalControlMetadataStore metadata;
    private final Executor controlExecutor;

    public KafkaBookKeeperInventoryV2(CanonicalControlMetadataStore metadata) {
        this(metadata, Runnable::run);
    }

    /** Native metadata callers supply their bounded Cell control executor and invoke control entrypoints on it. */
    public KafkaBookKeeperInventoryV2(CanonicalControlMetadataStore metadata, Executor controlExecutor) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.controlExecutor = Objects.requireNonNull(controlExecutor, "controlExecutor");
    }

    /** A missing reread never authorizes allocation, including a lost put response. */
    public boolean register(Task task) {
        CanonicalBytes bytes = KafkaBookKeeperInventoryCodecV2.encodeTask(task);
        metadata.putIfAbsent(taskKey(task.taskIdSha256()), bytes);
        Optional<CanonicalBytes> observed = metadata.get(taskKey(task.taskIdSha256()));
        if (observed.isPresent() && !observed.orElseThrow().equals(bytes)) {
            throw new IllegalStateException("different immutable BK task occupies its identity");
        }
        return observed.isPresent();
    }

    /** Concurrent losers burn their allocated ID, adopt the exact winner, and create no extra ledger. */
    public CompletionStage<Optional<Part>> reservePart(Task task, int ordinal, BookKeeperCellSession session) {
        requireTask(task, ordinal, session);
        Optional<Part> existing = readPart(task, ordinal);
        if (existing.isPresent()) {
            return CompletableFuture.completedFuture(existing);
        }
        return session.reserveLedgerIdentity()
                .thenApplyAsync(
                        result -> {
                            if (result.outcome() != ProviderMutationOutcomeV1.APPLIED_EXACT) {
                                return Optional.empty();
                            }
                            BookKeeperLedgerIdentity id = result.exactProof().orElseThrow();
                            RunLedgerConfigurationV1 configuration = task.configuration(ordinal);
                            Part part = new Part(
                                    task.taskIdSha256(),
                                    ordinal,
                                    new BookKeeperLedger(task.namespace(), id.ledgerId()),
                                    new RunLedgerHandleV1(
                                            configuration.providerScopeId(),
                                            configuration.runId(),
                                            id,
                                            configuration.configurationDigest()));
                            metadata.putIfAbsent(
                                    partKey(task.taskIdSha256(), ordinal),
                                    KafkaBookKeeperInventoryCodecV2.encodePart(part));
                            return readPart(task, ordinal);
                        },
                        controlExecutor);
    }

    /** Never allocates here. Every retry targets the same durably recorded native ID and exact run metadata. */
    public CompletionStage<CreateOutcome> createPart(Task task, Part part, BookKeeperCellSession session) {
        requireTask(task, part.ordinal(), session);
        if (!readPart(task, part.ordinal()).equals(Optional.of(part))) {
            throw new IllegalStateException("BK creation lacks its exact durable inventory");
        }
        return session.createReservedRunLedger(
                        task.configuration(part.ordinal()), part.handle().ledgerIdentity())
                .thenCompose(result -> {
                    if (result.outcome() == ProviderMutationOutcomeV1.APPLIED_EXACT) {
                        if (!result.exactProof().orElseThrow().equals(part.handle())) {
                            throw new IllegalStateException("native BK create returned a different inventoried handle");
                        }
                        return CompletableFuture.completedFuture(CreateOutcome.CREATED_WRITABLE);
                    }
                    return session.openRunLedger(part.handle()).thenApply(open -> {
                        if (open.outcome() == RunLedgerOpenOutcomeV1.OPENED_EXACT) {
                            if (!open.exactHandle().orElseThrow().equals(part.handle())) {
                                throw new IllegalStateException(
                                        "native BK open returned a different inventoried handle");
                            }
                            return CreateOutcome.EXISTING_EXACT_REQUIRES_RECOVERY;
                        }
                        return open.outcome() == RunLedgerOpenOutcomeV1.CONFIGURATION_MISMATCH
                                ? CreateOutcome.CONFLICT
                                : CreateOutcome.OUTCOME_UNKNOWN;
                    });
                });
    }

    public Optional<Part> readPart(Task task, int ordinal) {
        requireOrdinal(task, ordinal);
        Optional<Part> part =
                metadata.get(partKey(task.taskIdSha256(), ordinal)).map(KafkaBookKeeperInventoryCodecV2::decodePart);
        part.ifPresent(value -> {
            RunLedgerConfigurationV1 configuration = task.configuration(ordinal);
            if (!value.taskIdSha256().equals(task.taskIdSha256())
                    || value.ordinal() != ordinal
                    || !value.resource().namespace().equals(task.namespace())
                    || !value.handle().providerScopeId().equals(configuration.providerScopeId())
                    || !value.handle().runId().equals(configuration.runId())
                    || !value.handle().configurationDigest().equals(configuration.configurationDigest())) {
                throw new IllegalStateException("different immutable BK part occupies its inventory slot");
            }
        });
        return part;
    }

    public static String taskKey(Sha256Digest taskId) {
        requireDigest(taskId);
        return "v2/kafka-bk-compaction-v2/" + taskId.toHex() + "/task";
    }

    public static String partKey(Sha256Digest taskId, int ordinal) {
        if (ordinal < 0 || ordinal >= MAX_PARTS) {
            throw new IllegalArgumentException("BK inventory ordinal exceeds bound");
        }
        return taskKey(taskId) + "/part/" + ordinal;
    }

    private void requireTask(Task task, int ordinal, BookKeeperCellSession session) {
        requireOrdinal(task, ordinal);
        requireRegisteredTask(task, session);
    }

    void requireRegisteredTask(Task task, BookKeeperCellSession session) {
        Objects.requireNonNull(task, "task");
        Objects.requireNonNull(session, "session");
        if (!session.capabilitySnapshot().equals(task.capability())
                || !metadata.get(taskKey(task.taskIdSha256()))
                        .equals(Optional.of(KafkaBookKeeperInventoryCodecV2.encodeTask(task)))) {
            throw new IllegalStateException("BK allocation or creation lacks its exact durable task and capability");
        }
    }

    private static void requireOrdinal(Task task, int ordinal) {
        Objects.requireNonNull(task, "task");
        if (ordinal < 0 || ordinal >= task.parts().size()) {
            throw new IllegalArgumentException("BK inventory ordinal is outside this task");
        }
    }

    static void requireDigest(Sha256Digest digest) {
        if (Objects.requireNonNull(digest, "digest").isZero()) {
            throw new IllegalArgumentException("BK inventory digest must be nonzero");
        }
    }
}
