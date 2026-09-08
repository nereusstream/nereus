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
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.CompactionPlan;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.InputBatch;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2CodecV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2DataV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2FrameV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2IndexDirectoryEntryV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2ProtocolCheckpointV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RangeIndexBlockV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunFooterV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunHeaderV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootCatalogV2;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.api.kafka.KafkaRunRootStateV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.BookKeeperDeleteTargetV1;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.bookkeeper.RealBookKeeperCellSessionV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Completion;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Context;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationSourceCut;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceExtent;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
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

/**
 * Native sealed NBKE2 source capture and exact physical membership. Reads hold a physical ticket until their owned
 * session drains/closes. Protocol frontiers, semantic key/transaction proofs and M4 source protection remain owner
 * obligations; this adapter cannot manufacture them from a sealed ledger. Unknown close retains its read ticket.
 */
public final class KafkaBookKeeperRunSourceV2 implements KafkaBookKeeperPublicationTicketsV2.InputMembership {
    public record Bounds(
            int entries, int batches, int records, long nativeBytes, long payloadBytes, long decodedBytes) {
        public Bounds {
            if (entries < 2
                    || entries > 131072
                    || batches < 1
                    || batches > KafkaCompactionRecordsV1.MAX_BATCHES
                    || records < 1
                    || records > KafkaCompactionRecordsV1.MAX_RECORDS
                    || nativeBytes < 1
                    || nativeBytes > 256L * 1024 * 1024
                    || payloadBytes < 1
                    || payloadBytes > 128L * 1024 * 1024
                    || decodedBytes < 1
                    || decodedBytes > 256L * 1024 * 1024) {
                throw new IllegalArgumentException("native run source budget exceeds its hard bounds");
            }
        }
    }

    public record Snapshot(
            KafkaRunRootRecordV2 root,
            SourceExtent extent,
            List<InputBatch> batches,
            boolean rangeIndexesCoverAllData) {
        public Snapshot {
            batches = List.copyOf(batches);
        }

        public BindingIdentity binding() {
            return new BindingIdentity(
                    root.root().bindingId(),
                    Sha256Digest.hash(
                            TopicIncarnationIdentityCodecV1.encode(root.root().topicIncarnation())),
                    root.root().storageEpochId().digest());
        }
    }

    private record ReadOutcome(Snapshot snapshot, Throwable failure) {}

    private record DataRow(Nbke2DataV1 data, long groupOrdinal) {}

    private final KafkaRunRootCatalogV2 catalog;
    private final M5BookKeeperNativeCreateClientV2 client;
    private final M5TargetDeleteMultiWriterGuardV2 guard;
    private final Bounds bounds;
    private final Executor owner;

    public KafkaBookKeeperRunSourceV2(
            KafkaRunRootCatalogV2 catalog,
            M5BookKeeperNativeCreateClientV2 client,
            M5TargetDeleteMultiWriterGuardV2 guard,
            Bounds bounds,
            Executor owner) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.client = Objects.requireNonNull(client, "client");
        this.guard = Objects.requireNonNull(guard, "guard");
        this.bounds = Objects.requireNonNull(bounds, "bounds");
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    public CompletionStage<Snapshot> capture(String nativeRootKey) {
        return capture(nativeRootKey, new Budget(bounds));
    }

    @Override
    public CompletionStage<Map<Sha256Digest, List<PhysicalResourceIdV2>>> resolve(CompactionPlan plan) {
        Objects.requireNonNull(plan, "plan");
        return resolve(plan.sourceCut(), Optional.of(plan.inputBatches()));
    }

    /** Physical membership only; publication must use the complete-plan overload. */
    public CompletionStage<Map<Sha256Digest, List<PhysicalResourceIdV2>>> resolve(MaterializationSourceCut cut) {
        return resolve(cut, Optional.empty());
    }

    private CompletionStage<Map<Sha256Digest, List<PhysicalResourceIdV2>>> resolve(
            MaterializationSourceCut cut, Optional<List<InputBatch>> expectedInputs) {
        Objects.requireNonNull(cut, "cut");
        var budget = new Budget(bounds);
        Map<Sha256Digest, List<PhysicalResourceIdV2>> result = new LinkedHashMap<>();
        List<InputBatch> nativeInputs = new ArrayList<>();
        CompletionStage<Void> sequence = CompletableFuture.completedFuture(null);
        for (var extent : cut.sources()) {
            if (extent.kind() != SourceKind.BOOKKEEPER_LEDGER) {
                return CompletableFuture.failedFuture(
                        new IllegalArgumentException("native run catalog requires BK sources"));
            }
            sequence = sequence.thenComposeAsync(
                    ignored -> capture(extent.physicalKey(), budget).thenAccept(snapshot -> {
                        if (!snapshot.extent().equals(extent)
                                || !snapshot.binding().equals(cut.identity().binding())
                                || !snapshot.root()
                                        .root()
                                        .providerScopeId()
                                        .digest()
                                        .equals(cut.identity().providerScopeSha256())) {
                            throw new IllegalArgumentException(
                                    "native run source differs from the complete frozen source extent");
                        }
                        if (result.put(
                                        extent.sourceIdentitySha256(),
                                        List.of(snapshot.root().resource()))
                                != null) {
                            throw new IllegalArgumentException("duplicate native source identity");
                        }
                        if (expectedInputs.isPresent()) {
                            nativeInputs.addAll(snapshot.batches());
                        }
                    }),
                    owner);
        }
        return sequence.thenApply(ignored -> {
            if (expectedInputs.isPresent() && !nativeInputs.equals(expectedInputs.orElseThrow())) {
                throw new IllegalArgumentException("native source input batches differ from compaction plan");
            }
            return Map.copyOf(result);
        });
    }

    private CompletionStage<Snapshot> capture(String key, Budget budget) {
        var work = catalog.readSelectedRoot(key)
                .thenComposeAsync(
                        observed -> {
                            var actual = observed.orElseThrow(
                                    () -> new IllegalArgumentException("native source root is not selected"));
                            if (actual.root().state() != KafkaRunRootStateV1.SEALED
                                    || !actual.resource()
                                            .namespace()
                                            .equals(client.spec().namespace())
                                    || actual.root().kafkaEndOffsetExclusive().orElseThrow()
                                            == actual.root().kafkaStartOffset()) {
                                throw new IllegalArgumentException(
                                        "native source requires a nonempty sealed run in the exact namespace");
                            }
                            var stable =
                                    new KafkaRunRootRecordV2(actual.resource(), actual.root(), true, Optional.empty());
                            var identity = Sha256Digest.hash(stable.encode());
                            var context = new Context(
                                    ProofBoundWriterClassV1.OWNER_WORKER_LEASE_HANDLE_PIN_V1,
                                    client.capabilitySnapshot().configurationDigest(),
                                    stable.initialLink().initialRootSha256(),
                                    identity);
                            return guard.execute(List.of(stable.resource()), context, () -> {
                                        var reads = client.newSession();
                                        CompletionStage<Snapshot> scan;
                                        try {
                                            scan = new Scan(key, stable, reads, budget).start();
                                        } catch (RuntimeException failure) {
                                            scan = CompletableFuture.failedFuture(failure);
                                        }
                                        return scan.handle(ReadOutcome::new).thenCompose(outcome -> reads.closeAsync()
                                                .thenApply(ignored -> new Completion<>(
                                                        outcome,
                                                        Optional.of(Sha256Digest.hash(CanonicalBytes.copyOf(
                                                                ByteBuffer.allocate(36)
                                                                        .putInt(0x4d355243)
                                                                        .put(
                                                                                identity.bytes()
                                                                                        .toByteArray())
                                                                        .array()))))));
                                    })
                                    .thenCompose(result -> {
                                        if (!result.mutationInvoked()
                                                || result.value().isEmpty()) {
                                            return CompletableFuture.failedFuture(new IllegalStateException(
                                                    "native source physical admission failed"));
                                        }
                                        if (!result.unresolvedTargets().isEmpty()) {
                                            return CompletableFuture.failedFuture(new IllegalStateException(
                                                    "native source ticket release is unresolved"));
                                        }
                                        var outcome = result.value().orElseThrow();
                                        return outcome.failure() == null
                                                ? CompletableFuture.completedFuture(outcome.snapshot())
                                                : CompletableFuture.failedFuture(outcome.failure());
                                    });
                        },
                        owner);
        // Cancellation never abandons in-flight reads, session drain or release of this invocation's physical ticket.
        return work.thenApply(value -> value);
    }

    private static final class Budget {
        final Bounds maximum;
        int entries;
        int batches;
        int records;
        long nativeBytes;
        long payloadBytes;
        long decodedBytes;

        Budget(Bounds maximum) {
            this.maximum = maximum;
        }
    }

    private final class Scan {
        final String key;
        final KafkaRunRootRecordV2 root;
        final RealBookKeeperCellSessionV1 reads;
        final Budget budget;
        final RunLedgerHandleV1 handle;
        final Nbke2RunBindingV1 run;
        final MessageDigest frames = sha();
        final MessageDigest bodies = sha();
        final Map<Long, DataRow> data = new LinkedHashMap<>();
        final Map<Long, Nbke2RangeIndexBlockV1> indexes = new LinkedHashMap<>();
        final Set<Id128> groups = new HashSet<>();
        long nextOffset;
        long length;
        long nativeLength;
        long latestIndex = -1;
        int records;
        long minimumTimestamp = Long.MAX_VALUE;
        long maximumTimestamp = -1;
        long groupOrdinal;
        Nbke2DataV1 firstGroup;
        MessageDigest groupDigest;
        long firstGroupEntry;
        int nextMember;
        long latestCheckpoint = -1;
        BookKeeperDeleteTargetV1 seal;
        Nbke2RunFooterV1 footer;

        Scan(String key, KafkaRunRootRecordV2 root, RealBookKeeperCellSessionV1 reads, Budget budget) {
            this.key = key;
            this.root = root;
            this.reads = reads;
            this.budget = budget;
            var value = root.root();
            handle = new RunLedgerHandleV1(
                    value.providerScopeId(),
                    value.runId(),
                    value.ledgerIdentity(),
                    client.capabilitySnapshot().configurationDigest());
            run = new Nbke2RunBindingV1(
                    value.bindingId(),
                    value.topicIncarnation(),
                    value.partitionId(),
                    value.storageEpochId(),
                    value.creatorOwnerEpoch(),
                    value.kafkaLeaderEpoch(),
                    value.providerScopeId(),
                    value.runId());
            nextOffset = value.kafkaStartOffset();
        }

        CompletionStage<Snapshot> start() {
            if (!client.spec()
                    .configurations()
                    .contains(com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1.from(
                            client.capabilitySnapshot(), handle.runId()))) {
                throw new IllegalArgumentException("source run is outside the native client configuration");
            }
            return client.captureExactTarget(handle)
                    .thenCompose(captured -> {
                        seal = captured.exactTarget()
                                .orElseThrow(() ->
                                        new IllegalArgumentException("native source lacks exact closed metadata"));
                        long count = Math.addExact(seal.sealedLastEntryId(), 1);
                        if (count < 2 || count > budget.maximum.entries() - budget.entries) {
                            throw new IllegalArgumentException(
                                    "native source entry budget exhausted before data reads");
                        }
                        return reads.openRunLedger(handle);
                    })
                    .thenComposeAsync(
                            open -> {
                                if (!open.exactHandle().equals(Optional.of(handle))) {
                                    throw new IllegalArgumentException("native source exact open failed");
                                }
                                CompletionStage<Void> sequence = CompletableFuture.completedFuture(null);
                                for (long entry = 0; entry <= seal.sealedLastEntryId(); entry++) {
                                    long next = entry;
                                    sequence = sequence.thenComposeAsync(ignored -> read(next), owner);
                                }
                                return sequence;
                            },
                            owner)
                    .thenCompose(ignored -> catalog.readSelectedRoot(key))
                    .thenApplyAsync(
                            observed -> {
                                var current = observed.orElseThrow(
                                        () -> new IllegalArgumentException("native source root disappeared"));
                                if (!current.root().equals(root.root())
                                        || !current.resource().equals(root.resource())) {
                                    throw new IllegalArgumentException("native source root changed during capture");
                                }
                                return finish();
                            },
                            owner);
        }

        CompletionStage<Void> read(long entry) {
            return reads.readExactEntry(handle, entry)
                    .thenAcceptAsync(
                            result -> {
                                var exact = result.exactEntry()
                                        .orElseThrow(
                                                () -> new IllegalArgumentException("native source entry unavailable"));
                                if (!exact.handle().equals(handle)
                                        || exact.entryId() != entry
                                        || exact.payload().length()
                                                > budget.maximum.nativeBytes() - budget.nativeBytes) {
                                    throw new IllegalArgumentException(
                                            "native source entry identity or byte budget differs");
                                }
                                budget.entries++;
                                budget.nativeBytes += exact.payload().length();
                                nativeLength += exact.payload().length();
                                frames.update(ByteBuffer.allocate(12)
                                        .putLong(entry)
                                        .putInt(exact.payload().length())
                                        .array());
                                frames.update(exact.payload().toByteArray());
                                accept(
                                        entry,
                                        Nbke2CodecV1.decode(
                                                exact.payload().toByteArray(),
                                                handle.ledgerIdentity().ledgerId(),
                                                entry));
                            },
                            owner);
        }

        void accept(long entry, Nbke2FrameV1 frame) {
            if (!frame.runBinding().equals(run)) {
                throw new IllegalArgumentException("native source frame has another run binding");
            }
            if (entry == 0) {
                if (!(frame instanceof Nbke2RunHeaderV1 header)
                        || header.kafkaStartOffset() != nextOffset
                        || header.firstDataEntryId() != 1
                        || !header.ledgerConfigurationDigest().equals(handle.configurationDigest())) {
                    throw new IllegalArgumentException("native source header differs");
                }
                return;
            }
            if (frame instanceof Nbke2DataV1 batch) {
                acceptData(entry, batch);
                return;
            }
            if (firstGroup != null) {
                throw new IllegalArgumentException("control entry splits an incomplete append group");
            }
            if (frame instanceof Nbke2RangeIndexBlockV1 index) {
                if (entry <= index.lastDataEntryId()
                        || entry >= index.successorDataEntryId()
                        || index.predecessorBlockEntryId() != latestIndex) {
                    throw new IllegalArgumentException("native range index has another physical cut");
                }
                validateIndex(index);
                indexes.put(entry, index);
                latestIndex = entry;
            } else if (frame instanceof Nbke2ProtocolCheckpointV1 checkpoint) {
                KafkaProtocolCheckpointStateV1.fromNbke2(checkpoint);
                for (long covered : new long[] {
                    checkpoint.rangeIndexCoveredThrough(),
                    checkpoint.producerStateCoveredThrough(),
                    checkpoint.transactionIndexCoveredThrough(),
                    checkpoint.leaderEpochCoveredThrough()
                }) {
                    if (covered < root.root().kafkaStartOffset() || covered > nextOffset) {
                        throw new IllegalArgumentException("native checkpoint exceeds observed DATA coverage");
                    }
                }
                latestCheckpoint = entry;
            } else if (frame instanceof Nbke2RunFooterV1 last) {
                if (entry != seal.sealedLastEntryId()
                        || last.lastPhysicalEntryIdExclusive() != entry + 1
                        || last.kafkaEndOffsetExclusive() != nextOffset
                        || nextOffset != root.root().kafkaEndOffsetExclusive().orElseThrow()
                        || last.sealOwnerEpoch() < run.creatorOwnerEpoch()
                        || last.protocolCheckpointEntryId() != latestCheckpoint) {
                    throw new IllegalArgumentException("native source footer differs from actual terminal coverage");
                }
                footer = last;
            } else {
                throw new IllegalArgumentException("duplicate native run header");
            }
        }

        void acceptData(long entry, Nbke2DataV1 batch) {
            if (batch.baseOffset() != nextOffset
                    || budget.batches == budget.maximum.batches()
                    || batch.rawAssignedRecordBatch().length() > budget.maximum.payloadBytes() - budget.payloadBytes) {
                throw new IllegalArgumentException("native source DATA coverage or payload budget differs");
            }
            if (firstGroup == null) {
                if (batch.memberOrdinal() != 0 || !groups.add(batch.appendGroupId())) {
                    throw new IllegalArgumentException("native append group starts at another member or repeats");
                }
                firstGroup = batch;
                firstGroupEntry = entry;
                nextMember = 0;
                groupDigest = sha();
            }
            if (batch.memberOrdinal() != nextMember
                    || batch.memberCount() != firstGroup.memberCount()
                    || !batch.appendGroupId().equals(firstGroup.appendGroupId())
                    || !batch.storageAttemptId().equals(firstGroup.storageAttemptId())
                    || entry != firstGroupEntry + nextMember) {
                throw new IllegalArgumentException("native append-group members differ");
            }
            var parsed = KafkaRecordBatchCodecV1.parseBounded(
                    batch.rawAssignedRecordBatch(),
                    budget.maximum.records() - budget.records,
                    budget.maximum.decodedBytes() - budget.decodedBytes);
            var facts = parsed.batch();
            if (facts.baseOffset() != batch.baseOffset()
                    || facts.lastOffset() + 1 != batch.endOffsetExclusive()
                    || facts.partitionLeaderEpoch() != run.kafkaLeaderEpoch()) {
                throw new IllegalArgumentException("native Kafka body differs from its NBKE2 DATA envelope");
            }
            budget.batches++;
            budget.records += facts.records().size();
            budget.decodedBytes += parsed.decodedBytes();
            budget.payloadBytes += batch.rawAssignedRecordBatch().length();
            records += facts.records().size();
            length += batch.rawAssignedRecordBatch().length();
            for (var record : facts.records()) {
                if (record.timestamp() < 0) {
                    throw new IllegalArgumentException("source record timestamp is unrepresentable");
                }
                minimumTimestamp = Math.min(minimumTimestamp, record.timestamp());
                maximumTimestamp = Math.max(maximumTimestamp, record.timestamp());
            }
            bodies.update(batch.rawAssignedRecordBatch().toByteArray());
            groupDigest.update(batch.rawAssignedRecordBatch().toByteArray());
            data.put(entry, new DataRow(batch, groupOrdinal));
            nextOffset = batch.endOffsetExclusive();
            nextMember++;
            if (batch.terminalDescriptor().isPresent()) {
                var terminal = batch.terminalDescriptor().orElseThrow();
                if (terminal.groupStartOffset() != firstGroup.baseOffset()
                        || terminal.firstDataEntryId() != firstGroupEntry
                        || terminal.lastDataEntryId() != entry
                        || terminal.groupEndOffsetExclusive() != nextOffset
                        || !terminal.aggregateAssignedPayloadSha256()
                                .equals(Sha256Digest.copyOf(groupDigest.digest()))) {
                    throw new IllegalArgumentException("native terminal append-group digest or bounds differ");
                }
                firstGroup = null;
                groupOrdinal++;
            }
        }

        void validateIndex(Nbke2RangeIndexBlockV1 index) {
            Long groupBase = null;
            Long deltaBase = null;
            for (var locator : index.locators()) {
                var row = data.get(Math.addExact(index.anchorEntryId(), locator.entryIdDelta()));
                if (row == null
                        || row.data().baseOffset() != Math.addExact(index.anchorOffset(), locator.baseOffsetDelta())
                        || row.data().endOffsetExclusive() - row.data().baseOffset() != locator.logicalOffsetCount()
                        || row.data().rawAssignedRecordBatch().length() != locator.payloadLength()) {
                    throw new IllegalArgumentException("native index locator differs from actual DATA");
                }
                if (groupBase == null) {
                    groupBase = row.groupOrdinal();
                    deltaBase = locator.appendGroupDelta();
                }
                if (row.groupOrdinal() - groupBase != locator.appendGroupDelta() - deltaBase) {
                    throw new IllegalArgumentException("native index append-group delta differs");
                }
            }
        }

        Snapshot finish() {
            if (footer == null || firstGroup != null || data.isEmpty() || nativeLength != seal.sealedLength()) {
                throw new IllegalArgumentException("native source is incomplete");
            }
            var expectedDirectory = indexes.entrySet().stream()
                    .map(entry -> new Nbke2IndexDirectoryEntryV1(
                            entry.getKey(),
                            entry.getValue().anchorOffset(),
                            entry.getValue().coveredThroughOffset()))
                    .toList();
            if (!footer.indexDirectory().equals(expectedDirectory)) {
                throw new IllegalArgumentException("native footer index directory differs");
            }
            boolean completeIndexes = !indexes.isEmpty()
                    && expectedDirectory.get(0).blockStartOffset()
                            == root.root().kafkaStartOffset();
            var format = Sha256Digest.copyOf(frames.digest());
            var body = Sha256Digest.copyOf(bodies.digest());
            var identity = Sha256Digest.hash(
                    CanonicalBytes.copyOf(ByteBuffer.allocate(84 + root.encode().length())
                            .putInt(0x4d354e53)
                            .put(root.encode().toByteArray())
                            .putLong(seal.sealedLastEntryId())
                            .putLong(seal.sealedLength())
                            .put(format.bytes().toByteArray())
                            .put(body.bytes().toByteArray())
                            .array()));
            var extent = new SourceExtent(
                    SourceKind.BOOKKEEPER_LEDGER,
                    identity,
                    new ProtocolCoverage(
                            PositionDomain.KAFKA_OFFSET, root.root().kafkaStartOffset(), nextOffset),
                    key,
                    length,
                    records,
                    records == 0 ? -1 : minimumTimestamp,
                    maximumTimestamp,
                    body,
                    Optional.empty(),
                    Optional.of(root.resource().sha256()),
                    format,
                    Sha256Digest.hash(CanonicalUtf8.fromString("NBKE2_V1_PLAINTEXT/"
                                    + client.capabilitySnapshot().credentialIdentityVersion())
                            .bytes()),
                    true,
                    false,
                    List.of(root.root().bindingId().digest()));
            var batches = new ArrayList<InputBatch>();
            data.values()
                    .forEach(row -> batches.add(
                            new InputBatch(identity, batches.size(), row.data().rawAssignedRecordBatch())));
            return new Snapshot(root, extent, batches, completeIndexes);
        }
    }

    private static MessageDigest sha() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
