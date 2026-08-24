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

package com.nereusstream.kafka.bookkeeper.object.nwkcp1;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeAssignedRecordBatchV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaRawAssignedRecordBatchFactsV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaRecoveryCheckpointVectorV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaAssignedProtocolBatchV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaBatchDuplicateIdentityV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCommittedProducerStateV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaLeaderEpochIndexV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaSpeculativeCommitV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaNwg1ObjectPipelineV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectBindingKeyV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectExtentIdentityV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectExtentLocatorV1;
import com.nereusstream.kafka.bookkeeper.object.read.KafkaObjectActiveTailStateV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaRecoveryBatchProtocolAdapterV1;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ObjectWalLeafKeyV1;
import com.nereusstream.storage.object.control.ProviderResolvedExtentRowV1;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunControlKeys;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunReference;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.nwg1.Nwg1DirectoryV1;
import com.nereusstream.storage.object.nwg1.Nwg1ObjectReaderV1;
import com.nereusstream.storage.object.nwg1.Nwg1VerificationContextV1;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.recovery.BoundedObjectTailRecovery;
import com.nereusstream.storage.object.recovery.OwnerOpenRecoveryCoordinator;
import com.nereusstream.storage.object.recovery.RecoveredWalRunRuntimeCut;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Authenticated Root/checkpoint-chain driven NWG1 replay; callers cannot supply state or charge their own budget. */
public final class KafkaNwg1SuffixReplayV1 implements OwnerOpenRecoveryCoordinator.ProtocolRecoveryHandler {
    private static final int CONTROL_METADATA_CAP_BYTES = 1024 * 1024;
    private static final int RECOVERED_COMMIT_MAGIC = 0x4b525331; // KRS1
    private static final int RECOVERED_COMMIT_VERSION = 1;
    private static final int APPEND_COMMIT_SET_ID_BYTES = 16;
    private static final int RECOVERED_COMMIT_FIXED_BYTES = 102;
    private static final int RECOVERED_COMMIT_RECORD_OVERHEAD_BYTES =
            Integer.BYTES + RECOVERED_COMMIT_FIXED_BYTES + Sha256Digest.LENGTH;
    private static final int PHYSICAL_ROW_BYTES = 56;
    private static final Set<PosixFilePermission> DIRECTORY_PERMISSIONS = PosixFilePermissions.fromString("rwx------");
    private static final Set<PosixFilePermission> FILE_PERMISSIONS = PosixFilePermissions.fromString("rw-------");
    private static final Path DEFAULT_SPOOL_BASE =
            Path.of(System.getProperty("java.io.tmpdir"), "nereus-m3-kafka-recovery-spool-v1");

    @FunctionalInterface
    interface ReplayTempFileHook {
        void beforeMerge(List<Path> laneFiles) throws IOException;

        default void filesCreated(List<Path> laneFiles) throws IOException {}

        default void beforeCleanup(List<Path> laneFiles) throws IOException {}
    }

    public record ReplayResult(KafkaProtocolCheckpointStateV1 state, KafkaObjectRecoveredTailV1 recoveredTail) {
        public ReplayResult {
            Objects.requireNonNull(state, "state");
            Objects.requireNonNull(recoveredTail, "recoveredTail");
            if (recoveredTail.activeTail().endOffsetExclusive()
                    != state.vector().recoveryCoveredThrough()) {
                throw new IllegalArgumentException("replayed active tail differs from protocol coverage");
            }
        }
    }

    private final WalRunReference rootReference;
    private final CanonicalControlMetadataStore metadata;
    private final WalRunObjectSession objectSession;
    private final Nwg1VerificationContextV1 verificationContext;
    private final KafkaRecoveryBatchProtocolAdapterV1 protocolAdapter;
    private final Nbke2RunBindingV1 expectedRunBinding;
    private final KafkaPartitionFenceV1 currentFence;
    private final long startOffset;
    private final ReplayTempFileHook tempFileHook;
    private final Path spoolBase;
    private final WalRunRootRecord ownerOpenRoot;
    private LaneCommitSpools ownerOpenSpools;
    private ReplayResult ownerOpenResult;
    private boolean ownerOpenAborted;
    private WalRunObjectSession ownerOpenInstallSession;

    @SuppressWarnings("ParameterNumber")
    KafkaNwg1SuffixReplayV1(
            WalRunReference rootReference,
            CanonicalControlMetadataStore metadata,
            WalRunObjectSession objectSession,
            Nwg1VerificationContextV1 verificationContext,
            KafkaRecoveryBatchProtocolAdapterV1 protocolAdapter,
            Nbke2RunBindingV1 expectedRunBinding,
            KafkaPartitionFenceV1 currentFence,
            long startOffset) {
        this(
                rootReference,
                metadata,
                objectSession,
                verificationContext,
                protocolAdapter,
                expectedRunBinding,
                currentFence,
                startOffset,
                ignored -> {},
                DEFAULT_SPOOL_BASE);
    }

    @SuppressWarnings("ParameterNumber")
    KafkaNwg1SuffixReplayV1(
            WalRunReference rootReference,
            CanonicalControlMetadataStore metadata,
            WalRunObjectSession objectSession,
            Nwg1VerificationContextV1 verificationContext,
            KafkaRecoveryBatchProtocolAdapterV1 protocolAdapter,
            Nbke2RunBindingV1 expectedRunBinding,
            KafkaPartitionFenceV1 currentFence,
            long startOffset,
            ReplayTempFileHook tempFileHook) {
        this(
                rootReference,
                metadata,
                objectSession,
                verificationContext,
                protocolAdapter,
                expectedRunBinding,
                currentFence,
                startOffset,
                tempFileHook,
                DEFAULT_SPOOL_BASE);
    }

    @SuppressWarnings("ParameterNumber")
    KafkaNwg1SuffixReplayV1(
            WalRunReference rootReference,
            CanonicalControlMetadataStore metadata,
            WalRunObjectSession objectSession,
            Nwg1VerificationContextV1 verificationContext,
            KafkaRecoveryBatchProtocolAdapterV1 protocolAdapter,
            Nbke2RunBindingV1 expectedRunBinding,
            KafkaPartitionFenceV1 currentFence,
            long startOffset,
            ReplayTempFileHook tempFileHook,
            Path spoolBase) {
        this.rootReference = Objects.requireNonNull(rootReference, "rootReference");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.objectSession = Objects.requireNonNull(objectSession, "objectSession");
        this.objectSession.requireRecoveredCurrentRoot();
        this.verificationContext = Objects.requireNonNull(verificationContext, "verificationContext");
        this.protocolAdapter = Objects.requireNonNull(protocolAdapter, "protocolAdapter");
        this.expectedRunBinding = Objects.requireNonNull(expectedRunBinding, "expectedRunBinding");
        this.currentFence = requireExactCurrentFence(currentFence, expectedRunBinding);
        this.tempFileHook = Objects.requireNonNull(tempFileHook, "tempFileHook");
        this.spoolBase =
                Objects.requireNonNull(spoolBase, "spoolBase").toAbsolutePath().normalize();
        this.ownerOpenRoot = null;
        if (startOffset < 0
                || !rootReference.rootSha256().equals(objectSession.rootSha256())
                || !Arrays.equals(
                        rootReference.rootSha256().bytes().toByteArray(), verificationContext.walRunRootSha256())) {
            throw new IllegalArgumentException("NWG1 replay authorities differ from the exact WalRun Root");
        }
        this.startOffset = startOffset;
    }

    /** Creates the Kafka protocol staging half of common's one-fence owner-open recovery coordinator. */
    public static KafkaNwg1SuffixReplayV1 ownerOpenHandler(
            WalRunReference rootReference,
            WalRunRootRecord root,
            Nwg1VerificationContextV1 verificationContext,
            KafkaRecoveryBatchProtocolAdapterV1 protocolAdapter,
            Nbke2RunBindingV1 expectedRunBinding,
            KafkaPartitionFenceV1 currentFence,
            long startOffset) {
        return ownerOpenHandler(
                rootReference,
                root,
                verificationContext,
                protocolAdapter,
                expectedRunBinding,
                currentFence,
                startOffset,
                ignored -> {},
                DEFAULT_SPOOL_BASE);
    }

    @SuppressWarnings("ParameterNumber")
    static KafkaNwg1SuffixReplayV1 ownerOpenHandler(
            WalRunReference rootReference,
            WalRunRootRecord root,
            Nwg1VerificationContextV1 verificationContext,
            KafkaRecoveryBatchProtocolAdapterV1 protocolAdapter,
            Nbke2RunBindingV1 expectedRunBinding,
            KafkaPartitionFenceV1 currentFence,
            long startOffset,
            ReplayTempFileHook tempFileHook,
            Path spoolBase) {
        return new KafkaNwg1SuffixReplayV1(
                rootReference,
                root,
                verificationContext,
                protocolAdapter,
                expectedRunBinding,
                currentFence,
                startOffset,
                tempFileHook,
                spoolBase);
    }

    @SuppressWarnings("ParameterNumber")
    private KafkaNwg1SuffixReplayV1(
            WalRunReference rootReference,
            WalRunRootRecord root,
            Nwg1VerificationContextV1 verificationContext,
            KafkaRecoveryBatchProtocolAdapterV1 protocolAdapter,
            Nbke2RunBindingV1 expectedRunBinding,
            KafkaPartitionFenceV1 currentFence,
            long startOffset,
            ReplayTempFileHook tempFileHook,
            Path spoolBase) {
        this.rootReference = Objects.requireNonNull(rootReference, "rootReference");
        this.metadata = null;
        this.objectSession = null;
        this.verificationContext = Objects.requireNonNull(verificationContext, "verificationContext");
        this.protocolAdapter = Objects.requireNonNull(protocolAdapter, "protocolAdapter");
        this.expectedRunBinding = Objects.requireNonNull(expectedRunBinding, "expectedRunBinding");
        this.currentFence = requireExactCurrentFence(currentFence, expectedRunBinding);
        this.tempFileHook = Objects.requireNonNull(tempFileHook, "tempFileHook");
        this.spoolBase =
                Objects.requireNonNull(spoolBase, "spoolBase").toAbsolutePath().normalize();
        this.ownerOpenRoot = Objects.requireNonNull(root, "root");
        if (startOffset < 0
                || !rootReference.rootSha256().equals(WalRunControlCodec.rootSha256(root))
                || !Arrays.equals(
                        rootReference.rootSha256().bytes().toByteArray(), verificationContext.walRunRootSha256())) {
            throw new IllegalArgumentException("NWG1 owner-open authorities differ from the exact WalRun Root");
        }
        requireRunContext(root);
        this.startOffset = startOffset;
    }

    public ReplayResult replay() {
        if (ownerOpenRoot != null) {
            throw new IllegalStateException("owner-open Kafka recovery must be driven by the common coordinator");
        }
        objectSession.requireRecoveredCurrentRoot();
        WalRunRootRecord root = objectSession.rootRecord();
        requireRunContext(root);
        String physicalHeadKey = WalRunControlKeys.checkpointHeadKey(root.shardId(), root.shardRunEpoch());
        objectSession.chargeRecoveryControlMetadata(CONTROL_METADATA_CAP_BYTES);
        CanonicalBytes headBytes = metadata.get(physicalHeadKey)
                .orElseThrow(() -> new KafkaObjectCheckpointException("physical checkpoint Head is absent"));
        var physicalHead = WalRunControlCodec.decodeCheckpointHead(headBytes);
        try (PhysicalRowSpool rows = new PhysicalRowSpool();
                LaneCommitSpools commitSpools = new LaneCommitSpools(root, tempFileHook, spoolBase)) {
            var verifiedChain = objectSession.verifyCheckpointChainStreaming(metadata, physicalHead, rows::append);
            rows.entries()
                    .sort(Comparator.comparingInt(StagedPhysicalRow::laneId)
                            .thenComparingLong(StagedPhysicalRow::laneSequence));
            for (StagedPhysicalRow staged : rows.entries()) {
                replayObject(root, WalRunControlCodec.decodeCheckpointRow(staged.bytes()), commitSpools);
            }
            rows.close();
            commitSpools.finishWriting();
            return fold(
                    commitSpools,
                    verifiedChain.aggregateExtentCount(),
                    verifiedChain.coveredThrough(),
                    physicalHeadKey,
                    Sha256Digest.hash(headBytes));
        }
    }

    @Override
    public synchronized void stage(
            ProviderResolvedExtentRowV1 row,
            Nwg1ObjectReaderV1.AuthenticatedPrefix prefix,
            BoundedObjectTailRecovery.SelectedAppendUnitReader appendUnitReader)
            throws IOException {
        requireOwnerOpenStaging();
        Objects.requireNonNull(row, "row");
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(appendUnitReader, "appendUnitReader");
        LaneCommitSpools recovered = ownerOpenSpools();
        KafkaObjectExtentIdentityV1 extent = new KafkaObjectExtentIdentityV1(
                rootReference.rootSha256(),
                row.laneId().code(),
                row.laneSequence(),
                row.directoryPrefixEnd(),
                row.bodyLength(),
                row.objectSha256());
        for (Nwg1DirectoryV1.AppendUnit candidate : prefix.directory().appendUnits()) {
            Nwg1DirectoryV1.KafkaAppendUnit unit = (Nwg1DirectoryV1.KafkaAppendUnit) candidate;
            if (!matchesExpectedBinding(prefix.directory(), unit)) {
                appendUnitReader.read(unit.firstFrameOrdinal(), (ignoredFrame, ignoredPayload) -> {});
                continue;
            }
            RecoveredFrameFold frameFold = new RecoveredFrameFold(unit);
            Nwg1ObjectReaderV1.VerifiedAppendUnit selected =
                    appendUnitReader.read(unit.firstFrameOrdinal(), frameFold::accept);
            recovered.append(recoverCommit(selected, unit, extent, frameFold.finish()));
        }
    }

    @Override
    public synchronized void install(WalRunObjectSession recoveredSession) throws IOException {
        requireOwnerOpenStaging();
        Objects.requireNonNull(recoveredSession, "recoveredSession");
        if (!recoveredSession.rootRecord().equals(ownerOpenRoot)) {
            throw new IllegalArgumentException("Kafka owner-open install substituted the exact WalRun Root");
        }
        ownerOpenInstallSession = recoveredSession;
        LaneCommitSpools recovered = ownerOpenSpools();
        boolean completed = false;
        try {
            RecoveredWalRunRuntimeCut.PhysicalRowsSummary summary =
                    recoveredSession.consumeRecoveredPhysicalRows(ignoredRow -> {});
            recovered.finishWriting();
            ownerOpenResult = fold(
                    recovered,
                    summary.aggregateExtentCount(),
                    summary.resolvedThrough(),
                    summary.physicalHeadKey(),
                    summary.physicalHeadSha256());
            completed = true;
        } finally {
            try {
                recovered.close();
            } finally {
                ownerOpenSpools = null;
                if (!completed) {
                    ownerOpenAborted = true;
                }
            }
        }
    }

    @Override
    public synchronized void abort() {
        if (ownerOpenSpools != null) {
            ownerOpenSpools.close();
            ownerOpenSpools = null;
        }
        ownerOpenAborted = true;
    }

    /** Returns the fully staged and installed fallback state after the common owner-open coordinator succeeds. */
    public synchronized ReplayResult ownerOpenResult() {
        if (ownerOpenResult == null || ownerOpenAborted || ownerOpenSpools != null) {
            throw new IllegalStateException("Kafka owner-open replay result is not installed");
        }
        return ownerOpenResult;
    }

    private void requireOwnerOpenStaging() {
        if (ownerOpenRoot == null || ownerOpenResult != null || ownerOpenAborted) {
            throw new IllegalStateException("Kafka owner-open staging is absent, installed, or aborted");
        }
    }

    private LaneCommitSpools ownerOpenSpools() {
        if (ownerOpenSpools == null) {
            ownerOpenSpools = new LaneCommitSpools(ownerOpenRoot, tempFileHook, spoolBase);
        }
        return ownerOpenSpools;
    }

    private WalRunObjectSession workingSession() {
        WalRunObjectSession session = objectSession != null ? objectSession : ownerOpenInstallSession;
        if (session == null) {
            throw new IllegalStateException("Kafka recovery session is unavailable outside owner-open install");
        }
        return session;
    }

    private void replayObject(WalRunRootRecord root, ProviderResolvedExtentRowV1 row, LaneCommitSpools recovered) {
        String key = ObjectWalLeafKeyV1.fromRow(row).fullKey(root.providerConfiguration());
        ObjectIdentity identity = new ObjectIdentity(key, row.bodyLength(), row.objectSha256());
        try {
            Nwg1ObjectReaderV1.AuthenticatedPrefix prefix =
                    objectSession.recoverAndVerifyNwg1Directory(identity, verificationContext);
            KafkaObjectExtentIdentityV1 extent = new KafkaObjectExtentIdentityV1(
                    rootReference.rootSha256(),
                    row.laneId().code(),
                    row.laneSequence(),
                    row.directoryPrefixEnd(),
                    row.bodyLength(),
                    row.objectSha256());
            for (Nwg1DirectoryV1.AppendUnit candidate : prefix.directory().appendUnits()) {
                Nwg1DirectoryV1.KafkaAppendUnit unit = (Nwg1DirectoryV1.KafkaAppendUnit) candidate;
                if (!matchesExpectedBinding(prefix.directory(), unit)) {
                    continue;
                }
                RecoveredFrameFold frameFold = new RecoveredFrameFold(unit);
                Nwg1ObjectReaderV1.VerifiedAppendUnit selected = objectSession.recoverAndVerifyNwg1AppendUnit(
                        identity, prefix, verificationContext, unit.firstFrameOrdinal(), frameFold::accept);
                RecoveredCommitMaterial material = recoverCommit(selected, unit, extent, frameFold.finish());
                recovered.append(material);
            }
            return;
        } catch (IOException failure) {
            throw new KafkaObjectCheckpointException("authenticated NWG1 replay GET failed", failure);
        }
    }

    private boolean matchesExpectedBinding(Nwg1DirectoryV1 directory, Nwg1DirectoryV1.KafkaAppendUnit unit) {
        if (unit.contextOrdinal() >= directory.bindings().size()
                || unit.partitionId() != expectedRunBinding.partitionId()
                || unit.kafkaLeaderEpoch() != expectedRunBinding.kafkaLeaderEpoch()) {
            return false;
        }
        Nwg1DirectoryV1.BindingContext binding = directory.bindings().get(Math.toIntExact(unit.contextOrdinal()));
        return Arrays.equals(
                        binding.bindingId(),
                        expectedRunBinding.bindingId().digest().bytes().toByteArray())
                && Arrays.equals(
                        binding.storageEpochId(),
                        expectedRunBinding.storageEpochId().digest().bytes().toByteArray())
                && Arrays.equals(
                        binding.nti1Bytes(),
                        TopicIncarnationIdentityCodecV1.encode(expectedRunBinding.topicIncarnation())
                                .toByteArray());
    }

    private RecoveredCommitMaterial recoverCommit(
            Nwg1ObjectReaderV1.VerifiedAppendUnit verifiedUnit,
            Nwg1DirectoryV1.KafkaAppendUnit unit,
            KafkaObjectExtentIdentityV1 extent,
            List<KafkaAssignedProtocolBatchV1> batches) {
        if (verifiedUnit.protocolKind() != 1
                || verifiedUnit.contextOrdinal() != unit.contextOrdinal()
                || verifiedUnit.firstFrameOrdinal() != unit.firstFrameOrdinal()
                || verifiedUnit.frameCount() != unit.frameCount()
                || verifiedUnit.coverage0() != unit.startOffset()
                || verifiedUnit.coverage1() != unit.endOffsetExclusive()
                || !Arrays.equals(verifiedUnit.appendCommitSetId(), unit.appendCommitSetId())
                || !Arrays.equals(verifiedUnit.storageAttemptId(), unit.storageAttemptId())
                || !Arrays.equals(verifiedUnit.assignedPayloadSha256(), unit.assignedPayloadSha256())) {
            throw new KafkaObjectCheckpointException("streamed NWG1 unit differs from its authenticated Directory row");
        }
        KafkaSpeculativeCommitV1 commit =
                new KafkaSpeculativeCommitV1(unit.startOffset(), unit.endOffsetExclusive(), expectedFence(), batches);
        if (!Arrays.equals(unit.appendCommitSetId(), KafkaNwg1ObjectPipelineV1.commitSetId(commit))) {
            throw new KafkaObjectCheckpointException(
                    "NWG1 replayed native deltas differ from the sealed Kafka commit-set ID");
        }
        KafkaObjectExtentLocatorV1 locator = new KafkaObjectExtentLocatorV1(
                expectedBinding(),
                unit.startOffset(),
                unit.endOffsetExclusive(),
                extent,
                Math.toIntExact(unit.firstFrameOrdinal()),
                Math.toIntExact(unit.frameCount()));
        return new RecoveredCommitMaterial(commit, locator, unit.appendCommitSetId());
    }

    /** Folds each borrowed decoded frame into compact protocol state before common erases the frame payload. */
    private final class RecoveredFrameFold {
        private final Nwg1DirectoryV1.KafkaAppendUnit unit;
        private final ArrayList<KafkaAssignedProtocolBatchV1> batches = new ArrayList<>();
        private long nextOffset;

        private RecoveredFrameFold(Nwg1DirectoryV1.KafkaAppendUnit unit) {
            this.unit = Objects.requireNonNull(unit, "unit");
            this.nextOffset = unit.startOffset();
        }

        private void accept(Nwg1ObjectReaderV1.VerifiedFrame frame, ByteBuffer borrowedPayload) {
            Objects.requireNonNull(frame, "frame");
            Objects.requireNonNull(borrowedPayload, "borrowedPayload");
            long expectedAbsoluteOrdinal = Math.addExact(unit.firstFrameOrdinal(), batches.size());
            if (frame.absoluteFrameOrdinal() != expectedAbsoluteOrdinal
                    || frame.appendUnitFrameOrdinal() != batches.size()
                    || frame.coverage0() != nextOffset
                    || frame.decodedPayloadBytes() != borrowedPayload.remaining()) {
                throw new KafkaObjectCheckpointException(
                        "streamed NWG1 frame order/coverage/length differs from its Directory row");
            }
            byte[] raw = new byte[borrowedPayload.remaining()];
            borrowedPayload.get(raw);
            try {
                KafkaNativeAssignedRecordBatchV1 nativeBatch = KafkaNativeAssignedRecordBatchV1.validate(
                        KafkaRawAssignedRecordBatchFactsV1.parse(CanonicalBytes.copyOf(raw)));
                if (nativeBatch.baseOffset() != frame.coverage0()
                        || nativeBatch.endOffsetExclusive() != frame.coverage1()
                        || nativeBatch.partitionLeaderEpoch() != expectedRunBinding.kafkaLeaderEpoch()) {
                    throw new KafkaObjectCheckpointException(
                            "NWG1 native frame coverage differs from its Directory unit");
                }
                var delta = Objects.requireNonNull(protocolAdapter.protocolDelta(nativeBatch), "protocol delta");
                if (delta.logicalOffsetCount() != nativeBatch.endOffsetExclusive() - nativeBatch.baseOffset()) {
                    throw new KafkaObjectCheckpointException(
                            "native replay delta differs from exact RecordBatch coverage");
                }
                batches.add(new KafkaAssignedProtocolBatchV1(
                        nativeBatch.baseOffset(), nativeBatch.endOffsetExclusive(), delta));
                nextOffset = nativeBatch.endOffsetExclusive();
            } finally {
                Arrays.fill(raw, (byte) 0);
            }
        }

        private List<KafkaAssignedProtocolBatchV1> finish() {
            if (nextOffset != unit.endOffsetExclusive() || batches.size() != unit.frameCount()) {
                throw new KafkaObjectCheckpointException("NWG1 complete commit-set coverage is not exact");
            }
            return List.copyOf(batches);
        }
    }

    private ReplayResult fold(
            LaneCommitSpools recovered,
            long aggregatePhysicalExtents,
            com.nereusstream.storage.object.control.LaneSequenceVector coveredThrough,
            String physicalHeadKey,
            Sha256Digest physicalHeadSha) {
        KafkaCommittedProducerStateV1 producers = KafkaCommittedProducerStateV1.empty();
        KafkaTransactionStateV1 transactions = KafkaTransactionStateV1.empty();
        KafkaLeaderEpochIndexV1 leaderEpochs = KafkaLeaderEpochIndexV1.empty();
        KafkaObjectBindingKeyV1 binding = new KafkaObjectBindingKeyV1(
                expectedRunBinding.bindingId(),
                expectedRunBinding.topicIncarnation().topicId(),
                expectedRunBinding.partitionId(),
                expectedRunBinding.storageEpochId());
        KafkaObjectActiveTailStateV1 activeTail = KafkaObjectActiveTailStateV1.empty(binding, startOffset);
        long next = startOffset;
        LaneHead[] heads = recovered.initialHeads();
        while (true) {
            int selectedLane = selectNextLane(heads);
            if (selectedLane < 0) {
                break;
            }
            try (LeasedRecoveredCommit leased = recovered.read(heads[selectedLane])) {
                RecoveredCommit value = leased.value();
                if (value.commit.startOffset() != next) {
                    throw new KafkaObjectCheckpointException(
                            "authenticated NWG1 Kafka suffix contains a gap/duplicate");
                }
                producers = producers.apply(value.commit);
                transactions = transactions.apply(value.commit);
                leaderEpochs = leaderEpochs.observe(expectedRunBinding.kafkaLeaderEpoch(), value.commit.startOffset());
                activeTail = activeTail.append(value.locator);
                next = value.commit.endOffsetExclusive();
            }
            heads[selectedLane] = recovered.peek(selectedLane);
        }
        long computedPhysicalExtents = 0;
        for (long lane : coveredThrough.toArray()) {
            if (lane >= 0) {
                computedPhysicalExtents = Math.addExact(computedPhysicalExtents, Math.incrementExact(lane));
            }
        }
        if (computedPhysicalExtents != aggregatePhysicalExtents) {
            throw new KafkaObjectCheckpointException("physical checkpoint vector/count proof is inconsistent");
        }
        KafkaRecoveryCheckpointVectorV1 vector =
                new KafkaRecoveryCheckpointVectorV1(expectedRunBinding, next, next, next, next);
        return new ReplayResult(
                new KafkaProtocolCheckpointStateV1(vector, producers, transactions, leaderEpochs),
                new KafkaObjectRecoveredTailV1(
                        rootReference.rootSha256(), physicalHeadKey, physicalHeadSha, activeTail));
    }

    private void requireRunContext(WalRunRootRecord root) {
        if (!rootReference.rootSha256().equals(WalRunControlCodec.rootSha256(root))
                || root.shardId() != rootReference.shardId()
                || root.shardRunEpoch() != rootReference.shardRunEpoch()
                || !root.providerScopeId().equals(expectedRunBinding.providerScopeId())
                || !root.walRunSessionId().equals(expectedRunBinding.runId().value())
                || !root.protocolCellIdentity().equals(verificationContext.protocolCell())) {
            throw new KafkaObjectCheckpointException(
                    "NWG1 replay Root substituted ProviderScope/StorageRun/ProtocolCell context");
        }
    }

    private static int recoveredCommitCanonicalLength(RecoveredCommitMaterial material) {
        int exactBytes = RECOVERED_COMMIT_FIXED_BYTES;
        for (KafkaAssignedProtocolBatchV1 batch : material.commit().batches()) {
            exactBytes =
                    Math.addExact(exactBytes, batch.delta().duplicateIdentity().isPresent() ? 56 : 38);
        }
        return exactBytes;
    }

    private static byte[] encodeRecoveredCommit(RecoveredCommitMaterial material, int exactBytes) {
        KafkaObjectExtentLocatorV1 locator = material.locator();
        KafkaObjectExtentIdentityV1 extent = locator.extent();
        ByteBuffer target = ByteBuffer.allocate(exactBytes).order(ByteOrder.BIG_ENDIAN);
        target.putInt(RECOVERED_COMMIT_MAGIC);
        target.put((byte) RECOVERED_COMMIT_VERSION);
        target.put((byte) extent.laneId());
        target.putLong(extent.laneSequence());
        target.putInt(Math.toIntExact(extent.directoryPrefixEnd()));
        target.putLong(extent.bodyLength());
        target.put(extent.bodySha().bytes().toByteArray());
        target.putLong(material.commit().startOffset());
        target.putLong(material.commit().endOffsetExclusive());
        target.put(material.appendCommitSetId());
        target.putInt(locator.firstDirectoryRow());
        target.putInt(locator.directoryRowCount());
        target.putInt(material.commit().batches().size());
        for (KafkaAssignedProtocolBatchV1 batch : material.commit().batches()) {
            target.putLong(batch.startOffset());
            target.putLong(batch.endOffsetExclusive());
            var delta = batch.delta();
            target.putLong(delta.logicalOffsetCount());
            target.put((byte) (delta.duplicateIdentity().isPresent() ? 1 : 0));
            if (delta.duplicateIdentity().isPresent()) {
                KafkaBatchDuplicateIdentityV1 duplicate =
                        delta.duplicateIdentity().orElseThrow();
                target.putLong(duplicate.producerId());
                target.putShort(duplicate.producerEpoch());
                target.putInt(duplicate.baseSequence());
                target.putInt(duplicate.lastSequence());
            }
            target.put((byte) delta.transactionKind().ordinal());
            target.putLong(delta.transactionalProducerId());
            target.putInt(delta.coordinatorEpoch());
        }
        if (target.hasRemaining()) {
            throw new IllegalStateException("Kafka recovery spool length differs from its exact encoding");
        }
        return target.array();
    }

    private RecoveredCommit decodeRecoveredCommit(byte[] exact) {
        if (exact.length < RECOVERED_COMMIT_FIXED_BYTES) {
            throw new KafkaObjectCheckpointException("Kafka recovery spool entry is truncated");
        }
        ByteBuffer source = ByteBuffer.wrap(exact).order(ByteOrder.BIG_ENDIAN);
        if (source.getInt() != RECOVERED_COMMIT_MAGIC || Byte.toUnsignedInt(source.get()) != RECOVERED_COMMIT_VERSION) {
            throw new KafkaObjectCheckpointException("Kafka recovery spool preamble is not canonical");
        }
        int laneId = Byte.toUnsignedInt(source.get());
        long laneSequence = source.getLong();
        int directoryPrefixEnd = source.getInt();
        long bodyLength = source.getLong();
        byte[] bodySha = new byte[Sha256Digest.LENGTH];
        source.get(bodySha);
        long commitStart = source.getLong();
        long commitEnd = source.getLong();
        byte[] appendCommitSetId = new byte[APPEND_COMMIT_SET_ID_BYTES];
        source.get(appendCommitSetId);
        int firstDirectoryRow = source.getInt();
        int directoryRowCount = source.getInt();
        int batchCount = source.getInt();
        if (batchCount <= 0 || batchCount != directoryRowCount || batchCount > 65_536) {
            throw new KafkaObjectCheckpointException("Kafka recovery spool batch inventory is not canonical");
        }
        ArrayList<KafkaAssignedProtocolBatchV1> batches = new ArrayList<>(batchCount);
        long nextOffset = commitStart;
        for (int index = 0; index < batchCount; index++) {
            requireRemaining(source, 38);
            long batchStart = source.getLong();
            long batchEnd = source.getLong();
            long logicalOffsetCount = source.getLong();
            int duplicateFlag = Byte.toUnsignedInt(source.get());
            if (duplicateFlag > 1) {
                throw new KafkaObjectCheckpointException("Kafka recovery spool duplicate flag is not canonical");
            }
            Optional<KafkaBatchDuplicateIdentityV1> duplicate = Optional.empty();
            if (duplicateFlag == 1) {
                requireRemaining(source, 18);
                duplicate = Optional.of(new KafkaBatchDuplicateIdentityV1(
                        source.getLong(), source.getShort(), source.getInt(), source.getInt()));
            }
            int transactionKind = Byte.toUnsignedInt(source.get());
            if (transactionKind
                    >= com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionBatchKindV1.values().length) {
                throw new KafkaObjectCheckpointException("Kafka recovery spool transaction kind is not closed");
            }
            var delta = new com.nereusstream.kafka.bookkeeper.commit.KafkaProtocolBatchDeltaV1(
                    logicalOffsetCount,
                    duplicate,
                    com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionBatchKindV1.values()[transactionKind],
                    source.getLong(),
                    source.getInt());
            long exactCoverage;
            try {
                exactCoverage = Math.subtractExact(batchEnd, batchStart);
            } catch (ArithmeticException failure) {
                throw new KafkaObjectCheckpointException("Kafka recovery spool protocol coverage overflows", failure);
            }
            if (batchStart != nextOffset || exactCoverage != logicalOffsetCount) {
                throw new KafkaObjectCheckpointException("Kafka recovery spool protocol coverage is not canonical");
            }
            batches.add(new KafkaAssignedProtocolBatchV1(batchStart, batchEnd, delta));
            nextOffset = batchEnd;
        }
        if (source.hasRemaining() || nextOffset != commitEnd) {
            throw new KafkaObjectCheckpointException("Kafka recovery spool has trailing bytes or inexact coverage");
        }
        KafkaSpeculativeCommitV1 commit =
                new KafkaSpeculativeCommitV1(commitStart, commitEnd, expectedFence(), batches);
        if (!Arrays.equals(appendCommitSetId, KafkaNwg1ObjectPipelineV1.commitSetId(commit))) {
            throw new KafkaObjectCheckpointException("Kafka recovery spool commit-set identity is not canonical");
        }
        KafkaObjectExtentIdentityV1 extent = new KafkaObjectExtentIdentityV1(
                rootReference.rootSha256(),
                laneId,
                laneSequence,
                directoryPrefixEnd,
                bodyLength,
                Sha256Digest.copyOf(bodySha));
        KafkaObjectExtentLocatorV1 locator = new KafkaObjectExtentLocatorV1(
                expectedBinding(), commitStart, commitEnd, extent, firstDirectoryRow, directoryRowCount);
        return new RecoveredCommit(commit, locator);
    }

    private static void requireRemaining(ByteBuffer source, int required) {
        if (source.remaining() < required) {
            throw new KafkaObjectCheckpointException("Kafka recovery spool protocol delta is truncated");
        }
    }

    private KafkaPartitionFenceV1 expectedFence() {
        // NBKE2 freezes the historical run creator/leader witness. Takeover may use a later live owner/leader, but
        // it cannot rewrite an already sealed append commit-set identity.
        return new KafkaPartitionFenceV1(
                expectedRunBinding.bindingId(),
                expectedRunBinding.topicIncarnation(),
                expectedRunBinding.partitionId(),
                currentFence.bindingGeneration(),
                expectedRunBinding.storageEpochId(),
                expectedRunBinding.creatorOwnerEpoch(),
                expectedRunBinding.kafkaLeaderEpoch());
    }

    private static KafkaPartitionFenceV1 requireExactCurrentFence(
            KafkaPartitionFenceV1 currentFence, Nbke2RunBindingV1 expectedRunBinding) {
        Objects.requireNonNull(currentFence, "currentFence");
        if (!currentFence.bindingId().equals(expectedRunBinding.bindingId())
                || !currentFence.topicIncarnation().equals(expectedRunBinding.topicIncarnation())
                || currentFence.partitionId() != expectedRunBinding.partitionId()
                || !currentFence.storageEpochId().equals(expectedRunBinding.storageEpochId())
                || currentFence.ownerEpoch() < expectedRunBinding.creatorOwnerEpoch()
                || currentFence.kafkaLeaderEpoch() < expectedRunBinding.kafkaLeaderEpoch()) {
            throw new IllegalArgumentException(
                    "current Kafka partition fence differs from the exact Root-bound NBKE2 run binding");
        }
        return currentFence;
    }

    private KafkaObjectBindingKeyV1 expectedBinding() {
        return new KafkaObjectBindingKeyV1(
                expectedRunBinding.bindingId(),
                expectedRunBinding.topicIncarnation().topicId(),
                expectedRunBinding.partitionId(),
                expectedRunBinding.storageEpochId());
    }

    private final class PhysicalRowSpool implements AutoCloseable {
        private final ArrayList<StagedPhysicalRow> entries = new ArrayList<>();
        private WalRunObjectSession.RecoveryWorkingSetLease lease;
        private boolean closed;

        private void append(ProviderResolvedExtentRowV1 row) {
            requireOpen();
            int exactBytes = WalRunControlCodec.checkpointRowCanonicalLength(row);
            if (exactBytes != PHYSICAL_ROW_BYTES) {
                throw new KafkaObjectCheckpointException(
                        "M3 Kafka physical row spool requires the exact ProviderProofMode.NONE wire");
            }
            if (lease == null) {
                lease = objectSession.acquireRecoveryWorkingSet(exactBytes);
            } else {
                lease.grow(exactBytes);
            }
            CanonicalBytes encoded = WalRunControlCodec.encodeCheckpointRow(row);
            if (encoded.length() != exactBytes) {
                throw new IllegalStateException("Kafka physical row changed during exact spool staging");
            }
            entries.add(new StagedPhysicalRow(encoded, row.laneId().code(), row.laneSequence()));
            if (entries.size() > objectSession.rootRecord().bounds().maxExtentCount()) {
                throw new KafkaObjectCheckpointException("Kafka physical row spool exceeds the Root extent bound");
            }
        }

        private List<StagedPhysicalRow> entries() {
            requireOpen();
            return entries;
        }

        private void requireOpen() {
            if (closed) {
                throw new IllegalStateException("Kafka recovery spool was already released");
            }
        }

        @Override
        public void close() {
            if (!closed) {
                if (lease != null) {
                    lease.close();
                }
                entries.clear();
                closed = true;
            }
        }
    }

    private record StagedPhysicalRow(CanonicalBytes bytes, int laneId, long laneSequence) {}

    private final class LaneCommitSpools implements AutoCloseable {
        private final ReplayTempFileHook hook;
        private final SecureRecoverySpoolManager manager;
        private final Path[] paths;
        private final FileChannel[] channels;
        private final long[] readPositions = new long[3];
        private final long[] lastLaneSequence = {-1, -1, -1};
        private final long[] lastLaneEndOffset = {-1, -1, -1};
        private final long diskCap;
        private long diskBytes;
        private boolean writing = true;
        private boolean closed;

        private LaneCommitSpools(WalRunRootRecord root, ReplayTempFileHook hook, Path spoolBase) {
            Objects.requireNonNull(root, "root");
            this.hook = Objects.requireNonNull(hook, "hook");
            this.diskCap = recoveryDiskCap(root);
            try {
                this.manager = new SecureRecoverySpoolManager(
                        spoolBase, root, diskCap, recoveryCellDiskCap(root, diskCap), attemptWitness(root), hook);
                this.paths = manager.paths();
                this.channels = manager.channels();
            } catch (IOException failure) {
                throw new KafkaObjectCheckpointException("cannot create bounded Kafka recovery lane spools", failure);
            }
        }

        private void append(RecoveredCommitMaterial material) {
            requireWriting();
            int lane = material.locator().extent().laneId();
            long laneSequence = material.locator().extent().laneSequence();
            if (lane < 0
                    || lane >= channels.length
                    || laneSequence < lastLaneSequence[lane]
                    || material.commit().startOffset() < lastLaneEndOffset[lane]) {
                throw new KafkaObjectCheckpointException(
                        "Kafka recovery lane sequence or startOffset is not strictly monotonic");
            }
            int exactBytes = recoveredCommitCanonicalLength(material);
            if (Math.addExact(exactBytes, 2L * Sha256Digest.LENGTH)
                    > material.locator().extent().bodyLength()) {
                throw new KafkaObjectCheckpointException(
                        "Kafka recovery current-record working set exceeds its authenticated NWG1 body");
            }
            long recordBytes = Math.addExact(Math.addExact((long) Integer.BYTES, exactBytes), Sha256Digest.LENGTH);
            long nextDiskBytes = requireDiskCapacity(diskBytes, recordBytes, diskCap);
            WalRunObjectSession.RecoveryWorkingSetLease lease = objectSession == null
                    ? null
                    : objectSession.acquireRecoveryWorkingSet(
                            Math.addExact(exactBytes, Math.addExact(Integer.BYTES, Sha256Digest.LENGTH)));
            try (SecureRecoverySpoolManager.CellQuotaReservation ignored = manager.reserveCellBytes(recordBytes)) {
                byte[] exact = encodeRecoveredCommit(material, exactBytes);
                byte[] digest = sha256(exact);
                ByteBuffer length = ByteBuffer.allocate(Integer.BYTES)
                        .order(ByteOrder.BIG_ENDIAN)
                        .putInt(exactBytes);
                length.flip();
                writeFully(channels[lane], length);
                writeFully(channels[lane], ByteBuffer.wrap(exact));
                writeFully(channels[lane], ByteBuffer.wrap(digest));
            } catch (IOException failure) {
                throw new KafkaObjectCheckpointException("cannot append bounded Kafka recovery lane spool", failure);
            } finally {
                if (lease != null) {
                    lease.close();
                }
            }
            diskBytes = nextDiskBytes;
            lastLaneSequence[lane] = laneSequence;
            lastLaneEndOffset[lane] = material.commit().endOffsetExclusive();
        }

        private void finishWriting() {
            requireWriting();
            try {
                for (FileChannel channel : channels) {
                    channel.force(true);
                }
                hook.beforeMerge(laneFiles());
            } catch (IOException failure) {
                throw new KafkaObjectCheckpointException("Kafka recovery temp-spool hook failed", failure);
            }
            writing = false;
        }

        private LaneHead[] initialHeads() {
            requireReading();
            LaneHead[] heads = new LaneHead[channels.length];
            for (int lane = 0; lane < heads.length; lane++) {
                heads[lane] = peek(lane);
            }
            return heads;
        }

        private LaneHead peek(int lane) {
            requireReading();
            if (lane < 0 || lane >= channels.length) {
                throw new IllegalArgumentException("Kafka recovery lane is outside the permanent inventory");
            }
            try {
                long position = readPositions[lane];
                long size = channels[lane].size();
                if (position == size) {
                    return null;
                }
                if (position < 0 || position > size || size - position < Integer.BYTES) {
                    throw new KafkaObjectCheckpointException("Kafka recovery lane spool length prefix is truncated");
                }
                try (WalRunObjectSession.RecoveryWorkingSetLease lease = workingSession()
                        .acquireRecoveryWorkingSet(Math.addExact(Integer.BYTES, RECOVERED_COMMIT_FIXED_BYTES))) {
                    ByteBuffer length = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN);
                    readFully(channels[lane], position, length);
                    int entryBytes = length.flip().getInt();
                    long recordEnd = Math.addExact(
                            Math.addExact(position, Math.addExact((long) Integer.BYTES, entryBytes)),
                            Sha256Digest.LENGTH);
                    if (entryBytes < RECOVERED_COMMIT_FIXED_BYTES || recordEnd > size) {
                        throw new KafkaObjectCheckpointException("Kafka recovery lane spool record is truncated");
                    }
                    ByteBuffer fixed =
                            ByteBuffer.allocate(RECOVERED_COMMIT_FIXED_BYTES).order(ByteOrder.BIG_ENDIAN);
                    readFully(channels[lane], Math.addExact(position, Integer.BYTES), fixed);
                    fixed.flip();
                    if (fixed.getInt() != RECOVERED_COMMIT_MAGIC
                            || Byte.toUnsignedInt(fixed.get()) != RECOVERED_COMMIT_VERSION
                            || Byte.toUnsignedInt(fixed.get()) != lane) {
                        throw new KafkaObjectCheckpointException(
                                "Kafka recovery lane spool preamble/lane is not canonical");
                    }
                    fixed.position(58);
                    long start = fixed.getLong();
                    return new LaneHead(lane, position, entryBytes, start);
                }
            } catch (IOException failure) {
                throw new KafkaObjectCheckpointException("cannot inspect Kafka recovery lane spool", failure);
            }
        }

        private LeasedRecoveredCommit read(LaneHead head) {
            requireReading();
            Objects.requireNonNull(head, "head");
            if (head.position() != readPositions[head.laneId()]) {
                throw new KafkaObjectCheckpointException("Kafka recovery lane cursor was substituted");
            }
            int leasedBytes = Math.addExact(head.entryBytes(), 2 * Sha256Digest.LENGTH);
            WalRunObjectSession.RecoveryWorkingSetLease lease = workingSession().acquireRecoveryWorkingSet(leasedBytes);
            try {
                byte[] exact = new byte[head.entryBytes()];
                ByteBuffer body = ByteBuffer.wrap(exact);
                readFully(channels[head.laneId()], Math.addExact(head.position(), Integer.BYTES), body);
                byte[] storedDigest = new byte[Sha256Digest.LENGTH];
                readFully(
                        channels[head.laneId()],
                        Math.addExact(Math.addExact(head.position(), Integer.BYTES), head.entryBytes()),
                        ByteBuffer.wrap(storedDigest));
                if (!MessageDigest.isEqual(storedDigest, sha256(exact))) {
                    throw new KafkaObjectCheckpointException("Kafka recovery lane spool record SHA-256 mismatch");
                }
                RecoveredCommit value = decodeRecoveredCommit(exact);
                if (value.locator().extent().laneId() != head.laneId()
                        || value.commit().startOffset() != head.startOffset()) {
                    throw new KafkaObjectCheckpointException("Kafka recovery lane spool selected a different record");
                }
                readPositions[head.laneId()] = Math.addExact(
                        Math.addExact(head.position(), Math.addExact(Integer.BYTES, head.entryBytes())),
                        Sha256Digest.LENGTH);
                return new LeasedRecoveredCommit(value, lease);
            } catch (IOException | RuntimeException failure) {
                lease.close();
                if (failure instanceof KafkaObjectCheckpointException checkpointFailure) {
                    throw checkpointFailure;
                }
                throw new KafkaObjectCheckpointException("cannot read Kafka recovery lane spool", failure);
            }
        }

        private void requireWriting() {
            if (closed || !writing) {
                throw new IllegalStateException("Kafka recovery lane spool is not writable");
            }
        }

        private List<Path> laneFiles() {
            return List.of(paths[0], paths[1], paths[2]);
        }

        private void requireReading() {
            if (closed || writing) {
                throw new IllegalStateException("Kafka recovery lane spool is not merge-readable");
            }
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            manager.close();
        }
    }

    /**
     * Root-scoped deterministic replay workspace. The Root lock serializes old/new local attempts, while the Cell
     * quota lock makes cross-Root disk accounting atomic. Directories and files are owner-only and symlinks or
     * unknown entries fail closed; a later exact-Root attempt retries deletion of any prior orphan lane file.
     */
    private static final class SecureRecoverySpoolManager implements AutoCloseable {
        private static final String ATTEMPT_LOCK = "attempt.lock";
        private static final String CELL_QUOTA_LOCK = "quota.lock";
        private static final int LANE_COUNT = 3;

        private final ReplayTempFileHook hook;
        private final Path cellDirectory;
        private final Path rootDirectory;
        private final Path[] paths = new Path[LANE_COUNT];
        private final FileChannel[] channels = new FileChannel[LANE_COUNT];
        private final long cellDiskCap;
        private FileChannel quotaLockChannel;
        private FileChannel attemptLockChannel;
        private FileLock attemptLock;
        private boolean initialized;
        private boolean closed;

        private SecureRecoverySpoolManager(
                Path base,
                WalRunRootRecord root,
                long rootDiskCap,
                long cellDiskCap,
                byte[] attemptWitness,
                ReplayTempFileHook hook)
                throws IOException {
            Objects.requireNonNull(base, "base");
            Objects.requireNonNull(root, "root");
            if (rootDiskCap <= 0 || cellDiskCap < rootDiskCap) {
                throw new IllegalArgumentException("Kafka recovery disk-spool Root/Cell caps are inconsistent");
            }
            this.hook = Objects.requireNonNull(hook, "hook");
            this.cellDiskCap = cellDiskCap;
            Path exactBase = base.toAbsolutePath().normalize();
            this.cellDirectory =
                    exactBase.resolve(root.providerScopeId().digest().toHex());
            this.rootDirectory =
                    cellDirectory.resolve(WalRunControlCodec.rootSha256(root).toHex());
            try {
                requireSecureDirectory(exactBase);
                requireSecureDirectory(cellDirectory);
                quotaLockChannel = openSecureLockFile(cellDirectory.resolve(CELL_QUOTA_LOCK));
                requireSecureDirectory(rootDirectory);
                attemptLockChannel = openSecureLockFile(rootDirectory.resolve(ATTEMPT_LOCK));
                attemptLock = tryExclusiveLock(attemptLockChannel, "exact Root recovery attempt is already active");
                writeAttemptWitness(attemptWitness);
                try (CellQuotaReservation ignored = reserveCellBytes(1, false)) {
                    cleanExactRootOrphans();
                    for (int lane = 0; lane < LANE_COUNT; lane++) {
                        paths[lane] = rootDirectory.resolve("lane-" + lane + ".spool");
                        channels[lane] = createSecureLaneFile(paths[lane]);
                    }
                }
                hook.filesCreated(List.of(paths()));
                initialized = true;
            } catch (IOException | RuntimeException failure) {
                RuntimeException cleanupFailure = cleanup(false);
                if (cleanupFailure != null) {
                    failure.addSuppressed(cleanupFailure);
                }
                throw failure;
            }
        }

        private Path[] paths() {
            return paths.clone();
        }

        private FileChannel[] channels() {
            return channels.clone();
        }

        private CellQuotaReservation reserveCellBytes(long additionalBytes) throws IOException {
            return reserveCellBytes(additionalBytes, true);
        }

        private CellQuotaReservation reserveCellBytes(long additionalBytes, boolean includeAdditional)
                throws IOException {
            if (closed || additionalBytes <= 0) {
                throw new IllegalStateException("Kafka recovery Cell quota reservation is closed or non-positive");
            }
            FileLock quotaLock = tryExclusiveLock(quotaLockChannel, "Kafka recovery Cell quota is busy");
            boolean accepted = false;
            try {
                long current = exactCellSpoolBytes();
                if (includeAdditional) {
                    requireDiskCapacity(current, additionalBytes, cellDiskCap);
                } else if (current > cellDiskCap) {
                    throw new KafkaObjectCheckpointException(
                            "Kafka recovery Cell disk spool exceeds the exact Root-lineage cap");
                }
                CellQuotaReservation reservation = new CellQuotaReservation(quotaLock);
                accepted = true;
                return reservation;
            } finally {
                if (!accepted) {
                    quotaLock.release();
                }
            }
        }

        private long exactCellSpoolBytes() throws IOException {
            long exactBytes = 0;
            try (DirectoryStream<Path> roots = Files.newDirectoryStream(cellDirectory)) {
                for (Path candidateRoot : roots) {
                    String name = candidateRoot.getFileName().toString();
                    if (name.equals(CELL_QUOTA_LOCK)) {
                        requireSecureRegularFile(candidateRoot);
                        continue;
                    }
                    requireSecureDirectoryEntry(candidateRoot);
                    try (DirectoryStream<Path> entries = Files.newDirectoryStream(candidateRoot)) {
                        for (Path entry : entries) {
                            String entryName = entry.getFileName().toString();
                            if (entryName.equals(ATTEMPT_LOCK)) {
                                requireSecureRegularFile(entry);
                                continue;
                            }
                            if (!isLaneFile(entryName)) {
                                throw new KafkaObjectCheckpointException(
                                        "Kafka recovery Cell spool contains an unknown Root entry");
                            }
                            requireSecureRegularFile(entry);
                            exactBytes = Math.addExact(exactBytes, Files.size(entry));
                        }
                    }
                }
            } catch (ArithmeticException failure) {
                throw new KafkaObjectCheckpointException("Kafka recovery Cell disk accounting overflows", failure);
            }
            return exactBytes;
        }

        private void cleanExactRootOrphans() throws IOException {
            try (DirectoryStream<Path> entries = Files.newDirectoryStream(rootDirectory)) {
                for (Path entry : entries) {
                    String name = entry.getFileName().toString();
                    if (name.equals(ATTEMPT_LOCK)) {
                        requireSecureRegularFile(entry);
                        continue;
                    }
                    if (!isLaneFile(name)) {
                        throw new KafkaObjectCheckpointException(
                                "Kafka recovery Root spool contains an unknown orphan entry");
                    }
                    requireSecureRegularFile(entry);
                    Files.delete(entry);
                }
            }
        }

        private void writeAttemptWitness(byte[] exactWitness) throws IOException {
            byte[] witness =
                    Objects.requireNonNull(exactWitness, "attemptWitness").clone();
            if (witness.length == 0 || witness.length > 1024) {
                throw new IllegalArgumentException("Kafka recovery attempt witness is outside its exact cap");
            }
            attemptLockChannel.truncate(0);
            attemptLockChannel.position(0);
            writeFully(attemptLockChannel, ByteBuffer.wrap(witness));
            attemptLockChannel.force(true);
        }

        private static FileChannel createSecureLaneFile(Path path) throws IOException {
            if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
                throw new KafkaObjectCheckpointException("Kafka recovery lane orphan survived exact cleanup");
            }
            FileChannel channel = FileChannel.open(
                    path,
                    EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE),
                    PosixFilePermissions.asFileAttribute(FILE_PERMISSIONS));
            try {
                requireSecureRegularFile(path);
                return channel;
            } catch (IOException | RuntimeException failure) {
                channel.close();
                throw failure;
            }
        }

        private static FileChannel openSecureLockFile(Path path) throws IOException {
            if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
                requireSecureRegularFile(path);
            }
            FileChannel channel = FileChannel.open(
                    path,
                    EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE),
                    PosixFilePermissions.asFileAttribute(FILE_PERMISSIONS));
            try {
                Files.setPosixFilePermissions(path, FILE_PERMISSIONS);
                requireSecureRegularFile(path);
                return channel;
            } catch (IOException | RuntimeException failure) {
                channel.close();
                throw failure;
            }
        }

        private static FileLock tryExclusiveLock(FileChannel channel, String rejectedMessage) throws IOException {
            try {
                FileLock lock = channel.tryLock();
                if (lock == null) {
                    throw new KafkaObjectCheckpointException(rejectedMessage);
                }
                return lock;
            } catch (OverlappingFileLockException failure) {
                throw new KafkaObjectCheckpointException(rejectedMessage, failure);
            }
        }

        private static void requireSecureDirectory(Path directory) throws IOException {
            if (!Files.exists(directory, LinkOption.NOFOLLOW_LINKS)) {
                try {
                    Files.createDirectory(directory, PosixFilePermissions.asFileAttribute(DIRECTORY_PERMISSIONS));
                } catch (java.nio.file.FileAlreadyExistsException raced) {
                    // The exact postcondition below rejects a symlink or non-directory race.
                }
            }
            requireSecureDirectoryEntry(directory);
            Files.setPosixFilePermissions(directory, DIRECTORY_PERMISSIONS);
            if (!Files.getPosixFilePermissions(directory, LinkOption.NOFOLLOW_LINKS)
                    .equals(DIRECTORY_PERMISSIONS)) {
                throw new KafkaObjectCheckpointException("Kafka recovery spool directory is not mode 0700");
            }
        }

        private static void requireSecureDirectoryEntry(Path directory) throws IOException {
            if (Files.isSymbolicLink(directory) || !Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
                throw new KafkaObjectCheckpointException(
                        "Kafka recovery spool directory is absent, a symlink, or not a directory");
            }
        }

        private static void requireSecureRegularFile(Path file) throws IOException {
            if (Files.isSymbolicLink(file) || !Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
                throw new KafkaObjectCheckpointException(
                        "Kafka recovery spool file is absent, a symlink, or not a regular file");
            }
            Files.setPosixFilePermissions(file, FILE_PERMISSIONS);
            if (!Files.getPosixFilePermissions(file, LinkOption.NOFOLLOW_LINKS).equals(FILE_PERMISSIONS)) {
                throw new KafkaObjectCheckpointException("Kafka recovery spool file is not mode 0600");
            }
        }

        private static boolean isLaneFile(String name) {
            return name.equals("lane-0.spool") || name.equals("lane-1.spool") || name.equals("lane-2.spool");
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            RuntimeException cleanupFailure = cleanup(initialized);
            closed = true;
            if (cleanupFailure != null) {
                throw cleanupFailure;
            }
        }

        private RuntimeException cleanup(boolean invokeHook) {
            RuntimeException cleanupFailure = null;
            for (FileChannel channel : channels) {
                if (channel == null) {
                    continue;
                }
                try {
                    channel.close();
                } catch (IOException failure) {
                    cleanupFailure = accumulate(cleanupFailure, failure);
                }
            }
            if (invokeHook) {
                try {
                    hook.beforeCleanup(List.of(paths()));
                } catch (IOException failure) {
                    cleanupFailure = accumulate(cleanupFailure, failure);
                }
            }
            for (Path path : paths) {
                if (path == null) {
                    continue;
                }
                try {
                    if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
                        requireSecureRegularFile(path);
                        Files.delete(path);
                    }
                } catch (IOException | RuntimeException failure) {
                    if (failure instanceof IOException ioFailure) {
                        cleanupFailure = accumulate(cleanupFailure, ioFailure);
                    } else if (cleanupFailure == null) {
                        cleanupFailure = (RuntimeException) failure;
                    } else {
                        cleanupFailure.addSuppressed(failure);
                    }
                }
            }
            cleanupFailure = release(cleanupFailure, attemptLock);
            cleanupFailure = close(cleanupFailure, attemptLockChannel);
            cleanupFailure = close(cleanupFailure, quotaLockChannel);
            return cleanupFailure;
        }

        private static RuntimeException release(RuntimeException current, FileLock lock) {
            if (lock == null) {
                return current;
            }
            try {
                lock.release();
                return current;
            } catch (IOException failure) {
                return accumulate(current, failure);
            }
        }

        private static RuntimeException close(RuntimeException current, FileChannel channel) {
            if (channel == null) {
                return current;
            }
            try {
                channel.close();
                return current;
            } catch (IOException failure) {
                return accumulate(current, failure);
            }
        }

        private final class CellQuotaReservation implements AutoCloseable {
            private FileLock lock;

            private CellQuotaReservation(FileLock lock) {
                this.lock = Objects.requireNonNull(lock, "lock");
            }

            @Override
            public void close() throws IOException {
                if (lock == null) {
                    throw new IllegalStateException("Kafka recovery Cell quota reservation was already released");
                }
                FileLock exact = lock;
                lock = null;
                exact.release();
            }
        }
    }

    static long requireDiskCapacity(long currentBytes, long additionalBytes, long exactCap) {
        if (currentBytes < 0 || additionalBytes <= 0 || exactCap <= 0) {
            throw new IllegalArgumentException("Kafka recovery disk-spool accounting is outside its domain");
        }
        final long next;
        try {
            next = Math.addExact(currentBytes, additionalBytes);
        } catch (ArithmeticException failure) {
            throw new KafkaObjectCheckpointException("Kafka recovery disk-spool accounting overflow", failure);
        }
        if (next > exactCap) {
            throw new KafkaObjectCheckpointException("Kafka recovery disk spool exceeds the exact Root-derived cap");
        }
        return next;
    }

    static long recoveryDiskCap(WalRunRootRecord root) {
        // Each semantic batch wire is at most 56 bytes, strictly below Kafka magic-v2's 61-byte minimum. Thus all
        // batch bytes are bounded by the Root's aggregate canonical NWG1 body bytes. An admitted extent can expose
        // no more than the Root's exact append-unit cap; every such commit adds one length prefix, fixed 102-byte
        // locator/commit header, and 32-byte local spool digest. No caller recovery-envelope value can expand this.
        long maxCommitSets = Math.multiplyExact(
                root.bounds().maxExtentCount(), (long) root.nwg1AdmissionCaps().maxAppendUnits());
        return Math.addExact(
                root.bounds().maxCanonicalBodyBytes(),
                Math.multiplyExact(maxCommitSets, RECOVERED_COMMIT_RECORD_OVERHEAD_BYTES));
    }

    static long recoveryCellDiskCap(WalRunRootRecord root, long rootDiskCap) {
        // The common lineage envelope admits exactly maxLiveRoots simultaneously. A Cell-scoped replay directory
        // therefore cannot retain more than that many exact Root spools.
        return Math.multiplyExact(rootDiskCap, root.recoveryEnvelope().maxLiveRoots());
    }

    private byte[] attemptWitness(WalRunRootRecord root) {
        String exact = "rootSha256=" + WalRunControlCodec.rootSha256(root).toHex()
                + "\nproviderScope=" + root.providerScopeId().digest().toHex()
                + "\nbinding=" + expectedRunBinding.bindingId().digest().toHex()
                + "\nbindingGeneration=" + currentFence.bindingGeneration()
                + "\nstorageEpoch="
                + expectedRunBinding.storageEpochId().digest().toHex()
                + "\nrunCreatorOwnerEpoch=" + expectedRunBinding.creatorOwnerEpoch()
                + "\nrunCreatorLeaderEpoch=" + expectedRunBinding.kafkaLeaderEpoch()
                + "\ncurrentOwnerEpoch=" + currentFence.ownerEpoch()
                + "\ncurrentLeaderEpoch=" + currentFence.kafkaLeaderEpoch()
                + '\n';
        return exact.getBytes(StandardCharsets.US_ASCII);
    }

    private static int selectNextLane(LaneHead[] heads) {
        int selected = -1;
        for (int lane = 0; lane < heads.length; lane++) {
            if (heads[lane] != null && (selected < 0 || heads[lane].startOffset() < heads[selected].startOffset())) {
                selected = lane;
            }
        }
        return selected;
    }

    private static void writeFully(FileChannel channel, ByteBuffer source) throws IOException {
        while (source.hasRemaining()) {
            if (channel.write(source) == 0) {
                throw new IOException("zero-progress write in Kafka recovery lane spool");
            }
        }
    }

    private static void readFully(FileChannel channel, long position, ByteBuffer target) throws IOException {
        long next = position;
        while (target.hasRemaining()) {
            int read = channel.read(target, next);
            if (read < 0) {
                throw new IOException("unexpected EOF in Kafka recovery lane spool");
            }
            if (read == 0) {
                throw new IOException("zero-progress read in Kafka recovery lane spool");
            }
            next = Math.addExact(next, read);
        }
    }

    private static byte[] sha256(byte[] exact) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(exact);
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("JDK has no SHA-256 provider", failure);
        }
    }

    private static RuntimeException accumulate(RuntimeException current, IOException failure) {
        KafkaObjectCheckpointException wrapped =
                new KafkaObjectCheckpointException("cannot clean Kafka recovery lane spool", failure);
        if (current == null) {
            return wrapped;
        }
        current.addSuppressed(wrapped);
        return current;
    }

    private record LaneHead(int laneId, long position, int entryBytes, long startOffset) {}

    private record LeasedRecoveredCommit(RecoveredCommit value, WalRunObjectSession.RecoveryWorkingSetLease lease)
            implements AutoCloseable {
        private LeasedRecoveredCommit {
            Objects.requireNonNull(value, "value");
            Objects.requireNonNull(lease, "lease");
        }

        @Override
        public void close() {
            lease.close();
        }
    }

    private record RecoveredCommitMaterial(
            KafkaSpeculativeCommitV1 commit, KafkaObjectExtentLocatorV1 locator, byte[] appendCommitSetId) {
        private RecoveredCommitMaterial {
            Objects.requireNonNull(commit, "commit");
            Objects.requireNonNull(locator, "locator");
            appendCommitSetId = Objects.requireNonNull(appendCommitSetId, "appendCommitSetId")
                    .clone();
            if (appendCommitSetId.length != APPEND_COMMIT_SET_ID_BYTES) {
                throw new IllegalArgumentException("Kafka recovered commit spool material is incomplete");
            }
        }

        @Override
        public byte[] appendCommitSetId() {
            return appendCommitSetId.clone();
        }
    }

    private record RecoveredCommit(KafkaSpeculativeCommitV1 commit, KafkaObjectExtentLocatorV1 locator) {}
}
