/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.core.physical.GcReference;
import com.nereusstream.core.physical.GcReferenceDomain;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceQueryKind;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import com.nereusstream.core.physical.GcReferenceSnapshotBuilder;
import com.nereusstream.metadata.oxia.AppendRecoveryAnchor;
import com.nereusstream.metadata.oxia.AppendRecoveryCommit;
import com.nereusstream.metadata.oxia.AppendRecoveryHead;
import com.nereusstream.metadata.oxia.AppendRecoveryTailCursor;
import com.nereusstream.metadata.oxia.AppendRecoveryTailPage;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.VersionedRecoveryCheckpointRoot;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointReferenceRecord;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Exact live-tail and recovery-root reference domain for append replay authority. */
public final class AppendRecoveryReferenceDomain implements GcReferenceDomain {
    public static final String DOMAIN_ID = "append-recovery-v1";
    public static final int PROTOCOL_VERSION = 1;

    private static final ReadTargetCodecRegistry TARGET_CODECS = ReadTargetCodecRegistry.phase15();

    private final String cluster;
    private final OxiaMetadataStore l0MetadataStore;
    private final GenerationMetadataStore generationStore;
    private final PhysicalGcConfig config;
    private final OxiaKeyspace l0Keys;
    private final F4Keyspace f4Keys;

    public AppendRecoveryReferenceDomain(
            String cluster,
            OxiaMetadataStore l0MetadataStore,
            GenerationMetadataStore generationStore,
            PhysicalGcConfig config) {
        this.cluster = requireText(cluster, "cluster");
        this.l0MetadataStore = Objects.requireNonNull(l0MetadataStore, "l0MetadataStore");
        this.generationStore = Objects.requireNonNull(generationStore, "generationStore");
        this.config = Objects.requireNonNull(config, "config");
        this.l0Keys = new OxiaKeyspace(cluster);
        this.f4Keys = new F4Keyspace(cluster);
    }

    @Override
    public String domainId() {
        return DOMAIN_ID;
    }

    @Override
    public int protocolVersion() {
        return PROTOCOL_VERSION;
    }

    @Override
    public CompletableFuture<GcReferenceSnapshot> snapshot(GcReferenceQuery query) {
        Objects.requireNonNull(query, "query");
        if (query.kind() == GcReferenceQueryKind.OWNERLESS_ORPHAN_CANDIDATE) {
            return CompletableFuture.completedFuture(
                    GcReferenceSnapshotBuilder.unsupportedOwnerless(
                            DOMAIN_ID, PROTOCOL_VERSION, query));
        }
        GcReferenceSnapshotBuilder accumulator = new GcReferenceSnapshotBuilder(
                DOMAIN_ID, PROTOCOL_VERSION, query, config.referenceDomainConfig());
        return scanStream(query, accumulator, 0);
    }

    @Override
    public CompletableFuture<Boolean> stillMatches(
            GcReferenceQuery query, GcReferenceSnapshot snapshot) {
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(snapshot, "snapshot");
        if (!snapshot.domainId().equals(DOMAIN_ID)
                || snapshot.protocolVersion() != PROTOCOL_VERSION
                || !snapshot.queryIdentitySha256().equals(query.queryIdentitySha256())) {
            return CompletableFuture.completedFuture(false);
        }
        return snapshot(query).thenApply(snapshot::equals);
    }

    private CompletableFuture<GcReferenceSnapshot> scanStream(
            GcReferenceQuery query,
            GcReferenceSnapshotBuilder accumulator,
            int streamIndex) {
        if (accumulator.limitExceeded()
                || streamIndex == query.affectedStreams().size()) {
            return CompletableFuture.completedFuture(accumulator.build());
        }
        StreamId streamId = query.affectedStreams().get(streamIndex);
        return generationStore.getRecoveryRoot(cluster, streamId).thenCompose(optionalRoot -> {
            addRoot(query, accumulator, streamId, optionalRoot);
            if (accumulator.limitExceeded()) {
                return CompletableFuture.completedFuture(accumulator.build());
            }
            AppendRecoveryAnchor anchor = optionalRoot
                    .map(root -> anchor(streamId, root))
                    .orElseGet(() -> AppendRecoveryAnchor.genesis(streamId));
            return scanTail(
                    query,
                    accumulator,
                    streamIndex,
                    anchor,
                    Optional.empty(),
                    null);
        });
    }

    private CompletableFuture<GcReferenceSnapshot> scanTail(
            GcReferenceQuery query,
            GcReferenceSnapshotBuilder accumulator,
            int streamIndex,
            AppendRecoveryAnchor anchor,
            Optional<AppendRecoveryTailCursor> continuation,
            AppendRecoveryHead expectedHead) {
        StreamId streamId = query.affectedStreams().get(streamIndex);
        return l0MetadataStore.readAppendRecoveryTail(
                        cluster,
                        streamId,
                        anchor,
                        continuation,
                        config.metadataScanPageSize())
                .thenCompose(page -> {
                    AppendRecoveryHead observed = page.observedHead();
                    if (expectedHead != null && !expectedHead.equals(observed)) {
                        throw invariant("append recovery scan changed its observed head");
                    }
                    if (continuation.isEmpty()) {
                        accumulator.addAuthority(new GcAuthorityToken(
                                l0Keys.streamHeadKey(streamId),
                                observed.metadataVersion(),
                                headIdentity(observed)));
                    }
                    addCommits(query, accumulator, page);
                    if (accumulator.limitExceeded()) {
                        return CompletableFuture.completedFuture(accumulator.build());
                    }
                    if (page.continuation().isPresent()) {
                        return scanTail(
                                query,
                                accumulator,
                                streamIndex,
                                anchor,
                                page.continuation(),
                                observed);
                    }
                    return scanStream(query, accumulator, streamIndex + 1);
                });
    }

    private void addRoot(
            GcReferenceQuery query,
            GcReferenceSnapshotBuilder accumulator,
            StreamId streamId,
            Optional<VersionedRecoveryCheckpointRoot> optionalRoot) {
        if (optionalRoot.isEmpty()) {
            String key = f4Keys.recoveryRootKey(streamId);
            accumulator.addAuthority(new GcAuthorityToken(key, 0, absenceIdentity(key)));
            return;
        }
        VersionedRecoveryCheckpointRoot root = optionalRoot.orElseThrow();
        accumulator.addAuthority(new GcAuthorityToken(
                root.key(), root.metadataVersion(), root.durableValueSha256()));
        for (RecoveryCheckpointReferenceRecord checkpoint : root.value().checkpoints()) {
            if (checkpoint.objectKey().equals(query.object().objectKey().value())
                    && checkpoint.objectKeyHash().equals(query.object().objectKeyHash().value())
                    && (query.object().objectId().isEmpty()
                            || query.object().objectId().orElseThrow().value().equals(
                                    checkpoint.objectId()))) {
                accumulator.addReference(new GcReference(
                        "recovery-checkpoint-root",
                        streamId.value() + "/" + checkpoint.checkpointSequence(),
                        root.key(),
                        root.metadataVersion(),
                        root.durableValueSha256()));
                accumulator.veto();
            }
        }
    }

    private static void addCommits(
            GcReferenceQuery query,
            GcReferenceSnapshotBuilder accumulator,
            AppendRecoveryTailPage page) {
        for (AppendRecoveryCommit commit : page.commitsNewestFirst()) {
            accumulator.addAuthority(new GcAuthorityToken(
                    commit.key(),
                    commit.sourceMetadataVersion(),
                    commit.sourceRecordSha256()));
            ReadTarget target = TARGET_CODECS.decode(commit.canonicalCommit().readTarget());
            if (matches(query, target)) {
                accumulator.addReference(new GcReference(
                        "append-recovery-commit",
                        commit.canonicalCommit().commitId(),
                        commit.key(),
                        commit.sourceMetadataVersion(),
                        commit.sourceRecordSha256()));
                accumulator.veto();
            }
            if (accumulator.limitExceeded()) {
                return;
            }
        }
    }

    private static AppendRecoveryAnchor anchor(
            StreamId streamId, VersionedRecoveryCheckpointRoot root) {
        if (root.value().checkpoints().isEmpty()) {
            return AppendRecoveryAnchor.genesis(streamId);
        }
        return new AppendRecoveryAnchor(
                streamId,
                root.value().lastCommitId(),
                root.value().coveredEndOffset(),
                root.value().cumulativeSizeAtEnd(),
                root.value().lastCommitVersion());
    }

    private static boolean matches(GcReferenceQuery query, ReadTarget target) {
        if (!(target instanceof ObjectSliceReadTarget objectTarget)
                || !objectTarget.objectKey().equals(query.object().objectKey())) {
            return false;
        }
        return query.object().objectId().isEmpty()
                || query.object().objectId().orElseThrow().equals(objectTarget.objectId());
    }

    private static Checksum headIdentity(AppendRecoveryHead head) {
        DigestWriter writer = new DigestWriter("append-recovery-head-v1");
        writer.text(head.streamId().value());
        writer.text(head.lastCommitId());
        writer.int64(head.offsetEnd());
        writer.int64(head.cumulativeSize());
        writer.int64(head.commitVersion());
        writer.int64(head.metadataVersion());
        return writer.finish();
    }

    private static Checksum absenceIdentity(String key) {
        DigestWriter writer = new DigestWriter("append-recovery-root-absence-v1");
        writer.text(key);
        return writer.finish();
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must be non-blank");
        }
        return value;
    }

    private static NereusException invariant(String message) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static final class DigestWriter {
        private final MessageDigest digest;

        private DigestWriter(String domain) {
            try {
                digest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException failure) {
                throw new IllegalStateException("SHA-256 is unavailable", failure);
            }
            text(domain);
        }

        private void int64(long value) {
            digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
        }

        private void text(String value) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
            digest.update(bytes);
        }

        private Checksum finish() {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(digest.digest()));
        }
    }
}
