/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.recovery;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.AppendRecoveryAnchor;
import com.nereusstream.metadata.oxia.AppendRecoveryCommit;
import com.nereusstream.metadata.oxia.AppendRecoveryCommitEncoding;
import com.nereusstream.metadata.oxia.AppendRecoveryHead;
import com.nereusstream.metadata.oxia.AppendRecoveryTailCursor;
import com.nereusstream.metadata.oxia.AppendRecoveryTailPage;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.records.ReadTargetRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class AnchorAwareCommitWalkerTest {
    private static final StreamId STREAM = new StreamId("anchor-aware-stream");

    @Test
    void walksPagedLiveTailAndDoubleReadsStableRoot() {
        AppendRecoveryCommit first = commit("commit-1", "", 0, 1, 10, 1);
        AppendRecoveryCommit second = commit("commit-2", "commit-1", 1, 2, 20, 2);
        AppendRecoveryAnchor anchor = AppendRecoveryAnchor.genesis(STREAM);
        AppendRecoveryHead head = new AppendRecoveryHead(STREAM, "commit-2", 2, 20, 2, 9);
        AppendRecoveryTailCursor cursor = new AppendRecoveryTailCursor(
                STREAM, anchor, head, "commit-1", 1, 10, 1);
        List<AppendRecoveryTailPage> pages = List.of(
                new AppendRecoveryTailPage(
                        anchor, head, List.of(second), false, Optional.of(cursor)),
                new AppendRecoveryTailPage(
                        anchor, head, List.of(first), true, Optional.empty()));
        AtomicInteger pageReads = new AtomicInteger();
        AtomicInteger rootReads = new AtomicInteger();

        AnchorAwareCommitWalker walker = new AnchorAwareCommitWalker(
                "cluster",
                l0(pages, pageReads),
                generation(rootReads));

        AnchorAwareCommitWalk result = walker.walk(STREAM, 10, 1).join();

        assertThat(result.anchorReached()).isTrue();
        assertThat(result.continuation()).isEmpty();
        assertThat(result.commitsNewestFirst())
                .extracting(value -> value.canonicalCommit().commitId())
                .containsExactly("commit-2", "commit-1");
        assertThat(pageReads).hasValue(2);
        assertThat(rootReads).hasValue(2);
    }

    @Test
    void preservesContinuationWhenOverallBoundIsReached() {
        AppendRecoveryCommit second = commit("commit-2", "commit-1", 1, 2, 20, 2);
        AppendRecoveryAnchor anchor = AppendRecoveryAnchor.genesis(STREAM);
        AppendRecoveryHead head = new AppendRecoveryHead(STREAM, "commit-2", 2, 20, 2, 9);
        AppendRecoveryTailCursor cursor = new AppendRecoveryTailCursor(
                STREAM, anchor, head, "commit-1", 1, 10, 1);
        AtomicInteger pageReads = new AtomicInteger();

        AnchorAwareCommitWalk result = new AnchorAwareCommitWalker(
                "cluster",
                l0(List.of(new AppendRecoveryTailPage(
                        anchor, head, List.of(second), false, Optional.of(cursor))), pageReads),
                generation(new AtomicInteger()))
                .walk(STREAM, 1, 1)
                .join();

        assertThat(result.anchorReached()).isFalse();
        assertThat(result.continuation()).contains(cursor);
        assertThat(result.commitsNewestFirst()).containsExactly(second);
        assertThat(pageReads).hasValue(1);
    }

    private static OxiaMetadataStore l0(
            List<AppendRecoveryTailPage> pages,
            AtomicInteger reads) {
        List<AppendRecoveryTailPage> mutable = new ArrayList<>(pages);
        return (OxiaMetadataStore) Proxy.newProxyInstance(
                OxiaMetadataStore.class.getClassLoader(),
                new Class<?>[] {OxiaMetadataStore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "readAppendRecoveryTail" -> {
                        reads.incrementAndGet();
                        yield java.util.concurrent.CompletableFuture.completedFuture(mutable.remove(0));
                    }
                    case "close" -> null;
                    case "toString" -> "anchor-aware-l0";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static GenerationMetadataStore generation(AtomicInteger reads) {
        return (GenerationMetadataStore) Proxy.newProxyInstance(
                GenerationMetadataStore.class.getClassLoader(),
                new Class<?>[] {GenerationMetadataStore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getRecoveryRoot" -> {
                        reads.incrementAndGet();
                        yield java.util.concurrent.CompletableFuture.completedFuture(Optional.empty());
                    }
                    case "close" -> null;
                    case "toString" -> "anchor-aware-generation";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static AppendRecoveryCommit commit(
            String commitId,
            String previousCommitId,
            long start,
            long end,
            long cumulativeSize,
            long commitVersion) {
        StreamCommitTargetRecord value = new StreamCommitTargetRecord(
                STREAM.value(),
                commitId,
                previousCommitId,
                start,
                end,
                0,
                cumulativeSize,
                commitVersion,
                "writer",
                "writer-run",
                1,
                "fencing-hash",
                new ReadTargetRecord(
                        "OBJECT_SLICE",
                        1,
                        "canonical-target-v1",
                        new byte[] {1},
                        ChecksumType.SHA256.name(),
                        "a".repeat(64)),
                "OPAQUE_RECORD_BATCH",
                1,
                1,
                10,
                List.of(),
                "projection",
                1,
                1,
                1,
                0);
        byte[] canonical = MetadataRecordCodecFactory.encodeEnvelope(
                value, StreamCommitTargetRecord.class);
        Checksum digest = sha256(canonical);
        return new AppendRecoveryCommit(
                "/commit/" + commitId,
                AppendRecoveryCommitEncoding.GENERIC_STREAM_COMMIT_TARGET_V1,
                value,
                commitVersion,
                digest,
                ByteBuffer.wrap(canonical),
                digest);
    }

    private static Checksum sha256(byte[] bytes) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)));
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }
}
