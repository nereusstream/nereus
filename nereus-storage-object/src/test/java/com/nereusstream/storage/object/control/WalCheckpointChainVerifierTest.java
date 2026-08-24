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

package com.nereusstream.storage.object.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.recovery.CumulativeRecoveryBudget;
import com.nereusstream.storage.object.recovery.RecoveryEnvelopeLimits;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class WalCheckpointChainVerifierTest {
    @Test
    void completeChainFoldsExactContiguousRowsAndAggregates() {
        TestControlMetadataStore store = new TestControlMetadataStore();
        WalRunRootRecord root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        WalRunCheckpointPageV1 page0 = page(root, 0, Optional.empty(), 0, 1);
        CanonicalBytes bytes0 = WalRunControlCodec.encodeCheckpointPage(page0);
        Sha256Digest sha0 = Sha256Digest.hash(bytes0);
        String key0 = WalRunControlKeys.checkpointPageKey(7, 1, 0, sha0);
        store.putExact(key0, bytes0);
        WalRunCheckpointPageV1 page1 = page(root, 1, Optional.of(sha0), 1, 2);
        CanonicalBytes bytes1 = WalRunControlCodec.encodeCheckpointPage(page1);
        Sha256Digest sha1 = Sha256Digest.hash(bytes1);
        String key1 = WalRunControlKeys.checkpointPageKey(7, 1, 1, sha1);
        store.putExact(key1, bytes1);
        WalCheckpointHeadV1 head = new WalCheckpointHeadV1(
                WalRunControlCodec.rootSha256(root),
                1,
                1,
                1,
                Optional.of(key1),
                Optional.of(sha1),
                LaneSequenceVector.of(1, -1, -1));

        WalCheckpointChainVerifier.Verification verified = new WalCheckpointChainVerifier(store, root).verify(head);

        assertThat(verified.pages()).hasSize(2);
        assertThat(verified.aggregateExtentCount()).isEqualTo(2);
        assertThat(verified.aggregateCanonicalBodyBytes()).isEqualTo(1024);
    }

    @Test
    void missingDigestMismatchAndLaneGapFailClosed() {
        TestControlMetadataStore store = new TestControlMetadataStore();
        WalRunRootRecord root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        WalRunCheckpointPageV1 canonical = page(root, 0, Optional.empty(), 0, 3);
        CanonicalBytes bytes = WalRunControlCodec.encodeCheckpointPage(canonical);
        Sha256Digest sha = Sha256Digest.hash(bytes);
        String key = WalRunControlKeys.checkpointPageKey(7, 1, 0, sha);
        WalCheckpointHeadV1 head = head(root, key, sha, 0);

        assertThatThrownBy(() -> new WalCheckpointChainVerifier(store, root).verify(head))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("absent");

        store.putExact(key, WalRunControlCodec.encodeCheckpointPage(page(root, 0, Optional.empty(), 0, 4)));
        assertThatThrownBy(() -> new WalCheckpointChainVerifier(store, root).verify(head))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SHA-256");

        WalRunCheckpointPageV1 gap = page(root, 0, Optional.empty(), 1, 5);
        CanonicalBytes gapBytes = WalRunControlCodec.encodeCheckpointPage(gap);
        Sha256Digest gapSha = Sha256Digest.hash(gapBytes);
        String gapKey = WalRunControlKeys.checkpointPageKey(7, 1, 0, gapSha);
        store.putExact(gapKey, gapBytes);
        assertThatThrownBy(() -> new WalCheckpointChainVerifier(store, root).verify(head(root, gapKey, gapSha, 1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("lane gap");
    }

    @Test
    void streamingRecoveryWalksBackFromHeadAndChargesTheRootOwnedBudget() {
        TestControlMetadataStore store = new TestControlMetadataStore();
        WalRunRootRecord root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        WalRunCheckpointPageV1 page0 = page(root, 0, Optional.empty(), 0, 6);
        CanonicalBytes bytes0 = WalRunControlCodec.encodeCheckpointPage(page0);
        Sha256Digest sha0 = Sha256Digest.hash(bytes0);
        String key0 = WalRunControlKeys.checkpointPageKey(7, 1, 0, sha0);
        store.putExact(key0, bytes0);
        WalRunCheckpointPageV1 page1 = page(root, 1, Optional.of(sha0), 1, 7);
        CanonicalBytes bytes1 = WalRunControlCodec.encodeCheckpointPage(page1);
        Sha256Digest sha1 = Sha256Digest.hash(bytes1);
        String key1 = WalRunControlKeys.checkpointPageKey(7, 1, 1, sha1);
        store.putExact(key1, bytes1);
        WalCheckpointHeadV1 head = new WalCheckpointHeadV1(
                WalRunControlCodec.rootSha256(root),
                1,
                1,
                1,
                Optional.of(key1),
                Optional.of(sha1),
                LaneSequenceVector.of(1, -1, -1));
        CumulativeRecoveryBudget budget = new CumulativeRecoveryBudget(root.recoveryEnvelope(), () -> 0);

        ArrayList<ProviderResolvedExtentRowV1> stagedRows = new ArrayList<>();
        WalCheckpointChainVerifier.StreamingVerification verified =
                new WalCheckpointChainVerifier(store, root).verifyStreaming(head, budget, stagedRows::add);

        assertThat(verified.pageCount()).isEqualTo(2);
        assertThat(verified.aggregateExtentCount()).isEqualTo(2);
        assertThat(verified.coveredThrough()).isEqualTo(head.coveredThrough());
        assertThat(stagedRows)
                .extracting(ProviderResolvedExtentRowV1::laneSequence)
                .containsExactly(1L, 0L);
        assertThat(budget.snapshot().canonicalBodyBytes())
                .isEqualTo(2L * root.checkpointPolicy().maxCanonicalPageBytes());
    }

    @Test
    void streamingUnderboundFailsBeforeAnyPageMetadataRead() {
        TestControlMetadataStore store = new TestControlMetadataStore();
        WalRunRootRecord root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        WalRunCheckpointPageV1 page = page(root, 0, Optional.empty(), 0, 8);
        CanonicalBytes pageBytes = WalRunControlCodec.encodeCheckpointPage(page);
        Sha256Digest pageSha = Sha256Digest.hash(pageBytes);
        String pageKey = WalRunControlKeys.checkpointPageKey(7, 1, 0, pageSha);
        store.putExact(pageKey, pageBytes);
        WalCheckpointHeadV1 head = head(root, pageKey, pageSha, 0);
        RecoveryEnvelopeLimits limits = root.recoveryEnvelope();
        CumulativeRecoveryBudget underbound = new CumulativeRecoveryBudget(
                new RecoveryEnvelopeLimits(
                        limits.maxLiveRoots(),
                        limits.maxPredecessorRuns(),
                        limits.maxListPages(),
                        limits.maxListedKeys(),
                        limits.maxListedKeyBytes(),
                        limits.maxHeadRequests(),
                        limits.maxRangeGetRequests(),
                        limits.maxFullGetRequests(),
                        1,
                        limits.maxDecodedContexts(),
                        limits.maxDecodedFrames(),
                        limits.maxDecodedCommitSets(),
                        limits.maxWorkingMemoryBytes(),
                        limits.maxConcurrency(),
                        limits.maxRetryAttempts(),
                        limits.maxWallTimeNanos()),
                () -> 0);

        assertThatThrownBy(() -> new WalCheckpointChainVerifier(store, root).verifyStreaming(head, underbound))
                .isInstanceOf(com.nereusstream.storage.object.recovery.RecoveryEnvelopeExceededException.class);
        assertThat(store.operations()).isEmpty();
    }

    private static WalRunCheckpointPageV1 page(
            WalRunRootRecord root, long ordinal, Optional<Sha256Digest> predecessor, long sequence, int seed) {
        return new WalRunCheckpointPageV1(
                WalRunControlCodec.rootSha256(root),
                ordinal,
                predecessor,
                List.of(new ProviderResolvedExtentRowV1(
                        WalLaneId.OBJECT_LATENCY,
                        sequence,
                        256,
                        512,
                        ObjectWalControlTestFixtures.digest(seed),
                        ProviderVersionProof.none())),
                LaneSequenceVector.of(sequence, -1, -1));
    }

    private static WalCheckpointHeadV1 head(WalRunRootRecord root, String key, Sha256Digest sha, long coveredThrough) {
        return new WalCheckpointHeadV1(
                WalRunControlCodec.rootSha256(root),
                root.shardRunEpoch(),
                1,
                0,
                Optional.of(key),
                Optional.of(sha),
                LaneSequenceVector.of(coveredThrough, -1, -1));
    }
}
