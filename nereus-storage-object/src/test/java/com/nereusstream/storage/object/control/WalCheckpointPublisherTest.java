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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class WalCheckpointPublisherTest {
    @Test
    void oneRunWidePageAdvancesIndependentLaneComponents() {
        TestControlMetadataStore store = new TestControlMetadataStore();
        WalRunRootRecord root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        WalCheckpointPublisher publisher = publisher(store, root);
        publisher.initializeHead();
        publisher.enqueue(descriptor(root, WalLaneId.OBJECT_COST, 0, 1, 0));
        publisher.enqueue(descriptor(root, WalLaneId.OBJECT_LATENCY, 0, 2, 0));

        WalRunCheckpointPageV1 page = publisher.publishNext().orElseThrow();

        assertThat(page.extents())
                .extracting(ProviderResolvedExtentRowV1::laneId)
                .containsExactly(WalLaneId.OBJECT_LATENCY, WalLaneId.OBJECT_COST);
        assertThat(publisher.head().coveredThrough()).isEqualTo(LaneSequenceVector.of(0, -1, 0));
        assertThat(publisher.queueDepth()).isZero();
        publisher.requireFinalCoverage(LaneSequenceVector.of(0, -1, 0));
    }

    @Test
    void takeoverPreservesCommittedHeadAndStaleEpochCannotRegress() {
        TestControlMetadataStore store = new TestControlMetadataStore();
        WalRunRootRecord root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        WalCheckpointPublisher publisher = publisher(store, root);
        publisher.initializeHead();
        publisher.enqueue(descriptor(root, WalLaneId.OBJECT_BALANCED, 0, 3, 0));
        publisher.publishNext();
        WalCheckpointHeadV1 committed = publisher.head();

        publisher.takeover(11);

        assertThat(publisher.head().publisherEpoch()).isEqualTo(11);
        assertThat(publisher.head().pageSha256()).isEqualTo(committed.pageSha256());
        assertThat(publisher.head().coveredThrough()).isEqualTo(committed.coveredThrough());
        assertThatThrownBy(() -> publisher.takeover(10)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void queueCapsAndLowTrafficAgeAreFailClosed() {
        TestControlMetadataStore store = new TestControlMetadataStore();
        WalRunRootRecord root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        WalCheckpointPublisher publisher = publisher(store, root);
        publisher.enqueue(descriptor(root, WalLaneId.OBJECT_LATENCY, 0, 4, 100));

        assertThat(publisher.requiresAgeForcing(5100)).isTrue();
        assertThatThrownBy(() -> publisher.enqueue(new ProviderResolvedExtentDescriptor(
                        ObjectWalControlTestFixtures.digest(99),
                        descriptor(root, WalLaneId.OBJECT_COST, 0, 5, 100).row(),
                        100)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("different WalRun Root");
    }

    @Test
    void sameOrdinalForkCannotBeAdoptedAfterHeadCasConflict() {
        TestControlMetadataStore store = new TestControlMetadataStore();
        WalRunRootRecord root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        WalCheckpointPublisher publisher = publisher(store, root);
        publisher.initializeHead();
        publisher.enqueue(descriptor(root, WalLaneId.OBJECT_LATENCY, 0, 6, 0));
        publisher.publishNext();
        publisher.enqueue(descriptor(root, WalLaneId.OBJECT_LATENCY, 1, 7, 0));

        WalRunCheckpointPageV1 forkPage = new WalRunCheckpointPageV1(
                WalRunControlCodec.rootSha256(root),
                0,
                Optional.empty(),
                java.util.List.of(
                        descriptor(root, WalLaneId.OBJECT_LATENCY, 0, 99, 0).row()),
                LaneSequenceVector.of(0, -1, -1));
        com.nereusstream.domain.bytes.CanonicalBytes forkBytes = WalRunControlCodec.encodeCheckpointPage(forkPage);
        com.nereusstream.domain.bytes.Sha256Digest forkSha = com.nereusstream.domain.bytes.Sha256Digest.hash(forkBytes);
        String forkKey = WalRunControlKeys.checkpointPageKey(7, 1, 0, forkSha);
        store.putExact(forkKey, forkBytes);
        store.putExact(
                WalRunControlKeys.checkpointHeadKey(7, 1),
                WalRunControlCodec.encodeCheckpointHead(new WalCheckpointHeadV1(
                        WalRunControlCodec.rootSha256(root),
                        1,
                        10,
                        0,
                        Optional.of(forkKey),
                        Optional.of(forkSha),
                        LaneSequenceVector.of(0, -1, -1))));

        assertThatThrownBy(publisher::publishNext)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("same-ordinal");
    }

    @Test
    void prePositionPhysicalBodyChargeIncludesSharedMembersUntilExactCoverage() {
        var store = new TestControlMetadataStore();
        var root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        var publisher = publisher(store, root);
        publisher.initializeHead();
        var reservation =
                publisher.reserveBeforePosition(root.checkpointPolicy().maxUncheckpointedBytes());
        var first = reservation.attach();
        var second = reservation.attach();
        assertThat(publisher.uncoveredExtentCount()).isOne();
        assertThat(publisher.uncoveredBodyBytes())
                .isEqualTo(root.checkpointPolicy().maxUncheckpointedBytes());
        assertThatThrownBy(() -> publisher.reserveBeforePosition(512)).hasMessageContaining("capacity");
        assertThat(publisher.queueDepth()).isZero();
        assertThatThrownBy(() -> publisher.requiresAgeForcing(Long.MIN_VALUE)).hasMessageContaining("clock regressed");
        publisher.bindPlan(List.of(first, second), ObjectWalControlTestFixtures.digest(10), 512);
        publisher.sequenceStarted(reservation);
        assertThatThrownBy(
                        () -> publisher.bindPlan(List.of(first, second), ObjectWalControlTestFixtures.digest(10), 256))
                .hasMessageContaining("physical sequence effect");
        assertThat(publisher.uncoveredBodyBytes()).isEqualTo(512);
        publisher.enqueue(reservation, descriptor(root, WalLaneId.OBJECT_LATENCY, 0, 10, 0));
        first.close();
        first.close();
        second.close();
        assertThat(publisher.uncoveredExtentCount()).isOne();
        assertThat(publisher.uncoveredBodyBytes()).isEqualTo(512);
        publisher.flush();
        assertThat(publisher.uncoveredExtentCount()).isZero();
        assertThat(publisher.uncoveredBodyBytes()).isZero();
        publisher.close();
        publisher.flush(); // A repeated terminal flush checks existing coverage without reopening work.
        assertThatThrownBy(() -> publisher.reserveBeforePosition(512)).hasMessageContaining("closed");
    }

    @Test
    void uncertainPageWithFailedReadRetainsOriginalRowsAndBytesAcrossEnqueue() {
        var delegate = new TestControlMetadataStore();
        var root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        var pages = new ArrayList<CanonicalBytes>();
        CanonicalControlMetadataStore store = new CanonicalControlMetadataStore() {
            private boolean losePageReply = true;
            private String failedReadKey;

            @Override
            public Optional<CanonicalBytes> get(String key) {
                if (key.equals(failedReadKey)) {
                    failedReadKey = null;
                    throw new IllegalStateException("checkpoint page read unavailable");
                }
                return delegate.get(key);
            }

            @Override
            public ControlMutationOutcome putIfAbsent(String key, CanonicalBytes value) {
                var outcome = delegate.putIfAbsent(key, value);
                if (key.contains("/checkpoint/pages/")) {
                    pages.add(value);
                    if (losePageReply) {
                        losePageReply = false;
                        failedReadKey = key;
                        return ControlMutationOutcome.RESPONSE_UNKNOWN;
                    }
                }
                return outcome;
            }

            @Override
            public ControlMutationOutcome compareAndSet(
                    String key, Optional<CanonicalBytes> expected, CanonicalBytes candidate) {
                return delegate.compareAndSet(key, expected, candidate);
            }
        };
        var publisher = new WalCheckpointPublisher(
                store,
                WalRunControlKeys.checkpointHeadKey(7, 1),
                WalRunControlKeys.checkpointPagePrefix(7, 1),
                root,
                WalCheckpointHeadV1.empty(WalRunControlCodec.rootSha256(root), 1, 10));
        publisher.initializeHead();
        publisher.enqueue(descriptor(root, WalLaneId.OBJECT_LATENCY, 0, 10, 0));
        assertThatThrownBy(publisher::publishNext).hasMessageContaining("page read unavailable");
        var original = pages.get(0);
        publisher.enqueue(descriptor(root, WalLaneId.OBJECT_LATENCY, 1, 11, 0));
        var page = publisher.publishNext().orElseThrow();
        assertThat(WalRunControlCodec.encodeCheckpointPage(page)).isEqualTo(original);
        assertThat(pages).containsExactly(original);
        assertThat(publisher.head().coveredThrough()).isEqualTo(LaneSequenceVector.of(0, -1, -1));
        assertThat(publisher.uncoveredExtentCount()).isOne();
        publisher.flush();
        assertThat(pages).hasSize(2);
        assertThat(publisher.uncoveredExtentCount()).isZero();
        publisher.requireFinalCoverage(LaneSequenceVector.of(1, -1, -1));
    }

    private static WalCheckpointPublisher publisher(TestControlMetadataStore store, WalRunRootRecord root) {
        return new WalCheckpointPublisher(
                store,
                WalRunControlKeys.checkpointHeadKey(root.shardId(), root.shardRunEpoch()),
                WalRunControlKeys.checkpointPagePrefix(root.shardId(), root.shardRunEpoch()),
                root,
                WalCheckpointHeadV1.empty(WalRunControlCodec.rootSha256(root), root.shardRunEpoch(), 10));
    }

    private static ProviderResolvedExtentDescriptor descriptor(
            WalRunRootRecord root, WalLaneId lane, long sequence, int seed, long timestamp) {
        return new ProviderResolvedExtentDescriptor(
                WalRunControlCodec.rootSha256(root),
                new ProviderResolvedExtentRowV1(
                        lane,
                        sequence,
                        256,
                        512,
                        ObjectWalControlTestFixtures.digest(seed),
                        ProviderVersionProof.none()),
                timestamp);
    }
}
