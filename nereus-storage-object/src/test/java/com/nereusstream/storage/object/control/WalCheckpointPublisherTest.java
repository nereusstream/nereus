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
