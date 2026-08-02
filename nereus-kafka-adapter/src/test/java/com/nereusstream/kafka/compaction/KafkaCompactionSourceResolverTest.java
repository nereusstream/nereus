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

package com.nereusstream.kafka.compaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamState;
import com.nereusstream.materialization.CommittedSourceSetResolution;
import com.nereusstream.materialization.CommittedSourceSetResolver;
import com.nereusstream.materialization.ExactSourceSet;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class KafkaCompactionSourceResolverTest {
    @Test
    void derivesTheExactOutputTaskFromTheAuthoritativeDecisionPrefix() {
        KafkaCompactionPlanCodecV1Test.Fixture fixture = KafkaCompactionPlanCodecV1Test.fixture("UNCOMPRESSED");
        FakeCommittedSources committed = committedSources(fixture);
        KafkaCompactionSourceResolver resolver = new KafkaCompactionSourceResolver(committed);

        KafkaCompactionSourceResolver.ResolvedSources resolved = resolver.resolve(
                        fixture.outputTask().streamId(),
                        fixture.plan().candidate(),
                        fixture.outputTask().policy())
                .join();

        assertThat(committed.requestedCoverages())
                .containsExactly(
                        fixture.plan().candidate().outputCoverage(),
                        new OffsetRange(
                                fixture.plan().candidate().outputCoverage().endOffset(),
                                fixture.plan().candidate().decisionHorizon().endOffset()));
        assertThat(resolved.decisionSources()).isEqualTo(fixture.plan().decisionSources());
        assertThat(resolved.outputSources()).isEqualTo(fixture.plan().outputSources());
        assertThat(resolved.outputTask()).isEqualTo(fixture.outputTask());
    }

    @Test
    void rejectsAuthorityDriftBetweenTheOutputAndTailResolutions() {
        KafkaCompactionPlanCodecV1Test.Fixture fixture = KafkaCompactionPlanCodecV1Test.fixture("UNCOMPRESSED");
        ExactSourceSet output = fixture.plan().outputSources();
        ExactSourceSet tail = tailSources(fixture);
        KafkaCompactionSourceResolver resolver = new KafkaCompactionSourceResolver(new FakeCommittedSources(Map.of(
                output.coverage(),
                resolution(output, fixture.plan().decisionSources()),
                tail.coverage(),
                resolution(tail, tail))));

        assertThatThrownBy(() -> resolver.resolve(
                                fixture.outputTask().streamId(),
                                fixture.plan().candidate(),
                                fixture.outputTask().policy())
                        .join())
                .hasRootCauseMessage(
                        "COMMITTED source authority changed between Kafka compaction output and tail" + " resolution");
    }

    @Test
    void revalidatesSourcesBeforeTheBindingAuthorityGuard() {
        KafkaCompactionPlanCodecV1Test.Fixture fixture = KafkaCompactionPlanCodecV1Test.fixture("UNCOMPRESSED");
        FakeCommittedSources committed = committedSources(fixture);
        KafkaCompactionSourceResolver resolver = new KafkaCompactionSourceResolver(committed);
        KafkaCompactionSourceResolver.ResolvedSources resolved = resolver.resolve(
                        fixture.outputTask().streamId(),
                        fixture.plan().candidate(),
                        fixture.outputTask().policy())
                .join();
        AtomicInteger authorityChecks = new AtomicInteger();

        resolver.mutationGuard(resolved, () -> {
                    assertThat(committed.revalidationCount()).isEqualTo(1);
                    authorityChecks.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                })
                .revalidate()
                .join();

        assertThat(authorityChecks).hasValue(1);
        assertThat(committed.revalidated()).isEqualTo(resolved.resolution());
    }

    private static FakeCommittedSources committedSources(KafkaCompactionPlanCodecV1Test.Fixture fixture) {
        ExactSourceSet decision = fixture.plan().decisionSources();
        ExactSourceSet output = fixture.plan().outputSources();
        ExactSourceSet tail = tailSources(fixture);
        return new FakeCommittedSources(
                Map.of(output.coverage(), resolution(output, decision), tail.coverage(), resolution(tail, decision)));
    }

    private static ExactSourceSet tailSources(KafkaCompactionPlanCodecV1Test.Fixture fixture) {
        ExactSourceSet decision = fixture.plan().decisionSources();
        int outputSourceCount = fixture.plan().outputSources().sources().size();
        return ExactSourceSet.create(
                decision.view(),
                new OffsetRange(
                        fixture.plan().candidate().outputCoverage().endOffset(),
                        fixture.plan().candidate().decisionHorizon().endOffset()),
                decision.sources().subList(outputSourceCount, decision.sources().size()));
    }

    private static CommittedSourceSetResolution resolution(ExactSourceSet sourceSet, ExactSourceSet authoritySet) {
        var streamId = KafkaCompactionPlanCodecV1Test.fixture("UNCOMPRESSED")
                .outputTask()
                .streamId();
        long metadataVersion = 1;
        var authoritativeLast =
                authoritySet.sources().get(authoritySet.sources().size() - 1);
        StreamMetadataSnapshot snapshot = new StreamMetadataSnapshot(
                new StreamMetadataRecord(
                        streamId.value(),
                        "kafka-source-resolution",
                        "stream-hash",
                        StreamState.ACTIVE.name(),
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                        Map.of(),
                        1,
                        1,
                        metadataVersion),
                new CommittedEndOffsetRecord(
                        streamId.value(),
                        authoritySet.coverage().endOffset(),
                        authoritativeLast.cumulativeSizeAtEnd(),
                        authoritativeLast.commitVersion(),
                        metadataVersion),
                new TrimRecord(streamId.value(), authoritySet.coverage().startOffset(), "", 1, metadataVersion));
        MaterializationStreamRegistrationRecord registration = new MaterializationStreamRegistrationRecord(
                1,
                streamId.value(),
                ProjectionIdentity.encode(Optional.empty()),
                "a".repeat(64),
                StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                1,
                1,
                1,
                metadataVersion);
        return new CommittedSourceSetResolution(
                streamId,
                sourceSet,
                snapshot,
                Optional.of(new VersionedMaterializationStreamRegistration(
                        "/registration/" + streamId.value(),
                        registration,
                        metadataVersion,
                        new Checksum(ChecksumType.SHA256, "b".repeat(64)))));
    }

    private static final class FakeCommittedSources implements CommittedSourceSetResolver {
        private final Map<OffsetRange, CommittedSourceSetResolution> resolutions;
        private final ArrayList<OffsetRange> requestedCoverages = new ArrayList<>();
        private int revalidationCount;
        private CommittedSourceSetResolution revalidated;

        private FakeCommittedSources(Map<OffsetRange, CommittedSourceSetResolution> resolutions) {
            this.resolutions = Map.copyOf(resolutions);
        }

        @Override
        public CompletableFuture<CommittedSourceSetResolution> resolve(
                com.nereusstream.api.StreamId streamId, OffsetRange coverage) {
            requestedCoverages.add(coverage);
            CommittedSourceSetResolution resolution = resolutions.get(coverage);
            if (resolution == null) {
                return CompletableFuture.failedFuture(
                        new IllegalArgumentException("unexpected requested coverage " + coverage));
            }
            return CompletableFuture.completedFuture(resolution);
        }

        @Override
        public CompletableFuture<Void> revalidate(CommittedSourceSetResolution expected) {
            revalidated = expected;
            revalidationCount++;
            return CompletableFuture.completedFuture(null);
        }

        private List<OffsetRange> requestedCoverages() {
            return List.copyOf(requestedCoverages);
        }

        private int revalidationCount() {
            return revalidationCount;
        }

        private CommittedSourceSetResolution revalidated() {
            return revalidated;
        }
    }
}
