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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class KafkaCompactionSourceResolverTest {
  @Test
  void derivesTheExactOutputTaskFromTheAuthoritativeDecisionPrefix() {
    KafkaCompactionPlanCodecV1Test.Fixture fixture =
        KafkaCompactionPlanCodecV1Test.fixture("UNCOMPRESSED");
    FakeCommittedSources committed =
        new FakeCommittedSources(resolution(fixture.plan().decisionSources()));
    KafkaCompactionSourceResolver resolver = new KafkaCompactionSourceResolver(committed);

    KafkaCompactionSourceResolver.ResolvedSources resolved =
        resolver
            .resolve(
                fixture.outputTask().streamId(),
                fixture.plan().candidate(),
                fixture.outputTask().policy())
            .join();

    assertThat(committed.requestedCoverage())
        .isEqualTo(fixture.plan().candidate().decisionHorizon());
    assertThat(resolved.decisionSources()).isEqualTo(fixture.plan().decisionSources());
    assertThat(resolved.outputSources()).isEqualTo(fixture.plan().outputSources());
    assertThat(resolved.outputTask()).isEqualTo(fixture.outputTask());
  }

  @Test
  void rejectsAnOutputBoundaryThatCutsThroughOneAuthoritativeSource() {
    KafkaCompactionPlanCodecV1Test.Fixture fixture =
        KafkaCompactionPlanCodecV1Test.fixture("UNCOMPRESSED");
    KafkaCompactionPlanner.Candidate cut =
        new KafkaCompactionPlanner.Candidate(
            new OffsetRange(0, 1),
            fixture.plan().candidate().decisionHorizon(),
            1,
            fixture.plan().candidate().policy(),
            fixture.plan().candidate().previousMandatoryCoverage(),
            fixture.plan().candidate().evaluatedAtMillis());
    KafkaCompactionSourceResolver resolver =
        new KafkaCompactionSourceResolver(
            new FakeCommittedSources(resolution(fixture.plan().decisionSources())));

    assertThatThrownBy(
            () ->
                resolver
                    .resolve(fixture.outputTask().streamId(), cut, fixture.outputTask().policy())
                    .join())
        .hasRootCauseMessage(
            "Kafka compaction output boundary does not match an exact source prefix");
  }

  @Test
  void revalidatesSourcesBeforeTheBindingAuthorityGuard() {
    KafkaCompactionPlanCodecV1Test.Fixture fixture =
        KafkaCompactionPlanCodecV1Test.fixture("UNCOMPRESSED");
    FakeCommittedSources committed =
        new FakeCommittedSources(resolution(fixture.plan().decisionSources()));
    KafkaCompactionSourceResolver resolver = new KafkaCompactionSourceResolver(committed);
    KafkaCompactionSourceResolver.ResolvedSources resolved =
        resolver
            .resolve(
                fixture.outputTask().streamId(),
                fixture.plan().candidate(),
                fixture.outputTask().policy())
            .join();
    AtomicInteger authorityChecks = new AtomicInteger();

    resolver
        .mutationGuard(
            resolved,
            () -> {
              assertThat(committed.revalidationCount()).isEqualTo(1);
              authorityChecks.incrementAndGet();
              return CompletableFuture.completedFuture(null);
            })
        .revalidate()
        .join();

    assertThat(authorityChecks).hasValue(1);
  }

  private static CommittedSourceSetResolution resolution(ExactSourceSet sourceSet) {
    var streamId = KafkaCompactionPlanCodecV1Test.fixture("UNCOMPRESSED").outputTask().streamId();
    long metadataVersion = 1;
    StreamMetadataSnapshot snapshot =
        new StreamMetadataSnapshot(
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
                sourceSet.coverage().endOffset(),
                sourceSet.sources().get(sourceSet.sources().size() - 1).cumulativeSizeAtEnd(),
                sourceSet.sources().get(sourceSet.sources().size() - 1).commitVersion(),
                metadataVersion),
            new TrimRecord(
                streamId.value(), sourceSet.coverage().startOffset(), "", 1, metadataVersion));
    MaterializationStreamRegistrationRecord registration =
        new MaterializationStreamRegistrationRecord(
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
        new VersionedMaterializationStreamRegistration(
            "/registration/" + streamId.value(),
            registration,
            metadataVersion,
            new Checksum(ChecksumType.SHA256, "b".repeat(64))));
  }

  private static final class FakeCommittedSources implements CommittedSourceSetResolver {
    private final CommittedSourceSetResolution resolution;
    private OffsetRange requestedCoverage;
    private int revalidationCount;

    private FakeCommittedSources(CommittedSourceSetResolution resolution) {
      this.resolution = resolution;
    }

    @Override
    public CompletableFuture<CommittedSourceSetResolution> resolve(
        com.nereusstream.api.StreamId streamId, OffsetRange coverage) {
      requestedCoverage = coverage;
      return CompletableFuture.completedFuture(resolution);
    }

    @Override
    public CompletableFuture<Void> revalidate(CommittedSourceSetResolution expected) {
      assertThat(expected).isEqualTo(resolution);
      revalidationCount++;
      return CompletableFuture.completedFuture(null);
    }

    private OffsetRange requestedCoverage() {
      return requestedCoverage;
    }

    private int revalidationCount() {
      return revalidationCount;
    }
  }
}
