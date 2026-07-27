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
import com.nereusstream.kafka.compaction.KafkaCompactionPlanCodecV1Test.Fixture;
import com.nereusstream.metadata.oxia.KafkaCompactionPlanMetadataStore;
import com.nereusstream.metadata.oxia.KafkaCompactionPlanScanPage;
import com.nereusstream.metadata.oxia.KafkaCompactionPlanScanToken;
import com.nereusstream.metadata.oxia.KafkaPartitionId;
import com.nereusstream.metadata.oxia.VersionedKafkaCompactionPlan;
import com.nereusstream.metadata.oxia.records.KafkaCompactionPlanRecord;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class KafkaCompactionPlanCoordinatorTest {
  @Test
  void publishesThePlanBeforeTaskAdmissionAndRecoversByMaterializationTaskId() {
    Fixture fixture = KafkaCompactionPlanCodecV1Test.fixture("UNCOMPRESSED");
    ArrayList<String> events = new ArrayList<>();
    FakePlanStore plans = new FakePlanStore(events);
    KafkaCompactionPlanCoordinator.TaskRoots tasks =
        (task, guard) -> {
          events.add("task-guard");
          return guard
              .revalidate()
              .thenApply(
                  ignored -> {
                    events.add("task-admit");
                    return task;
                  });
        };
    KafkaCompactionPlanCoordinator coordinator =
        new KafkaCompactionPlanCoordinator(
            plans,
            tasks,
            new KafkaCompactionPlanRecordMapper(),
            Clock.fixed(Instant.ofEpochMilli(2_000), ZoneOffset.UTC));
    KafkaPartitionId partition = new KafkaPartitionId("kraft", topicId(7), 1);

    KafkaCompactionPlanCoordinator.Converged converged =
        coordinator
            .converge(
                partition,
                fixture.outputTask(),
                fixture.plan().bindingMetadataVersion(),
                fixture.plan().lastStableOffset(),
                fixture.plan().highWatermark(),
                fixture.plan().candidate(),
                fixture.plan().decisionSources(),
                fixture.plan().passOneSnapshot(),
                () -> {
                  events.add("authority");
                  return CompletableFuture.completedFuture(null);
                })
            .join();

    assertThat(converged.plan()).isEqualTo(fixture.plan());
    assertThat(events)
        .containsExactly(
            "authority", "plan-put", "task-guard", "authority", "plan-get", "task-admit");
    assertThat(coordinator.recover(partition, fixture.outputTask()).join().plan())
        .isEqualTo(fixture.plan());
    assertThat(plans.lastLookupTaskId()).isEqualTo(fixture.outputTask().taskId());
  }

  @Test
  void refusesTaskAdmissionWhenTheDurablePlanDisappears() {
    Fixture fixture = KafkaCompactionPlanCodecV1Test.fixture("UNCOMPRESSED");
    FakePlanStore plans = new FakePlanStore(new ArrayList<>());
    plans.removeBeforeFirstGet = true;
    KafkaCompactionPlanCoordinator coordinator =
        new KafkaCompactionPlanCoordinator(
            plans,
            (task, guard) -> guard.revalidate().thenApply(ignored -> task),
            new KafkaCompactionPlanRecordMapper(),
            Clock.systemUTC());

    assertThatThrownBy(
            () ->
                coordinator
                    .converge(
                        new KafkaPartitionId("kraft", topicId(8), 1),
                        fixture.outputTask(),
                        fixture.plan().bindingMetadataVersion(),
                        fixture.plan().lastStableOffset(),
                        fixture.plan().highWatermark(),
                        fixture.plan().candidate(),
                        fixture.plan().decisionSources(),
                        fixture.plan().passOneSnapshot(),
                        () -> CompletableFuture.completedFuture(null))
                    .join())
        .isInstanceOf(CompletionException.class)
        .hasRootCauseMessage("Kafka compaction plan disappeared before task creation");
  }

  private static String topicId(int value) {
    byte[] bytes = new byte[16];
    bytes[15] = (byte) value;
    return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
  }

  private static final class FakePlanStore implements KafkaCompactionPlanMetadataStore {
    private final List<String> events;
    private VersionedKafkaCompactionPlan current;
    private String lastLookupTaskId;
    private boolean removeBeforeFirstGet;

    private FakePlanStore(List<String> events) {
      this.events = events;
    }

    @Override
    public CompletableFuture<Optional<VersionedKafkaCompactionPlan>> getCompactionPlan(
        KafkaPartitionId id, String materializationTaskId) {
      events.add("plan-get");
      lastLookupTaskId = materializationTaskId;
      if (removeBeforeFirstGet) {
        removeBeforeFirstGet = false;
        current = null;
      }
      return CompletableFuture.completedFuture(Optional.ofNullable(current));
    }

    @Override
    public CompletableFuture<VersionedKafkaCompactionPlan> putCompactionPlanIfAbsent(
        KafkaCompactionPlanRecord value) {
      events.add("plan-put");
      if (current == null) {
        current =
            new VersionedKafkaCompactionPlan(
                "plans/" + value.materializationTaskId(),
                value.withMetadataVersion(1),
                1,
                new Checksum(ChecksumType.SHA256, "f".repeat(64)));
      } else if (!current.value().withMetadataVersion(0).equals(value)) {
        return CompletableFuture.failedFuture(new IllegalStateException("conflicting plan"));
      }
      return CompletableFuture.completedFuture(current);
    }

    @Override
    public CompletableFuture<KafkaCompactionPlanScanPage> scanCompactionPlans(
        KafkaPartitionId id, Optional<KafkaCompactionPlanScanToken> continuation, int limit) {
      return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Void> deleteCompactionPlan(VersionedKafkaCompactionPlan expected) {
      if (!expected.equals(current)) {
        return CompletableFuture.failedFuture(new IllegalStateException("stale plan"));
      }
      current = null;
      return CompletableFuture.completedFuture(null);
    }

    private String lastLookupTaskId() {
      return lastLookupTaskId;
    }
  }
}
