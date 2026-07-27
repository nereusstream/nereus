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

package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.PublicationId;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import com.nereusstream.metadata.oxia.records.TaskFailureClass;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

/** Test-only access to the package-private durable materialization mapper. */
public final class KafkaCompactionTaskTestSupport {
  private KafkaCompactionTaskTestSupport() {}

  public static VersionedMaterializationTask planned(
      MaterializationTask task, long metadataVersion) {
    MaterializationTaskRecord value =
        MaterializationRecordMapper.plannedTask(task, 100).withMetadataVersion(metadataVersion);
    return versioned(value);
  }

  public static VersionedMaterializationTask cancelled(
      MaterializationTask task, long metadataVersion) {
    MaterializationTaskRecord planned = MaterializationRecordMapper.plannedTask(task, 100);
    MaterializationTaskRecord value =
        new MaterializationTaskRecord(
            planned.schemaVersion(),
            planned.taskId(),
            planned.taskSequence(),
            planned.streamId(),
            planned.readViewId(),
            planned.taskKindId(),
            planned.offsetStart(),
            planned.offsetEnd(),
            planned.sources(),
            planned.sourceSetSha256(),
            planned.policyId(),
            planned.policyVersion(),
            planned.policySha256(),
            planned.policy(),
            TaskLifecycle.CANCELLED,
            0,
            Optional.empty(),
            Optional.empty(),
            OptionalLong.empty(),
            "",
            TaskFailureClass.CLOSED.wireId(),
            "cancelled for terminal-retirement test",
            0,
            planned.createdAtMillis(),
            200,
            metadataVersion);
    return versioned(value);
  }

  /**
   * Test-only idempotent publication state transition for adapter integration tests that do not
   * need the full physical-protection/index protocol.
   */
  public static CompletableFuture<Void> publish(
      MaterializationTaskStore tasks,
      MaterializationTask task,
      long generation,
      PublicationId publicationId) {
    if (generation <= 0) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("generation must be positive"));
    }
    return tasks
        .get(task.streamId(), task.taskId())
        .thenCompose(
            optional ->
                advancePublication(tasks, task, optional.orElseThrow(), generation, publicationId));
  }

  private static CompletableFuture<Void> advancePublication(
      MaterializationTaskStore tasks,
      MaterializationTask task,
      VersionedMaterializationTask current,
      long generation,
      PublicationId publicationId) {
    if (!tasks.requireTask(current).equals(task)) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("publication task identity changed"));
    }
    MaterializationTaskRecord replacement;
    switch (current.value().lifecycle()) {
      case OUTPUT_READY ->
          replacement =
              MaterializationRecordMapper.publishing(
                  current.value(), publicationId, current.value().updatedAtMillis() + 1);
      case PUBLISHING -> {
        if (!current.value().publicationId().equals(publicationId.value())) {
          return CompletableFuture.failedFuture(
              new IllegalArgumentException("publication identity changed"));
        }
        if (current.value().allocatedGeneration().isPresent()) {
          if (current.value().allocatedGeneration().orElseThrow() != generation) {
            return CompletableFuture.failedFuture(
                new IllegalArgumentException("publication generation changed"));
          }
          replacement =
              MaterializationRecordMapper.published(
                  current.value(), current.value().updatedAtMillis() + 1);
        } else {
          replacement =
              MaterializationRecordMapper.attachGeneration(
                  current.value(), generation, current.value().updatedAtMillis() + 1);
        }
      }
      case PUBLISHED -> {
        if (!current.value().publicationId().equals(publicationId.value())
            || current.value().allocatedGeneration().orElseThrow() != generation) {
          return CompletableFuture.failedFuture(
              new IllegalArgumentException("published generation identity changed"));
        }
        return CompletableFuture.completedFuture(null);
      }
      default -> {
        return CompletableFuture.failedFuture(
            new IllegalArgumentException("task is not publishable"));
      }
    }
    return tasks
        .compareAndSet(replacement, current.metadataVersion())
        .thenCompose(
            updated -> advancePublication(tasks, task, updated, generation, publicationId));
  }

  private static VersionedMaterializationTask versioned(MaterializationTaskRecord value) {
    return new VersionedMaterializationTask(
        "tasks/" + value.taskId(),
        value,
        value.metadataVersion(),
        new Checksum(ChecksumType.SHA256, "c".repeat(64)));
  }
}
