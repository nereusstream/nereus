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

package com.nereusstream.kafka.runtime;

import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.kafka.compaction.KafkaCompactionPartitionPass;
import com.nereusstream.kafka.compaction.KafkaCompactionTwoPassExecutor;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;

/** Bounded production settings for the F9 Object-WAL compaction runtime. */
public record NereusKafkaCompactionRuntimeConfiguration(
    Duration interval,
    int maxConcurrentPartitions,
    int maxPartitionsPerPass,
    int metadataScanPageSize,
    ReadOptions sourceReadOptions,
    int sourceReadPageRecords,
    int sourceReadPageBytes,
    KafkaCompactionTwoPassExecutor.Limits executorLimits,
    Path stagingDirectory,
    long maxStagingBytes,
    int uploadChunkBytes,
    Duration stagingOrphanGrace,
    Duration generationOperationTimeout,
    KafkaCompactionPartitionPass.Configuration partitionPass) {

  public NereusKafkaCompactionRuntimeConfiguration {
    interval = positive(interval, "interval");
    bounded(maxConcurrentPartitions, 1, 256, "maxConcurrentPartitions");
    bounded(maxPartitionsPerPass, 1, 100_000, "maxPartitionsPerPass");
    if (maxPartitionsPerPass < maxConcurrentPartitions) {
      throw new IllegalArgumentException(
          "maxPartitionsPerPass must be at least maxConcurrentPartitions");
    }
    bounded(metadataScanPageSize, 1, 1_000, "metadataScanPageSize");
    sourceReadOptions = Objects.requireNonNull(sourceReadOptions, "sourceReadOptions");
    if (sourceReadOptions.isolation() != ReadIsolation.COMMITTED) {
      throw new IllegalArgumentException("compaction source reads must use COMMITTED isolation");
    }
    bounded(sourceReadPageRecords, 1, 65_536, "sourceReadPageRecords");
    bounded(
        sourceReadPageBytes,
        64 * 1024,
        64 * 1024 * 1024,
        "sourceReadPageBytes");
    executorLimits = Objects.requireNonNull(executorLimits, "executorLimits");
    stagingDirectory = Objects.requireNonNull(stagingDirectory, "stagingDirectory").normalize();
    if (!stagingDirectory.isAbsolute()) {
      throw new IllegalArgumentException("stagingDirectory must be absolute");
    }
    if (maxStagingBytes <= 0) {
      throw new IllegalArgumentException("maxStagingBytes must be positive");
    }
    bounded(
        uploadChunkBytes,
        StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
        StagingFileManager.MAX_UPLOAD_CHUNK_BYTES,
        "uploadChunkBytes");
    if (maxStagingBytes < uploadChunkBytes) {
      throw new IllegalArgumentException("maxStagingBytes must be at least uploadChunkBytes");
    }
    stagingOrphanGrace = positive(stagingOrphanGrace, "stagingOrphanGrace");
    generationOperationTimeout =
        positive(generationOperationTimeout, "generationOperationTimeout");
    partitionPass = Objects.requireNonNull(partitionPass, "partitionPass");
  }

  private static Duration positive(Duration value, String field) {
    Duration exact = Objects.requireNonNull(value, field);
    if (exact.isZero() || exact.isNegative() || exact.toMillis() <= 0) {
      throw new IllegalArgumentException(field + " must be positive and millisecond-representable");
    }
    return exact;
  }

  private static void bounded(int value, int minimum, int maximum, String field) {
    if (value < minimum || value > maximum) {
      throw new IllegalArgumentException(
          field + " must be in [" + minimum + ", " + maximum + "]");
    }
  }
}
