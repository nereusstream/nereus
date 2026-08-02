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

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;

/**
 * Production checkpoint and durable-trim settings shared by retention and DeleteRecords.
 */
public record NereusKafkaMaintenanceConfiguration(
        Path stagingDirectory,
        long maxStagingBytes,
        int uploadChunkBytes,
        Duration stagingOrphanGrace,
        Duration checkpointObjectTimeout,
        Duration checkpointVerificationTimeout,
        Duration trimTimeout,
        Duration pendingProtectionTtl,
        Duration retentionInterval,
        int maxConcurrentPartitions,
        int maxPartitionsPerPass,
        Checksum contentPolicySha256,
        String writerBuild) {

    public NereusKafkaMaintenanceConfiguration {
        stagingDirectory =
                Objects.requireNonNull(stagingDirectory, "stagingDirectory").normalize();
        if (!stagingDirectory.isAbsolute()) {
            throw new IllegalArgumentException("stagingDirectory must be absolute");
        }
        if (maxStagingBytes <= 0) {
            throw new IllegalArgumentException("maxStagingBytes must be positive");
        }
        if (uploadChunkBytes < StagingFileManager.MIN_UPLOAD_CHUNK_BYTES
                || uploadChunkBytes > StagingFileManager.MAX_UPLOAD_CHUNK_BYTES) {
            throw new IllegalArgumentException("uploadChunkBytes must be in [64 KiB, 8 MiB]");
        }
        if (maxStagingBytes < uploadChunkBytes) {
            throw new IllegalArgumentException("maxStagingBytes must be at least uploadChunkBytes");
        }
        stagingOrphanGrace = positive(stagingOrphanGrace, "stagingOrphanGrace");
        checkpointObjectTimeout = positive(checkpointObjectTimeout, "checkpointObjectTimeout");
        checkpointVerificationTimeout = positive(checkpointVerificationTimeout, "checkpointVerificationTimeout");
        trimTimeout = positive(trimTimeout, "trimTimeout");
        pendingProtectionTtl = positive(pendingProtectionTtl, "pendingProtectionTtl");
        retentionInterval = positive(retentionInterval, "retentionInterval");
        bounded(maxConcurrentPartitions, 1, 256, "maxConcurrentPartitions");
        bounded(maxPartitionsPerPass, 1, 100_000, "maxPartitionsPerPass");
        if (maxPartitionsPerPass < maxConcurrentPartitions) {
            throw new IllegalArgumentException("maxPartitionsPerPass must be at least maxConcurrentPartitions");
        }
        contentPolicySha256 = Objects.requireNonNull(contentPolicySha256, "contentPolicySha256");
        if (contentPolicySha256.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("contentPolicySha256 must use SHA256");
        }
        writerBuild = text(writerBuild, "writerBuild");
    }

    private static Duration positive(Duration value, String field) {
        Duration exact = Objects.requireNonNull(value, field);
        if (exact.isZero() || exact.isNegative() || exact.toMillis() <= 0) {
            throw new IllegalArgumentException(field + " must be positive and millisecond-representable");
        }
        return exact;
    }

    private static String text(String value, String field) {
        String exact = Objects.requireNonNull(value, field);
        if (exact.isBlank()) {
            throw new IllegalArgumentException(field + " must be nonblank");
        }
        return exact;
    }

    private static void bounded(int value, int minimum, int maximum, String field) {
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException(field + " must be in [" + minimum + ", " + maximum + "]");
        }
    }
}
