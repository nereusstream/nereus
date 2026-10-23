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

package com.nereusstream.storage.object.retention;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import java.util.Objects;
import java.util.Optional;

/** Same-authority trim facts shared by Binding and Pulsar metadata cells. */
public final class M5LogicalTrimRecordsV2 {
    public static final int MAX_MUTATIONS = 256;

    private M5LogicalTrimRecordsV2() {}

    /** An active-Binding trim fence; it does not require or imply a DELETED selector. */
    public record LogicalTrimFenceV2(
            Sha256Digest attemptIdSha256, Sha256Digest floorSnapshotRootSha256, AuthorityFactV1 openAuthority) {
        public LogicalTrimFenceV2 {
            M5RetentionRecordsV1.requireDigest(attemptIdSha256, "attemptIdSha256");
            M5RetentionRecordsV1.requireDigest(floorSnapshotRootSha256, "floorSnapshotRootSha256");
            Objects.requireNonNull(openAuthority, "openAuthority");
        }
    }

    /** Present after wire-3 migration, including an aborted first trim with no committed frontier. */
    public record LogicalTrimStateV2(
            Optional<M5RetentionRecordsV1.BindingTrimFrontierV1> frontier, Optional<LogicalTrimFenceV2> fence) {
        public LogicalTrimStateV2 {
            frontier = Objects.requireNonNull(frontier, "frontier");
            fence = Objects.requireNonNull(fence, "fence");
        }
    }

    /** One original conditional external write; UNKNOWN keeps this exact admission. */
    public record LogicalTrimMutationV2(
            Sha256Digest operationId,
            String key,
            Optional<MetadataVersion> predecessorVersion,
            Optional<Sha256Digest> predecessorSha256,
            Sha256Digest candidateSha256) {
        public LogicalTrimMutationV2 {
            M5RetentionRecordsV1.requireDigest(operationId, "operationId");
            M5RetentionRecordsV1.requireText(key, "key");
            if (com.nereusstream.domain.bytes.CanonicalUtf8.fromString(key)
                            .bytes()
                            .length()
                    > 1024) {
                throw new IllegalArgumentException("trim mutation key exceeds the metadata key bound");
            }
            predecessorVersion = Objects.requireNonNull(predecessorVersion, "predecessorVersion");
            predecessorSha256 = Objects.requireNonNull(predecessorSha256, "predecessorSha256");
            if (predecessorVersion.isPresent() != predecessorSha256.isPresent()) {
                throw new IllegalArgumentException("trim mutation predecessor fields disagree");
            }
            predecessorVersion.ifPresent(v -> {
                if (v.value().isEmpty() || v.value().length() > 4096) {
                    throw new IllegalArgumentException("trim mutation predecessor version exceeds its bound");
                }
            });
            predecessorSha256.ifPresent(v -> M5RetentionRecordsV1.requireDigest(v, "predecessorSha256"));
            M5RetentionRecordsV1.requireDigest(candidateSha256, "candidateSha256");
        }
    }
}
