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

package com.nereusstream.metadata.oxia.records;

import com.nereusstream.api.AppendAuthority;
import java.util.Objects;
import java.util.Optional;

public record AppendSessionRecord(
        String streamId,
        String writerId,
        long epoch,
        String fencingToken,
        long leaseVersion,
        long expiresAtMillis,
        String authorityType,
        String authorityId,
        long authorityEpoch,
        String authorityOwnerId,
        long authorityOwnerEpoch) {
    public AppendSessionRecord(
            String streamId,
            String writerId,
            long epoch,
            String fencingToken,
            long leaseVersion,
            long expiresAtMillis) {
        this(streamId, writerId, epoch, fencingToken, leaseVersion, expiresAtMillis, "", "", 0, "", 0);
    }

    public AppendSessionRecord {
        requireNonBlank(streamId, "streamId");
        requireNonBlank(writerId, "writerId");
        requireNonBlank(fencingToken, "fencingToken");
        Objects.requireNonNull(authorityType, "authorityType");
        Objects.requireNonNull(authorityId, "authorityId");
        Objects.requireNonNull(authorityOwnerId, "authorityOwnerId");
        if (epoch <= 0 || leaseVersion <= 0 || expiresAtMillis <= 0
                || authorityEpoch < 0 || authorityOwnerEpoch < 0) {
            throw new IllegalArgumentException("append session numeric fields must be positive");
        }
        boolean emptyAuthority = authorityType.isEmpty();
        if (emptyAuthority != authorityId.isEmpty()
                || emptyAuthority != authorityOwnerId.isEmpty()
                || (emptyAuthority && (authorityEpoch != 0 || authorityOwnerEpoch != 0))) {
            throw new IllegalArgumentException("append authority fields must be all empty or all set");
        }
    }

    public static AppendSessionRecord fromHead(String streamId, AppendSessionSnapshotRecord snapshot) {
        return new AppendSessionRecord(
                streamId,
                snapshot.writerId(),
                snapshot.epoch(),
                snapshot.fencingToken(),
                snapshot.leaseVersion(),
                snapshot.expiresAtMillis(),
                snapshot.authorityType(),
                snapshot.authorityId(),
                snapshot.authorityEpoch(),
                snapshot.authorityOwnerId(),
                snapshot.authorityOwnerEpoch());
    }

    public Optional<AppendAuthority> authority() {
        return authorityType.isEmpty()
                ? Optional.empty()
                : Optional.of(new AppendAuthority(
                        authorityType, authorityId, authorityEpoch,
                        authorityOwnerId, authorityOwnerEpoch));
    }

    private static void requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
    }
}
