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

package io.nereus.metadata.oxia.records;

import java.util.Objects;

public record AppendSessionRecord(
        String streamId,
        String writerId,
        long epoch,
        String fencingToken,
        long leaseVersion,
        long expiresAtMillis) {
    public AppendSessionRecord {
        requireNonBlank(streamId, "streamId");
        requireNonBlank(writerId, "writerId");
        requireNonBlank(fencingToken, "fencingToken");
        if (epoch < 0 || leaseVersion < 0 || expiresAtMillis < 0) {
            throw new IllegalArgumentException("append session numeric fields must be non-negative");
        }
    }

    public static AppendSessionRecord fromHead(String streamId, AppendSessionSnapshotRecord snapshot) {
        return new AppendSessionRecord(
                streamId,
                snapshot.writerId(),
                snapshot.epoch(),
                snapshot.fencingToken(),
                snapshot.leaseVersion(),
                snapshot.expiresAtMillis());
    }

    private static void requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
    }
}
