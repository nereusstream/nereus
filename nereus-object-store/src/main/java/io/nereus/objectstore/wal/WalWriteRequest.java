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

package io.nereus.objectstore.wal;

import java.util.List;
import java.util.Objects;

public record WalWriteRequest(
        String cluster,
        String writerId,
        String writerRunIdHash,
        long writerEpoch,
        List<WalStreamSliceInput> slices,
        WalWriteOptions options) {
    public WalWriteRequest {
        cluster = requireNonBlank(cluster, "cluster");
        writerId = requireNonBlank(writerId, "writerId");
        writerRunIdHash = requireNonBlank(writerRunIdHash, "writerRunIdHash");
        slices = List.copyOf(Objects.requireNonNull(slices, "slices"));
        Objects.requireNonNull(options, "options");
        if (writerEpoch < 0) {
            throw new IllegalArgumentException("writerEpoch must be non-negative");
        }
        if (slices.isEmpty()) {
            throw new IllegalArgumentException("slices cannot be empty");
        }
    }

    private static String requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
        return value;
    }
}
