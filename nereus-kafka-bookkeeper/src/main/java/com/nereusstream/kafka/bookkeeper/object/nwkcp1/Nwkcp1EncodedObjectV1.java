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

package com.nereusstream.kafka.bookkeeper.object.nwkcp1;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.Objects;

/** Exact immutable provider request identity for one encoded NWKCP1 body. */
public record Nwkcp1EncodedObjectV1(String key, long length, Sha256Digest digest, CanonicalBytes body) {
    public Nwkcp1EncodedObjectV1 {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(digest, "digest");
        Objects.requireNonNull(body, "body");
        if (length != body.length() || length <= 0 || length > Nwkcp1ConstantsV1.FORMAT_MAX_OBJECT_BYTES) {
            throw new IllegalArgumentException("NWKCP1 encoded length differs from its canonical body");
        }
        if (!Sha256Digest.hash(body).equals(digest)) {
            throw new IllegalArgumentException("NWKCP1 encoded digest differs from its canonical body");
        }
    }
}
