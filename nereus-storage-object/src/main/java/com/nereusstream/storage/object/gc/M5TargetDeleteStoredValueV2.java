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

package com.nereusstream.storage.object.gc;

import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityV1;
import java.util.Objects;
import java.util.Optional;

/** Exact stored union. A compact done is a present permanent veto, never a missing active authority. */
public record M5TargetDeleteStoredValueV2(
        VersionedValue exactStoredValue,
        Optional<TargetDeleteAuthorityV1> fullAuthority,
        Optional<M5TargetDeleteDoneV2> compactDone) {
    public M5TargetDeleteStoredValueV2 {
        Objects.requireNonNull(exactStoredValue, "exactStoredValue");
        fullAuthority = Objects.requireNonNull(fullAuthority, "fullAuthority");
        compactDone = Objects.requireNonNull(compactDone, "compactDone");
        if (fullAuthority.isPresent() == compactDone.isPresent()) {
            throw new IllegalArgumentException("stored delete value requires exactly one canonical family");
        }
        var encoded = fullAuthority.isPresent()
                ? M5TargetDeleteAuthorityCodecV1.encodeAuthority(fullAuthority.orElseThrow())
                : compactDone.orElseThrow().encode();
        var key = fullAuthority.isPresent()
                ? fullAuthority.orElseThrow().authorityKey()
                : compactDone.orElseThrow().authorityKey();
        if (!exactStoredValue.key().equals(key)
                || !exactStoredValue.canonicalStoredBytes().equals(encoded)) {
            throw new IllegalArgumentException("stored delete resource key or canonical value differs");
        }
    }

    public PhysicalResourceIdV2 resource() {
        return fullAuthority.isPresent()
                ? fullAuthority.orElseThrow().target().resourceId()
                : compactDone.orElseThrow().resource();
    }

    public TargetDeleteAuthorityStateV1 state() {
        return fullAuthority.isPresent()
                ? fullAuthority.orElseThrow().state()
                : TargetDeleteAuthorityStateV1.DELETE_DONE_V1;
    }

    public static M5TargetDeleteStoredValueV2 decode(VersionedValue value) {
        return M5TargetDeleteDoneV2.isCompactDone(value.canonicalStoredBytes())
                ? new M5TargetDeleteStoredValueV2(
                        value, Optional.empty(), Optional.of(M5TargetDeleteDoneV2.decode(value.canonicalStoredBytes())))
                : new M5TargetDeleteStoredValueV2(
                        value,
                        Optional.of(M5TargetDeleteAuthorityCodecV1.decodeAuthority(value.canonicalStoredBytes())),
                        Optional.empty());
    }
}
