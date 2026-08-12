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

package com.nereusstream.metadata.spi.model;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.VirtualLedgerSliceViewV1;
import java.util.Objects;

/** Small immutable Registry projection used by the later allocator without rereading NVR1. */
public record VersionedVirtualLedgerSliceViewV1(
        VirtualLedgerSliceViewV1 value, MetadataVersion registryMetadataVersion, Sha256Digest canonicalRegistryDigest) {
    public VersionedVirtualLedgerSliceViewV1 {
        Objects.requireNonNull(value, "value");
        Objects.requireNonNull(registryMetadataVersion, "registryMetadataVersion");
        Objects.requireNonNull(canonicalRegistryDigest, "canonicalRegistryDigest");
        if (canonicalRegistryDigest.isZero()) {
            throw new IllegalArgumentException("canonical Registry digest must be non-zero");
        }
    }
}
