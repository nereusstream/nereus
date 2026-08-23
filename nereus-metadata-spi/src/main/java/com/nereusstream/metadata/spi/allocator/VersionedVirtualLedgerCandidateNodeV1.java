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

package com.nereusstream.metadata.spi.allocator;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.VirtualLedgerCandidateNodeV1;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public record VersionedVirtualLedgerCandidateNodeV1(
        Sha256Digest ledgerIdCompatibilityNamespaceId,
        Sha256Digest sliceAssignmentId,
        String authorityKey,
        VirtualLedgerCandidateNodeV1 value,
        MetadataVersion metadataVersion) {
    public VersionedVirtualLedgerCandidateNodeV1 {
        Objects.requireNonNull(ledgerIdCompatibilityNamespaceId, "ledgerIdCompatibilityNamespaceId");
        Objects.requireNonNull(sliceAssignmentId, "sliceAssignmentId");
        Objects.requireNonNull(authorityKey, "authorityKey");
        Objects.requireNonNull(value, "value");
        Objects.requireNonNull(metadataVersion, "metadataVersion");
        if (ledgerIdCompatibilityNamespaceId.isZero()
                || sliceAssignmentId.isZero()
                || !authorityKey.startsWith("/")
                || authorityKey.endsWith("/")
                || authorityKey.contains("//")
                || authorityKey.getBytes(StandardCharsets.UTF_8).length > 512) {
            throw new IllegalArgumentException("versioned allocator node has invalid authority provenance");
        }
    }
}
