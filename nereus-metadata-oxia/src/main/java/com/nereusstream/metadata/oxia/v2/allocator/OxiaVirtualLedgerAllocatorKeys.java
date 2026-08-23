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

package com.nereusstream.metadata.oxia.v2.allocator;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.ManagedLedgerIncarnationIdV1;
import java.util.Locale;
import java.util.Objects;

/** Exact bounded Oxia key grammar for Cell, ManagedLedger Head, and immutable node authority. */
public final class OxiaVirtualLedgerAllocatorKeys {
    public static final int MAX_KEY_BYTES = 512;
    private final String root;

    public OxiaVirtualLedgerAllocatorKeys(String root) {
        this.root = Objects.requireNonNull(root, "root");
        if (!root.startsWith("/") || root.endsWith("/") || root.contains("//")) {
            throw new IllegalArgumentException("allocator root must be an absolute normalized Oxia prefix");
        }
    }

    public String cellKey(Sha256Digest namespaceId, Sha256Digest sliceAssignmentId) {
        return bounded(sliceRoot(namespaceId, sliceAssignmentId) + "/cell");
    }

    public String headKey(
            Sha256Digest namespaceId, Sha256Digest sliceAssignmentId, ManagedLedgerIncarnationIdV1 incarnation) {
        return bounded(sliceRoot(namespaceId, sliceAssignmentId)
                + "/managed-ledgers/"
                + Objects.requireNonNull(incarnation, "incarnation").value().toHex()
                + "/head");
    }

    public String nodeKey(
            Sha256Digest namespaceId,
            Sha256Digest sliceAssignmentId,
            ManagedLedgerIncarnationIdV1 incarnation,
            long ledgerId) {
        if (ledgerId <= 0) {
            throw new IllegalArgumentException("virtual ledger ID must be positive");
        }
        return bounded(sliceRoot(namespaceId, sliceAssignmentId)
                + "/managed-ledgers/"
                + Objects.requireNonNull(incarnation, "incarnation").value().toHex()
                + "/nodes/"
                + String.format(Locale.ROOT, "%019d", ledgerId));
    }

    private String sliceRoot(Sha256Digest namespaceId, Sha256Digest sliceAssignmentId) {
        Objects.requireNonNull(namespaceId, "namespaceId");
        Objects.requireNonNull(sliceAssignmentId, "sliceAssignmentId");
        if (namespaceId.isZero() || sliceAssignmentId.isZero()) {
            throw new IllegalArgumentException("allocator namespace and slice assignment IDs must be non-zero");
        }
        return root + "/virtual-ledger-allocator/v1/" + namespaceId.toHex() + "/" + sliceAssignmentId.toHex();
    }

    private static String bounded(String key) {
        if (key.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_KEY_BYTES) {
            throw new IllegalArgumentException("allocator Oxia key exceeds 512 UTF-8 bytes");
        }
        return key;
    }
}
