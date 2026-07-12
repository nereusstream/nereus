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

package com.nereusstream.managedledger.projection;

import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import java.util.Objects;

/** Immutable mapping between one managed-ledger topic incarnation and one Nereus stream. */
public record VirtualLedgerProjection(
        StreamId streamId,
        String managedLedgerName,
        long storageClassBindingGeneration,
        long incarnation,
        long virtualLedgerId,
        int mappingVersion,
        String payloadMapping,
        long createdAtMillis,
        long metadataVersion) {
    public static final int MAPPING_VERSION = ManagedLedgerProjectionNames.POSITION_MAPPING_VERSION;
    public static final long MIN_VIRTUAL_LEDGER_ID = ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID;

    public VirtualLedgerProjection {
        Objects.requireNonNull(streamId, "streamId");
        managedLedgerName = ManagedLedgerProjectionNames.requireManagedLedgerName(managedLedgerName);
        Objects.requireNonNull(payloadMapping, "payloadMapping");
        if (storageClassBindingGeneration < 1) {
            throw new ProjectionValidationException("storageClassBindingGeneration must be positive");
        }
        if (incarnation < 1) {
            throw new ProjectionValidationException("incarnation must be positive");
        }
        if (virtualLedgerId < MIN_VIRTUAL_LEDGER_ID || virtualLedgerId >= Long.MAX_VALUE) {
            throw new ProjectionValidationException("virtualLedgerId is outside the reserved F2 range");
        }
        if (mappingVersion != ManagedLedgerProjectionNames.POSITION_MAPPING_VERSION) {
            throw new ProjectionValidationException("unsupported projection mappingVersion");
        }
        if (!ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1.equals(payloadMapping)) {
            throw new ProjectionValidationException("unsupported projection payloadMapping");
        }
        if (createdAtMillis < 0 || metadataVersion < 0) {
            throw new ProjectionValidationException("projection versions and timestamps must be non-negative");
        }
        StreamId expectedStreamId = ManagedLedgerProjectionNames.streamId(managedLedgerName, incarnation);
        if (!expectedStreamId.equals(streamId)) {
            throw new ProjectionValidationException("streamId does not match managed-ledger name/incarnation");
        }
    }

    public void requireStorageClassBindingGeneration(long expectedGeneration) {
        if (expectedGeneration < 1) {
            throw new ProjectionValidationException("expected storage-class binding generation must be positive");
        }
        if (storageClassBindingGeneration != expectedGeneration) {
            throw new ProjectionValidationException("storage-class binding generation mismatch");
        }
    }
}
