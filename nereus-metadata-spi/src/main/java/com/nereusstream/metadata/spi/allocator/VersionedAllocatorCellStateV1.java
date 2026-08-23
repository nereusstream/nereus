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

import com.nereusstream.domain.registry.allocator.VirtualLedgerCellAllocatorStateV1;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import java.util.Objects;

public record VersionedAllocatorCellStateV1(VirtualLedgerCellAllocatorStateV1 value, MetadataVersion metadataVersion) {
    public VersionedAllocatorCellStateV1 {
        Objects.requireNonNull(value, "value");
        Objects.requireNonNull(metadataVersion, "metadataVersion");
    }
}
