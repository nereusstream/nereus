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

package com.nereusstream.pulsar.offload.objectwal;

import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ExtentLocator;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ManifestSource;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ReadSource;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.PulsarBindingKey;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Immutable owner-local projection captured only when the Pulsar Object-WAL route view is published. */
public record PulsarObjectWalReadViewV1(
        PulsarBindingKey binding, long ownerEpoch, long viewVersion, List<LedgerView> ledgers) {
    public PulsarObjectWalReadViewV1 {
        Objects.requireNonNull(binding, "binding");
        ledgers = List.copyOf(Objects.requireNonNull(ledgers, "ledgers"));
        if (ownerEpoch <= 0 || viewVersion <= 0 || ledgers.isEmpty()) {
            throw new IllegalArgumentException("Pulsar Object-WAL read view identity is outside its domain");
        }
        List<LedgerView> ordered = ledgers.stream()
                .sorted(Comparator.comparingLong(LedgerView::virtualLedgerId))
                .toList();
        if (!ledgers.equals(ordered) || ledgers.stream().distinct().count() != ledgers.size()) {
            throw new IllegalArgumentException("Pulsar Object-WAL ledger views are not sorted unique");
        }
    }

    public LedgerView requireLedger(long virtualLedgerId) {
        return ledgers.stream()
                .filter(ledger -> ledger.virtualLedgerId() == virtualLedgerId)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("virtual ledger is absent from captured read view"));
    }

    public record LedgerView(
            long virtualLedgerId,
            long manifestThrough,
            long readableThrough,
            long durableThrough,
            long manifestGeneration,
            List<SourceInterval> intervals) {
        public LedgerView {
            intervals = List.copyOf(Objects.requireNonNull(intervals, "intervals"));
            if (virtualLedgerId <= 0
                    || manifestThrough < -1
                    || readableThrough < manifestThrough
                    || durableThrough < readableThrough
                    || manifestGeneration < 0) {
                throw new IllegalArgumentException("Pulsar Object-WAL ledger view frontiers are invalid");
            }
            long expected = 0;
            for (SourceInterval interval : intervals) {
                if (interval.virtualLedgerId() != virtualLedgerId || interval.startEntryIdInclusive() != expected) {
                    throw new IllegalArgumentException("Pulsar Object-WAL read intervals are not contiguous");
                }
                expected = interval.endEntryIdExclusive();
            }
            if (expected != Math.addExact(readableThrough, 1)) {
                throw new IllegalArgumentException("Pulsar Object-WAL read intervals differ from readable coverage");
            }
        }

        public SourceInterval requireInterval(long entryId) {
            return intervals.stream()
                    .filter(interval ->
                            interval.startEntryIdInclusive() <= entryId && entryId < interval.endEntryIdExclusive())
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("entry is absent from captured read view"));
        }
    }

    public record SourceInterval(
            long virtualLedgerId,
            long startEntryIdInclusive,
            long endEntryIdExclusive,
            ReadSource source,
            Optional<ManifestSource> manifest,
            Optional<ExtentLocator> activeLocator) {
        public SourceInterval {
            Objects.requireNonNull(source, "source");
            manifest = Objects.requireNonNull(manifest, "manifest");
            activeLocator = Objects.requireNonNull(activeLocator, "activeLocator");
            if (virtualLedgerId <= 0
                    || startEntryIdInclusive < 0
                    || endEntryIdExclusive <= startEntryIdInclusive
                    || (source == ReadSource.MANIFEST) != manifest.isPresent()
                    || (source == ReadSource.ACTIVE_TAIL) != activeLocator.isPresent()) {
                throw new IllegalArgumentException("Pulsar Object-WAL physical read interval is invalid");
            }
            activeLocator.ifPresent(locator -> {
                if (locator.position().virtualLedgerId() != virtualLedgerId
                        || locator.position().entryId() != startEntryIdInclusive
                        || endEntryIdExclusive != Math.addExact(startEntryIdInclusive, 1)) {
                    throw new IllegalArgumentException("active locator differs from its typed Pulsar interval");
                }
            });
        }
    }
}
