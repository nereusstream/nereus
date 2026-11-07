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

package com.nereusstream.kafka.bookkeeper.broker;

import com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1.NativeReadSources;
import com.nereusstream.kafka.bookkeeper.read.KafkaObjectMaterializedPrefixV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;

/** One captured Object generation and the exact raw BK source set it may read under the selector. */
public record KafkaBookKeeperNativeObjectReadSourcesV1(
        NativeReadSources rawSources,
        KafkaObjectMaterializedPrefixV1 prefix,
        Namespace bookKeeperNamespace,
        boolean fallback,
        Executor providerReads) {
    public KafkaBookKeeperNativeObjectReadSourcesV1 {
        Objects.requireNonNull(rawSources, "rawSources");
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(bookKeeperNamespace, "bookKeeperNamespace");
        Objects.requireNonNull(providerReads, "providerReads");
        if (bookKeeperNamespace.providerKind() != PhysicalResourceIdV2.ProviderKind.BOOKKEEPER) {
            throw new IllegalArgumentException("native Kafka Object read must retain its actual BK tail");
        }
        var coverage = prefix.coverage();
        if (rawSources.activeRoot().kafkaStartOffset() < coverage.exclusiveEnd()) {
            throw new IllegalArgumentException("Kafka Object prefix overlaps the active BK tail");
        }
        if (fallback) {
            requireOriginalFallback(rawSources, prefix, bookKeeperNamespace);
        }
    }

    public void requireSelector(BindingReadSelector selector) {
        var original = prefix.sourceCut().predecessorSelector();
        if (!selector.binding().equals(original.binding())
                || !selector.capability().equals(original.capability())
                || selector.ownerEpoch() < original.ownerEpoch()
                || selector.sourceGeneration() < prefix.sourceGeneration()
                || !selector.selectedViewSha256().equals(prefix.manifestSha256())
                || selector.mode() != (fallback ? SelectorMode.PREFERRED_WITH_FALLBACK : SelectorMode.PREFERRED_ONLY)
                || !selector.fallbackSetSha256()
                        .equals(
                                fallback
                                        ? Optional.of(
                                                M4ReadControlCodecV1.calculateFallbackSetSha256(fallbackSources()))
                                        : Optional.empty())) {
            throw new IllegalStateException("Kafka Object read view differs from its exact admitted selector");
        }
    }

    public List<SourceProtectionIdentity> fallbackSources() {
        return fallback ? prefix.originalFallbackSources() : List.of();
    }

    KafkaBookKeeperNativeObjectReadSourcesV1 withRawSources(NativeReadSources successor) {
        KafkaBookKeeperPartitionV1.NativePreferredReadSources.requireSourceExtension(rawSources, successor);
        return new KafkaBookKeeperNativeObjectReadSourcesV1(
                successor, prefix, bookKeeperNamespace, fallback, providerReads);
    }

    private static void requireOriginalFallback(
            NativeReadSources raw, KafkaObjectMaterializedPrefixV1 prefix, Namespace namespace) {
        var cut = prefix.sourceCut();
        var cover = cut.coverage();
        var roots = raw.sealedRoots().stream()
                .filter(root -> root.kafkaEndOffsetExclusive().orElseThrow() > cover.inclusiveStart()
                        && root.kafkaStartOffset() < cover.exclusiveEnd()
                        && root.kafkaStartOffset()
                                < root.kafkaEndOffsetExclusive().orElseThrow())
                .toList();
        if (roots.size() != cut.sources().size()) {
            throw new IllegalStateException("Kafka Object fallback lacks its complete original BK membership");
        }
        for (int i = 0; i < roots.size(); i++) {
            var root = roots.get(i);
            var source = cut.sources().get(i);
            var resource = new PhysicalResourceIdV2.BookKeeperLedger(
                    namespace, root.ledgerIdentity().ledgerId());
            if (source.kind() != SourceKind.BOOKKEEPER_LEDGER
                    || !source.ledgerIdentitySha256().equals(Optional.of(resource.sha256()))
                    || source.coverage().inclusiveStart() != root.kafkaStartOffset()
                    || source.coverage().exclusiveEnd()
                            != root.kafkaEndOffsetExclusive().orElseThrow()) {
                throw new IllegalStateException("Kafka Object fallback would read a different BK resource or range");
            }
        }
    }
}
