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

    KafkaBookKeeperNativeObjectReadSourcesV1 withoutFallback() {
        return new KafkaBookKeeperNativeObjectReadSourcesV1(
                rawSources, prefix, bookKeeperNamespace, false, providerReads);
    }

    /** Local recovery-reference transfer, separate from fallback read release and physical delete authorization. */
    KafkaBookKeeperNativeObjectReadSourcesV1 retainedBookKeeperTail() {
        if (fallback
                || rawSources.sealedRoots().isEmpty()
                || rawSources.sealedRoots().get(0).kafkaStartOffset()
                        != prefix.coverage().inclusiveStart()) {
            throw new IllegalStateException("Object generation is not the closed, complete native BK prefix");
        }
        requireOriginalFallback(rawSources, prefix, bookKeeperNamespace);
        long boundary = prefix.coverage().exclusiveEnd();
        var tailRoots = rawSources.sealedRoots().stream()
                .filter(root -> root.kafkaStartOffset() >= boundary)
                .toList();
        var tailRuns = rawSources.sealedReadRuns().stream()
                .filter(run -> run.startOffset() >= boundary)
                .toList();
        return new KafkaBookKeeperNativeObjectReadSourcesV1(
                new NativeReadSources(tailRoots, rawSources.activeRoot(), tailRuns, rawSources.configurationSha256()),
                prefix,
                bookKeeperNamespace,
                false,
                providerReads);
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
        var inputs = cut.sources();
        var first = inputs.get(0);
        boolean replacesSelectedObject = first.kind() == SourceKind.KAFKA_NMS1_DATA_V2;
        if (replacesSelectedObject
                && (prefix.rawFallbackEquivalent() || !first.formatRootSha256().equals(cut.predecessorViewSha256()))) {
            throw new IllegalStateException("Kafka Object successor has another selected input");
        }
        long rawStart = replacesSelectedObject ? first.coverage().exclusiveEnd() : cover.inclusiveStart();
        var rawInputs = replacesSelectedObject ? inputs.subList(1, inputs.size()) : inputs;
        var roots = raw.sealedRoots().stream()
                .filter(root -> root.kafkaEndOffsetExclusive().orElseThrow() > rawStart
                        && root.kafkaStartOffset() < cover.exclusiveEnd()
                        && root.kafkaStartOffset()
                                < root.kafkaEndOffsetExclusive().orElseThrow())
                .toList();
        if (roots.size() != rawInputs.size()) {
            throw new IllegalStateException("Kafka Object fallback lacks its complete original BK membership");
        }
        for (int i = 0; i < roots.size(); i++) {
            var root = roots.get(i);
            var source = rawInputs.get(i);
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
