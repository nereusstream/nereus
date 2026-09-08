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

package com.nereusstream.kafka.bookkeeper.compaction;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperCompactionLayoutV2.PartBody;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperCompactionWriterV2.SealedMetadataReader;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.RunLedgerOpenOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerReadOutcomeV1;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.BookKeeperDeleteTargetV1;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.CaptureOutcome;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * Read-only descriptor recovery. It never allocates, appends, fences, reads old inputs or contacts an Object provider.
 * The caller must hold M4 read/source-plan admission and reserve cache/temporary memory in its Cell budget.
 */
public final class KafkaSealedBookKeeperReaderV2 {
    public record DecodingBounds(int records, long bytes) {
        public DecodingBounds {
            if (records < 0
                    || records > KafkaCompactionRecordsV1.MAX_RECORDS
                    || bytes < 0
                    || bytes > 256L * 1024 * 1024) {
                throw new IllegalArgumentException("selected BK decoding budget exceeds its hard bounds");
            }
        }
    }

    private final BookKeeperCellSession session;
    private final SealedMetadataReader metadataReader;
    private final int maximumEncodedRecoveryBytes;
    private final Optional<DecodingBounds> decodingBounds;
    private final Executor decodingExecutor;

    public KafkaSealedBookKeeperReaderV2(
            BookKeeperCellSession session, SealedMetadataReader metadataReader, int maximumEncodedRecoveryBytes) {
        this(session, metadataReader, maximumEncodedRecoveryBytes, Optional.empty(), Runnable::run);
    }

    public KafkaSealedBookKeeperReaderV2(
            BookKeeperCellSession session,
            SealedMetadataReader metadataReader,
            int maximumEncodedRecoveryBytes,
            DecodingBounds decodingBounds,
            Executor decodingExecutor) {
        this(session, metadataReader, maximumEncodedRecoveryBytes, Optional.of(decodingBounds), decodingExecutor);
    }

    private KafkaSealedBookKeeperReaderV2(
            BookKeeperCellSession session,
            SealedMetadataReader metadataReader,
            int maximumEncodedRecoveryBytes,
            Optional<DecodingBounds> decodingBounds,
            Executor decodingExecutor) {
        this.session = Objects.requireNonNull(session, "session");
        this.metadataReader = Objects.requireNonNull(metadataReader, "metadataReader");
        this.decodingBounds = decodingBounds;
        this.decodingExecutor = Objects.requireNonNull(decodingExecutor, "decodingExecutor");
        if (maximumEncodedRecoveryBytes <= 0) {
            throw new IllegalArgumentException("BK descriptor recovery requires a positive pre-admitted byte bound");
        }
        this.maximumEncodedRecoveryBytes = maximumEncodedRecoveryBytes;
    }

    /**
     * The descriptor hash must be bound to the caller's selected view, or used only for pre-publication validation.
     */
    public CompletionStage<KafkaBookKeeperReadViewV2> recover(KafkaSealedBookKeeperDescriptorV2 descriptor) {
        long required = descriptor.task().parts().stream()
                .mapToLong(KafkaBookKeeperInventoryV2.PartPlan::length)
                .sum();
        if (required > maximumEncodedRecoveryBytes
                || !session.capabilitySnapshot().equals(descriptor.task().capability())) {
            throw new IllegalStateException("BK descriptor recovery lacks exact capability or bounded byte admission");
        }
        CompletionStage<List<PartBody>> result = CompletableFuture.completedFuture(new ArrayList<>());
        for (int ordinal = 0; ordinal < descriptor.sealedParts().size(); ordinal++) {
            int part = ordinal;
            result = result.thenCompose(parts -> readPart(descriptor, part).thenApply(body -> {
                parts.add(body);
                return parts;
            }));
        }
        return result.thenApplyAsync(
                parts -> new KafkaBookKeeperReadViewV2(
                        descriptor,
                        KafkaBookKeeperArtifactAssemblerV2.assemble(descriptor.task(), parts),
                        decodingBounds),
                decodingExecutor);
    }

    private CompletionStage<PartBody> readPart(KafkaSealedBookKeeperDescriptorV2 descriptor, int ordinal) {
        BookKeeperDeleteTargetV1 expected = descriptor.sealedParts().get(ordinal);
        var plan = descriptor.task().parts().get(ordinal);
        return requireMetadata(expected)
                .thenCompose(ignored -> session.openRunLedger(expected.handle()))
                .thenCompose(open -> {
                    if (open.outcome() != RunLedgerOpenOutcomeV1.OPENED_EXACT
                            || !open.exactHandle().orElseThrow().equals(expected.handle())) {
                        throw new IllegalStateException(
                                "selected BK ledger is missing or has a different run identity");
                    }
                    CompletionStage<List<CanonicalBytes>> entries =
                            CompletableFuture.completedFuture(new ArrayList<>());
                    long[] observedBytes = {0};
                    for (int entry = 0; entry < plan.entryCount(); entry++) {
                        int id = entry;
                        entries = entries.thenCompose(values -> session.readExactEntry(expected.handle(), id)
                                .thenApply(read -> {
                                    if (read.outcome() != RunLedgerReadOutcomeV1.FOUND_EXACT) {
                                        throw new IllegalStateException("selected BK entry is missing or unreadable");
                                    }
                                    var exact = read.exactEntry().orElseThrow();
                                    if (!exact.handle().equals(expected.handle())
                                            || exact.entryId() != id
                                            || exact.payload().length()
                                                    > descriptor
                                                            .task()
                                                            .capability()
                                                            .maximumAddPayloadBytes()) {
                                        throw new IllegalStateException(
                                                "selected BK read returned a different or oversized entry");
                                    }
                                    observedBytes[0] = Math.addExact(
                                            observedBytes[0], exact.payload().length());
                                    if (observedBytes[0] > plan.length()) {
                                        throw new IllegalStateException(
                                                "selected BK entries exceed the admitted exact part length");
                                    }
                                    values.add(exact.payload());
                                    return values;
                                }));
                    }
                    return entries.thenCompose(
                            values -> requireMetadata(expected).thenApply(ignored -> {
                                var body = new PartBody(plan.kind(), values);
                                if (!body.plan().equals(plan)) {
                                    throw new IllegalStateException(
                                            "selected BK complete part checksum or length differs");
                                }
                                return body;
                            }));
                });
    }

    private CompletionStage<Void> requireMetadata(BookKeeperDeleteTargetV1 expected) {
        return metadataReader.capture(expected.handle()).thenApply(result -> {
            if (result.outcome() != CaptureOutcome.EXACT_TARGET
                    || !result.exactTarget().orElseThrow().equals(expected)) {
                throw new IllegalStateException("selected BK native sealed metadata is missing, changed or unknown");
            }
            return null;
        });
    }
}
