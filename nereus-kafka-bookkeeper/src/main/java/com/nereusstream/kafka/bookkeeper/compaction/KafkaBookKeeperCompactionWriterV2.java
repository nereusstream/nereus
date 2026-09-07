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
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperCompactionLayoutV2.Layout;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperCompactionLayoutV2.PartBody;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2.CreateOutcome;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2.Part;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2.Task;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerAppendRequestV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerReadOutcomeV1;
import com.nereusstream.storage.bookkeeper.ImmutableRetainedStoragePayload;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.BookKeeperDeleteTargetV1;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.CaptureOutcome;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.CaptureResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * Writes and natively seals inventoried BK compaction parts, then verifies every entry and full metadata fingerprint.
 *
 * <p>No selector, task terminal record, physical deletion or Object provider is reachable here. The enclosing
 * coordinator must retain native writer authority and source protection throughout this asynchronous operation.
 * A failed or partial ledger remains inventoried and unselected; it is never rewritten after recovery fences it.
 */
public final class KafkaBookKeeperCompactionWriterV2 {
    @FunctionalInterface
    public interface SealedMetadataReader {
        CompletionStage<CaptureResult> capture(RunLedgerHandleV1 handle);
    }

    public record VerifiedPart(Part part, BookKeeperDeleteTargetV1 sealedMetadata) {
        public VerifiedPart {
            Objects.requireNonNull(part, "part");
            Objects.requireNonNull(sealedMetadata, "sealedMetadata");
            if (!part.handle().equals(sealedMetadata.handle())) {
                throw new IllegalArgumentException("verified BK part and sealed metadata differ");
            }
        }
    }

    private final KafkaBookKeeperInventoryV2 inventory;
    private final BookKeeperCellSession session;
    private final SealedMetadataReader metadataReader;
    private final Executor controlExecutor;

    public KafkaBookKeeperCompactionWriterV2(
            KafkaBookKeeperInventoryV2 inventory, BookKeeperCellSession session, SealedMetadataReader metadataReader) {
        this(inventory, session, metadataReader, Runnable::run);
    }

    /** Native metadata calls and continuation admission execute on the owner's bounded control executor. */
    public KafkaBookKeeperCompactionWriterV2(
            KafkaBookKeeperInventoryV2 inventory,
            BookKeeperCellSession session,
            SealedMetadataReader metadataReader,
            Executor controlExecutor) {
        this.controlExecutor = Objects.requireNonNull(controlExecutor, "controlExecutor");
        this.inventory = Objects.requireNonNull(inventory, "inventory");
        this.session = Objects.requireNonNull(session, "session");
        this.metadataReader = Objects.requireNonNull(metadataReader, "metadataReader");
    }

    /** Produces a complete list only after every part verifies. This result is not a selected read descriptor. */
    public CompletionStage<List<VerifiedPart>> write(Layout layout) {
        if (!inventory.register(layout.task())) {
            return CompletableFuture.failedFuture(new IllegalStateException("BK task registration remains unknown"));
        }
        CompletionStage<List<VerifiedPart>> result = CompletableFuture.completedFuture(new ArrayList<>());
        for (int index = 0; index < layout.parts().size(); index++) {
            int ordinal = index;
            result = result.thenComposeAsync(
                    parts -> writePart(layout.task(), ordinal, layout.parts().get(ordinal))
                            .thenApply(part -> {
                                parts.add(part);
                                return parts;
                            }),
                    controlExecutor);
        }
        return result.thenApply(List::copyOf);
    }

    /** Recovery never allocates or creates. Missing, partial or corrupt output cannot fall back to old input. */
    public CompletionStage<List<VerifiedPart>> recover(Layout layout) {
        inventory.requireRegisteredTask(layout.task(), session);
        CompletionStage<List<VerifiedPart>> result = CompletableFuture.completedFuture(new ArrayList<>());
        for (int index = 0; index < layout.parts().size(); index++) {
            int ordinal = index;
            result = result.thenComposeAsync(
                    parts -> {
                        Part part = inventory
                                .readPart(layout.task(), ordinal)
                                .orElseThrow(() -> new IllegalStateException("BK recovery lacks an inventoried part"));
                        return sealAndVerify(layout.task(), part, layout.parts().get(ordinal))
                                .thenApply(verified -> {
                                    parts.add(verified);
                                    return parts;
                                });
                    },
                    controlExecutor);
        }
        return result.thenApply(List::copyOf);
    }

    private CompletionStage<VerifiedPart> writePart(Task task, int ordinal, PartBody body) {
        return inventory
                .reservePart(task, ordinal, session)
                .thenComposeAsync(
                        reserved -> {
                            Part part = reserved.orElseThrow(
                                    () -> new IllegalStateException("BK part reservation remains unknown"));
                            return inventory.createPart(task, part, session).thenCompose(created -> {
                                if (created == CreateOutcome.CONFLICT) {
                                    return CompletableFuture.failedFuture(
                                            new IllegalStateException("different BK run occupies part"));
                                }
                                if (created == CreateOutcome.CREATED_WRITABLE) {
                                    // An unknown append may have applied. Recovery settles native writes before exact
                                    // full validation.
                                    return appendAll(part, body)
                                            .handle((ignored, failure) -> null)
                                            .thenCompose(ignored -> sealAndVerify(task, part, body));
                                }
                                return sealAndVerify(task, part, body);
                            });
                        },
                        controlExecutor);
    }

    private CompletionStage<Void> appendAll(Part part, PartBody body) {
        CompletionStage<Void> result = CompletableFuture.completedFuture(null);
        for (int index = 0; index < body.entries().size(); index++) {
            int entryId = index;
            result = result.thenCompose(
                    ignored -> append(part.handle(), entryId, body.entries().get(entryId)));
        }
        return result;
    }

    private CompletionStage<Void> append(RunLedgerHandleV1 handle, int entryId, CanonicalBytes bytes) {
        var payload = ImmutableRetainedStoragePayload.copyOf(bytes.toByteArray());
        try {
            return session.appendExplicitEntry(new RunLedgerAppendRequestV1(handle, entryId, payload))
                    .thenApply(result -> {
                        if (result.outcome() != ProviderMutationOutcomeV1.APPLIED_EXACT) {
                            throw new IllegalStateException("BK compaction append requires native recovery");
                        }
                        var proof = result.exactProof().orElseThrow();
                        if (!proof.handle().equals(handle)
                                || proof.entryId() != entryId
                                || !proof.payloadSha256()
                                        .equals(com.nereusstream.domain.bytes.Sha256Digest.hash(bytes))) {
                            throw new IllegalStateException("BK append returned a different entry proof");
                        }
                        return null;
                    });
        } finally {
            payload.release();
        }
    }

    private CompletionStage<VerifiedPart> sealAndVerify(Task task, Part part, PartBody body) {
        return session.openRunLedger(part.handle()).thenCompose(open -> {
            if (open.outcome() != com.nereusstream.storage.api.bookkeeper.RunLedgerOpenOutcomeV1.OPENED_EXACT
                    || !open.exactHandle().orElseThrow().equals(part.handle())) {
                throw new IllegalStateException("BK compaction recovery lacks exact run identity before fencing");
            }
            return fenceAndVerify(task, part, body);
        });
    }

    private CompletionStage<VerifiedPart> fenceAndVerify(Task task, Part part, PartBody body) {
        return session.fenceAndRecoverRunLedger(part.handle()).thenCompose(recovered -> {
            if (recovered.outcome() != ProviderMutationOutcomeV1.APPLIED_EXACT
                    || !recovered.exactProof().orElseThrow().handle().equals(part.handle())
                    || recovered.exactProof().orElseThrow().lastAddConfirmed()
                            != body.entries().size() - 1L) {
                throw new IllegalStateException("BK compaction part is missing, unsealed or incomplete");
            }
            return capture(part, body).thenCompose(before -> verifyEntries(task, part, body)
                    .thenCompose(ignored -> capture(part, body))
                    .thenApply(after -> {
                        if (!before.equals(after)) {
                            throw new IllegalStateException("BK sealed metadata changed during output verification");
                        }
                        return new VerifiedPart(part, after);
                    }));
        });
    }

    private CompletionStage<BookKeeperDeleteTargetV1> capture(Part part, PartBody body) {
        return metadataReader.capture(part.handle()).thenApply(captured -> {
            if (captured.outcome() != CaptureOutcome.EXACT_TARGET) {
                throw new IllegalStateException("BK compaction lacks exact native sealed metadata");
            }
            BookKeeperDeleteTargetV1 seal = captured.exactTarget().orElseThrow();
            if (!seal.handle().equals(part.handle())
                    || seal.sealedLastEntryId() != body.entries().size() - 1L
                    || seal.sealedLength() != body.plan().length()) {
                throw new IllegalStateException("BK compaction sealed metadata differs from the complete part");
            }
            return seal;
        });
    }

    private CompletionStage<Void> verifyEntries(Task task, Part part, PartBody body) {
        if (!body.plan().equals(task.parts().get(part.ordinal()))) {
            throw new IllegalArgumentException("BK output bytes differ from the registered layout");
        }
        CompletionStage<Void> result = CompletableFuture.completedFuture(null);
        for (int index = 0; index < body.entries().size(); index++) {
            int entryId = index;
            result = result.thenCompose(
                    ignored -> session.readExactEntry(part.handle(), entryId).thenApply(read -> {
                        if (read.outcome() != RunLedgerReadOutcomeV1.FOUND_EXACT) {
                            throw new IllegalStateException("BK output entry is missing or unreadable");
                        }
                        var exact = read.exactEntry().orElseThrow();
                        if (!exact.handle().equals(part.handle())
                                || exact.entryId() != entryId
                                || !exact.payload().equals(body.entries().get(entryId))) {
                            throw new IllegalStateException("BK output entry differs from the semantic carrier bytes");
                        }
                        return null;
                    }));
        }
        return result;
    }
}
