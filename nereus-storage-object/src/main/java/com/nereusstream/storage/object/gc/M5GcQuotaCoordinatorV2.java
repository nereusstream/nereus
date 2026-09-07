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

package com.nereusstream.storage.object.gc;

import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Entry;
import com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Head;
import com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Layout;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Crash-recoverable quota grants/refunds serialized by one exact head CAS, without multi-key transaction emulation. */
public final class M5GcQuotaCoordinatorV2 {
    public enum Result {
        GRANTED,
        SETTLED,
        EXHAUSTED,
        RETRY
    }

    public record Snapshot(Head head, long chargedCanonicalBytes, long availableCanonicalBytes) {}

    private final Layout layout;
    private final ExactMetadataTransactionStoreV1 accounting;
    private final ExactMetadataTransactionStoreV1 authorities;

    /** Accounting must enforce permanent entries, canonical scope and an explicitly initialized empty namespace. */
    public M5GcQuotaCoordinatorV2(
            Layout layout, ExactMetadataTransactionStoreV1 accounting, ExactMetadataTransactionStoreV1 authorities) {
        this.layout = Objects.requireNonNull(layout, "layout");
        this.accounting = Objects.requireNonNull(accounting, "accounting");
        this.authorities = Objects.requireNonNull(authorities, "authorities");
    }

    public CompletionStage<Snapshot> snapshot() {
        return head().thenApply(value -> {
            var current = decode(value);
            return new Snapshot(current, current.usedBytes(layout), current.availableBytes(layout));
        });
    }

    /** At most one operation is helped per call. RETRY is bounded contention/uncertainty, never an absence proof. */
    public CompletionStage<Result> reserve(PhysicalResourceIdV2 resource) {
        layout.requireResource(resource);
        // Capture the full head BEFORE observing absence. An intervening complete reserve/settle cycle changes it.
        return head().thenCompose(captured -> entry(resource).thenCompose(existing -> {
            if (existing.isPresent()) {
                return completed(existing.orElseThrow().settled() ? Result.SETTLED : Result.GRANTED);
            }
            var current = decode(captured);
            if (current.pending().isPresent()) {
                return recover(captured).thenApply(ignored -> Result.RETRY);
            }
            if (current.availableBytes(layout) < layout.reservationCharge()) {
                return completed(Result.EXHAUSTED);
            }
            return authorities.read(resource.authorityKey()).thenCompose(authority -> {
                if (authority.isPresent()) {
                    throw new IllegalStateException("unaccounted authority cannot be imported into a zero quota grant");
                }
                return commit(captured, current.reserve(layout, resource.sha256()))
                        .thenCompose(ignored -> entry(resource).thenApply(observed -> observed.map(
                                        value -> value.settled() ? Result.SETTLED : Result.GRANTED)
                                .orElse(Result.RETRY)));
            });
        }));
    }

    /** Existing authority mutations use their durable reservation even if another quota operation is pending/full. */
    public CompletionStage<Entry> requireExistingReservation(PhysicalResourceIdV2 resource) {
        layout.requireResource(resource);
        return head().thenCompose(ignored -> entry(resource)
                .thenApply(value -> value.orElseThrow(
                        () -> new IllegalStateException("authority lacks a permanent quota reservation"))));
    }

    /** Refund only unused reservation, after authoritative immutable compact done at the original authority key. */
    public CompletionStage<Result> settle(PhysicalResourceIdV2 resource) {
        layout.requireResource(resource);
        return head().thenCompose(captured -> entry(resource).thenCompose(observed -> {
            var existing = observed.orElseThrow(() -> new IllegalStateException("cannot settle an ungranted resource"));
            return compactDone(resource.authorityKey()).thenCompose(done -> {
                var settled = existing.grant().settle(done);
                if (existing.settled()) {
                    if (!existing.equals(settled)) {
                        throw new IllegalStateException("permanent quota settlement differs from authoritative done");
                    }
                    return completed(Result.SETTLED);
                }
                var current = decode(captured);
                if (current.pending().isPresent()) {
                    return recover(captured).thenApply(ignored -> Result.RETRY);
                }
                return commit(captured, current.settle(settled)).thenCompose(ignored -> entry(resource)
                        .thenApply(value -> value.filter(settled::equals).isPresent() ? Result.SETTLED : Result.RETRY));
            });
        }));
    }

    /** Operator composition authorizes explicit capacity expansion; there is no automatic expansion or shrink. */
    public CompletionStage<Boolean> expand(long capacityBytes) {
        return head().thenCompose(captured -> {
            var candidate = decode(captured).expand(capacityBytes);
            return accounting
                    .compareAndSet(Optional.of(captured), M5GcQuotaRecordsV2.HEAD_KEY, candidate.encode())
                    .thenCompose(ignored -> head())
                    .thenApply(observed -> decode(observed).capacityBytes() >= capacityBytes);
        });
    }

    public CompletionStage<Boolean> recover() {
        return head().thenCompose(this::recover);
    }

    private CompletionStage<Void> commit(VersionedValue captured, Head candidate) {
        layout.verify(candidate);
        return accounting
                .compareAndSet(Optional.of(captured), M5GcQuotaRecordsV2.HEAD_KEY, candidate.encode())
                .thenCompose(ignored -> head())
                .thenCompose(observed -> {
                    // Another helper or expansion can advance the head. Only its current exact pending work is helped.
                    return recover(observed).thenApply(done -> null);
                });
    }

    private CompletionStage<Boolean> recover(VersionedValue captured) {
        var current = decode(captured);
        if (current.pending().isEmpty()) {
            return CompletableFuture.completedFuture(true);
        }
        var pending = current.pending().orElseThrow();
        return accounting
                .read(pending.key())
                .thenCompose(observed -> {
                    observed.ifPresent(value -> verifyEntry(value, pending.key()));
                    if (observed.isPresent()
                            && Entry.decode(observed.orElseThrow().canonicalStoredBytes())
                                    .equals(pending)) {
                        return CompletableFuture.completedFuture(true);
                    }
                    if (pending.settled()) {
                        if (observed.isEmpty()
                                || !Entry.decode(observed.orElseThrow().canonicalStoredBytes())
                                        .equals(pending.grant())) {
                            throw new IllegalStateException(
                                    "pending quota settlement lost or changed its original grant");
                        }
                        return compactDone(M5GcQuotaRecordsV2.authorityKey(pending.resource()))
                                .thenCompose(done -> {
                                    if (!pending.grant().settle(done).equals(pending)) {
                                        throw new IllegalStateException(
                                                "pending quota settlement has another permanent done");
                                    }
                                    return writeEntry(observed, pending);
                                });
                    }
                    if (observed.isPresent()) {
                        var newer = Entry.decode(observed.orElseThrow().canonicalStoredBytes());
                        if (!newer.grant().equals(pending)) {
                            throw new IllegalStateException("pending quota grant has another permanent reservation");
                        }
                        // This captured head is stale; a different helper already completed and settled this exact
                        // grant.
                        return CompletableFuture.completedFuture(false);
                    }
                    return writeEntry(Optional.empty(), pending);
                })
                .thenCompose(exact -> {
                    if (!exact) {
                        return CompletableFuture.completedFuture(false);
                    }
                    return accounting
                            .compareAndSet(
                                    Optional.of(captured),
                                    M5GcQuotaRecordsV2.HEAD_KEY,
                                    current.clearPending().encode())
                            .thenCompose(ignored -> head())
                            .thenApply(observed -> decode(observed).pending().isEmpty());
                });
    }

    private CompletionStage<Boolean> writeEntry(Optional<VersionedValue> previous, Entry candidate) {
        return accounting
                .compareAndSet(previous, candidate.key(), candidate.encode())
                .thenCompose(ignored -> accounting.read(candidate.key()))
                .thenApply(observed -> {
                    observed.ifPresent(value -> verifyEntry(value, candidate.key()));
                    return observed.filter(value -> value.canonicalStoredBytes().equals(candidate.encode()))
                            .isPresent();
                });
    }

    private CompletionStage<M5TargetDeleteDoneV2> compactDone(String key) {
        return authorities.read(key).thenApply(observed -> {
            var value =
                    observed.orElseThrow(() -> new IllegalStateException("quota settlement requires permanent done"));
            if (!value.key().equals(key)) {
                throw new IllegalArgumentException("quota authority read returned another resource key");
            }
            var done = M5TargetDeleteStoredValueV2.decode(value)
                    .compactDone()
                    .orElseThrow(() -> new IllegalStateException(
                            "active or full done authority still retains its complete reservation"));
            layout.requireResource(done.resource());
            return done;
        });
    }

    private CompletionStage<Optional<Entry>> entry(PhysicalResourceIdV2 resource) {
        String key = M5GcQuotaRecordsV2.entryKey(resource.sha256());
        return accounting.read(key).thenApply(value -> value.map(observed -> verifyEntry(observed, key)));
    }

    private Entry verifyEntry(VersionedValue value, String key) {
        var entry = Entry.decode(value.canonicalStoredBytes());
        layout.verify(entry);
        if (!value.key().equals(key) || !entry.key().equals(key)) {
            throw new IllegalArgumentException("quota entry resource key differs");
        }
        return entry;
    }

    private CompletionStage<VersionedValue> head() {
        return accounting.read(M5GcQuotaRecordsV2.HEAD_KEY).thenApply(observed -> {
            var value = observed.orElseThrow(() -> new IllegalStateException("durable GC quota is not initialized"));
            decode(value);
            return value;
        });
    }

    private Head decode(VersionedValue value) {
        if (!value.key().equals(M5GcQuotaRecordsV2.HEAD_KEY)) {
            throw new IllegalArgumentException("quota head read returned another key");
        }
        var head = Head.decode(value.canonicalStoredBytes());
        layout.verify(head);
        return head;
    }

    private static CompletionStage<Result> completed(Result result) {
        return CompletableFuture.completedFuture(result);
    }
}
