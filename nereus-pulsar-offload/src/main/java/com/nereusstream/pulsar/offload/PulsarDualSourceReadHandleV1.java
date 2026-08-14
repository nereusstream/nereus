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

package com.nereusstream.pulsar.offload;

import com.nereusstream.pulsar.offload.PulsarObjectReadHandleV1.ObjectReadException;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.DeleteState;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.RetentionClass;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.EntryPayload;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** ManagedLedger-owned whole-range dual-source handle with exact-version source pins. */
public final class PulsarDualSourceReadHandleV1 {
    public enum Source {
        OBJECT,
        BOOKKEEPER
    }

    public enum Preference {
        OBJECT_FIRST,
        BOOKKEEPER_FIRST
    }

    public enum FailureKind {
        NO_SUCH_LEDGER,
        TRANSIENT,
        TIMEOUT,
        UNAVAILABLE,
        SHORT_READ,
        INTEGRITY,
        FORMAT,
        INVALID_RANGE,
        CANCELLED,
        CLOSED,
        UNSUPPORTED,
        FENCED
    }

    public record MetadataSnapshot(
            long ledgerId,
            long version,
            UUID attemptUuid,
            boolean offloadComplete,
            RetentionClass retentionClass,
            DeleteState deleteState,
            boolean bookkeeperDeleted) {
        public MetadataSnapshot {
            Objects.requireNonNull(retentionClass, "retentionClass");
            Objects.requireNonNull(deleteState, "deleteState");
            if (ledgerId < 0 || version < 0 || (offloadComplete && attemptUuid == null)) {
                throw new IllegalArgumentException("native metadata identity is incomplete");
            }
            boolean expectedDeleted = deleteState != DeleteState.BK_DELETE_NONE;
            if (bookkeeperDeleted != expectedDeleted
                    || (!offloadComplete && deleteState != DeleteState.BK_DELETE_NONE)
                    || (retentionClass == RetentionClass.RETAIN_BK && deleteState != DeleteState.BK_DELETE_NONE)) {
                throw new IllegalArgumentException("native metadata source/delete facts are inconsistent");
            }
        }

        public boolean sameVersionAndAttempt(MetadataSnapshot other) {
            return other != null
                    && ledgerId == other.ledgerId
                    && version == other.version
                    && Objects.equals(attemptUuid, other.attemptUuid);
        }

        public boolean eligible(Source source) {
            if (!offloadComplete) {
                return source == Source.BOOKKEEPER;
            }
            if (deleteState == DeleteState.BK_DELETE_NONE) {
                return true;
            }
            return source == Source.OBJECT;
        }
    }

    @FunctionalInterface
    public interface MetadataSupplier {
        MetadataSnapshot current();
    }

    public interface ChildSource {
        CompletionStage<RangeResult> read(long firstEntryId, long lastEntryId);

        CompletionStage<Void> close();
    }

    @FunctionalInterface
    public interface ObjectFailureObserver {
        void record(MetadataSnapshot snapshot, SourceReadException failure);
    }

    public static final class RangeResult {
        private final Source source;
        private final List<EntryPayload> entries;
        private final Supplier<CompletionStage<Void>> release;
        private final AtomicBoolean released = new AtomicBoolean();

        public RangeResult(Source source, List<EntryPayload> entries, Supplier<CompletionStage<Void>> release) {
            this.source = Objects.requireNonNull(source, "source");
            this.entries = List.copyOf(entries);
            this.release = Objects.requireNonNull(release, "release");
        }

        public Source source() {
            return source;
        }

        public List<EntryPayload> entries() {
            return entries;
        }

        public CompletionStage<Void> release() {
            if (!released.compareAndSet(false, true)) {
                return CompletableFuture.completedFuture(null);
            }
            return callStage(release);
        }
    }

    public static final class SourceReadException extends IllegalStateException {
        private final Source source;
        private final FailureKind kind;
        private final RangeResult partialResult;

        public SourceReadException(Source source, FailureKind kind, String message) {
            this(source, kind, message, null, null);
        }

        public SourceReadException(Source source, FailureKind kind, String message, Throwable cause) {
            this(source, kind, message, cause, null);
        }

        public SourceReadException(
                Source source, FailureKind kind, String message, Throwable cause, RangeResult partialResult) {
            super(message, cause);
            this.source = source;
            this.kind = Objects.requireNonNull(kind, "kind");
            this.partialResult = partialResult;
        }

        public Source source() {
            return source;
        }

        public FailureKind kind() {
            return kind;
        }

        public RangeResult partialResult() {
            return partialResult;
        }
    }

    private final MetadataSupplier metadataSupplier;
    private final Preference preference;
    private final LazyChild objectChild;
    private final LazyChild bookKeeperChild;
    private final ObjectFailureObserver objectFailureObserver;
    private final PinState pins = new PinState();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object closeMonitor = new Object();
    private CompletableFuture<Void> closeFuture;

    public PulsarDualSourceReadHandleV1(
            MetadataSupplier metadataSupplier,
            Preference preference,
            Supplier<CompletionStage<ChildSource>> objectFactory,
            Supplier<CompletionStage<ChildSource>> bookKeeperFactory,
            ObjectFailureObserver objectFailureObserver) {
        this.metadataSupplier = Objects.requireNonNull(metadataSupplier, "metadataSupplier");
        this.preference = Objects.requireNonNull(preference, "preference");
        this.objectChild = new LazyChild(Source.OBJECT, objectFactory);
        this.bookKeeperChild = new LazyChild(Source.BOOKKEEPER, bookKeeperFactory);
        this.objectFailureObserver = Objects.requireNonNull(objectFailureObserver, "objectFailureObserver");
    }

    public static ChildSource objectChild(PulsarObjectReadHandleV1 handle) {
        Objects.requireNonNull(handle, "handle");
        return new ChildSource() {
            @Override
            public CompletionStage<RangeResult> read(long firstEntryId, long lastEntryId) {
                return handle.read(firstEntryId, lastEntryId)
                        .thenApply(entries ->
                                new RangeResult(Source.OBJECT, entries, () -> CompletableFuture.completedFuture(null)))
                        .exceptionallyCompose(failure -> CompletableFuture.failedFuture(mapObjectFailure(failure)));
            }

            @Override
            public CompletionStage<Void> close() {
                return handle.close();
            }
        };
    }

    public CompletionStage<RangeResult> read(long firstEntryId, long lastEntryId) {
        if (firstEntryId < 0 || lastEntryId < firstEntryId) {
            return CompletableFuture.failedFuture(
                    new SourceReadException(null, FailureKind.INVALID_RANGE, "invalid inclusive entry range"));
        }
        if (closed.get()) {
            return CompletableFuture.failedFuture(
                    new SourceReadException(null, FailureKind.CLOSED, "dual-source handle is closed"));
        }
        MetadataSnapshot snapshot;
        try {
            snapshot = Objects.requireNonNull(metadataSupplier.current(), "metadata snapshot");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(
                    new SourceReadException(null, FailureKind.TRANSIENT, "cannot read native metadata", failure));
        }
        try {
            Source primary = primary(snapshot);
            return readFrom(primary, snapshot, firstEntryId, lastEntryId, true);
        } catch (SourceReadException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    public CompletionStage<Void> fenceBookKeeper(MetadataSnapshot expected) {
        Objects.requireNonNull(expected, "expected");
        MetadataSnapshot current;
        try {
            current = metadataSupplier.current();
        } catch (Throwable metadataFailure) {
            return CompletableFuture.failedFuture(new SourceReadException(
                    Source.BOOKKEEPER, FailureKind.TRANSIENT, "cannot read metadata before BK fence", metadataFailure));
        }
        if (!expected.sameVersionAndAttempt(current)) {
            return CompletableFuture.failedFuture(
                    new SourceReadException(Source.BOOKKEEPER, FailureKind.FENCED, "metadata changed before BK fence"));
        }
        return pins.fence(Source.BOOKKEEPER);
    }

    public boolean unfenceBookKeeper(MetadataSnapshot expected) {
        MetadataSnapshot current;
        try {
            current = metadataSupplier.current();
        } catch (Throwable metadataFailure) {
            return false;
        }
        return expected.sameVersionAndAttempt(current)
                && current.deleteState() == DeleteState.BK_DELETE_NONE
                && pins.unfenceBookKeeper();
    }

    public CompletionStage<Void> closeBookKeeperAfterIntent() {
        return pins.fence(Source.BOOKKEEPER).thenCompose(ignored -> bookKeeperChild.close());
    }

    public CompletionStage<Void> close() {
        synchronized (closeMonitor) {
            if (closeFuture != null) {
                return closeFuture;
            }
            closed.set(true);
            closeFuture = new CompletableFuture<>();
            CompletionStage<Void> drains = pins.fenceBoth();
            drains.thenCompose(ignored -> allOf(objectChild.close(), bookKeeperChild.close()))
                    .whenComplete((ignored, failure) -> {
                        if (failure == null) {
                            closeFuture.complete(null);
                        } else {
                            closeFuture.completeExceptionally(unwrap(failure));
                        }
                    });
            return closeFuture;
        }
    }

    private CompletionStage<RangeResult> readFrom(
            Source source, MetadataSnapshot snapshot, long firstEntryId, long lastEntryId, boolean allowFallback) {
        Pin pin;
        try {
            if (closed.get() || !snapshot.eligible(source)) {
                throw new SourceReadException(source, FailureKind.FENCED, "source is not eligible");
            }
            pin = pins.acquire(source, snapshot);
            MetadataSnapshot afterPin = metadataSupplier.current();
            if (!snapshot.sameVersionAndAttempt(afterPin) || !afterPin.eligible(source)) {
                pin.release();
                throw new SourceReadException(source, FailureKind.FENCED, "metadata changed during pin admission");
            }
        } catch (SourceReadException failure) {
            try {
                MetadataSnapshot current = metadataSupplier.current();
                Source alternate = source == Source.OBJECT ? Source.BOOKKEEPER : Source.OBJECT;
                if (allowFallback
                        && failure.kind() == FailureKind.FENCED
                        && current != null
                        && current.offloadComplete()
                        && current.deleteState() == DeleteState.BK_DELETE_NONE
                        && current.eligible(alternate)) {
                    return readFrom(alternate, current, firstEntryId, lastEntryId, false);
                }
            } catch (Throwable metadataFailure) {
                failure.addSuppressed(metadataFailure);
            }
            return CompletableFuture.failedFuture(failure);
        }

        CompletableFuture<RangeResult> result = new CompletableFuture<>();
        child(source).read(firstEntryId, lastEntryId).whenComplete((childResult, childFailure) -> {
            if (childFailure == null) {
                try {
                    validateRange(childResult, source, firstEntryId, lastEntryId);
                    result.complete(
                            new RangeResult(source, childResult.entries(), () -> releaseThen(childResult, pin)));
                } catch (Throwable validationFailure) {
                    releaseThen(childResult, pin).whenComplete((ignored, releaseFailure) -> {
                        SourceReadException mapped = sourceFailure(source, validationFailure);
                        if (releaseFailure != null) {
                            mapped.addSuppressed(unwrap(releaseFailure));
                        }
                        afterFailure(result, mapped, snapshot, firstEntryId, lastEntryId, allowFallback);
                    });
                }
                return;
            }
            SourceReadException mapped = sourceFailure(source, childFailure);
            releaseThen(mapped.partialResult(), pin).whenComplete((ignored, releaseFailure) -> {
                if (releaseFailure != null) {
                    mapped.addSuppressed(unwrap(releaseFailure));
                }
                afterFailure(result, mapped, snapshot, firstEntryId, lastEntryId, allowFallback);
            });
        });
        return result;
    }

    private void afterFailure(
            CompletableFuture<RangeResult> result,
            SourceReadException primaryFailure,
            MetadataSnapshot originalSnapshot,
            long firstEntryId,
            long lastEntryId,
            boolean allowFallback) {
        if (primaryFailure.source() == Source.OBJECT
                && (primaryFailure.kind() == FailureKind.INTEGRITY || primaryFailure.kind() == FailureKind.FORMAT)) {
            try {
                objectFailureObserver.record(originalSnapshot, primaryFailure);
            } catch (Throwable observerFailure) {
                primaryFailure.addSuppressed(observerFailure);
            }
        }
        MetadataSnapshot current;
        try {
            current = metadataSupplier.current();
        } catch (Throwable metadataFailure) {
            primaryFailure.addSuppressed(metadataFailure);
            result.completeExceptionally(primaryFailure);
            return;
        }
        if (!allowFallback || !canFallback(primaryFailure, originalSnapshot, current)) {
            result.completeExceptionally(primaryFailure);
            return;
        }
        Source secondary = primaryFailure.source() == Source.OBJECT ? Source.BOOKKEEPER : Source.OBJECT;
        readFrom(secondary, current, firstEntryId, lastEntryId, false).whenComplete((range, secondaryFailure) -> {
            if (secondaryFailure == null) {
                result.complete(range);
            } else {
                primaryFailure.addSuppressed(unwrap(secondaryFailure));
                result.completeExceptionally(primaryFailure);
            }
        });
    }

    private static boolean canFallback(
            SourceReadException failure, MetadataSnapshot original, MetadataSnapshot current) {
        if (current == null
                || !Objects.equals(original.attemptUuid(), current.attemptUuid())
                || !current.offloadComplete()
                || current.deleteState() != DeleteState.BK_DELETE_NONE
                || !current.eligible(Source.OBJECT)
                || !current.eligible(Source.BOOKKEEPER)) {
            return false;
        }
        if (failure.source() == Source.OBJECT) {
            return switch (failure.kind()) {
                case NO_SUCH_LEDGER, TIMEOUT, UNAVAILABLE, SHORT_READ, INTEGRITY, FORMAT -> true;
                default -> false;
            };
        }
        return failure.source() == Source.BOOKKEEPER && failure.kind() == FailureKind.NO_SUCH_LEDGER;
    }

    private Source primary(MetadataSnapshot snapshot) {
        Source preferred = preference == Preference.OBJECT_FIRST ? Source.OBJECT : Source.BOOKKEEPER;
        if (snapshot.eligible(preferred)) {
            return preferred;
        }
        Source alternate = preferred == Source.OBJECT ? Source.BOOKKEEPER : Source.OBJECT;
        if (snapshot.eligible(alternate)) {
            return alternate;
        }
        throw new SourceReadException(null, FailureKind.FENCED, "native metadata authorizes no read source");
    }

    private LazyChild child(Source source) {
        return source == Source.OBJECT ? objectChild : bookKeeperChild;
    }

    private static void validateRange(RangeResult range, Source source, long first, long last) {
        if (range == null || range.source() != source) {
            throw new SourceReadException(source, FailureKind.INTEGRITY, "child source identity differs");
        }
        long expected = first;
        for (EntryPayload entry : range.entries()) {
            if (entry.entryId() != expected) {
                throw new SourceReadException(source, FailureKind.INTEGRITY, "child range is mixed or non-contiguous");
            }
            expected = Math.addExact(expected, 1);
        }
        if (expected != Math.addExact(last, 1)) {
            throw new SourceReadException(source, FailureKind.SHORT_READ, "child range ended early");
        }
    }

    private static SourceReadException mapObjectFailure(Throwable failure) {
        Throwable unwrapped = unwrap(failure);
        if (unwrapped instanceof ObjectReadException objectFailure) {
            FailureKind kind =
                    switch (objectFailure.kind()) {
                        case NOT_FOUND -> FailureKind.NO_SUCH_LEDGER;
                        case TIMEOUT -> FailureKind.TIMEOUT;
                        case UNAVAILABLE -> FailureKind.UNAVAILABLE;
                        case SHORT_READ -> FailureKind.SHORT_READ;
                        case INTEGRITY -> FailureKind.INTEGRITY;
                        case FORMAT -> FailureKind.FORMAT;
                        case INVALID_RANGE -> FailureKind.INVALID_RANGE;
                        case CANCELLED -> FailureKind.CANCELLED;
                        case CLOSED -> FailureKind.CLOSED;
                    };
            return new SourceReadException(Source.OBJECT, kind, "Object child read failed", objectFailure);
        }
        return sourceFailure(Source.OBJECT, unwrapped);
    }

    private static SourceReadException sourceFailure(Source source, Throwable failure) {
        Throwable unwrapped = unwrap(failure);
        if (unwrapped instanceof SourceReadException sourceFailure) {
            return sourceFailure;
        }
        return new SourceReadException(source, FailureKind.TRANSIENT, source + " child read failed", unwrapped);
    }

    private static CompletionStage<Void> releaseThen(RangeResult range, Pin pin) {
        CompletionStage<Void> rangeRelease = range == null ? CompletableFuture.completedFuture(null) : range.release();
        CompletableFuture<Void> result = new CompletableFuture<>();
        rangeRelease.whenComplete((ignored, rangeFailure) -> pin.release().whenComplete((pinIgnored, pinFailure) -> {
            Throwable failure = rangeFailure == null ? null : unwrap(rangeFailure);
            if (pinFailure != null) {
                if (failure == null) {
                    failure = unwrap(pinFailure);
                } else {
                    failure.addSuppressed(unwrap(pinFailure));
                }
            }
            if (failure == null) {
                result.complete(null);
            } else {
                result.completeExceptionally(failure);
            }
        }));
        return result;
    }

    private static CompletionStage<Void> allOf(CompletionStage<Void> first, CompletionStage<Void> second) {
        return CompletableFuture.allOf(first.toCompletableFuture(), second.toCompletableFuture());
    }

    private static <T> CompletionStage<T> callStage(Supplier<CompletionStage<T>> call) {
        try {
            CompletionStage<T> stage = call.get();
            return stage == null
                    ? CompletableFuture.failedFuture(new NullPointerException("child returned a null stage"))
                    : stage;
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private static Throwable unwrap(Throwable failure) {
        if (failure instanceof CompletionException && failure.getCause() != null) {
            return failure.getCause();
        }
        return failure;
    }

    private final class LazyChild {
        private final Source source;
        private final Supplier<CompletionStage<ChildSource>> factory;
        private CompletableFuture<ChildSource> initialized;
        private boolean closeRequested;
        private CompletableFuture<Void> childClose;

        private LazyChild(Source source, Supplier<CompletionStage<ChildSource>> factory) {
            this.source = source;
            this.factory = Objects.requireNonNull(factory, "factory");
        }

        private synchronized CompletableFuture<ChildSource> get() {
            if (closeRequested) {
                return CompletableFuture.failedFuture(
                        new SourceReadException(source, FailureKind.CLOSED, "child source is closing"));
            }
            if (initialized == null) {
                initialized = callStage(factory).toCompletableFuture();
            }
            return initialized;
        }

        private CompletionStage<RangeResult> read(long first, long last) {
            return get().thenCompose(child -> callStage(() -> child.read(first, last)));
        }

        private synchronized CompletionStage<Void> close() {
            if (childClose != null) {
                return childClose;
            }
            closeRequested = true;
            if (initialized == null) {
                childClose = CompletableFuture.completedFuture(null);
            } else {
                childClose = initialized.thenCompose(child -> callStage(child::close));
            }
            return childClose;
        }
    }

    private static final class PinState {
        private boolean bookKeeperFenced;
        private boolean objectFenced;
        private int bookKeeperPins;
        private int objectPins;
        private CompletableFuture<Void> bookKeeperDrain;
        private CompletableFuture<Void> objectDrain;

        private synchronized Pin acquire(Source source, MetadataSnapshot snapshot) {
            if ((source == Source.BOOKKEEPER && bookKeeperFenced) || (source == Source.OBJECT && objectFenced)) {
                throw new SourceReadException(source, FailureKind.FENCED, "source pin admission is fenced");
            }
            if (source == Source.BOOKKEEPER) {
                bookKeeperPins++;
            } else {
                objectPins++;
            }
            return new Pin(this, source, snapshot.version(), snapshot.attemptUuid());
        }

        private synchronized CompletionStage<Void> fence(Source source) {
            if (source == Source.BOOKKEEPER) {
                bookKeeperFenced = true;
                if (bookKeeperPins == 0) {
                    return CompletableFuture.completedFuture(null);
                }
                if (bookKeeperDrain == null) {
                    bookKeeperDrain = new CompletableFuture<>();
                }
                return bookKeeperDrain;
            }
            objectFenced = true;
            if (objectPins == 0) {
                return CompletableFuture.completedFuture(null);
            }
            if (objectDrain == null) {
                objectDrain = new CompletableFuture<>();
            }
            return objectDrain;
        }

        private synchronized CompletionStage<Void> fenceBoth() {
            CompletionStage<Void> bookKeeper = fence(Source.BOOKKEEPER);
            CompletionStage<Void> object = fence(Source.OBJECT);
            return allOf(bookKeeper, object);
        }

        private synchronized boolean unfenceBookKeeper() {
            if (objectFenced || bookKeeperPins != 0) {
                return false;
            }
            bookKeeperFenced = false;
            bookKeeperDrain = null;
            return true;
        }

        private synchronized CompletionStage<Void> release(Source source) {
            if (source == Source.BOOKKEEPER) {
                if (bookKeeperPins <= 0) {
                    return CompletableFuture.failedFuture(new IllegalStateException("BK pin underflow"));
                }
                bookKeeperPins--;
                if (bookKeeperPins == 0 && bookKeeperDrain != null) {
                    bookKeeperDrain.complete(null);
                }
            } else {
                if (objectPins <= 0) {
                    return CompletableFuture.failedFuture(new IllegalStateException("Object pin underflow"));
                }
                objectPins--;
                if (objectPins == 0 && objectDrain != null) {
                    objectDrain.complete(null);
                }
            }
            return CompletableFuture.completedFuture(null);
        }
    }

    private static final class Pin {
        private final PinState owner;
        private final Source source;

        @SuppressWarnings("unused")
        private final long metadataVersion;

        @SuppressWarnings("unused")
        private final UUID attemptUuid;

        private final AtomicBoolean released = new AtomicBoolean();

        private Pin(PinState owner, Source source, long metadataVersion, UUID attemptUuid) {
            this.owner = owner;
            this.source = source;
            this.metadataVersion = metadataVersion;
            this.attemptUuid = attemptUuid;
        }

        private CompletionStage<Void> release() {
            return released.compareAndSet(false, true)
                    ? owner.release(source)
                    : CompletableFuture.completedFuture(null);
        }
    }
}
