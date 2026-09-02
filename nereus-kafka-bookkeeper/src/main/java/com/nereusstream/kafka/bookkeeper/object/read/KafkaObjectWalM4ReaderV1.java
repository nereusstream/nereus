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

package com.nereusstream.kafka.bookkeeper.object.read;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectCoherentProtocolSnapshotV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectExtentLocatorV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectSourceProtectionTrackerV1;
import com.nereusstream.kafka.bookkeeper.object.read.KafkaObjectBindingReadAdapterV1.PhysicalRoute;
import com.nereusstream.kafka.bookkeeper.object.read.KafkaObjectBindingReadAdapterV1.ReadCell;
import com.nereusstream.storage.object.read.BindingReadAsyncExecutorV1;
import com.nereusstream.storage.object.read.BindingReadAuthorityV1;
import com.nereusstream.storage.object.read.BindingReadHazardPoolV1;
import com.nereusstream.storage.object.read.BindingReadHazardPoolV1.ScanOutcome;
import com.nereusstream.storage.object.read.BindingReadPlanBufferV1;
import com.nereusstream.storage.object.read.BindingReadPlannerV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/** Current-source Kafka Object-WAL range reader under one captured M4 generation authority. */
public final class KafkaObjectWalM4ReaderV1 {
    @FunctionalInterface
    public interface LocatorPin {
        AutoCloseable pin(KafkaObjectExtentLocatorV1 locator);
    }

    @FunctionalInterface
    public interface ExtentReader {
        CompletionStage<ValidatedRange> read(
                KafkaObjectExtentLocatorV1 locator, long startOffset, long endOffsetExclusive);
    }

    public record ValidatedRange(
            KafkaObjectExtentLocatorV1 locator,
            long startOffset,
            long endOffsetExclusive,
            CanonicalBytes kafkaRecordBatchBytes) {
        public ValidatedRange {
            Objects.requireNonNull(locator, "locator");
            Objects.requireNonNull(kafkaRecordBatchBytes, "kafkaRecordBatchBytes");
            if (startOffset < locator.startOffset()
                    || endOffsetExclusive > locator.endOffsetExclusive()
                    || endOffsetExclusive <= startOffset
                    || kafkaRecordBatchBytes.length() == 0) {
                throw new IllegalArgumentException("validated Kafka range differs from its exact M3 locator");
            }
        }
    }

    public record ReadResult(
            long requestedStartOffset,
            long requestedEndOffsetExclusive,
            long capturedSourceGeneration,
            List<ValidatedRange> ranges) {
        public ReadResult {
            ranges = List.copyOf(Objects.requireNonNull(ranges, "ranges"));
            if (requestedStartOffset < 0
                    || requestedEndOffsetExclusive <= requestedStartOffset
                    || capturedSourceGeneration <= 0
                    || ranges.isEmpty()) {
                throw new IllegalArgumentException("Kafka M4 read result is outside its exact range/generation");
            }
            long cursor = requestedStartOffset;
            for (ValidatedRange range : ranges) {
                if (range.startOffset() != cursor) {
                    throw new IllegalArgumentException("Kafka M4 read result contains a gap or reordered range");
                }
                cursor = range.endOffsetExclusive();
            }
            if (cursor != requestedEndOffsetExclusive) {
                throw new IllegalArgumentException("Kafka M4 read result does not cover the complete request");
            }
        }
    }

    private static final int MAX_PLAN_INTERVALS = 256;

    private final LocatorPin locatorPin;
    private final ExtentReader extentReader;
    private final BindingReadHazardPoolV1 hazardPool;
    private final BindingReadAsyncExecutorV1 asyncExecutor;
    private final AtomicReference<BindingReadAuthorityV1> current = new AtomicReference<>();

    public KafkaObjectWalM4ReaderV1(
            KafkaObjectCoherentProtocolSnapshotV1 initialSnapshot,
            BindingReadSelector initialSelector,
            LocatorPin locatorPin,
            ExtentReader extentReader,
            BindingReadHazardPoolV1 hazardPool,
            Executor ownerEventLoop) {
        this.locatorPin = Objects.requireNonNull(locatorPin, "locatorPin");
        this.extentReader = Objects.requireNonNull(extentReader, "extentReader");
        this.hazardPool = Objects.requireNonNull(hazardPool, "hazardPool");
        asyncExecutor = new BindingReadAsyncExecutorV1(ownerEventLoop);
        refresh(initialSnapshot, initialSelector);
    }

    public KafkaObjectWalM4ReaderV1(
            KafkaObjectCoherentProtocolSnapshotV1 initialSnapshot,
            BindingReadSelector initialSelector,
            KafkaObjectSourceProtectionTrackerV1 sourceProtection,
            ExtentReader extentReader,
            BindingReadHazardPoolV1 hazardPool,
            Executor ownerEventLoop) {
        this(
                initialSnapshot,
                initialSelector,
                Objects.requireNonNull(sourceProtection, "sourceProtection")::pin,
                extentReader,
                hazardPool,
                ownerEventLoop);
        sourceProtection.registerM4RetirementGuard(
                () -> hazardPool.scanBinding(initialSnapshot.root().fence().bindingId()) == ScanOutcome.CLEAN);
    }

    /** Atomically publishes one later M3 coherent root after its root CAS has succeeded. */
    public BindingReadAuthorityV1 refresh(
            KafkaObjectCoherentProtocolSnapshotV1 snapshot, BindingReadSelector selector) {
        BindingReadAuthorityV1 authority = KafkaObjectBindingReadAdapterV1.publish(snapshot, selector);
        current.set(authority);
        return authority;
    }

    public AtomicReference<BindingReadAuthorityV1> currentAuthority() {
        return current;
    }

    public CompletableFuture<ReadResult> read(
            long startOffset, long endOffsetExclusive, long protocolUpperBoundExclusive) {
        return asyncExecutor.execute(current, hazardPool, authority -> {
            if (!(authority.publicationCell().protocolStateReference() instanceof ReadCell cell)) {
                throw new IllegalStateException("captured Kafka authority lacks its M3 current-source cell");
            }
            BindingReadPlanBufferV1 plan = new BindingReadPlanBufferV1(MAX_PLAN_INTERVALS);
            BindingReadPlannerV1.Outcome outcome = BindingReadPlannerV1.plan(
                    authority.publicationCell(), startOffset, endOffsetExclusive, protocolUpperBoundExclusive, plan);
            if (outcome != BindingReadPlannerV1.Outcome.PLANNED) {
                throw new IllegalStateException("captured Kafka source plan failed closed: " + outcome);
            }
            return executePlan(
                    cell,
                    plan,
                    authority.sourceGeneration(),
                    startOffset,
                    Math.min(endOffsetExclusive, protocolUpperBoundExclusive));
        });
    }

    private CompletionStage<ReadResult> executePlan(
            ReadCell cell,
            BindingReadPlanBufferV1 plan,
            long sourceGeneration,
            long requestedStart,
            long requestedEnd) {
        CompletableFuture<List<ValidatedRange>> sequence =
                CompletableFuture.completedFuture(new ArrayList<>(plan.size()));
        for (int index = 0; index < plan.size(); index++) {
            PhysicalRoute physical = cell.requirePhysical(plan.route(index));
            long start = plan.startInclusive(index);
            long end = plan.endExclusive(index);
            sequence = sequence.thenCompose(
                    results -> readOne(physical.locator(), start, end).thenApply(validated -> {
                        results.add(validated);
                        return results;
                    }));
        }
        return sequence.thenApply(ranges -> new ReadResult(requestedStart, requestedEnd, sourceGeneration, ranges));
    }

    private CompletionStage<ValidatedRange> readOne(KafkaObjectExtentLocatorV1 locator, long start, long end) {
        final AutoCloseable pin;
        try {
            pin = Objects.requireNonNull(locatorPin.pin(locator), "Kafka active-locator pin");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        final CompletionStage<ValidatedRange> read;
        try {
            read = Objects.requireNonNull(extentReader.read(locator, start, end), "Kafka extent read stage");
        } catch (Throwable failure) {
            closePin(pin, failure);
            return CompletableFuture.failedFuture(failure);
        }
        CompletableFuture<ValidatedRange> result = new CompletableFuture<>();
        read.whenComplete((value, failure) -> {
            Throwable terminal = failure;
            try {
                if (failure == null
                        && (!value.locator().equals(locator)
                                || value.startOffset() != start
                                || value.endOffsetExclusive() != end)) {
                    terminal = new IllegalStateException("Kafka reader substituted its captured locator/range");
                }
                pin.close();
            } catch (Throwable closeFailure) {
                if (terminal == null) {
                    terminal = closeFailure;
                } else {
                    terminal.addSuppressed(closeFailure);
                }
            }
            if (terminal == null) {
                result.complete(value);
            } else {
                result.completeExceptionally(terminal);
            }
        });
        return result;
    }

    private static void closePin(AutoCloseable pin, Throwable primary) {
        try {
            pin.close();
        } catch (Throwable closeFailure) {
            primary.addSuppressed(closeFailure);
        }
    }
}
