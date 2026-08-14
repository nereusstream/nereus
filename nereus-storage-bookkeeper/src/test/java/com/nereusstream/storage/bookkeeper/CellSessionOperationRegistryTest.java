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

package com.nereusstream.storage.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import org.junit.jupiter.api.Test;

class CellSessionOperationRegistryTest {
    @Test
    void acceptedAppendOwnsAnIndependentReferenceUntilTerminalReconciliation() {
        CellSessionOperationRegistry registry = new CellSessionOperationRegistry();
        ImmutableRetainedStoragePayload payload = ImmutableRetainedStoragePayload.copyOf(new byte[] {1, 2, 3});

        CellSessionOperationRegistry.OperationLease lease = registry.acceptAppend(payload);
        assertThat(payload.referenceCount()).isEqualTo(2);
        assertThat(registry.acceptedOperations()).isEqualTo(1);
        assertThat(registry.retainedPayloadBytes()).isEqualTo(3);

        assertThat(payload.release()).isFalse();
        assertThat(payload.referenceCount()).isEqualTo(1);
        assertThat(lease.resolveTerminal()).isTrue();
        assertThat(payload.referenceCount()).isZero();
        assertThat(registry.acceptedOperations()).isZero();
        assertThat(registry.retainedPayloadBytes()).isZero();
    }

    @Test
    void drainWaitsForEveryAcceptedOperationAndResolver() {
        CellSessionOperationRegistry registry = new CellSessionOperationRegistry();
        CellSessionOperationRegistry.OperationLease first = registry.acceptOperation();
        CellSessionOperationRegistry.OperationLease unknownOutcomeResolver = registry.acceptOperation();

        CompletableFuture<Void> drained = registry.drain().toCompletableFuture();
        assertThat(drained).isNotDone();
        first.resolveTerminal();
        assertThat(drained).isNotDone();
        unknownOutcomeResolver.resolveTerminal();
        assertThat(drained).isCompletedWithValue(null);
        assertThat(registry.state()).isEqualTo(CellSessionOperationRegistry.State.DRAINING);
    }

    @Test
    void observerCancellationDoesNotResolveAnAcceptedOperation() {
        CellSessionOperationRegistry registry = new CellSessionOperationRegistry();
        CellSessionOperationRegistry.OperationLease lease = registry.acceptOperation();
        CompletableFuture<Void> observer = new CompletableFuture<>();

        assertThat(observer.cancel(false)).isTrue();
        CompletionStage<Void> drained = registry.drain();
        assertThat(drained.toCompletableFuture()).isNotDone();
        assertThat(lease.isTerminal()).isFalse();

        lease.resolveTerminal();
        assertThat(drained.toCompletableFuture()).isDone();
    }

    @Test
    void admissionAfterDrainFailsBeforeRetainingCallerPayload() {
        CellSessionOperationRegistry registry = new CellSessionOperationRegistry();
        ImmutableRetainedStoragePayload payload = ImmutableRetainedStoragePayload.copyOf(new byte[] {1});
        registry.drain();

        assertThatThrownBy(() -> registry.acceptAppend(payload)).isInstanceOf(RejectedExecutionException.class);
        assertThat(payload.referenceCount()).isEqualTo(1);
        assertThat(payload.release()).isTrue();
    }

    @Test
    void closeCompletesOnlyAfterTerminalRelease() {
        CellSessionOperationRegistry registry = new CellSessionOperationRegistry();
        ImmutableRetainedStoragePayload payload = ImmutableRetainedStoragePayload.copyOf(new byte[] {7, 8});
        CellSessionOperationRegistry.OperationLease lease = registry.acceptAppend(payload);
        payload.release();

        CompletableFuture<Void> closed = registry.closeAsync().toCompletableFuture();
        assertThat(closed).isNotDone();
        assertThat(registry.state()).isEqualTo(CellSessionOperationRegistry.State.DRAINING);
        lease.resolveTerminal();
        assertThat(closed).isCompletedWithValue(null);
        assertThat(registry.state()).isEqualTo(CellSessionOperationRegistry.State.CLOSED);
        assertThat(payload.referenceCount()).isZero();
    }

    @Test
    void closingOneRegistryCannotCloseAnotherCellRegistry() {
        CellSessionOperationRegistry firstCell = new CellSessionOperationRegistry();
        CellSessionOperationRegistry secondCell = new CellSessionOperationRegistry();

        assertThat(firstCell.closeAsync().toCompletableFuture()).isCompletedWithValue(null);
        CellSessionOperationRegistry.OperationLease secondLease = secondCell.acceptOperation();

        assertThat(firstCell.state()).isEqualTo(CellSessionOperationRegistry.State.CLOSED);
        assertThat(secondCell.state()).isEqualTo(CellSessionOperationRegistry.State.OPEN);
        assertThat(secondLease.resolveTerminal()).isTrue();
    }

    @Test
    void terminalResolutionIsExactlyOnce() {
        CellSessionOperationRegistry registry = new CellSessionOperationRegistry();
        CellSessionOperationRegistry.OperationLease lease = registry.acceptOperation();

        assertThat(lease.resolveTerminal()).isTrue();
        assertThat(lease.resolveTerminal()).isFalse();
        assertThat(registry.acceptedOperations()).isZero();
    }

    @Test
    void immutablePayloadRejectsMutationThroughItsReadViewAndUseAfterRelease() {
        byte[] source = new byte[] {1, 2, 3};
        ImmutableRetainedStoragePayload payload = ImmutableRetainedStoragePayload.copyOf(source);
        source[0] = 99;

        assertThat(payload.readOnlyBuffer().get(0)).isEqualTo((byte) 1);
        assertThatThrownBy(() -> payload.readOnlyBuffer().put(0, (byte) 4))
                .isInstanceOf(java.nio.ReadOnlyBufferException.class);
        assertThat(payload.release()).isTrue();
        assertThatThrownBy(payload::readOnlyBuffer).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(payload::retain).isInstanceOf(IllegalStateException.class);
    }
}
