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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Completion;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Context;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class M5TargetDeleteMultiWriterGuardV2Test {
    private static final Context CONTEXT = new Context(
            ProofBoundWriterClassV1.MANIFEST_SELECTOR_GENERATION_REPRESENTATION_V1,
            SyntheticDeleteAuthorityFixturesV2.digest("cap"),
            SyntheticDeleteAuthorityFixturesV2.digest("owner"),
            SyntheticDeleteAuthorityFixturesV2.digest("exact descriptor"));
    private static final com.nereusstream.domain.bytes.Sha256Digest TERMINAL =
            SyntheticDeleteAuthorityFixturesV2.digest("native terminal fixture");

    @Test
    void deduplicatesAndOrdersEveryTargetBeforeDispatchAndClearsOnlyAfterTerminal() {
        var fixture = new Fixture();
        var resources = fixture.resources(3);
        var hold = new CompletableFuture<Completion<String>>();
        var result = fixture.guard
                .execute(
                        List.of(resources.get(2), resources.get(0), resources.get(1), resources.get(0)),
                        CONTEXT,
                        () -> {
                            assertThat(fixture.store.casKeys)
                                    .containsExactlyElementsOf(resources.stream()
                                            .map(PhysicalResourceIdV2::authorityKey)
                                            .toList());
                            resources.forEach(resource ->
                                    assertThat(fixture.tickets(resource)).hasSize(1));
                            return hold;
                        })
                .toCompletableFuture();
        assertThat(result).isNotDone();
        hold.complete(new Completion<>("selected", Optional.of(TERMINAL)));
        assertThat(result.join().value()).contains("selected");
        assertThat(result.join().unresolvedTargets()).isEmpty();
        resources.forEach(resource -> assertThat(fixture.tickets(resource)).isEmpty());
    }

    @Test
    void laterFencePreventsAllDispatchAndRollsBackOnlyThisInvocationsTickets() {
        var fixture = new Fixture();
        var resources = fixture.resources(3);
        fixture.seed(resources.get(2), 1);
        var result = fixture.guard
                .execute(resources, CONTEXT, () -> {
                    throw new AssertionError("must not dispatch");
                })
                .toCompletableFuture()
                .join();
        assertThat(result.mutationInvoked()).isFalse();
        assertThat(result.unresolvedTargets()).isEmpty();
        assertThat(fixture.tickets(resources.get(0))).isEmpty();
        assertThat(fixture.tickets(resources.get(1))).isEmpty();
    }

    @Test
    void unknownExternalResponseRetainsEveryTicketUntilExplicitTerminalReconciliation() {
        var fixture = new Fixture();
        var resources = fixture.resources(2);
        var result = fixture.guard
                .execute(
                        resources,
                        CONTEXT,
                        () -> CompletableFuture.completedFuture(new Completion<>("unknown", Optional.empty())))
                .toCompletableFuture()
                .join();
        assertThat(result.unresolvedTargets()).containsExactlyElementsOf(resources);
        resources.forEach(resource -> assertThat(fixture.tickets(resource)).hasSize(1));
        var restarted = new M5TargetDeleteMultiWriterGuardV2(new M5TargetDeleteAuthorityCoordinatorV1(fixture.store));
        assertThat(restarted
                        .reconcileTerminal(resources, CONTEXT, TERMINAL)
                        .toCompletableFuture()
                        .join())
                .isEmpty();
        resources.forEach(resource -> assertThat(fixture.tickets(resource)).isEmpty());
    }

    @Test
    void terminalRecoveryLeavesUnscannedTargetsForAnotherBoundedPass() {
        var fixture = new Fixture();
        var resources = fixture.resources(M5TargetDeleteMultiWriterGuardV2.MAX_RECOVERY_TICKETS + 2);
        fixture.guard
                .execute(
                        resources,
                        CONTEXT,
                        () -> CompletableFuture.completedFuture(new Completion<>("unknown", Optional.empty())))
                .toCompletableFuture()
                .join();
        var remaining = fixture.guard
                .reconcileTerminal(resources, CONTEXT, TERMINAL)
                .toCompletableFuture()
                .join();
        assertThat(remaining).containsExactlyElementsOf(resources.subList(256, 258));
        resources.subList(0, 256).forEach(resource -> assertThat(fixture.tickets(resource))
                .isEmpty());
        remaining.forEach(resource -> assertThat(fixture.tickets(resource)).hasSize(1));
        assertThat(fixture.guard
                        .reconcileTerminal(remaining, CONTEXT, TERMINAL)
                        .toCompletableFuture()
                        .join())
                .isEmpty();
        remaining.forEach(resource -> assertThat(fixture.tickets(resource)).isEmpty());
    }

    @Test
    void concurrentLogicalRetriesHaveDistinctTicketsAndOneCompletionCannotReleaseTheOther() {
        var fixture = new Fixture();
        var resources = fixture.resources(1);
        var first = new CompletableFuture<Completion<String>>();
        var second = new CompletableFuture<Completion<String>>();
        var a = fixture.guard.execute(resources, CONTEXT, () -> first).toCompletableFuture();
        var b = fixture.guard.execute(resources, CONTEXT, () -> second).toCompletableFuture();
        assertThat(fixture.tickets(resources.get(0))).hasSize(2);
        first.complete(new Completion<>("first", Optional.of(TERMINAL)));
        assertThat(fixture.tickets(resources.get(0))).hasSize(1);
        second.complete(new Completion<>("unknown second", Optional.empty()));
        assertThat(a.join().operationId()).isNotEqualTo(b.join().operationId());
        assertThat(b.join().unresolvedTargets()).containsExactlyElementsOf(resources);
    }

    @Test
    void observerCancellationKeepsNativeCompletionAndCleanupAlive() {
        var fixture = new Fixture();
        var resources = fixture.resources(1);
        var external = new CompletableFuture<Completion<String>>();
        var observer = fixture.guard.execute(resources, CONTEXT, () -> external).toCompletableFuture();
        assertThat(observer.cancel(false)).isTrue();
        assertThat(external).isNotCancelled();
        assertThat(fixture.tickets(resources.get(0))).hasSize(1);
        external.complete(new Completion<>("actual terminal", Optional.of(TERMINAL)));
        assertThat(fixture.tickets(resources.get(0))).isEmpty();
    }

    @Test
    void appliedAcquisitionResponseLossUsesTheExactDurableTicket() {
        var fixture = new Fixture();
        var resources = fixture.resources(2);
        fixture.store.loseApplied = true;
        var result = fixture.guard
                .execute(resources, CONTEXT, () -> {
                    resources.forEach(
                            resource -> assertThat(fixture.tickets(resource)).hasSize(1));
                    return CompletableFuture.completedFuture(new Completion<>("done", Optional.of(TERMINAL)));
                })
                .toCompletableFuture()
                .join();
        assertThat(result.mutationInvoked()).isTrue();
        assertThat(result.unresolvedTargets()).isEmpty();
    }

    @Test
    void unresolvedAcquisitionCanArriveLateButNeverInvokesTheAbandonedWriter() {
        var fixture = new Fixture();
        var resources = fixture.resources(2);
        fixture.store.holdKey = resources.get(1).authorityKey();
        var result = fixture.guard
                .execute(resources, CONTEXT, () -> {
                    throw new AssertionError("abandoned writer");
                })
                .toCompletableFuture()
                .join();
        assertThat(result.mutationInvoked()).isFalse();
        assertThat(result.unresolvedTargets()).containsExactly(resources.get(1));
        assertThat(fixture.tickets(resources.get(0))).isEmpty();
        fixture.store.late.run();
        assertThat(fixture.tickets(resources.get(1))).hasSize(1);
        assertThat(fixture.guard
                        .reconcileTerminal(resources, CONTEXT, TERMINAL)
                        .toCompletableFuture()
                        .join())
                .isEmpty();
    }

    @Test
    void missingPermanentAuthorityAndTargetBoundsFailBeforeWriterInvocation() {
        var fixture = new Fixture();
        var absent = SyntheticDeleteAuthorityFixturesV2.resource(301);
        assertThat(fixture.guard
                        .execute(List.of(absent), CONTEXT, () -> {
                            throw new AssertionError("missing target");
                        })
                        .toCompletableFuture()
                        .join()
                        .mutationInvoked())
                .isFalse();
        assertThatThrownBy(() -> fixture.guard.execute(List.of(), CONTEXT, () -> null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() ->
                        M5TargetDeleteMultiWriterGuardV2.canonicalTargets(java.util.Collections.nCopies(2049, absent)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(fixture.store.casKeys).isEmpty();
    }

    private static final class Fixture {
        final Store store = new Store();
        final M5TargetDeleteMultiWriterGuardV2 guard =
                new M5TargetDeleteMultiWriterGuardV2(new M5TargetDeleteAuthorityCoordinatorV1(store));

        List<PhysicalResourceIdV2> resources(int count) {
            var values = new ArrayList<PhysicalResourceIdV2>();
            for (int i = 0; i < count; i++) {
                var resource = SyntheticDeleteAuthorityFixturesV2.resource(i + 1);
                seed(resource, 0);
                values.add(resource);
            }
            return M5TargetDeleteMultiWriterGuardV2.canonicalTargets(values);
        }

        void seed(PhysicalResourceIdV2 resource, int phase) {
            store.put(
                    resource.authorityKey(),
                    M5TargetDeleteAuthorityCodecV1.encodeAuthority(
                            SyntheticDeleteAuthorityFixturesV2.phases(resource).get(phase)));
        }

        List<M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterTicketV1> tickets(PhysicalResourceIdV2 resource) {
            return M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                            store.values.get(resource.authorityKey()).canonicalStoredBytes())
                    .activeWriterTickets();
        }
    }

    private static final class Store implements ExactMetadataTransactionStoreV1 {
        final Map<String, VersionedValue> values = new HashMap<>();
        final List<String> casKeys = new ArrayList<>();
        long version;
        boolean loseApplied;
        String holdKey;
        Runnable late;

        void put(String key, CanonicalBytes bytes) {
            values.put(
                    key,
                    VersionedValue.of(
                            key,
                            bytes,
                            new MetadataVersion(CanonicalBytes.copyOf(
                                    ByteBuffer.allocate(8).putLong(++version).array()))));
        }

        public CompletionStage<Optional<VersionedValue>> read(String key) {
            return CompletableFuture.completedFuture(Optional.ofNullable(values.get(key)));
        }

        public CompletionStage<MutationOutcome> compareAndSet(
                Optional<VersionedValue> expected, String key, CanonicalBytes bytes) {
            casKeys.add(key);
            if (!Optional.ofNullable(values.get(key)).equals(expected)) {
                return CompletableFuture.completedFuture(MutationOutcome.DEFINITIVE_CONFLICT);
            }
            if (key.equals(holdKey)) {
                holdKey = null;
                late = () -> {
                    if (Optional.ofNullable(values.get(key)).equals(expected)) {
                        put(key, bytes);
                    }
                };
                return CompletableFuture.completedFuture(MutationOutcome.RESPONSE_UNKNOWN);
            }
            put(key, bytes);
            boolean lost = loseApplied;
            loseApplied = false;
            return CompletableFuture.completedFuture(
                    lost ? MutationOutcome.RESPONSE_UNKNOWN : MutationOutcome.APPLIED_EXACT);
        }

        public CompletionStage<TransactionOutcome> conditionalTransaction(ExactTransaction ignored) {
            throw new AssertionError("no multi-key transaction");
        }

        public boolean supportsAtomicMultiKeyTransactions() {
            return false;
        }
    }
}
