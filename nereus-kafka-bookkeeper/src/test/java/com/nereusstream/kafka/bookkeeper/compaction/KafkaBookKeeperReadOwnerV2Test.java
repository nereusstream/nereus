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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.RunLedgerReadResultV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.CaptureOutcome;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.CaptureResult;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2;
import com.nereusstream.storage.object.gc.SyntheticDeleteAuthorityFixturesV2;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/** Synthetic native/physical admission fixtures; the local lifecycle is exercised through actual M4 and guard code. */
class KafkaBookKeeperReadOwnerV2Test {
    @Test
    void closureKeepsAllTicketsThroughCancelledReadAndActualSessionTermination() {
        var f = new Fixture();
        var read = new AtomicReference<CompletableFuture<KafkaBookKeeperM4RecoveryV2.RecoveryResult>>();
        var close = new AtomicReference<CompletionStage<KafkaBookKeeperReadOwnerV2.DrainEvidence>>();
        var owner = new AtomicReference<KafkaBookKeeperReadOwnerV2>();
        var result = f.run(value -> {
                    owner.set(value);
                    read.set(value.recover());
                    close.set(value.closeFallbackAndDrain(f.physical.sources));
                    return close.get();
                })
                .toCompletableFuture();
        assertThat(result).isNotDone();
        f.assertTickets(1);
        assertThat(read.get().cancel(true)).isTrue();
        assertThat(f.pending).isNotCancelled();
        assertThat(f.closes).isZero();
        assertThatThrownBy(() -> owner.get().recover().join()).hasRootCauseMessage("BK read owner admission is closed");
        f.pending.complete(f.pendingValue);
        assertThat(f.closes).isEqualTo(1);
        assertThat(result).isNotDone();
        f.assertTickets(1);
        f.closed.complete(null);
        var evidence = result.join();
        assertThat(evidence.anchor().closedReadAdmissionEpoch())
                .isEqualTo(evidence.predecessor().readAdmissionEpoch());
        assertThat(evidence.successor().readAdmissionEpoch())
                .isEqualTo(evidence.predecessor().readAdmissionEpoch() + 1);
        f.assertTickets(0);
        assertThatThrownBy(() -> owner.get()
                        .closeFallbackAndDrain(f.physical.sources)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("BK read owner lifetime has ended");
    }

    @Test
    void unknownClosureDoesNotReturnEvidenceAndExactRetryWaitsForTheOldRead() {
        var f = new Fixture();
        f.physical.store.dropNextSelectorCas = true;
        var result = f.run(value -> {
                    value.recover();
                    assertThatThrownBy(() -> value.closeFallbackAndDrain(f.physical.sources)
                                    .toCompletableFuture()
                                    .join())
                            .hasRootCauseMessage("BK read owner closure is unresolved: RETRY_EXACT_PREDECESSOR");
                    assertThatThrownBy(() -> value.recover().join())
                            .hasRootCauseMessage("BK read owner admission is closed");
                    return value.closeFallbackAndDrain(f.physical.sources);
                })
                .toCompletableFuture();
        assertThat(result).isNotDone();
        assertThat(f.closes).isZero();
        f.pending.complete(f.pendingValue);
        f.closed.complete(null);
        assertThat(result.join().successor().pendingAnchors()).hasSize(1);
        f.assertTickets(0);
    }

    @Test
    void unknownSessionCloseRetainsEveryPhysicalTicketAndProducesNoDrain() {
        var f = new Fixture();
        var result = f.run(value -> {
                    value.recover();
                    return value.closeFallbackAndDrain(f.physical.sources);
                })
                .toCompletableFuture();
        f.pending.complete(f.pendingValue);
        f.closed.completeExceptionally(new IllegalStateException("native close unknown"));
        assertThatThrownBy(result::join).hasRootCauseMessage("BK read owner terminal or tickets unresolved");
        f.assertTickets(1);
        assertThat(f.closes).isEqualTo(1);
    }

    @Test
    void cancelledLifetimeStillDrainsAndMissingAuthorityNeverCreatesSession() {
        var f = new Fixture();
        var lifetime = new CompletableFuture<String>();
        var result = f.run(value -> {
                    value.recover();
                    return lifetime;
                })
                .toCompletableFuture();
        lifetime.cancel(false);
        assertThat(f.closes).isZero();
        assertThat(result).isNotDone();
        f.assertTickets(1);
        f.pending.complete(f.pendingValue);
        assertThat(result).isNotDone();
        f.closed.complete(null);
        assertThat(result).isCompletedExceptionally();
        f.assertTickets(0);
        var absent = new Fixture();
        absent.store.values.clear();
        assertThatThrownBy(() -> absent.run(value -> value.recover())
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("BK read owner physical admission failed");
        assertThat(absent.sessions).isZero();
    }

    @Test
    void cancellingOuterObserverCannotAbandonSessionCloseOrTicketRelease() {
        var f = new Fixture();
        var result = f.run(value -> {
                    value.recover();
                    return value.closeFallbackAndDrain(f.physical.sources);
                })
                .toCompletableFuture();
        assertThat(result.cancel(true)).isTrue();
        f.assertTickets(1);
        assertThat(f.closes).isZero();
        f.pending.complete(f.pendingValue);
        assertThat(f.closes).isEqualTo(1);
        f.assertTickets(1);
        f.closed.complete(null);
        f.assertTickets(0);
        assertThat(result).isCancelled();
    }

    private static final class Fixture {
        final KafkaSealedBookKeeperDescriptorV2Test.Fixture physical =
                new KafkaSealedBookKeeperDescriptorV2Test.Fixture(false);
        final Store store = new Store();
        final M5TargetDeleteMultiWriterGuardV2 guard =
                new M5TargetDeleteMultiWriterGuardV2(new M5TargetDeleteAuthorityCoordinatorV1(store));
        final CompletableFuture<RunLedgerReadResultV1> pending = new CompletableFuture<>();
        final CompletableFuture<Void> closed = new CompletableFuture<>();
        final List<PhysicalResourceIdV2> resources;
        RunLedgerReadResultV1 pendingValue;
        int sessions;
        int closes;
        int reads;

        Fixture() {
            physical.publish();
            resources = physical.descriptor.sealedParts().stream()
                    .map(part -> (PhysicalResourceIdV2) new PhysicalResourceIdV2.BookKeeperLedger(
                            physical.descriptor.task().namespace(),
                            part.handle().ledgerIdentity().ledgerId()))
                    .toList();
            for (var resource : resources) {
                store.put(
                        resource.authorityKey(),
                        M5TargetDeleteAuthorityCodecV1.encodeAuthority(
                                SyntheticDeleteAuthorityFixturesV2.phases(resource)
                                        .get(0)));
            }
        }

        <T> CompletionStage<T> run(Function<KafkaBookKeeperReadOwnerV2, CompletionStage<T>> work) {
            return KafkaBookKeeperReadOwnerV2.run(
                    physical.descriptor,
                    physical.m4,
                    guard,
                    () -> {
                        sessions++;
                        assertTickets(1);
                        return (BookKeeperCellSession) Proxy.newProxyInstance(
                                BookKeeperCellSession.class.getClassLoader(),
                                new Class<?>[] {BookKeeperCellSession.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("closeAsync")) {
                                        closes++;
                                        return closed;
                                    }
                                    Object result;
                                    try {
                                        result = method.invoke(physical.session, args);
                                    } catch (InvocationTargetException failure) {
                                        throw failure.getCause();
                                    }
                                    if (method.getName().equals("readExactEntry") && reads++ == 0) {
                                        pendingValue = (RunLedgerReadResultV1) ((CompletionStage<?>) result)
                                                .toCompletableFuture()
                                                .join();
                                        return pending;
                                    }
                                    return result;
                                });
                    },
                    handle -> CompletableFuture.completedFuture(new CaptureResult(
                            CaptureOutcome.EXACT_TARGET,
                            Optional.of(
                                    physical.seals.get(handle.ledgerIdentity().ledgerId())))),
                    Runnable::run,
                    new KafkaBookKeeperSelectedSourceV2.Bounds(128, 32, 1000, 1000000, 500000),
                    2,
                    work);
        }

        void assertTickets(int count) {
            for (var resource : resources) {
                assertThat(M5TargetDeleteAuthorityCodecV1.decodeAuthority(store.values
                                        .get(resource.authorityKey())
                                        .canonicalStoredBytes())
                                .activeWriterTickets())
                        .hasSize(count);
            }
        }
    }

    private static final class Store implements ExactMetadataTransactionStoreV1 {
        final Map<String, VersionedValue> values = new HashMap<>();
        long version;

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
            if (!Optional.ofNullable(values.get(key)).equals(expected)) {
                return CompletableFuture.completedFuture(MutationOutcome.DEFINITIVE_CONFLICT);
            }
            put(key, bytes);
            return CompletableFuture.completedFuture(MutationOutcome.APPLIED_EXACT);
        }

        public CompletionStage<TransactionOutcome> conditionalTransaction(ExactTransaction ignored) {
            throw new AssertionError("no multi-key transaction");
        }

        public boolean supportsAtomicMultiKeyTransactions() {
            return false;
        }
    }
}
