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

import static com.nereusstream.storage.bookkeeper.M5BookKeeperNativeDeleteV2RealTest.await;
import static com.nereusstream.storage.bookkeeper.M5BookKeeperNativeDeleteV2RealTest.digest;
import static com.nereusstream.storage.bookkeeper.M5BookKeeperNativeDeleteV2RealTest.sealed;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.DeleteOutcome;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Actual bound native dispatch lifecycle; intent token/authority digests remain synthetic fixture inputs. */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class M5BookKeeperDeleteCellBudgetV2RealTest {
    @Test
    void cancelledObserverRetainsInflightCapacityAcrossReconnectAndCellsAreIndependent() throws Exception {
        var a = capability("cell-lifetime-a");
        var b = capability("cell-lifetime-b");
        var specA = spec(a);
        var specB = spec(b);
        try (var backendA = M5BookKeeperNamespaceAuthorityV2.connect(uri(), a);
                var backendB = M5BookKeeperNamespaceAuthorityV2.connect(uri(), b)) {
            var binding = await(backendA.readBinding()).orElseThrow();
            var quota = backendA.nativeDeleteQuota();
            var previous = await(quota.snapshot());
            await(quota.expand(quota.capacityForResources(previous.reservedResources() + 3)));
            var budgetA = backendA.nativeDeleteCellBudget();
            var budgetB = backendB.nativeDeleteCellBudget();
            await(budgetA.initialize(new M5BookKeeperDeleteCellBudgetV2.Limits(1, 1)));
            await(budgetB.initialize(new M5BookKeeperDeleteCellBudgetV2.Limits(1, 1)));
            assertThatThrownBy(() -> await(budgetA.initialize(new M5BookKeeperDeleteCellBudgetV2.Limits(2, 2))))
                    .hasRootCauseMessage("native Cell delete limits differ");
            try (var clientA = M5BookKeeperNativeCreateClientV2.connect(uri(), a, specA, binding);
                    var clientB = M5BookKeeperNativeCreateClientV2.connect(uri(), b, specB, binding)) {
                var first = sealed(clientA, specA);
                var second = sealed(clientA, specA);
                var other = sealed(clientB, specB);
                await(clientA.fenceCreates());
                await(clientB.fenceCreates());
                var target =
                        await(clientA.captureExactTarget(first)).exactTarget().orElseThrow();
                var target2 =
                        await(clientA.captureExactTarget(second)).exactTarget().orElseThrow();
                var targetB =
                        await(clientB.captureExactTarget(other)).exactTarget().orElseThrow();
                var gate = new CompletableFuture<Void>();
                var ready = new CompletableFuture<Void>();
                var authority = clientA.deleteAuthority(first, () -> {
                    ready.complete(null);
                    return gate;
                });
                var authority2 = clientA.deleteAuthority(second);
                var authorityB = clientB.deleteAuthority(other);
                var intent = bind(authority, target.metadataSha256());
                var intent2 = bind(authority2, target2.metadataSha256());
                var intentB = bind(authorityB, targetB.metadataSha256());
                var observer = authority.deleteExact(budgetA, intent, target).toCompletableFuture();
                await(ready);
                assertThat(observer.cancel(true)).isTrue();
                var held = await(budgetA.snapshot());
                assertThat(held.reservations()).hasSize(1);
                assertThat(held.reservations().get(0).terminalUnknown()).isFalse();
                try (var reconnected = M5BookKeeperNamespaceAuthorityV2.connect(uri(), a)) {
                    assertThat(await(reconnected.nativeDeleteCellBudget().snapshot()))
                            .isEqualTo(held);
                }
                assertThatThrownBy(() -> await(authority.reconcileCellDelete(budgetA, intent, target)))
                        .hasRootCauseMessage("native Cell reservation is in flight or belongs to another intent");
                assertThatThrownBy(() -> await(authority.deleteExact(budgetA, intent, target)))
                        .hasRootCauseMessage("native delete already has a Cell reservation");
                assertThatThrownBy(() -> await(authority2.deleteExact(budgetA, intent2, target2)))
                        .hasRootCauseMessage("native Cell dispatch or unknown capacity exhausted");
                assertThatThrownBy(() -> await(authority2.deleteExact(budgetB, intent2, target2)))
                        .hasRootCauseMessage("native delete budget belongs to another Cell");
                assertThat(await(clientA.captureExactTarget(second)).exactTarget())
                        .contains(target2);
                var otherResult = await(authorityB.deleteExact(budgetB, intentB, targetB));
                assertThat(otherResult.deleteResult().outcome()).isEqualTo(DeleteOutcome.AUTHORITATIVELY_ABSENT);
                assertThat(otherResult.reservationRetained()).isFalse();
                assertThat(await(budgetB.snapshot()).reservations()).isEmpty();
                assertThat(await(budgetA.snapshot())).isEqualTo(held);
                gate.complete(null);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (!await(budgetA.snapshot()).reservations().isEmpty() && System.nanoTime() < deadline) {
                    Thread.sleep(10);
                }
                assertThat(await(budgetA.snapshot()).reservations()).isEmpty();
                assertThat(await(clientA.captureExactTarget(first)).exactTarget())
                        .isEmpty();
                var rejectedBeforeDispatch = clientA.deleteAuthority(
                        second,
                        () -> CompletableFuture.failedFuture(
                                new IllegalStateException("fixture pre-dispatch failure")));
                assertThatThrownBy(() -> await(rejectedBeforeDispatch.deleteExact(budgetA, intent2, target2)))
                        .hasRootCauseMessage("fixture pre-dispatch failure");
                assertThat(await(budgetA.snapshot()).reservations()).isEmpty();
                assertThat(await(clientA.captureExactTarget(second)).exactTarget())
                        .contains(target2);
                var next = await(authority2.deleteExact(budgetA, intent2, target2));
                assertThat(next.deleteResult().outcome()).isEqualTo(DeleteOutcome.AUTHORITATIVELY_ABSENT);
                assertThat(next.reservationRetained()).isFalse();
                assertThat(await(budgetA.snapshot()).reservations()).isEmpty();
            }
        }
    }

    @Test
    void terminalUnknownRetainsCapacityUntilReadOnlyNativeAbsenceReconciliation() throws Exception {
        var capability = capability("cell-terminal-unknown");
        var spec = spec(capability);
        try (var backend = M5BookKeeperNamespaceAuthorityV2.connect(uri(), capability)) {
            var binding = await(backend.readBinding()).orElseThrow();
            var quota = backend.nativeDeleteQuota();
            await(quota.expand(
                    quota.capacityForResources(await(quota.snapshot()).reservedResources() + 1)));
            var budget = backend.nativeDeleteCellBudget();
            await(budget.initialize(new M5BookKeeperDeleteCellBudgetV2.Limits(2, 1)));
            try (var state = new M5BookKeeperNativeCreateV2RealTest.NativeFaults(
                    uri(), capability, spec, Optional.of(binding))) {
                var handle = sealed(state);
                var target = M5BookKeeperDeleteAdapterV1.exactTarget(
                                await(state.manager.readLedgerMetadata(
                                                handle.ledgerIdentity().ledgerId()))
                                        .getValue(),
                                handle,
                                capability,
                                new byte[0])
                        .orElseThrow();
                var configuration = RealBookKeeperClientConfigurationV1.from(uri(), capability);
                var authority = new M5BookKeeperNativeDeleteAuthorityV2(
                        state.zk,
                        state.manager,
                        state.guard,
                        org.apache.bookkeeper.util.ZkUtils.getACLs(configuration),
                        handle,
                        spec.namespace(),
                        capability,
                        () -> CompletableFuture.completedFuture(null));
                var intent = bind(authority, target.metadataSha256());
                state.zk.dropNextDelete = true;
                var result = await(authority.deleteExact(budget, intent, target));
                assertThat(result.deleteResult().outcome()).isEqualTo(DeleteOutcome.OUTCOME_UNKNOWN);
                assertThat(result.reservationRetained()).isTrue();
                var held = await(budget.snapshot());
                assertThat(held.reservations()).hasSize(1);
                assertThat(held.reservations().get(0).terminalUnknown()).isTrue();
                assertThat(await(authority.reconcileCellDelete(budget, intent, target))
                                .reservationRetained())
                        .isTrue();
                assertThat(await(budget.snapshot())).isEqualTo(held);
                assertThatThrownBy(() -> await(authority.deleteExact(budget, intent, target)))
                        .hasRootCauseMessage("native delete already has a Cell reservation");
                // Package-only primitive simulates the prior request becoming applied; reconciliation never dispatches.
                assertThat(await(authority.deleteExact(intent.epoch(), target)).outcome())
                        .isEqualTo(DeleteOutcome.AUTHORITATIVELY_ABSENT);
                var reconciled = await(authority.reconcileCellDelete(budget, intent, target));
                assertThat(reconciled.deleteResult().outcome()).isEqualTo(DeleteOutcome.AUTHORITATIVELY_ABSENT);
                assertThat(reconciled.reservationRetained()).isFalse();
                assertThat(await(budget.snapshot()).reservations()).isEmpty();
            }
        }
    }

    private static M5BookKeeperNativeDeleteIntentV2 bind(
            M5BookKeeperNativeDeleteAuthorityV2 authority, com.nereusstream.domain.bytes.Sha256Digest metadata)
            throws Exception {
        var epoch = await(authority.claim(Optional.empty(), UUID.randomUUID()));
        return await(authority.bindIntent(epoch, digest("fixture-token"), digest("fixture-authority"), metadata));
    }

    private static M5BookKeeperNativeCreateSpecV2 spec(BookKeeperCapabilitySnapshotV1 capability) throws Exception {
        var id = UUID.randomUUID();
        return M5BookKeeperNativeCreateSpecV2.of(
                M5BookKeeperNativeCreateClientV2.discoverInstanceId(uri(), capability),
                digest(id.toString()),
                List.of(RunLedgerConfigurationV1.from(
                        capability,
                        new StorageRunId(new Id128(id.getMostSignificantBits(), id.getLeastSignificantBits())))));
    }

    private static BookKeeperCapabilitySnapshotV1 capability(String scope) {
        var c = M5BookKeeperNativeDeleteV2RealTest.CAPABILITY;
        return new BookKeeperCapabilitySnapshotV1(
                new CellProviderScopeId(digest(scope)),
                c.clientSourceCommit(),
                c.clientArtifactSha256(),
                c.serverSourceCommit(),
                c.serverImageManifestSha256(),
                c.protocolMode(),
                c.clientFrameLimitBytes(),
                c.serverFrameLimitBytes(),
                c.maximumAddPayloadBytes(),
                c.explicitEntryIdsSupported(),
                c.ensembleSize(),
                c.writeQuorumSize(),
                c.ackQuorumSize(),
                c.digestType(),
                c.fencingSupported(),
                c.recoverySupported(),
                c.timeoutClass(),
                c.credentialIdentityVersion(),
                c.configurationDigest());
    }

    private static String uri() {
        return System.getProperty("nereus.bookkeeper.metadataServiceUri");
    }
}
