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

import static com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteCellBudgetV2RealTest.bind;
import static com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteCellBudgetV2RealTest.capability;
import static com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteCellBudgetV2RealTest.spec;
import static com.nereusstream.storage.bookkeeper.M5BookKeeperNativeDeleteV2RealTest.await;
import static com.nereusstream.storage.bookkeeper.M5BookKeeperNativeDeleteV2RealTest.sealed;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.BookKeeperDeleteTargetV1;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.DeleteOutcome;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Separate JVMs and actual service restart retain unresolved native Cell records, without dispatch replay. */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class M5BookKeeperDeleteCellBudgetV2RestartTest {
    @Test
    void writeBeforeServerRestart() throws Exception {
        for (boolean unknown : List.of(false, true)) {
            var capability = capability(scope(unknown));
            var spec = spec(capability);
            try (var backend = M5BookKeeperNamespaceAuthorityV2.connect(uri(), capability)) {
                var binding = await(backend.readBinding()).orElseThrow();
                var quota = backend.nativeDeleteQuota();
                await(quota.expand(
                        quota.capacityForResources(await(quota.snapshot()).reservedResources() + 1)));
                var budget = backend.nativeDeleteCellBudget();
                await(budget.initialize(new M5BookKeeperDeleteCellBudgetV2.Limits(1, 1)));
                try (var client = M5BookKeeperNativeCreateClientV2.connect(uri(), capability, spec, binding);
                        var state = new M5BookKeeperNativeCreateV2RealTest.NativeFaults(
                                uri(), capability, spec, Optional.of(binding))) {
                    var handle = sealed(client, spec);
                    await(client.fenceCreates());
                    var target = await(client.captureExactTarget(handle))
                            .exactTarget()
                            .orElseThrow();
                    var authority = new M5BookKeeperNativeDeleteAuthorityV2(
                            state.zk,
                            state.manager,
                            state.guard,
                            org.apache.bookkeeper.util.ZkUtils.getACLs(
                                    RealBookKeeperClientConfigurationV1.from(uri(), capability)),
                            handle,
                            spec.namespace(),
                            capability,
                            () -> CompletableFuture.completedFuture(null));
                    var intent = bind(authority, target.metadataSha256());
                    if (unknown) {
                        state.zk.dropNextDelete = true;
                        var result = await(authority.deleteExact(budget, intent, target));
                        assertThat(result.deleteResult().outcome()).isEqualTo(DeleteOutcome.OUTCOME_UNKNOWN);
                        assertThat(result.reservationRetained()).isTrue();
                        assertThat(await(client.captureExactTarget(handle)).exactTarget())
                                .contains(target);
                    } else {
                        // Apply the actual native delete, but never deliver its callback to the accepted invocation.
                        state.zk.holdNextDeleteReply = true;
                        var observer =
                                authority.deleteExact(budget, intent, target).toCompletableFuture();
                        await(state.zk.held);
                        assertThat(observer.isDone()).isFalse();
                        assertThat(observer.cancel(true)).isTrue();
                        assertThat(await(client.captureExactTarget(handle)).outcome())
                                .isEqualTo(M5BookKeeperDeleteAdapterV1.CaptureOutcome.DEFINITIVELY_ABSENT);
                    }
                    var held = await(budget.snapshot());
                    assertThat(held.reservations()).hasSize(1);
                    assertThat(held.reservations().get(0).terminalUnknown()).isEqualTo(unknown);
                    Files.write(
                            checkpoint(unknown),
                            List.of(
                                    spec.encode().toHex(),
                                    Long.toString(handle.ledgerIdentity().ledgerId()),
                                    Sha256Digest.hash(intent.epoch().encode()).toHex(),
                                    Sha256Digest.hash(intent.encode()).toHex(),
                                    Sha256Digest.hash(held.encode()).toHex(),
                                    target.metadataSha256().toHex(),
                                    Long.toString(target.sealedLastEntryId()),
                                    Long.toString(target.sealedLength()),
                                    Integer.toString(target.metadataFormatVersion()),
                                    Long.toString(target.metadataCToken())));
                }
            }
        }
    }

    @Test
    void readAfterServerRestart() throws Exception {
        for (boolean unknown : List.of(false, true)) {
            var lines = Files.readAllLines(checkpoint(unknown));
            assertThat(lines).hasSize(10);
            var capability = capability(scope(unknown));
            var spec = M5BookKeeperNativeCreateSpecV2.decode(
                    CanonicalBytes.copyOf(HexFormat.of().parseHex(lines.get(0))));
            var run = spec.configurations().get(0);
            var handle = new RunLedgerHandleV1(
                    run.providerScopeId(),
                    run.runId(),
                    new BookKeeperLedgerIdentity(Long.parseLong(lines.get(1))),
                    run.configurationDigest());
            var target = new BookKeeperDeleteTargetV1(
                    handle,
                    Long.parseLong(lines.get(6)),
                    Long.parseLong(lines.get(7)),
                    capability.ensembleSize(),
                    capability.writeQuorumSize(),
                    capability.ackQuorumSize(),
                    capability.digestType(),
                    capability.credentialIdentityVersion(),
                    Sha256Digest.hash(CanonicalBytes.copyOf(new byte[0])),
                    Integer.parseInt(lines.get(8)),
                    Long.parseLong(lines.get(9)),
                    Sha256Digest.copyOf(HexFormat.of().parseHex(lines.get(5))));
            try (var backend = M5BookKeeperNamespaceAuthorityV2.connect(uri(), capability)) {
                var binding = await(backend.readBinding()).orElseThrow();
                var budget = backend.nativeDeleteCellBudget();
                var held = await(budget.snapshot());
                assertThat(Sha256Digest.hash(held.encode()).toHex()).isEqualTo(lines.get(4));
                assertThat(held.reservations()).hasSize(1);
                assertThat(held.reservations().get(0).terminalUnknown()).isEqualTo(unknown);
                try (var client = M5BookKeeperNativeCreateClientV2.connect(uri(), capability, spec, binding)) {
                    var authority = client.deleteAuthority(handle);
                    var epoch = await(authority.read()).orElseThrow();
                    var intent = await(authority.readIntent()).orElseThrow();
                    assertThat(Sha256Digest.hash(epoch.encode()).toHex()).isEqualTo(lines.get(2));
                    assertThat(Sha256Digest.hash(intent.encode()).toHex()).isEqualTo(lines.get(3));
                    assertThat(intent.ledgerMetadataSha256()).isEqualTo(target.metadataSha256());
                    if (unknown) {
                        assertThat(await(client.captureExactTarget(handle)).exactTarget())
                                .contains(target);
                        var observed = await(authority.reconcileCellDelete(budget, intent, target));
                        assertThat(observed.deleteResult().outcome()).isEqualTo(DeleteOutcome.OUTCOME_UNKNOWN);
                        assertThat(observed.reservationRetained()).isTrue();
                    } else {
                        assertThat(await(client.captureExactTarget(handle)).outcome())
                                .isEqualTo(M5BookKeeperDeleteAdapterV1.CaptureOutcome.DEFINITIVELY_ABSENT);
                        assertThatThrownBy(() -> await(authority.reconcileCellDelete(budget, intent, target)))
                                .hasRootCauseMessage(
                                        "native Cell reservation is in flight or belongs to another intent");
                    }
                    assertThatThrownBy(() -> await(authority.deleteExact(budget, intent, target)))
                            .hasRootCauseMessage("native delete already has a Cell reservation");
                    assertThat(await(budget.snapshot())).isEqualTo(held);
                }
            }
        }
        // A fresh healthy configured Cell can still perform actual deletion while both unresolved heads remain full.
        var capability = capability("cell-restart-healthy");
        var spec = spec(capability);
        try (var backend = M5BookKeeperNamespaceAuthorityV2.connect(uri(), capability)) {
            var binding = await(backend.readBinding()).orElseThrow();
            var quota = backend.nativeDeleteQuota();
            await(quota.expand(
                    quota.capacityForResources(await(quota.snapshot()).reservedResources() + 1)));
            var budget = backend.nativeDeleteCellBudget();
            await(budget.initialize(new M5BookKeeperDeleteCellBudgetV2.Limits(1, 1)));
            try (var client = M5BookKeeperNativeCreateClientV2.connect(uri(), capability, spec, binding)) {
                var handle = sealed(client, spec);
                await(client.fenceCreates());
                var target =
                        await(client.captureExactTarget(handle)).exactTarget().orElseThrow();
                var authority = client.deleteAuthority(handle);
                var intent = bind(authority, target.metadataSha256());
                var result = await(authority.deleteExact(budget, intent, target));
                assertThat(result.deleteResult().outcome()).isEqualTo(DeleteOutcome.AUTHORITATIVELY_ABSENT);
                assertThat(result.reservationRetained()).isFalse();
                assertThat(await(budget.snapshot()).reservations()).isEmpty();
            }
        }
        for (boolean unknown : List.of(false, true)) {
            try (var backend = M5BookKeeperNamespaceAuthorityV2.connect(uri(), capability(scope(unknown)))) {
                assertThat(Sha256Digest.hash(
                                        await(backend.nativeDeleteCellBudget().snapshot())
                                                .encode())
                                .toHex())
                        .isEqualTo(Files.readAllLines(checkpoint(unknown)).get(4));
            }
        }
    }

    @Test
    void recoverFencedActiveAfterServerRestart() throws Exception {
        var lines = Files.readAllLines(checkpoint(false));
        assertThat(lines).hasSize(10);
        var capability = capability(scope(false));
        var spec = M5BookKeeperNativeCreateSpecV2.decode(
                CanonicalBytes.copyOf(HexFormat.of().parseHex(lines.get(0))));
        var run = spec.configurations().get(0);
        var handle = new RunLedgerHandleV1(
                run.providerScopeId(),
                run.runId(),
                new BookKeeperLedgerIdentity(Long.parseLong(lines.get(1))),
                run.configurationDigest());
        try (var backend = M5BookKeeperNamespaceAuthorityV2.connect(uri(), capability)) {
            var binding = await(backend.readBinding()).orElseThrow();
            var budget = backend.nativeDeleteCellBudget();
            var held = await(budget.snapshot());
            assertThat(Sha256Digest.hash(held.encode()).toHex()).isEqualTo(lines.get(4));
            assertThat(held.reservations()).hasSize(1);
            assertThat(held.reservations().get(0).terminalUnknown()).isFalse();
            try (var client = M5BookKeeperNativeCreateClientV2.connect(uri(), capability, spec, binding)) {
                var authority = client.deleteAuthority(handle);
                var predecessor = await(authority.read()).orElseThrow();
                var oldIntent = await(authority.readIntent()).orElseThrow();
                assertThat(Sha256Digest.hash(predecessor.encode()).toHex()).isEqualTo(lines.get(2));
                assertThat(Sha256Digest.hash(oldIntent.encode()).toHex()).isEqualTo(lines.get(3));
                assertThatThrownBy(() -> await(authority.reconcileFencedActiveCellDelete(budget, oldIntent)))
                        .hasRootCauseMessage("native delete predecessor epoch is not fenced");
                var successor = await(authority.claim(Optional.of(predecessor), UUID.randomUUID()));
                assertThat(successor.nativeVersion()).isGreaterThan(predecessor.nativeVersion());
                assertThat(await(authority.reconcileFencedActiveCellDelete(budget, oldIntent)))
                        .isTrue();
                assertThat(await(budget.snapshot()).reservations()).isEmpty();
                assertThat(await(client.captureExactTarget(handle)).outcome())
                        .isEqualTo(M5BookKeeperDeleteAdapterV1.CaptureOutcome.DEFINITIVELY_ABSENT);
                assertThat(await(authority.read())).contains(successor);
                assertThat(await(authority.readIntent())).contains(oldIntent);
            }
        }
    }

    private static String scope(boolean unknown) {
        return unknown ? "cell-restart-unknown" : "cell-restart-active";
    }

    private static Path checkpoint(boolean unknown) {
        return Path.of(
                System.getProperty("nereus.m5.nativeCell.restartCheckpoint") + (unknown ? "-unknown" : "-active"));
    }

    private static String uri() {
        return System.getProperty("nereus.bookkeeper.metadataServiceUri");
    }
}
