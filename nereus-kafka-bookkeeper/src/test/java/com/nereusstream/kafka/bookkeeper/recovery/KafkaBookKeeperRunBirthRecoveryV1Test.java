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

package com.nereusstream.kafka.bookkeeper.recovery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.run.KafkaRunTestFixtures;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.RunLedgerOpenOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerOpenResultV1;
import com.nereusstream.storage.api.kafka.KafkaBookKeeperOwnerAuthorityV1;
import com.nereusstream.storage.api.kafka.KafkaOwnerAdmissionV1;
import com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootStateV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class KafkaBookKeeperRunBirthRecoveryV1Test {
    @Test
    void contradictoryAdmittedRootCannotPassAsNeverCreatedBirth() {
        var binding = KafkaRunTestFixtures.binding(6, 11, 5);
        var root = new KafkaRunRootSnapshotV1(
                binding.bindingId(),
                binding.topicIncarnation(),
                binding.partitionId(),
                binding.storageEpochId(),
                binding.creatorOwnerEpoch(),
                binding.kafkaLeaderEpoch(),
                binding.providerScopeId(),
                binding.runId(),
                new BookKeeperLedgerIdentity(47),
                100,
                OptionalLong.empty(),
                KafkaRunRootStateV1.ACTIVE,
                Optional.empty());
        var namespace = new PhysicalResourceIdV2.Namespace(
                PhysicalResourceIdV2.ProviderKind.BOOKKEEPER,
                CanonicalUtf8.fromString("birth-recovery-test"),
                CanonicalUtf8.fromString("ledger-id-space"));
        var record = new KafkaRunRootRecordV2(
                new PhysicalResourceIdV2.BookKeeperLedger(namespace, 47), root, true, Optional.empty());
        var closed = new KafkaOwnerAdmissionV1(
                Sha256Digest.hash(KafkaRunRootRecordV2.Scope.of(root).encode()),
                new KafkaOwnerIdentityV1(11, 5, 1, 1, 1),
                true,
                Optional.empty(),
                List.of());
        var authority = (KafkaBookKeeperOwnerAuthorityV1) Proxy.newProxyInstance(
                KafkaBookKeeperOwnerAuthorityV1.class.getClassLoader(),
                new Class<?>[] {KafkaBookKeeperOwnerAuthorityV1.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("readRunBirths")) {
                        return CompletableFuture.completedFuture(Optional.of(List.of(record)));
                    }
                    throw new AssertionError("unexpected authority call: " + method.getName());
                });
        var fences = new AtomicInteger();

        assertThatThrownBy(() -> new KafkaBookKeeperRunBirthRecoveryV1(
                                new KafkaRunTestFixtures.FakeSession(), authority, (owner, birth, handle) -> {
                                    fences.incrementAndGet();
                                    return CompletableFuture.completedFuture(null);
                                })
                        .observe(closed)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("unselected run birth contradicts its admitted root: " + root.runId());
        assertThat(fences).hasValue(0);
    }

    @Test
    void birthInventoryChangeAfterNativeFenceCannotAuthorizeTakeover() {
        var binding = KafkaRunTestFixtures.binding(6, 11, 5);
        var root = new KafkaRunRootSnapshotV1(
                binding.bindingId(),
                binding.topicIncarnation(),
                binding.partitionId(),
                binding.storageEpochId(),
                binding.creatorOwnerEpoch(),
                binding.kafkaLeaderEpoch(),
                binding.providerScopeId(),
                binding.runId(),
                new BookKeeperLedgerIdentity(47),
                100,
                OptionalLong.empty(),
                KafkaRunRootStateV1.ACTIVE,
                Optional.empty());
        var namespace = new PhysicalResourceIdV2.Namespace(
                PhysicalResourceIdV2.ProviderKind.BOOKKEEPER,
                CanonicalUtf8.fromString("birth-recovery-test"),
                CanonicalUtf8.fromString("ledger-id-space"));
        var pending = KafkaRunRootRecordV2.pending(namespace, root);
        var closed = new KafkaOwnerAdmissionV1(
                Sha256Digest.hash(KafkaRunRootRecordV2.Scope.of(root).encode()),
                new KafkaOwnerIdentityV1(11, 5, 1, 1, 1),
                true,
                Optional.empty(),
                List.of());
        var reads = new AtomicInteger();
        var authority = (KafkaBookKeeperOwnerAuthorityV1) Proxy.newProxyInstance(
                KafkaBookKeeperOwnerAuthorityV1.class.getClassLoader(),
                new Class<?>[] {KafkaBookKeeperOwnerAuthorityV1.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("readRunBirths")) {
                        return CompletableFuture.completedFuture(
                                Optional.of(List.of(reads.getAndIncrement() == 0 ? pending : pending.admit())));
                    }
                    throw new AssertionError("unexpected authority call: " + method.getName());
                });
        var fences = new AtomicInteger();
        var session = new KafkaRunTestFixtures.FakeSession();
        session.openOverride = RunLedgerOpenResultV1.withoutHandle(RunLedgerOpenOutcomeV1.ABSENT);

        assertThatThrownBy(() -> new KafkaBookKeeperRunBirthRecoveryV1(session, authority, (owner, birth, handle) -> {
                            fences.incrementAndGet();
                            return CompletableFuture.completedFuture(null);
                        })
                        .observe(closed)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("closed owner run births changed after native fencing");
        assertThat(reads).hasValue(2);
        assertThat(fences).hasValue(1);

        var stableAuthority = (KafkaBookKeeperOwnerAuthorityV1) Proxy.newProxyInstance(
                KafkaBookKeeperOwnerAuthorityV1.class.getClassLoader(),
                new Class<?>[] {KafkaBookKeeperOwnerAuthorityV1.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("readRunBirths")) {
                        return CompletableFuture.completedFuture(Optional.of(List.of(pending)));
                    }
                    throw new AssertionError("unexpected authority call: " + method.getName());
                });
        assertThatThrownBy(() -> new KafkaBookKeeperRunBirthRecoveryV1(
                                new KafkaRunTestFixtures.FakeSession(),
                                stableAuthority,
                                (owner, birth, handle) -> CompletableFuture.completedFuture(null))
                        .observe(closed)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("unselected physical birth has no writer ticket reconciliation");
    }
}
