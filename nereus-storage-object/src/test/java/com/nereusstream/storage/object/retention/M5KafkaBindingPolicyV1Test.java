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

package com.nereusstream.storage.object.retention;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome;
import com.nereusstream.storage.api.lifecycle.MetadataNamespaceIdentityV2;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class M5KafkaBindingPolicyV1Test {
    @Test
    void cancellingUnconfirmedScanFencesTheDelayedOriginalCasWithoutChangingPolicy() {
        var store = new Store(enrolled());
        var original = store.value;
        var coordinator = new M5BindingLogicalTrimCoordinatorV2(store, original.key());
        store.loseBeforeCas = true;
        var pending = coordinator
                .beginSourceScan(original, digest("delayed"))
                .toCompletableFuture()
                .join();
        assertThat(pending.outcome()).isEqualTo(M5SingleAuthorityLogicalTrimCoordinatorV2.Outcome.RESPONSE_UNKNOWN);
        assertThat(store.value).isEqualTo(original);
        var cancelled = coordinator
                .cancelUnconfirmedSourceScan(original)
                .toCompletableFuture()
                .join();
        assertThat(cancelled.outcome()).isEqualTo(M5SingleAuthorityLogicalTrimCoordinatorV2.Outcome.APPLIED_EXACT);
        var closed = M5BindingAuthorityCodecV1.decodeAuthority(store.value.canonicalStoredBytes());
        var before = M5BindingAuthorityCodecV1.decodeAuthority(original.canonicalStoredBytes());
        assertThat(closed.kafkaPolicy()).isEqualTo(before.kafkaPolicy());
        assertThat(closed.writerEnrollment()).isEqualTo(before.writerEnrollment());
        assertThat(closed.bootstrap()).isEqualTo(before.bootstrap());
        assertThat(closed.sourceScan()).isEmpty();
        assertThat(store.compareAndSet(Optional.of(original), original.key(), store.delayedCandidate)
                        .toCompletableFuture()
                        .join())
                .isEqualTo(MutationOutcome.DEFINITIVE_CONFLICT);
        assertThat(store.value).isEqualTo(cancelled.exactAuthority().orElseThrow());
    }

    @Test
    void actualCoordinatorCasExcludesPolicyChangesAndRejectsSameBytesAtANewVersion() {
        var store = new Store(enrolled());
        var original = store.value;
        var coordinator = new M5BindingLogicalTrimCoordinatorV2(store, original.key());
        var scan = coordinator
                .beginSourceScan(original, digest("attempt"))
                .toCompletableFuture()
                .join()
                .exactAuthority()
                .orElseThrow();
        var changed = policy().withConfiguration(
                        10, (short) 2, CanonicalUtf8.fromString("changed").bytes());
        assertThat(coordinator
                        .updateKafkaPolicy(original, changed)
                        .toCompletableFuture()
                        .join()
                        .outcome())
                .isEqualTo(M5SingleAuthorityLogicalTrimCoordinatorV2.Outcome.CONFLICT);
        assertThat(coordinator
                        .updateKafkaPolicy(scan, changed)
                        .toCompletableFuture()
                        .join()
                        .outcome())
                .isEqualTo(M5SingleAuthorityLogicalTrimCoordinatorV2.Outcome.RETAIN);
        assertThat(store.value).isEqualTo(scan);
        assertThatThrownBy(() -> coordinator.abortSourceScan(scan, digest("wrong")))
                .isInstanceOf(IllegalArgumentException.class);
        store.rewrite(scan.canonicalStoredBytes());
        assertThatThrownBy(() -> coordinator
                        .requireSourceScanCurrent(scan)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("Kafka source-scan authority changed");
    }

    @Test
    void lostSourceFenceReplyAndReadRetryTheExactOriginalCandidate() {
        var store = new Store(enrolled());
        var original = store.value;
        var coordinator = new M5BindingLogicalTrimCoordinatorV2(store, original.key());
        store.loseReadAfterCas = true;
        var first = coordinator
                .beginSourceScan(original, digest("attempt"))
                .toCompletableFuture()
                .join();
        assertThat(first.outcome()).isEqualTo(M5SingleAuthorityLogicalTrimCoordinatorV2.Outcome.RESPONSE_UNKNOWN);
        var applied = store.value;
        var retried = coordinator
                .beginSourceScan(original, digest("attempt"))
                .toCompletableFuture()
                .join();
        assertThat(retried.outcome()).isEqualTo(M5SingleAuthorityLogicalTrimCoordinatorV2.Outcome.EXISTING_EXACT);
        assertThat(retried.exactAuthority()).contains(applied);
        assertThat(store.value).isEqualTo(applied);
        var reopened = coordinator
                .abortSourceScan(applied, digest("attempt"))
                .toCompletableFuture()
                .join();
        assertThat(reopened.outcome()).isEqualTo(M5SingleAuthorityLogicalTrimCoordinatorV2.Outcome.APPLIED_EXACT);
        assertThat(M5BindingAuthorityCodecV1.decodeAuthority(store.value.canonicalStoredBytes())
                        .sourceScan())
                .isEmpty();
    }

    @Test
    void firstPolicyAndLaterAuditHoldsSurviveCanonicalAuthorityTransitions() {
        var first = policy();
        assertThat(M5KafkaBindingPolicyV1.decode(first.encode())).isEqualTo(first);
        var bootstrap = bootstrap(Optional.of(first));
        assertThat(M5BindingBootstrapV1.decode(bootstrap.encode())).isEqualTo(bootstrap);
        var activated = M5BindingAuthorityCodecV1.activateBootstrap(bootstrap, selector());
        assertThat(activated.wireVersion()).isEqualTo(7);
        assertThat(activated.kafkaPolicy()).contains(first);
        var hold = new M5KafkaBindingPolicyV1.AuditHold(
                digest("hold"),
                new PhysicalResourceIdV2.BookKeeperLedger(
                        first.physicalBinding().physicalNamespace(), 7),
                0,
                10,
                2);
        var held = first.withAuditHolds(List.of(hold));
        var updated = M5BindingAuthorityCodecV1.kafkaPolicySuccessor(activated, held);
        M5BindingAuthorityCodecV1.requireTaskSelectionTransition(activated, updated);
        var trimmed = M5BindingAuthorityCodecV1.logicalTrimSuccessor(
                updated, new M5LogicalTrimRecordsV2.LogicalTrimStateV2(Optional.empty(), Optional.empty()), List.of());
        var read = M5BindingAuthorityCodecV1.decodeAuthority(M5BindingAuthorityCodecV1.encodeAuthority(trimmed));
        assertThat(read.kafkaPolicy()).contains(held);
        assertThat(read.bootstrap()).contains(bootstrap);
        assertThatThrownBy(() -> bootstrap(Optional.of(held))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void sourceFenceRequiresEnrollmentAndKeepsOriginalPolicyUntilExactAbort() {
        var open = M5BindingAuthorityCodecV1.activateBootstrap(bootstrap(Optional.of(policy())), selector());
        assertThatThrownBy(() -> M5BindingAuthorityCodecV1.sourceScanSuccessor(open, digest("attempt")))
                .isInstanceOf(IllegalArgumentException.class);
        var enrollment = new M5BindingAuthorityRecordsV1.ReferenceWriterEnrollmentV1(
                open.capability(),
                List.of(M5RetentionRecordsV1.FloorClassV1.values()),
                List.of(M5RetentionRecordsV1.ReferenceKindV1.values()),
                digest("registry"));
        var enrolled = M5BindingAuthorityCodecV1.successor(
                open,
                M5BindingAuthorityRecordsV1.BindingAuthorityStateV1.OPEN_V1,
                open.selectorProjection(),
                open.batchSlots(),
                Optional.empty(),
                List.of(),
                Optional.of(enrollment));
        var fenced = M5BindingAuthorityCodecV1.sourceScanSuccessor(enrolled, digest("attempt"));
        M5BindingAuthorityCodecV1.requireTaskSelectionTransition(enrolled, fenced);
        assertThat(M5BindingAuthorityCodecV1.decodeAuthority(M5BindingAuthorityCodecV1.encodeAuthority(fenced)))
                .isEqualTo(fenced);
        assertThat(fenced.sourceScan().orElseThrow().openAuthoritySha256())
                .isEqualTo(Sha256Digest.hash(M5BindingAuthorityCodecV1.encodeAuthority(enrolled)));
        assertThatThrownBy(() -> M5BindingAuthorityCodecV1.kafkaPolicySuccessor(
                        fenced, fenced.kafkaPolicy().orElseThrow().withAuditHolds(List.of())))
                .isInstanceOf(IllegalArgumentException.class);
        var reopened = M5BindingAuthorityCodecV1.abortSourceScan(fenced);
        M5BindingAuthorityCodecV1.requireTaskSelectionTransition(fenced, reopened);
        assertThat(reopened.sourceScan()).isEmpty();
        assertThat(reopened.kafkaPolicy()).isEqualTo(enrolled.kafkaPolicy());
        assertThat(reopened.bootstrap()).isEqualTo(enrolled.bootstrap());
    }

    @Test
    void legacyBootstrapStaysWithoutPolicyAndNewCodecRejectsUnknownTruncatedOrWeakGrace() {
        var legacy = bootstrap(Optional.empty());
        assertThat(legacy.encode().length()).isEqualTo(208);
        assertThat(M5BindingBootstrapV1.decode(legacy.encode())).isEqualTo(legacy);
        assertThat(M5BindingAuthorityCodecV1.activateBootstrap(legacy, selector())
                        .wireVersion())
                .isEqualTo(6);
        var bytes = policy().encode().toByteArray();
        bytes[7] = 2;
        assertThatThrownBy(() -> M5KafkaBindingPolicyV1.decode(CanonicalBytes.copyOf(bytes)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> M5KafkaBindingPolicyV1.decode(CanonicalBytes.copyOf(new byte[7])))
                .isInstanceOf(IllegalArgumentException.class);
        var first = policy();
        assertThatThrownBy(() -> new M5KafkaBindingPolicyV1(
                        first.physicalBinding(),
                        1,
                        9,
                        (short) 2,
                        first.nativeLogConfiguration(),
                        first.providerConfigurationSha256(),
                        first.admittedAuditPolicy(),
                        0,
                        26,
                        25,
                        List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> first.withConfiguration(8, (short) 2, first.nativeLogConfiguration()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static M5KafkaBindingPolicyV1 policy() {
        var namespace = new PhysicalResourceIdV2.Namespace(
                PhysicalResourceIdV2.ProviderKind.BOOKKEEPER,
                CanonicalUtf8.fromString("bk-service"),
                CanonicalUtf8.fromString("bk-instance"));
        return new M5KafkaBindingPolicyV1(
                new PhysicalNamespaceAuthorityBindingV2(namespace, new MetadataNamespaceIdentityV2(new Id128(1, 2), 3)),
                1,
                9,
                (short) 2,
                CanonicalUtf8.fromString("actual-native-config-fixture").bytes(),
                digest("provider"),
                CanonicalUtf8.fromString("explicit-audit-input-fixture").bytes(),
                0,
                10,
                25,
                List.of());
    }

    private static M5BindingBootstrapV1 bootstrap(Optional<M5KafkaBindingPolicyV1> policy) {
        return new M5BindingBootstrapV1(selector().binding(), selector().capability(), digest("scope"), policy);
    }

    private static M5BindingAuthorityRecordsV1.BindingRetirementAuthorityV1 enrolled() {
        var open = M5BindingAuthorityCodecV1.activateBootstrap(bootstrap(Optional.of(policy())), selector());
        return M5BindingAuthorityCodecV1.successor(
                open,
                M5BindingAuthorityRecordsV1.BindingAuthorityStateV1.OPEN_V1,
                open.selectorProjection(),
                open.batchSlots(),
                Optional.empty(),
                List.of(),
                Optional.of(new M5BindingAuthorityRecordsV1.ReferenceWriterEnrollmentV1(
                        open.capability(),
                        List.of(M5RetentionRecordsV1.FloorClassV1.values()),
                        List.of(M5RetentionRecordsV1.ReferenceKindV1.values()),
                        digest("registry"))));
    }

    private static final class Store implements ExactMetadataTransactionStoreV1 {
        private VersionedValue value;
        private long version;
        private boolean loseReadAfterCas;
        private boolean failRead;
        private boolean loseBeforeCas;
        private CanonicalBytes delayedCandidate;

        Store(M5BindingAuthorityRecordsV1.BindingRetirementAuthorityV1 initial) {
            var key = new com.nereusstream.storage.object.read.control.M4ReadControlKeysV1(7, initial.binding())
                    .selector();
            value = VersionedValue.of(key, M5BindingAuthorityCodecV1.encodeAuthority(initial), nextVersion());
        }

        private MetadataVersion nextVersion() {
            return new MetadataVersion(CanonicalBytes.copyOf(
                    java.nio.ByteBuffer.allocate(8).putLong(++version).array()));
        }

        private void rewrite(CanonicalBytes bytes) {
            value = VersionedValue.of(value.key(), bytes, nextVersion());
        }

        @Override
        public CompletionStage<Optional<VersionedValue>> read(String key) {
            if (failRead) {
                failRead = false;
                return CompletableFuture.failedFuture(new IllegalStateException("lost original read response"));
            }
            return CompletableFuture.completedFuture(key.equals(value.key()) ? Optional.of(value) : Optional.empty());
        }

        @Override
        public CompletionStage<MutationOutcome> compareAndSet(
                Optional<VersionedValue> expected, String key, CanonicalBytes candidate) {
            if (!key.equals(value.key()) || !expected.equals(Optional.of(value))) {
                return CompletableFuture.completedFuture(MutationOutcome.DEFINITIVE_CONFLICT);
            }
            if (loseBeforeCas) {
                loseBeforeCas = false;
                failRead = true;
                delayedCandidate = candidate;
                return CompletableFuture.completedFuture(MutationOutcome.RESPONSE_UNKNOWN);
            }
            rewrite(candidate);
            if (loseReadAfterCas) {
                loseReadAfterCas = false;
                failRead = true;
                return CompletableFuture.completedFuture(MutationOutcome.RESPONSE_UNKNOWN);
            }
            return CompletableFuture.completedFuture(MutationOutcome.APPLIED_EXACT);
        }

        @Override
        public CompletionStage<TransactionOutcome> conditionalTransaction(ExactTransaction transaction) {
            return CompletableFuture.completedFuture(TransactionOutcome.UNSUPPORTED);
        }

        @Override
        public boolean supportsAtomicMultiKeyTransactions() {
            return false;
        }
    }

    private static M4ReadControlRecordsV1.BindingReadSelector selector() {
        return new M4ReadControlRecordsV1.BindingReadSelector(
                new M4ReadControlRecordsV1.BindingIdentity(
                        new TopicBindingId(digest("binding")), digest("incarnation"), digest("storage")),
                digest("view"),
                1,
                1,
                1,
                M4ReadControlRecordsV1.SelectorMode.PREFERRED_ONLY,
                M4ReadControlRecordsV1.AdmissionState.ADMITTING,
                Optional.empty(),
                new M4ReadControlRecordsV1.CapabilityBinding(1, digest("capability")),
                List.of(),
                List.of());
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalUtf8.fromString(value).bytes());
    }
}
