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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.pulsar.offload.M5PulsarObjectCleanupOrderV1.CleanupComponentV1;
import com.nereusstream.pulsar.offload.M5PulsarObjectCleanupOrderV1.CleanupObservationV1;
import com.nereusstream.pulsar.offload.M5PulsarObjectCleanupOrderV1.CleanupStageV1;
import com.nereusstream.pulsar.offload.M5PulsarObjectCleanupOrderV1.ComponentReconciliationV1;
import com.nereusstream.pulsar.offload.M5PulsarObjectCleanupOrderV1.TransitionOutcomeV1;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class M5PulsarObjectCleanupOrderV1Test {
    @Test
    void rootMustBeAuthoritativelyAbsentBeforeDataAndMultipartCanAdvance() {
        var progress = M5PulsarObjectCleanupOrderV1.start(target());

        var dataFirst = M5PulsarObjectCleanupOrderV1.observe(progress, absent(CleanupComponentV1.NPD1_DATA, 30));
        assertThat(dataFirst.outcome()).isEqualTo(TransitionOutcomeV1.ORDER_VIOLATION);
        assertThat(dataFirst.progress()).isEqualTo(progress);

        var root = M5PulsarObjectCleanupOrderV1.observe(progress, absent(CleanupComponentV1.NPO1_ROOT, 31));
        assertThat(root.outcome()).isEqualTo(TransitionOutcomeV1.ADVANCED);
        assertThat(root.progress().stage()).isEqualTo(CleanupStageV1.DATA_ABSENCE_REQUIRED);

        var multipartBeforeData = M5PulsarObjectCleanupOrderV1.observe(
                root.progress(), absent(CleanupComponentV1.OWNED_MULTIPART_RESIDUE, 32));
        assertThat(multipartBeforeData.outcome()).isEqualTo(TransitionOutcomeV1.ORDER_VIOLATION);

        var data = M5PulsarObjectCleanupOrderV1.observe(root.progress(), absent(CleanupComponentV1.NPD1_DATA, 33));
        assertThat(data.progress().stage()).isEqualTo(CleanupStageV1.MULTIPART_ABSENCE_REQUIRED);
        var complete = M5PulsarObjectCleanupOrderV1.observe(
                data.progress(), absent(CleanupComponentV1.OWNED_MULTIPART_RESIDUE, 34));
        assertThat(complete.outcome()).isEqualTo(TransitionOutcomeV1.COMPLETE);
        assertThat(complete.progress().stage()).isEqualTo(CleanupStageV1.AUTHORITATIVELY_ABSENT);
        assertThat(M5PulsarObjectCleanupOrderV1.observe(complete.progress(), absent(CleanupComponentV1.NPO1_ROOT, 35))
                        .outcome())
                .isEqualTo(TransitionOutcomeV1.COMPLETE);
    }

    @Test
    void responseUnknownAndExactOldIdentityRemainingNeverAdvance() {
        var progress = M5PulsarObjectCleanupOrderV1.start(target());
        var unknown = M5PulsarObjectCleanupOrderV1.observe(
                progress,
                new CleanupObservationV1(
                        CleanupComponentV1.NPO1_ROOT, ComponentReconciliationV1.OUTCOME_UNKNOWN, digest(40)));
        assertThat(unknown.outcome()).isEqualTo(TransitionOutcomeV1.OUTCOME_UNKNOWN);
        assertThat(unknown.progress()).isEqualTo(progress);

        var remains = M5PulsarObjectCleanupOrderV1.observe(
                progress,
                new CleanupObservationV1(
                        CleanupComponentV1.NPO1_ROOT,
                        ComponentReconciliationV1.EXACT_OLD_IDENTITY_REMAINS,
                        digest(41)));
        assertThat(remains.outcome()).isEqualTo(TransitionOutcomeV1.RETRYABLE_EXACT_REMAINS);
        assertThat(remains.progress()).isEqualTo(progress);
    }

    @Test
    void differentOrForeignIdentityQuarantinesPermanently() {
        var progress = M5PulsarObjectCleanupOrderV1.start(target());
        var conflict = M5PulsarObjectCleanupOrderV1.observe(
                progress,
                new CleanupObservationV1(
                        CleanupComponentV1.NPO1_ROOT,
                        ComponentReconciliationV1.DIFFERENT_OR_FOREIGN_IDENTITY,
                        digest(42)));
        assertThat(conflict.outcome()).isEqualTo(TransitionOutcomeV1.QUARANTINED);
        assertThat(conflict.progress().stage()).isEqualTo(CleanupStageV1.QUARANTINED);
        assertThat(M5PulsarObjectCleanupOrderV1.observe(conflict.progress(), absent(CleanupComponentV1.NPO1_ROOT, 43))
                        .outcome())
                .isEqualTo(TransitionOutcomeV1.QUARANTINED);
    }

    @Test
    void targetBindsExactAttemptKeysBodiesVersionsAndAuthorityRoots() {
        var target = target();
        assertThat(target.rootIdentity().key())
                .isEqualTo(target.attempt().keys().rootKey());
        assertThat(target.dataIdentity().key())
                .isEqualTo(target.attempt().keys().dataKey());
        assertThat(target.targetRoot().isZero()).isFalse();

        assertThatThrownBy(() -> new M5PulsarObjectCleanupOrderV1.CleanupTargetV1(
                        target.attempt(),
                        target.rootIdentity(),
                        target.rootImmutableVersion(),
                        target.dataIdentity(),
                        target.dataImmutableVersion(),
                        target.persistedIntentBindingRoot(),
                        target.m4ReleasedProofRoot(),
                        target.referenceFreeProofRoot(),
                        target.multipartInventoryRoot(),
                        target.providerAdmissionRoot(),
                        digest(99)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("target root differs");
    }

    @Test
    void retainBookKeeperAttemptCannotBecomeAPhysicalCleanupTarget() {
        PulsarSealedLedgerAttemptV1 retain = attempt(PulsarSealedLedgerAttemptV1.RetentionClass.RETAIN_BK);
        PulsarOffloadKeysV1 keys = retain.keys();
        assertThatThrownBy(() -> M5PulsarObjectCleanupOrderV1.target(
                        retain,
                        identity(keys.rootKey(), 10),
                        bytes("root-v1"),
                        identity(keys.dataKey(), 20),
                        bytes("data-v1"),
                        digest(1),
                        digest(2),
                        digest(3),
                        digest(4),
                        digest(5)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sealed-ledger attempt");
    }

    @Test
    void pureOrderingCoreExposesNoExternalDeleteMethod() {
        assertThat(Arrays.stream(M5PulsarObjectCleanupOrderV1.class.getDeclaredMethods())
                        .filter(method -> Modifier.isPublic(method.getModifiers()))
                        .map(method -> method.getName())
                        .collect(Collectors.toSet()))
                .containsExactlyInAnyOrder("target", "start", "observe");
    }

    private static M5PulsarObjectCleanupOrderV1.CleanupTargetV1 target() {
        PulsarSealedLedgerAttemptV1 attempt = attempt(PulsarSealedLedgerAttemptV1.RetentionClass.DELETE_AFTER_VERIFIED);
        PulsarOffloadKeysV1 keys = attempt.keys();
        return M5PulsarObjectCleanupOrderV1.target(
                attempt,
                identity(keys.rootKey(), 10),
                bytes("root-v1"),
                identity(keys.dataKey(), 20),
                bytes("data-v1"),
                digest(1),
                digest(2),
                digest(3),
                digest(4),
                digest(5));
    }

    private static PulsarSealedLedgerAttemptV1 attempt(PulsarSealedLedgerAttemptV1.RetentionClass retention) {
        return new PulsarSealedLedgerAttemptV1(
                17,
                UUID.fromString("00000000-0000-0000-0000-000000000017"),
                9,
                10,
                1000,
                1234,
                7,
                "cell-17",
                retention,
                PulsarSealedLedgerAttemptV1.DeleteState.BK_DELETE_NONE,
                false);
    }

    private static CleanupObservationV1 absent(CleanupComponentV1 component, int evidence) {
        return new CleanupObservationV1(component, ComponentReconciliationV1.AUTHORITATIVELY_ABSENT, digest(evidence));
    }

    private static ObjectIdentity identity(String key, long bytes) {
        return new ObjectIdentity(key, bytes, digest(Math.toIntExact(bytes)));
    }

    private static CanonicalBytes bytes(String value) {
        return CanonicalBytes.copyOf(value.getBytes(StandardCharsets.US_ASCII));
    }

    private static Sha256Digest digest(int lastByte) {
        byte[] bytes = new byte[Sha256Digest.LENGTH];
        bytes[bytes.length - 1] = (byte) lastByte;
        return Sha256Digest.copyOf(bytes);
    }
}
