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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

/** Pure M5 NPO1-root-before-NPD1-data cleanup ordering; this type performs no external mutation. */
public final class M5PulsarObjectCleanupOrderV1 {
    private static final int MAX_VERSION_TOKEN_BYTES = 1024;
    private static final byte[] TARGET_DOMAIN =
            "NEREUS-M5-PULSAR-CLEANUP-TARGET-V1".getBytes(StandardCharsets.US_ASCII);

    private M5PulsarObjectCleanupOrderV1() {}

    public static CleanupTargetV1 target(
            PulsarSealedLedgerAttemptV1 attempt,
            ObjectIdentity rootIdentity,
            CanonicalBytes rootImmutableVersion,
            ObjectIdentity dataIdentity,
            CanonicalBytes dataImmutableVersion,
            Sha256Digest persistedIntentBindingRoot,
            Sha256Digest m4ReleasedProofRoot,
            Sha256Digest referenceFreeProofRoot,
            Sha256Digest multipartInventoryRoot,
            Sha256Digest providerAdmissionRoot) {
        Sha256Digest targetRoot = targetRoot(
                attempt,
                rootIdentity,
                rootImmutableVersion,
                dataIdentity,
                dataImmutableVersion,
                persistedIntentBindingRoot,
                m4ReleasedProofRoot,
                referenceFreeProofRoot,
                multipartInventoryRoot,
                providerAdmissionRoot);
        return new CleanupTargetV1(
                attempt,
                rootIdentity,
                rootImmutableVersion,
                dataIdentity,
                dataImmutableVersion,
                persistedIntentBindingRoot,
                m4ReleasedProofRoot,
                referenceFreeProofRoot,
                multipartInventoryRoot,
                providerAdmissionRoot,
                targetRoot);
    }

    public static CleanupProgressV1 start(CleanupTargetV1 target) {
        Objects.requireNonNull(target, "target");
        return new CleanupProgressV1(
                target,
                CleanupStageV1.ROOT_ABSENCE_REQUIRED,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    public static TransitionResultV1 observe(CleanupProgressV1 progress, CleanupObservationV1 observation) {
        Objects.requireNonNull(progress, "progress");
        Objects.requireNonNull(observation, "observation");
        if (progress.stage() == CleanupStageV1.QUARANTINED) {
            return TransitionResultV1.unchanged(TransitionOutcomeV1.QUARANTINED, progress);
        }
        if (progress.stage() == CleanupStageV1.AUTHORITATIVELY_ABSENT) {
            return TransitionResultV1.unchanged(TransitionOutcomeV1.COMPLETE, progress);
        }
        CleanupComponentV1 expected = expectedComponent(progress.stage());
        if (observation.component() != expected) {
            return TransitionResultV1.unchanged(TransitionOutcomeV1.ORDER_VIOLATION, progress);
        }
        return switch (observation.reconciliation()) {
            case EXACT_OLD_IDENTITY_REMAINS ->
                TransitionResultV1.unchanged(TransitionOutcomeV1.RETRYABLE_EXACT_REMAINS, progress);
            case OUTCOME_UNKNOWN -> TransitionResultV1.unchanged(TransitionOutcomeV1.OUTCOME_UNKNOWN, progress);
            case DIFFERENT_OR_FOREIGN_IDENTITY -> quarantine(progress, observation.evidenceRoot());
            case AUTHORITATIVELY_ABSENT -> advance(progress, observation.evidenceRoot());
        };
    }

    private static TransitionResultV1 advance(CleanupProgressV1 progress, Sha256Digest evidenceRoot) {
        CleanupProgressV1 advanced =
                switch (progress.stage()) {
                    case ROOT_ABSENCE_REQUIRED ->
                        new CleanupProgressV1(
                                progress.target(),
                                CleanupStageV1.DATA_ABSENCE_REQUIRED,
                                Optional.of(evidenceRoot),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty());
                    case DATA_ABSENCE_REQUIRED ->
                        new CleanupProgressV1(
                                progress.target(),
                                CleanupStageV1.MULTIPART_ABSENCE_REQUIRED,
                                progress.rootAbsenceEvidenceRoot(),
                                Optional.of(evidenceRoot),
                                Optional.empty(),
                                Optional.empty());
                    case MULTIPART_ABSENCE_REQUIRED ->
                        new CleanupProgressV1(
                                progress.target(),
                                CleanupStageV1.AUTHORITATIVELY_ABSENT,
                                progress.rootAbsenceEvidenceRoot(),
                                progress.dataAbsenceEvidenceRoot(),
                                Optional.of(evidenceRoot),
                                Optional.empty());
                    case AUTHORITATIVELY_ABSENT, QUARANTINED ->
                        throw new IllegalStateException("terminal cleanup cannot advance");
                };
        return new TransitionResultV1(
                advanced.stage() == CleanupStageV1.AUTHORITATIVELY_ABSENT
                        ? TransitionOutcomeV1.COMPLETE
                        : TransitionOutcomeV1.ADVANCED,
                advanced);
    }

    private static TransitionResultV1 quarantine(CleanupProgressV1 progress, Sha256Digest evidenceRoot) {
        CleanupProgressV1 quarantined = new CleanupProgressV1(
                progress.target(),
                CleanupStageV1.QUARANTINED,
                progress.rootAbsenceEvidenceRoot(),
                progress.dataAbsenceEvidenceRoot(),
                progress.multipartAbsenceEvidenceRoot(),
                Optional.of(evidenceRoot));
        return new TransitionResultV1(TransitionOutcomeV1.QUARANTINED, quarantined);
    }

    private static CleanupComponentV1 expectedComponent(CleanupStageV1 stage) {
        return switch (stage) {
            case ROOT_ABSENCE_REQUIRED -> CleanupComponentV1.NPO1_ROOT;
            case DATA_ABSENCE_REQUIRED -> CleanupComponentV1.NPD1_DATA;
            case MULTIPART_ABSENCE_REQUIRED -> CleanupComponentV1.OWNED_MULTIPART_RESIDUE;
            case AUTHORITATIVELY_ABSENT, QUARANTINED ->
                throw new IllegalStateException("terminal cleanup has no expected component");
        };
    }

    private static Sha256Digest targetRoot(
            PulsarSealedLedgerAttemptV1 attempt,
            ObjectIdentity rootIdentity,
            CanonicalBytes rootImmutableVersion,
            ObjectIdentity dataIdentity,
            CanonicalBytes dataImmutableVersion,
            Sha256Digest persistedIntentBindingRoot,
            Sha256Digest m4ReleasedProofRoot,
            Sha256Digest referenceFreeProofRoot,
            Sha256Digest multipartInventoryRoot,
            Sha256Digest providerAdmissionRoot) {
        requireTargetMembers(
                attempt,
                rootIdentity,
                rootImmutableVersion,
                dataIdentity,
                dataImmutableVersion,
                persistedIntentBindingRoot,
                m4ReleasedProofRoot,
                referenceFreeProofRoot,
                multipartInventoryRoot,
                providerAdmissionRoot);
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                writeBytes(output, TARGET_DOMAIN);
                output.writeLong(attempt.ledgerId());
                output.writeLong(attempt.attemptUuid().getMostSignificantBits());
                output.writeLong(attempt.attemptUuid().getLeastSignificantBits());
                output.writeLong(attempt.lastAddConfirmed());
                output.writeLong(attempt.entryCount());
                output.writeLong(attempt.logicalLength());
                output.writeLong(attempt.creationTimestampMillis());
                output.writeLong(attempt.metadataVersion());
                writeString(output, attempt.providerScopePrefix());
                writeString(output, attempt.retentionClass().name());
                writeObject(output, rootIdentity);
                writeBytes(output, rootImmutableVersion.toByteArray());
                writeObject(output, dataIdentity);
                writeBytes(output, dataImmutableVersion.toByteArray());
                writeDigest(output, persistedIntentBindingRoot);
                writeDigest(output, m4ReleasedProofRoot);
                writeDigest(output, referenceFreeProofRoot);
                writeDigest(output, multipartInventoryRoot);
                writeDigest(output, providerAdmissionRoot);
            }
            return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory Pulsar cleanup target encoding failed", impossible);
        }
    }

    private static void requireTargetMembers(
            PulsarSealedLedgerAttemptV1 attempt,
            ObjectIdentity rootIdentity,
            CanonicalBytes rootImmutableVersion,
            ObjectIdentity dataIdentity,
            CanonicalBytes dataImmutableVersion,
            Sha256Digest persistedIntentBindingRoot,
            Sha256Digest m4ReleasedProofRoot,
            Sha256Digest referenceFreeProofRoot,
            Sha256Digest multipartInventoryRoot,
            Sha256Digest providerAdmissionRoot) {
        Objects.requireNonNull(attempt, "attempt");
        Objects.requireNonNull(rootIdentity, "rootIdentity");
        Objects.requireNonNull(dataIdentity, "dataIdentity");
        rootImmutableVersion = requireToken(rootImmutableVersion, "rootImmutableVersion");
        dataImmutableVersion = requireToken(dataImmutableVersion, "dataImmutableVersion");
        requireDigest(persistedIntentBindingRoot, "persistedIntentBindingRoot");
        requireDigest(m4ReleasedProofRoot, "m4ReleasedProofRoot");
        requireDigest(referenceFreeProofRoot, "referenceFreeProofRoot");
        requireDigest(multipartInventoryRoot, "multipartInventoryRoot");
        requireDigest(providerAdmissionRoot, "providerAdmissionRoot");
        PulsarOffloadKeysV1 keys = attempt.keys();
        if (attempt.retentionClass() != PulsarSealedLedgerAttemptV1.RetentionClass.DELETE_AFTER_VERIFIED
                || !rootIdentity.key().equals(keys.rootKey())
                || !dataIdentity.key().equals(keys.dataKey())
                || rootIdentity.key().equals(dataIdentity.key())) {
            throw new IllegalArgumentException("Pulsar cleanup target differs from the exact sealed-ledger attempt");
        }
    }

    private static CanonicalBytes requireToken(CanonicalBytes value, String label) {
        Objects.requireNonNull(value, label);
        if (value.isEmpty() || value.length() > MAX_VERSION_TOKEN_BYTES) {
            throw new IllegalArgumentException(label + " is empty or exceeds the canonical cap");
        }
        return value;
    }

    private static void requireDigest(Sha256Digest value, String label) {
        Objects.requireNonNull(value, label);
        if (value.isZero()) {
            throw new IllegalArgumentException(label + " must be non-zero");
        }
    }

    private static void writeObject(DataOutputStream output, ObjectIdentity identity) throws IOException {
        writeString(output, identity.key());
        output.writeLong(identity.bodyLength());
        writeDigest(output, identity.bodySha256());
    }

    private static void writeDigest(DataOutputStream output, Sha256Digest digest) throws IOException {
        writeBytes(output, digest.bytes().toByteArray());
    }

    private static void writeString(DataOutputStream output, String value) throws IOException {
        writeBytes(output, value.getBytes(StandardCharsets.UTF_8));
    }

    private static void writeBytes(DataOutputStream output, byte[] value) throws IOException {
        output.writeInt(value.length);
        output.write(value);
    }

    public enum CleanupComponentV1 {
        NPO1_ROOT,
        NPD1_DATA,
        OWNED_MULTIPART_RESIDUE
    }

    public enum ComponentReconciliationV1 {
        AUTHORITATIVELY_ABSENT,
        EXACT_OLD_IDENTITY_REMAINS,
        DIFFERENT_OR_FOREIGN_IDENTITY,
        OUTCOME_UNKNOWN
    }

    public enum CleanupStageV1 {
        ROOT_ABSENCE_REQUIRED,
        DATA_ABSENCE_REQUIRED,
        MULTIPART_ABSENCE_REQUIRED,
        AUTHORITATIVELY_ABSENT,
        QUARANTINED
    }

    public enum TransitionOutcomeV1 {
        ADVANCED,
        RETRYABLE_EXACT_REMAINS,
        OUTCOME_UNKNOWN,
        ORDER_VIOLATION,
        QUARANTINED,
        COMPLETE
    }

    public record CleanupTargetV1(
            PulsarSealedLedgerAttemptV1 attempt,
            ObjectIdentity rootIdentity,
            CanonicalBytes rootImmutableVersion,
            ObjectIdentity dataIdentity,
            CanonicalBytes dataImmutableVersion,
            Sha256Digest persistedIntentBindingRoot,
            Sha256Digest m4ReleasedProofRoot,
            Sha256Digest referenceFreeProofRoot,
            Sha256Digest multipartInventoryRoot,
            Sha256Digest providerAdmissionRoot,
            Sha256Digest targetRoot) {
        public CleanupTargetV1 {
            requireTargetMembers(
                    attempt,
                    rootIdentity,
                    rootImmutableVersion,
                    dataIdentity,
                    dataImmutableVersion,
                    persistedIntentBindingRoot,
                    m4ReleasedProofRoot,
                    referenceFreeProofRoot,
                    multipartInventoryRoot,
                    providerAdmissionRoot);
            rootImmutableVersion = CanonicalBytes.copyOf(rootImmutableVersion.toByteArray());
            dataImmutableVersion = CanonicalBytes.copyOf(dataImmutableVersion.toByteArray());
            requireDigest(targetRoot, "targetRoot");
            Sha256Digest expected = M5PulsarObjectCleanupOrderV1.targetRoot(
                    attempt,
                    rootIdentity,
                    rootImmutableVersion,
                    dataIdentity,
                    dataImmutableVersion,
                    persistedIntentBindingRoot,
                    m4ReleasedProofRoot,
                    referenceFreeProofRoot,
                    multipartInventoryRoot,
                    providerAdmissionRoot);
            if (!targetRoot.equals(expected)) {
                throw new IllegalArgumentException("Pulsar cleanup target root differs from exact target fields");
            }
        }
    }

    public record CleanupObservationV1(
            CleanupComponentV1 component, ComponentReconciliationV1 reconciliation, Sha256Digest evidenceRoot) {
        public CleanupObservationV1 {
            Objects.requireNonNull(component, "component");
            Objects.requireNonNull(reconciliation, "reconciliation");
            requireDigest(evidenceRoot, "evidenceRoot");
        }
    }

    public record CleanupProgressV1(
            CleanupTargetV1 target,
            CleanupStageV1 stage,
            Optional<Sha256Digest> rootAbsenceEvidenceRoot,
            Optional<Sha256Digest> dataAbsenceEvidenceRoot,
            Optional<Sha256Digest> multipartAbsenceEvidenceRoot,
            Optional<Sha256Digest> quarantineEvidenceRoot) {
        public CleanupProgressV1 {
            Objects.requireNonNull(target, "target");
            Objects.requireNonNull(stage, "stage");
            Objects.requireNonNull(rootAbsenceEvidenceRoot, "rootAbsenceEvidenceRoot");
            Objects.requireNonNull(dataAbsenceEvidenceRoot, "dataAbsenceEvidenceRoot");
            Objects.requireNonNull(multipartAbsenceEvidenceRoot, "multipartAbsenceEvidenceRoot");
            Objects.requireNonNull(quarantineEvidenceRoot, "quarantineEvidenceRoot");
            requireOptionalDigest(rootAbsenceEvidenceRoot, "rootAbsenceEvidenceRoot");
            requireOptionalDigest(dataAbsenceEvidenceRoot, "dataAbsenceEvidenceRoot");
            requireOptionalDigest(multipartAbsenceEvidenceRoot, "multipartAbsenceEvidenceRoot");
            requireOptionalDigest(quarantineEvidenceRoot, "quarantineEvidenceRoot");
            boolean valid =
                    switch (stage) {
                        case ROOT_ABSENCE_REQUIRED ->
                            rootAbsenceEvidenceRoot.isEmpty()
                                    && dataAbsenceEvidenceRoot.isEmpty()
                                    && multipartAbsenceEvidenceRoot.isEmpty()
                                    && quarantineEvidenceRoot.isEmpty();
                        case DATA_ABSENCE_REQUIRED ->
                            rootAbsenceEvidenceRoot.isPresent()
                                    && dataAbsenceEvidenceRoot.isEmpty()
                                    && multipartAbsenceEvidenceRoot.isEmpty()
                                    && quarantineEvidenceRoot.isEmpty();
                        case MULTIPART_ABSENCE_REQUIRED ->
                            rootAbsenceEvidenceRoot.isPresent()
                                    && dataAbsenceEvidenceRoot.isPresent()
                                    && multipartAbsenceEvidenceRoot.isEmpty()
                                    && quarantineEvidenceRoot.isEmpty();
                        case AUTHORITATIVELY_ABSENT ->
                            rootAbsenceEvidenceRoot.isPresent()
                                    && dataAbsenceEvidenceRoot.isPresent()
                                    && multipartAbsenceEvidenceRoot.isPresent()
                                    && quarantineEvidenceRoot.isEmpty();
                        case QUARANTINED -> quarantineEvidenceRoot.isPresent();
                    };
            if (!valid) {
                throw new IllegalArgumentException("Pulsar cleanup stage and evidence roots disagree");
            }
        }
    }

    private static void requireOptionalDigest(Optional<Sha256Digest> value, String label) {
        if (value.isPresent()) {
            requireDigest(value.orElseThrow(), label);
        }
    }

    public record TransitionResultV1(TransitionOutcomeV1 outcome, CleanupProgressV1 progress) {
        public TransitionResultV1 {
            Objects.requireNonNull(outcome, "outcome");
            Objects.requireNonNull(progress, "progress");
        }

        static TransitionResultV1 unchanged(TransitionOutcomeV1 outcome, CleanupProgressV1 progress) {
            return new TransitionResultV1(outcome, progress);
        }
    }
}
