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

package com.nereusstream.kafka.activation;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamState;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationActivationSubject;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.capability.LiveStreamSubject;
import com.nereusstream.materialization.DirectMaterializationStreamAuthority;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Kafka-native generation publication admission rooted in ACTIVE/readiness and a projection-free
 * stream registration.
 *
 * <p>The compaction-aware constructor additionally admits
 * {@code BOOKKEEPER_WAL_ONLY/TOPIC_COMPACTED_PUBLISH} from the live L0 stream authority. It does
 * not create or tolerate an F4 materialization registration for that profile.
 *
 * <p>Partition leadership is deliberately not inferred here. The compaction caller supplies its
 * partition-lock/KRaft guard separately to {@code GenerationCommitter.publish}; both authorities
 * are revalidated before the Generation COMMITTED CAS.
 */
public final class KafkaGenerationProtocolActivationGuard
        implements GenerationProtocolActivationGuard {
    private static final Checksum REFERENCE_DOMAIN_SHA256 =
            sha256("nereus-kafka-direct-stream-publication-domain-v1");

    private final String cluster;
    private final F4Keyspace keyspace;
    private final GenerationMetadataStore generations;
    private final Optional<OxiaMetadataStore> l0Metadata;
    private final KafkaStorageActivationVerifier activationVerifier;
    private final Clock clock;

    public KafkaGenerationProtocolActivationGuard(
            String cluster,
            GenerationMetadataStore generations,
            KafkaStorageActivationVerifier activationVerifier,
            Clock clock) {
        this(cluster, generations, Optional.empty(), activationVerifier, clock);
    }

    public KafkaGenerationProtocolActivationGuard(
            String cluster,
            GenerationMetadataStore generations,
            OxiaMetadataStore l0Metadata,
            KafkaStorageActivationVerifier activationVerifier,
            Clock clock) {
        this(
                cluster,
                generations,
                Optional.of(Objects.requireNonNull(l0Metadata, "l0Metadata")),
                activationVerifier,
                clock);
    }

    private KafkaGenerationProtocolActivationGuard(
            String cluster,
            GenerationMetadataStore generations,
            Optional<OxiaMetadataStore> l0Metadata,
            KafkaStorageActivationVerifier activationVerifier,
            Clock clock) {
        this.cluster = requireText(cluster, "cluster");
        this.keyspace = new F4Keyspace(this.cluster);
        this.generations = Objects.requireNonNull(generations, "generations");
        this.l0Metadata = Objects.requireNonNull(l0Metadata, "l0Metadata");
        this.activationVerifier = Objects.requireNonNull(activationVerifier, "activationVerifier");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletableFuture<GenerationActivationProof> requireReady(
            GenerationOperation operation,
            GenerationActivationSubject subject,
            boolean activateLiveProjectionIfAbsent) {
        LiveStreamSubject live;
        try {
            live = requireCombination(operation, subject, activateLiveProjectionIfAbsent);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return loadAuthority(operation, live)
                .thenCompose(
                        authority ->
                                activationVerifier
                                        .verifyCurrent()
                                        .thenApply(
                                                active ->
                                                        GenerationActivationProof.create(
                                                                operation,
                                                                live,
                                                                authority
                                                                        .validationVersion(),
                                                                active.activation()
                                                                        .metadataVersion(),
                                                                active.readiness()
                                                                        .value()
                                                                        .readinessEpoch(),
                                                                REFERENCE_DOMAIN_SHA256,
                                                                true,
                                                                false,
                                                                Math.max(0, clock.millis()))))
                .toCompletableFuture();
    }

    @Override
    public CompletableFuture<Void> revalidate(GenerationActivationProof proof) {
        GenerationActivationProof exact;
        LiveStreamSubject live;
        try {
            exact = Objects.requireNonNull(proof, "proof");
            live = requireCombination(exact.operation(), exact.subject(), false);
            if (!exact.referenceDomainSetSha256().equals(REFERENCE_DOMAIN_SHA256)
                    || !exact.publicationEnabled()
                    || exact.deletionEnabled()) {
                throw invariant("Kafka generation activation proof uses another capability domain");
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return loadAuthority(exact.operation(), live)
                .thenCompose(
                        authority -> {
                            if (authority.validationVersion()
                                    != exact.subjectValidationVersion()) {
                                return CompletableFuture.failedFuture(
                                        condition(
                                                "Kafka direct-stream authority changed after"
                                                    + " activation proof"));
                            }
                            return activationVerifier
                                    .verifyCurrent()
                                    .thenAccept(
                                            active -> {
                                                if (active.activation().metadataVersion()
                                                                != exact
                                                                        .clusterActivationMetadataVersion()
                                                        || active.readiness()
                                                                        .value()
                                                                        .readinessEpoch()
                                                                != exact
                                                                        .brokerCapabilityReadinessEpoch()) {
                                                    throw condition(
                                                            "Kafka ACTIVE/readiness authority"
                                                                + " changed after activation"
                                                                + " proof");
                                                }
                                            });
                        })
                .toCompletableFuture();
    }

    private CompletableFuture<SubjectAuthority> loadAuthority(
            GenerationOperation operation, LiveStreamSubject subject) {
        return generations
                .getStreamRegistration(cluster, subject.streamId())
                .thenCompose(
                        optional -> {
                            if (optional.isPresent()) {
                                return CompletableFuture.completedFuture(
                                        registrationAuthority(
                                                subject, optional.orElseThrow()));
                            }
                            if (operation == GenerationOperation.TOPIC_COMPACTED_PUBLISH
                                    && l0Metadata.isPresent()) {
                                return l0Metadata
                                        .orElseThrow()
                                        .getStreamSnapshot(cluster, subject.streamId())
                                        .thenApply(
                                                snapshot ->
                                                        walOnlyCompactionAuthority(
                                                                subject, snapshot));
                            }
                            return CompletableFuture.failedFuture(
                                    condition(
                                            "Kafka direct-stream registration is absent"));
                        });
    }

    private SubjectAuthority registrationAuthority(
            LiveStreamSubject subject,
            VersionedMaterializationStreamRegistration registration) {
        MaterializationStreamRegistrationRecord value = registration.value();
        StorageProfile profile;
        try {
            profile = StorageProfile.valueOf(value.storageProfile()).canonical();
        } catch (IllegalArgumentException failure) {
            throw invariant(
                    "Kafka direct-stream registration contains an unknown profile", failure);
        }
        Checksum expected =
                DirectMaterializationStreamAuthority.identitySha256(
                        subject.streamId(), profile);
        if (!registration
                        .key()
                        .equals(keyspace.materializationRegistryKey(subject.streamId()))
                || !value.streamId().equals(subject.streamId().value())
                || !profile.objectMaterializationEnabled()
                || !value.projectionRef()
                        .equals(DirectMaterializationStreamAuthority.encodedProjectionRef())
                || !value.projectionIdentitySha256().equals(expected.value())
                || !subject.streamIdentitySha256().equals(expected)) {
            throw condition(
                    "Kafka direct-stream registration no longer matches publication authority");
        }
        return new SubjectAuthority(registration.metadataVersion());
    }

    private SubjectAuthority walOnlyCompactionAuthority(
            LiveStreamSubject subject, StreamMetadataSnapshot snapshot) {
        StorageProfile profile;
        StreamState state;
        try {
            profile = StorageProfile.valueOf(snapshot.metadata().profile()).canonical();
            state = StreamState.valueOf(snapshot.metadata().state());
        } catch (IllegalArgumentException failure) {
            throw invariant(
                    "Kafka WAL-only compaction authority contains an unknown state/profile",
                    failure);
        }
        Checksum expected =
                DirectMaterializationStreamAuthority.identitySha256(
                        subject.streamId(), profile);
        if (!snapshot.metadata().streamId().equals(subject.streamId().value())
                || profile != StorageProfile.BOOKKEEPER_WAL_ONLY
                || (state != StreamState.ACTIVE && state != StreamState.SEALED)
                || !subject.streamIdentitySha256().equals(expected)) {
            throw condition(
                    "Kafka WAL-only compaction stream no longer matches publication authority");
        }
        return new SubjectAuthority(snapshot.metadata().policyVersion());
    }

    private static LiveStreamSubject requireCombination(
            GenerationOperation operation,
            GenerationActivationSubject subject,
            boolean activateLiveProjectionIfAbsent) {
        GenerationOperation exactOperation = Objects.requireNonNull(operation, "operation");
        GenerationActivationSubject exactSubject = Objects.requireNonNull(subject, "subject");
        if ((exactOperation
                        != GenerationOperation
                                .TOPIC_COMPACTED_PUBLISH
                        && exactOperation
                                != GenerationOperation
                                        .GENERATION_PUBLISH)
                || activateLiveProjectionIfAbsent
                || !(exactSubject instanceof LiveStreamSubject live)) {
            throw new IllegalArgumentException(
                    "Kafka generation guard admits only non-activating direct-stream generation publication");
        }
        return live;
    }

    private static Checksum sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of()
                            .formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8))));
        } catch (NoSuchAlgorithmException failure) {
            throw new ExceptionInInitializerError(failure);
        }
    }

    private static NereusException condition(String message) {
        return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    private static NereusException invariant(String message) {
        return invariant(message, null);
    }

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    private record SubjectAuthority(long validationVersion) {
        private SubjectAuthority {
            if (validationVersion < 0) {
                throw new IllegalArgumentException(
                        "subject authority validation version must be non-negative");
            }
        }
    }
}
