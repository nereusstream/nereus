/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.generation;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamState;
import com.nereusstream.core.capability.DomainValidatedDeletionSubject;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationActivationSubject;
import com.nereusstream.core.capability.GenerationCapabilityReadiness;
import com.nereusstream.core.capability.GenerationCapabilityReadinessProvider;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.GenerationProjectionAuthoritySnapshot;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.capability.LiveProjectionSubject;
import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.core.physical.GcReferenceDomain;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStore;
import com.nereusstream.metadata.oxia.ManagedLedgerFacadeState;
import com.nereusstream.metadata.oxia.ManagedLedgerGenerationProtocol;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionKeyspace;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedGenerationProtocolActivation;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationLifecycle;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.ReferenceDomainVersionRecord;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

/**
 * Product-owned F4 mutation guard that binds broker readiness, cluster
 * activation, exact projection/registration truth, and the monotonic topic
 * marker into one short-lived proof.
 */
public final class ManagedLedgerGenerationProtocolActivationGuard
        implements GenerationProtocolActivationGuard {
    private static final String REFERENCE_DOMAIN_DIGEST =
            "nereus-generation-reference-domain-set-v1";
    private static final String PROJECTION_DOMAIN_ID =
            "projection-generation-v1";
    private static final int PROJECTION_DOMAIN_VERSION = 1;
    private static final Map<String, String> PAYLOAD_ATTRIBUTES = Map.of(
            ManagedLedgerProjectionNames.PAYLOAD_MAPPING_ATTRIBUTE,
            ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1);

    private final String cluster;
    private final boolean firstActivationEnabled;
    private final GenerationCapabilityReadinessProvider readinessProvider;
    private final GenerationProtocolActivationStore activations;
    private final List<ReferenceDomainVersionRecord> requiredDomains;
    private final Checksum referenceDomainSetSha256;
    private final ManagedLedgerProjectionMetadataStore projections;
    private final ManagedLedgerGenerationProjectionAuthorityReader projectionReader;
    private final ManagedLedgerProjectionKeyspace projectionKeys;
    private final OxiaMetadataStore l0;
    private final GenerationMetadataStore generations;
    private final GcReferenceDomain projectionReferenceDomain;
    private final Clock clock;

    public ManagedLedgerGenerationProtocolActivationGuard(
            String cluster,
            boolean firstActivationEnabled,
            GenerationCapabilityReadinessProvider readinessProvider,
            GenerationProtocolActivationStore activations,
            List<ReferenceDomainVersionRecord> requiredDomains,
            ManagedLedgerProjectionMetadataStore projections,
            OxiaMetadataStore l0,
            GenerationMetadataStore generations,
            GcReferenceDomain projectionReferenceDomain,
            Clock clock) {
        this.cluster = new OxiaKeyspace(cluster).cluster();
        this.firstActivationEnabled = firstActivationEnabled;
        this.readinessProvider = Objects.requireNonNull(
                readinessProvider, "readinessProvider");
        this.activations = Objects.requireNonNull(
                activations, "activations");
        this.requiredDomains = canonicalDomains(requiredDomains);
        this.referenceDomainSetSha256 =
                referenceDomainSetSha256(this.requiredDomains);
        this.projections = Objects.requireNonNull(
                projections, "projections");
        this.projectionReader =
                new ManagedLedgerGenerationProjectionAuthorityReader(
                        this.cluster, projections);
        this.projectionKeys =
                new ManagedLedgerProjectionKeyspace(this.cluster);
        this.l0 = Objects.requireNonNull(l0, "l0");
        this.generations = Objects.requireNonNull(
                generations, "generations");
        this.projectionReferenceDomain = Objects.requireNonNull(
                projectionReferenceDomain, "projectionReferenceDomain");
        if (!PROJECTION_DOMAIN_ID.equals(
                        projectionReferenceDomain.domainId())
                || projectionReferenceDomain.protocolVersion()
                        != PROJECTION_DOMAIN_VERSION
                || this.requiredDomains.stream().noneMatch(
                        value -> value.domainId()
                                        .equals(PROJECTION_DOMAIN_ID)
                                && value.protocolVersion()
                                        == PROJECTION_DOMAIN_VERSION)) {
            throw new IllegalArgumentException(
                    "projectionReferenceDomain must be the installed projection-generation-v1 domain");
        }
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletableFuture<GenerationActivationProof> requireReady(
            GenerationOperation operation,
            GenerationActivationSubject subject,
            boolean activateLiveProjectionIfAbsent) {
        final GenerationOperation exactOperation;
        final GenerationActivationSubject exactSubject;
        try {
            exactOperation = Objects.requireNonNull(
                    operation, "operation");
            exactSubject = Objects.requireNonNull(subject, "subject");
            requireSubjectCombination(
                    exactOperation,
                    exactSubject,
                    activateLiveProjectionIfAbsent);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return loadClusterAuthority(exactOperation)
                .thenCompose(clusterAuthority -> {
                    if (exactSubject
                            instanceof LiveProjectionSubject live) {
                        return requireLiveReady(
                                exactOperation,
                                live,
                                activateLiveProjectionIfAbsent,
                                clusterAuthority);
                    }
                    return requireDeletionReady(
                            exactOperation,
                            (DomainValidatedDeletionSubject)
                                    exactSubject,
                            clusterAuthority);
                });
    }

    @Override
    public CompletableFuture<Void> revalidate(
            GenerationActivationProof proof) {
        final GenerationActivationProof exact;
        try {
            exact = Objects.requireNonNull(proof, "proof");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return revalidateClusterProof(exact)
                .thenCompose(ignored -> {
                    if (exact.subject()
                            instanceof LiveProjectionSubject live) {
                        return captureLive(live)
                                .thenAccept(authority -> {
                                    if (!authority.markerActivated()) {
                                        throw notReady(
                                                "generation marker is absent during proof revalidation");
                                    }
                                    if (authority.topicMetadataVersion()
                                            != exact.subjectValidationVersion()) {
                                        throw notReady(
                                                "projection metadata version changed after activation proof");
                                    }
                                });
                    }
                    return captureDeletion(
                                    (DomainValidatedDeletionSubject)
                                            exact.subject())
                            .thenAccept(ignoredSnapshot -> {
                            });
                });
    }

    private CompletableFuture<GenerationActivationProof>
            requireLiveReady(
                    GenerationOperation operation,
                    LiveProjectionSubject subject,
                    boolean activateIfAbsent,
                    ClusterAuthority clusterAuthority) {
        return captureLive(subject)
                .thenCompose(authority -> {
                    if (authority.markerActivated()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    if (!activateIfAbsent) {
                        return CompletableFuture.failedFuture(notReady(
                                "generation marker is absent"));
                    }
                    if (!firstActivationEnabled) {
                        return CompletableFuture.failedFuture(notReady(
                                "first generation activation is disabled"));
                    }
                    return activateMarker(subject, authority);
                })
                .thenCompose(ignored -> revalidateClusterAuthority(
                        operation, clusterAuthority))
                .thenCompose(ignored -> captureLive(subject))
                .thenApply(authority -> {
                    if (!authority.markerActivated()) {
                        throw notReady(
                                "generation marker is absent after activation");
                    }
                    return GenerationActivationProof.create(
                            operation,
                            subject,
                            authority.topicMetadataVersion(),
                            clusterAuthority.activation()
                                    .metadataVersion(),
                            clusterAuthority.readiness()
                                    .brokerReadinessEpoch(),
                            referenceDomainSetSha256,
                            clusterAuthority.activation()
                                    .value()
                                    .publicationEnabled(),
                            deletionEnabled(
                                    clusterAuthority.activation()
                                            .value()),
                            Math.max(0, clock.millis()));
                });
    }

    private CompletableFuture<GenerationActivationProof>
            requireDeletionReady(
                    GenerationOperation operation,
                    DomainValidatedDeletionSubject subject,
                    ClusterAuthority clusterAuthority) {
        return captureDeletion(subject)
                .thenCompose(ignored -> revalidateClusterAuthority(
                        operation, clusterAuthority))
                .thenCompose(ignored -> captureDeletion(subject))
                .thenApply(ignored -> GenerationActivationProof.create(
                        operation,
                        subject,
                        0,
                        clusterAuthority.activation()
                                .metadataVersion(),
                        clusterAuthority.readiness()
                                .brokerReadinessEpoch(),
                        referenceDomainSetSha256,
                        clusterAuthority.activation()
                                .value()
                                .publicationEnabled(),
                        deletionEnabled(
                                clusterAuthority.activation()
                                    .value()),
                        Math.max(0, clock.millis())));
    }

    private CompletableFuture<ClusterAuthority> loadClusterAuthority(
            GenerationOperation operation) {
        return readinessProvider
                .requireGenerationCapabilityReadiness()
                .thenCompose(readiness -> activations
                        .get(cluster)
                        .thenApply(optional -> {
                            VersionedGenerationProtocolActivation
                                    activation =
                                            optional.orElseThrow(() ->
                                                    notReady(
                                                            "generation cluster activation is absent"));
                            requireClusterRecord(
                                    operation,
                                    readiness,
                                    activation);
                            return new ClusterAuthority(
                                    readiness, activation);
                        }));
    }

    private CompletableFuture<Void> revalidateClusterAuthority(
            GenerationOperation operation,
            ClusterAuthority expected) {
        Optional<GenerationCapabilityReadiness> current =
                readinessProvider
                        .currentGenerationCapabilityReadiness();
        if (current.isEmpty()
                || !current.orElseThrow()
                        .equals(expected.readiness())) {
            return CompletableFuture.failedFuture(notReady(
                    "broker generation readiness changed around activation proof"));
        }
        return activations.get(cluster)
                .thenAccept(optional -> {
                    VersionedGenerationProtocolActivation activation =
                            optional.orElseThrow(() -> notReady(
                                    "generation cluster activation disappeared"));
                    if (!activation.equals(expected.activation())) {
                        throw notReady(
                                "generation cluster activation changed around proof");
                    }
                    requireClusterRecord(
                            operation,
                            current.orElseThrow(),
                            activation);
                });
    }

    private CompletableFuture<Void> revalidateClusterProof(
            GenerationActivationProof proof) {
        Optional<GenerationCapabilityReadiness> current =
                readinessProvider
                        .currentGenerationCapabilityReadiness();
        if (current.isEmpty()
                || current.orElseThrow()
                                .brokerReadinessEpoch()
                        != proof.brokerCapabilityReadinessEpoch()) {
            return CompletableFuture.failedFuture(notReady(
                    "broker generation readiness is unavailable or changed"));
        }
        return activations.get(cluster)
                .thenAccept(optional -> {
                    VersionedGenerationProtocolActivation activation =
                            optional.orElseThrow(() -> notReady(
                                    "generation cluster activation disappeared"));
                    if (activation.metadataVersion()
                            != proof.clusterActivationMetadataVersion()) {
                        throw notReady(
                                "generation cluster activation metadata version changed");
                    }
                    requireClusterRecord(
                            proof.operation(),
                            current.orElseThrow(),
                            activation);
                    if (!referenceDomainSetSha256.equals(
                                    proof.referenceDomainSetSha256())
                            || activation.value().publicationEnabled()
                                    != proof.publicationEnabled()
                            || deletionEnabled(activation.value())
                                    != proof.deletionEnabled()) {
                        throw notReady(
                                "generation activation proof capability facts changed");
                    }
                });
    }

    private void requireClusterRecord(
            GenerationOperation operation,
            GenerationCapabilityReadiness readiness,
            VersionedGenerationProtocolActivation activation) {
        GenerationProtocolActivationRecord value =
                activation.value();
        if (value.lifecycle()
                        != GenerationProtocolActivationLifecycle.ACTIVE
                || !value.publicationEnabled()) {
            throw notReady(
                    "generation publication is not active");
        }
        if (!value.requiredReferenceDomains()
                .equals(requiredDomains)) {
            throw invariant(
                    "durable generation reference-domain set differs from the local runtime");
        }
        if (value.brokerCapabilityReadinessEpoch()
                        != readiness.brokerReadinessEpoch()
                || !value.streamRegistrationBackfill().complete()
                || value.streamRegistrationBackfill()
                                .brokerReadinessEpoch()
                        != readiness.brokerReadinessEpoch()) {
            throw notReady(
                    "generation activation does not carry the current registration coverage proof");
        }
        if (requiresDeletion(operation)
                && (!deletionEnabled(value)
                        || !value.physicalRootBackfill().complete()
                        || !value.cursorSnapshotBackfill().complete()
                        || value.physicalRootBackfill()
                                        .brokerReadinessEpoch()
                                != readiness.brokerReadinessEpoch()
                        || value.cursorSnapshotBackfill()
                                        .brokerReadinessEpoch()
                                != readiness.brokerReadinessEpoch()
                        || value.objectStoreCapabilitySha256()
                                .isEmpty())) {
            throw notReady(
                    "generation physical deletion is not active for the current readiness epoch");
        }
    }

    private CompletableFuture<LiveAuthority> captureLive(
            LiveProjectionSubject subject) {
        final ManagedLedgerGenerationProjectionRefV1 decoded;
        try {
            decoded = ManagedLedgerGenerationProjectionRefV1.from(
                    subject.projectionRef());
            if (!decoded.identity().streamId()
                            .equals(subject.streamId().value())
                    || !decoded.projectionIdentitySha256()
                            .equals(subject.projectionIdentitySha256())) {
                throw invariant(
                        "live activation subject does not match its NPR1 identity");
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return projectionReader.capture(subject)
                .thenCompose(snapshot -> {
                    if (!snapshot.live()
                            || !snapshot.managedLedgerIdentity()
                                    .equals(Optional.of(
                                            decoded.identity()))) {
                        return CompletableFuture.failedFuture(notReady(
                                "live projection activation subject is no longer authoritative"));
                    }
                    long topicVersion = topicMetadataVersion(
                            decoded, snapshot);
                    return projections
                            .getProjection(
                                    cluster,
                                    decoded.managedLedgerName())
                            .thenCompose(optional -> {
                                TopicProjectionRecord projection =
                                        optional.orElseThrow(() ->
                                                notReady(
                                                        "authoritative topic projection is absent"));
                                requireProjection(
                                        subject,
                                        decoded,
                                        topicVersion,
                                        projection);
                                return l0.getStreamSnapshot(
                                                cluster,
                                                subject.streamId())
                                        .thenCompose(stream -> {
                                            requireL0(
                                                    projection,
                                                    stream);
                                            return generations
                                                    .getStreamRegistration(
                                                            cluster,
                                                            subject.streamId())
                                                    .thenApply(registration -> {
                                                        requireRegistration(
                                                                subject,
                                                                projection,
                                                                registration);
                                                        return new LiveAuthority(
                                                                decoded,
                                                                topicVersion,
                                                                ManagedLedgerGenerationProtocol
                                                                        .isActivated(
                                                                                projection));
                                                    });
                                        });
                            });
                });
    }

    private CompletableFuture<Void> activateMarker(
            LiveProjectionSubject subject,
            LiveAuthority authority) {
        CompletableFuture<TopicProjectionRecord> write =
                projections.activateGenerationProtocol(
                        cluster,
                        authority.reference().managedLedgerName(),
                        authority.reference().identity(),
                        authority.topicMetadataVersion());
        return write.handle((activated, failure) -> {
                    if (failure == null) {
                        return CompletableFuture.<Void>completedFuture(
                                null);
                    }
                    Throwable original = unwrap(failure);
                    return captureLive(subject)
                            .thenCompose(current -> current
                                            .markerActivated()
                                    ? CompletableFuture
                                            .<Void>completedFuture(null)
                                    : CompletableFuture
                                            .<Void>failedFuture(original));
                })
                .thenCompose(Function.identity());
    }

    private CompletableFuture<GcReferenceSnapshot> captureDeletion(
            DomainValidatedDeletionSubject subject) {
        return projectionReferenceDomain
                .snapshot(subject.referenceQuery())
                .thenApply(snapshot -> {
                    if (!PROJECTION_DOMAIN_ID.equals(
                                    snapshot.domainId())
                            || snapshot.protocolVersion()
                                    != PROJECTION_DOMAIN_VERSION
                            || !snapshot.queryIdentitySha256()
                                    .equals(subject
                                            .referenceQuery()
                                            .queryIdentitySha256())
                            || !snapshot.complete()
                            || snapshot.veto()
                            || !snapshot.snapshotSha256()
                                    .equals(subject
                                            .projectionDomainSnapshotSha256())) {
                        throw notReady(
                                "projection-generation deletion authority changed or vetoed");
                    }
                    return snapshot;
                });
    }

    private long topicMetadataVersion(
            ManagedLedgerGenerationProjectionRefV1 decoded,
            GenerationProjectionAuthoritySnapshot snapshot) {
        String topicKey = projectionKeys.topicProjectionKey(
                decoded.managedLedgerName());
        List<GcAuthorityToken> matches =
                snapshot.authorities().stream()
                        .filter(authority -> authority
                                .authorityKey()
                                .equals(topicKey))
                        .toList();
        if (matches.size() != 1
                || matches.get(0).metadataVersion() < 0) {
            throw invariant(
                    "projection authority snapshot lacks one exact topic authority");
        }
        return matches.get(0).metadataVersion();
    }

    private static void requireProjection(
            LiveProjectionSubject subject,
            ManagedLedgerGenerationProjectionRefV1 decoded,
            long topicVersion,
            TopicProjectionRecord projection) {
        ManagedLedgerFacadeState state =
                projection.parsedFacadeState();
        if (!projection.managedLedgerName()
                        .equals(decoded.managedLedgerName())
                || !projection.projectionIdentity()
                        .equals(decoded.identity())
                || !projection.streamId()
                        .equals(subject.streamId().value())
                || projection.metadataVersion() != topicVersion) {
            throw notReady(
                    "topic projection changed during activation capture");
        }
        if (state != ManagedLedgerFacadeState.OPEN
                && state != ManagedLedgerFacadeState.SEALED) {
            throw notReady(
                    "topic projection is not live");
        }
    }

    private static void requireL0(
            TopicProjectionRecord projection,
            StreamMetadataSnapshot stream) {
        if (!stream.metadata().streamId()
                        .equals(projection.streamId())
                || !stream.metadata().streamName()
                        .equals(projection.streamName())
                || !stream.metadata().profile()
                        .equals(projection.storageProfile())
                || !stream.metadata().attributes()
                        .equals(PAYLOAD_ATTRIBUTES)
                || stream.metadata().createdAtMillis()
                        != projection.createdAtMillis()) {
            throw invariant(
                    "L0 stream identity/profile differs from topic projection");
        }
        final StorageProfile profile;
        final StreamState state;
        try {
            profile = StorageProfile.valueOf(
                    stream.metadata().profile());
            state = StreamState.valueOf(
                    stream.metadata().state());
        } catch (IllegalArgumentException failure) {
            throw invariant(
                    "L0 stream has an unknown profile or lifecycle",
                    failure);
        }
        if (profile.canonical()
                        != StorageProfile.valueOf(
                                        projection.storageProfile())
                                .canonical()
                || (state != StreamState.ACTIVE
                        && state != StreamState.SEALED)) {
            throw notReady(
                    "L0 stream is not live under the projection profile");
        }
        if (projection.parsedFacadeState()
                                == ManagedLedgerFacadeState.SEALED
                && state != StreamState.SEALED) {
            throw invariant(
                    "topic projection lifecycle leads L0 stream truth");
        }
    }

    private static void requireRegistration(
            LiveProjectionSubject subject,
            TopicProjectionRecord projection,
            Optional<VersionedMaterializationStreamRegistration>
                    optional) {
        VersionedMaterializationStreamRegistration registration =
                optional.orElseThrow(() -> notReady(
                        "materialization stream registration is absent"));
        MaterializationStreamRegistrationRecord value =
                registration.value();
        if (!value.streamId()
                        .equals(subject.streamId().value())
                || !ProjectionIdentity.decode(value.projectionRef())
                        .equals(Optional.of(
                                subject.projectionRef()))
                || !value.projectionIdentitySha256()
                        .equals(subject
                                .projectionIdentitySha256()
                                .value())
                || !value.storageProfile()
                        .equals(projection.storageProfile())) {
            throw invariant(
                    "materialization registration identity conflicts with projection");
        }
    }

    private static void requireSubjectCombination(
            GenerationOperation operation,
            GenerationActivationSubject subject,
            boolean activateLiveProjectionIfAbsent) {
        if (subject instanceof DomainValidatedDeletionSubject) {
            if (operation != GenerationOperation.PHYSICAL_DELETE
                    || activateLiveProjectionIfAbsent) {
                throw new IllegalArgumentException(
                        "domain-validated subjects are only legal for non-activating physical delete");
            }
            return;
        }
        if (!(subject instanceof LiveProjectionSubject)
                || operation == GenerationOperation.PHYSICAL_DELETE) {
            throw new IllegalArgumentException(
                    "live projection subjects are required for non-physical-delete operations");
        }
    }

    private static boolean requiresDeletion(
            GenerationOperation operation) {
        return operation == GenerationOperation.PHYSICAL_DELETE
                || operation == GenerationOperation.LOGICAL_TRIM;
    }

    private static boolean deletionEnabled(
            GenerationProtocolActivationRecord value) {
        return value.physicalDeleteEnabled()
                && value.cursorSnapshotDeleteEnabled();
    }

    private static List<ReferenceDomainVersionRecord> canonicalDomains(
            List<ReferenceDomainVersionRecord> supplied) {
        List<ReferenceDomainVersionRecord> domains =
                Objects.requireNonNull(
                                supplied, "requiredDomains")
                        .stream()
                        .sorted(Comparator.naturalOrder())
                        .toList();
        if (domains.isEmpty()
                || domains.size()
                        > GenerationProtocolActivationRecord
                                .MAX_REFERENCE_DOMAINS) {
            throw new IllegalArgumentException(
                    "requiredDomains must be non-empty and bounded");
        }
        for (int index = 1; index < domains.size(); index++) {
            if (domains.get(index - 1)
                            .compareTo(domains.get(index))
                    >= 0
                    || domains.get(index - 1)
                            .domainId()
                            .equals(domains.get(index).domainId())) {
                throw new IllegalArgumentException(
                        "requiredDomains must be unique");
            }
        }
        return domains;
    }

    private static Checksum referenceDomainSetSha256(
            List<ReferenceDomainVersionRecord> domains) {
        MessageDigest digest = sha256();
        add(digest, REFERENCE_DOMAIN_DIGEST);
        digest.update(ByteBuffer.allocate(Integer.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(domains.size())
                .array());
        for (ReferenceDomainVersionRecord domain : domains) {
            add(digest, domain.domainId());
            digest.update(ByteBuffer.allocate(Integer.BYTES)
                    .order(ByteOrder.BIG_ENDIAN)
                    .putInt(domain.protocolVersion())
                    .array());
        }
        return new Checksum(
                ChecksumType.SHA256,
                HexFormat.of().formatHex(digest.digest()));
    }

    private static void add(
            MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(bytes.length)
                .array());
        digest.update(bytes);
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException(
                    "SHA-256 is unavailable", failure);
        }
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException notReady(String message) {
        return new NereusException(
                ErrorCode.METADATA_CONDITION_FAILED,
                true,
                message);
    }

    private static NereusException invariant(String message) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                false,
                message);
    }

    private static NereusException invariant(
            String message, Throwable cause) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                false,
                message,
                cause);
    }

    private record ClusterAuthority(
            GenerationCapabilityReadiness readiness,
            VersionedGenerationProtocolActivation activation) {
        private ClusterAuthority {
            Objects.requireNonNull(readiness, "readiness");
            Objects.requireNonNull(activation, "activation");
        }
    }

    private record LiveAuthority(
            ManagedLedgerGenerationProjectionRefV1 reference,
            long topicMetadataVersion,
            boolean markerActivated) {
        private LiveAuthority {
            Objects.requireNonNull(reference, "reference");
            if (topicMetadataVersion < 0) {
                throw new IllegalArgumentException(
                        "topicMetadataVersion must be non-negative");
            }
        }
    }
}
