/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.generation;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.core.capability.GenerationProjectionAuthorityReader;
import com.nereusstream.core.capability.GenerationProjectionAuthoritySnapshot;
import com.nereusstream.core.capability.LiveProjectionSubject;
import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.metadata.oxia.ManagedLedgerFacadeState;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionKeyspace;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerStreamProjection;
import com.nereusstream.metadata.oxia.VersionedTopicProjection;
import com.nereusstream.metadata.oxia.VersionedVirtualLedgerProjection;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Linearizable F2 binding/topic implementation of the protocol-neutral authority reader. */
public final class ManagedLedgerGenerationProjectionAuthorityReader
        implements GenerationProjectionAuthorityReader {
    private static final String ABSENCE_DOMAIN =
            "managed-ledger-generation-projection-absence-v1";

    private final String cluster;
    private final ManagedLedgerProjectionMetadataStore metadata;
    private final ManagedLedgerProjectionKeyspace keys;

    public ManagedLedgerGenerationProjectionAuthorityReader(
            String cluster,
            ManagedLedgerProjectionMetadataStore metadata) {
        this.keys = new ManagedLedgerProjectionKeyspace(cluster);
        this.cluster = requireText(cluster, "cluster");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
    }

    @Override
    public CompletableFuture<GenerationProjectionAuthoritySnapshot> capture(
            LiveProjectionSubject subject) {
        LiveProjectionSubject exact = Objects.requireNonNull(subject, "subject");
        final ManagedLedgerGenerationProjectionRefV1 decoded;
        try {
            decoded =
                    ManagedLedgerGenerationProjectionRefV1.from(exact.projectionRef());
            if (!decoded.identity().streamId().equals(exact.streamId().value())
                    || !decoded.projectionIdentitySha256()
                            .equals(exact.projectionIdentitySha256())) {
                throw new IllegalArgumentException(
                        "projection subject does not match its NPR1 immutable identity");
            }
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return metadata.getProjectionByStream(cluster, exact.streamId())
                .thenApply(view -> snapshot(exact, decoded, view));
    }

    private GenerationProjectionAuthoritySnapshot snapshot(
            LiveProjectionSubject subject,
            ManagedLedgerGenerationProjectionRefV1 decoded,
            ManagedLedgerStreamProjection view) {
        if (!view.streamId().equals(subject.streamId())) {
            throw new IllegalArgumentException(
                    "projection lookup returned another stream");
        }
        String bindingKey = keys.virtualLedgerProjectionKey(subject.streamId());
        if (view.streamBinding().isEmpty()) {
            return nonLive(subject, List.of(absence(bindingKey)));
        }
        VersionedVirtualLedgerProjection binding =
                view.streamBinding().orElseThrow();
        GcAuthorityToken bindingAuthority = new GcAuthorityToken(
                binding.key(),
                binding.metadataVersion(),
                binding.durableValueSha256());
        String topicKey = keys.topicProjectionKey(
                binding.value().managedLedgerName());
        if (view.currentTopic().isEmpty()) {
            return nonLive(
                    subject, List.of(bindingAuthority, absence(topicKey)));
        }
        VersionedTopicProjection topic = view.currentTopic().orElseThrow();
        GcAuthorityToken topicAuthority = new GcAuthorityToken(
                topic.key(),
                topic.metadataVersion(),
                topic.durableValueSha256());
        boolean exactIdentity = binding.value().managedLedgerName()
                        .equals(decoded.managedLedgerName())
                && binding.value().identity().equals(decoded.identity())
                && topic.value().managedLedgerName()
                        .equals(decoded.managedLedgerName())
                && topic.value().projectionIdentity()
                        .equals(decoded.identity());
        ManagedLedgerFacadeState state = topic.value().parsedFacadeState();
        boolean live = exactIdentity
                && (state == ManagedLedgerFacadeState.OPEN
                        || state == ManagedLedgerFacadeState.SEALED);
        if (!live) {
            return nonLive(
                    subject, List.of(bindingAuthority, topicAuthority));
        }
        return new GenerationProjectionAuthoritySnapshot(
                subject,
                true,
                Optional.of(decoded.identity()),
                List.of(bindingAuthority, topicAuthority));
    }

    private static GenerationProjectionAuthoritySnapshot nonLive(
            LiveProjectionSubject subject,
            List<GcAuthorityToken> authorities) {
        return new GenerationProjectionAuthoritySnapshot(
                subject, false, Optional.empty(), authorities);
    }

    private static GcAuthorityToken absence(String key) {
        MessageDigest digest = sha256();
        add(digest, ABSENCE_DOMAIN);
        add(digest, key);
        return new GcAuthorityToken(
                key,
                0,
                new Checksum(
                        ChecksumType.SHA256,
                        HexFormat.of().formatHex(digest.digest())));
    }

    private static void add(MessageDigest digest, String value) {
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
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
