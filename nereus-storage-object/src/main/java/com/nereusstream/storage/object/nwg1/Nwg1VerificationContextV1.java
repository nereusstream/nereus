/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

import com.nereusstream.domain.codec.ProtocolCellIdentityCodecV1;
import com.nereusstream.domain.protocol.ProtocolCellIdentity;
import java.util.Objects;

/** Root-bound authorities and protocol validation capabilities required by every production read. */
public final class Nwg1VerificationContextV1 {
    @FunctionalInterface
    public interface OwnerWitnessProvider {
        byte[] canonicalWitness(byte[] bindingId, int ownerFenceKind, int ownerFenceVersion);
    }

    public interface NativePayloadVerifier {
        NativeCoverage validateKafka(
                byte[] exactAssignedBytes,
                long partitionId,
                long kafkaLeaderEpoch,
                long expectedCoverageStart,
                long expectedCoverageEnd);
    }

    public record NativeCoverage(long startInclusive, long endExclusive) {
        public NativeCoverage {
            if (startInclusive < 0 || endExclusive <= startInclusive) {
                throw new IllegalArgumentException("invalid native coverage");
            }
        }
    }

    private final ProtocolCellIdentity protocolCell;
    private final byte[] cellProviderScopeId;
    private final byte[] walRunRootSha256;
    private final Nwg1EnvelopeV1 envelope;
    private final OwnerWitnessProvider ownerWitnessProvider;
    private final NativePayloadVerifier nativePayloadVerifier;
    private final long pulsarSliceBaseInclusive;
    private final long pulsarSliceEndExclusive;

    public Nwg1VerificationContextV1(
            ProtocolCellIdentity protocolCell,
            byte[] cellProviderScopeId,
            byte[] walRunRootSha256,
            Nwg1EnvelopeV1 envelope,
            OwnerWitnessProvider ownerWitnessProvider,
            NativePayloadVerifier nativePayloadVerifier,
            long pulsarSliceBaseInclusive,
            long pulsarSliceEndExclusive) {
        this.protocolCell = Objects.requireNonNull(protocolCell, "protocolCell");
        this.cellProviderScopeId = exact32(cellProviderScopeId, "cellProviderScopeId");
        this.walRunRootSha256 = exact32(walRunRootSha256, "walRunRootSha256");
        this.envelope = Objects.requireNonNull(envelope, "envelope");
        this.ownerWitnessProvider = Objects.requireNonNull(ownerWitnessProvider, "ownerWitnessProvider");
        this.nativePayloadVerifier = Objects.requireNonNull(nativePayloadVerifier, "nativePayloadVerifier");
        if (pulsarSliceBaseInclusive < 0 || pulsarSliceEndExclusive < pulsarSliceBaseInclusive) {
            throw new IllegalArgumentException("invalid Pulsar admitted slice");
        }
        if (protocolCell.protocolKind().code() == 2
                && Math.subtractExact(pulsarSliceEndExclusive, pulsarSliceBaseInclusive) != (1L << 40)) {
            throw new IllegalArgumentException("Pulsar admitted slice must be exactly 2^40 ledger IDs");
        }
        this.pulsarSliceBaseInclusive = pulsarSliceBaseInclusive;
        this.pulsarSliceEndExclusive = pulsarSliceEndExclusive;
    }

    public ProtocolCellIdentity protocolCell() {
        return protocolCell;
    }

    public byte[] exactNpc1() {
        return ProtocolCellIdentityCodecV1.encode(protocolCell).toByteArray();
    }

    public byte[] cellProviderScopeId() {
        return cellProviderScopeId.clone();
    }

    public byte[] walRunRootSha256() {
        return walRunRootSha256.clone();
    }

    public Nwg1EnvelopeV1 envelope() {
        return envelope;
    }

    public OwnerWitnessProvider ownerWitnessProvider() {
        return ownerWitnessProvider;
    }

    public NativePayloadVerifier nativePayloadVerifier() {
        return nativePayloadVerifier;
    }

    public boolean admitsPulsarLedger(long ledgerId) {
        return ledgerId >= pulsarSliceBaseInclusive && ledgerId < pulsarSliceEndExclusive;
    }

    private static byte[] exact32(byte[] value, String field) {
        Objects.requireNonNull(value, field);
        if (value.length != 32) {
            throw new IllegalArgumentException(field + " must be 32 bytes");
        }
        return value.clone();
    }
}
