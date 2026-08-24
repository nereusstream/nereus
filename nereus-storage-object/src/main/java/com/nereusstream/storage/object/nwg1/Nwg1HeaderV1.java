/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

import java.util.Arrays;
import java.util.Objects;

/** Immutable semantic form of the exact 256-byte NWG1 v1 Header. */
public final class Nwg1HeaderV1 {
    private final int protocolKind;
    private final long shardId;
    private final long shardRunEpoch;
    private final long laneSequence;
    private final int packingPolicyVersion;
    private final long resolvedTargetPayloadBytes;
    private final long resolvedLingerNanos;
    private final long actualPayloadBytesAtPlanSeal;
    private final long actualCloseLingerNanos;
    private final long directoryPlaintextLength;
    private final long directoryStoredLength;
    private final long bindingContextCount;
    private final long appendUnitCount;
    private final long frameCount;
    private final long directoryPrefixEnd;
    private final long canonicalBodyLength;
    private final int laneId;
    private final int closeReason;
    private final byte[] protocolCellCommitment;
    private final byte[] cellProviderScopeId;
    private final byte[] walRunRootSha256;
    private final byte[] wrappedEnvelopeCommitment;

    @SuppressWarnings("ParameterNumber")
    public Nwg1HeaderV1(
            int protocolKind,
            long shardId,
            long shardRunEpoch,
            long laneSequence,
            int packingPolicyVersion,
            long resolvedTargetPayloadBytes,
            long resolvedLingerNanos,
            long actualPayloadBytesAtPlanSeal,
            long actualCloseLingerNanos,
            long directoryPlaintextLength,
            long directoryStoredLength,
            long bindingContextCount,
            long appendUnitCount,
            long frameCount,
            long directoryPrefixEnd,
            long canonicalBodyLength,
            int laneId,
            int closeReason,
            byte[] protocolCellCommitment,
            byte[] cellProviderScopeId,
            byte[] walRunRootSha256,
            byte[] wrappedEnvelopeCommitment) {
        this.protocolKind = protocolKind;
        this.shardId = shardId;
        this.shardRunEpoch = shardRunEpoch;
        this.laneSequence = laneSequence;
        this.packingPolicyVersion = packingPolicyVersion;
        this.resolvedTargetPayloadBytes = resolvedTargetPayloadBytes;
        this.resolvedLingerNanos = resolvedLingerNanos;
        this.actualPayloadBytesAtPlanSeal = actualPayloadBytesAtPlanSeal;
        this.actualCloseLingerNanos = actualCloseLingerNanos;
        this.directoryPlaintextLength = directoryPlaintextLength;
        this.directoryStoredLength = directoryStoredLength;
        this.bindingContextCount = bindingContextCount;
        this.appendUnitCount = appendUnitCount;
        this.frameCount = frameCount;
        this.directoryPrefixEnd = directoryPrefixEnd;
        this.canonicalBodyLength = canonicalBodyLength;
        this.laneId = laneId;
        this.closeReason = closeReason;
        this.protocolCellCommitment = digest(protocolCellCommitment, "protocolCellCommitment");
        this.cellProviderScopeId = digest(cellProviderScopeId, "cellProviderScopeId");
        this.walRunRootSha256 = nonZeroDigest(walRunRootSha256, "walRunRootSha256");
        this.wrappedEnvelopeCommitment = digest(wrappedEnvelopeCommitment, "wrappedEnvelopeCommitment");
        validate();
    }

    private void validate() {
        require(protocolKind == 1 || protocolKind == 2, "unknown protocol kind");
        require(shardId >= 0 && shardId <= 0xffff_ffffL, "shardId outside u32");
        require(shardRunEpoch >= 0 && laneSequence >= 0, "negative identity tuple");
        require(packingPolicyVersion > 0 && packingPolicyVersion <= 0xffff, "invalid packing policy version");
        require(
                resolvedTargetPayloadBytes > 0
                        && resolvedTargetPayloadBytes <= Nwg1ConstantsV1.MAX_TOTAL_DECODED_PAYLOAD_BYTES,
                "invalid resolved target");
        require(resolvedLingerNanos >= 0 && actualCloseLingerNanos >= 0, "negative linger");
        require(
                actualPayloadBytesAtPlanSeal >= 0
                        && actualPayloadBytesAtPlanSeal <= Nwg1ConstantsV1.MAX_TOTAL_DECODED_PAYLOAD_BYTES,
                "invalid actual payload bytes");
        require(
                directoryPlaintextLength >= 0
                        && directoryPlaintextLength <= Nwg1ConstantsV1.MAX_DIRECTORY_PLAINTEXT_BYTES,
                "invalid directory plaintext length");
        require(
                directoryStoredLength == directoryPlaintextLength + Nwg1ConstantsV1.GCM_TAG_BYTES,
                "directory stored length equation failed");
        require(
                directoryPrefixEnd == Nwg1ConstantsV1.HEADER_BYTES + directoryStoredLength,
                "directory prefix equation failed");
        require(directoryPrefixEnd <= Nwg1ConstantsV1.MAX_DIRECTORY_PREFIX_BYTES, "directory prefix cap exceeded");
        require(
                canonicalBodyLength >= directoryPrefixEnd
                        && canonicalBodyLength <= Nwg1ConstantsV1.MAX_CANONICAL_BODY_BYTES,
                "invalid canonical body length");
        require(
                bindingContextCount > 0 && bindingContextCount <= Nwg1ConstantsV1.MAX_BINDING_CONTEXTS,
                "invalid binding count");
        require(
                appendUnitCount > 0 && appendUnitCount <= Nwg1ConstantsV1.MAX_APPEND_UNITS,
                "invalid append unit count");
        require(frameCount > 0 && frameCount <= Nwg1ConstantsV1.MAX_FRAMES, "invalid frame count");
        require(laneId >= 0 && laneId <= 2, "unknown lane id");
        Nwg1CloseReasonV1.fromCode(closeReason);
        require(
                protocolKind == Nwg1ConstantsV1.PROTOCOL_PULSAR || actualPayloadBytesAtPlanSeal > 0,
                "only Pulsar permits a zero-byte Object");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new Nwg1ValidationException(
                    Nwg1RejectionV1.VALUE_DOMAIN_VIOLATION,
                    Nwg1ValidationStageV1.HEADER_GRAMMAR,
                    Nwg1IsolationScopeV1.SHARED_OBJECT,
                    message);
        }
    }

    private static byte[] digest(byte[] value, String name) {
        Objects.requireNonNull(value, name);
        if (value.length != 32) {
            throw new IllegalArgumentException(name + " must be 32 bytes");
        }
        return Arrays.copyOf(value, value.length);
    }

    private static byte[] nonZeroDigest(byte[] value, String name) {
        byte[] result = digest(value, name);
        int aggregate = 0;
        for (byte item : result) {
            aggregate |= item;
        }
        if (aggregate == 0) {
            throw new Nwg1ValidationException(
                    Nwg1RejectionV1.VALUE_DOMAIN_VIOLATION,
                    Nwg1ValidationStageV1.HEADER_GRAMMAR,
                    Nwg1IsolationScopeV1.SHARED_OBJECT,
                    name + " must be non-zero");
        }
        return result;
    }

    public int protocolKind() {
        return protocolKind;
    }

    public long shardId() {
        return shardId;
    }

    public long shardRunEpoch() {
        return shardRunEpoch;
    }

    public long laneSequence() {
        return laneSequence;
    }

    public int packingPolicyVersion() {
        return packingPolicyVersion;
    }

    public long resolvedTargetPayloadBytes() {
        return resolvedTargetPayloadBytes;
    }

    public long resolvedLingerNanos() {
        return resolvedLingerNanos;
    }

    public long actualPayloadBytesAtPlanSeal() {
        return actualPayloadBytesAtPlanSeal;
    }

    public long actualCloseLingerNanos() {
        return actualCloseLingerNanos;
    }

    public long directoryPlaintextLength() {
        return directoryPlaintextLength;
    }

    public long directoryStoredLength() {
        return directoryStoredLength;
    }

    public long bindingContextCount() {
        return bindingContextCount;
    }

    public long appendUnitCount() {
        return appendUnitCount;
    }

    public long frameCount() {
        return frameCount;
    }

    public long directoryPrefixEnd() {
        return directoryPrefixEnd;
    }

    public long canonicalBodyLength() {
        return canonicalBodyLength;
    }

    public int laneId() {
        return laneId;
    }

    public int closeReason() {
        return closeReason;
    }

    public byte[] protocolCellCommitment() {
        return protocolCellCommitment.clone();
    }

    public byte[] cellProviderScopeId() {
        return cellProviderScopeId.clone();
    }

    public byte[] walRunRootSha256() {
        return walRunRootSha256.clone();
    }

    public byte[] wrappedEnvelopeCommitment() {
        return wrappedEnvelopeCommitment.clone();
    }
}
