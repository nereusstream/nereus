/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

import java.util.Objects;

/**
 * Exact Root-bound authority fields injected after the Root itself has been verified.
 *
 * <p>This is deliberately not a Root wire codec. M3 synthetic fixtures do not freeze the complete Root or Pointer
 * wire.
 */
public record Nwg1RootAuthorityV1(
        byte[] exactNpc1,
        byte[] protocolCellCommitment,
        byte[] cellProviderScopeId,
        byte[] walRunRootSha256,
        byte[] framedEnvelope,
        byte[] wrappedEnvelopeCommitment) {
    public Nwg1RootAuthorityV1 {
        exactNpc1 = nonEmpty(exactNpc1, "exactNpc1");
        protocolCellCommitment = exact32(protocolCellCommitment, "protocolCellCommitment");
        cellProviderScopeId = exact32(cellProviderScopeId, "cellProviderScopeId");
        walRunRootSha256 = exact32(walRunRootSha256, "walRunRootSha256");
        framedEnvelope = nonEmpty(framedEnvelope, "framedEnvelope");
        wrappedEnvelopeCommitment = exact32(wrappedEnvelopeCommitment, "wrappedEnvelopeCommitment");
    }

    @Override
    public byte[] exactNpc1() {
        return exactNpc1.clone();
    }

    @Override
    public byte[] protocolCellCommitment() {
        return protocolCellCommitment.clone();
    }

    @Override
    public byte[] cellProviderScopeId() {
        return cellProviderScopeId.clone();
    }

    @Override
    public byte[] walRunRootSha256() {
        return walRunRootSha256.clone();
    }

    @Override
    public byte[] framedEnvelope() {
        return framedEnvelope.clone();
    }

    @Override
    public byte[] wrappedEnvelopeCommitment() {
        return wrappedEnvelopeCommitment.clone();
    }

    private static byte[] nonEmpty(byte[] value, String field) {
        Objects.requireNonNull(value, field);
        if (value.length == 0) {
            throw new IllegalArgumentException(field + " must not be empty");
        }
        return value.clone();
    }

    private static byte[] exact32(byte[] value, String field) {
        Objects.requireNonNull(value, field);
        if (value.length != 32) {
            throw new IllegalArgumentException(field + " must be 32 bytes");
        }
        return value.clone();
    }
}
