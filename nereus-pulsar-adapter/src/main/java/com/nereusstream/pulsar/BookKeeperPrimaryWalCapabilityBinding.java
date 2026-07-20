/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.util.Objects;

/** Secret-free exact binding advertised by a broker that installed the production BK runtime. */
public record BookKeeperPrimaryWalCapabilityBinding(
        int protocolVersion,
        Checksum configurationBindingSha256,
        Checksum ledgerIdNamespaceSha256,
        Checksum activationRecordSha256,
        int requiredObjectGenerationCompletionVersion) {
    public BookKeeperPrimaryWalCapabilityBinding {
        if (protocolVersion != 1 || requiredObjectGenerationCompletionVersion != 1) {
            throw new IllegalArgumentException("unsupported BookKeeper primary-WAL capability version");
        }
        configurationBindingSha256 = sha256(
                configurationBindingSha256, "configurationBindingSha256");
        ledgerIdNamespaceSha256 = sha256(
                ledgerIdNamespaceSha256, "ledgerIdNamespaceSha256");
        activationRecordSha256 = sha256(
                activationRecordSha256, "activationRecordSha256");
    }

    private static Checksum sha256(Checksum value, String name) {
        Checksum exact = Objects.requireNonNull(value, name);
        if (exact.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException(name + " must use SHA256");
        }
        return exact;
    }
}
