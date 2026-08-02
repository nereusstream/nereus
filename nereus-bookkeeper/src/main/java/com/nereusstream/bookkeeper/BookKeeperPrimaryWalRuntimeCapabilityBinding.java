/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.bookkeeper;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.util.Objects;

/**
 * Secret-free exact binding installed by a provider-neutral BookKeeper primary-WAL runtime.
 *
 * <p>Adapters may project this value into their own broker capability records, but must not
 * recompute or weaken any digest.
 */
public record BookKeeperPrimaryWalRuntimeCapabilityBinding(
        int protocolVersion,
        Checksum configurationBindingSha256,
        Checksum ledgerIdNamespaceSha256,
        Checksum publicationActivationSha256,
        int requiredObjectGenerationCompletionVersion) {
    public BookKeeperPrimaryWalRuntimeCapabilityBinding {
        if (protocolVersion != 1 || requiredObjectGenerationCompletionVersion != 1) {
            throw new IllegalArgumentException("unsupported BookKeeper primary-WAL capability version");
        }
        configurationBindingSha256 = sha256(configurationBindingSha256, "configurationBindingSha256");
        ledgerIdNamespaceSha256 = sha256(ledgerIdNamespaceSha256, "ledgerIdNamespaceSha256");
        publicationActivationSha256 = sha256(publicationActivationSha256, "publicationActivationSha256");
    }

    private static Checksum sha256(Checksum value, String name) {
        Checksum exact = Objects.requireNonNull(value, name);
        if (exact.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException(name + " must use SHA256");
        }
        return exact;
    }
}
