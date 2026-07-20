/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.util.Objects;

/** Exact real-provider create/write/read/fence/delete/absence canary evidence. */
public record BookKeeperScopeCapabilityProof(
        String runId,
        long brokerReadinessEpoch,
        Checksum brokerSetSha256,
        long canaryLedgerId,
        Checksum canaryPayloadSha256,
        Checksum capabilitySha256) {
    public BookKeeperScopeCapabilityProof {
        runId = Objects.requireNonNull(runId, "runId");
        if (runId.isBlank() || brokerReadinessEpoch <= 0 || canaryLedgerId <= 0) {
            throw new IllegalArgumentException("scope capability proof identity is invalid");
        }
        brokerSetSha256 = sha256(brokerSetSha256, "brokerSetSha256");
        canaryPayloadSha256 = sha256(canaryPayloadSha256, "canaryPayloadSha256");
        capabilitySha256 = sha256(capabilitySha256, "capabilitySha256");
    }

    private static Checksum sha256(Checksum value, String name) {
        Checksum exact = Objects.requireNonNull(value, name);
        if (exact.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException(name + " must use SHA256");
        }
        return exact;
    }
}
