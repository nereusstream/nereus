/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterStateRecord;
import java.util.List;
import java.util.Objects;

/** Complete empty-reference capture for one sealed ledger; this is not a durable second truth. */
public record BookKeeperLedgerRetirementCandidate(
        BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
        BookKeeperVersionedValue<BookKeeperWriterStateRecord> writer,
        List<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>> retiredProtections,
        Checksum referenceSetSha256,
        BookKeeperProtocolActivationProof activationProof,
        long capturedAtMillis) {
    public BookKeeperLedgerRetirementCandidate {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(writer, "writer");
        retiredProtections = List.copyOf(Objects.requireNonNull(
                retiredProtections, "retiredProtections"));
        Objects.requireNonNull(referenceSetSha256, "referenceSetSha256");
        if (referenceSetSha256.type() != ChecksumType.SHA256 || capturedAtMillis < 0) {
            throw new IllegalArgumentException("invalid BookKeeper retirement capture");
        }
        Objects.requireNonNull(activationProof, "activationProof");
    }
}
