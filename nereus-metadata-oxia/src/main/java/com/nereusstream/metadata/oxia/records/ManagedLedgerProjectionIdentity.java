/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import java.util.Objects;
import java.util.regex.Pattern;

public record ManagedLedgerProjectionIdentity(
        long storageClassBindingGeneration,
        long incarnation,
        String streamId,
        long virtualLedgerId) {
    private static final Pattern STREAM_ID = Pattern.compile("s-[a-z2-7]{52}");

    public ManagedLedgerProjectionIdentity {
        Objects.requireNonNull(streamId, "streamId");
        if (storageClassBindingGeneration < 1 || incarnation < 1) {
            throw new IllegalArgumentException("projection identity generations must be positive");
        }
        if (!STREAM_ID.matcher(streamId).matches()) {
            throw new IllegalArgumentException("projection identity streamId is not a deterministic Nereus ID");
        }
        if (virtualLedgerId < ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID
                || virtualLedgerId >= Long.MAX_VALUE) {
            throw new IllegalArgumentException("projection identity virtualLedgerId is outside the F2 range");
        }
    }
}
