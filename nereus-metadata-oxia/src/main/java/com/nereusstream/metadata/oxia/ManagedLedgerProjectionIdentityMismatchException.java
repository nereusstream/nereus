/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import java.util.Objects;

public final class ManagedLedgerProjectionIdentityMismatchException extends NereusException {
    private final ManagedLedgerProjectionIdentity expected;
    private final ManagedLedgerProjectionIdentity actual;

    public ManagedLedgerProjectionIdentityMismatchException(
            ManagedLedgerProjectionIdentity expected,
            ManagedLedgerProjectionIdentity actual) {
        super(
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                false,
                "managed-ledger projection identity mismatch: expected=" + expected + ", actual=" + actual);
        this.expected = Objects.requireNonNull(expected, "expected");
        this.actual = Objects.requireNonNull(actual, "actual");
    }

    public ManagedLedgerProjectionIdentity expected() {
        return expected;
    }

    public ManagedLedgerProjectionIdentity actual() {
        return actual;
    }
}
