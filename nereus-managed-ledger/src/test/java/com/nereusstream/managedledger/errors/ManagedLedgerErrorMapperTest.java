/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.errors;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamState;
import java.util.Optional;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.junit.jupiter.api.Test;

class ManagedLedgerErrorMapperTest {
    private final ManagedLedgerErrorMapper mapper = new ManagedLedgerErrorMapper();

    @Test
    void mapsStableL0ClassificationsToLockedPulsarExceptionTypes() {
        assertThat(mapper.map(failure(ErrorCode.STREAM_NOT_FOUND), OperationContext.ledger("open")))
                .isInstanceOf(ManagedLedgerException.ManagedLedgerNotFoundException.class);
        assertThat(mapper.map(failure(ErrorCode.BACKPRESSURE_REJECTED), OperationContext.ledger("append")))
                .isInstanceOf(ManagedLedgerException.TooManyRequestsException.class);
        assertThat(mapper.map(failure(ErrorCode.FENCED_APPEND), OperationContext.ledger("append")))
                .isInstanceOf(ManagedLedgerException.ManagedLedgerFencedException.class);
        assertThat(mapper.map(
                        failure(ErrorCode.STREAM_NOT_ACTIVE),
                        new OperationContext("append", false, false, Optional.of(StreamState.SEALED))))
                .isInstanceOf(ManagedLedgerException.ManagedLedgerTerminatedException.class);
        assertThat(mapper.map(
                        failure(ErrorCode.OFFSET_TRIMMED),
                        new OperationContext("read", false, true, Optional.of(StreamState.ACTIVE))))
                .isInstanceOf(ManagedLedgerException.InvalidCursorPositionException.class);
        assertThat(mapper.map(
                        failure(ErrorCode.PRIMARY_WAL_CHECKSUM_MISMATCH),
                        OperationContext.ledger("read")))
                .isInstanceOf(ManagedLedgerException.NonRecoverableLedgerException.class)
                .hasCauseInstanceOf(NereusException.class);
    }

    @Test
    void unsupportedChannelsShareTheStablePrefix() {
        assertThat(mapper.unsupported("offloadPrefix").getMessage())
                .isEqualTo("NEREUS_UNSUPPORTED_OPERATION:offloadPrefix");
        assertThat(mapper.unsupportedRuntime("migrate").getMessage())
                .isEqualTo("NEREUS_UNSUPPORTED_OPERATION:migrate");
    }

    private static NereusException failure(ErrorCode code) {
        return new NereusException(code, false, "test failure");
    }
}
