/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.client.api.BKException;

/** Stable provider-neutral error classification for the BookKeeper 4.18 client surface. */
public final class BookKeeperExceptionMapper {
    public enum Operation { READ, WRITE, METADATA, DELETE }

    private BookKeeperExceptionMapper() { }

    public static NereusException map(Throwable failure, Operation operation) {
        Throwable cause = unwrap(failure);
        if (cause instanceof NereusException nereus) return nereus;
        if (cause instanceof TimeoutException) {
            return new NereusException(ErrorCode.TIMEOUT, true, "BookKeeper operation timed out", cause);
        }
        if (cause instanceof BKException bookKeeper) {
            int code = bookKeeper.getCode();
            if (code == BKException.Code.NoSuchLedgerExistsException
                    || code == BKException.Code.NoSuchLedgerExistsOnMetadataServerException
                    || code == BKException.Code.NoSuchEntryException) {
                return new NereusException(ErrorCode.PRIMARY_WAL_TARGET_NOT_FOUND, false,
                        "BookKeeper target is absent", cause);
            }
            if (code == BKException.Code.DigestMatchException) {
                return new NereusException(ErrorCode.PRIMARY_WAL_CHECKSUM_MISMATCH, false,
                        "BookKeeper digest validation failed", cause);
            }
            if (code == BKException.Code.UnauthorizedAccessException
                    || code == BKException.Code.SecurityException) {
                return new NereusException(
                        operation == Operation.READ ? ErrorCode.PRIMARY_WAL_READ_FAILED
                                : ErrorCode.PRIMARY_WAL_WRITE_FAILED,
                        false, "BookKeeper authentication failed", cause);
            }
            if (code == BKException.Code.LedgerFencedException
                    || code == BKException.Code.LedgerClosedException) {
                return new NereusException(ErrorCode.FENCED_APPEND, false,
                        "BookKeeper ledger is fenced or closed", cause);
            }
            boolean retriable = code == BKException.Code.ReadException
                    || code == BKException.Code.WriteException
                    || code == BKException.Code.QuorumException
                    || code == BKException.Code.NoBookieAvailableException
                    || code == BKException.Code.NotEnoughBookiesException
                    || code == BKException.Code.BookieHandleNotAvailableException
                    || code == BKException.Code.AddEntryQuorumTimeoutException
                    || code == BKException.Code.TimeoutException
                    || code == BKException.Code.TooManyRequestsException
                    || code == BKException.Code.MetaStoreException;
            return new NereusException(errorCode(operation), retriable,
                    "BookKeeper " + operation.name().toLowerCase() + " failed: " + BKException.getMessage(code), cause);
        }
        return new NereusException(errorCode(operation), true,
                "BookKeeper " + operation.name().toLowerCase() + " failed", cause);
    }

    private static ErrorCode errorCode(Operation operation) {
        return operation == Operation.READ ? ErrorCode.PRIMARY_WAL_READ_FAILED : ErrorCode.PRIMARY_WAL_WRITE_FAILED;
    }
    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) current = current.getCause();
        return current;
    }
}
