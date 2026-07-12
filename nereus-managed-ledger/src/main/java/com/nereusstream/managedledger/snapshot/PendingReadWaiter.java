/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.snapshot;

import com.nereusstream.api.StreamMetadata;
import java.util.OptionalLong;
import org.apache.bookkeeper.mledger.ManagedLedgerException;

public interface PendingReadWaiter {
    long nextOffset();
    OptionalLong inclusiveMaxOffset();
    boolean trySignal(StreamMetadata snapshot);
    boolean tryFail(ManagedLedgerException error);
}
