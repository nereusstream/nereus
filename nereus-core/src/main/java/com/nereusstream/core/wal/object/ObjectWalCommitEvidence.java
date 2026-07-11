/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.wal.object;
import com.nereusstream.core.wal.ProviderCommitEvidence;
import com.nereusstream.objectstore.wal.WalWriteResult;
import java.util.Objects;
public record ObjectWalCommitEvidence(WalWriteResult writeResult) implements ProviderCommitEvidence {
    public ObjectWalCommitEvidence { Objects.requireNonNull(writeResult); }
}
