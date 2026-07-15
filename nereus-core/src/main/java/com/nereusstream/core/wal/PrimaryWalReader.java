/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.wal;
import com.nereusstream.core.read.ReadTargetReader;
import com.nereusstream.core.read.ReadTargetReaderKey;
import com.nereusstream.api.target.ReadTargetType;

/** Compatibility marker for a reader backed by a primary WAL implementation. */
public interface PrimaryWalReader extends ReadTargetReader {
    default ReadTargetType targetType() {
        return key().targetType();
    }

    @Override
    ReadTargetReaderKey key();
}
