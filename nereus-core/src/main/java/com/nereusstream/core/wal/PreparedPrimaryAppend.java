/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.wal;
import com.nereusstream.api.StreamId;
public interface PreparedPrimaryAppend {
    StreamId streamId(); int recordCount(); int entryCount(); long logicalBytes(); long reservedBytes();
}
