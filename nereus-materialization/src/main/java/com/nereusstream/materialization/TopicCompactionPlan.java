/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.objectstore.compacted.CompactedObjectRow;
import java.util.concurrent.Flow;

/** Close-owned pass-one survivor proof and cold single-subscription pass-two row stream. */
public interface TopicCompactionPlan extends Flow.Publisher<CompactedObjectRow>, AutoCloseable {
    int outputRecordCount();

    @Override
    void close();
}
