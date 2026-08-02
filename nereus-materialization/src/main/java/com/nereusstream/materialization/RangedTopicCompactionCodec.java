/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ReadBatch;

/**
 * Versioned protocol codec for ranged source entries that may contain multiple logical records.
 *
 * <p>The materialization engine owns ordering, spill, winner selection, and two-pass source
 * verification. Implementations own strict protocol decoding and byte-exact survivor rewrite.
 */
public interface RangedTopicCompactionCodec {
    String codecId();

    long codecVersion();

    Checksum messageFormatSha256();

    void decode(ReadBatch rangedBatch, DecodedRecordConsumer consumer);

    RewrittenCompactionRecord rewrite(DecodedCompactionRecord survivor, CompactionRewriteContext context);

    @FunctionalInterface
    interface DecodedRecordConsumer {
        void accept(DecodedCompactionRecord record);
    }
}
