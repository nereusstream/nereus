/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import java.nio.ByteBuffer;
import java.util.Optional;

/** Mapping-specific parser that exposes only compaction facts while source payload bytes remain opaque. */
public interface TopicCompactionDecoder {
    String id();

    /** Empty means unkeyed and retain-exact, never drop. */
    Optional<CompactionRecord> decode(long offset, ByteBuffer exactPayload);
}
