/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

/** Versioned deterministic tombstone policy for the isolated TOPIC_COMPACTED view. */
public interface TopicCompactionStrategy {
    String id();

    long version();

    boolean retainTombstone(CompactionRecord tombstone, long planningTimeMillis);
}
