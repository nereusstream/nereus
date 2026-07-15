/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

/** Generates a fresh non-authorizing identity for one GC candidate or attempt. */
@FunctionalInterface
public interface GcIdGenerator {
    String next();
}
