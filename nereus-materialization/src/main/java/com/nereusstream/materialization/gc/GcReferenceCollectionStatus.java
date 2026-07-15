/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

/** Non-exceptional deletion decision from a bounded reference-domain pass. */
public enum GcReferenceCollectionStatus {
    CLEAR,
    VETOED,
    INCOMPLETE,
    LIMIT_EXCEEDED
}
