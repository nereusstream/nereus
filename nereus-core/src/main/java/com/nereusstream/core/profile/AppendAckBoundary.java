/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.profile;

/** Exact internal predicate that must be proven before producer success. */
public enum AppendAckBoundary {
    STABLE_HEAD,
    GENERATION_ZERO_VISIBLE,
    REQUIRED_OBJECT_GENERATION
}
