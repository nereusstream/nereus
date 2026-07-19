/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.api;

/** Producer completion predicate independent from primary-WAL durability. */
public enum AppendCompletionPolicy {
    PROFILE_DEFAULT,
    STABLE_HEAD,
    GENERATION_ZERO_INDEX,
    REQUIRED_OBJECT_GENERATION
}
