/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.api;

/**
 * Producer completion predicate independent from primary-WAL durability.
 *
 * <p>{@link #REQUIRED_OBJECT_GENERATION} means that the exact committed append must have a higher Object generation
 * accepted through the normal read path. It does not redefine generation-zero durability.
 */
public enum AppendCompletionPolicy {
    PROFILE_DEFAULT,
    STABLE_HEAD,
    GENERATION_ZERO_INDEX,
    REQUIRED_OBJECT_GENERATION
}
