/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

/** Protocol-neutral bounds shared by every metadata-backed GC reference domain. */
public record GcReferenceDomainConfig(
        int metadataScanPageSize,
        int maxAuthoritiesPerSnapshot,
        int maxReferencesPerSnapshot) {
    public static final int MAX_PAGE_SIZE = 1_000;
    public static final int MAX_SNAPSHOT_VALUES = 100_000;

    public GcReferenceDomainConfig {
        requireInRange(metadataScanPageSize, 1, MAX_PAGE_SIZE, "metadataScanPageSize");
        requireInRange(
                maxAuthoritiesPerSnapshot,
                1,
                MAX_SNAPSHOT_VALUES,
                "maxAuthoritiesPerSnapshot");
        requireInRange(
                maxReferencesPerSnapshot,
                1,
                MAX_SNAPSHOT_VALUES,
                "maxReferencesPerSnapshot");
    }

    private static void requireInRange(int value, int minimum, int maximum, String name) {
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException(
                    name + " must be in [" + minimum + ", " + maximum + "]");
        }
    }
}
