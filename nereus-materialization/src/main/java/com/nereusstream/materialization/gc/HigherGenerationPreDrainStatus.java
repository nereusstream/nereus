/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

/** Terminal classification for one bounded higher-generation pre-drain attempt. */
public enum HigherGenerationPreDrainStatus {
    DRAINING_READY,
    NO_MATCHING_INDEX,
    MUTATION_DISABLED
}
