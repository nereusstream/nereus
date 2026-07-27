/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

/** Authority shape admitted by a materialization runtime. */
public enum MaterializationStreamAuthorityMode {
    /** Existing Pulsar/F4 path: every source and output is rooted in one live projection. */
    PROJECTION_REQUIRED,

    /** Native protocol path: source and output projection hints stay empty. */
    DIRECT_STREAM
}
