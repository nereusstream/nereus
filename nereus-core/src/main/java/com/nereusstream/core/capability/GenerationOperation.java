/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.capability;

public enum GenerationOperation {
    GENERATION_PUBLISH,
    RECOVERY_CHECKPOINT,
    LOGICAL_TRIM,
    PHYSICAL_DELETE,
    TOPIC_COMPACTED_PUBLISH
}
