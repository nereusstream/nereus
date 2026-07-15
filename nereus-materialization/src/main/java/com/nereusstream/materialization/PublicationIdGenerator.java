/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.PublicationId;

/** Generates a fresh stable id only for a new OUTPUT_READY -> PUBLISHING attempt. */
@FunctionalInterface
public interface PublicationIdGenerator {
    PublicationId next();
}
