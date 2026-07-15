/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.retirement;

/** Closed key identity for the two generation-zero committed-marker encodings. */
public sealed interface GenerationZeroMarkerIdentity
        permits LegacyCommittedSliceIdentity, GenericCommittedAppendIdentity {
}
