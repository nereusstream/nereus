/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

/** Closed ordered NWG1 deep-resign operation language. */
public enum Nwg1ResignOperationV1 {
    RECOMPUTE_HEADER_CRC,
    RECOMPUTE_DIRECTORY_CRC,
    REENCRYPT_DIRECTORY,
    REENCRYPT_FRAME,
    RECOMPUTE_BODY_SHA_AND_LEAF,
    RECOMPUTE_PROTOCOL_CELL_COMMITMENT,
    RECOMPUTE_OWNER_FENCE_COMMITMENT,
    RECOMPUTE_ENVELOPE_COMMITMENT
}
