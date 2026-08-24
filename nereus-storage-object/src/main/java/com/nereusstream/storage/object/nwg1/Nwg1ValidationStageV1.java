/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

/** Closed, earliest-error-ordered verification stages. */
public enum Nwg1ValidationStageV1 {
    ROOT_AUTHORITY,
    LEAF,
    OBJECT_BODY_DIGEST,
    HEADER_GRAMMAR,
    HEADER_CRC,
    HEADER_AUTHORITY,
    KMS_ENVELOPE,
    DIRECTORY_AEAD,
    DIRECTORY_CRC,
    DIRECTORY_STRUCTURE,
    BINDING_SEMANTICS,
    FRAME_AEAD,
    FRAME_CODEC,
    FRAME_PAYLOAD_CRC,
    NATIVE_FRAME,
    APPEND_UNIT_SEMANTICS
}
