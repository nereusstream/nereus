/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

/** Closed ten-token NWG1 negative-corpus mutation language. */
public enum Nwg1MutationOperationV1 {
    SET_U16,
    SET_U32,
    SET_U64,
    XOR_BYTE,
    REPLACE_COMPONENT,
    TRUNCATE_COMPONENT,
    APPEND_BYTES,
    SWAP_ROWS,
    DUPLICATE_ROW,
    REMOVE_ROW
}
