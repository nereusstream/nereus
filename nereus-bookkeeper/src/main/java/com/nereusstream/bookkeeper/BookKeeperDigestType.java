/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import org.apache.bookkeeper.client.api.DigestType;

/** Stable Nereus configuration name mapped explicitly to the BookKeeper 4.18 client enum. */
public enum BookKeeperDigestType {
    CRC32,
    CRC32C,
    MAC;

    public DigestType toClientType() {
        return switch (this) {
            case CRC32 -> DigestType.CRC32;
            case CRC32C -> DigestType.CRC32C;
            case MAC -> DigestType.MAC;
        };
    }
}
