/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import java.time.Duration;

final class BookKeeperTestConfigurations {
    private BookKeeperTestConfigurations() { }
    static BookKeeperWalConfiguration valid() {
        return new BookKeeperWalConfiguration(
                "primary", "11".repeat(32), 12, 0x801, "reservation-1",
                3, 3, 2, BookKeeperDigestType.CRC32C,
                new BookKeeperSecretRef("secret://bookkeeper/password", "v7"),
                100_000, 256L * 1024 * 1024, 1_000, 8, 64, 32,
                Duration.ofHours(1), 1, 8, 64L * 1024 * 1024,
                Duration.ofSeconds(30), Duration.ofSeconds(20), Duration.ofSeconds(30),
                Duration.ofSeconds(30), Duration.ofMinutes(2), Duration.ofSeconds(30),
                Duration.ofMinutes(1), 256);
    }
}
