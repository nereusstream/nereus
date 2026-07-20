/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

/** Broker-owned publication seam invoked only after the exact local BK binding is verified. */
@FunctionalInterface
public interface BookKeeperPrimaryWalCapabilitySink {
    void install(BookKeeperPrimaryWalCapabilityBinding binding);

    static BookKeeperPrimaryWalCapabilitySink unavailable() {
        return ignored -> {
            throw new IllegalStateException(
                    "BookKeeper primary-WAL capability publication is unavailable");
        };
    }
}
