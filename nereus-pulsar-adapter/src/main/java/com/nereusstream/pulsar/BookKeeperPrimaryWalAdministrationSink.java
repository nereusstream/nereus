/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

/** Broker-owned installation point for the explicit BookKeeper primary-WAL administration surface. */
@FunctionalInterface
public interface BookKeeperPrimaryWalAdministrationSink {
    void install(BookKeeperPrimaryWalAdministration administration);

    static BookKeeperPrimaryWalAdministrationSink unavailable() {
        return ignored -> {
        };
    }
}
