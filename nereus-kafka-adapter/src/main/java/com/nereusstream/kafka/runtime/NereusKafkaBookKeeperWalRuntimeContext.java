/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.kafka.runtime;

import com.nereusstream.bookkeeper.BookKeeperBrokerReadinessProvider;
import com.nereusstream.bookkeeper.BookKeeperClientOperations;
import com.nereusstream.bookkeeper.BookKeeperPasswordProvider;
import com.nereusstream.bookkeeper.DefaultBookKeeperClientOperations;
import java.util.Objects;
import org.apache.bookkeeper.client.api.BookKeeper;

/**
 * Borrowed BookKeeper provider dependencies for Kafka runtime bootstrap.
 *
 * <p>The Kafka adapter never closes these values. The embedding Kafka runtime must keep them alive
 * until the returned Nereus runtime has closed.
 */
public record NereusKafkaBookKeeperWalRuntimeContext(
        BookKeeper client,
        BookKeeperBrokerReadinessProvider brokerReadiness,
        BookKeeperPasswordProvider passwords,
        BookKeeperClientOperations operations) {
    public NereusKafkaBookKeeperWalRuntimeContext(
            BookKeeper client,
            BookKeeperBrokerReadinessProvider brokerReadiness,
            BookKeeperPasswordProvider passwords) {
        this(
                client,
                brokerReadiness,
                passwords,
                new DefaultBookKeeperClientOperations(Objects.requireNonNull(client, "client")));
    }

    public NereusKafkaBookKeeperWalRuntimeContext {
        Objects.requireNonNull(client, "client");
        Objects.requireNonNull(brokerReadiness, "brokerReadiness");
        Objects.requireNonNull(passwords, "passwords");
        Objects.requireNonNull(operations, "operations");
    }
}
