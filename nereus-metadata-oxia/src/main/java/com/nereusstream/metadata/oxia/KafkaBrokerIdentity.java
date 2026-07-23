/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

/** Exact KRaft broker registration identity used by capability and readiness records. */
public record KafkaBrokerIdentity(int brokerId, long brokerEpoch)
        implements Comparable<KafkaBrokerIdentity> {
    public KafkaBrokerIdentity {
        if (brokerId < 0 || brokerEpoch < 0) {
            throw new IllegalArgumentException("broker identity must be non-negative");
        }
    }

    @Override
    public int compareTo(KafkaBrokerIdentity other) {
        int brokerOrder = Integer.compare(brokerId, other.brokerId);
        return brokerOrder != 0 ? brokerOrder : Long.compare(brokerEpoch, other.brokerEpoch);
    }
}
