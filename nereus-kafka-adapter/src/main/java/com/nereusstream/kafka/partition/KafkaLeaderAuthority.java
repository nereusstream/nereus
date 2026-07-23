/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

import com.nereusstream.api.AppendAuthority;
import java.util.Objects;

/** Exact KRaft leader/broker term mapped to the Nereus external append authority tuple. */
public record KafkaLeaderAuthority(
        KafkaPartitionIdentity identity,
        int leaderId,
        int leaderEpoch,
        long brokerEpoch) {
    public static final String AUTHORITY_TYPE = "kafka-partition-leader-v1";

    public KafkaLeaderAuthority {
        Objects.requireNonNull(identity, "identity");
        if (leaderId < 0 || leaderEpoch < 0 || brokerEpoch < 0) {
            throw new IllegalArgumentException("Kafka leader authority fields must be non-negative");
        }
    }

    public AppendAuthority appendAuthority() {
        return new AppendAuthority(
                AUTHORITY_TYPE,
                identity.durableId().canonicalIdentity(),
                leaderEpoch,
                Integer.toString(leaderId),
                brokerEpoch);
    }

    public AuthorityRelation relationTo(KafkaLeaderAuthority current) {
        Objects.requireNonNull(current, "current");
        if (!identity.equals(current.identity)) return AuthorityRelation.DIFFERENT_PARTITION;
        if (equals(current)) return AuthorityRelation.EXACT;
        if (leaderEpoch > current.leaderEpoch) return AuthorityRelation.DOMINATES;
        if (leaderEpoch < current.leaderEpoch) return AuthorityRelation.STALE;
        if (leaderId == current.leaderId && brokerEpoch > current.brokerEpoch) {
            return AuthorityRelation.DOMINATES;
        }
        return AuthorityRelation.CONFLICTING;
    }

    public enum AuthorityRelation {
        EXACT,
        DOMINATES,
        STALE,
        CONFLICTING,
        DIFFERENT_PARTITION
    }
}
