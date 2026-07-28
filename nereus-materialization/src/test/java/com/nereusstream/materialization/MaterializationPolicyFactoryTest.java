/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ReadView;
import org.junit.jupiter.api.Test;

class MaterializationPolicyFactoryTest {
    @Test
    void freezesTheBuiltInIdentityAndDeterministicOperatorPolicyVersion() {
        MaterializationPolicy first = MaterializationPolicyFactory.losslessCommitted(
                2, 128, 1_048_576, 256L * 1024 * 1024, 65_536, "ZSTD");
        MaterializationPolicy recovered = MaterializationPolicyFactory.losslessCommitted(
                2, 128, 1_048_576, 256L * 1024 * 1024, 65_536, "ZSTD");

        assertThat(first).isEqualTo(recovered);
        assertThat(first.policyId()).isEqualTo("nereus-committed-default");
        assertThat(first.policyVersion()).isEqualTo(6_800_208_744_780_364_741L);
        assertThat(first.view()).isEqualTo(ReadView.COMMITTED);
        assertThat(first.taskKind()).isEqualTo(TaskKind.LOSSLESS_REWRITE);
        assertThat(first.targetPhysicalFormat()).isEqualTo(MaterializationPolicy.COMMITTED_FORMAT);
        assertThat(first.topicCompaction()).isEmpty();
    }

    @Test
    void everySemanticOperatorFieldChangesTheVersionAndInvalidValuesStillFailPolicyConstruction() {
        MaterializationPolicy baseline = MaterializationPolicyFactory.losslessCommitted(
                2, 16, 1_000, 1_000_000, 512, "ZSTD");

        assertThat(MaterializationPolicyFactory.losslessCommitted(
                        3, 16, 1_000, 1_000_000, 512, "ZSTD").policyVersion())
                .isNotEqualTo(baseline.policyVersion());
        assertThat(MaterializationPolicyFactory.losslessCommitted(
                        2, 17, 1_000, 1_000_000, 512, "ZSTD").policyVersion())
                .isNotEqualTo(baseline.policyVersion());
        assertThat(MaterializationPolicyFactory.losslessCommitted(
                        2, 16, 1_001, 1_000_000, 512, "ZSTD").policyVersion())
                .isNotEqualTo(baseline.policyVersion());
        assertThat(MaterializationPolicyFactory.losslessCommitted(
                        2, 16, 1_000, 1_000_001, 512, "ZSTD").policyVersion())
                .isNotEqualTo(baseline.policyVersion());
        assertThat(MaterializationPolicyFactory.losslessCommitted(
                        2, 16, 1_000, 1_000_000, 513, "ZSTD").policyVersion())
                .isNotEqualTo(baseline.policyVersion());
        assertThat(MaterializationPolicyFactory.losslessCommitted(
                        2, 16, 1_000, 1_000_000, 512, "UNCOMPRESSED").policyVersion())
                .isNotEqualTo(baseline.policyVersion());

        assertThatThrownBy(() -> MaterializationPolicyFactory.losslessCommitted(
                        1, 16, 1_000, 1_000_000, 512, "ZSTD"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MaterializationPolicyFactory.losslessCommitted(
                        2, 16, 1_000, 1_000_000, 512, "SNAPPY"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void freezesKafkaLosslessCommittedAsADistinctNcp2PolicyIdentity() {
        MaterializationPolicy ncp1 =
                MaterializationPolicyFactory.losslessCommitted(
                        2,
                        128,
                        1_048_576,
                        256L * 1024 * 1024,
                        65_536,
                        "ZSTD");
        MaterializationPolicy ncp2 =
                MaterializationPolicyFactory.kafkaLosslessCommitted(
                        2,
                        128,
                        1_048_576,
                        256L * 1024 * 1024,
                        65_536,
                        "ZSTD");

        assertThat(ncp2.policyId())
                .isEqualTo("nereus-kafka-committed-v2");
        assertThat(ncp2.targetPhysicalFormat())
                .isEqualTo(
                        MaterializationPolicy
                                .KAFKA_COMMITTED_FORMAT);
        assertThat(ncp2.view()).isEqualTo(ReadView.COMMITTED);
        assertThat(ncp2.taskKind())
                .isEqualTo(TaskKind.LOSSLESS_REWRITE);
        assertThat(ncp2.topicCompaction()).isEmpty();
        assertThat(ncp2.policyVersion())
                .isNotEqualTo(ncp1.policyVersion());
        assertThat(ncp2.digestSha256())
                .isNotEqualTo(ncp1.digestSha256());
        assertThat(MaterializationPolicyFactory
                        .kafkaLosslessCommitted(
                                2,
                                128,
                                1_048_576,
                                256L * 1024 * 1024,
                                65_536,
                                "ZSTD"))
                .isEqualTo(ncp2);
        assertThat(MaterializationPolicy
                        .isLosslessCommittedFormat(
                                ncp1.targetPhysicalFormat()))
                .isTrue();
        assertThat(MaterializationPolicy
                        .isLosslessCommittedFormat(
                                ncp2.targetPhysicalFormat()))
                .isTrue();
    }

    @Test
    void freezesKafkaTopicCompactionAsADistinctNtc2PolicyIdentity() {
        TopicCompactionSpec spec = new TopicCompactionSpec("kafka-log-cleaner-v1", 1, "KCK2");

        MaterializationPolicy ntc1 = MaterializationPolicyFactory.topicCompacted(
                spec, 2, 128, 1_048_576, 256L * 1024 * 1024, 65_536, "ZSTD");
        MaterializationPolicy ntc2 = MaterializationPolicyFactory.kafkaTopicCompacted(
                spec, 2, 128, 1_048_576, 256L * 1024 * 1024, 65_536, "ZSTD");

        assertThat(ntc2.policyId()).isEqualTo("nereus-kafka-topic-compacted-v2");
        assertThat(ntc2.targetPhysicalFormat())
                .isEqualTo(MaterializationPolicy.KAFKA_TOPIC_COMPACTED_FORMAT);
        assertThat(ntc2.view()).isEqualTo(ReadView.TOPIC_COMPACTED);
        assertThat(ntc2.taskKind()).isEqualTo(TaskKind.TOPIC_KEY_COMPACTION);
        assertThat(ntc2.topicCompaction()).contains(spec);
        assertThat(ntc2.policyVersion()).isNotEqualTo(ntc1.policyVersion());
        assertThat(ntc2.digestSha256()).isNotEqualTo(ntc1.digestSha256());
        assertThat(MaterializationPolicyFactory.kafkaTopicCompacted(
                        spec, 2, 128, 1_048_576, 256L * 1024 * 1024, 65_536, "ZSTD"))
                .isEqualTo(ntc2);
    }
}
