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
}
