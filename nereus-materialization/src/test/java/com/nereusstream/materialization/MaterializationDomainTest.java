/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.security.SecureRandom;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class MaterializationDomainTest {
    @Test
    void canonicalPolicySourceAndTaskIdentityIsStableAcrossIndependentRecovery() {
        try (GenerationPublicationTestSupport.Context first =
                        GenerationPublicationTestSupport.context();
                GenerationPublicationTestSupport.Context second =
                        GenerationPublicationTestSupport.context()) {
            assertThat(second.task()).isEqualTo(first.task());
            assertThat(second.task().taskId()).isEqualTo(first.task().taskId());
            assertThat(second.task().sourceSetSha256()).isEqualTo(first.task().sourceSetSha256());
            assertThat(second.task().policyDigestSha256()).isEqualTo(first.task().policyDigestSha256());
            assertThat(second.output()).isEqualTo(first.output());
        }
    }

    @Test
    void durableTaskRejectsAnIdentityThatDoesNotMatchItsCanonicalFacts() {
        try (GenerationPublicationTestSupport.Context context =
                GenerationPublicationTestSupport.context()) {
            MaterializationTask task = context.task();
            assertThatThrownBy(() -> new MaterializationTask(
                            "not-the-canonical-task-id",
                            task.streamId(),
                            task.view(),
                            task.taskKind(),
                            task.coverage(),
                            task.sources(),
                            task.sourceSetSha256(),
                            task.policy(),
                            task.policyDigestSha256()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("canonical task identity");
        }
    }

    @Test
    void publicationIdsDeriveAFullUnpaddedSha256Base32IdentityFromFreshEntropy() {
        AtomicInteger sequence = new AtomicInteger();
        SecurePublicationIdGenerator generator = new SecurePublicationIdGenerator(new SecureRandom() {
            @Override
            public void nextBytes(byte[] bytes) {
                int value = sequence.incrementAndGet();
                for (int index = 0; index < bytes.length; index++) {
                    bytes[index] = (byte) (value + index);
                }
            }
        });

        Set<String> ids = new HashSet<>();
        for (int index = 0; index < 32; index++) {
            String value = generator.next().value();
            assertThat(value).hasSize(52).matches("[a-z2-7]{52}");
            ids.add(value);
        }
        assertThat(ids).hasSize(32);
    }
}
