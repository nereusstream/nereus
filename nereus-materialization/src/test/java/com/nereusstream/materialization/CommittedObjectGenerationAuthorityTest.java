/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.GenerationPublicationTestSupport.CLUSTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.metadata.oxia.ObjectProtectionIdentity;
import com.nereusstream.metadata.oxia.records.MaterializationCheckpointRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class CommittedObjectGenerationAuthorityTest {
    @Test
    void requiresExactCommittedIndexActiveRootVisibleProtectionAndCoveringCheckpoint() {
        try (GenerationPublicationTestSupport.Context context =
                GenerationPublicationTestSupport.context()) {
            context.committer(
                            context.generations(),
                            GenerationPublicationTestSupport.successfulGuard())
                    .publish(context.task(), context.output())
                    .join();
            AtomicBoolean readable = new AtomicBoolean(true);
            CommittedObjectGenerationAuthority authority = new CommittedObjectGenerationAuthority(
                    CLUSTER,
                    context.generations(),
                    context.physical(),
                    (proof, timeout) -> java.util.concurrent.CompletableFuture.completedFuture(readable.get()),
                    1,
                    Duration.ofSeconds(10),
                    context.scheduler());

            assertThat(authority.prove(
                            context.task().streamId(), new OffsetRange(0, 2), 1).join())
                    .isEmpty();

            var created = context.generations().getOrCreateMaterializationCheckpoint(
                    CLUSTER,
                    context.task().streamId(),
                    context.task().policy().policyId(),
                    context.task().policy().policyVersion(),
                    context.task().policyDigestSha256()).join();
            MaterializationCheckpointRecord checkpoint = created.value();
            context.generations().compareAndSetMaterializationCheckpoint(
                    CLUSTER,
                    new MaterializationCheckpointRecord(
                            checkpoint.schemaVersion(),
                            checkpoint.streamId(),
                            checkpoint.policyId(),
                            checkpoint.policyVersion(),
                            checkpoint.policySha256(),
                            2,
                            context.task().taskSequence(),
                            context.task().taskSequence(),
                            context.task().taskId(),
                            2_000,
                            0),
                    created.metadataVersion()).join();

            readable.set(false);
            assertThat(authority.prove(
                            context.task().streamId(), new OffsetRange(0, 2), 1).join())
                    .isEmpty();
            readable.set(true);

            CommittedObjectGenerationProof proof = authority.prove(
                            context.task().streamId(), new OffsetRange(0, 2), 1)
                    .join().orElseThrow();
            assertThat(proof.index().value().taskId()).isEqualTo(context.task().taskId());
            assertThat(proof.target()).isEqualTo(context.output().readTarget());
            assertThat(proof.visibleProtection().value().protectionTypeId())
                    .isEqualTo(ObjectProtectionType.VISIBLE_GENERATION.wireId());
            authority.revalidate(proof).join();

            readable.set(false);
            assertThatThrownBy(() -> authority.revalidate(proof).join())
                    .hasRootCauseInstanceOf(com.nereusstream.api.NereusException.class);
            readable.set(true);

            ObjectSliceReadTarget target = proof.target();
            context.physical().deleteProtection(
                    CLUSTER,
                    new ObjectProtectionIdentity(
                            context.output().objectKeyHash(),
                            ObjectProtectionType.VISIBLE_GENERATION,
                            proof.visibleProtection().value().referenceId()),
                    proof.visibleProtection().metadataVersion()).join();

            assertThat(authority.prove(
                            context.task().streamId(), new OffsetRange(0, 2), 1).join())
                    .isEqualTo(Optional.empty());
            assertThatThrownBy(() -> authority.revalidate(proof).join())
                    .hasRootCauseInstanceOf(com.nereusstream.api.NereusException.class);
        }
    }
}
