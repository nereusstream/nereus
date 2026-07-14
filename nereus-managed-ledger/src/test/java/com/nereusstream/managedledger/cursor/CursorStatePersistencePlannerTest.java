/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ObjectKey;
import com.nereusstream.metadata.oxia.VersionedCursorState;
import com.nereusstream.metadata.oxia.codec.F3MetadataCodecs;
import com.nereusstream.metadata.oxia.codec.MetadataValueTooLargeException;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

class CursorStatePersistencePlannerTest {
    private static final String OWNER = "00112233445566778899aabbccddeeff";
    private static final String ATTEMPT = "11111111111111111111111111111111";

    @Test
    void hydratesInlineOnlyRootAsTheCompleteAckState() {
        CursorStorageConfig config = CursorStorageConfig.defaults();
        CursorStatePersistencePlanner planner = planner(config);
        CursorState state = state(config, 5);
        CursorState candidate = next(
                state,
                new CursorAckState(
                        5,
                        List.of(new OffsetRange(7, 9)),
                        new TreeMap<>(Map.of(10L, batch8(0x03L)))),
                Optional.empty());
        CursorStateRecord root = planner.recordWithoutSnapshot(candidate);

        CursorStatePersistencePlanner.HydratedState hydrated = planner.hydrate(
                new VersionedCursorState(root, 7),
                candidate.identity().ledger(),
                Optional.empty());

        assertThat(hydrated.state().acknowledgements()).isEqualTo(candidate.acknowledgements());
        assertThat(hydrated.state().metadataVersion()).isEqualTo(7);
        assertThat(hydrated.snapshotBase()).isEmpty();
    }

    @Test
    void snapshotDeltaUsesFullPartialOverrideAndWholeRangeWinsOnHydration() throws Exception {
        CursorStorageConfig config = CursorStorageConfig.defaults();
        CursorStatePersistencePlanner planner = planner(config);
        CursorStateMachine machine = new CursorStateMachine(config);
        SnapshotFixture fixture = snapshotFixture(planner, config);

        CursorState narrowed = machine.individualAck(
                        fixture.current().state(),
                        List.of(new CursorAckRequest(
                                15,
                                Optional.of(new BatchAckState(65, new long[] {-4L, 1L})),
                                Map.of())),
                        0,
                        30,
                        106)
                .state();
        CursorStatePersistencePlanner.InlinePlan partialPlan =
                (CursorStatePersistencePlanner.InlinePlan) planner.plan(fixture.current(), narrowed);

        assertThat(partialPlan.record().inlineWholeAckDeltas()).isEmpty();
        assertThat(partialPlan.record().inlinePartialAckOverrides()).hasSize(1);
        assertThat(partialPlan.record().inlinePartialAckOverrides().get(0).entryOffset())
                .isEqualTo(15);
        CursorStatePersistencePlanner.HydratedState partialHydrated = planner.hydrate(
                new VersionedCursorState(partialPlan.record(), 5),
                narrowed.identity().ledger(),
                Optional.of(fixture.snapshotBase()));
        assertThat(partialHydrated.state().acknowledgements())
                .isEqualTo(narrowed.acknowledgements());

        CursorState whole = machine.individualAck(
                        fixture.current().state(),
                        List.of(new CursorAckRequest(15, Optional.empty(), Map.of())),
                        0,
                        30,
                        107)
                .state();
        CursorStatePersistencePlanner.InlinePlan wholePlan =
                (CursorStatePersistencePlanner.InlinePlan) planner.plan(fixture.current(), whole);

        assertThat(wholePlan.record().inlineWholeAckDeltas())
                .extracting(range -> List.of(range.startOffset(), range.endOffset()))
                .containsExactly(List.of(15L, 16L));
        assertThat(wholePlan.record().inlinePartialAckOverrides()).isEmpty();
        CursorState wholeHydrated = planner.hydrate(
                        new VersionedCursorState(wholePlan.record(), 6),
                        whole.identity().ledger(),
                        Optional.of(fixture.snapshotBase()))
                .state();
        assertThat(wholeHydrated.acknowledgements()).isEqualTo(whole.acknowledgements());
        assertThat(wholeHydrated.acknowledgements().partialBatchAcks()).doesNotContainKey(15L);
    }

    @Test
    void newerRootMarkDeleteClipsSnapshotButRejectsAnUnfoldedBoundary() {
        CursorStorageConfig config = CursorStorageConfig.defaults();
        CursorStatePersistencePlanner planner = planner(config);
        SnapshotFixture fixture = snapshotFixture(planner, config);
        CursorStateRecord root = fixture.current().root().value();
        CursorStateRecord noncanonical = copyRoot(root, 12);

        assertThatThrownBy(() -> planner.hydrate(
                        new VersionedCursorState(noncanonical, 9),
                        fixture.current().state().identity().ledger(),
                        Optional.of(fixture.snapshotBase())))
                .isInstanceOf(CursorSnapshotCodecV1.CursorSnapshotCorruptionException.class)
                .hasMessageContaining("mark-delete");
    }

    @Test
    void snapshotPresenceAndProjectionMustMatchTheRootExactly() {
        CursorStorageConfig config = CursorStorageConfig.defaults();
        CursorStatePersistencePlanner planner = planner(config);
        SnapshotFixture fixture = snapshotFixture(planner, config);

        assertThatThrownBy(() -> planner.hydrate(
                        fixture.current().root(),
                        fixture.current().state().identity().ledger(),
                        Optional.empty()))
                .isInstanceOf(CursorSnapshotCodecV1.CursorSnapshotCorruptionException.class)
                .hasMessageContaining("presence");

        CursorState inline = state(config, 5);
        assertThatThrownBy(() -> planner.hydrate(
                        new VersionedCursorState(planner.recordWithoutSnapshot(inline), 1),
                        inline.identity().ledger(),
                        Optional.of(fixture.snapshotBase())))
                .isInstanceOf(CursorSnapshotCodecV1.CursorSnapshotCorruptionException.class)
                .hasMessageContaining("presence");
    }

    @Test
    void inlineCountThresholdIsInclusiveAndSpillsOnlyAboveIt() {
        CursorStorageConfig config = copyConfig(
                CursorStorageConfig.defaults(), 4_096, 8_192, 1, 16_384, 8_192, 16_384);
        CursorStatePersistencePlanner planner = planner(config);
        CursorStatePersistencePlanner.HydratedState current = inlineCurrent(planner, state(config, 5), 3);

        CursorState oneRange = next(
                current.state(),
                new CursorAckState(5, List.of(new OffsetRange(7, 8)), new TreeMap<>()),
                Optional.empty());
        assertThat(planner.plan(current, oneRange))
                .isInstanceOf(CursorStatePersistencePlanner.InlinePlan.class);

        CursorState twoRanges = next(
                current.state(),
                new CursorAckState(
                        5,
                        List.of(new OffsetRange(7, 8), new OffsetRange(9, 10)),
                        new TreeMap<>()),
                Optional.empty());
        CursorStatePersistencePlanner.SnapshotPlan spill =
                (CursorStatePersistencePlanner.SnapshotPlan) planner.plan(current, twoRanges);
        assertThat(spill.request().fullState()).isEqualTo(twoRanges.acknowledgements());
        assertThat(spill.request().sourceMutationSequence()).isEqualTo(twoRanges.mutationSequence());
    }

    @Test
    void emptyInlineDeltaNeverCreatesAnEmptyReplacementSnapshot() {
        CursorStorageConfig config = copyConfig(
                CursorStorageConfig.defaults(), 4_096, 1, 1, 16_384, 8_192, 16_384);
        CursorStatePersistencePlanner planner = planner(config);
        CursorStatePersistencePlanner.HydratedState current = inlineCurrent(planner, state(config, 5), 3);
        CursorState propertyOnly = new CursorState(
                current.state().identity(),
                current.state().ownerSessionId(),
                current.state().lifecycle(),
                current.state().mutationSequence() + 1,
                current.state().ackStateEpoch(),
                current.state().lastProtectionAttemptId(),
                current.state().acknowledgements(),
                current.state().positionProperties(),
                Map.of("property", "value"),
                current.state().snapshotReference(),
                current.state().createdAtMillis(),
                current.state().updatedAtMillis() + 1,
                current.state().metadataVersion());

        assertThat(planner.plan(current, propertyOnly))
                .isInstanceOf(CursorStatePersistencePlanner.InlinePlan.class);
    }

    @Test
    void inlineRootBeyondHardLimitSpillsInsteadOfFailingBeforeSnapshotPlanning() {
        CursorStorageConfig config = CursorStorageConfig.defaults();
        CursorStatePersistencePlanner planner = planner(config);
        CursorStatePersistencePlanner.HydratedState current = inlineCurrent(planner, state(config, 10), 3);
        List<OffsetRange> ranges = new ArrayList<>();
        for (int index = 0; index < 4_096; index++) {
            long start = 100L + index * 2L;
            ranges.add(new OffsetRange(start, start + 1));
        }
        CursorState candidate = next(
                current.state(),
                new CursorAckState(10, ranges, new TreeMap<>()),
                Optional.empty());

        assertThatThrownBy(() -> F3MetadataCodecs.encodeEnvelope(
                        planner.recordWithoutSnapshot(candidate), CursorStateRecord.class))
                .isInstanceOf(MetadataValueTooLargeException.class);
        assertThat(planner.plan(current, candidate))
                .isInstanceOf(CursorStatePersistencePlanner.SnapshotPlan.class);
    }

    @Test
    void nonMonotonicDeltaCannotReuseSnapshotAndGetsAReplacementSnapshot() {
        CursorStorageConfig config = CursorStorageConfig.defaults();
        CursorStatePersistencePlanner planner = planner(config);
        SnapshotFixture fixture = snapshotFixture(planner, config);
        CursorAckState missingSnapshotRange = new CursorAckState(
                10,
                List.of(new OffsetRange(12, 14)),
                new TreeMap<>(Map.of(15L, new BatchAckState(65, new long[] {-2L, 1L}))));
        CursorState candidate = next(
                fixture.current().state(),
                missingSnapshotRange,
                fixture.current().state().snapshotReference());

        assertThat(planner.plan(fixture.current(), candidate))
                .isInstanceOf(CursorStatePersistencePlanner.SnapshotPlan.class);
    }

    @Test
    void composedReferencedRootMustFitTheSafetyAdjustedLimitBeforeUpload() {
        CursorStorageConfig config = copyConfig(
                CursorStorageConfig.defaults(), 65_200, 64, 1, 8, 8, 8);
        CursorStatePersistencePlanner planner = planner(config);
        CursorStatePersistencePlanner.HydratedState current = inlineCurrent(planner, state(config, 5), 3);
        CursorState candidate = next(
                current.state(),
                new CursorAckState(5, List.of(new OffsetRange(7, 8)), new TreeMap<>()),
                Optional.empty());

        assertThatThrownBy(() -> planner.plan(current, candidate))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("margin-adjusted metadata bound after snapshot spill");
    }

    private static CursorStatePersistencePlanner planner(CursorStorageConfig config) {
        return new CursorStatePersistencePlanner(CursorTestSamples.CLUSTER, config);
    }

    private static CursorState state(CursorStorageConfig config, long markDeleteOffset) {
        return new CursorStateMachine(config).create(
                new CursorOwnerSession(CursorTestSamples.identity().ledger(), OWNER),
                CursorTestSamples.CURSOR,
                1,
                1,
                ATTEMPT,
                markDeleteOffset,
                Map.of(),
                Map.of(),
                100);
    }

    private static CursorStatePersistencePlanner.HydratedState inlineCurrent(
            CursorStatePersistencePlanner planner, CursorState state, long metadataVersion) {
        return planner.hydrate(
                new VersionedCursorState(planner.recordWithoutSnapshot(state), metadataVersion),
                state.identity().ledger(),
                Optional.empty());
    }

    private static SnapshotFixture snapshotFixture(
            CursorStatePersistencePlanner planner, CursorStorageConfig config) {
        CursorAckState base = CursorTestSamples.complexState();
        CursorState created = state(config, base.markDeleteOffset());
        CursorState snapshotState = new CursorState(
                created.identity(),
                created.ownerSessionId(),
                created.lifecycle(),
                8,
                created.ackStateEpoch(),
                created.lastProtectionAttemptId(),
                base,
                Map.of(),
                Map.of(),
                Optional.empty(),
                created.createdAtMillis(),
                105,
                0);
        CursorSnapshotCodecV1.EncodedSnapshot encoded = CursorSnapshotCodecV1.encode(
                new CursorSnapshotWriteRequest(snapshotState.identity(), 8, base, 105),
                CursorTestSamples.SNAPSHOT,
                config);
        CursorSnapshotReference reference = new CursorSnapshotReference(
                new ObjectKey("cursor-snapshots/fixture.ncs"),
                CursorTestSamples.SNAPSHOT,
                snapshotState.identity().cursorGeneration(),
                8,
                base.markDeleteOffset(),
                encoded.objectLength(),
                encoded.storageChecksum(),
                encoded.formatCrc32c(),
                1,
                105);
        CursorState referenced = new CursorState(
                snapshotState.identity(),
                snapshotState.ownerSessionId(),
                snapshotState.lifecycle(),
                snapshotState.mutationSequence(),
                snapshotState.ackStateEpoch(),
                snapshotState.lastProtectionAttemptId(),
                snapshotState.acknowledgements(),
                snapshotState.positionProperties(),
                snapshotState.cursorProperties(),
                Optional.of(reference),
                snapshotState.createdAtMillis(),
                snapshotState.updatedAtMillis(),
                0);
        CursorStateRecord root = planner.afterSnapshot(referenced, reference);
        CursorStatePersistencePlanner.HydratedState current = planner.hydrate(
                new VersionedCursorState(root, 4),
                referenced.identity().ledger(),
                Optional.of(base));
        return new SnapshotFixture(base, current);
    }

    private static CursorState next(
            CursorState current,
            CursorAckState acknowledgements,
            Optional<CursorSnapshotReference> reference) {
        return new CursorState(
                current.identity(),
                current.ownerSessionId(),
                current.lifecycle(),
                Math.addExact(current.mutationSequence(), 1),
                current.ackStateEpoch(),
                current.lastProtectionAttemptId(),
                acknowledgements,
                current.positionProperties(),
                current.cursorProperties(),
                reference,
                current.createdAtMillis(),
                current.updatedAtMillis() + 1,
                current.metadataVersion());
    }

    private static CursorStateRecord copyRoot(CursorStateRecord root, long markDeleteOffset) {
        return new CursorStateRecord(
                0,
                root.projection(),
                root.ownerSessionId(),
                root.cursorName(),
                root.cursorNameHash(),
                root.cursorGeneration(),
                root.lifecycle(),
                root.mutationSequence(),
                root.ackStateEpoch(),
                root.lastProtectionAttemptId(),
                markDeleteOffset,
                root.snapshotReference(),
                root.inlineWholeAckDeltas(),
                root.inlinePartialAckOverrides(),
                root.positionProperties(),
                root.cursorProperties(),
                root.createdAtMillis(),
                root.updatedAtMillis(),
                root.deletedAtMillis());
    }

    private static BatchAckState batch8(long words) {
        return new BatchAckState(8, new long[] {words});
    }

    private static CursorStorageConfig copyConfig(
            CursorStorageConfig source,
            int safetyMargin,
            int inlineAckBytes,
            int inlineDeltaCount,
            int cursorNameBytes,
            int positionPropertyBytes,
            int cursorPropertyBytes) {
        return new CursorStorageConfig(
                source.cursorMetadataValueMaxBytes(),
                safetyMargin,
                inlineAckBytes,
                inlineDeltaCount,
                cursorNameBytes,
                positionPropertyBytes,
                cursorPropertyBytes,
                source.cursorSnapshotMaxBytes(),
                source.cursorAckPositionsPerRequestMax(),
                source.cursorBatchIndexesMax(),
                source.cursorProtectionIntentMaxBytes(),
                source.cursorTrimReasonMaxUtf8Bytes(),
                source.cursorScanPageSize(),
                source.cursorRecordsPerStreamMax(),
                source.cursorOwnerClaimConcurrency(),
                source.cursorMutationQueueMax(),
                source.cursorMaxCasAttempts(),
                source.cursorHydrationMaxAttempts(),
                source.cursorSnapshotIdMaxAttempts(),
                source.cursorMetadataOperationTimeout(),
                source.cursorSnapshotOperationTimeout());
    }

    private record SnapshotFixture(
            CursorAckState snapshotBase,
            CursorStatePersistencePlanner.HydratedState current) {
    }
}
