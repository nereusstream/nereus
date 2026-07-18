/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.retention;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ProjectionType;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationActivationSubject;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.capability.LiveProjectionSubject;
import com.nereusstream.managedledger.NereusManagedLedgerOwnershipGuard;
import com.nereusstream.managedledger.cursor.CursorLedgerIdentity;
import com.nereusstream.managedledger.cursor.CursorOwnerSession;
import com.nereusstream.managedledger.cursor.CursorRetentionCoordinator;
import com.nereusstream.managedledger.cursor.CursorRetentionView;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NereusManagedLedgerRetentionTest {
    private static final String TOPIC =
            "persistent://tenant/ns/retention-service";
    private static final StreamId STREAM =
            ManagedLedgerProjectionNames.streamId(TOPIC, 1);
    private static final RetentionPolicySnapshot POLICY =
            new RetentionPolicySnapshot(99, 7_000, 150);
    private static final Checksum SHA_A = sha('a');
    private static final Checksum SHA_B = sha('b');

    private CursorOwnerSession owner;
    private LiveProjectionSubject subject;
    private RetentionCandidate candidate;

    @BeforeEach
    void setUp() {
        ManagedLedgerProjectionIdentity projection =
                new ManagedLedgerProjectionIdentity(
                        1,
                        1,
                        STREAM.value(),
                        ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID + 19);
        owner = new CursorOwnerSession(
                new CursorLedgerIdentity(
                        TOPIC,
                        ManagedLedgerProjectionNames.managedLedgerNameHash(TOPIC),
                        projection),
                "0123456789abcdef0123456789abcdef");
        subject = new LiveProjectionSubject(
                STREAM,
                new ProjectionRef(
                        ProjectionType.VIRTUAL_LEDGER,
                        "retention-service-projection"),
                SHA_A);
        candidate = new RetentionCandidate(
                STREAM,
                0,
                30,
                25,
                20,
                10,
                20,
                7,
                11,
                POLICY.policyVersion(),
                List.of(),
                SHA_B,
                10_000);
    }

    @Test
    void gatesAndRevalidatesBeforeEnteringF3Trim() {
        List<String> calls = new ArrayList<>();
        NereusManagedLedgerRetentionService service = service(
                calls,
                new AtomicInteger(),
                Optional.of(candidate));

        Optional<RetentionCandidate> result = service.trim(
                        "pulsar-retention")
                .join();

        assertThat(result).contains(candidate);
        assertThat(calls).containsExactly(
                "owned",
                "activation-require:LOGICAL_TRIM:true",
                "policy",
                "planner-plan",
                "activation-revalidate",
                "planner-revalidate",
                "request-trim:20:pulsar-retention",
                "owned",
                "observer:20");
    }

    @Test
    void noOpStillRequiresFinalOwnershipButDoesNotMutate() {
        List<String> calls = new ArrayList<>();
        NereusManagedLedgerRetentionService service = service(
                calls,
                new AtomicInteger(),
                Optional.empty());

        assertThat(service.trim("housekeeping").join()).isEmpty();
        assertThat(calls).containsExactly(
                "owned",
                "activation-require:LOGICAL_TRIM:true",
                "policy",
                "planner-plan",
                "owned");
    }

    @Test
    void policyAdmissionActivatesAndRevalidatesWithoutPlanningOrCursorMutation() {
        List<String> calls = new ArrayList<>();
        NereusManagedLedgerRetentionService service = service(
                calls,
                new AtomicInteger(),
                Optional.of(candidate));

        service.ensurePolicyAdmissionReady().join();

        assertThat(calls).containsExactly(
                "owned",
                "activation-require:LOGICAL_TRIM:true",
                "activation-revalidate",
                "owned");
    }

    @Test
    void lostOwnershipAfterDurableTrimFailsCallbackAndSkipsObserver() {
        List<String> calls = new ArrayList<>();
        AtomicInteger ownershipChecks = new AtomicInteger();
        NereusManagedLedgerRetentionService service = service(
                calls,
                ownershipChecks,
                Optional.of(candidate));

        assertThatThrownBy(() -> service.trim("admin-trim").join())
                .hasCauseInstanceOf(
                        ManagedLedgerException.ManagedLedgerFencedException.class);
        assertThat(calls).contains("request-trim:20:admin-trim");
        assertThat(calls).doesNotContain("observer:20");
    }

    private NereusManagedLedgerRetentionService service(
            List<String> calls,
            AtomicInteger ownershipChecks,
            Optional<RetentionCandidate> planned) {
        NereusManagedLedgerOwnershipGuard ownership =
                NereusManagedLedgerOwnershipGuard.checked(
                        () -> {
                            calls.add("owned");
                            int check = ownershipChecks.incrementAndGet();
                            boolean owned = !calls.contains(
                                            "request-trim:20:admin-trim")
                                    || check == 1;
                            return CompletableFuture.completedFuture(owned);
                        },
                        Duration.ofSeconds(1));
        GenerationProtocolActivationGuard activation =
                new GenerationProtocolActivationGuard() {
                    @Override
                    public CompletableFuture<GenerationActivationProof>
                            requireReady(
                                    GenerationOperation operation,
                                    GenerationActivationSubject supplied,
                                    boolean activateLiveProjectionIfAbsent) {
                        calls.add("activation-require:"
                                + operation
                                + ":"
                                + activateLiveProjectionIfAbsent);
                        return CompletableFuture.completedFuture(
                                GenerationActivationProof.create(
                                        operation,
                                        supplied,
                                        7,
                                        3,
                                        2,
                                        SHA_B,
                                        true,
                                        true,
                                        10_000));
                    }

                    @Override
                    public CompletableFuture<Void> revalidate(
                            GenerationActivationProof proof) {
                        calls.add("activation-revalidate");
                        return CompletableFuture.completedFuture(null);
                    }
                };
        RetentionCandidatePlanner planner =
                new RetentionCandidatePlanner() {
                    @Override
                    public CompletableFuture<Optional<RetentionCandidate>>
                            plan(
                                    StreamId streamId,
                                    RetentionPolicySnapshot policy) {
                        calls.add("planner-plan");
                        return CompletableFuture.completedFuture(planned);
                    }

                    @Override
                    public CompletableFuture<Void> revalidate(
                            RetentionCandidate supplied,
                            RetentionPolicySnapshot policy) {
                        calls.add("planner-revalidate");
                        return CompletableFuture.completedFuture(null);
                    }
                };
        CursorRetentionCoordinator coordinator =
                new CursorRetentionCoordinator() {
                    @Override
                    public CompletableFuture<CursorRetentionView>
                            claimAndRecover(CursorOwnerSession supplied) {
                        return unsupported();
                    }

                    @Override
                    public CompletableFuture<ProtectionLease> beginProtection(
                            CursorOwnerSession supplied,
                            ProtectionRequest request) {
                        return unsupported();
                    }

                    @Override
                    public CompletableFuture<CursorRetentionView>
                            completeProtection(ProtectionLease lease) {
                        return unsupported();
                    }

                    @Override
                    public CompletableFuture<CursorRetentionView>
                            reconcileFloor(CursorOwnerSession supplied) {
                        return unsupported();
                    }

                    @Override
                    public CompletableFuture<CursorRetentionView> requestTrim(
                            CursorOwnerSession supplied,
                            long candidateOffset,
                            String reason) {
                        calls.add("request-trim:"
                                + candidateOffset
                                + ":"
                                + reason);
                        return CompletableFuture.completedFuture(
                                activeTrim(candidateOffset));
                    }

                    @Override
                    public void close() {
                    }
                };
        return new NereusManagedLedgerRetentionService(
                STREAM,
                subject,
                ownership,
                activation,
                streamId -> {
                    calls.add("policy");
                    return CompletableFuture.completedFuture(POLICY);
                },
                planner,
                coordinator,
                owner,
                view -> calls.add(
                        "observer:" + view.lastCompletedTrimOffset()));
    }

    private CursorRetentionView activeTrim(long trimOffset) {
        return new CursorRetentionView(
                owner.ledger(),
                owner.ownerSessionId(),
                CursorRetentionView.Lifecycle.ACTIVE,
                2,
                12,
                trimOffset,
                trimOffset,
                Optional.empty(),
                Optional.empty());
    }

    private static <T> CompletableFuture<T> unsupported() {
        return CompletableFuture.failedFuture(
                new UnsupportedOperationException());
    }

    private static Checksum sha(char value) {
        return new Checksum(
                ChecksumType.SHA256,
                Character.toString(value).repeat(64));
    }
}
