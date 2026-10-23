/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.kafka.bookkeeper.object.publication;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.protocol.ProtocolKindV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.KafkaObjectAuthorizedRecoveryV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.KafkaProtocolCheckpointHeadCodecV1;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.control.TerminalProtocolCheckpointBindingV1;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunControlKeys;
import com.nereusstream.storage.object.control.WalRunKafkaProtocolEmptyClosureV1;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunPredecessor;
import com.nereusstream.storage.object.control.WalRunRecoveryRetirementVerifierV1;
import com.nereusstream.storage.object.control.WalRunReference;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.control.WalRunTerminalClosureProofV1;
import com.nereusstream.storage.object.nwg1.Nwg1VerificationContextV1;
import com.nereusstream.storage.object.recovery.CumulativeRecoveryBudget;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;

/**
 * Create-only whole-Run recovery retirement, derived from actual sealed inventory and selected common checkpoints.
 * Closed-history records and their grants/checkpoints stay protected by the existing monotonic authorization Head.
 * Neither certificate publication nor its read-side proof releases an M4 pin or authorizes any physical deletion.
 */
public final class KafkaObjectRunRecoveryRetirementV1 implements WalRunRecoveryRetirementVerifierV1 {
    private final KafkaObjectAuthorizationV1 authority;
    private final CanonicalControlMetadataStore metadata;
    private Attempt pending;
    private final KafkaObjectRunProtocolClosureV1 protocolClosure;

    public KafkaObjectRunRecoveryRetirementV1(KafkaObjectAuthorizationV1 authority) {
        this.authority = Objects.requireNonNull(authority, "authority");
        metadata = authority.retirementMetadata();
        protocolClosure = new KafkaObjectRunProtocolClosureV1(metadata);
    }

    /** An opaque confirmed decision; callers cannot construct a certificate or a recovery cut with a hash/list. */
    public static final class Retirement {
        private final String key;
        private final Sha256Digest digest;
        private final WalRunReference root;
        private final long prefixRuns;

        private Retirement(String key, CanonicalBytes raw, KafkaObjectRunRetirementCertificateV1 certificate) {
            this.key = key;
            digest = Sha256Digest.hash(raw);
            root = certificate.seal().root();
            prefixRuns = certificate.prefixRuns();
        }

        public Sha256Digest certificateSha256() {
            return digest;
        }

        public WalRunReference retiredRoot() {
            return root;
        }

        public long prefixRuns() {
            return prefixRuns;
        }

        /** Exact immutable aggregate terminal binding, not a legacy single-Binding NWKCP1 Head. */
        public TerminalProtocolCheckpointBindingV1 terminalBinding() {
            return new TerminalProtocolCheckpointBindingV1(ProtocolKindV1.KAFKA, key, digest);
        }
    }

    public synchronized Retirement retire(
            WalRunObjectSession session,
            WalRunTerminalClosureProofV1 closure,
            Nwg1VerificationContextV1 verification,
            List<KafkaObjectAuthorizationV1.ClosedHistory> members)
            throws IOException {
        return retire(session, closure, verification, members, grant -> {
            if (!grant.locator().extent().walRunRootSha().equals(session.rootSha256())) {
                throw new IllegalStateException("protocol transfer requires the exact retained cross-Run source");
            }
            return new KafkaObjectAuthorizedRecoveryV1.Source(session, verification);
        });
    }

    public synchronized Retirement retire(
            WalRunObjectSession session,
            WalRunTerminalClosureProofV1 closure,
            Nwg1VerificationContextV1 verification,
            List<KafkaObjectAuthorizationV1.ClosedHistory> members,
            KafkaObjectAuthorizedRecoveryV1.Resolver resolver)
            throws IOException {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(closure, "closure");
        if (pending != null) {
            if (pending.session != session || !pending.certificate.seal().root().equals(closure.root())) {
                throw new IllegalStateException("retain the original unresolved Run retirement candidate");
            }
            return convergePending();
        }
        // An existing complete immutable decision must remain recoverable after its old protocol Object retires.
        if (!protocolClosure.hasPending()) {
            try (var io = session.acquireTerminalIo();
                    var scope = sessionScope(session)) {
                var existing = scope.get(key(closure.root()));
                if (existing.isPresent()) {
                    var certificate = verifyCertificate(existing.orElseThrow(), session.rootRecord(), scope);
                    if (!certificate.seal().root().equals(closure.root())) {
                        throw new IllegalStateException("existing retirement certificate names a different Run");
                    }
                    return new Retirement(key(closure.root()), existing.orElseThrow(), certificate);
                }
            }
        }
        try (var domain = protocolClosure.close(session, closure);
                var scope = sessionScope(session)) {
            scope.adopt(domain.transferMemory());
            var root = session.rootRecord();
            requireKafka(root);
            var seal =
                    WalRunKafkaProtocolEmptyClosureV1.forClosure(root, closure).seal();
            WalRunReference genesis = seal.root();
            long count = 1;
            Optional<Sha256Digest> previous = Optional.empty();
            if (root.predecessor().isPresent()) {
                var boundary = root.predecessor().orElseThrow();
                CanonicalBytes priorRaw = scope.get(key(boundary.root()))
                        .orElseThrow(() ->
                                new IllegalStateException("retirement cannot cross an unqualified predecessor prefix"));
                var prior = verifyCertificate(priorRaw, root, scope);
                requireBoundary(prior, boundary, root);
                genesis = prior.genesis();
                count = Math.addExact(prior.prefixRuns(), 1);
                previous = Optional.of(Sha256Digest.hash(priorRaw));
            }
            var reader = authority.retirementReader(scope, root.shardId());
            var snapshots = members.stream()
                    .map(KafkaObjectAuthorizationV1::closedSnapshotBytes)
                    .sorted(Comparator.comparing(
                            value -> Sha256Digest.hash(value).toHex()))
                    .toList();
            if (snapshots.size() > KafkaObjectRunRetirementCertificateV1.MAX_MEMBERS) {
                throw new IllegalArgumentException("retirement member inventory exceeds its format bound");
            }
            scope.decoded(snapshots.size());
            var transfer = new KafkaObjectRunReplayTransferV1(reader, root, snapshots, domain.checkpoint());
            List<CanonicalBytes> qualified = transfer.verify(session, metadata, seal, verification, resolver);
            var certificate = new KafkaObjectRunRetirementCertificateV1(
                    root, seal, genesis, count, previous, domain.exactHead(), qualified);
            CanonicalBytes raw = certificate.encode();
            var memory = session.acquireRecoveryWorkingSet(Math.multiplyExact(3L, raw.length()));
            pending = new Attempt(session, certificate, raw, domain.transferIo(), memory);
        }
        return convergePending();
    }

    @Override
    public boolean isRetiredPrefix(
            WalRunPredecessor boundary, WalRunRootRecord retainedRoot, CumulativeRecoveryBudget budget) {
        if (!retainedRoot.predecessor().equals(Optional.of(boundary))) {
            throw new IllegalArgumentException("retirement lookup is not the actual retained-to-retired boundary");
        }
        try (var scope = new ReadScope(
                metadata,
                budget::reserveControlMetadata,
                bytes -> budgetWorkspace(budget, bytes),
                contexts -> budget.chargeDecoded(contexts, 0, 0))) {
            var raw = scope.get(key(boundary.root()));
            if (raw.isEmpty()) {
                return false;
            }
            var certificate = verifyCertificate(raw.orElseThrow(), retainedRoot, scope);
            requireBoundary(certificate, boundary, retainedRoot);
            return true;
        }
    }

    private Retirement convergePending() {
        Attempt attempt = pending;
        var root = attempt.certificate.seal().root();
        String key = key(root);
        // The original session I/O and memory leases survive dispatch, response loss, read failure and retry.
        attempt.session.chargeRecoveryControlMetadata(attempt.raw.length());
        ControlMutationOutcome outcome = metadata.putIfAbsent(key, attempt.raw);
        Retirement result = null;
        boolean conflict = false;
        try (var scope = sessionScope(attempt.session)) {
            var found = scope.get(key);
            if (found.isPresent() && found.orElseThrow().equals(attempt.raw)) {
                // Legal Head advancement cannot roll back this already-qualified selected closed history.
                result = new Retirement(key, attempt.raw, attempt.certificate);
            } else if (outcome == ControlMutationOutcome.DEFINITIVE_CONFLICT && found.isPresent()) {
                conflict = true;
            }
        }
        if (result != null || conflict) {
            attempt.close();
            pending = null;
            if (conflict) {
                throw new IllegalStateException("Run retirement lost to a different immutable certificate");
            }
            return result;
        }
        throw new KafkaObjectAuthorizationV1.Unknown(
                "Run retirement response is unknown; retain the exact original candidate and leases");
    }

    private KafkaObjectRunRetirementCertificateV1 verifyCertificate(
            CanonicalBytes raw, WalRunRootRecord expectedCell, ReadScope scope) {
        var certificate = KafkaObjectRunRetirementCertificateV1.decode(raw);
        var root = certificate.root();
        requireKafka(root);
        if (!root.protocolCellIdentity().equals(expectedCell.protocolCellIdentity())
                || !root.providerScopeId().equals(expectedCell.providerScopeId())
                || root.shardId() != expectedCell.shardId()) {
            throw new IllegalStateException("retirement certificate substituted Cell/provider/shard");
        }
        var reference = certificate.seal().root();
        if (!reference.rootKey().equals(WalRunControlKeys.rootKey(root.shardId(), root.shardRunEpoch()))
                || !scope.get(reference.rootKey()).equals(Optional.of(WalRunControlCodec.encodeRoot(root)))
                || !scope.get(WalRunControlKeys.sealKey(root.shardId(), root.shardRunEpoch()))
                        .equals(Optional.of(WalRunControlCodec.encodeSeal(certificate.seal())))) {
            throw new IllegalStateException("retirement certificate lost its exact immutable Root/Seal");
        }
        var protocolKey = WalRunControlKeys.kafkaProtocolCheckpointHeadKey(root.shardId(), root.shardRunEpoch());
        if (!scope.get(protocolKey).equals(Optional.of(certificate.protocolHead()))) {
            throw new IllegalStateException("retirement lost its exact terminal protocol-domain selection");
        }
        scope.decoded(certificate.closedSnapshots().size());
        var reader = authority.retirementReader(scope, root.shardId());
        var bindings = new java.util.HashSet<String>();
        var selectedMembers = new java.util.ArrayList<KafkaObjectAuthorizationV1.ClosedHistory>();
        for (var snapshot : certificate.closedSnapshots()) {
            var selected = reader.verifyRetirementSnapshot(snapshot);
            if (!bindings.add(reader.headKey(selected.head().fence()))) {
                throw new IllegalStateException("retirement certificate repeats a Binding checkpoint witness");
            }
            KafkaObjectRunReplayTransferV1.requireCheckpoint(reader, root, selected);
            selectedMembers.add(selected);
        }
        if (!WalRunKafkaProtocolEmptyClosureV1.hasMagic(certificate.protocolHead())) {
            var head = KafkaProtocolCheckpointHeadCodecV1.decodeForRoot(root, certificate.protocolHead());
            KafkaObjectRunReplayTransferV1.requireProtocolMember(reader, head, selectedMembers);
        }
        if (root.predecessor().isPresent()) {
            // One direct immutable link is checked; never recursively traverse all retired certificates/history.
            var boundary = root.predecessor().orElseThrow();
            CanonicalBytes priorRaw = scope.get(key(boundary.root()))
                    .orElseThrow(() -> new IllegalStateException("retirement direct-prefix certificate is absent"));
            if (!Sha256Digest.hash(priorRaw)
                    .equals(certificate.previousCertificate().orElseThrow())) {
                throw new IllegalStateException("retirement direct-prefix certificate digest differs");
            }
            var prior = KafkaObjectRunRetirementCertificateV1.decode(priorRaw);
            requireBoundary(prior, boundary, root);
            if (!prior.genesis().equals(certificate.genesis())
                    || Math.addExact(prior.prefixRuns(), 1) != certificate.prefixRuns()) {
                throw new IllegalStateException("retirement prefix genesis/count is not the exact direct extension");
            }
        }
        return certificate;
    }

    private static void requireBoundary(
            KafkaObjectRunRetirementCertificateV1 certificate, WalRunPredecessor boundary, WalRunRootRecord retained) {
        if (!certificate.seal().root().equals(boundary.root())
                || !WalRunControlCodec.sealSha256(certificate.seal()).equals(boundary.sealSha256())
                || !boundary.sealKey()
                        .equals(WalRunControlKeys.sealKey(
                                boundary.root().shardId(), boundary.root().shardRunEpoch()))
                || !certificate.root().protocolCellIdentity().equals(retained.protocolCellIdentity())
                || !certificate.root().providerScopeId().equals(retained.providerScopeId())
                || certificate.root().shardId() != retained.shardId()
                || certificate.root().shardRunEpoch() >= retained.shardRunEpoch()) {
            throw new IllegalStateException("retirement certificate is not the exact older prefix boundary");
        }
    }

    private static void requireKafka(WalRunRootRecord root) {
        if (root.protocolCellIdentity().protocolKind() != ProtocolKindV1.KAFKA) {
            throw new IllegalArgumentException("Kafka Run retirement requires the actual Kafka protocol Cell");
        }
    }

    private static String key(WalRunReference root) {
        WalRunControlKeys.requireRootKey(root.rootKey(), root.shardId(), root.shardRunEpoch());
        return root.rootKey() + "/recovery-retirement-v1";
    }

    private ReadScope sessionScope(WalRunObjectSession session) {
        return new ReadScope(
                metadata,
                session::reserveRecoveryControlMetadata,
                bytes -> sessionWorkspace(session, bytes),
                contexts -> session.chargeRecoveryDecoded(contexts, 0, 0));
    }

    private interface Workspace extends AutoCloseable {
        void grow(long bytes);

        @Override
        void close();
    }

    private static Workspace sessionWorkspace(WalRunObjectSession session, long bytes) {
        return leaseWorkspace(session.acquireRecoveryWorkingSet(bytes));
    }

    private static Workspace leaseWorkspace(WalRunObjectSession.RecoveryWorkingSetLease lease) {
        return new Workspace() {
            @Override
            public void grow(long additional) {
                lease.grow(additional);
            }

            @Override
            public void close() {
                lease.close();
            }
        };
    }

    private static Workspace budgetWorkspace(CumulativeRecoveryBudget budget, long bytes) {
        budget.acquireWorkingSet(bytes);
        return new Workspace() {
            private long held = bytes;

            @Override
            public void grow(long additional) {
                budget.growWorkingSet(additional);
                held = Math.addExact(held, additional);
            }

            @Override
            public void close() {
                budget.releaseWorkingSet(held);
            }
        };
    }

    private static final class Attempt {
        private final WalRunObjectSession session;
        private final KafkaObjectRunRetirementCertificateV1 certificate;
        private final CanonicalBytes raw;
        private final WalRunObjectSession.IoLease io;
        private final WalRunObjectSession.RecoveryWorkingSetLease memory;

        private Attempt(
                WalRunObjectSession session,
                KafkaObjectRunRetirementCertificateV1 certificate,
                CanonicalBytes raw,
                WalRunObjectSession.IoLease io,
                WalRunObjectSession.RecoveryWorkingSetLease memory) {
            this.session = session;
            this.certificate = certificate;
            this.raw = raw;
            this.io = io;
            this.memory = memory;
        }

        private void close() {
            memory.close();
            io.close();
        }
    }

    private static final class ReadScope implements CanonicalControlMetadataStore, AutoCloseable {
        private final CanonicalControlMetadataStore delegate;
        private final LongFunction<CumulativeRecoveryBudget.ControlMetadataReservation> reserveControl;
        private final LongFunction<Workspace> reserve;
        private final LongConsumer decoded;
        private Workspace held;

        private ReadScope(
                CanonicalControlMetadataStore delegate,
                LongFunction<CumulativeRecoveryBudget.ControlMetadataReservation> reserveControl,
                LongFunction<Workspace> reserve,
                LongConsumer decoded) {
            this.delegate = delegate;
            this.reserveControl = reserveControl;
            this.reserve = reserve;
            this.decoded = decoded;
        }

        @Override
        public Optional<CanonicalBytes> get(String key) {
            var reservation = reserveControl.apply(KafkaObjectRunRetirementCertificateV1.MAX_BYTES);
            var value = delegate.get(key);
            if (value.isPresent()) {
                if (value.orElseThrow().length() > KafkaObjectRunRetirementCertificateV1.MAX_BYTES) {
                    throw new IllegalStateException("retirement metadata value exceeds its admitted control bound");
                }
                long bytes = Math.multiplyExact(3L, value.orElseThrow().length());
                if (bytes > 0) {
                    if (held == null) {
                        held = reserve.apply(bytes);
                    } else {
                        held.grow(bytes);
                    }
                }
            }
            reservation.settle(value.map(CanonicalBytes::length).orElse(0));
            return value;
        }

        private void adopt(WalRunObjectSession.RecoveryWorkingSetLease lease) {
            if (held != null) {
                throw new IllegalStateException("retirement workspace is already initialized");
            }
            held = leaseWorkspace(lease);
        }

        private void decoded(long contexts) {
            decoded.accept(contexts);
        }

        @Override
        public ControlMutationOutcome putIfAbsent(String key, CanonicalBytes value) {
            throw new UnsupportedOperationException("retirement witness verification is read-only");
        }

        @Override
        public ControlMutationOutcome compareAndSet(
                String key, Optional<CanonicalBytes> expected, CanonicalBytes value) {
            throw new UnsupportedOperationException("retirement witness verification is read-only");
        }

        @Override
        public void close() {
            if (held != null) {
                held.close();
            }
        }
    }
}
