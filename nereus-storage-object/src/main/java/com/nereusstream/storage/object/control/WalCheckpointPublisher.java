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

package com.nereusstream.storage.object.control;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** Bounded single-candidate physical checkpoint combiner. It is asynchronous and outside append ACK. */
public final class WalCheckpointPublisher implements AutoCloseable {
    private static final long MAX_CONTROL_METADATA_BYTES = 1024L * 1024;
    private static final Comparator<ProviderResolvedExtentDescriptor> ORDER = Comparator.comparingInt(
                    (ProviderResolvedExtentDescriptor value) ->
                            value.row().laneId().code())
            .thenComparingLong(value -> value.row().laneSequence());

    private final CanonicalControlMetadataStore metadata;
    private final String headKey;
    private final String pageKeyPrefix;
    private final WalRunRootRecord root;
    private final Sha256Digest rootSha256;
    private final WalRunObjectSession objectSession;
    private final ArrayList<ProviderResolvedExtentDescriptor> queue = new ArrayList<>();
    private WalCheckpointHeadV1 head;
    private long queuedBodyBytes;
    private final Set<Reservation> reservations = new HashSet<>();
    private long reservedBodyBytes;
    private Attempt pending;
    private boolean ioInFlight;
    private boolean closed;
    private boolean initialized;
    private long generation;
    private RuntimeException lastFailure;
    private ScheduledExecutorService worker;
    private ScheduledFuture<?> scheduled;
    private long scheduleIdentity;
    private final boolean backgroundEnabled;

    WalCheckpointPublisher(
            CanonicalControlMetadataStore metadata,
            String headKey,
            String pageKeyPrefix,
            WalRunRootRecord root,
            WalCheckpointHeadV1 initialHead) {
        this(metadata, headKey, pageKeyPrefix, root, initialHead, null, true);
    }

    /** Production constructor: all adoption/reconciliation reads consume the sole Root-owned recovery budget. */
    public WalCheckpointPublisher(
            CanonicalControlMetadataStore metadata,
            String headKey,
            String pageKeyPrefix,
            WalRunRootRecord root,
            WalCheckpointHeadV1 initialHead,
            WalRunObjectSession objectSession) {
        this(metadata, headKey, pageKeyPrefix, root, initialHead, objectSession, false);
    }

    private WalCheckpointPublisher(
            CanonicalControlMetadataStore metadata,
            String headKey,
            String pageKeyPrefix,
            WalRunRootRecord root,
            WalCheckpointHeadV1 initialHead,
            WalRunObjectSession objectSession,
            boolean isolatedTestFixture) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.root = Objects.requireNonNull(root, "root");
        this.objectSession =
                isolatedTestFixture ? objectSession : Objects.requireNonNull(objectSession, "objectSession");
        if (objectSession != null
                && (!objectSession.rootSha256().equals(WalRunControlCodec.rootSha256(root))
                        || !objectSession.rootRecord().equals(root))) {
            throw new IllegalArgumentException("checkpoint publisher session differs from the exact Root");
        }
        WalRunControlKeys.requireCheckpointHeadKey(headKey, root.shardId(), root.shardRunEpoch());
        WalRunControlKeys.requireCheckpointPagePrefix(pageKeyPrefix, root.shardId(), root.shardRunEpoch());
        this.headKey = headKey;
        this.pageKeyPrefix = pageKeyPrefix;
        this.rootSha256 = WalRunControlCodec.rootSha256(root);
        this.head = Objects.requireNonNull(initialHead, "initialHead");
        backgroundEnabled = !isolatedTestFixture;
        initialized = isolatedTestFixture || initialHead.pageOrdinal() >= 0;
        requireHeadForRoot(initialHead);
        if (initialHead.pageOrdinal() >= 0) {
            if (objectSession != null) {
                objectSession.requireRecoveredCurrentRoot();
            }
            CanonicalBytes expectedHead = WalRunControlCodec.encodeCheckpointHead(initialHead);
            if (!readControl(headKey, MAX_CONTROL_METADATA_BYTES)
                    .map(expectedHead::equals)
                    .orElse(false)) {
                throw new IllegalArgumentException("non-empty initial checkpoint Head is not the exact stored value");
            }
            if (objectSession == null) {
                new WalCheckpointChainVerifier(metadata, root).verify(initialHead);
            } else {
                objectSession.verifyCheckpointChainStreaming(metadata, initialHead, ignored -> {});
            }
        }
        if (objectSession != null) {
            objectSession.attachCheckpointPublisher(this);
        }
    }

    public void initializeHead() {
        CanonicalBytes candidate;
        synchronized (this) {
            requireLive();
            if (ioInFlight || pending != null) {
                throw new IllegalStateException("checkpoint publisher already has an in-flight candidate");
            }
            candidate = WalRunControlCodec.encodeCheckpointHead(head);
            ioInFlight = true;
        }
        try (var lease = acquireSessionIo()) {
            reconcileImmutableOrExact(
                    headKey, candidate, metadata.putIfAbsent(headKey, candidate), MAX_CONTROL_METADATA_BYTES);
            synchronized (this) {
                initialized = true;
            }
        } finally {
            finishIo();
        }
    }

    /** One physical extent/body upper bound, acquired before any member receives a protocol position. */
    public synchronized Reservation reserveBeforePosition(long maximumBodyBytes) {
        requireLive();
        if (!initialized
                || maximumBodyBytes <= 0
                || maximumBodyBytes > root.nwg1AdmissionCaps().maxCanonicalBodyBytes()) {
            throw new IllegalArgumentException("checkpoint pre-position body/Head admission is invalid");
        }
        if (objectSession != null) {
            objectSession.requireCheckpointAdmission();
        }
        long now = System.currentTimeMillis();
        if (reservations.size() >= root.checkpointPolicy().maxUncheckpointedExtents()
                || Math.addExact(reservedBodyBytes, maximumBodyBytes)
                        > root.checkpointPolicy().maxUncheckpointedBytes()
                || requiresAgeForcing(now)) {
            requestBackground(0);
            throw new IllegalStateException("checkpoint pre-position capacity/age is exhausted");
        }
        var reservation = new Reservation(this, maximumBodyBytes, now);
        reservations.add(reservation);
        reservedBodyBytes = Math.addExact(reservedBodyBytes, maximumBodyBytes);
        return reservation;
    }

    public static final class Reservation {
        private final WalCheckpointPublisher owner;
        private final long admittedAtMillis;
        private long bodyBytes;
        private int members;
        private Sha256Digest planSha;
        private boolean sequenced;
        private ProviderResolvedExtentDescriptor descriptor;
        private boolean released;

        private Reservation(WalCheckpointPublisher owner, long bytes, long now) {
            this.owner = owner;
            bodyBytes = bytes;
            admittedAtMillis = now;
        }

        /** Shared members retain the same physical reservation before their individual offset allocations. */
        public Attachment attach() {
            synchronized (owner) {
                owner.requireReservation(this);
                if (planSha != null || sequenced || descriptor != null) {
                    throw new IllegalStateException("checkpoint reservation is already bound to a sealed plan");
                }
                members = Math.incrementExact(members);
                return new Attachment(this);
            }
        }

        public void cancelUnused() {
            synchronized (owner) {
                if (!released) {
                    owner.requireReservation(this);
                    if (members != 0 || sequenced) {
                        throw new IllegalStateException("checkpoint reservation still has admitted members");
                    }
                    owner.releaseReservation(this);
                }
            }
        }
    }

    public static final class Attachment implements AutoCloseable {
        private final Reservation reservation;
        private boolean closed;

        private Attachment(Reservation reservation) {
            this.reservation = reservation;
        }

        @Override
        public void close() {
            var owner = reservation.owner;
            synchronized (owner) {
                if (!closed) {
                    closed = true;
                    reservation.members--;
                    if (!reservation.released && reservation.members == 0 && !reservation.sequenced) {
                        owner.releaseReservation(reservation);
                    }
                }
            }
        }
    }

    /** Checks all pre-position member attachments and narrows their one physical charge to the exact sealed size. */
    public synchronized Reservation bindPlan(List<Attachment> members, Sha256Digest planSha, long exactBodyBytes) {
        requireLive();
        Objects.requireNonNull(planSha, "planSha");
        if (members.isEmpty()) {
            throw new IllegalArgumentException("checkpoint plan has no pre-position reservation");
        }
        var reservation = members.get(0).reservation;
        requireReservation(reservation);
        if (reservation.sequenced || reservation.descriptor != null) {
            throw new IllegalStateException("checkpoint plan already has a physical sequence effect");
        }
        if (members.size() != reservation.members || new HashSet<>(members).size() != members.size()) {
            throw new IllegalArgumentException("checkpoint plan must contain every distinct reserved shared member");
        }
        for (var member : members) {
            if (member.closed || member.reservation != reservation) {
                throw new IllegalArgumentException(
                        "shared extent must use one exact pre-position physical reservation");
            }
        }
        if (exactBodyBytes <= 0
                || exactBodyBytes > reservation.bodyBytes
                || reservation.planSha != null && !reservation.planSha.equals(planSha)) {
            throw new IllegalArgumentException("checkpoint plan differs from its pre-position body/identity charge");
        }
        reservedBodyBytes -= reservation.bodyBytes - exactBodyBytes;
        reservation.bodyBytes = exactBodyBytes;
        reservation.planSha = planSha;
        return reservation;
    }

    public synchronized void sequenceStarted(Reservation reservation) {
        requireReservation(reservation);
        if (reservation.planSha == null) {
            throw new IllegalStateException("checkpoint physical sequence has no bound plan");
        }
        reservation.sequenced = true;
    }

    public synchronized void definitiveNoObject(Reservation reservation) {
        requireReservation(reservation);
        if (reservation.descriptor != null) {
            throw new IllegalStateException("a checkpoint descriptor cannot become absent");
        }
        releaseReservation(reservation);
    }

    /** Existing descriptor-only callers retain their own publication scheduling and bounded resolution admission. */
    public synchronized void enqueue(ProviderResolvedExtentDescriptor descriptor) {
        requireLive();
        var reservation = new Reservation(this, descriptor.row().bodyLength(), descriptor.providerResolvedAtMillis());
        if (reservations.size() >= root.checkpointPolicy().maxUncheckpointedExtents()
                || reservedBodyBytes + reservation.bodyBytes
                        > root.checkpointPolicy().maxUncheckpointedBytes()) {
            throw new IllegalStateException("uncheckpointed-tail bound reached");
        }
        reservations.add(reservation);
        reservedBodyBytes += reservation.bodyBytes;
        reservation.sequenced = true;
        try {
            enqueueReserved(reservation, descriptor);
        } catch (RuntimeException failure) {
            releaseReservation(reservation);
            throw failure;
        }
    }

    public synchronized Attachment attach(Reservation reservation) {
        requireReservation(reservation);
        return reservation.attach();
    }

    /** Converts the already charged physical candidate into its one descriptor without a capacity allocation. */
    public synchronized void enqueue(Reservation reservation, ProviderResolvedExtentDescriptor descriptor) {
        enqueueReserved(reservation, descriptor);
        requestBackground(nextDelayMillis());
    }

    private void enqueueReserved(Reservation reservation, ProviderResolvedExtentDescriptor descriptor) {
        requireLive();
        requireReservation(reservation);
        Objects.requireNonNull(descriptor, "descriptor");
        if (!descriptor.rootSha256().equals(rootSha256)) {
            throw new IllegalArgumentException("descriptor belongs to a different WalRun Root");
        }
        var row = descriptor.row();
        if (!reservation.sequenced
                || reservation.descriptor != null
                || row.bodyLength() != reservation.bodyBytes
                || row.laneSequence() <= head.coveredThrough().get(row.laneId())) {
            throw new IllegalArgumentException("descriptor differs from the exact reserved physical candidate");
        }
        if (row.directoryPrefixEnd() > root.nwg1AdmissionCaps().maxDirectoryPrefixBytes()
                || row.bodyLength() > root.nwg1AdmissionCaps().maxCanonicalBodyBytes()
                || row.providerProof().mode() != root.providerConfiguration().proofMode()
                || row.providerProof().canonicalVersionToken().length()
                        > root.providerConfiguration().proofTokenHardCap()) {
            throw new IllegalArgumentException("descriptor exceeds the Root-admitted NWG1/proof caps");
        }
        for (var queued : queue) {
            if (queued.row().laneId() == row.laneId() && queued.row().laneSequence() == row.laneSequence()) {
                throw new IllegalArgumentException("duplicate provider-resolved extent descriptor");
            }
        }
        queue.add(descriptor);
        reservation.descriptor = descriptor;
        queuedBodyBytes = Math.addExact(queuedBodyBytes, row.bodyLength());
    }

    public synchronized boolean requiresAgeForcing(long nowMillis) {
        for (var reservation : reservations) {
            if (nowMillis < reservation.admittedAtMillis) {
                throw new IllegalArgumentException("clock regressed before checkpoint admission");
            }
            if (nowMillis - reservation.admittedAtMillis
                    >= root.checkpointPolicy().maxUncheckpointedAgeMillis()) {
                return true;
            }
        }
        return false;
    }

    /** Freezes one immutable candidate under a short lock; all page/Head I/O and adoption reads run outside it. */
    public Optional<WalRunCheckpointPageV1> publishNext() {
        Attempt attempt;
        synchronized (this) {
            requireLive();
            while (ioInFlight) {
                try {
                    wait();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("checkpoint wait interrupted; I/O remains protected", interrupted);
                }
                requireLive();
            }
            if (!initialized) {
                throw new IllegalStateException("checkpoint publisher has an uninitialized Head");
            }
            if (pending == null) {
                if (queue.isEmpty()) {
                    return Optional.empty();
                }
                pending = freezePage();
            }
            attempt = pending;
            ioInFlight = true;
        }
        try (var lease = acquireSessionIo()) {
            WalCheckpointHeadV1 resolved = execute(attempt);
            synchronized (this) {
                if (pending != attempt || generation != attempt.generation || head != attempt.expected) {
                    throw new IllegalStateException("checkpoint completion belongs to a stale publisher generation");
                }
                head = resolved;
                removeCovered();
                pending = null;
                lastFailure = null;
                return resolved.equals(attempt.candidate) ? Optional.ofNullable(attempt.page) : Optional.empty();
            }
        } catch (RuntimeException failure) {
            synchronized (this) {
                lastFailure = failure;
            }
            throw failure;
        } finally {
            finishIo();
        }
    }

    private Attempt freezePage() {
        var ordered = new ArrayList<>(queue);
        ordered.sort(ORDER);
        var selected = selectContiguousRows(ordered);
        if (selected.isEmpty()) {
            throw new IllegalStateException("queued descriptors contain no contiguous lane successor");
        }
        selected.subList(Math.min(root.checkpointPolicy().maxRowsPerPage(), selected.size()), selected.size())
                .clear();
        WalRunCheckpointPageV1 page;
        CanonicalBytes bytes;
        while (true) {
            page = new WalRunCheckpointPageV1(
                    rootSha256,
                    Math.incrementExact(head.pageOrdinal()),
                    head.pageSha256(),
                    selected,
                    advancedVector(head.coveredThrough(), selected));
            try {
                bytes = WalRunControlCodec.encodeCheckpointPage(page);
                if (bytes.length() > root.checkpointPolicy().maxCanonicalPageBytes()) {
                    throw new IllegalArgumentException("page exceeds Root-frozen canonical byte cap");
                }
                break;
            } catch (IllegalArgumentException overCap) {
                if (selected.size() == 1) {
                    throw overCap;
                }
                selected.remove(selected.size() - 1);
            }
        }
        var sha = Sha256Digest.hash(bytes);
        var key = pageKey(page.pageOrdinal(), sha);
        var candidate = new WalCheckpointHeadV1(
                rootSha256,
                root.shardRunEpoch(),
                head.publisherEpoch(),
                page.pageOrdinal(),
                Optional.of(key),
                Optional.of(sha),
                page.coveredThrough());
        return new Attempt(generation, head, candidate, page, key, bytes);
    }

    /** Only the single claimed I/O path mutates attempt progress; an uncertain response retains these exact bytes. */
    private WalCheckpointHeadV1 execute(Attempt attempt) {
        if (attempt.page != null && !attempt.pageExact) {
            if (attempt.pageDispatched) {
                var observed =
                        readControl(attempt.pageKey, root.checkpointPolicy().maxCanonicalPageBytes());
                if (observed.isPresent()) {
                    if (!observed.orElseThrow().equals(attempt.pageBytes)) {
                        throw new IllegalStateException("immutable checkpoint page differs from retained candidate");
                    }
                    attempt.pageExact = true;
                }
            }
            if (!attempt.pageExact) {
                attempt.pageDispatched = true;
                reconcileImmutableOrExact(
                        attempt.pageKey,
                        attempt.pageBytes,
                        metadata.putIfAbsent(attempt.pageKey, attempt.pageBytes),
                        root.checkpointPolicy().maxCanonicalPageBytes());
                attempt.pageExact = true;
            }
        }
        if (attempt.headDispatched) {
            var observed = readControl(headKey, MAX_CONTROL_METADATA_BYTES);
            if (observed.map(attempt.candidateBytes::equals).orElse(false)) {
                return attempt.candidate;
            }
            if (!observed.map(attempt.expectedBytes::equals).orElse(false)) {
                return verifiedConflict(attempt.expected, observed);
            }
        }
        attempt.headDispatched = true;
        var outcome = metadata.compareAndSet(headKey, Optional.of(attempt.expectedBytes), attempt.candidateBytes);
        if (outcome == ControlMutationOutcome.APPLIED) {
            return attempt.candidate;
        }
        var observed = readControl(headKey, MAX_CONTROL_METADATA_BYTES);
        if (observed.map(attempt.candidateBytes::equals).orElse(false)) {
            return attempt.candidate;
        }
        if (outcome == ControlMutationOutcome.RESPONSE_UNKNOWN
                && observed.map(attempt.expectedBytes::equals).orElse(false)) {
            throw new IllegalStateException("checkpoint Head UNKNOWN retains the exact candidate for retry");
        }
        return verifiedConflict(attempt.expected, observed);
    }

    private WalCheckpointHeadV1 verifiedConflict(WalCheckpointHeadV1 expected, Optional<CanonicalBytes> observedBytes) {
        var observed = observedBytes
                .map(WalRunControlCodec::decodeCheckpointHead)
                .orElseThrow(() -> new IllegalStateException("checkpoint Head disappeared after CAS"));
        requireAdoptableConflict(expected, observed);
        return observed;
    }

    /** A takeover checks the in-flight cut; retry uses the same frozen candidate after response loss. */
    public void takeover(long newPublisherEpoch) {
        synchronized (this) {
            requireLive();
            if (objectSession != null) {
                objectSession.requireCheckpointAdmission();
            }
            if (ioInFlight) {
                throw new IllegalStateException("checkpoint publisher takeover requires drained I/O");
            }
            if (pending != null) {
                if (pending.page != null || pending.candidate.publisherEpoch() != newPublisherEpoch) {
                    throw new IllegalStateException("checkpoint publisher retains a different unresolved candidate");
                }
            } else {
                if (newPublisherEpoch <= head.publisherEpoch()) {
                    throw new IllegalArgumentException("publisher epoch must increase monotonically");
                }
                var candidate = new WalCheckpointHeadV1(
                        head.rootSha256(),
                        head.shardRunEpoch(),
                        newPublisherEpoch,
                        head.pageOrdinal(),
                        head.pageKey(),
                        head.pageSha256(),
                        head.coveredThrough());
                pending = new Attempt(generation, head, candidate, null, null, null);
            }
        }
        publishNext();
    }

    /** Seal waits for actual I/O termination and exact coverage, without holding the publisher monitor. */
    public void flush() {
        while (true) {
            synchronized (this) {
                if (closed && queue.isEmpty() && pending == null && reservations.isEmpty()) {
                    return;
                }
                requireLive();
                while (ioInFlight) {
                    try {
                        wait();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(
                                "checkpoint drain interrupted; I/O remains protected", interrupted);
                    }
                    requireLive();
                }
                if (queue.isEmpty() && pending == null) {
                    return;
                }
            }
            publishNext();
        }
    }

    private synchronized void requestBackground(long delayMillis) {
        if (!backgroundEnabled || closed || queue.isEmpty() || ioInFlight) {
            return;
        }
        if (scheduled != null && !scheduled.isDone()) {
            if (scheduled.getDelay(TimeUnit.MILLISECONDS) <= delayMillis) {
                return;
            }
            scheduled.cancel(false);
        }
        if (worker == null) {
            worker = Executors.newSingleThreadScheduledExecutor(task -> {
                Thread thread = new Thread(task, "nwg1-checkpoint-" + root.shardId() + "-" + root.shardRunEpoch());
                thread.setDaemon(true);
                return thread;
            });
        }
        long identity = ++scheduleIdentity;
        scheduled = worker.schedule(() -> publishBackground(identity), Math.max(0, delayMillis), TimeUnit.MILLISECONDS);
    }

    private void publishBackground(long identity) {
        synchronized (this) {
            if (identity != scheduleIdentity) {
                return;
            }
            scheduled = null;
            if (closed || ioInFlight || queue.isEmpty()) {
                return;
            }
        }
        try {
            publishNext();
        } catch (RuntimeException failure) {
            synchronized (this) {
                lastFailure = failure;
            }
        } finally {
            synchronized (this) {
                if (!closed && !queue.isEmpty()) {
                    long retryDelay = Math.max(
                            1,
                            root.checkpointPolicy().proactiveCadenceMillis() > 0
                                    ? root.checkpointPolicy().proactiveCadenceMillis()
                                    : root.checkpointPolicy().maxUncheckpointedAgeMillis());
                    requestBackground(lastFailure == null ? nextDelayMillis() : retryDelay);
                }
            }
        }
    }

    private long nextDelayMillis() {
        long now = System.currentTimeMillis();
        if (reservations.size() >= root.checkpointPolicy().maxUncheckpointedExtents()
                || reservedBodyBytes >= root.checkpointPolicy().maxUncheckpointedBytes()) {
            return 0;
        }
        long delay = root.checkpointPolicy().maxUncheckpointedAgeMillis();
        long cadence = root.checkpointPolicy().proactiveCadenceMillis();
        if (cadence > 0) {
            delay = Math.min(delay, cadence);
        }
        for (var reservation : reservations) {
            delay = Math.min(
                    delay,
                    Math.max(
                            0,
                            root.checkpointPolicy().maxUncheckpointedAgeMillis()
                                    - Math.max(0, now - reservation.admittedAtMillis)));
        }
        return delay;
    }

    private synchronized void finishIo() {
        ioInFlight = false;
        notifyAll();
    }

    private WalRunObjectSession.IoLease acquireSessionIo() {
        return objectSession == null ? null : objectSession.acquireIo();
    }

    public synchronized int uncoveredExtentCount() {
        return reservations.size();
    }

    public synchronized long uncoveredBodyBytes() {
        return reservedBodyBytes;
    }

    public synchronized Optional<RuntimeException> lastFailure() {
        return Optional.ofNullable(lastFailure);
    }

    private void requireLive() {
        if (closed) {
            throw new IllegalStateException("checkpoint publisher is closed");
        }
    }

    private void requireReservation(Reservation reservation) {
        requireLive();
        if (reservation.owner != this || reservation.released || !reservations.contains(reservation)) {
            throw new IllegalArgumentException("checkpoint reservation belongs to another publisher or is terminal");
        }
    }

    private void releaseReservation(Reservation reservation) {
        if (reservation.released || !reservations.remove(reservation)) {
            throw new IllegalStateException("checkpoint capacity was released twice");
        }
        reservedBodyBytes = Math.subtractExact(reservedBodyBytes, reservation.bodyBytes);
        reservation.released = true;
    }

    @Override
    public synchronized void close() {
        if (ioInFlight) {
            throw new IllegalStateException("checkpoint publisher retains in-flight I/O; drain before close");
        }
        if (!closed) {
            closed = true;
            generation++;
            if (scheduled != null) {
                scheduled.cancel(false);
            }
            if (worker != null) {
                worker.shutdown();
            }
        }
    }

    /** Stops scheduled work after the current real I/O has terminated; unresolved candidates remain retained. */
    public synchronized void closeAfterDrain() {
        while (ioInFlight) {
            try {
                wait();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("checkpoint close interrupted; I/O remains protected", interrupted);
            }
        }
        close();
    }

    private static final class Attempt {
        private final long generation;
        private final WalCheckpointHeadV1 expected;
        private final WalCheckpointHeadV1 candidate;
        private final CanonicalBytes expectedBytes;
        private final CanonicalBytes candidateBytes;
        private final WalRunCheckpointPageV1 page;
        private final String pageKey;
        private final CanonicalBytes pageBytes;
        private boolean pageDispatched;
        private boolean pageExact;
        private boolean headDispatched;

        private Attempt(
                long generation,
                WalCheckpointHeadV1 expected,
                WalCheckpointHeadV1 candidate,
                WalRunCheckpointPageV1 page,
                String pageKey,
                CanonicalBytes pageBytes) {
            this.generation = generation;
            this.expected = expected;
            this.candidate = candidate;
            this.page = page;
            this.pageKey = pageKey;
            this.pageBytes = pageBytes;
            expectedBytes = WalRunControlCodec.encodeCheckpointHead(expected);
            candidateBytes = WalRunControlCodec.encodeCheckpointHead(candidate);
        }
    }

    public synchronized WalCheckpointHeadV1 head() {
        return head;
    }

    public synchronized int queueDepth() {
        return queue.size();
    }

    public synchronized long queuedBodyBytes() {
        return queuedBodyBytes;
    }

    public synchronized void requireFinalCoverage(LaneSequenceVector terminalSequence) {
        if (ioInFlight
                || pending != null
                || !reservations.isEmpty()
                || !queue.isEmpty()
                || !head.coveredThrough().equals(terminalSequence)) {
            throw new IllegalStateException("final checkpoint head does not exactly cover the Seal terminal vector");
        }
    }

    private ArrayList<ProviderResolvedExtentRowV1> selectContiguousRows(
            List<ProviderResolvedExtentDescriptor> ordered) {
        long[] expected = head.coveredThrough().toArray();
        boolean[] exhausted = new boolean[expected.length];
        for (int index = 0; index < expected.length; index++) {
            if (expected[index] == Long.MAX_VALUE) {
                exhausted[index] = true;
            } else {
                expected[index] = Math.incrementExact(expected[index]);
            }
        }
        ArrayList<ProviderResolvedExtentRowV1> selected = new ArrayList<>();
        for (ProviderResolvedExtentDescriptor descriptor : ordered) {
            ProviderResolvedExtentRowV1 row = descriptor.row();
            int lane = row.laneId().code();
            if (!exhausted[lane] && row.laneSequence() == expected[lane]) {
                selected.add(row);
                if (row.laneSequence() == Long.MAX_VALUE) {
                    exhausted[lane] = true;
                } else {
                    expected[lane] = Math.incrementExact(expected[lane]);
                }
            }
        }
        return selected;
    }

    private static LaneSequenceVector advancedVector(
            LaneSequenceVector predecessor, List<ProviderResolvedExtentRowV1> rows) {
        LaneSequenceVector result = predecessor;
        for (ProviderResolvedExtentRowV1 row : rows) {
            result = result.with(row.laneId(), row.laneSequence());
        }
        return result;
    }

    private void removeCovered() {
        Iterator<ProviderResolvedExtentDescriptor> iterator = queue.iterator();
        while (iterator.hasNext()) {
            ProviderResolvedExtentDescriptor descriptor = iterator.next();
            if (head.coveredThrough().get(descriptor.row().laneId())
                    >= descriptor.row().laneSequence()) {
                queuedBodyBytes =
                        Math.subtractExact(queuedBodyBytes, descriptor.row().bodyLength());
                iterator.remove();
                var reservation = reservations.stream()
                        .filter(value -> value.descriptor == descriptor)
                        .findFirst()
                        .orElseThrow();
                releaseReservation(reservation);
            }
        }
    }

    private void requireAdoptableConflict(WalCheckpointHeadV1 predecessor, WalCheckpointHeadV1 observed) {
        requireHeadForRoot(observed);
        if (observed.publisherEpoch() != predecessor.publisherEpoch()
                || observed.pageOrdinal() < predecessor.pageOrdinal()
                || !observed.coveredThrough().componentWiseAtLeast(predecessor.coveredThrough())) {
            throw new IllegalStateException("checkpoint head conflict is stale, forked, or regressing");
        }
        if (observed.pageOrdinal() == predecessor.pageOrdinal()) {
            if (!observed.pageKey().equals(predecessor.pageKey())
                    || !observed.pageSha256().equals(predecessor.pageSha256())
                    || !observed.coveredThrough().equals(predecessor.coveredThrough())) {
                throw new IllegalStateException("same-ordinal checkpoint Head conflict is a fork");
            }
        } else if (objectSession == null) {
            WalCheckpointChainVerifier.Verification verified =
                    new WalCheckpointChainVerifier(metadata, root).verify(observed);
            if (predecessor.pageOrdinal() >= 0
                    && !verified.containsExact(
                            predecessor.pageOrdinal(),
                            predecessor.pageKey().orElseThrow(),
                            predecessor.pageSha256().orElseThrow())) {
                throw new IllegalStateException("checkpoint Head conflict is not a descendant of the current page");
            }
        } else {
            objectSession.verifyCheckpointChainDescendant(
                    metadata,
                    observed,
                    predecessor.pageOrdinal(),
                    predecessor.pageKey().orElse(""),
                    predecessor.pageSha256().orElse(rootSha256));
        }
    }

    private void requireHeadForRoot(WalCheckpointHeadV1 value) {
        if (!value.rootSha256().equals(rootSha256) || value.shardRunEpoch() != root.shardRunEpoch()) {
            throw new IllegalArgumentException("checkpoint head belongs to a different Root/run epoch");
        }
    }

    private void reconcileImmutableOrExact(
            String key, CanonicalBytes candidate, ControlMutationOutcome outcome, long maximumCanonicalBytes) {
        if (outcome == ControlMutationOutcome.APPLIED
                || readControl(key, maximumCanonicalBytes)
                        .map(candidate::equals)
                        .orElse(false)) {
            return;
        }
        throw new IllegalStateException("immutable checkpoint record did not converge to exact candidate");
    }

    private Optional<CanonicalBytes> readControl(String key, long maximumCanonicalBytes) {
        if (objectSession != null) {
            objectSession.chargeRecoveryControlMetadata(maximumCanonicalBytes);
        }
        return metadata.get(key);
    }

    private String pageKey(long ordinal, Sha256Digest pageSha) {
        return WalRunControlKeys.checkpointPageKey(root.shardId(), root.shardRunEpoch(), ordinal, pageSha);
    }
}
