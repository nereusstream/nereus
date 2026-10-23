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

package com.nereusstream.pulsar.offload;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.protocol.ProtocolCellIdentity;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1;
import com.nereusstream.storage.api.lifecycle.MetadataNamespaceIdentityV2;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import io.oxia.client.api.AsyncOxiaClient;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/** Fixed owner/hazard-slot shares for one admitted local Cell owner. Retain this instance for its entire life. */
public final class PulsarNativeReadCellBudgetV1 {
    public record Usage(long owners, long readSlots, long encodedBytes, long decodedBytes) {
        public Usage {
            if (owners < 0 || readSlots < 0 || encodedBytes < 0 || decodedBytes < 0) {
                throw new IllegalArgumentException("negative Native Cell usage");
            }
        }

        private Usage plus(Usage value) {
            return new Usage(
                    Math.addExact(owners, value.owners),
                    Math.addExact(readSlots, value.readSlots),
                    Math.addExact(encodedBytes, value.encodedBytes),
                    Math.addExact(decodedBytes, value.decodedBytes));
        }

        private Usage minus(Usage value) {
            return new Usage(
                    Math.subtractExact(owners, value.owners),
                    Math.subtractExact(readSlots, value.readSlots),
                    Math.subtractExact(encodedBytes, value.encodedBytes),
                    Math.subtractExact(decodedBytes, value.decodedBytes));
        }

        private boolean within(Usage value) {
            return owners <= value.owners
                    && readSlots <= value.readSlots
                    && encodedBytes <= value.encodedBytes
                    && decodedBytes <= value.decodedBytes;
        }
    }

    public record ReadLimits(int maximumEntries, long maximumOwnedEntryBytes, long maximumOwnedBatchBytes) {
        public ReadLimits {
            if (maximumEntries < 1
                    || maximumEntries > 65_536
                    || maximumOwnedEntryBytes < 1
                    || maximumOwnedEntryBytes > Integer.MAX_VALUE
                    || maximumOwnedBatchBytes < maximumOwnedEntryBytes) {
                throw new IllegalArgumentException("Native read memory limits are invalid");
            }
        }
    }

    private static final Usage ZERO = new Usage(0, 0, 0, 0);
    private final MetadataNamespaceIdentityV2 namespace;
    private final ProtocolCellIdentity cell;
    private final Usage limit;
    private final Map<BindingIdentity, Usage> shares;
    private final Map<BindingIdentity, Usage> used = new HashMap<>();
    private final Map<BindingIdentity, Reservation> materializationSources = new HashMap<>();
    private final Map<UUID, Reservation> undeliveredSdk = new HashMap<>();
    private final Map<UUID, Reservation> undeliveredOwners = new HashMap<>();
    private Usage total = ZERO;
    private final ReadLimits readLimits;
    private final PulsarNativeReadCellAuthorityV1 authority;
    private boolean admitted;
    private boolean closing;
    private CompletableFuture<Void> closed;
    private CompletableFuture<Void> sealing;

    PulsarNativeReadCellBudgetV1(
            AsyncOxiaClient client,
            PulsarNativeBindingV1 actualBinding,
            Usage limit,
            Map<BindingIdentity, Usage> shares,
            ReadLimits readLimits) {
        this.readLimits = Objects.requireNonNull(readLimits, "read limits");
        namespace = actualBinding.metadataNamespace();
        cell = actualBinding.authority().aggregate().binding().cellIdentity();
        this.limit = Objects.requireNonNull(limit, "limit");
        this.shares = Map.copyOf(shares);
        if (this.shares.isEmpty()
                || this.shares.size() > 1024
                || limit.owners < 1
                || limit.owners > 65_536
                || limit.readSlots < 1
                || limit.readSlots > 65_536) {
            throw new IllegalArgumentException("Native Cell profile exceeds bounded owner/Binding capacity");
        }
        var reserved = ZERO;
        for (var entry : this.shares.entrySet()) {
            var share = entry.getValue();
            if (share.owners < 1 || share.readSlots < 1 || share.encodedBytes < 1 || share.decodedBytes < 1) {
                throw new IllegalArgumentException("Native Binding share must reserve positive capacity");
            }
            reserved = reserved.plus(share);
            used.put(entry.getKey(), ZERO);
        }
        if (!reserved.within(limit)) {
            throw new IllegalArgumentException("Native Binding shares exceed the Cell hard limit");
        }
        var out = ByteBuffer.allocate(60 + 128 * this.shares.size());
        out.putInt(0x4e504342).putInt(1);
        writeUsage(out, limit);
        out.putInt(readLimits.maximumEntries())
                .putLong(readLimits.maximumOwnedEntryBytes())
                .putLong(readLimits.maximumOwnedBatchBytes());
        for (var entry : this.shares.entrySet().stream()
                .sorted(Comparator.comparing((Map.Entry<BindingIdentity, Usage> value) ->
                                value.getKey().bindingId().digest().toHex())
                        .thenComparing(
                                value -> value.getKey().incarnationSha256().toHex())
                        .thenComparing(
                                value -> value.getKey().storageEpochSha256().toHex()))
                .toList()) {
            out.put(entry.getKey().bindingId().digest().bytes().toByteArray())
                    .put(entry.getKey().incarnationSha256().bytes().toByteArray())
                    .put(entry.getKey().storageEpochSha256().bytes().toByteArray());
            writeUsage(out, entry.getValue());
        }
        authority = new PulsarNativeReadCellAuthorityV1(client, actualBinding, CanonicalBytes.copyOf(out.array()));
    }

    private static void writeUsage(ByteBuffer out, Usage value) {
        out.putLong(value.owners())
                .putLong(value.readSlots())
                .putLong(value.encodedBytes())
                .putLong(value.decodedBytes());
    }

    /** Actual exact Cell owner/profile CAS; repeated attempts retain this object's same private owner identity. */
    public CompletableFuture<PulsarNativeReadCellBudgetV1> admit() {
        return authority
                .admit()
                .thenApply(ignored -> {
                    synchronized (this) {
                        admitted = true;
                        if (closing) {
                            sealIfDrained();
                            throw new IllegalStateException("Native Cell allocator is closing");
                        }
                    }
                    return this;
                })
                .copy();
    }

    public CompletableFuture<Usage> durableUsage() {
        return authority.durableUsage();
    }

    public String ownerKey() {
        return authority.headKey();
    }

    synchronized CanonicalBytes nativeOwnerIdentity(PulsarNativeBindingV1 actualBinding) {
        if (!admitted
                || closing
                || !namespace.equals(actualBinding.metadataNamespace())
                || !cell.equals(actualBinding.authority().aggregate().binding().cellIdentity())
                || !shares.containsKey(actualBinding.bindingIdentity())) {
            throw new IllegalStateException("Native create scope differs from its admitted Cell owner");
        }
        return authority.nativeOwnerIdentity();
    }

    CompletableFuture<Void> requireNativeOwner(PulsarNativeBindingV1 actualBinding, CanonicalBytes expected) {
        synchronized (this) {
            if (!admitted
                    || !namespace.equals(actualBinding.metadataNamespace())
                    || !cell.equals(
                            actualBinding.authority().aggregate().binding().cellIdentity())
                    || !shares.containsKey(actualBinding.bindingIdentity())) {
                throw new IllegalStateException("Existing Native SDK scope differs from the admitted Cell");
            }
        }
        return authority.requireNativeOwner(expected);
    }

    CompletableFuture<PulsarNativeReadCellAuthorityV1.OwnerEpoch> captureOwnerEpoch(
            PulsarNativeBindingV1 actualBinding) {
        nativeOwnerIdentity(actualBinding);
        return authority.captureOwnerEpoch();
    }

    synchronized Reservation reserveNativeClient(PulsarNativeBindingV1 actualBinding, int scopeBytes) {
        nativeOwnerIdentity(actualBinding);
        if (scopeBytes < 1 || scopeBytes > com.nereusstream.storage.bookkeeper.M5PulsarNativeCreateSpecV1.MAX_BYTES) {
            throw new IllegalArgumentException("Native SDK scope exceeds its byte limit");
        }
        long metadata = Math.multiplyExact(
                (long) com.nereusstream.storage.bookkeeper.M5PulsarNativeCreateSpecV1.MAX_PENDING_METADATA_MUTATIONS,
                com.nereusstream.storage.bookkeeper.M5PulsarNativeCreateSpecV1.MAX_METADATA_BYTES);
        long birthReads = Math.multiplyExact(
                (long) com.nereusstream.storage.bookkeeper.M5PulsarNativeCreateSpecV1.MAX_BIRTH_READS,
                com.nereusstream.storage.bookkeeper.M5PulsarNativeCreateSpecV1.BIRTH_READ_WORKSPACE_BYTES);
        long handles = Math.multiplyExact(
                (long) com.nereusstream.storage.bookkeeper.M5PulsarNativeCreateSpecV1.MAX_SDK_METADATA_HANDLES,
                com.nereusstream.storage.bookkeeper.M5PulsarNativeCreateSpecV1.SDK_METADATA_HANDLE_BYTES);
        handles = Math.addExact(
                handles,
                Math.addExact(
                        com.nereusstream.storage.bookkeeper.M5PulsarNativeCreateSpecV1.MAX_PENDING_SDK_DATA_BYTES,
                        (long) com.nereusstream.storage.bookkeeper.M5PulsarNativeCreateSpecV1.MAX_PENDING_SDK_DATA_RPCS
                                * com.nereusstream.storage.bookkeeper.M5PulsarNativeCreateSpecV1
                                        .SDK_DATA_RPC_DESCRIPTOR_BYTES));
        return reserveRequest(
                actualBinding,
                new Usage(
                        1,
                        0,
                        Math.addExact(
                                Math.addExact(
                                        Math.addExact(
                                                Math.multiplyExact(4L, scopeBytes), Math.multiplyExact(4, metadata)),
                                        Math.multiplyExact(4, birthReads)),
                                handles),
                        Math.addExact(
                                Math.addExact(
                                        Math.addExact(
                                                Math.multiplyExact(12L, scopeBytes), Math.multiplyExact(8, metadata)),
                                        Math.multiplyExact(8, birthReads)),
                                handles)));
    }

    boolean isDrainedOwnerValue(byte[] bytes) {
        return authority.isDrainedValue(CanonicalBytes.copyOf(bytes));
    }

    boolean hasDurableReservationValue(byte[] bytes) {
        return authority.hasReservationValue(CanonicalBytes.copyOf(bytes));
    }

    /** Stops new reservations and waits for actual owned source/actor drains before the durable terminal CAS. */
    public CompletableFuture<Void> closeCell() {
        final CompletableFuture<Void> observer;
        synchronized (this) {
            closing = true;
            if (closed == null) {
                closed = new CompletableFuture<>();
            }
            sealIfDrained();
            observer = closed.copy();
        }
        reconcileUndeliveredSdkCleanup();
        reconcileUndeliveredOwnerCleanup();
        return observer;
    }

    /** Retries only original SDK openings that were never handed to a caller. Does not close delivered owners. */
    public CompletableFuture<Void> reconcileUndeliveredSdkCleanup() {
        return reconcileUndelivered(undeliveredSdk);
    }

    /** Retries only original owner openings and grants that were never handed to a caller. */
    public CompletableFuture<Void> reconcileUndeliveredOwnerCleanup() {
        return reconcileUndelivered(undeliveredOwners);
    }

    private CompletableFuture<Void> reconcileUndelivered(Map<UUID, Reservation> retained) {
        final java.util.List<Reservation> originals;
        synchronized (this) {
            originals = java.util.List.copyOf(retained.values());
        }
        return CompletableFuture.allOf(originals.stream()
                        .map(Reservation::reconcileUndelivered)
                        .toArray(CompletableFuture[]::new))
                .copy();
    }

    private void sealIfDrained() {
        if (!closing
                || !total.equals(ZERO)
                || !undeliveredOwners.isEmpty()
                || !admitted
                || closed.isDone()
                || sealing != null && !sealing.isDone()) {
            return;
        }
        sealing = authority.closeAfterLocalDrain();
        sealing.whenComplete((ignored, failure) -> {
            if (failure == null) {
                closed.complete(null);
            }
            // UNKNOWN keeps closed pending. closeCell retries this same owner/generation terminal.
        });
    }

    public synchronized Usage usage() {
        return total;
    }

    public synchronized Map<BindingIdentity, Usage> bindingUsage() {
        return Map.copyOf(used);
    }

    synchronized Reservation reserve(PulsarNativeBindingV1 binding, PulsarNativeBindingReadViewV1 view, int capacity) {
        if (capacity < 1
                || capacity > 65_536
                || !namespace.equals(binding.metadataNamespace())
                || !cell.equals(binding.authority().aggregate().binding().cellIdentity())) {
            throw new IllegalArgumentException("Native read budget differs from actual Cell/namespace/capacity");
        }
        return reserveRequest(binding, view.readCharge(readLimits, capacity));
    }

    synchronized Reservation reservePreparation(PulsarNativeBindingV1 binding, int sources, int objects) {
        if (sources < 1
                || sources > 512
                || objects < 0
                || objects > sources
                || !namespace.equals(binding.metadataNamespace())
                || !cell.equals(binding.authority().aggregate().binding().cellIdentity())) {
            throw new IllegalArgumentException("Native source preparation differs from its bounded Cell cut");
        }
        long roots = Math.multiplyExact((long) objects, Npo1CodecV1.MAX_ROOT_BYTES);
        long metadata = Math.multiplyExact((long) sources, Npo1CodecV1.MAX_LEDGER_METADATA_BYTES);
        return reserveRequest(
                binding,
                new Usage(
                        1,
                        0,
                        Math.addExact(Math.multiplyExact(4, roots), Math.multiplyExact(2, metadata)),
                        Math.addExact(Math.multiplyExact(12, roots), Math.multiplyExact(4, metadata))));
    }

    private Reservation reserveRequest(PulsarNativeBindingV1 binding, Usage request) {
        var identity = binding.bindingIdentity();
        var share = shares.get(identity);
        if (share == null) {
            throw new IllegalStateException("Native Binding has no admitted Cell share");
        }
        var nextBinding = used.get(identity).plus(request);
        var nextTotal = total.plus(request);
        if (!nextBinding.within(share) || !nextTotal.within(limit)) {
            throw new IllegalStateException("Native Binding or Cell owner capacity exhausted");
        }
        if (!admitted || closing) {
            throw new IllegalStateException("Native Cell allocator is not admitted or is closing");
        }
        used.put(identity, nextBinding);
        total = nextTotal;
        return new Reservation(identity, request);
    }

    synchronized Reservation reserveMaterializationSource(PulsarNativeBindingV1 binding, int directoryBytes) {
        if (directoryBytes < 1
                || directoryBytes > 1_048_576
                || !namespace.equals(binding.metadataNamespace())
                || !cell.equals(binding.authority().aggregate().binding().cellIdentity())) {
            throw new IllegalArgumentException("Native materialization workspace differs from its Cell/source");
        }
        if (materializationSources.containsKey(binding.bindingIdentity())) {
            throw new IllegalStateException("Native Binding already owns its original materialization source");
        }
        var original = reserveRequest(
                binding,
                new Usage(
                        1,
                        0,
                        0,
                        Math.addExact(
                                Math.multiplyExact(4, readLimits.maximumOwnedEntryBytes()),
                                Math.addExact(Math.multiplyExact(8L, directoryBytes), 8L * 2 * 1024 * 1024))));
        original.materializationSource = true;
        materializationSources.put(binding.bindingIdentity(), original);
        return original;
    }

    synchronized Reservation reserveMaterializationOutput(PulsarNativeBindingV1 binding, long bytes) {
        if (bytes <= 0
                || !namespace.equals(binding.metadataNamespace())
                || !cell.equals(binding.authority().aggregate().binding().cellIdentity())) {
            throw new IllegalArgumentException("Native output workspace differs from its actual Cell");
        }
        return reserveRequest(binding, new Usage(0, 0, bytes, bytes)); // Same existing Source/Task and SDK loan.
    }

    synchronized Reservation reserveCursorObservation(PulsarNativeBindingV1 binding, int maximumBytes) {
        if (maximumBytes <= 0
                || maximumBytes > 8_388_608
                || !namespace.equals(binding.metadataNamespace())
                || !cell.equals(binding.authority().aggregate().binding().cellIdentity())) {
            throw new IllegalArgumentException("Native cursor workspace differs from its Cell/bounds");
        }
        return reserveRequest(
                binding, new Usage(1, 0, 0, Math.addExact(Math.multiplyExact(8L, maximumBytes), 16L * 1024 * 1024)));
    }

    ReadLimits readLimits() {
        return readLimits;
    }

    final class Reservation {
        private final BindingIdentity binding;
        private final Usage charge;
        private boolean actorStarted;
        private boolean released;
        private boolean materializationSource;
        private final UUID operation = UUID.randomUUID();
        private final CompletableFuture<Void> ready;
        private CompletableFuture<Void> releasing;
        private java.util.function.Supplier<CompletableFuture<Void>> undeliveredCleanup;
        private CompletableFuture<Void> cleanupAttempt;

        private Reservation(BindingIdentity binding, Usage charge) {
            this.binding = binding;
            this.charge = charge;
            ready = authority.reserve(operation, binding, charge);
        }

        CompletableFuture<Void> ready() {
            return ready.copy();
        }

        UUID operationId() {
            return operation;
        }

        boolean sameAllocator(Reservation other) {
            return other.allocator() == PulsarNativeReadCellBudgetV1.this;
        }

        private PulsarNativeReadCellBudgetV1 allocator() {
            return PulsarNativeReadCellBudgetV1.this;
        }

        CompletableFuture<PulsarNativeReadCellAuthorityV1.Reservations> captureReservations() {
            synchronized (PulsarNativeReadCellBudgetV1.this) {
                if (released || !actorStarted) {
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("Native root observation has no original active workspace"));
                }
            }
            return authority.captureReservations(operation, binding, charge);
        }

        void retainUndeliveredSdk(java.util.function.Supplier<CompletableFuture<Void>> cleanup, boolean start) {
            retainUndelivered(cleanup, start, undeliveredSdk);
        }

        void retainUndeliveredOwner(java.util.function.Supplier<CompletableFuture<Void>> cleanup, boolean start) {
            retainUndelivered(cleanup, start, undeliveredOwners);
        }

        private void retainUndelivered(
                java.util.function.Supplier<CompletableFuture<Void>> cleanup,
                boolean start,
                Map<UUID, Reservation> retained) {
            final boolean run;
            synchronized (PulsarNativeReadCellBudgetV1.this) {
                if (retained == undeliveredOwners && retained.containsKey(operation)) {
                    return; // The initial opening already retains this original reservation's handoff.
                }
                if (released && retained == undeliveredSdk) {
                    return;
                }
                if (undeliveredCleanup != null) {
                    throw new IllegalStateException("Original undelivered cleanup was already retained");
                }
                undeliveredCleanup = cleanup;
                retained.put(operation, this);
                run = start || closing;
            }
            if (run) {
                reconcileUndelivered();
            }
        }

        synchronized CompletableFuture<Void> reconcileUndelivered() {
            if (cleanupAttempt == null || cleanupAttempt.isCompletedExceptionally()) {
                try {
                    cleanupAttempt = undeliveredCleanup.get().whenComplete((ignored, failure) -> {
                        if (failure == null) {
                            confirmUndeliveredOwnerCleanup();
                        }
                    });
                } catch (RuntimeException failure) {
                    cleanupAttempt = CompletableFuture.failedFuture(failure);
                }
            }
            return cleanupAttempt.copy();
        }

        void confirmUndeliveredOwnerCleanup() {
            synchronized (PulsarNativeReadCellBudgetV1.this) {
                undeliveredOwners.remove(operation);
                sealIfDrained();
            }
        }

        void actorStarting() {
            synchronized (PulsarNativeReadCellBudgetV1.this) {
                if (released || !ready.isDone() || ready.isCompletedExceptionally() || ready.isCancelled()) {
                    throw new IllegalStateException("Native Cell reservation was already released");
                }
                actorStarted = true;
            }
        }

        void sourceStarting() {
            actorStarting();
        }

        CompletableFuture<Void> releaseBeforeActor() {
            synchronized (PulsarNativeReadCellBudgetV1.this) {
                return actorStarted ? CompletableFuture.completedFuture(null) : release();
            }
        }

        CompletableFuture<Void> releaseAfterDrain() {
            synchronized (PulsarNativeReadCellBudgetV1.this) {
                return release();
            }
        }

        private CompletableFuture<Void> release() {
            if (released) {
                return CompletableFuture.completedFuture(null);
            }
            if (releasing == null || releasing.isCompletedExceptionally()) {
                releasing = authority.release(operation, binding, charge).thenRun(() -> {
                    synchronized (PulsarNativeReadCellBudgetV1.this) {
                        if (!released) {
                            used.put(binding, used.get(binding).minus(charge));
                            total = total.minus(charge);
                            released = true;
                            if (materializationSource) {
                                materializationSources.remove(binding, this);
                            }
                            undeliveredSdk.remove(operation);
                            sealIfDrained();
                        }
                    }
                });
            }
            return releasing.copy();
        }
    }
}
