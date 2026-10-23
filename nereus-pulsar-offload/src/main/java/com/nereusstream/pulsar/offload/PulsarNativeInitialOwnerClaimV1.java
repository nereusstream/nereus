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
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** One original Native initial opening. A claim is not a selector grant, actor drain or expiry authority. */
final class PulsarNativeInitialOwnerClaimV1 {
    private static final int MAGIC = 0x4e504943;
    private static final int BYTES = 324;
    private final Oxia09ExactMetadataTransactionStoreV1 metadata;
    private final String key;
    private final CanonicalBytes owner;
    private final BindingIdentity binding;
    private final UUID operation;
    private final Sha256Digest view;
    private final Sha256Digest receipt;
    private Optional<VersionedValue> predecessor;
    private CanonicalBytes active;
    private CanonicalBytes aborted;
    private CanonicalBytes initialization;
    private CanonicalBytes admittedProfile;
    private boolean profileObserved;
    private boolean headObserved;
    private VersionedValue activeFact;

    PulsarNativeInitialOwnerClaimV1(
            Oxia09ExactMetadataTransactionStoreV1 metadata,
            String key,
            CanonicalBytes owner,
            BindingIdentity binding,
            UUID operation,
            Sha256Digest view,
            Sha256Digest receipt) {
        this.metadata = metadata;
        this.key = key;
        this.owner = owner;
        this.binding = binding;
        this.operation = operation;
        this.view = view;
        this.receipt = receipt;
    }

    /** Existing low-frequency control executor only. No competing ACTIVE invocation can enter selector create. */
    synchronized void acquire() {
        if (active != null) {
            throw new IllegalStateException("Original initial claim was already attempted");
        }
        predecessor = read();
        var profile = metadata.read(key + "/profile").toCompletableFuture().join();
        if (profile.isEmpty()) {
            if (predecessor.isPresent()) {
                throw new IllegalStateException("Native initial claim profile is missing");
            }
            initialization = encode(1, false);
            active = initialization;
            aborted = encode(1, true);
            var initialized = metadata.compareAndSet(Optional.empty(), key + "/profile", initialization)
                    .toCompletableFuture()
                    .join();
            profileObserved = initialized
                    == com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome
                            .APPLIED_EXACT;
            profile = metadata.read(key + "/profile").toCompletableFuture().join();
        }
        var profileBytes = profile.orElseThrow(
                        () -> new IllegalStateException("Native initial claim profile remains unconfirmed"))
                .canonicalStoredBytes();
        var first = decode(profileBytes);
        if (first.generation() != 1 || first.aborted()) {
            throw new IllegalStateException("Native initial claim initialization profile differs");
        }
        if (initialization != null && !initialization.equals(profileBytes)) {
            if (profileObserved) {
                throw new IllegalStateException("Previously observed initial claim profile changed");
            }
            active = null;
            aborted = null;
            initialization = null; // Its permanent foreign value fences our delayed profile create-if-absent.
        }
        admittedProfile = profileBytes;
        profileObserved = initialization != null && initialization.equals(profileBytes);
        predecessor = read();
        if (predecessor.isEmpty() && !profileObserved) {
            throw new IllegalStateException("Native initial claim head is missing or initialization belongs elsewhere");
        }
        long generation = 1;
        if (predecessor.isPresent()) {
            var previous = decode(predecessor.orElseThrow().canonicalStoredBytes());
            if (!previous.aborted()) {
                throw new IllegalStateException("Native initial owner claim belongs to another invocation");
            }
            generation = Math.addExact(previous.generation(), 1);
        }
        active = encode(generation, false);
        aborted = encode(generation, true);
        var outcome = metadata.compareAndSet(predecessor, key, active)
                .toCompletableFuture()
                .join();
        headObserved = outcome
                == com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome
                        .APPLIED_EXACT;
        requireActive();
    }

    synchronized void requireActive() {
        requireProfile();
        var actual = read();
        if (active == null
                || actual.filter(value -> value.canonicalStoredBytes().equals(active))
                        .isEmpty()
                || activeFact != null && actual.filter(activeFact::equals).isEmpty()) {
            throw new IllegalStateException("Original Native initial claim is unconfirmed or changed");
        }
        headObserved = true;
        activeFact = actual.orElseThrow();
    }

    synchronized VersionedValue activeIdentity() {
        requireActive();
        return activeFact;
    }

    /** Observes a stored opening identity for an independently prepared cold reader; performs no mutation. */
    static VersionedValue readActiveIdentity(
            Oxia09ExactMetadataTransactionStoreV1 metadata,
            String key,
            CanonicalBytes currentCellOwner,
            BindingIdentity binding,
            Sha256Digest expectedView,
            Sha256Digest expectedReceipt) {
        var profile =
                metadata.read(key + "/profile").toCompletableFuture().join().orElseThrow();
        var head = metadata.read(key).toCompletableFuture().join().orElseThrow();
        var first = decode(profile.canonicalStoredBytes(), currentCellOwner, binding);
        var actual = decode(head.canonicalStoredBytes(), currentCellOwner, binding);
        if (first.generation() != 1
                || first.aborted()
                || actual.aborted()
                || !actual.view().equals(expectedView)
                || !actual.receipt().equals(expectedReceipt)
                || actual.generation() == 1 && !profile.canonicalStoredBytes().equals(head.canonicalStoredBytes())) {
            throw new IllegalStateException("Stored initial claim differs from its original grant identity");
        }
        return head;
    }

    /** Called only after the private opening failed before entering selector create. Never aborts a grant. */
    CompletableFuture<Void> abortBeforeGrant(Executor executor) {
        return CompletableFuture.runAsync(this::abort, executor);
    }

    private synchronized void abort() {
        if (active == null) {
            return; // No claim candidate was dispatched, including rejection of an existing foreign ACTIVE claim.
        }
        if (initialization != null && !confirmInitialization()) {
            return; // Another permanent initializer prevents this original delayed create-if-absent.
        }
        requireProfile();
        var current = read();
        if (isAbortOrFenced(current)) {
            return;
        }
        if (current.isEmpty() && (headObserved || predecessor.isPresent())) {
            throw new IllegalStateException("Previously observed initial claim is missing");
        }
        metadata.compareAndSet(current, key, aborted).toCompletableFuture().join();
        if (!isAbortOrFenced(read())) {
            throw new IllegalStateException("Original Native initial claim abort remains unconfirmed");
        }
    }

    private boolean confirmInitialization() {
        var profile = metadata.read(key + "/profile").toCompletableFuture().join();
        if (profile.isEmpty()) {
            if (profileObserved) {
                throw new IllegalStateException("Previously observed initial claim profile is missing");
            }
            metadata.compareAndSet(Optional.empty(), key + "/profile", initialization)
                    .toCompletableFuture()
                    .join();
            profile = metadata.read(key + "/profile").toCompletableFuture().join();
        }
        var value = profile.orElseThrow(
                        () -> new IllegalStateException("Original claim initialization remains unconfirmed"))
                .canonicalStoredBytes();
        var first = decode(value);
        if (first.generation() != 1 || first.aborted()) {
            throw new IllegalStateException("Native initial claim initialization profile differs");
        }
        if (profileObserved && !value.equals(initialization)) {
            throw new IllegalStateException("Previously observed initial claim profile changed");
        }
        profileObserved = value.equals(initialization);
        admittedProfile = value;
        return profileObserved;
    }

    private void requireProfile() {
        if (admittedProfile == null
                || metadata.read(key + "/profile")
                        .toCompletableFuture()
                        .join()
                        .filter(value -> value.canonicalStoredBytes().equals(admittedProfile))
                        .isEmpty()) {
            throw new IllegalStateException("Native initial claim profile is missing or changed");
        }
    }

    private boolean isAbortOrFenced(Optional<VersionedValue> current) {
        if (current.isEmpty()) {
            return false;
        }
        var bytes = current.orElseThrow().canonicalStoredBytes();
        decode(bytes); // Unknown/corrupt/foreign Binding values cannot establish a conditional-write fence.
        if (bytes.equals(aborted)) {
            return true;
        }
        // An exact old predecessor can still accept this original delayed ACTIVE CAS. It needs our abort CAS.
        // A valid different permanent value/version prevents both an old exact CAS and create-if-absent.
        return !bytes.equals(active) && !current.equals(predecessor);
    }

    private Optional<VersionedValue> read() {
        return metadata.read(key).toCompletableFuture().join();
    }

    private CanonicalBytes encode(long generation, boolean terminal) {
        var out = ByteBuffer.allocate(BYTES);
        out.putInt(MAGIC).putInt(1).putLong(generation).putInt(terminal ? 2 : 1);
        out.putLong(operation.getMostSignificantBits()).putLong(operation.getLeastSignificantBits());
        out.put(owner.toByteArray());
        out.put(binding.bindingId().digest().bytes().toByteArray());
        out.put(binding.incarnationSha256().bytes().toByteArray());
        out.put(binding.storageEpochSha256().bytes().toByteArray());
        out.put(view.bytes().toByteArray()).put(receipt.bytes().toByteArray());
        if (out.position() != BYTES - 32) {
            throw new IllegalStateException("Initial claim identity differs from its fixed wire profile");
        }
        out.put(Sha256Digest.hash(CanonicalBytes.copyOf(java.util.Arrays.copyOf(out.array(), BYTES - 32)))
                .bytes()
                .toByteArray());
        return CanonicalBytes.copyOf(out.array());
    }

    private record Head(long generation, boolean aborted, Sha256Digest view, Sha256Digest receipt) {}

    private Head decode(CanonicalBytes bytes) {
        return decode(bytes, owner, binding);
    }

    private static Head decode(CanonicalBytes bytes, CanonicalBytes owner, BindingIdentity binding) {
        if (bytes.length() != BYTES) {
            throw new IllegalStateException("Native initial claim length differs");
        }
        byte[] value = bytes.toByteArray();
        byte[] body = java.util.Arrays.copyOf(value, BYTES - 32);
        if (!java.util.Arrays.equals(
                Sha256Digest.hash(CanonicalBytes.copyOf(body)).bytes().toByteArray(),
                java.util.Arrays.copyOfRange(value, BYTES - 32, BYTES))) {
            throw new IllegalStateException("Native initial claim checksum differs");
        }
        var in = ByteBuffer.wrap(body);
        int magic = in.getInt();
        int version = in.getInt();
        long generation = in.getLong();
        int state = in.getInt();
        var original = new UUID(in.getLong(), in.getLong());
        byte[] actualOwner = new byte[96];
        in.get(actualOwner);
        var nativeOwner = ByteBuffer.wrap(actualOwner);
        int ownerMagic = nativeOwner.getInt();
        int ownerVersion = nativeOwner.getInt();
        nativeOwner.position(72);
        long ownerGeneration = nativeOwner.getLong();
        var ownerId = new UUID(nativeOwner.getLong(), nativeOwner.getLong());
        var identities = ByteBuffer.allocate(96)
                .put(binding.bindingId().digest().bytes().toByteArray())
                .put(binding.incarnationSha256().bytes().toByteArray())
                .put(binding.storageEpochSha256().bytes().toByteArray())
                .array();
        byte[] actualBinding = new byte[96];
        in.get(actualBinding);
        byte[] actualView = new byte[32];
        byte[] actualReceipt = new byte[32];
        in.get(actualView).get(actualReceipt);
        if (magic != MAGIC
                || version != 1
                || generation <= 0
                || state != 1 && state != 2
                || original.equals(new UUID(0, 0))
                || ownerMagic != 0x4e50434f
                || ownerVersion != 1
                || ownerGeneration <= 0
                || ownerId.equals(new UUID(0, 0))
                || !java.util.Arrays.equals(
                        java.util.Arrays.copyOf(actualOwner, 72), java.util.Arrays.copyOf(owner.toByteArray(), 72))
                || !java.util.Arrays.equals(identities, actualBinding)
                || java.util.Arrays.equals(actualView, new byte[32])
                || java.util.Arrays.equals(actualReceipt, new byte[32])
                || in.hasRemaining()) {
            throw new IllegalStateException("Native initial claim identity/profile differs");
        }
        return new Head(generation, state == 2, Sha256Digest.copyOf(actualView), Sha256Digest.copyOf(actualReceipt));
    }
}
