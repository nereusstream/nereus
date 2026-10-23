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

package com.nereusstream.storage.object.gc;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.retention.M5MaterializationWorkerV1;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/** Fixed-size, permanent quota accounting. Charges canonical native keys/values, not backend WAL or disk bytes. */
public final class M5GcQuotaRecordsV2 {
    public static final String HEAD_KEY = "v2/physical-delete-quota-v2/head";
    public static final String ENTRY_PREFIX = "v2/physical-delete-quota-v2/resources/";
    public static final int ENTRY_BYTES = 156;
    public static final int HEAD_BYTES = 272;
    public static final int BIRTH_HEAD_BYTES = HEAD_BYTES + 28 + M5ObjectBirthCreditV3.MAX_BYTES;
    public static final String PHYSICAL_MEMBER_PREFIX = "v2/physical-members-m5-v2/";
    public static final int PHYSICAL_MEMBER_MAX_VALUE_BYTES = 8_554;
    private static final int ENTRY_MAGIC = 0x4d354751; // M5GQ
    private static final int HEAD_MAGIC = 0x4d354748; // M5GH
    private static final Sha256Digest ZERO = Sha256Digest.copyOf(new byte[32]);

    private M5GcQuotaRecordsV2() {}

    /** The composition owner must assign this same native root to every user of the physical namespace. */
    public enum RecordProfile {
        GC_ONLY_V2,
        GC_AND_PHYSICAL_MEMBERS_V2
    }

    public record Layout(String nativeRoot, PhysicalResourceIdV2.Namespace namespace, RecordProfile profile) {
        public Layout(String nativeRoot, PhysicalResourceIdV2.Namespace namespace) {
            this(nativeRoot, namespace, RecordProfile.GC_ONLY_V2);
        }

        public static Layout withPhysicalMembers(String nativeRoot, PhysicalResourceIdV2.Namespace namespace) {
            return new Layout(nativeRoot, namespace, RecordProfile.GC_AND_PHYSICAL_MEMBERS_V2);
        }

        public Layout {
            Objects.requireNonNull(nativeRoot, "nativeRoot");
            Objects.requireNonNull(namespace, "namespace");
            Objects.requireNonNull(profile, "profile");
            if (!nativeRoot.matches("/[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*")
                    || nativeRoot.length() + 1 + authorityKey(ZERO).length() > 512
                    || (profile == RecordProfile.GC_AND_PHYSICAL_MEMBERS_V2
                            && nativeRoot.length() + 1 + physicalMemberKey(ZERO).length() > 512)) {
                throw new IllegalArgumentException("quota root is noncanonical or exceeds native key bound");
            }
        }

        public Sha256Digest scope() {
            var root = CanonicalUtf8.fromString(nativeRoot).bytes();
            var service = namespace.serviceIdentity().bytes();
            var container = namespace.containerIdentity().bytes();
            var identity = ByteBuffer.allocate(16 + root.length() + service.length() + container.length())
                    .putInt(namespace.providerKind().ordinal())
                    .putInt(root.length())
                    .put(root.toByteArray())
                    .putInt(service.length())
                    .put(service.toByteArray())
                    .putInt(container.length())
                    .put(container.toByteArray())
                    .array();
            if (profile == RecordProfile.GC_ONLY_V2) {
                return Sha256Digest.hash(CanonicalBytes.copyOf(identity));
            }
            var memberKey = CanonicalUtf8.fromString(physicalMemberKey(ZERO)).bytes();
            return Sha256Digest.hash(
                    CanonicalBytes.copyOf(ByteBuffer.allocate(12 + memberKey.length() + identity.length)
                            .putInt(0x4d354d51)
                            .putInt(PHYSICAL_MEMBER_MAX_VALUE_BYTES)
                            .putInt(memberKey.length())
                            .put(memberKey.toByteArray())
                            .put(identity)
                            .array()));
        }

        public long headCharge() {
            return nativeKey(HEAD_KEY).length() + HEAD_BYTES;
        }

        public long entryCharge() {
            return nativeKey(entryKey(ZERO)).length() + ENTRY_BYTES;
        }

        public long authorityKeyCharge() {
            return nativeKey(authorityKey(ZERO)).length();
        }

        public long reservationCharge() {
            return entryCharge()
                    + authorityKeyCharge()
                    + ExactMetadataTransactionStoreV1.MAX_VALUE_BYTES
                    + permanentMemberCharge();
        }

        public long birthRecordCharge() {
            return nativeKey(M5ObjectBirthCreditV3.PREFIX + ZERO.toHex()).length() + M5ObjectBirthCreditV3.MAX_BYTES;
        }

        /** Append-only member history retains its bounded native key/value reservation after physical DONE. */
        public long permanentMemberCharge() {
            return profile == RecordProfile.GC_ONLY_V2
                    ? 0
                    : nativeKey(physicalMemberKey(ZERO)).length() + PHYSICAL_MEMBER_MAX_VALUE_BYTES;
        }

        public String nativeKey(String key) {
            if (!key.equals(HEAD_KEY)
                    && !key.matches(ENTRY_PREFIX + "[0-9a-f]{64}")
                    && !key.matches(M5ObjectBirthCreditV3.PREFIX + "[0-9a-f]{64}")
                    && !key.matches("v2/physical-delete-m5-v2/[0-9a-f]{64}/authority-v2")
                    && !(profile == RecordProfile.GC_AND_PHYSICAL_MEMBERS_V2
                            && key.matches(PHYSICAL_MEMBER_PREFIX + "[0-9a-f]{64}/inventory-v2"))) {
                throw new IllegalArgumentException("key is outside the quota and authority families");
            }
            return nativeRoot + "/" + key;
        }

        public void requireResource(PhysicalResourceIdV2 resource) {
            if (!Objects.requireNonNull(resource, "resource").namespace().equals(namespace)) {
                throw new IllegalArgumentException("quota physical namespace differs");
            }
        }

        public void verify(Head head) {
            if (!head.scope().equals(scope()) || head.usedBytes(this) > head.capacityBytes()) {
                throw new IllegalArgumentException("quota head scope or accounted capacity differs");
            }
            if (head.births().isPresent()
                    && namespace.providerKind() != PhysicalResourceIdV2.ProviderKind.OBJECT_PROVIDER) {
                throw new IllegalArgumentException("Object birth quota cannot be used in a BookKeeper namespace");
            }
            head.births()
                    .flatMap(Births::pending)
                    .flatMap(M5ObjectBirthCreditV3::resource)
                    .ifPresent(this::requireResource);
        }

        public void verify(Entry entry) {
            if (!entry.scope().equals(scope())) {
                throw new IllegalArgumentException("quota entry scope differs");
            }
        }
    }

    /** Issued credits retain bounded permanent metadata; only unsettled credits retain future GC/member capacity. */
    public record Births(long issued, long assigned, long cancelled, Optional<M5ObjectBirthCreditV3> pending) {
        public Births {
            Objects.requireNonNull(pending, "pending");
            if (issued < 1 || assigned < 0 || cancelled < 0 || Math.addExact(assigned, cancelled) > issued) {
                throw new IllegalArgumentException("invalid Object birth credit counters");
            }
        }

        public long unassigned() {
            return Math.subtractExact(issued, Math.addExact(assigned, cancelled));
        }

        public Births clearPending() {
            if (pending.isEmpty()) {
                throw new IllegalStateException("birth quota has no pending record");
            }
            return new Births(issued, assigned, cancelled, Optional.empty());
        }
    }

    /** A grant is never removed. Settlement replaces it at the same key and retains the original grant revision. */
    public record Entry(
            Sha256Digest scope,
            Sha256Digest resource,
            long grantRevision,
            Optional<Sha256Digest> compactDoneSha256,
            long compactDoneValueBytes) {
        public Entry {
            Objects.requireNonNull(scope, "scope");
            Objects.requireNonNull(resource, "resource");
            Objects.requireNonNull(compactDoneSha256, "compactDoneSha256");
            if (grantRevision < 2
                    || compactDoneValueBytes < 0
                    || compactDoneValueBytes > M5TargetDeleteDoneV2.MAX_BYTES
                    || (compactDoneSha256.isPresent() != (compactDoneValueBytes > 0))) {
                throw new IllegalArgumentException("invalid permanent quota entry");
            }
        }

        public boolean settled() {
            return compactDoneSha256.isPresent();
        }

        public String key() {
            return entryKey(resource);
        }

        public Entry grant() {
            return new Entry(scope, resource, grantRevision, Optional.empty(), 0);
        }

        public Entry settle(M5TargetDeleteDoneV2 done) {
            if (settled() || !done.resource().sha256().equals(resource)) {
                throw new IllegalArgumentException("settlement requires the granted physical resource");
            }
            return new Entry(
                    scope,
                    resource,
                    grantRevision,
                    Optional.of(Sha256Digest.hash(done.encode())),
                    done.encode().length());
        }

        public CanonicalBytes encode() {
            var out = header(ENTRY_BYTES, ENTRY_MAGIC)
                    .put(scope.bytes().toByteArray())
                    .put(resource.bytes().toByteArray())
                    .putLong(grantRevision)
                    .putInt(settled() ? 1 : 0)
                    .put(compactDoneSha256.orElse(ZERO).bytes().toByteArray())
                    .putLong(compactDoneValueBytes);
            return checksum(out);
        }

        public static Entry decode(CanonicalBytes bytes) {
            var in = checked(bytes, ENTRY_BYTES, ENTRY_MAGIC);
            var scope = digest(in);
            var resource = digest(in);
            long revision = in.getLong();
            int state = in.getInt();
            var done = digest(in);
            long size = in.getLong();
            if ((state != 0 && state != 1) || (state == 0 && !done.equals(ZERO))) {
                throw new IllegalArgumentException("noncanonical quota entry state");
            }
            var entry = new Entry(scope, resource, revision, state == 0 ? Optional.empty() : Optional.of(done), size);
            if (!entry.encode().equals(bytes)) {
                throw new IllegalArgumentException("quota entry canonical bytes differ");
            }
            return entry;
        }
    }

    /** One inline operation bounds resident work; counts include its already committed reservation or refund. */
    public record Head(
            Sha256Digest scope,
            long revision,
            long capacityBytes,
            long reservedResources,
            long settledResources,
            long compactDoneValueBytes,
            Optional<Entry> pending,
            Optional<Births> births) {
        public Head(
                Sha256Digest scope,
                long revision,
                long capacityBytes,
                long reservedResources,
                long settledResources,
                long compactDoneValueBytes,
                Optional<Entry> pending) {
            this(
                    scope,
                    revision,
                    capacityBytes,
                    reservedResources,
                    settledResources,
                    compactDoneValueBytes,
                    pending,
                    Optional.empty());
        }

        public Head {
            Objects.requireNonNull(scope, "scope");
            Objects.requireNonNull(pending, "pending");
            Objects.requireNonNull(births, "births");
            if (revision < 1
                    || capacityBytes < 1
                    || reservedResources < 0
                    || settledResources < 0
                    || compactDoneValueBytes < settledResources
                    || compactDoneValueBytes > Math.multiplyExact(settledResources, M5TargetDeleteDoneV2.MAX_BYTES)) {
                throw new IllegalArgumentException("invalid durable quota counters");
            }
            pending.ifPresent(entry -> {
                if (!entry.scope().equals(scope)
                        || entry.grantRevision() > revision
                        || (entry.settled() ? settledResources == 0 : reservedResources == 0)) {
                    throw new IllegalArgumentException("pending quota operation differs from its head");
                }
            });
            births.flatMap(Births::pending).ifPresent(credit -> {
                if (!credit.scope().equals(scope)
                        || credit.grantRevision() > revision
                        || ((credit.state() == M5ObjectBirthCreditV3.State.RESERVED
                                        || credit.state() == M5ObjectBirthCreditV3.State.WRITE_ARMED)
                                && births.orElseThrow().unassigned() == 0)
                        || (credit.state() == M5ObjectBirthCreditV3.State.ASSIGNED
                                && births.orElseThrow().assigned() == 0)
                        || (credit.state() == M5ObjectBirthCreditV3.State.CANCELLED_NO_DISPATCH
                                && births.orElseThrow().cancelled() == 0)) {
                    throw new IllegalArgumentException("pending birth credit differs from its head");
                }
                if (pending.isPresent()
                        && (credit.state() != M5ObjectBirthCreditV3.State.ASSIGNED
                                || pending.orElseThrow().settled()
                                || !pending.orElseThrow()
                                        .resource()
                                        .equals(credit.resource().orElseThrow().sha256()))) {
                    throw new IllegalArgumentException("compound birth assignment has another resource reservation");
                }
            });
        }

        public static Head empty(Layout layout, long capacity) {
            var head = new Head(layout.scope(), 1, capacity, 0, 0, 0, Optional.empty());
            layout.verify(head);
            return head;
        }

        public long usedBytes(Layout layout) {
            long active = Math.multiplyExact(reservedResources, layout.reservationCharge());
            long terminal = Math.addExact(
                    Math.multiplyExact(
                            settledResources,
                            layout.entryCharge() + layout.authorityKeyCharge() + layout.permanentMemberCharge()),
                    compactDoneValueBytes);
            long birthBytes = births.map(value -> Math.addExact(
                            BIRTH_HEAD_BYTES - HEAD_BYTES,
                            Math.addExact(
                                    Math.multiplyExact(value.issued(), layout.birthRecordCharge()),
                                    Math.multiplyExact(value.unassigned(), layout.reservationCharge()))))
                    .orElse(0L);
            return Math.addExact(layout.headCharge(), Math.addExact(birthBytes, Math.addExact(active, terminal)));
        }

        public long availableBytes(Layout layout) {
            return Math.subtractExact(capacityBytes, usedBytes(layout));
        }

        public Head reserve(Layout layout, Sha256Digest resource) {
            requireIdle();
            if (availableBytes(layout) < layout.reservationCharge()) {
                throw new IllegalStateException("durable GC quota exhausted");
            }
            long next = Math.addExact(revision, 1);
            return new Head(
                    scope,
                    next,
                    capacityBytes,
                    Math.addExact(reservedResources, 1),
                    settledResources,
                    compactDoneValueBytes,
                    Optional.of(new Entry(scope, resource, next, Optional.empty(), 0)),
                    births);
        }

        public Head settle(Entry entry) {
            requireIdle();
            if (!entry.settled() || reservedResources == 0) {
                throw new IllegalArgumentException("quota refund requires a reserved permanent done");
            }
            return new Head(
                    scope,
                    Math.addExact(revision, 1),
                    capacityBytes,
                    reservedResources - 1,
                    Math.addExact(settledResources, 1),
                    Math.addExact(compactDoneValueBytes, entry.compactDoneValueBytes()),
                    Optional.of(entry),
                    births);
        }

        public Head clearPending() {
            if (pending.isEmpty()) {
                throw new IllegalStateException("quota has no pending operation");
            }
            return new Head(
                    scope,
                    Math.addExact(revision, 1),
                    capacityBytes,
                    reservedResources,
                    settledResources,
                    compactDoneValueBytes,
                    Optional.empty(),
                    births);
        }

        public Head expand(long capacity) {
            if (capacity <= capacityBytes) {
                throw new IllegalArgumentException("quota expansion must be strictly monotonic");
            }
            return new Head(
                    scope,
                    Math.addExact(revision, 1),
                    capacity,
                    reservedResources,
                    settledResources,
                    compactDoneValueBytes,
                    pending,
                    births);
        }

        public Head reserveBirth(
                Layout layout,
                M5MaterializationWorkerV1 worker,
                com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2.Slot slot) {
            requireIdle();
            long next = Math.addExact(revision, 1);
            var credit = new M5ObjectBirthCreditV3(
                    scope, worker, slot, next, M5ObjectBirthCreditV3.State.RESERVED, Optional.empty());
            long issued = births.map(value -> Math.addExact(value.issued(), 1)).orElse(1L);
            var candidate = new Head(
                    scope,
                    next,
                    capacityBytes,
                    reservedResources,
                    settledResources,
                    compactDoneValueBytes,
                    Optional.empty(),
                    Optional.of(new Births(
                            issued,
                            births.map(Births::assigned).orElse(0L),
                            births.map(Births::cancelled).orElse(0L),
                            Optional.of(credit))));
            layout.verify(candidate); // Includes credit history, future resource/member records and larger head.
            return candidate;
        }

        public Head finishBirth(M5ObjectBirthCreditV3 credit, boolean newResource) {
            requireIdle();
            var before = births.orElseThrow(() -> new IllegalStateException("birth quota is not enabled"));
            if (!credit.scope().equals(scope)
                    || before.unassigned() == 0
                    || credit.state() == M5ObjectBirthCreditV3.State.RESERVED
                    || credit.state() == M5ObjectBirthCreditV3.State.WRITE_ARMED
                    || (newResource && credit.state() != M5ObjectBirthCreditV3.State.ASSIGNED)) {
                throw new IllegalArgumentException("birth settlement differs from its original reserved credit");
            }
            long next = Math.addExact(revision, 1);
            var after = new Births(
                    before.issued(),
                    Math.addExact(before.assigned(), credit.state() == M5ObjectBirthCreditV3.State.ASSIGNED ? 1 : 0),
                    Math.addExact(
                            before.cancelled(),
                            credit.state() == M5ObjectBirthCreditV3.State.CANCELLED_NO_DISPATCH ? 1 : 0),
                    Optional.of(credit));
            return new Head(
                    scope,
                    next,
                    capacityBytes,
                    Math.addExact(reservedResources, newResource ? 1 : 0),
                    settledResources,
                    compactDoneValueBytes,
                    newResource
                            ? Optional.of(new Entry(
                                    scope, credit.resource().orElseThrow().sha256(), next, Optional.empty(), 0))
                            : Optional.empty(),
                    Optional.of(after));
        }

        public Head clearPendingBirth() {
            if (pending.isPresent()) {
                throw new IllegalStateException("birth assignment must first confirm its resource reservation");
            }
            return new Head(
                    scope,
                    Math.addExact(revision, 1),
                    capacityBytes,
                    reservedResources,
                    settledResources,
                    compactDoneValueBytes,
                    pending,
                    Optional.of(births.orElseThrow().clearPending()));
        }

        public Head armBirth(M5ObjectBirthCreditV3 credit) {
            requireIdle();
            var before = births.orElseThrow(() -> new IllegalStateException("birth quota is not enabled"));
            if (!credit.scope().equals(scope)
                    || before.unassigned() == 0
                    || credit.state() != M5ObjectBirthCreditV3.State.WRITE_ARMED) {
                throw new IllegalArgumentException("write intent requires its original reserved birth credit");
            }
            return new Head(
                    scope,
                    Math.addExact(revision, 1),
                    capacityBytes,
                    reservedResources,
                    settledResources,
                    compactDoneValueBytes,
                    Optional.empty(),
                    Optional.of(
                            new Births(before.issued(), before.assigned(), before.cancelled(), Optional.of(credit))));
        }

        public CanonicalBytes encode() {
            var out = ByteBuffer.allocate(births.isPresent() ? BIRTH_HEAD_BYTES : HEAD_BYTES)
                    .putInt(HEAD_MAGIC)
                    .putInt(births.isPresent() ? 3 : 2)
                    .put(scope.bytes().toByteArray())
                    .putLong(revision)
                    .putLong(capacityBytes)
                    .putLong(reservedResources)
                    .putLong(settledResources)
                    .putLong(compactDoneValueBytes)
                    .putInt(pending.isPresent() ? 1 : 0)
                    .put(pending.map(entry -> entry.encode().toByteArray()).orElseGet(() -> new byte[ENTRY_BYTES]));
            if (births.isPresent()) {
                var value = births.orElseThrow();
                var credit = value.pending().map(M5ObjectBirthCreditV3::encode).orElse(CanonicalBytes.empty());
                out.putLong(value.issued())
                        .putLong(value.assigned())
                        .putLong(value.cancelled())
                        .putInt(credit.length())
                        .put(credit.toByteArray())
                        .put(new byte[M5ObjectBirthCreditV3.MAX_BYTES - credit.length()]);
            }
            return checksum(out);
        }

        public static Head decode(CanonicalBytes bytes) {
            boolean extended = bytes.length() == BIRTH_HEAD_BYTES;
            var in = checked(bytes, extended ? BIRTH_HEAD_BYTES : HEAD_BYTES, HEAD_MAGIC, extended ? 3 : 2);
            var scope = digest(in);
            long revision = in.getLong();
            long capacity = in.getLong();
            long reserved = in.getLong();
            long settled = in.getLong();
            long doneBytes = in.getLong();
            int flag = in.getInt();
            var raw = new byte[ENTRY_BYTES];
            in.get(raw);
            if ((flag != 0 && flag != 1) || (flag == 0 && !Arrays.equals(raw, new byte[ENTRY_BYTES]))) {
                throw new IllegalArgumentException("noncanonical quota pending operation");
            }
            Optional<Births> births = Optional.empty();
            if (extended) {
                long issued = in.getLong();
                long assigned = in.getLong();
                long cancelled = in.getLong();
                int size = in.getInt();
                var credit = new byte[M5ObjectBirthCreditV3.MAX_BYTES];
                in.get(credit);
                if (size < 0
                        || size > credit.length
                        || !Arrays.equals(
                                Arrays.copyOfRange(credit, size, credit.length), new byte[credit.length - size])) {
                    throw new IllegalArgumentException("noncanonical birth quota pending bytes");
                }
                births = Optional.of(new Births(
                        issued,
                        assigned,
                        cancelled,
                        size == 0
                                ? Optional.empty()
                                : Optional.of(M5ObjectBirthCreditV3.decode(
                                        CanonicalBytes.copyOf(Arrays.copyOf(credit, size))))));
            }
            var head = new Head(
                    scope,
                    revision,
                    capacity,
                    reserved,
                    settled,
                    doneBytes,
                    flag == 0 ? Optional.empty() : Optional.of(Entry.decode(CanonicalBytes.copyOf(raw))),
                    births);
            if (!head.encode().equals(bytes)) {
                throw new IllegalArgumentException("quota head canonical bytes differ");
            }
            return head;
        }

        private void requireIdle() {
            if (pending.isPresent() || births.flatMap(Births::pending).isPresent()) {
                throw new IllegalStateException("quota operation must first reconcile the exact pending record");
            }
        }
    }

    public static String entryKey(Sha256Digest resource) {
        return ENTRY_PREFIX + resource.toHex();
    }

    public static String authorityKey(Sha256Digest resource) {
        return "v2/physical-delete-m5-v2/" + resource.toHex() + "/authority-v2";
    }

    public static String physicalMemberKey(Sha256Digest resource) {
        return PHYSICAL_MEMBER_PREFIX + resource.toHex() + "/inventory-v2";
    }

    private static ByteBuffer header(int size, int magic) {
        return ByteBuffer.allocate(size).putInt(magic).putInt(2);
    }

    private static CanonicalBytes checksum(ByteBuffer out) {
        if (out.remaining() != 32) {
            throw new IllegalStateException("quota canonical length differs");
        }
        out.put(Sha256Digest.hash(CanonicalBytes.copyOf(Arrays.copyOf(out.array(), out.position())))
                .bytes()
                .toByteArray());
        return CanonicalBytes.copyOf(out.array());
    }

    private static ByteBuffer checked(CanonicalBytes bytes, int size, int magic) {
        return checked(bytes, size, magic, 2);
    }

    private static ByteBuffer checked(CanonicalBytes bytes, int size, int magic, int version) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length() != size) {
            throw new IllegalArgumentException("quota encoded length differs");
        }
        var raw = bytes.toByteArray();
        var in = ByteBuffer.wrap(raw);
        if (in.getInt() != magic
                || in.getInt() != version
                || !Sha256Digest.hash(CanonicalBytes.copyOf(Arrays.copyOf(raw, size - 32)))
                        .equals(Sha256Digest.copyOf(Arrays.copyOfRange(raw, size - 32, size)))) {
            throw new IllegalArgumentException("quota wire version or checksum differs");
        }
        return in;
    }

    private static Sha256Digest digest(ByteBuffer in) {
        var bytes = new byte[32];
        in.get(bytes);
        return Sha256Digest.copyOf(bytes);
    }
}
