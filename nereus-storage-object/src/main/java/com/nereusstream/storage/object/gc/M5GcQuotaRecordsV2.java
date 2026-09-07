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
    private static final int ENTRY_MAGIC = 0x4d354751; // M5GQ
    private static final int HEAD_MAGIC = 0x4d354748; // M5GH
    private static final Sha256Digest ZERO = Sha256Digest.copyOf(new byte[32]);

    private M5GcQuotaRecordsV2() {}

    /** The composition owner must assign this same native root to every user of the physical namespace. */
    public record Layout(String nativeRoot, PhysicalResourceIdV2.Namespace namespace) {
        public Layout {
            Objects.requireNonNull(nativeRoot, "nativeRoot");
            Objects.requireNonNull(namespace, "namespace");
            if (!nativeRoot.matches("/[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*")
                    || nativeRoot.length() + 1 + authorityKey(ZERO).length() > 512) {
                throw new IllegalArgumentException("quota root is noncanonical or exceeds native key bound");
            }
        }

        public Sha256Digest scope() {
            var root = CanonicalUtf8.fromString(nativeRoot).bytes();
            var service = namespace.serviceIdentity().bytes();
            var container = namespace.containerIdentity().bytes();
            return Sha256Digest.hash(CanonicalBytes.copyOf(
                    ByteBuffer.allocate(16 + root.length() + service.length() + container.length())
                            .putInt(namespace.providerKind().ordinal())
                            .putInt(root.length())
                            .put(root.toByteArray())
                            .putInt(service.length())
                            .put(service.toByteArray())
                            .putInt(container.length())
                            .put(container.toByteArray())
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
            return entryCharge() + authorityKeyCharge() + ExactMetadataTransactionStoreV1.MAX_VALUE_BYTES;
        }

        public String nativeKey(String key) {
            if (!key.equals(HEAD_KEY)
                    && !key.matches(ENTRY_PREFIX + "[0-9a-f]{64}")
                    && !key.matches("v2/physical-delete-m5-v2/[0-9a-f]{64}/authority-v2")) {
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
        }

        public void verify(Entry entry) {
            if (!entry.scope().equals(scope())) {
                throw new IllegalArgumentException("quota entry scope differs");
            }
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
            Optional<Entry> pending) {
        public Head {
            Objects.requireNonNull(scope, "scope");
            Objects.requireNonNull(pending, "pending");
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
        }

        public static Head empty(Layout layout, long capacity) {
            var head = new Head(layout.scope(), 1, capacity, 0, 0, 0, Optional.empty());
            layout.verify(head);
            return head;
        }

        public long usedBytes(Layout layout) {
            long active = Math.multiplyExact(reservedResources, layout.reservationCharge());
            long terminal = Math.addExact(
                    Math.multiplyExact(settledResources, layout.entryCharge() + layout.authorityKeyCharge()),
                    compactDoneValueBytes);
            return Math.addExact(layout.headCharge(), Math.addExact(active, terminal));
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
                    Optional.of(new Entry(scope, resource, next, Optional.empty(), 0)));
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
                    Optional.of(entry));
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
                    Optional.empty());
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
                    pending);
        }

        public CanonicalBytes encode() {
            var out = header(HEAD_BYTES, HEAD_MAGIC)
                    .put(scope.bytes().toByteArray())
                    .putLong(revision)
                    .putLong(capacityBytes)
                    .putLong(reservedResources)
                    .putLong(settledResources)
                    .putLong(compactDoneValueBytes)
                    .putInt(pending.isPresent() ? 1 : 0)
                    .put(pending.map(entry -> entry.encode().toByteArray()).orElseGet(() -> new byte[ENTRY_BYTES]));
            return checksum(out);
        }

        public static Head decode(CanonicalBytes bytes) {
            var in = checked(bytes, HEAD_BYTES, HEAD_MAGIC);
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
            var head = new Head(
                    scope,
                    revision,
                    capacity,
                    reserved,
                    settled,
                    doneBytes,
                    flag == 0 ? Optional.empty() : Optional.of(Entry.decode(CanonicalBytes.copyOf(raw))));
            if (!head.encode().equals(bytes)) {
                throw new IllegalArgumentException("quota head canonical bytes differ");
            }
            return head;
        }

        private void requireIdle() {
            if (pending.isPresent()) {
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
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length() != size) {
            throw new IllegalArgumentException("quota encoded length differs");
        }
        var raw = bytes.toByteArray();
        var in = ByteBuffer.wrap(raw);
        if (in.getInt() != magic
                || in.getInt() != 2
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
