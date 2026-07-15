/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.CompletionException;

final class ObjectProtectionTestSupport {
    static final String CLUSTER = "cluster-a";
    static final long NOW = 2_000_000L;

    private ObjectProtectionTestSupport() {
    }

    static DefaultObjectProtectionManager manager(
            FakePhysicalObjectMetadataStore store,
            Clock clock) {
        return new DefaultObjectProtectionManager(
                CLUSTER,
                store,
                Duration.ofMinutes(1),
                Duration.ofSeconds(1),
                Duration.ofMinutes(5),
                clock);
    }

    static PhysicalObjectIdentity object() {
        return PhysicalObjectIdentity.create(
                new ObjectKey("objects/protection-target"),
                Optional.empty(),
                PhysicalObjectKind.COMMITTED_COMPACTED,
                128,
                new Checksum(ChecksumType.CRC32C, "01020304"),
                Optional.of(new Checksum(ChecksumType.SHA256, "a".repeat(64))),
                Optional.of("etag"));
    }

    static ObjectProtectionOwner owner(String suffix, long version) {
        return new ObjectProtectionOwner(
                "/owners/" + suffix,
                version,
                new Checksum(ChecksumType.SHA256, suffix.repeat(64).substring(0, 64)));
    }

    static ObjectProtectionRequest permanent(ObjectProtectionOwner owner) {
        return new ObjectProtectionRequest(
                object(),
                ObjectProtectionType.VISIBLE_GENERATION,
                "generation-ref",
                owner,
                0);
    }

    static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    static final class MutableClock extends Clock {
        private long millis;

        MutableClock(long millis) {
            this.millis = millis;
        }

        void advance(Duration duration) {
            millis = Math.addExact(millis, duration.toMillis());
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override
        public long millis() {
            return millis;
        }
    }
}
