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

package com.nereusstream.pulsar.broker;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.pulsar.broker.storage.nereus.v2.NereusPulsarActiveFence.ValidWord;
import org.apache.pulsar.broker.storage.nereus.v2.NereusPulsarTopicAuthority;

/** The stable numeric Object WAL epoch selected by one actual native ownership acquisition. */
final class PulsarM6ObjectWalOwner {
    private static final int MAGIC = 0x4e504f45; // NPOE
    private static final int BYTES = Integer.BYTES + 2 * Long.BYTES + 2 * Sha256Digest.LENGTH;
    private final ExactMetadataTransactionStoreV1 metadata;
    private final String key;
    private final NereusPulsarTopicAuthority nativeAuthority;
    private final Claim claim;

    private PulsarM6ObjectWalOwner(
            ExactMetadataTransactionStoreV1 metadata,
            String key,
            NereusPulsarTopicAuthority nativeAuthority,
            Claim claim) {
        this.metadata = metadata;
        this.key = key;
        this.nativeAuthority = nativeAuthority;
        this.claim = claim;
    }

    /** First acquisition requires the original, empty allocator Head. A later missing epoch is never recreated. */
    static CompletionStage<PulsarM6ObjectWalOwner> acquire(
            ExactMetadataTransactionStoreV1 metadata,
            String key,
            NereusPulsarTopicAuthority nativeAuthority,
            boolean originalEmptyHead) {
        var captured = nativeAuthority.captureOrNull();
        if (captured == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("Native Object WAL ownership is absent"));
        }
        var binding = captured.binding().aggregateDigest();
        var acquisition = acquisition(captured);
        long version = captured.ownership().backendVersion();
        return metadata.read(key).thenCompose(previous -> {
            if (!nativeAuthority.isCurrent(captured)) {
                throw new IllegalStateException("Native Object WAL ownership changed before epoch admission");
            }
            var old = previous.map(value -> Claim.decode(value.canonicalStoredBytes()));
            if (old.isPresent() && !old.orElseThrow().binding().equals(binding)) {
                throw new IllegalStateException("Native Object WAL owner record belongs to another Binding");
            }
            if (old.filter(value -> value.acquisition().equals(acquisition)).isPresent()) {
                return CompletableFuture.completedFuture(
                        new PulsarM6ObjectWalOwner(metadata, key, nativeAuthority, old.orElseThrow()));
            }
            if (old.isEmpty() && !originalEmptyHead
                    || old.isPresent() && version <= old.orElseThrow().nativeVersion()) {
                throw new IllegalStateException("Native Object WAL has no exact predecessor ownership");
            }
            var claim = new Claim(
                    old.map(value -> Math.incrementExact(value.epoch())).orElse(1L), version, binding, acquisition);
            return metadata.compareAndSet(previous, key, claim.encode())
                    .thenCompose(unused -> metadata.read(key))
                    .thenApply(exact -> {
                        if (!nativeAuthority.isCurrent(captured)
                                || !exact.map(value -> value.canonicalStoredBytes())
                                        .equals(Optional.of(claim.encode()))) {
                            throw new IllegalStateException("Original Native Object WAL epoch remains unresolved");
                        }
                        return new PulsarM6ObjectWalOwner(metadata, key, nativeAuthority, claim);
                    });
        });
    }

    long epoch() {
        return claim.epoch();
    }

    Sha256Digest commitment() {
        return Sha256Digest.hash(witness());
    }

    CanonicalBytes witness() {
        return claim.encode();
    }

    static void requireWitness(CanonicalBytes witness, Sha256Digest bindingAggregate) {
        if (!Claim.decode(witness).binding().equals(bindingAggregate)) {
            throw new IllegalStateException("Object WAL Root creator belongs to a different Binding");
        }
    }

    ValidWord capture() {
        var word = nativeAuthority.captureOrNull();
        if (word == null
                || !claim.binding().equals(word.binding().aggregateDigest())
                || !claim.acquisition().equals(acquisition(word))) {
            throw new IllegalStateException("Native Object WAL owner was fenced");
        }
        return word;
    }

    boolean isCurrent(ValidWord captured) {
        return nativeAuthority.isCurrent(captured);
    }

    CompletionStage<Void> requireCurrent() {
        var word = capture();
        return metadata.read(key).thenAccept(exact -> {
            if (!isCurrent(word)
                    || !exact.map(value -> value.canonicalStoredBytes()).equals(Optional.of(claim.encode()))) {
                throw new IllegalStateException("Native Object WAL owner record changed");
            }
        });
    }

    /** A write already accepted before native stop may finish only under the same durable epoch. */
    CompletionStage<Void> requireSelectedEpoch() {
        return metadata.read(key).thenAccept(exact -> {
            if (!exact.map(value -> value.canonicalStoredBytes()).equals(Optional.of(witness()))) {
                throw new IllegalStateException("Original Pulsar Object WAL epoch was replaced");
            }
        });
    }

    private static Sha256Digest acquisition(ValidWord word) {
        var ownership = word.ownership();
        return Sha256Digest.hash(CanonicalUtf8.fromString(String.join(
                        "\0",
                        "NEREUS_PULSAR_OBJECT_ACQUISITION_V1",
                        ownership.serviceUnit(),
                        ownership.localBrokerId(),
                        ownership.brokerIncarnationId().value(),
                        ownership.acquisitionId().value()))
                .bytes());
    }

    private record Claim(long epoch, long nativeVersion, Sha256Digest binding, Sha256Digest acquisition) {
        private Claim {
            if (epoch <= 0 || nativeVersion < 0 || binding.isZero() || acquisition.isZero()) {
                throw new IllegalArgumentException("Native Object WAL owner claim is outside its domain");
            }
        }

        CanonicalBytes encode() {
            return CanonicalBytes.copyOf(ByteBuffer.allocate(BYTES)
                    .putInt(MAGIC)
                    .putLong(epoch)
                    .putLong(nativeVersion)
                    .put(binding.bytes().toByteArray())
                    .put(acquisition.bytes().toByteArray())
                    .array());
        }

        static Claim decode(CanonicalBytes bytes) {
            if (bytes.length() != BYTES) {
                throw new IllegalArgumentException("Native Object WAL owner claim length differs");
            }
            var input = ByteBuffer.wrap(bytes.toByteArray());
            if (input.getInt() != MAGIC) {
                throw new IllegalArgumentException("Native Object WAL owner claim version differs");
            }
            long epoch = input.getLong();
            long nativeVersion = input.getLong();
            var binding = new byte[Sha256Digest.LENGTH];
            var acquisition = new byte[Sha256Digest.LENGTH];
            input.get(binding).get(acquisition);
            return new Claim(epoch, nativeVersion, Sha256Digest.copyOf(binding), Sha256Digest.copyOf(acquisition));
        }
    }
}
