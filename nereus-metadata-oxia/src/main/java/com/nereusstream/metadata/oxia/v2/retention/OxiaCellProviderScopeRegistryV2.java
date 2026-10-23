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

package com.nereusstream.metadata.oxia.v2.retention;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.codec.ProtocolCellIdentityCodecV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.object.materialization.M5CellProviderProfileV2;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** One bounded append-only scope catalogue on the existing physical root. It grants no Task or delete authority. */
public final class OxiaCellProviderScopeRegistryV2 {
    private static final int MAGIC = 0x4d355352; // M5SR
    public static final int MAX_SCOPES = 256;
    public static final int MAX_BYTES = ExactMetadataTransactionStoreV1.MAX_VALUE_BYTES;
    private static final Comparator<M5CellProviderProfileV2> ORDER = Comparator.comparing(value -> HexFormat.of()
            .formatHex(ProtocolCellIdentityCodecV1.encode(value.protocolCell()).toByteArray()));
    private final ExactMetadataTransactionStoreV1 metadata;
    private final PhysicalNamespaceAuthorityBindingV2 binding;
    private final String key;

    /** Only the actual metadata namespace factory installs the guarded native backend route. */
    OxiaCellProviderScopeRegistryV2(
            ExactMetadataTransactionStoreV1 metadata, PhysicalNamespaceAuthorityBindingV2 binding) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.binding = Objects.requireNonNull(binding, "binding");
        key = binding.authorityRoot() + "/cell-provider-scopes-v2";
    }

    public String key() {
        return key;
    }

    /** Read-only GC/recovery reopen. Missing or different history cannot be admitted by this path. */
    public CompletableFuture<Admission> reopen(M5CellProviderProfileV2 profile) {
        if (!profile.physicalBinding().equals(binding)) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Cell Provider scope differs"));
        }
        return metadata.read(key)
                .thenApply(actual -> {
                    if (actual.map(this::decode)
                            .flatMap(value -> sameCell(value, profile))
                            .filter(profile::equals)
                            .isEmpty()) {
                        throw new IllegalStateException("Original Cell Provider scope is absent or differs");
                    }
                    return new Admission(profile);
                })
                .toCompletableFuture();
    }

    public Attempt prepare(M5CellProviderProfileV2 profile) {
        if (!profile.physicalBinding().equals(binding)) {
            throw new IllegalArgumentException("Cell Provider scope belongs to another physical namespace assignment");
        }
        return new Attempt(profile);
    }

    public record Catalogue(long revision, List<M5CellProviderProfileV2> profiles) {
        public Catalogue {
            profiles = List.copyOf(profiles);
            if (revision < 1
                    || profiles.isEmpty()
                    || profiles.size() > MAX_SCOPES
                    || !profiles.equals(profiles.stream().sorted(ORDER).toList())) {
                throw new IllegalArgumentException("Cell Provider catalogue revision/count/order differs");
            }
            var physical = profiles.get(0).physicalBinding();
            for (int i = 0; i < profiles.size(); i++) {
                var profile = profiles.get(i);
                if (!physical.equals(profile.physicalBinding())) {
                    throw new IllegalArgumentException("Cell Provider catalogue has a foreign physical assignment");
                }
                for (int j = 0; j < i; j++) {
                    var previous = profiles.get(j);
                    if (profile.protocolCell().equals(previous.protocolCell()) || profile.overlapsPrefix(previous)) {
                        throw new IllegalArgumentException(
                                "Cell Provider catalogue has duplicate Cell or overlapping prefix");
                    }
                }
            }
        }

        public CanonicalBytes encode() {
            try {
                var bytes = new ByteArrayOutputStream();
                try (var out = new DataOutputStream(bytes)) {
                    out.writeInt(MAGIC);
                    out.writeInt(2);
                    out.writeLong(revision);
                    out.writeInt(profiles.size());
                    for (var profile : profiles) {
                        var encoded = profile.encode();
                        out.writeInt(encoded.length());
                        out.write(encoded.toByteArray());
                        if (bytes.size() > MAX_BYTES) {
                            throw new IllegalArgumentException("Cell Provider catalogue exceeds its byte cap");
                        }
                    }
                }
                return CanonicalBytes.copyOf(bytes.toByteArray());
            } catch (IOException impossible) {
                throw new IllegalStateException("Cannot encode Cell Provider catalogue", impossible);
            }
        }

        public static Catalogue decode(CanonicalBytes bytes) {
            if (bytes == null || bytes.isEmpty() || bytes.length() > MAX_BYTES) {
                throw new IllegalArgumentException("Cell Provider catalogue body length differs");
            }
            try (var in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                if (in.readInt() != MAGIC || in.readInt() != 2) {
                    throw new IllegalArgumentException("Cell Provider catalogue type/version differs");
                }
                long revision = in.readLong();
                int count = in.readInt();
                if (count < 1 || count > MAX_SCOPES) {
                    throw new IllegalArgumentException("Cell Provider catalogue count exceeds its cap");
                }
                var profiles = new ArrayList<M5CellProviderProfileV2>(count);
                for (int i = 0; i < count; i++) {
                    int length = in.readInt();
                    if (length < 1 || length > M5CellProviderProfileV2.MAX_BYTES || length > in.available()) {
                        throw new IllegalArgumentException("Cell Provider catalogue profile length differs");
                    }
                    var encoded = new byte[length];
                    in.readFully(encoded);
                    profiles.add(M5CellProviderProfileV2.decode(CanonicalBytes.copyOf(encoded)));
                }
                var result = new Catalogue(revision, profiles);
                if (in.read() != -1 || !result.encode().equals(bytes)) {
                    throw new IllegalArgumentException("Cell Provider catalogue is not exact canonical bytes");
                }
                return result;
            } catch (IOException failure) {
                throw new IllegalArgumentException("Cell Provider catalogue is truncated", failure);
            }
        }
    }

    private Catalogue decode(VersionedValue stored) {
        if (!key.equals(stored.key())) {
            throw new IllegalArgumentException("Cell Provider catalogue native key differs");
        }
        var catalogue = Catalogue.decode(stored.canonicalStoredBytes());
        if (!catalogue.profiles().get(0).physicalBinding().equals(binding)) {
            throw new IllegalArgumentException("Cell Provider catalogue physical assignment differs");
        }
        return catalogue;
    }

    private static Optional<M5CellProviderProfileV2> sameCell(Catalogue catalogue, M5CellProviderProfileV2 profile) {
        return catalogue.profiles().stream()
                .filter(value -> value.protocolCell().equals(profile.protocolCell()))
                .findFirst();
    }

    public final class Attempt {
        private final M5CellProviderProfileV2 profile;
        private CompletableFuture<Admission> original;
        private Optional<VersionedValue> predecessor;
        private CanonicalBytes candidate;
        private boolean mutationAttempted;
        private boolean mutationExcluded;
        private boolean confirmed;

        private Attempt(M5CellProviderProfileV2 profile) {
            this.profile = profile;
        }

        public M5CellProviderProfileV2 profile() {
            return profile;
        }

        public synchronized CompletableFuture<Admission> admit() {
            if (original == null) {
                original = perform();
            }
            return original.copy();
        }

        /** Original profile only; observation cancellation cannot alter the accepted native future. */
        public synchronized CompletableFuture<Admission> reconcile() {
            if (original == null || original.isCompletedExceptionally()) {
                original = perform();
            }
            return original.copy();
        }

        /** No live original callback, and either exact admission or known exclusion/no metadata dispatch. */
        public synchronized boolean isSettledForClose() {
            return original == null || original.isDone() && (confirmed || !mutationAttempted || mutationExcluded);
        }

        private CompletableFuture<Admission> perform() {
            return metadata.read(key)
                    .thenCompose(observed -> {
                        if (mutationAttempted && !observed.equals(predecessor)) {
                            // The catalogue cannot be deleted by this protocol. Exact native version advancement
                            // excludes
                            // the old CAS; presence excludes the old create-only operation.
                            mutationExcluded = observed.isPresent()
                                    && (predecessor.isEmpty()
                                            || !observed.orElseThrow()
                                                    .metadataVersion()
                                                    .equals(predecessor
                                                            .orElseThrow()
                                                            .metadataVersion()));
                        }
                        var current = observed.map(OxiaCellProviderScopeRegistryV2.this::decode);
                        var existing = current.flatMap(value -> sameCell(value, profile));
                        if (existing.isPresent()) {
                            if (!existing.equals(Optional.of(profile))) {
                                throw new IllegalStateException(
                                        "Cell Provider profile is already frozen with different fields");
                            }
                            return confirm();
                        }
                        // A later exact native version excludes an earlier conditional write; the profile stays
                        // identical.
                        // An unchanged predecessor retries the identical original catalogue candidate, never another
                        // scope.
                        if (candidate == null || !observed.equals(predecessor)) {
                            var profiles = new ArrayList<>(
                                    current.map(Catalogue::profiles).orElse(List.of()));
                            profiles.add(profile);
                            var next = new Catalogue(
                                    current.map(value -> Math.addExact(value.revision(), 1))
                                            .orElse(1L),
                                    profiles.stream().sorted(ORDER).toList());
                            predecessor = observed;
                            candidate = next.encode();
                        }
                        mutationAttempted = true;
                        mutationExcluded = false;
                        return metadata.compareAndSet(predecessor, key, candidate)
                                .thenCompose(outcome -> {
                                    if (outcome == ExactMetadataTransactionStoreV1.MutationOutcome.DEFINITIVE_CONFLICT
                                            || outcome
                                                    == ExactMetadataTransactionStoreV1.MutationOutcome
                                                            .PREDECESSOR_UNCHANGED) {
                                        mutationExcluded = true;
                                    }
                                    return confirm();
                                });
                    })
                    .toCompletableFuture();
        }

        private CompletableFuture<Admission> confirm() {
            return metadata.read(key)
                    .thenApply(value -> {
                        var observed = value.map(OxiaCellProviderScopeRegistryV2.this::decode)
                                .flatMap(catalogue -> sameCell(catalogue, profile));
                        if (!observed.equals(Optional.of(profile))) {
                            throw new IllegalStateException(
                                    "Original Cell Provider scope admission remains unresolved");
                        }
                        confirmed = true;
                        return new Admission(profile);
                    })
                    .toCompletableFuture();
        }
    }

    /** Constructed only after the guarded namespace route independently observes the exact persisted profile. */
    public final class Admission {
        private final M5CellProviderProfileV2 profile;

        private Admission(M5CellProviderProfileV2 profile) {
            this.profile = profile;
        }

        public M5CellProviderProfileV2 profile() {
            return profile;
        }

        public CompletableFuture<com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1>
                captureAuthority() {
            return metadata.read(key)
                    .thenApply(value -> {
                        var actual = value.orElseThrow();
                        if (!sameCell(decode(actual), profile).equals(Optional.of(profile))) {
                            throw new IllegalStateException("Admitted Cell Provider scope is no longer exact");
                        }
                        return new com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1(
                                actual.key(), actual.metadataVersion(), actual.canonicalStoredSha256());
                    })
                    .toCompletableFuture();
        }

        public CompletableFuture<Void> requireCurrent() {
            return metadata.read(key)
                    .thenAccept(value -> {
                        var current = value.map(OxiaCellProviderScopeRegistryV2.this::decode)
                                .flatMap(catalogue -> sameCell(catalogue, profile));
                        if (!current.equals(Optional.of(profile))) {
                            throw new IllegalStateException("Admitted Cell Provider scope is no longer exact");
                        }
                    })
                    .toCompletableFuture();
        }
    }
}
