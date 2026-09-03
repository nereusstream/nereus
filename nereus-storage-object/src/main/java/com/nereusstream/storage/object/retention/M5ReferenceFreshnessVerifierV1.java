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

package com.nereusstream.storage.object.retention;

import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceFreeProofV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorSnapshotV1;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Exact authoritative version-vector reread for floor snapshots and reference-free proofs. */
public final class M5ReferenceFreshnessVerifierV1 {
    private final ExactMetadataTransactionStoreV1 metadata;

    public M5ReferenceFreshnessVerifierV1(ExactMetadataTransactionStoreV1 metadata) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
    }

    public CompletionStage<List<VersionedValue>> requireFresh(RetentionFloorSnapshotV1 snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        M5RetentionCodecV1.encodeSnapshot(snapshot);
        List<AuthorityFactV1> facts = new ArrayList<>();
        snapshot.rows().forEach(row -> facts.add(row.authority()));
        facts.add(snapshot.ownerFence());
        facts.add(snapshot.storageFence());
        return loadExact(deduplicate(facts));
    }

    public CompletionStage<List<VersionedValue>> requireFresh(ReferenceFreeProofV1 proof) {
        Objects.requireNonNull(proof, "proof");
        M5RetentionCodecV1.encodeReferenceFreeProof(proof);
        List<AuthorityFactV1> facts = new ArrayList<>();
        facts.add(proof.selectorRoot());
        facts.add(proof.manifestRoot());
        facts.add(proof.trimRoot());
        proof.m4Releases().forEach(release -> facts.add(release.protectionAuthority()));
        proof.observations().forEach(observation -> facts.add(observation.authority()));
        facts.add(proof.ownerFence());
        facts.add(proof.workerFence());
        facts.add(proof.storageFence());
        facts.add(proof.providerFence());
        return loadExact(deduplicate(facts));
    }

    private CompletionStage<List<VersionedValue>> loadExact(List<AuthorityFactV1> facts) {
        CompletionStage<List<VersionedValue>> stage = CompletableFuture.completedFuture(new ArrayList<>());
        for (AuthorityFactV1 fact : facts) {
            stage = stage.thenCompose(values -> metadata.read(fact.key()).thenApply(observed -> {
                VersionedValue exact = observed.orElseThrow(
                        () -> new StaleAuthorityException("required authority is absent: " + fact.key()));
                if (!fact.metadataVersion().equals(exact.metadataVersion())
                        || !fact.valueSha256().equals(exact.canonicalStoredSha256())) {
                    throw new StaleAuthorityException("required authority version/value changed: " + fact.key());
                }
                values.add(exact);
                return values;
            }));
        }
        return stage.thenApply(List::copyOf);
    }

    static List<AuthorityFactV1> deduplicate(List<AuthorityFactV1> facts) {
        Map<String, AuthorityFactV1> byKey = new LinkedHashMap<>();
        facts.forEach(fact -> {
            AuthorityFactV1 previous = byKey.remove(fact.key());
            if (previous != null && !previous.equals(fact)) {
                throw new IllegalArgumentException("one authority key has conflicting facts: " + fact.key());
            }
            byKey.put(fact.key(), fact);
        });
        return List.copyOf(byKey.values());
    }

    /** A version-vector member changed or disappeared, so the snapshot/proof must be rebuilt. */
    public static final class StaleAuthorityException extends IllegalStateException {
        public StaleAuthorityException(String message) {
            super(message);
        }
    }
}
