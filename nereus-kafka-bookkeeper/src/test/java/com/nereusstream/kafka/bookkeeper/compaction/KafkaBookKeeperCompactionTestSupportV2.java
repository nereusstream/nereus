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

package com.nereusstream.kafka.bookkeeper.compaction;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperDigestTypeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperProtocolModeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperTimeoutClassV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.ProviderKind;
import com.nereusstream.storage.bookkeeper.BookKeeperV3Crc32cAddPayloadLimitV1;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.SimpleRecord;

/** Explicit synthetic control/source authority fixtures; real BK tests reuse only the physical provider path. */
final class KafkaBookKeeperCompactionTestSupportV2 {
    private KafkaBookKeeperCompactionTestSupportV2() {}

    static KafkaBookKeeperCompactionLayoutV2.Layout layout(boolean empty, long attempt) {
        return input(empty, attempt).layout();
    }

    record Input(
            KafkaCompactionRecordsV1.CompactionPlan plan,
            KafkaCompactionSemanticOutputV2 semantic,
            KafkaBookKeeperCompactionLayoutV2.Layout layout,
            com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityEvidence
                    capabilityEvidence) {}

    static Input input(boolean empty, long attempt) {
        var bodies = empty
                ? List.of(KafkaSemanticCompactorV1Test.emptyBatch(0, 1))
                : List.of(
                        KafkaSemanticCompactorV1Test.records(
                                0, 1, Compression.NONE, new SimpleRecord(1, new byte[] {1}, new byte[] {4, 5, 6})),
                        KafkaSemanticCompactorV1Test.records(
                                1, 1, Compression.NONE, new SimpleRecord(2, new byte[] {1}, new byte[1024])));
        var proofs = empty
                ? List.<KafkaCompactionRecordsV1.LatestKeyProof>of()
                : List.of(new KafkaCompactionRecordsV1.LatestKeyProof(
                        CanonicalBytes.copyOf(new byte[] {1}), 1, true, true, false, 100));
        return input(bodies, 0, empty ? 1 : 2, proofs, List.of(), attempt);
    }

    static Input input(
            List<CanonicalBytes> bodies,
            long start,
            long end,
            List<KafkaCompactionRecordsV1.LatestKeyProof> proofs,
            List<KafkaCompactionRecordsV1.TransactionRange> transactions,
            long attempt) {
        var fixture = new KafkaSemanticCompactorV1Test.Fixture(bodies, start, end);
        var initial = fixture.plan(proofs, transactions, List.of(), 100, CompressionType.NONE);
        var capabilityEvidence =
                new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityEvidence(
                        initial.sourceCut().identity().binding(),
                        1,
                        1,
                        com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityKind
                                .DURABLE_DRAIN_ONLY_V1,
                        com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityState.ADMITTED,
                        digest("synthetic-adapter"),
                        digest("synthetic-backend"),
                        digest("synthetic-contract"),
                        digest("synthetic-verifier"),
                        digest("synthetic-receipt-id"),
                        digest("synthetic-receipt"),
                        digest("synthetic-time"),
                        10_000,
                        0,
                        0);
        var admitted = new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding(
                1,
                com.nereusstream.storage.object.read.control.M4ReadControlCodecV1.capabilityEvidenceSha256(
                        capabilityEvidence));
        var plan = new KafkaSemanticCompactorV1Test.Fixture(bodies, start, end, admitted)
                .plan(proofs, transactions, List.of(), 100, CompressionType.NONE);
        var semantic = new KafkaSemanticCompactorV1().compileSemantic(plan);
        var layout = KafkaBookKeeperCompactionLayoutV2.plan(
                plan,
                semantic,
                new Namespace(
                        ProviderKind.BOOKKEEPER,
                        CanonicalUtf8.fromString("test-bk-incarnation"),
                        CanonicalUtf8.fromString("test-ledgers-generation")),
                capability(plan.sourceCut().identity().providerScopeSha256()),
                attempt,
                512,
                1024);
        return new Input(plan, semantic, layout, capabilityEvidence);
    }

    static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalUtf8.fromString(value).bytes());
    }

    static List<com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity>
            installReadAuthority(Store store, Input input) {
        store.allowSelectorCas = true;
        return installReadAuthority((CanonicalControlMetadataStore) store, input);
    }

    static List<com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity>
            installReadAuthority(CanonicalControlMetadataStore store, Input input) {
        var cut = input.plan().sourceCut();
        var m4 = new com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1(
                store, 7, cut.identity().binding());
        m4.createCapability(input.capabilityEvidence());
        var sources = cut.sources().stream()
                .map(source ->
                        new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1
                                .SourceProtectionIdentity(
                                source.sourceIdentitySha256(),
                                1,
                                1,
                                cut.predecessorSelector().sourceGeneration(),
                                cut.identity().capability()))
                .sorted(java.util.Comparator.comparing(
                        source -> source.sourceIdentitySha256().toHex()))
                .toList();
        for (var source : sources) {
            m4.createProtection(
                    new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtection(
                            cut.identity().binding(),
                            source,
                            com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProtectionState
                                    .PROTECTED,
                            Optional.empty(),
                            Optional.empty()));
        }
        m4.createSelector(cut.predecessorSelector());
        return sources;
    }

    private static BookKeeperCapabilitySnapshotV1 capability(Sha256Digest scope) {
        int limit = 5_242_880;
        return new BookKeeperCapabilitySnapshotV1(
                new CellProviderScopeId(scope),
                "cd06340851d6d657b7c7546df01df365c18980de",
                sha("8e64f2b7436bb814705f611eb0ac48d64d90de7a50d295905c459d89bc3f9d8f"),
                "cd06340851d6d657b7c7546df01df365c18980de",
                sha("c0a128931c402d6bf6a6f973ba2f305b9be261659e30754ab95a29510a33bc0d"),
                BookKeeperProtocolModeV1.V3,
                limit,
                limit,
                BookKeeperV3Crc32cAddPayloadLimitV1.maximumAddPayloadBytes(limit, limit),
                true,
                3,
                3,
                2,
                BookKeeperDigestTypeV1.CRC32C,
                true,
                true,
                new BookKeeperTimeoutClassV1(10_000, 5_000, 5_000, 30_000),
                "bk-k0-no-auth:v1",
                sha("eaf41c4b42b767b8ea6e86023a784425b8073f174dbade92b4249c8f3d301dbd"));
    }

    private static Sha256Digest sha(String value) {
        return Sha256Digest.copyOf(HexFormat.of().parseHex(value));
    }

    static final class Store implements CanonicalControlMetadataStore {
        final Map<String, CanonicalBytes> values = new LinkedHashMap<>();
        final List<String> operations = new ArrayList<>();
        boolean loseNextPutResponse;
        boolean dropNextPut;
        boolean allowSelectorCas;
        boolean loseNextSelectorCasResponse;
        boolean dropNextSelectorCas;
        int selectorCasCount;
        Runnable beforeNextSelectorCas;

        @Override
        public synchronized Optional<CanonicalBytes> get(String key) {
            operations.add("read:" + key);
            return Optional.ofNullable(values.get(key));
        }

        @Override
        public synchronized ControlMutationOutcome putIfAbsent(String key, CanonicalBytes value) {
            operations.add("put:" + key);
            if (dropNextPut) {
                dropNextPut = false;
                return ControlMutationOutcome.RESPONSE_UNKNOWN;
            }
            boolean inserted = values.putIfAbsent(key, value) == null;
            if (loseNextPutResponse) {
                loseNextPutResponse = false;
                return ControlMutationOutcome.RESPONSE_UNKNOWN;
            }
            return inserted ? ControlMutationOutcome.APPLIED : ControlMutationOutcome.DEFINITIVE_CONFLICT;
        }

        @Override
        public synchronized ControlMutationOutcome compareAndSet(
                String key, Optional<CanonicalBytes> expected, CanonicalBytes value) {
            if (!allowSelectorCas || !key.endsWith("/selector")) {
                throw new AssertionError("immutable BK inventory must never overwrite a prior allocation");
            }
            selectorCasCount++;
            Runnable before = beforeNextSelectorCas;
            beforeNextSelectorCas = null;
            if (before != null) {
                before.run();
            }
            if (dropNextSelectorCas) {
                dropNextSelectorCas = false;
                return ControlMutationOutcome.RESPONSE_UNKNOWN;
            }
            boolean matches = Optional.ofNullable(values.get(key)).equals(expected);
            if (matches) {
                values.put(key, value);
            }
            if (loseNextSelectorCasResponse) {
                loseNextSelectorCasResponse = false;
                return ControlMutationOutcome.RESPONSE_UNKNOWN;
            }
            return matches ? ControlMutationOutcome.APPLIED : ControlMutationOutcome.DEFINITIVE_CONFLICT;
        }
    }
}
