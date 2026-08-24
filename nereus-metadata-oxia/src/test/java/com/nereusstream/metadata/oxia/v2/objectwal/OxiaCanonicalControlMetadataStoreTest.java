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

package com.nereusstream.metadata.oxia.v2.objectwal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.testing.DeterministicOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.testing.DeterministicOxiaConditionalClient.MutationMode;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OxiaCanonicalControlMetadataStoreTest {
    private static final String CELL_ROOT = "/nereus/v2/m3/cells/cell-a";
    private static final String POINTER = "v2/object-wal/shards/0000000007/current";
    private static final String ROOT = "v2/object-wal/shards/0000000007/runs/00000000000000000011/root";
    private static final String SEAL = "v2/object-wal/shards/0000000007/runs/00000000000000000011/seal";
    private static final String CHECKPOINT_HEAD =
            "v2/object-wal/shards/0000000007/runs/00000000000000000011/checkpoint/head";
    private static final String CHECKPOINT_PAGE = "v2/object-wal/shards/0000000007/runs/00000000000000000011/"
            + "checkpoint/pages/00000000000000000003-"
            + "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    private static final String KAFKA_HEAD =
            "v2/object-wal/shards/0000000007/runs/00000000000000000011/protocol/kafka/nwkcp1-v1/head";
    private static final CanonicalBytes FIRST = bytes("first");
    private static final CanonicalBytes SECOND = bytes("second");
    private static final CanonicalBytes DIFFERENT = bytes("different");

    private DeterministicOxiaConditionalClient client;
    private OxiaCanonicalControlMetadataStore store;

    @BeforeEach
    void setUp() {
        client = new DeterministicOxiaConditionalClient();
        store = new OxiaCanonicalControlMetadataStore(client, CELL_ROOT, 7);
    }

    @Test
    void acceptsOnlyTheClosedCellShardControlFamilies() {
        for (String key : new String[] {POINTER, ROOT, SEAL, CHECKPOINT_HEAD, CHECKPOINT_PAGE, KAFKA_HEAD}) {
            assertThat(store.putIfAbsent(key, FIRST)).isEqualTo(ControlMutationOutcome.APPLIED);
            assertThat(store.get(key)).contains(FIRST);
            assertThat(client.stored(CELL_ROOT + "/" + key)).isPresent();
        }

        assertThatThrownBy(() -> store.get("v2/object-wal/shards/0000000008/current"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> store.get(
                        "v2/object-wal/shards/0000000007/runs/00000000000000000011/protocol/kafka/nwkcp1-v1/objects/x"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> store.get("v2/object-wal/shards/0000000007/runs/00000000000000000011/../seal"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OxiaCanonicalControlMetadataStore(client, "/nereus//cell-a", 7))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OxiaCanonicalControlMetadataStore(client, "/nereus/单元", 7))
                .isInstanceOf(IllegalArgumentException.class);

        int exactRootLength = 512 - 1 - CHECKPOINT_PAGE.length();
        String exactBoundaryRoot = "/" + "a".repeat(exactRootLength - 1);
        var exactBoundaryStore = new OxiaCanonicalControlMetadataStore(client, exactBoundaryRoot, 7);
        assertThat(exactBoundaryStore.putIfAbsent(CHECKPOINT_PAGE, FIRST)).isEqualTo(ControlMutationOutcome.APPLIED);
        assertThat(client.stored(exactBoundaryRoot + "/" + CHECKPOINT_PAGE)).isPresent();
        assertThatThrownBy(() -> new OxiaCanonicalControlMetadataStore(client, exactBoundaryRoot + "a", 7))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void putIfAbsentConvergesExactResponseLossAndClosesConflicts() {
        client.nextMutation(MutationMode.APPLY_THEN_RESPONSE_LOSS);
        assertThat(store.putIfAbsent(ROOT, FIRST)).isEqualTo(ControlMutationOutcome.APPLIED);
        assertThat(store.putIfAbsent(ROOT, FIRST)).isEqualTo(ControlMutationOutcome.APPLIED);
        assertThat(store.putIfAbsent(ROOT, DIFFERENT)).isEqualTo(ControlMutationOutcome.DEFINITIVE_CONFLICT);

        client.nextMutation(MutationMode.RESPONSE_LOSS_WITHOUT_APPLY);
        assertThat(store.putIfAbsent(SEAL, SECOND)).isEqualTo(ControlMutationOutcome.RESPONSE_UNKNOWN);
        assertThat(store.get(SEAL)).isEmpty();

        client.nextSuccessStores(DIFFERENT);
        assertThat(store.putIfAbsent(CHECKPOINT_HEAD, SECOND)).isEqualTo(ControlMutationOutcome.RESPONSE_UNKNOWN);
        assertThat(store.get(CHECKPOINT_HEAD)).contains(DIFFERENT);
    }

    @Test
    void compareAndSetMapsExactExpectedBytesToTheOxiaVersionFence() {
        assertThat(store.putIfAbsent(POINTER, FIRST)).isEqualTo(ControlMutationOutcome.APPLIED);

        assertThat(store.compareAndSet(POINTER, Optional.of(FIRST), SECOND)).isEqualTo(ControlMutationOutcome.APPLIED);
        assertThat(client.casCount()).isOne();
        assertThat(client.stored(CELL_ROOT + "/" + POINTER)).get().satisfies(record -> {
            assertThat(record.storedBytes()).isEqualTo(SECOND);
            assertThat(record.versionId()).isEqualTo(1);
        });

        assertThat(store.compareAndSet(POINTER, Optional.of(FIRST), DIFFERENT))
                .isEqualTo(ControlMutationOutcome.DEFINITIVE_CONFLICT);
        assertThat(client.casCount()).isOne();
    }

    @Test
    void compareAndSetResponseLossUsesOnlyTheExactSameKeyReread() {
        assertThat(store.putIfAbsent(POINTER, FIRST)).isEqualTo(ControlMutationOutcome.APPLIED);
        client.nextMutation(MutationMode.APPLY_THEN_RESPONSE_LOSS);

        assertThat(store.compareAndSet(POINTER, Optional.of(FIRST), SECOND)).isEqualTo(ControlMutationOutcome.APPLIED);
        assertThat(store.get(POINTER)).contains(SECOND);

        client.nextMutation(MutationMode.RESPONSE_LOSS_WITHOUT_APPLY);
        assertThat(store.compareAndSet(POINTER, Optional.of(SECOND), DIFFERENT))
                .isEqualTo(ControlMutationOutcome.RESPONSE_UNKNOWN);
        assertThat(store.get(POINTER)).contains(SECOND);
    }

    @Test
    void exactAbsenceCasUsesConditionalCreateAndNeverOverwrites() {
        assertThat(store.compareAndSet(KAFKA_HEAD, Optional.empty(), FIRST)).isEqualTo(ControlMutationOutcome.APPLIED);
        assertThat(store.compareAndSet(KAFKA_HEAD, Optional.empty(), SECOND))
                .isEqualTo(ControlMutationOutcome.DEFINITIVE_CONFLICT);
        assertThat(store.get(KAFKA_HEAD)).contains(FIRST);
    }

    @Test
    void readFailureIsNeverReportedAsAbsenceAndMutationRereadFailureIsUnknown() {
        client.failNextRead();
        assertThatThrownBy(() -> store.get(ROOT))
                .isInstanceOf(OxiaCanonicalControlMetadataStore.ControlMetadataBackendException.class);

        client.failNextRead();
        assertThat(store.putIfAbsent(ROOT, FIRST)).isEqualTo(ControlMutationOutcome.RESPONSE_UNKNOWN);
        assertThat(store.get(ROOT)).contains(FIRST);
    }

    @Test
    void mapsTheVersionReturnedByTheExactPreReadRatherThanGuessingARevision() {
        assertThat(store.putIfAbsent(POINTER, FIRST)).isEqualTo(ControlMutationOutcome.APPLIED);
        client.beforeNextRead(() -> client.seed(CELL_ROOT + "/" + POINTER, FIRST, 9));

        assertThat(store.compareAndSet(POINTER, Optional.of(FIRST), SECOND)).isEqualTo(ControlMutationOutcome.APPLIED);
        assertThat(client.stored(CELL_ROOT + "/" + POINTER))
                .get()
                .extracting(record -> record.versionId())
                .isEqualTo(10L);
    }

    @Test
    void identicalBytesCannotHideAnOxiaVersionAbaBetweenPreReadAndCas() {
        String backendKey = CELL_ROOT + "/" + POINTER;
        client.seed(backendKey, FIRST, 4);
        OxiaConditionalClient abaClient = new OxiaConditionalClient() {
            @Override
            public CompletionStage<Optional<AuthorityRecord>> read(String key) {
                return client.read(key);
            }

            @Override
            public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes storedBytes) {
                return client.createIfAbsent(key, storedBytes);
            }

            @Override
            public CompletionStage<Void> compareAndSet(String key, CanonicalBytes storedBytes, long expectedVersionId) {
                client.seed(key, FIRST, Math.addExact(expectedVersionId, 1));
                return client.compareAndSet(key, storedBytes, expectedVersionId);
            }
        };
        var abaStore = new OxiaCanonicalControlMetadataStore(abaClient, CELL_ROOT, 7);

        assertThat(abaStore.compareAndSet(POINTER, Optional.of(FIRST), SECOND))
                .isEqualTo(ControlMutationOutcome.DEFINITIVE_CONFLICT);
        assertThat(abaStore.get(POINTER)).contains(FIRST);
    }

    private static CanonicalBytes bytes(String value) {
        return CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8));
    }
}
