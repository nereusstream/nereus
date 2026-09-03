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

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.metadata.oxia.v2.mutation.MetadataVersionMapper;
import com.nereusstream.metadata.oxia.v2.testing.DeterministicOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.testing.DeterministicOxiaConditionalClient.MutationMode;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.ExactCondition;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.ExactPut;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.ExactTransaction;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.TransactionOutcome;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class Oxia09ExactMetadataTransactionStoreV1Test {
    @Test
    void singleKeyCasUsesExactVersionAndConvergesAfterResponseLoss() {
        DeterministicOxiaConditionalClient client = new DeterministicOxiaConditionalClient();
        CanonicalBytes predecessorBytes = bytes("predecessor");
        CanonicalBytes candidate = bytes("candidate");
        client.seed("/key", predecessorBytes, 7);
        VersionedValue predecessor = VersionedValue.of("/key", predecessorBytes, MetadataVersionMapper.fromOxia(7));
        client.nextMutation(MutationMode.APPLY_THEN_RESPONSE_LOSS);
        Oxia09ExactMetadataTransactionStoreV1 store = new Oxia09ExactMetadataTransactionStoreV1(client);

        assertThat(store.compareAndSet(Optional.of(predecessor), "/key", candidate)
                        .toCompletableFuture()
                        .join())
                .isEqualTo(MutationOutcome.APPLIED_EXACT);
        assertThat(store.read("/key").toCompletableFuture().join())
                .get()
                .extracting(VersionedValue::canonicalStoredBytes)
                .isEqualTo(candidate);
    }

    @Test
    void multiKeyTransactionIsExplicitlyUnsupportedWithoutAnyBackendMutation() {
        DeterministicOxiaConditionalClient client = new DeterministicOxiaConditionalClient();
        Oxia09ExactMetadataTransactionStoreV1 store = new Oxia09ExactMetadataTransactionStoreV1(client);
        ExactTransaction transaction = new ExactTransaction(
                "/partition",
                List.of(ExactCondition.absent("/a"), ExactCondition.absent("/b")),
                List.of(ExactPut.of("/a", bytes("a")), ExactPut.of("/b", bytes("b"))));

        assertThat(store.supportsAtomicMultiKeyTransactions()).isFalse();
        assertThat(store.conditionalTransaction(transaction)
                        .toCompletableFuture()
                        .join())
                .isEqualTo(TransactionOutcome.UNSUPPORTED);
        assertThat(client.createCount()).isZero();
        assertThat(client.casCount()).isZero();
        assertThat(client.readCount()).isZero();
    }

    private static CanonicalBytes bytes(String value) {
        return CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8));
    }
}
