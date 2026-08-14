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

package com.nereusstream.storage.api.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ProviderOutcomeContractV1Test {
    @Test
    void mutationOutcomeTableIsClosedToFourStates() {
        assertThat(EnumSet.allOf(ProviderMutationOutcomeV1.class))
                .containsExactlyInAnyOrder(
                        ProviderMutationOutcomeV1.APPLIED_EXACT,
                        ProviderMutationOutcomeV1.DEFINITIVELY_NOT_APPLIED,
                        ProviderMutationOutcomeV1.OUTCOME_UNKNOWN,
                        ProviderMutationOutcomeV1.FENCED_OR_CONFLICT);
    }

    @Test
    void onlyAppliedExactCarriesProof() {
        RunLedgerHandleV1 handle = handle();

        assertThat(ProviderMutationResultV1.appliedExact(handle).exactProof()).contains(handle);
        assertThat(ProviderMutationResultV1.outcomeUnknown().exactProof()).isEmpty();
        assertThatThrownBy(() ->
                        new ProviderMutationResultV1<>(ProviderMutationOutcomeV1.OUTCOME_UNKNOWN, Optional.of(handle)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void openPreservesMismatchFenceAbsenceAndProviderFailure() {
        assertThat(EnumSet.allOf(RunLedgerOpenOutcomeV1.class))
                .containsExactlyInAnyOrder(
                        RunLedgerOpenOutcomeV1.OPENED_EXACT,
                        RunLedgerOpenOutcomeV1.CONFIGURATION_MISMATCH,
                        RunLedgerOpenOutcomeV1.FENCED,
                        RunLedgerOpenOutcomeV1.ABSENT,
                        RunLedgerOpenOutcomeV1.PROVIDER_FAILURE);
        assertThat(RunLedgerOpenResultV1.openedExact(handle()).exactHandle()).isPresent();
        assertThat(RunLedgerOpenResultV1.withoutHandle(RunLedgerOpenOutcomeV1.ABSENT)
                        .exactHandle())
                .isEmpty();
    }

    @Test
    void exactReadDefensivelyBindsBytesAndDigest() {
        CanonicalBytes payload = CanonicalBytes.copyOf(new byte[] {1, 2, 3});
        ExactLedgerEntryV1 entry = new ExactLedgerEntryV1(handle(), 7, payload, Sha256Digest.hash(payload));

        assertThat(RunLedgerReadResultV1.foundExact(entry).exactEntry()).contains(entry);
        assertThatThrownBy(() ->
                        new ExactLedgerEntryV1(handle(), 7, payload, BookKeeperCapabilitySnapshotV1Test.digest(9)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("differs");
    }

    @Test
    void appendRequestRejectsInvalidEntryAndLength() {
        RetainedStoragePayload empty = new RetainedStoragePayload() {
            @Override
            public int readableBytes() {
                return 0;
            }

            @Override
            public Sha256Digest sha256() {
                return BookKeeperCapabilitySnapshotV1Test.digest(1);
            }

            @Override
            public ByteBuffer readOnlyBuffer() {
                return ByteBuffer.allocate(0).asReadOnlyBuffer();
            }

            @Override
            public RetainedStoragePayload retain() {
                return this;
            }

            @Override
            public boolean release() {
                return false;
            }
        };

        assertThatThrownBy(() -> new RunLedgerAppendRequestV1(handle(), -1, empty))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RunLedgerAppendRequestV1(handle(), 0, empty))
                .isInstanceOf(IllegalArgumentException.class);
    }

    static RunLedgerHandleV1 handle() {
        return new RunLedgerHandleV1(
                new CellProviderScopeId(BookKeeperCapabilitySnapshotV1Test.digest(1)),
                new StorageRunId(new Id128(0, 7)),
                new BookKeeperLedgerIdentity(11),
                BookKeeperCapabilitySnapshotV1Test.digest(4));
    }
}
