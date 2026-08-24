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

package com.nereusstream.kafka.bookkeeper.object.nwkcp1;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.storage.object.control.WalRunTerminalClosureProofV1;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/** Narrow provider-object plus metadata-CAS mapping required by NWKCP1 publication. */
public interface Nwkcp1BackendV1 {
    /** Opaque backend-issued capability for the exact object whose conditional create converged. */
    interface CreatedObjectToken {
        String key();

        long length();

        Sha256Digest digest();
    }

    /** Opaque backend-issued capability for the exact object selected by one exact Head value SHA. */
    interface SelectedObjectToken {
        String key();

        long length();

        Sha256Digest digest();

        Sha256Digest exactHeadValueSha256();
    }

    enum CreateDisposition {
        APPLIED,
        EXISTING_EXACT,
        DEFINITIVELY_NOT_APPLIED,
        UNKNOWN,
        CONFLICT
    }

    enum CasDisposition {
        APPLIED,
        NOT_APPLIED,
        UNKNOWN
    }

    record CreateResult(CreateDisposition disposition, Optional<CreatedObjectToken> createdToken) {
        public CreateResult {
            java.util.Objects.requireNonNull(disposition, "disposition");
            java.util.Objects.requireNonNull(createdToken, "createdToken");
            boolean exact = disposition == CreateDisposition.APPLIED || disposition == CreateDisposition.EXISTING_EXACT;
            if (exact != createdToken.isPresent()) {
                throw new IllegalArgumentException("only exact NWKCP1 creation may issue a created-object token");
            }
        }
    }

    CompletionStage<CreateResult> conditionalCreateObject(String key, CanonicalBytes body, Sha256Digest bodyDigest);

    CompletionStage<Optional<CanonicalBytes>> readCreatedObject(CreatedObjectToken token);

    /** Strictly decodes one exact Head value and binds its selected Object identity to the supplied value SHA. */
    SelectedObjectToken selectObjectFromHead(
            String headKey, CanonicalBytes exactHeadValue, Sha256Digest exactHeadValueSha256);

    CompletionStage<Optional<CanonicalBytes>> readSelectedObject(SelectedObjectToken token, boolean recovery);

    CompletionStage<Optional<CanonicalBytes>> readHead(String key);

    /** The exact expected bytes (or exact absence) are the CAS predecessor and ABA fence. */
    CompletionStage<CasDisposition> compareAndSetHead(
            String key, Optional<CanonicalBytes> exactExpected, CanonicalBytes replacement);

    /** Production backend verifies every exact Root/Seal/physical-Head/page-chain closure fact. */
    default void verifyPhysicalClosure(
            WalRunTerminalClosureProofV1 proof,
            Nbke2RunBindingV1 expectedRunBinding,
            KafkaProtocolCheckpointStateV1 finalProtocolState) {
        throw new KafkaObjectCheckpointException("NWKCP1 backend has no physical closure verifier");
    }

    default void chargeDecoded(long contexts, long frames, long commitSets) {}

    default void chargeControlMetadata(long canonicalBytes) {}

    default void enterFallback() {}
}
