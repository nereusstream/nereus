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

package com.nereusstream.storage.api.kafka;

import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerRecoveryProofV1;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletionStage;

/** Low-frequency protocol admission; callers must validate the current native Controller assignment. */
public interface KafkaBookKeeperOwnerAuthorityV1 {
    CompletionStage<Optional<KafkaOwnerAdmissionV1>> readOwnerAdmission();

    /** Only bootstrap or an exact closed predecessor may grant a newer native owner. Never reopens a closed epoch. */
    CompletionStage<ProviderMutationResultV1<KafkaOwnerAdmissionV1>> openOwner(
            KafkaOwnerIdentityV1 owner, Optional<KafkaOwnerAdmissionV1> expectedClosed);

    /** Fixes the complete legally attached run set before any ledger fencing/recovery. Exact retries are stable. */
    CompletionStage<ProviderMutationResultV1<KafkaOwnerAdmissionV1>> closeOwner(KafkaOwnerIdentityV1 expectedOwner);

    CompletionStage<Optional<KafkaOwnerAdmissionV1>> readClosedOwner(long ownerEpoch);

    /** Exact admitted initial identity, including a prewrite whose admission response was lost. */
    CompletionStage<Optional<KafkaRunRootRecordV2>> readAdmittedRun(KafkaRunRootRecordV2.Link link);

    /** Closes a crash run at the independently verified fenced prefix, without fabricating a footer. */
    CompletionStage<ProviderMutationResultV1<KafkaRunRootSnapshotV1>> sealRecoveredRun(
            KafkaRunRootSnapshotV1 expectedActive,
            KafkaOwnerAdmissionV1 closedOwner,
            RunLedgerRecoveryProofV1 proof,
            long endOffset,
            OptionalLong inertFromEntryId);
}
