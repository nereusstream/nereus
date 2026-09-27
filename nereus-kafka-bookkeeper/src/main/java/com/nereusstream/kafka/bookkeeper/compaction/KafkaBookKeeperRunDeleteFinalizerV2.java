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

import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.kafka.KafkaRunRootAuthority;
import com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootStateV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteDoneV2;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Post-delete completion for one exact sealed BK run; the caller must supply a fully qualified M5 intent. */
public final class KafkaBookKeeperRunDeleteFinalizerV2 {
    private final M5TargetDeleteAuthorityCoordinatorV1 deletes;
    private final M5GcQuotaCoordinatorV2 quota;
    private final KafkaRunRootAuthority roots;

    public KafkaBookKeeperRunDeleteFinalizerV2(
            M5TargetDeleteAuthorityCoordinatorV1 deletes, M5GcQuotaCoordinatorV2 quota, KafkaRunRootAuthority roots) {
        this.deletes = Objects.requireNonNull(deletes, "deletes");
        this.quota = Objects.requireNonNull(quota, "quota");
        this.roots = Objects.requireNonNull(roots, "roots");
    }

    /**
     * Native absence and current eligibility establish DONE; only then may the root retire and its quota settle.
     * Each step is durable and may be retried with the same exact intent after a lost response or process restart.
     */
    public CompletionStage<KafkaRunRootSnapshotV1> finishAbsent(
            VersionedValue exactIntent, KafkaRunRootSnapshotV1 exactSealed) {
        Objects.requireNonNull(exactIntent, "exactIntent");
        Objects.requireNonNull(exactSealed, "exactSealed");
        var intent = M5TargetDeleteAuthorityCodecV1.decodeAuthority(exactIntent.canonicalStoredBytes());
        var resource = intent.target().resourceId();
        if (!(resource instanceof PhysicalResourceIdV2.BookKeeperLedger ledger)
                || intent.state() != TargetDeleteAuthorityStateV1.DELETE_INTENT_V1
                || !exactIntent.key().equals(resource.authorityKey())
                || exactSealed.state() != KafkaRunRootStateV1.SEALED
                || exactSealed.ledgerIdentity().ledgerId() != ledger.ledgerId()) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("run delete intent and sealed root differ"));
        }
        return deletes.completeAbsent(exactIntent)
                .thenCompose(done -> {
                    if (!done.exactTerminalIsAuthoritative()) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("exact physical DELETE_DONE is unresolved"));
                    }
                    var stored = done.observed().orElseThrow();
                    if (M5TargetDeleteDoneV2.isCompactDone(stored.canonicalStoredBytes())) {
                        return CompletableFuture.completedFuture(null);
                    }
                    return deletes.compactDone(stored).thenApply(compacted -> {
                        if (!compacted.exactCandidateIsAuthoritative()) {
                            throw new IllegalStateException("compact physical DELETE_DONE is unresolved");
                        }
                        return null;
                    });
                })
                .thenCompose(ignored -> roots.retireDeletedRoot(exactSealed))
                .thenCompose(retired -> {
                    if (!retired.exactProof().equals(java.util.Optional.of(exactSealed))) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("exact run-root retirement is unresolved"));
                    }
                    return roots.isDurablyRetired(exactSealed);
                })
                .thenCompose(retired -> {
                    if (!retired) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("exact run-root retirement marker is absent"));
                    }
                    return quota.settle(resource);
                })
                .thenApply(settled -> {
                    if (settled != M5GcQuotaCoordinatorV2.Result.SETTLED) {
                        throw new IllegalStateException("physical delete quota settlement is unresolved");
                    }
                    return exactSealed;
                });
    }
}
