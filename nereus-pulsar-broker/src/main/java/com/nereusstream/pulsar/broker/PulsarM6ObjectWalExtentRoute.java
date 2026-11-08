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

import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ActiveTailRecoveryRequest;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ExtentIdentity;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ExtentLocator;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ManifestHandoffRequest;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ManifestSource;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ObjectWalExtentStore;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.PlannedEntry;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ProductionObjectWalExtentStore;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.PublishResult;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.PulsarPosition;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ReadEntry;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.SealedExtentPlan;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.VerifiedActiveTailRecovery;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.VerifiedManifestHandoff;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.PulsarBindingKey;
import com.nereusstream.storage.object.control.WalLaneId;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/** Routes native reads by the exact captured Root, preserving the live writer's original candidate. */
final class PulsarM6ObjectWalExtentRoute implements ObjectWalExtentStore {
    private final PulsarM6ObjectWalRuns.Fresh fresh;
    private final ProductionObjectWalExtentStore active;
    private final PulsarM6ObjectWalHistory history;
    private final Executor io;

    PulsarM6ObjectWalExtentRoute(
            PulsarM6ObjectWalRuns.Fresh fresh,
            ProductionObjectWalExtentStore active,
            PulsarM6ObjectWalHistory history,
            Executor io) {
        this.fresh = fresh;
        this.active = active;
        this.history = history;
        this.io = io;
    }

    @Override
    public SealedExtentPlan sealPlan(WalLaneId lane, int policy, List<PlannedEntry> members, long maximumBytes) {
        return active.sealPlan(lane, policy, members, maximumBytes);
    }

    @Override
    public CompletionStage<PublishResult> publish(SealedExtentPlan plan) {
        return active.publish(plan);
    }

    @Override
    public CompletionStage<PublishResult> reconcile(SealedExtentPlan plan, Optional<ExtentIdentity> original) {
        return active.reconcile(plan, original);
    }

    @Override
    public CompletionStage<PublishResult> resumeSame(
            SealedExtentPlan plan, Optional<ExtentIdentity> original, List<PlannedEntry> unresolved) {
        return active.resumeSame(plan, original, unresolved);
    }

    @Override
    public CompletionStage<PublishResult> publishSuccessor(SealedExtentPlan previous, SealedExtentPlan successor) {
        return active.publishSuccessor(previous, successor);
    }

    @Override
    public CompletionStage<VerifiedManifestHandoff> verifyManifestHandoff(ManifestHandoffRequest request) {
        return active.verifyManifestHandoff(request);
    }

    @Override
    public CompletionStage<ReadEntry> readActive(ExtentLocator locator) {
        if (locator.identity()
                .walRunRootSha256()
                .equals(fresh.selected().reference().rootSha256())) {
            return active.readActive(locator);
        }
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return history.read(locator);
                    } catch (IOException failure) {
                        throw new CompletionException(failure);
                    }
                },
                io);
    }

    @Override
    public CompletionStage<ReadEntry> readManifest(
            PulsarBindingKey binding, PulsarPosition position, long generation, ManifestSource source) {
        return active.readManifest(binding, position, generation, source);
    }

    @Override
    public CompletionStage<VerifiedActiveTailRecovery> verifyRecoveryTail(ActiveTailRecoveryRequest request) {
        history.requireRecovered(request);
        return CompletableFuture.completedFuture(new VerifiedActiveTailRecovery(request));
    }
}
