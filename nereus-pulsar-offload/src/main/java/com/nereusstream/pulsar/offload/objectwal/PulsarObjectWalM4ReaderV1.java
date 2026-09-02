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

package com.nereusstream.pulsar.offload.objectwal;

import com.nereusstream.pulsar.offload.objectwal.PulsarObjectBindingReadAdapterV1.PhysicalRoute;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectBindingReadAdapterV1.ReadCell;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.PulsarPosition;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ReadEntry;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.PulsarBindingKey;
import com.nereusstream.storage.object.read.BindingReadAsyncExecutorV1;
import com.nereusstream.storage.object.read.BindingReadAuthorityV1;
import com.nereusstream.storage.object.read.BindingReadHazardPoolV1;
import com.nereusstream.storage.object.read.BindingReadHazardPoolV1.ScanOutcome;
import com.nereusstream.storage.object.read.BindingReadPlannerV1;
import com.nereusstream.storage.object.read.PulsarBindingReadPlanBufferV1;
import com.nereusstream.storage.object.read.PulsarBindingReadPlannerV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/** Actual P4 Object-WAL reader wrapped by one M4 generation lease and exact captured source plan. */
public final class PulsarObjectWalM4ReaderV1 {
    private final PulsarObjectWalBridgeV1 bridge;
    private final PulsarBindingKey binding;
    private final BindingReadHazardPoolV1 hazardPool;
    private final BindingReadAsyncExecutorV1 asyncExecutor;
    private final AtomicReference<BindingReadAuthorityV1> current = new AtomicReference<>();

    public PulsarObjectWalM4ReaderV1(
            PulsarObjectWalBridgeV1 bridge,
            PulsarBindingKey binding,
            BindingReadSelector initialSelector,
            BindingReadHazardPoolV1 hazardPool,
            Executor ownerEventLoop) {
        this.bridge = Objects.requireNonNull(bridge, "bridge");
        this.binding = Objects.requireNonNull(binding, "binding");
        this.hazardPool = Objects.requireNonNull(hazardPool, "hazardPool");
        asyncExecutor = new BindingReadAsyncExecutorV1(ownerEventLoop);
        refresh(initialSelector);
        bridge.registerM4RetirementGuard(
                binding,
                () -> hazardPool.scanBinding(PulsarObjectBindingReadAdapterV1.bindingIdentity(binding)
                                .bindingId())
                        == ScanOutcome.CLEAN);
    }

    /** Publishes a new cached cell after the owning P4 mutation has completed. */
    public BindingReadAuthorityV1 refresh(BindingReadSelector selector) {
        BindingReadAuthorityV1 authority = PulsarObjectBindingReadAdapterV1.publish(
                bridge.captureReadView(binding), Objects.requireNonNull(selector, "selector"));
        current.set(authority);
        return authority;
    }

    public AtomicReference<BindingReadAuthorityV1> currentAuthority() {
        return current;
    }

    public CompletableFuture<ReadEntry> read(PulsarPosition position) {
        Objects.requireNonNull(position, "position");
        if (position.entryId() == Long.MAX_VALUE) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Pulsar entry upper bound overflows"));
        }
        return asyncExecutor.execute(
                current,
                hazardPool,
                authority -> {
                    if (!(authority.publicationCell().protocolStateReference() instanceof ReadCell cell)) {
                        throw new IllegalStateException(
                                "captured Pulsar authority lacks its typed current-source cell");
                    }
                    PulsarObjectWalReadViewV1.LedgerView ledger = cell.view().requireLedger(position.virtualLedgerId());
                    PulsarBindingReadPlanBufferV1 plan = new PulsarBindingReadPlanBufferV1(1);
                    BindingReadPlannerV1.Outcome outcome = PulsarBindingReadPlannerV1.plan(
                            cell.routes(),
                            position.virtualLedgerId(),
                            position.entryId(),
                            Math.addExact(position.entryId(), 1),
                            Math.addExact(ledger.readableThrough(), 1),
                            plan);
                    if (outcome != BindingReadPlannerV1.Outcome.PLANNED || plan.size() != 1) {
                        throw new IllegalStateException("captured Pulsar source plan failed closed: " + outcome);
                    }
                    PhysicalRoute expected = cell.requirePhysical(plan.route(0));
                    return bridge.readCaptured(cell.view(), binding, position).thenApply(result -> {
                        if (!result.binding().equals(binding)
                                || !result.position().equals(position)
                                || result.source() != expected.source()) {
                            throw new IllegalStateException("P4 read result differs from the exact captured M4 route");
                        }
                        return result;
                    });
                },
                () -> bridge.reconcileM4Retirement(binding));
    }
}
