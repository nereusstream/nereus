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

import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.BindingTrimFrontierV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorSnapshotV1;
import java.util.Objects;
import java.util.Optional;

/** Pure M5-C logical-trim planner. It never releases, retires, or deletes a physical source. */
public final class M5RetentionPlannerV1 {
    public BindingTrimFrontierV1 plan(RetentionFloorSnapshotV1 snapshot, Optional<BindingTrimFrontierV1> current) {
        Objects.requireNonNull(snapshot, "snapshot");
        Optional<BindingTrimFrontierV1> predecessor = Objects.requireNonNull(current, "current");
        M5RetentionCodecV1.encodeSnapshot(snapshot);
        if (snapshot.pulsarContext().isPresent()) {
            var context = snapshot.pulsarContext().orElseThrow();
            predecessor.ifPresent(value -> {
                var prior = value.pulsarContext()
                        .orElseThrow(() -> new IllegalArgumentException("typed trim predecessor has scalar coverage"));
                if (!value.identity().binding().equals(snapshot.identity().binding())
                        || !value.retentionPolicyRootSha256().equals(snapshot.retentionPolicyRootSha256())
                        || !value.capability().equals(snapshot.identity().capability())
                        || !prior.coverage().equals(context.coverage())
                        || !prior.newFrontier().equals(context.priorTrimFrontier())) {
                    throw new IllegalArgumentException("typed trim predecessor changes Binding/policy/chain/frontier");
                }
            });
            if (predecessor.isEmpty()) {
                var first = context.coverage().chain().get(0);
                if (!context.priorTrimFrontier()
                        .equals(new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                                first.ledgerId(), 0))) {
                    throw new IllegalArgumentException(
                            "initial typed trim predecessor is not the captured chain start");
                }
            }
            return new BindingTrimFrontierV1(
                    snapshot.identity(),
                    snapshot.domain(),
                    -1,
                    -1,
                    snapshot.retentionPolicyRootSha256(),
                    snapshot.snapshotRootSha256(),
                    snapshot.ownerFence(),
                    snapshot.storageFence(),
                    predecessor
                            .map(value -> Math.addExact(value.generation(), 1))
                            .orElse(1L),
                    snapshot.identity().capability(),
                    Optional.of(new M5RetentionRecordsV1.PulsarTrimContextV2(
                            context.coverage(), context.priorTrimFrontier(), snapshot.minimumPulsarFloor())));
        }
        if (predecessor.stream().anyMatch(value -> value.pulsarContext().isPresent())) {
            throw new IllegalArgumentException("scalar floor snapshot cannot replace a typed trim frontier");
        }
        long prior = predecessor.map(BindingTrimFrontierV1::newFrontier).orElse(0L);
        long generation =
                predecessor.map(value -> Math.addExact(value.generation(), 1)).orElse(1L);
        if (snapshot.priorTrimFrontier() != prior) {
            throw new IllegalArgumentException("floor snapshot trim predecessor differs from current authority");
        }
        predecessor.ifPresent(value -> {
            if (!value.identity().binding().equals(snapshot.identity().binding())
                    || value.domain() != snapshot.domain()
                    || !value.retentionPolicyRootSha256().equals(snapshot.retentionPolicyRootSha256())
                    || !value.capability().equals(snapshot.identity().capability())) {
                throw new IllegalArgumentException(
                        "logical trim predecessor changes identity, domain, policy, or capability");
            }
        });
        long minimum = snapshot.minimumSafeFloor();
        if (minimum < prior) {
            throw new IllegalArgumentException(
                    "newly observed retention floor would regress the logical trim frontier");
        }
        return new BindingTrimFrontierV1(
                snapshot.identity(),
                snapshot.domain(),
                prior,
                minimum,
                snapshot.retentionPolicyRootSha256(),
                snapshot.snapshotRootSha256(),
                snapshot.ownerFence(),
                snapshot.storageFence(),
                generation,
                snapshot.identity().capability());
    }
}
