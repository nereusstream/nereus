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

import com.nereusstream.kafka.bookkeeper.broker.KafkaObjectNativeAppendV1;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.WalCheckpointHeadV1;
import com.nereusstream.storage.object.control.WalRunLifecycleManager;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunPredecessor;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import java.util.List;
import java.util.Optional;

/** Publishes only the deterministic terminal Kafka checkpoint of the recovered closed grant prefix. */
public final class KafkaObjectRecoveredProtocolClosureV1 {
    private KafkaObjectRecoveredProtocolClosureV1() {}

    public static WalRunPredecessor seal(
            WalRunObjectSession restored,
            CanonicalControlMetadataStore metadata,
            WalCheckpointHeadV1 physical,
            KafkaObjectAuthorizedRecoveryV1.Result recovered,
            long currentOwnerEpoch) {
        var state = recovered.state();
        recovered.tail().requireAuthorization(state);
        var root = restored.rootRecord();
        var oldRun = state.vector().runBinding();
        if (!oldRun.providerScopeId().equals(root.providerScopeId())
                || !oldRun.runId().value().equals(root.walRunSessionId())
                || currentOwnerEpoch < oldRun.creatorOwnerEpoch()) {
            throw new IllegalArgumentException("recovered Kafka terminal state differs from the native run");
        }
        String prefix = KafkaObjectNativeAppendV1.checkpointPrefix(root);
        var encoded = Nwkcp1CodecV1.encode(prefix, new Nwkcp1ObjectV1(restored.rootSha256(), List.of(state)));
        var closure = restored.prepareRecoveredProtocolClosure(
                physical, new ObjectIdentity(encoded.key(), encoded.length(), encoded.digest()));
        var backend = new StorageObjectNwkcp1BackendV1(restored, metadata, prefix, closure);
        var context = new KafkaNwkcp1WalRunContextV1(restored.rootSha256(), oldRun);
        var probe = new ObjectKafkaProtocolCheckpointStoreV1(prefix, context, currentOwnerEpoch, backend);
        var selected = probe.currentHead().toCompletableFuture().join();
        var store = selectPublisher(prefix, context, backend, probe, selected, currentOwnerEpoch);
        if (selected.filter(head -> head.state() == KafkaProtocolCheckpointHeadStateV1.TERMINAL)
                .isPresent()) {
            var terminal = selected.orElseThrow();
            if (!terminal.checkpointObjectKey().equals(encoded.key())
                    || terminal.checkpointObjectLength() != encoded.length()
                    || !terminal.checkpointObjectDigest().equals(encoded.digest())) {
                throw new IllegalStateException("existing terminal NWKCP1 differs from the recovered Kafka cut");
            }
        } else {
            store.publish(state).toCompletableFuture().join();
        }
        var seal = restored.sealRecovered(new WalRunLifecycleManager(metadata), physical);
        var terminal = store.terminalize(seal).toCompletableFuture().join();
        return new WalRunPredecessor(seal.root(), seal.sealKey(), seal.sealSha256(), Optional.of(terminal));
    }

    private static ObjectKafkaProtocolCheckpointStoreV1 selectPublisher(
            String prefix,
            KafkaNwkcp1WalRunContextV1 context,
            StorageObjectNwkcp1BackendV1 backend,
            ObjectKafkaProtocolCheckpointStoreV1 probe,
            Optional<KafkaProtocolCheckpointHeadV1> selected,
            long currentOwnerEpoch) {
        if (selected.isEmpty() || selected.orElseThrow().publisherEpoch() == currentOwnerEpoch) {
            return probe;
        }
        var old = selected.orElseThrow();
        if (old.publisherEpoch() > currentOwnerEpoch) {
            throw new IllegalStateException("native Object WAL terminal publisher has been superseded");
        }
        var store = new ObjectKafkaProtocolCheckpointStoreV1(prefix, context, old.publisherEpoch(), backend);
        if (old.state() != KafkaProtocolCheckpointHeadStateV1.TERMINAL) {
            store.takeover(currentOwnerEpoch).toCompletableFuture().join();
        }
        return store;
    }
}
