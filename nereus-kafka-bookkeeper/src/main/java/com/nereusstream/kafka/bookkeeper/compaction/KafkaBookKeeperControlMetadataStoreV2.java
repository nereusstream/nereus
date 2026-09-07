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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.ProviderKind;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlKeysV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Typed BK task/part/descriptor records composed with the raw Binding M4/history envelope view.
 * The owner supplies stores for the same admitted native Cell route; provisioning/owner authority is separate.
 * M4ReadControlCoordinatorV1 owns the selector projection, so the control delegate must expose raw envelopes.
 */
public final class KafkaBookKeeperControlMetadataStoreV2 implements CanonicalControlMetadataStore {
    private static final String PREFIX = "v2/kafka-bk-compaction-v2/";
    private static final Pattern RECORD = Pattern.compile(PREFIX
            + "(?:[0-9a-f]{64}/task(?:/candidate|/terminal|/part/(?:0|[1-9][0-9]{0,2}))?"
            + "|views/[0-9a-f]{64}/descriptor)");
    private final CanonicalControlMetadataStore records;
    private final CanonicalControlMetadataStore control;
    private final BindingIdentity binding;
    private final Namespace namespace;
    private final CellProviderScopeId providerScope;
    private final String selectorKey;

    public KafkaBookKeeperControlMetadataStoreV2(
            CanonicalControlMetadataStore records,
            CanonicalControlMetadataStore control,
            int shardId,
            BindingIdentity binding,
            Namespace namespace,
            CellProviderScopeId providerScope) {
        this.records = Objects.requireNonNull(records, "records");
        this.control = Objects.requireNonNull(control, "control");
        this.binding = Objects.requireNonNull(binding, "binding");
        this.namespace = Objects.requireNonNull(namespace, "namespace");
        this.providerScope = Objects.requireNonNull(providerScope, "providerScope");
        this.selectorKey = new M4ReadControlKeysV1(shardId, binding).selector();
        if (namespace.providerKind() != ProviderKind.BOOKKEEPER) {
            throw new IllegalArgumentException("BK control route requires a BookKeeper namespace");
        }
    }

    public String selectorKey() {
        return selectorKey;
    }

    @Override
    public Optional<CanonicalBytes> get(String key) {
        if (!recordKey(key)) {
            return control.get(key);
        }
        return records.get(key).map(value -> {
            verify(key, value);
            return value;
        });
    }

    @Override
    public ControlMutationOutcome putIfAbsent(String key, CanonicalBytes value) {
        if (!recordKey(key)) {
            return control.putIfAbsent(key, value);
        }
        verify(key, value);
        return records.putIfAbsent(key, value);
    }

    @Override
    public ControlMutationOutcome compareAndSet(String key, Optional<CanonicalBytes> expected, CanonicalBytes value) {
        if (!recordKey(key)) {
            return control.compareAndSet(key, expected, value);
        }
        if (Objects.requireNonNull(expected, "expected").isPresent()) {
            throw new IllegalArgumentException("typed BK compaction records are immutable create-only values");
        }
        return putIfAbsent(key, value);
    }

    private static boolean recordKey(String key) {
        Objects.requireNonNull(key, "key");
        if (!key.startsWith(PREFIX)) {
            return false;
        }
        if (key.length() > 512
                || !RECORD.matcher(key).matches()
                || (key.contains("/part/") && Integer.parseInt(key.substring(key.lastIndexOf('/') + 1)) >= 256)) {
            throw new IllegalArgumentException("noncanonical BK compaction record key");
        }
        return true;
    }

    private KafkaBookKeeperInventoryV2.Task task(CanonicalBytes bytes) {
        var task = KafkaBookKeeperInventoryCodecV2.decodeTask(bytes);
        var cut = M5MaterializationCodecV1.decodeSourceCut(task.sourceCut());
        if (!cut.identity().binding().equals(binding)
                || !task.namespace().equals(namespace)
                || !task.capability().providerScopeId().equals(providerScope)) {
            throw new IllegalArgumentException("BK task Binding, namespace or provider scope differs from its route");
        }
        return task;
    }

    private KafkaBookKeeperInventoryV2.Task readTask(Sha256Digest id) {
        var task = task(records.get(KafkaBookKeeperInventoryV2.taskKey(id))
                .orElseThrow(() -> new IllegalStateException("BK record lacks its immutable task")));
        if (!task.taskIdSha256().equals(id)) {
            throw new IllegalArgumentException("BK record task content address differs");
        }
        return task;
    }

    private KafkaSealedBookKeeperDescriptorV2 descriptor(CanonicalBytes bytes) {
        var descriptor = KafkaSealedBookKeeperDescriptorCodecV2.decode(bytes);
        task(KafkaBookKeeperInventoryCodecV2.encodeTask(descriptor.task()));
        if (!readTask(descriptor.task().taskIdSha256()).equals(descriptor.task())) {
            throw new IllegalArgumentException("BK descriptor differs from its immutable task");
        }
        return descriptor;
    }

    private void verify(String key, CanonicalBytes bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.isEmpty() || bytes.length() > KafkaBookKeeperInventoryV2.MAX_TASK_BYTES) {
            throw new IllegalArgumentException("typed BK record exceeds its value bound");
        }
        if (key.endsWith("/task")) {
            if (!KafkaBookKeeperInventoryV2.taskKey(task(bytes).taskIdSha256()).equals(key)) {
                throw new IllegalArgumentException("BK task key differs from its content address");
            }
        } else if (key.endsWith("/terminal")) {
            var terminal = KafkaBookKeeperTaskTerminalV2.decode(bytes);
            if (!readTask(terminal.task().taskIdSha256()).equals(terminal.task())
                    || !key.equals(
                            KafkaBookKeeperTaskTerminalV2.key(terminal.task().taskIdSha256()))
                    || !control.get(terminal.selection().key())
                            .equals(Optional.of(terminal.selection().encode()))) {
                throw new IllegalArgumentException(
                        "BK terminal lacks its immutable task and exact cancelled selection archive");
            }
        } else if (key.contains("/part/")) {
            var part = KafkaBookKeeperInventoryCodecV2.decodePart(bytes);
            var task = readTask(part.taskIdSha256());
            var configuration = task.configuration(part.ordinal());
            if (!KafkaBookKeeperInventoryV2.partKey(part.taskIdSha256(), part.ordinal())
                            .equals(key)
                    || !part.resource().namespace().equals(namespace)
                    || !part.handle().providerScopeId().equals(configuration.providerScopeId())
                    || !part.handle().runId().equals(configuration.runId())
                    || !part.handle().configurationDigest().equals(configuration.configurationDigest())) {
                throw new IllegalArgumentException("BK part key, namespace or exact task configuration differs");
            }
        } else if (key.endsWith("/descriptor")) {
            var descriptor = descriptor(bytes);
            if (!KafkaBookKeeperCompactionPublicationV2.descriptorKey(descriptor.descriptorSha256())
                    .equals(key)) {
                throw new IllegalArgumentException("BK descriptor key differs from its content address");
            }
        } else {
            var digest = Sha256Digest.copyOf(bytes.toByteArray());
            var descriptor = descriptor(records.get(KafkaBookKeeperCompactionPublicationV2.descriptorKey(digest))
                    .orElseThrow(() -> new IllegalStateException("BK candidate lacks its immutable descriptor")));
            if (!descriptor.descriptorSha256().equals(digest)
                    || !KafkaBookKeeperCompactionPublicationV2.candidateKey(
                                    descriptor.task().taskIdSha256())
                            .equals(key)) {
                throw new IllegalArgumentException("BK candidate descriptor or task differs from its key");
            }
        }
    }
}
