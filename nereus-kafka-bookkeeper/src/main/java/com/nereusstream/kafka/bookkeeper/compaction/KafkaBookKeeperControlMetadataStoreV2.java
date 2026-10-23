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
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlKeysV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProtectionState;
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
            + "(?:[0-9a-f]{64}/task(?:/candidate|/terminal|/cache-budget|/part/(?:0|[1-9][0-9]{0,2}))?"
            + "|views/[0-9a-f]{64}/(?:descriptor|publication-targets|replacement-semantics"
            + "|native-read-scope/[1-9][0-9]{0,18}/[1-9][0-9]{0,18}"
            + "|native-recovery-scope/[1-9][0-9]{0,18}/[1-9][0-9]{0,18}))");
    private final CanonicalControlMetadataStore records;
    private final CanonicalControlMetadataStore control;
    private final BindingIdentity binding;
    private final Namespace namespace;
    private final CellProviderScopeId providerScope;
    private final String selectorKey;
    private final M4ReadControlKeysV1 controlKeys;

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
        this.controlKeys = new M4ReadControlKeysV1(shardId, binding);
        this.selectorKey = controlKeys.selector();
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
        } else if (key.endsWith("/cache-budget")) {
            var budget = KafkaBookKeeperSelectedCacheBudgetV1.decode(bytes);
            budget.requireTask(readTask(budget.taskReservation().compactionTaskIdSha256()));
            if (!budget.key().equals(key)) {
                throw new IllegalArgumentException("selected cache budget differs from its immutable Task key");
            }
            records.get(KafkaBookKeeperCompactionPublicationV2.descriptorKey(budget.descriptorSha256()))
                    .ifPresent(value -> budget.requireDescriptor(descriptor(value)));
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
        } else if (key.endsWith("/publication-targets")) {
            var targets = KafkaBookKeeperPublicationTargetsV2.decode(bytes);
            var task = readTask(targets.taskId());
            if (!targets.key().equals(key) || !targets.sourceCutSha256().equals(Sha256Digest.hash(task.sourceCut()))) {
                throw new IllegalArgumentException("original publication targets differ from their native task route");
            }
            for (int ordinal = 0; ordinal < task.parts().size(); ordinal++) {
                var part = KafkaBookKeeperInventoryCodecV2.decodePart(get(KafkaBookKeeperInventoryV2.partKey(
                                task.taskIdSha256(), ordinal))
                        .orElseThrow(() -> new IllegalStateException("publication targets lack an output inventory")));
                if (!targets.resources().contains(part.resource())) {
                    throw new IllegalArgumentException("original publication targets omit an output part");
                }
            }
        } else if (key.endsWith("/replacement-semantics")) {
            var comparison = KafkaBookKeeperReplacementSemanticsV2.decode(bytes);
            var task = readTask(comparison.taskId());
            var targets = get(KafkaBookKeeperPublicationTargetsV2.key(comparison.descriptorId()))
                    .orElseThrow(() -> new IllegalStateException("replacement semantics lack original native members"));
            if (!key.equals(KafkaBookKeeperReplacementSemanticsV2.key(comparison.descriptorId()))
                    || !comparison.sourceCutSha256().equals(Sha256Digest.hash(task.sourceCut()))
                    || !comparison.publicationTargetsSha256().equals(Sha256Digest.hash(targets))
                    || !KafkaBookKeeperPublicationTargetsV2.decode(targets)
                            .taskId()
                            .equals(comparison.taskId())) {
                throw new IllegalArgumentException("replacement semantics differ from the native task and membership");
            }
            records.get(KafkaBookKeeperCompactionPublicationV2.descriptorKey(comparison.descriptorId()))
                    .ifPresent(value -> comparison.requireDescriptor(descriptor(value)));
        } else if (key.contains("/native-read-scope/")) {
            verifyReadScope(key, bytes);
        } else if (key.contains("/native-recovery-scope/")) {
            var scope = KafkaBookKeeperNativeRecoveryScopeV2.decode(bytes);
            var selected =
                    descriptor(records.get(KafkaBookKeeperCompactionPublicationV2.descriptorKey(scope.descriptorId()))
                            .orElseThrow(() ->
                                    new IllegalStateException("native recovery scope lacks its selected descriptor")));
            Optional<KafkaSealedBookKeeperDescriptorV2> previous = Optional.empty();
            var first = selected.sourceCut().sources().get(0);
            if (first.kind()
                    == com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind
                            .KAFKA_BK_COMPACTED_GENERATION_V2) {
                previous = Optional.of(descriptor(
                        records.get(KafkaBookKeeperCompactionPublicationV2.descriptorKey(first.formatRootSha256()))
                                .orElseThrow(() -> new IllegalStateException(
                                        "native recovery scope lacks its immutable predecessor descriptor"))));
            }
            scope.requireDescriptor(selected, previous);
            if (!key.equals(scope.key())) {
                throw new IllegalArgumentException("native recovery scope key differs from its exact owner/read epoch");
            }
        } else if (key.endsWith("/descriptor")) {
            var descriptor = descriptor(bytes);
            if (!KafkaBookKeeperCompactionPublicationV2.descriptorKey(descriptor.descriptorSha256())
                    .equals(key)) {
                throw new IllegalArgumentException("BK descriptor key differs from its content address");
            }
            records.get(KafkaBookKeeperReplacementSemanticsV2.key(descriptor.descriptorSha256()))
                    .ifPresent(value ->
                            KafkaBookKeeperReplacementSemanticsV2.decode(value).requireDescriptor(descriptor));
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

    private void verifyReadScope(String key, CanonicalBytes bytes) {
        var scope = KafkaBookKeeperNativeReadScopeV2.decode(bytes);
        var descriptor = descriptor(records.get(
                        KafkaBookKeeperCompactionPublicationV2.descriptorKey(scope.descriptorId()))
                .orElseThrow(() -> new IllegalStateException("native read scope lacks its selected descriptor")));
        var previous = scope.predecessorDescriptorId()
                .map(id -> descriptor(records.get(KafkaBookKeeperCompactionPublicationV2.descriptorKey(id))
                        .orElseThrow(() -> new IllegalStateException(
                                "native read scope lacks its immutable predecessor descriptor"))));
        scope.requireDescriptor(descriptor, previous);
        if (!key.equals(scope.key())) {
            throw new IllegalArgumentException("native read scope key differs from its exact epochs");
        }
        var terminalBytes =
                control.get(controlKeys.terminal(scope.closedReadEpoch())).orElseThrow();
        var terminal = M4ReadControlCodecV1.decodeTerminal(terminalBytes);
        if (!terminal.binding().equals(binding)
                || terminal.ownerEpoch() != scope.owner().ownerEpoch()
                || !Sha256Digest.hash(terminalBytes).equals(scope.terminalSha256())
                || !control.get(controlKeys.proof(scope.closedReadEpoch()))
                        .map(Sha256Digest::hash)
                        .equals(Optional.of(scope.proofSha256()))) {
            throw new IllegalArgumentException("native read scope differs from its authoritative M4 drain");
        }
        for (var source : descriptor.sourceCut().sources()) {
            var protection = M4ReadControlCodecV1.decodeProtection(
                    control.get(controlKeys.protection(source.sourceIdentitySha256(), descriptor.sourceGeneration()))
                            .orElseThrow());
            if (!protection.binding().equals(binding)
                    || protection.state() != ProtectionState.RELEASED
                    || !protection.releasedByBatchSha256().equals(Optional.of(scope.batchId()))) {
                throw new IllegalArgumentException("native read scope lacks exact M4 RELEASED membership");
            }
        }
    }
}
