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

package com.nereusstream.storage.object.materialization;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationSourceCut;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.OutputPartPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PayloadKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.RepresentationMode;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceExtent;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/** Deterministic M5-A mode and immutable output-plan selection. */
public final class M5MaterializationPlannerV1 {
    /** Exact policy inputs selected outside the worker. */
    public record PlanningPolicy(
            PayloadKind rewritePayloadKind,
            boolean forceRewrite,
            Sha256Digest encryptionGenerationSha256,
            Sha256Digest compressionPolicySha256,
            Sha256Digest checksumPolicySha256,
            List<IndexKind> requiredIndexes,
            String exclusiveCellNamespace) {
        public PlanningPolicy {
            Objects.requireNonNull(rewritePayloadKind, "rewritePayloadKind");
            requireDigest(encryptionGenerationSha256, "encryptionGenerationSha256");
            requireDigest(compressionPolicySha256, "compressionPolicySha256");
            requireDigest(checksumPolicySha256, "checksumPolicySha256");
            requiredIndexes = List.copyOf(Objects.requireNonNull(requiredIndexes, "requiredIndexes"));
            List<IndexKind> sorted = requiredIndexes.stream()
                    .sorted(Comparator.comparingInt(Enum::ordinal))
                    .toList();
            if (!requiredIndexes.equals(sorted)
                    || requiredIndexes.stream().distinct().count() != requiredIndexes.size()
                    || requiredIndexes.size() > M5MaterializationRecordsV1.MAX_INDEXES) {
                throw new IllegalArgumentException("required indexes are not sorted unique within the M5 cap");
            }
            if (exclusiveCellNamespace == null
                    || exclusiveCellNamespace.isEmpty()
                    || exclusiveCellNamespace.endsWith("/")
                    || exclusiveCellNamespace.indexOf('\0') >= 0) {
                throw new IllegalArgumentException("exclusive Cell namespace is invalid");
            }
            if (rewritePayloadKind == PayloadKind.NATIVE_EXTENT_REFERENCE_V1) {
                throw new IllegalArgumentException("rewrite policy must name a concrete NMS1 payload kind");
            }
            if (rewritePayloadKind == PayloadKind.KAFKA_SEMANTIC_COMPACTED_V1) {
                throw new IllegalArgumentException("Kafka semantic compaction is owned by M5-B");
            }
        }
    }

    public MaterializationPlan plan(MaterializationSourceCut sourceCut, PlanningPolicy policy) {
        Objects.requireNonNull(sourceCut, "sourceCut");
        Objects.requireNonNull(policy, "policy");
        M5MaterializationCodecV1.encodeSourceCut(sourceCut);
        requireProtocolPayloadCompatibility(sourceCut, policy.rewritePayloadKind());
        requireSharedObjectIsolation(sourceCut);

        boolean payloadReusable = sourceCut.sources().stream().allMatch(this::payloadReusable);
        boolean indexesComplete = sourceCut.sources().stream().allMatch(SourceExtent::requiredIndexesPresent);
        RepresentationMode mode;
        PayloadKind payloadKind;
        if (!policy.forceRewrite() && payloadReusable && indexesComplete) {
            mode = RepresentationMode.REFERENCE_REUSE;
            payloadKind = PayloadKind.NATIVE_EXTENT_REFERENCE_V1;
        } else if (!policy.forceRewrite() && payloadReusable) {
            mode = RepresentationMode.INDEX_ONLY_GENERATION;
            payloadKind = PayloadKind.NATIVE_EXTENT_REFERENCE_V1;
        } else {
            mode = RepresentationMode.REWRITE_GENERATION;
            payloadKind = policy.rewritePayloadKind();
        }

        Sha256Digest taskId = M5MaterializationCodecV1.calculateTaskId(sourceCut);
        List<OutputPartPlan> parts = outputParts(sourceCut, policy, mode, payloadKind, taskId);
        List<IndexPlan> indexes =
                mode == RepresentationMode.REFERENCE_REUSE ? List.of() : indexPlans(sourceCut, policy, taskId);
        Sha256Digest outputIdentity = M5MaterializationCodecV1.calculateOutputIdentity(
                sourceCut,
                mode,
                payloadKind,
                taskId,
                policy.encryptionGenerationSha256(),
                policy.compressionPolicySha256(),
                policy.checksumPolicySha256(),
                parts,
                indexes);
        return new MaterializationPlan(
                sourceCut,
                mode,
                payloadKind,
                taskId,
                outputIdentity,
                policy.encryptionGenerationSha256(),
                policy.compressionPolicySha256(),
                policy.checksumPolicySha256(),
                parts,
                indexes);
    }

    private List<OutputPartPlan> outputParts(
            MaterializationSourceCut cut,
            PlanningPolicy policy,
            RepresentationMode mode,
            PayloadKind kind,
            Sha256Digest taskId) {
        List<OutputPartPlan> result = new ArrayList<>(cut.sources().size());
        for (int ordinal = 0; ordinal < cut.sources().size(); ordinal++) {
            SourceExtent source = cut.sources().get(ordinal);
            String key = mode == RepresentationMode.REWRITE_GENERATION
                    ? String.format(
                            Locale.ROOT,
                            "%s/m5/materialization/%s/payload/%06d.nms1",
                            policy.exclusiveCellNamespace(),
                            taskId.toHex(),
                            ordinal)
                    : source.physicalKey();
            Sha256Digest planSha = hash(
                    "M5-A-PART-V1",
                    taskId.toHex(),
                    Integer.toString(ordinal),
                    source.sourceIdentitySha256().toHex(),
                    kind.name(),
                    Long.toString(source.coverage().inclusiveStart()),
                    Long.toString(source.coverage().exclusiveEnd()),
                    key);
            result.add(new OutputPartPlan(ordinal, source.coverage(), kind, planSha, key));
        }
        return List.copyOf(result);
    }

    private List<IndexPlan> indexPlans(MaterializationSourceCut cut, PlanningPolicy policy, Sha256Digest taskId) {
        List<IndexPlan> result = new ArrayList<>(policy.requiredIndexes().size());
        for (IndexKind kind : policy.requiredIndexes()) {
            String key = String.format(
                    Locale.ROOT,
                    "%s/m5/materialization/%s/index/%02d.idx",
                    policy.exclusiveCellNamespace(),
                    taskId.toHex(),
                    kind.ordinal());
            Sha256Digest planSha = hash(
                    "M5-A-INDEX-V1",
                    taskId.toHex(),
                    kind.name(),
                    cut.coverage().domain().name(),
                    Long.toString(cut.coverage().inclusiveStart()),
                    Long.toString(cut.coverage().exclusiveEnd()),
                    key);
            result.add(new IndexPlan(kind, cut.coverage(), 1, planSha, key));
        }
        return List.copyOf(result);
    }

    private boolean payloadReusable(SourceExtent source) {
        return source.kind() != SourceKind.BOOKKEEPER_LEDGER && source.payloadLongLivedReadable();
    }

    private void requireSharedObjectIsolation(MaterializationSourceCut cut) {
        Sha256Digest bindingId = cut.identity().binding().bindingId().digest();
        for (SourceExtent source : cut.sources()) {
            if (!source.memberBindingIds().contains(bindingId)) {
                throw new IllegalArgumentException("source extent does not name the materialized Binding");
            }
            if (source.sharedPhysicalObject()
                    && source.kind() != SourceKind.OBJECT_WAL_NWG1
                    && source.kind() != SourceKind.PULSAR_NPD1_DATA) {
                throw new IllegalArgumentException("shared physical source kind is not admitted for reuse");
            }
        }
    }

    private void requireProtocolPayloadCompatibility(MaterializationSourceCut cut, PayloadKind rewriteKind) {
        if (cut.coverage().domain() == PositionDomain.KAFKA_OFFSET
                && rewriteKind != PayloadKind.KAFKA_BATCH_PRESERVING_V1) {
            throw new IllegalArgumentException("Kafka source cut requires byte-preserving Kafka M5-A output");
        }
        if (cut.coverage().domain() == PositionDomain.PULSAR_ENTRY
                && rewriteKind != PayloadKind.PULSAR_ENTRY_PRESERVING_V1) {
            throw new IllegalArgumentException("Pulsar source cut requires entry-preserving M5-A output");
        }
    }

    private static Sha256Digest hash(String domain, String... values) {
        String joined = domain + "\0" + String.join("\0", values);
        return Sha256Digest.hash(CanonicalBytes.copyOf(joined.getBytes(StandardCharsets.UTF_8)));
    }

    private static void requireDigest(Sha256Digest digest, String label) {
        Objects.requireNonNull(digest, label);
        if (digest.isZero()) {
            throw new IllegalArgumentException(label + " is the zero digest");
        }
    }
}
