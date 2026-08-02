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

package com.nereusstream.kafka.compaction;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.Snapshot;
import com.nereusstream.kafka.compaction.KafkaCompactionPlanner.Candidate;
import com.nereusstream.materialization.ExactSourceSet;
import com.nereusstream.materialization.MaterializationTask;
import com.nereusstream.materialization.TaskKind;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatV2;
import com.nereusstream.objectstore.compacted.KafkaCompactionKeyEncodingV2;
import java.util.List;
import java.util.Objects;

/**
 * Durable semantic input for one Kafka compaction attempt.
 *
 * <p>The generic materialization task remains the workflow root for output coverage. This plan
 * links to that exact task and additionally freezes the full decision-horizon sources and Kafka
 * transaction/compatibility facts required to reproduce the two passes after restart.
 */
public record KafkaCompactionPlan(
        String planId,
        StreamId streamId,
        String materializationTaskId,
        long bindingMetadataVersion,
        long lastStableOffset,
        long highWatermark,
        Candidate candidate,
        ExactSourceSet decisionSources,
        int outputSourceCount,
        Checksum outputSourceSetSha256,
        Checksum materializationPolicySha256,
        Snapshot passOneSnapshot,
        Compatibility compatibility) {

    public KafkaCompactionPlan(
            String planId,
            StreamId streamId,
            String materializationTaskId,
            long bindingMetadataVersion,
            long lastStableOffset,
            long highWatermark,
            Candidate candidate,
            ExactSourceSet decisionSources,
            int outputSourceCount,
            Checksum outputSourceSetSha256,
            Checksum materializationPolicySha256,
            Snapshot passOneSnapshot,
            Compatibility compatibility) {
        this.streamId = Objects.requireNonNull(streamId, "streamId");
        this.materializationTaskId = requireText(materializationTaskId, "materializationTaskId");
        this.candidate = Objects.requireNonNull(candidate, "candidate");
        this.decisionSources = Objects.requireNonNull(decisionSources, "decisionSources");
        this.outputSourceSetSha256 = requireSha256(outputSourceSetSha256, "outputSourceSetSha256");
        this.materializationPolicySha256 = requireSha256(materializationPolicySha256, "materializationPolicySha256");
        this.passOneSnapshot = Objects.requireNonNull(passOneSnapshot, "passOneSnapshot");
        this.compatibility = Objects.requireNonNull(compatibility, "compatibility");
        this.bindingMetadataVersion = bindingMetadataVersion;
        this.lastStableOffset = lastStableOffset;
        this.highWatermark = highWatermark;
        this.outputSourceCount = outputSourceCount;

        if (bindingMetadataVersion < 0
                || !candidate.shouldCompact()
                || decisionSources.view() != ReadView.COMMITTED
                || !decisionSources.coverage().equals(candidate.decisionHorizon())
                || decisionSources.sources().stream()
                        .anyMatch(source -> source.payloadFormat() != PayloadFormat.KAFKA_RECORD_BATCH)
                || outputSourceCount <= 0
                || outputSourceCount > decisionSources.sources().size()
                || candidate.outputCoverage().endOffset() > lastStableOffset
                || lastStableOffset > highWatermark
                || highWatermark > candidate.decisionHorizon().endOffset()
                || !passOneSnapshot.outputCoverage().equals(candidate.outputCoverage())
                || !passOneSnapshot.decisionHorizon().equals(candidate.decisionHorizon())
                || passOneSnapshot.nowMillis() != candidate.evaluatedAtMillis()
                || passOneSnapshot.deleteRetentionMs() != candidate.policy().deleteRetentionMs()) {
            throw new IllegalArgumentException("invalid durable Kafka compaction plan facts");
        }
        ExactSourceSet outputSources = outputSources(decisionSources, outputSourceCount, candidate);
        if (!outputSources.sourceSetSha256().equals(outputSourceSetSha256)) {
            throw new IllegalArgumentException("Kafka compaction output source-set digest changed");
        }
        candidate.previousMandatoryCoverage().ifPresent(coverage -> {
            if (coverage.endOffset() != candidate.outputCoverage().startOffset()) {
                throw new IllegalArgumentException("Kafka compaction plan does not resume at mandatory coverage end");
            }
        });
        compatibility.requireCurrent();

        this.planId = requireText(planId, "planId");
        if (!KafkaCompactionPlanCodecV1.matchesPlanId(
                this.planId,
                streamId,
                materializationTaskId,
                bindingMetadataVersion,
                lastStableOffset,
                highWatermark,
                candidate,
                decisionSources,
                outputSourceCount,
                outputSourceSetSha256,
                materializationPolicySha256,
                passOneSnapshot,
                compatibility)) {
            throw new IllegalArgumentException("Kafka compaction planId is not canonical");
        }
    }

    public static KafkaCompactionPlan create(
            MaterializationTask outputTask,
            long bindingMetadataVersion,
            long lastStableOffset,
            long highWatermark,
            Candidate candidate,
            ExactSourceSet decisionSources,
            Snapshot passOneSnapshot) {
        MaterializationTask task = Objects.requireNonNull(outputTask, "outputTask");
        Candidate exactCandidate = Objects.requireNonNull(candidate, "candidate");
        ExactSourceSet exactDecisionSources = Objects.requireNonNull(decisionSources, "decisionSources");
        Snapshot exactSnapshot = Objects.requireNonNull(passOneSnapshot, "passOneSnapshot");
        Compatibility compatibility = Compatibility.current();
        int outputSourceCount = task.sources().size();
        String planId = KafkaCompactionPlanCodecV1.planIdFor(
                task.streamId(),
                task.taskId(),
                bindingMetadataVersion,
                lastStableOffset,
                highWatermark,
                exactCandidate,
                exactDecisionSources,
                outputSourceCount,
                task.sourceSetSha256(),
                task.policyDigestSha256(),
                exactSnapshot,
                compatibility);
        KafkaCompactionPlan plan = new KafkaCompactionPlan(
                planId,
                task.streamId(),
                task.taskId(),
                bindingMetadataVersion,
                lastStableOffset,
                highWatermark,
                exactCandidate,
                exactDecisionSources,
                outputSourceCount,
                task.sourceSetSha256(),
                task.policyDigestSha256(),
                exactSnapshot,
                compatibility);
        plan.requireMaterializationTask(task);
        return plan;
    }

    public ExactSourceSet outputSources() {
        return outputSources(decisionSources, outputSourceCount, candidate);
    }

    public void requireMaterializationTask(MaterializationTask task) {
        MaterializationTask exact = Objects.requireNonNull(task, "task");
        if (!exact.taskId().equals(materializationTaskId)
                || !exact.streamId().equals(streamId)
                || exact.view() != ReadView.TOPIC_COMPACTED
                || exact.taskKind() != TaskKind.TOPIC_KEY_COMPACTION
                || !exact.coverage().equals(candidate.outputCoverage())
                || !exact.sources().equals(outputSources().sources())
                || !exact.sourceSetSha256().equals(outputSourceSetSha256)
                || !exact.policyDigestSha256().equals(materializationPolicySha256)
                || exact.policy().topicCompaction().isEmpty()
                || !exact.policy().topicCompaction().orElseThrow().strategyId().equals(compatibility.strategyId())
                || exact.policy().topicCompaction().orElseThrow().strategyVersion() != compatibility.strategyVersion()
                || !exact.policy().topicCompaction().orElseThrow().keyCodecId().equals(compatibility.keyCodecId())) {
            throw new IllegalArgumentException("materialization task does not match the durable Kafka compaction plan");
        }
    }

    private static ExactSourceSet outputSources(
            ExactSourceSet decisionSources, int outputSourceCount, Candidate candidate) {
        List<com.nereusstream.materialization.SourceGeneration> prefix =
                decisionSources.sources().subList(0, outputSourceCount);
        if (prefix.get(prefix.size() - 1).range().endOffset()
                != candidate.outputCoverage().endOffset()) {
            throw new IllegalArgumentException("Kafka compaction output sources are not the exact decision prefix");
        }
        return ExactSourceSet.create(ReadView.COMMITTED, candidate.outputCoverage(), prefix);
    }

    private static Checksum requireSha256(Checksum value, String field) {
        Objects.requireNonNull(value, field);
        if (value.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException(field + " must use SHA256");
        }
        return value;
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    /**
     * Versioned compatibility identities included in the canonical plan id.
     */
    public record Compatibility(
            String strategyId,
            long strategyVersion,
            String keyCodecId,
            String rewriteCodecId,
            Checksum messageFormatSha256) {
        public Compatibility {
            strategyId = requireText(strategyId, "strategyId");
            keyCodecId = requireText(keyCodecId, "keyCodecId");
            rewriteCodecId = requireText(rewriteCodecId, "rewriteCodecId");
            messageFormatSha256 = requireSha256(messageFormatSha256, "messageFormatSha256");
            if (strategyVersion <= 0) {
                throw new IllegalArgumentException("strategyVersion must be positive");
            }
        }

        public static Compatibility current() {
            return new Compatibility(
                    KafkaCompactionStrategyV1.STRATEGY_ID,
                    KafkaCompactionStrategyV1.STRATEGY_VERSION,
                    KafkaCompactionKeyEncodingV2.ID,
                    CompactedObjectFormatV2.KAFKA_REWRITE_CODEC,
                    KafkaTopicCompactionCodecV1.MESSAGE_FORMAT_SHA256);
        }

        void requireCurrent() {
            if (!equals(current())) {
                throw new IllegalArgumentException("Kafka compaction plan uses an unsupported compatibility tuple");
            }
        }
    }
}
