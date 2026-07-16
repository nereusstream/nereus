/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.ReadView;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Chooses the exact completed-trim or view-specific replacement proof for a higher source. */
final class HigherGenerationRetirementEligibilityVerifier {
    private final CompletedTrimRetirementVerifier trim;
    private final HigherGenerationRecoveryCoverageVerifier committed;
    private final TopicCompactedReplacementVerifier topicCompacted;

    HigherGenerationRetirementEligibilityVerifier(
            CompletedTrimRetirementVerifier trim,
            HigherGenerationRecoveryCoverageVerifier committed,
            TopicCompactedReplacementVerifier topicCompacted) {
        this.trim = Objects.requireNonNull(trim, "trim");
        this.committed = Objects.requireNonNull(committed, "committed");
        this.topicCompacted = Objects.requireNonNull(
                topicCompacted, "topicCompacted");
    }

    CompletableFuture<EligibilityProof> prove(
            GcReferenceQuery query,
            VersionedGenerationIndex source) {
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(source, "source");
        return trim.proveIfCompleted(source).thenCompose(trimProof -> {
            if (trimProof.isPresent()) {
                return CompletableFuture.completedFuture(
                        new CompletedTrimEligibility(
                                source, trimProof.orElseThrow()));
            }
            ReadView view = ReadView.fromWireId(source.value().readViewId());
            if (view == ReadView.COMMITTED) {
                return committed.prove(query, source).thenApply(proof ->
                        new CommittedReplacementEligibility(source, proof));
            }
            return topicCompacted.prove(query, source).thenApply(proof ->
                    new TopicCompactedReplacementEligibility(source, proof));
        });
    }

    sealed interface EligibilityProof permits
            CompletedTrimEligibility,
            CommittedReplacementEligibility,
            TopicCompactedReplacementEligibility {
        VersionedGenerationIndex source();
    }

    record CompletedTrimEligibility(
            VersionedGenerationIndex source,
            CompletedTrimRetirementVerifier.CompletedTrimProof proof)
            implements EligibilityProof {
        CompletedTrimEligibility {
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(proof, "proof");
            if (!proof.source().equals(source)) {
                throw new IllegalArgumentException(
                        "completed-trim eligibility source does not match its proof");
            }
        }
    }

    record CommittedReplacementEligibility(
            VersionedGenerationIndex source,
            HigherGenerationRecoveryCoverageVerifier.CoverageProof proof)
            implements EligibilityProof {
        CommittedReplacementEligibility {
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(proof, "proof");
            if (!proof.source().equals(source)) {
                throw new IllegalArgumentException(
                        "COMMITTED replacement eligibility source does not match its proof");
            }
        }
    }

    record TopicCompactedReplacementEligibility(
            VersionedGenerationIndex source,
            TopicCompactedReplacementVerifier.ReplacementProof proof)
            implements EligibilityProof {
        TopicCompactedReplacementEligibility {
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(proof, "proof");
            if (!proof.source().equals(source)) {
                throw new IllegalArgumentException(
                        "TOPIC_COMPACTED replacement eligibility source does not match its proof");
            }
        }
    }
}
