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
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationTask;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PublicationOutcome;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.TaskState;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1.ValidatedGeneration;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Persists immutable M5-A values and delegates the sole mutable read publication to the exact M4 selector CAS. */
public final class M5MaterializationCoordinatorV1 {
    private final CanonicalControlMetadataStore metadata;
    private final M5MaterializationKeysV1 keys;
    private final M4ReadControlCoordinatorV1 m4;
    private final BindingIdentity binding;

    public M5MaterializationCoordinatorV1(
            CanonicalControlMetadataStore metadata, int shardId, BindingIdentity binding) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.binding = Objects.requireNonNull(binding, "binding");
        this.keys = new M5MaterializationKeysV1(shardId, binding);
        this.m4 = new M4ReadControlCoordinatorV1(metadata, shardId, binding);
    }

    /** Creates immutable source cut and PLANNED task. Unknown outcomes are reconciled by exact reread. */
    public PublicationOutcome register(MaterializationPlan plan) {
        requireBinding(plan);
        CanonicalBytes cutBytes = M5MaterializationCodecV1.encodeSourceCut(plan.sourceCut());
        Sha256Digest cutSha = M5MaterializationCodecV1.sourceCutSha256(plan.sourceCut());
        MaterializationTask planned = new MaterializationTask(
                plan.taskIdSha256(),
                TaskState.PLANNED,
                cutSha,
                plan.outputIdentitySha256(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        PublicationOutcome cut = reconcileCreate(keys.sourceCut(plan.taskIdSha256()), cutBytes);
        if (cut != PublicationOutcome.APPLIED_EXACT && cut != PublicationOutcome.EXISTING_EXACT) {
            return cut;
        }
        return reconcileCreate(keys.task(plan.taskIdSha256()), M5MaterializationCodecV1.encodeTask(planned));
    }

    /**
     * Publishes immutable validation/generation/manifest first, then executes exactly one M4 selector CAS. Task state
     * is audit/recovery state and never substitutes for that selector.
     */
    public PublicationOutcome publish(
            MaterializationPlan plan,
            ValidatedGeneration validated,
            List<SourceProtectionIdentity> exactFallbackSources) {
        requireBinding(plan);
        Objects.requireNonNull(validated, "validated");
        Objects.requireNonNull(exactFallbackSources, "exactFallbackSources");
        requireValidatedIdentity(plan, validated);

        Sha256Digest validationSha = M5MaterializationCodecV1.validationRootSha256(validated.validationRoot());
        Sha256Digest generationSha = M5MaterializationCodecV1.generationSha256(validated.generation());
        Sha256Digest manifestSha = M5MaterializationCodecV1.manifestSha256(validated.manifestView());
        MaterializationTask planned = new MaterializationTask(
                plan.taskIdSha256(),
                TaskState.PLANNED,
                M5MaterializationCodecV1.sourceCutSha256(plan.sourceCut()),
                plan.outputIdentitySha256(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        MaterializationTask verified = new MaterializationTask(
                plan.taskIdSha256(),
                TaskState.OUTPUT_VERIFIED,
                planned.sourceCutSha256(),
                plan.outputIdentitySha256(),
                Optional.of(validationSha),
                Optional.of(generationSha),
                Optional.of(manifestSha));
        MaterializationTask published = new MaterializationTask(
                plan.taskIdSha256(),
                TaskState.PUBLISHED,
                planned.sourceCutSha256(),
                plan.outputIdentitySha256(),
                Optional.of(validationSha),
                Optional.of(generationSha),
                Optional.of(manifestSha));

        PublicationOutcome immutable = reconcileCreate(
                keys.validation(validationSha),
                M5MaterializationCodecV1.encodeValidationRoot(validated.validationRoot()));
        if (!success(immutable)) {
            return immutable;
        }
        immutable = reconcileCreate(
                keys.generation(generationSha), M5MaterializationCodecV1.encodeGeneration(validated.generation()));
        if (!success(immutable)) {
            return immutable;
        }
        immutable = reconcileCreate(
                keys.manifest(manifestSha), M5MaterializationCodecV1.encodeManifest(validated.manifestView()));
        if (!success(immutable)) {
            return immutable;
        }

        PublicationOutcome task = reconcileTaskTransition(planned, verified);
        if (!success(task)) {
            Optional<MaterializationTask> current = readTask(plan.taskIdSha256());
            if (current.isEmpty()
                    || (!current.orElseThrow().equals(verified)
                            && !current.orElseThrow().equals(published))) {
                return task;
            }
        }

        PublicationOutcome selector = publishSelector(plan, manifestSha, exactFallbackSources);
        if (!success(selector)) {
            return selector;
        }
        PublicationOutcome finalTask = reconcileTaskTransition(verified, published);
        if (success(finalTask)) {
            return selector == PublicationOutcome.APPLIED_EXACT
                    ? PublicationOutcome.APPLIED_EXACT
                    : PublicationOutcome.EXISTING_EXACT;
        }
        Optional<BindingReadSelector> current = m4.readSelector();
        if (current.isPresent()
                && current.orElseThrow().selectedViewSha256().equals(manifestSha)
                && current.orElseThrow().sourceGeneration()
                        == validated.generation().sourceGeneration()) {
            return PublicationOutcome.OUTCOME_UNKNOWN;
        }
        return finalTask;
    }

    public Optional<MaterializationTask> readTask(Sha256Digest taskId) {
        return metadata.get(keys.task(taskId)).map(M5MaterializationCodecV1::decodeTask);
    }

    private PublicationOutcome publishSelector(
            MaterializationPlan plan, Sha256Digest manifestSha, List<SourceProtectionIdentity> exactFallbackSources) {
        BindingReadSelector expected = plan.sourceCut().predecessorSelector();
        Optional<BindingReadSelector> observed = m4.readSelector();
        if (observed.isPresent()
                && observed.orElseThrow().selectedViewSha256().equals(manifestSha)
                && observed.orElseThrow().sourceGeneration() == expected.sourceGeneration() + 1
                && observed.orElseThrow().mode() == SelectorMode.PREFERRED_WITH_FALLBACK) {
            return PublicationOutcome.EXISTING_EXACT;
        }
        if (observed.isEmpty() || !observed.orElseThrow().equals(expected)) {
            return observed.isPresent() && observed.orElseThrow().sourceGeneration() > expected.sourceGeneration()
                    ? PublicationOutcome.CANCELLED_STALE
                    : PublicationOutcome.CONFLICT;
        }
        M4ReadControlCoordinatorV1.Outcome outcome = expected.mode() == SelectorMode.PREFERRED_ONLY
                ? m4.introduceFallback(
                        expected, manifestSha, Math.addExact(expected.sourceGeneration(), 1), exactFallbackSources)
                : m4.updateMembershipNeutralView(
                        expected, manifestSha, Math.addExact(expected.sourceGeneration(), 1), exactFallbackSources);
        return switch (outcome) {
            case APPLIED -> PublicationOutcome.APPLIED_EXACT;
            case EXISTING_EXACT -> PublicationOutcome.EXISTING_EXACT;
            case RETRY_EXACT_PREDECESSOR -> PublicationOutcome.DEFINITIVELY_NOT_APPLIED;
            case RETAIN -> PublicationOutcome.OUTCOME_UNKNOWN;
            case CONFLICT, STOPPED, ADOPTED_DIFFERENT_VALID_TERMINAL, QUARANTINED_INVALID_OCCUPANT ->
                PublicationOutcome.CONFLICT;
        };
    }

    private PublicationOutcome reconcileTaskTransition(MaterializationTask expected, MaterializationTask candidate) {
        String key = keys.task(expected.taskIdSha256());
        CanonicalBytes expectedBytes = M5MaterializationCodecV1.encodeTask(expected);
        CanonicalBytes candidateBytes = M5MaterializationCodecV1.encodeTask(candidate);
        ControlMutationOutcome mutation = metadata.compareAndSet(key, Optional.of(expectedBytes), candidateBytes);
        Optional<CanonicalBytes> observed = metadata.get(key);
        if (observed.isPresent() && observed.orElseThrow().equals(candidateBytes)) {
            return mutation == ControlMutationOutcome.APPLIED
                    ? PublicationOutcome.APPLIED_EXACT
                    : PublicationOutcome.EXISTING_EXACT;
        }
        if (observed.isPresent() && observed.orElseThrow().equals(expectedBytes)) {
            return mutation == ControlMutationOutcome.DEFINITIVE_CONFLICT
                    ? PublicationOutcome.CONFLICT
                    : PublicationOutcome.DEFINITIVELY_NOT_APPLIED;
        }
        return observed.isEmpty() ? PublicationOutcome.OUTCOME_UNKNOWN : PublicationOutcome.CONFLICT;
    }

    private PublicationOutcome reconcileCreate(String key, CanonicalBytes candidate) {
        ControlMutationOutcome mutation = metadata.putIfAbsent(key, candidate);
        Optional<CanonicalBytes> observed = metadata.get(key);
        if (observed.isPresent() && observed.orElseThrow().equals(candidate)) {
            return mutation == ControlMutationOutcome.APPLIED
                    ? PublicationOutcome.APPLIED_EXACT
                    : PublicationOutcome.EXISTING_EXACT;
        }
        if (observed.isEmpty()) {
            return mutation == ControlMutationOutcome.DEFINITIVE_CONFLICT
                    ? PublicationOutcome.CONFLICT
                    : PublicationOutcome.OUTCOME_UNKNOWN;
        }
        return PublicationOutcome.CONFLICT;
    }

    private void requireBinding(MaterializationPlan plan) {
        Objects.requireNonNull(plan, "plan");
        if (!binding.equals(plan.sourceCut().identity().binding())) {
            throw new IllegalArgumentException("M5 materialization plan belongs to another Binding");
        }
    }

    private static void requireValidatedIdentity(MaterializationPlan plan, ValidatedGeneration validated) {
        if (!validated.validationRoot().taskIdSha256().equals(plan.taskIdSha256())
                || !validated.validationRoot().outputIdentitySha256().equals(plan.outputIdentitySha256())
                || !validated.generation().taskIdSha256().equals(plan.taskIdSha256())
                || !validated.generation().outputIdentitySha256().equals(plan.outputIdentitySha256())
                || !validated
                        .manifestView()
                        .preferredGenerationSha256()
                        .equals(M5MaterializationCodecV1.generationSha256(validated.generation()))) {
            throw new IllegalArgumentException("M5 validated generation differs from its deterministic plan");
        }
    }

    private static boolean success(PublicationOutcome outcome) {
        return outcome == PublicationOutcome.APPLIED_EXACT || outcome == PublicationOutcome.EXISTING_EXACT;
    }
}
