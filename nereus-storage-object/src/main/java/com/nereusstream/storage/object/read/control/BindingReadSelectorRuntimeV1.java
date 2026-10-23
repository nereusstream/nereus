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

package com.nereusstream.storage.object.read.control;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.read.BindingReadAuthorityV1;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1.Outcome;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Couples one durable selector transition to the owner-local read-admission reference.
 *
 * <p>The local predecessor is closed before the low-frequency CAS is dispatched. It remains
 * closed on an unresolved exact-predecessor response or a conflict; only an exact durable
 * successor installs an admitting local successor. This is the response-unknown E-admission
 * fence required by the M4 selector contract.
 */
public final class BindingReadSelectorRuntimeV1 {
    private final BindingIdentity binding;
    private final M4ReadControlCoordinatorV1 coordinator;
    private final AtomicReference<BindingReadAuthorityV1> current;

    public BindingReadSelectorRuntimeV1(
            BindingIdentity binding,
            M4ReadControlCoordinatorV1 coordinator,
            BindingReadSelector initialSelector,
            BindingReadAuthorityV1 initialAuthority) {
        this.binding = Objects.requireNonNull(binding, "binding");
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
        requireAligned(initialSelector, initialAuthority);
        current = new AtomicReference<>(initialAuthority);
    }

    public AtomicReference<BindingReadAuthorityV1> currentAuthority() {
        return current;
    }

    /** Stops local capture before the owner rereads durable state for a source-map transition. */
    public BindingReadAuthorityV1 stopLocalAdmission() {
        BindingReadAuthorityV1 admitted = current.get();
        if (!admitted.admitting() || !current.compareAndSet(admitted, closed(admitted))) {
            throw new IllegalStateException("M4 local read admission already changed");
        }
        return admitted;
    }

    /** Fences the old local owner before the durable takeover CAS can authorize a new native owner. */
    public Outcome grantTakeover(
            BindingReadSelector expectedSelector,
            BindingReadAuthorityV1 expectedAuthority,
            BindingReadAuthorityV1 successorAuthority,
            Sha256Digest successorViewSha256,
            long successorOwnerEpoch,
            long successorSourceGeneration) {
        return transition(
                expectedSelector,
                expectedAuthority,
                successorAuthority,
                () -> coordinator.grantTakeover(
                        expectedSelector, successorViewSha256, successorOwnerEpoch, successorSourceGeneration));
    }

    /** Installs a selected source and its protected fallback only after the durable selector changes. */
    public Outcome introduceFallback(
            BindingReadSelector expectedSelector,
            BindingReadAuthorityV1 expectedAuthority,
            BindingReadAuthorityV1 successorAuthority,
            Sha256Digest fallbackViewSha256,
            long successorSourceGeneration,
            List<SourceProtectionIdentity> sources) {
        return transition(
                expectedSelector,
                expectedAuthority,
                successorAuthority,
                () -> coordinator.introduceFallback(
                        expectedSelector, fallbackViewSha256, successorSourceGeneration, sources));
    }

    /** Keeps the same admission epoch while replacing one authenticated native source map. */
    public Outcome updateMembershipNeutralView(
            BindingReadSelector expectedSelector,
            BindingReadAuthorityV1 expectedAuthority,
            BindingReadAuthorityV1 successorAuthority,
            Sha256Digest successorViewSha256,
            long successorSourceGeneration,
            List<SourceProtectionIdentity> exactFallbackSources) {
        return transition(
                expectedSelector,
                expectedAuthority,
                successorAuthority,
                () -> coordinator.updateMembershipNeutralView(
                        expectedSelector, successorViewSha256, successorSourceGeneration, exactFallbackSources));
    }

    /** Executes the fused PWF E to PO E+1 transition with local close-before-dispatch. */
    public Outcome closeFallback(
            BindingReadSelector expectedSelector,
            BindingReadAuthorityV1 expectedAuthority,
            BindingReadAuthorityV1 successorAuthority,
            Sha256Digest preferredOnlyViewSha256,
            long successorSourceGeneration,
            List<SourceProtectionIdentity> sources) {
        return transition(
                expectedSelector,
                expectedAuthority,
                successorAuthority,
                () -> coordinator.closeFallback(
                        expectedSelector, preferredOnlyViewSha256, successorSourceGeneration, sources));
    }

    private Outcome transition(
            BindingReadSelector expectedSelector,
            BindingReadAuthorityV1 expectedAuthority,
            BindingReadAuthorityV1 successorAuthority,
            java.util.function.Supplier<Outcome> durableTransition) {
        requireAligned(expectedSelector, expectedAuthority);
        Objects.requireNonNull(successorAuthority, "successorAuthority");
        Objects.requireNonNull(durableTransition, "durableTransition");
        BindingReadAuthorityV1 closed = closed(expectedAuthority);
        BindingReadAuthorityV1 installedFence = closed;
        if (!current.compareAndSet(expectedAuthority, closed)) {
            BindingReadAuthorityV1 observed = current.get();
            if (observed.equals(successorAuthority)) {
                return Outcome.EXISTING_EXACT;
            }
            if (!observed.equals(closed)) {
                return Outcome.CONFLICT;
            }
            installedFence = observed;
        }

        Outcome outcome = durableTransition.get();
        if (outcome != Outcome.APPLIED && outcome != Outcome.EXISTING_EXACT) {
            return outcome;
        }
        BindingReadSelector durable = coordinator
                .readSelector()
                .orElseThrow(() -> new IllegalStateException("successful selector CAS has no durable successor"));
        requireAligned(durable, successorAuthority);
        if (!current.compareAndSet(installedFence, successorAuthority) && current.get() != successorAuthority) {
            throw new IllegalStateException(
                    "local selector authority changed while installing exact durable successor");
        }
        return outcome;
    }

    /**
     * Installs an exact durable selector after recovery. A stopped local reference is never
     * reopened from caller-provided fields alone.
     */
    public void installExactDurable(BindingReadAuthorityV1 candidate) {
        BindingReadSelector durable =
                coordinator.readSelector().orElseThrow(() -> new IllegalStateException("durable selector is absent"));
        requireAligned(durable, candidate);
        current.set(candidate);
    }

    private void requireAligned(BindingReadSelector selector, BindingReadAuthorityV1 authority) {
        Objects.requireNonNull(selector, "selector");
        Objects.requireNonNull(authority, "authority");
        if (!selector.binding().equals(binding)
                || !authority.bindingId().equals(binding.bindingId())
                || !authority.topicIncarnationIdentity().equals(binding.incarnationSha256())
                || !authority.storageEpochId().digest().equals(binding.storageEpochSha256())
                || !authority.selectedViewSha256().equals(selector.selectedViewSha256())
                || authority.ownerEpoch() != selector.ownerEpoch()
                || authority.readAdmissionEpoch() != selector.readAdmissionEpoch()
                || authority.admitting()
                        != (selector.admissionState() == M4ReadControlRecordsV1.AdmissionState.ADMITTING)
                || authority.sourceGeneration() != selector.sourceGeneration()
                || authority.capabilityGeneration() != selector.capability().generation()
                || !authority
                        .capabilityEvidenceSha256()
                        .equals(selector.capability().evidenceSha256())) {
            throw new IllegalArgumentException("local read authority differs from the exact durable selector");
        }
    }

    private static BindingReadAuthorityV1 closed(BindingReadAuthorityV1 authority) {
        return new BindingReadAuthorityV1(
                authority.bindingId(),
                authority.topicIncarnationIdentity(),
                authority.storageEpochId(),
                authority.protocol(),
                authority.selectedViewSha256(),
                authority.ownerEpoch(),
                authority.readAdmissionEpoch(),
                false,
                authority.capabilityGeneration(),
                authority.capabilityEvidenceSha256(),
                authority.publicationCell());
    }
}
