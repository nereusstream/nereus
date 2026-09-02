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

package com.nereusstream.storage.object.read;

import com.nereusstream.storage.object.read.BindingReadRouteV1.FailureClass;
import java.util.Objects;

/** One interval's sequential primary/one-shot-fallback decision under the outer hazard lease. */
public final class BindingReadAttemptControllerV1 {
    public enum Outcome {
        PRIMARY_IN_FLIGHT,
        FALLBACK_READY,
        FALLBACK_IN_FLIGHT,
        PRIMARY,
        FALLBACK,
        SAFE_FAILURE,
        QUARANTINED
    }

    private static final long PRIMARY_ATTEMPT = 1;
    private static final long FALLBACK_ATTEMPT = 2;

    private final BindingReadBatchContextV1 batch;
    private final BindingReadRouteV1 route;
    private Outcome outcome;
    private FailureClass primaryFailure;

    public BindingReadAttemptControllerV1(BindingReadBatchContextV1 batch, BindingReadRouteV1 route) {
        this.batch = Objects.requireNonNull(batch, "batch");
        this.route = Objects.requireNonNull(route, "route");
    }

    public boolean startPrimary() {
        if (outcome != null || !batch.beginAttempt(PRIMARY_ATTEMPT)) {
            return false;
        }
        outcome = Outcome.PRIMARY_IN_FLIGHT;
        return true;
    }

    public Outcome completePrimary(FailureClass failure, boolean cleanupAndBufferDrainProven) {
        requireOutcome(Outcome.PRIMARY_IN_FLIGHT);
        if (!cleanupAndBufferDrainProven || !batch.endAttempt(PRIMARY_ATTEMPT)) {
            batch.quarantine();
            outcome = Outcome.QUARANTINED;
            return outcome;
        }
        if (failure == null) {
            batch.closeNewSourceUse();
            outcome = Outcome.PRIMARY;
            return outcome;
        }
        primaryFailure = failure;
        if (!batch.observable() && route.allowsFallback(failure)) {
            outcome = Outcome.FALLBACK_READY;
            return outcome;
        }
        batch.closeNewSourceUse();
        outcome = Outcome.SAFE_FAILURE;
        return outcome;
    }

    public boolean startFallback() {
        requireOutcome(Outcome.FALLBACK_READY);
        if (!batch.beginAttempt(FALLBACK_ATTEMPT)) {
            batch.closeNewSourceUse();
            outcome = Outcome.SAFE_FAILURE;
            return false;
        }
        outcome = Outcome.FALLBACK_IN_FLIGHT;
        return true;
    }

    public Outcome completeFallback(FailureClass failure, boolean cleanupAndBufferDrainProven) {
        requireOutcome(Outcome.FALLBACK_IN_FLIGHT);
        if (!cleanupAndBufferDrainProven || !batch.endAttempt(FALLBACK_ATTEMPT)) {
            batch.quarantine();
            outcome = Outcome.QUARANTINED;
            return outcome;
        }
        batch.closeNewSourceUse();
        outcome = failure == null ? Outcome.FALLBACK : Outcome.SAFE_FAILURE;
        return outcome;
    }

    public Outcome outcome() {
        if (outcome == null) {
            throw new IllegalStateException("primary attempt has not started");
        }
        return outcome;
    }

    public FailureClass primaryFailure() {
        return primaryFailure;
    }

    private void requireOutcome(Outcome expected) {
        if (outcome != expected) {
            throw new IllegalStateException("read attempt state differs: expected=" + expected + " actual=" + outcome);
        }
    }
}
