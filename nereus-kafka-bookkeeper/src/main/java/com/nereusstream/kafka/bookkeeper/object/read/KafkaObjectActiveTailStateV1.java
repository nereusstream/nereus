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

package com.nereusstream.kafka.bookkeeper.object.read;

import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectBindingKeyV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectExtentLocatorV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectSourceProtectionTrackerV1;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Immutable binding-local Object active tail selected only by the M2 coherent root. */
public record KafkaObjectActiveTailStateV1(
        KafkaObjectBindingKeyV1 binding,
        long startOffset,
        long endOffsetExclusive,
        List<KafkaObjectExtentLocatorV1> locators) {
    public KafkaObjectActiveTailStateV1 {
        Objects.requireNonNull(binding, "binding");
        locators = List.copyOf(Objects.requireNonNull(locators, "locators"));
        if (startOffset < 0 || endOffsetExclusive < startOffset) {
            throw new IllegalArgumentException("Kafka Object active-tail range is invalid");
        }
        long next = startOffset;
        for (KafkaObjectExtentLocatorV1 locator : locators) {
            if (!locator.binding().equals(binding) || locator.startOffset() != next) {
                throw new IllegalArgumentException("Kafka Object active-tail locator is not binding-contiguous");
            }
            next = locator.endOffsetExclusive();
        }
        if (next != endOffsetExclusive) {
            throw new IllegalArgumentException("Kafka Object active-tail coverage differs from its end");
        }
    }

    public static KafkaObjectActiveTailStateV1 empty(KafkaObjectBindingKeyV1 binding, long startOffset) {
        return new KafkaObjectActiveTailStateV1(binding, startOffset, startOffset, List.of());
    }

    public KafkaObjectActiveTailStateV1 append(KafkaObjectExtentLocatorV1 locator) {
        Objects.requireNonNull(locator, "locator");
        if (!locator.binding().equals(binding) || locator.startOffset() != endOffsetExclusive) {
            throw new IllegalArgumentException("Kafka Object locator does not extend this binding active tail");
        }
        List<KafkaObjectExtentLocatorV1> replacement = new ArrayList<>(locators);
        replacement.add(locator);
        return new KafkaObjectActiveTailStateV1(binding, startOffset, locator.endOffsetExclusive(), replacement);
    }

    public Optional<KafkaObjectExtentLocatorV1> floor(long offset, long readableFrontier) {
        if (offset < startOffset || offset >= readableFrontier || readableFrontier > endOffsetExclusive) {
            return Optional.empty();
        }
        for (int index = locators.size() - 1; index >= 0; index--) {
            KafkaObjectExtentLocatorV1 locator = locators.get(index);
            if (locator.startOffset() <= offset && offset < locator.endOffsetExclusive()) {
                return Optional.of(locator);
            }
        }
        return Optional.empty();
    }

    public KafkaObjectActiveTailStateV1 retirePrefix(
            KafkaObjectSourceProtectionTrackerV1.RetirementPlan retirementPlan) {
        Objects.requireNonNull(retirementPlan, "retirementPlan");
        long replacementCoveredThrough = retirementPlan.replacementCoveredThrough();
        if (!retirementPlan.binds(this)
                || replacementCoveredThrough < startOffset
                || replacementCoveredThrough > endOffsetExclusive) {
            throw new IllegalStateException("Kafka Object active tail cannot retire outside replacement/pin safety");
        }
        int retainedFrom = 0;
        while (retainedFrom < locators.size()
                && locators.get(retainedFrom).endOffsetExclusive() <= replacementCoveredThrough) {
            retainedFrom++;
        }
        if (retainedFrom < locators.size() && locators.get(retainedFrom).startOffset() != replacementCoveredThrough) {
            throw new IllegalArgumentException("replacement frontier cuts through an Object locator");
        }
        return new KafkaObjectActiveTailStateV1(
                binding,
                replacementCoveredThrough,
                endOffsetExclusive,
                locators.subList(retainedFrom, locators.size()));
    }
}
