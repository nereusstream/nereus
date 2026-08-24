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

package com.nereusstream.kafka.bookkeeper.object.publication;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Shared-header failure domain plus independently verifiable per-binding complete-unit results. */
public final class KafkaSharedExtentValidationV1 {
    public enum State {
        PENDING,
        VERIFIED,
        FAILED
    }

    private final KafkaObjectExtentIdentityV1 extent;
    private final Map<KafkaObjectBindingKeyV1, State> members = new LinkedHashMap<>();
    private State sharedState = State.PENDING;

    public KafkaSharedExtentValidationV1(
            KafkaObjectExtentIdentityV1 extent, List<KafkaObjectBindingKeyV1> expectedMembers) {
        this.extent = Objects.requireNonNull(extent, "extent");
        expectedMembers = List.copyOf(Objects.requireNonNull(expectedMembers, "expectedMembers"));
        if (expectedMembers.isEmpty() || expectedMembers.size() > 256) {
            throw new IllegalArgumentException("shared Kafka extent member count exceeds its bound");
        }
        expectedMembers.forEach(member -> {
            if (members.put(member, State.PENDING) != null) {
                throw new IllegalArgumentException("shared Kafka extent contains a duplicate binding member");
            }
        });
    }

    /** Called only after Object SHA, KMS envelope, fixed header, and directory AEAD all verify once. */
    public synchronized void sharedVerified(KafkaObjectExtentIdentityV1 verifiedExtent) {
        if (sharedState != State.PENDING || !extent.equals(verifiedExtent)) {
            throw new IllegalStateException("shared Kafka extent cannot enter VERIFIED");
        }
        sharedState = State.VERIFIED;
    }

    /** Shared identity/crypto failure blocks every member of the Object. */
    public synchronized void sharedFailed() {
        if (sharedState != State.PENDING) {
            throw new IllegalStateException("shared Kafka extent already has a terminal validation result");
        }
        sharedState = State.FAILED;
        members.replaceAll((ignored, state) -> State.FAILED);
    }

    public synchronized void memberVerified(KafkaObjectBindingKeyV1 binding) {
        transitionMember(binding, State.VERIFIED);
    }

    /** Frame/CRC/native/typed-coverage failure blocks only the owning complete commit set. */
    public synchronized void memberFailed(KafkaObjectBindingKeyV1 binding) {
        transitionMember(binding, State.FAILED);
    }

    public synchronized boolean canPublish(KafkaObjectBindingKeyV1 binding) {
        return sharedState == State.VERIFIED && requireMember(binding) == State.VERIFIED;
    }

    public synchronized State sharedState() {
        return sharedState;
    }

    public KafkaObjectExtentIdentityV1 extent() {
        return extent;
    }

    private void transitionMember(KafkaObjectBindingKeyV1 binding, State replacement) {
        if (sharedState != State.VERIFIED || requireMember(binding) != State.PENDING) {
            throw new IllegalStateException("Kafka extent member cannot enter its terminal validation state");
        }
        members.put(binding, replacement);
    }

    private State requireMember(KafkaObjectBindingKeyV1 binding) {
        State state = members.get(Objects.requireNonNull(binding, "binding"));
        if (state == null) {
            throw new IllegalArgumentException("binding is not a member of the shared Kafka extent");
        }
        return state;
    }
}
