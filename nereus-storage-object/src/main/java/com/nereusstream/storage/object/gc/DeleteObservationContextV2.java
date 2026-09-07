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

package com.nereusstream.storage.object.gc;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Durable native facts for one observation epoch; existence alone does not prove native ownership. */
public record DeleteObservationContextV2(
        long observationEpoch,
        AuthorityFactV1 coordinatorOwner,
        AuthorityFactV1 capability,
        Optional<AuthorityFactV1> predecessorOwnerFenced) {
    public DeleteObservationContextV2 {
        M5TargetDeleteAuthorityRecordsV1.requirePositive(observationEpoch, "observationEpoch");
        Objects.requireNonNull(coordinatorOwner, "coordinatorOwner");
        Objects.requireNonNull(capability, "capability");
        predecessorOwnerFenced = Objects.requireNonNull(predecessorOwnerFenced, "predecessorOwnerFenced");
        // Apply the same strict key/version bounds as persisted eligibility facts.
        java.util.Map<String, AuthorityFactV1> facts = new java.util.HashMap<>();
        for (AuthorityFactV1 fact : predecessorOwnerFenced.isPresent()
                ? List.of(coordinatorOwner, capability, predecessorOwnerFenced.orElseThrow())
                : List.of(coordinatorOwner, capability)) {
            encodeFact(fact);
            AuthorityFactV1 previous = facts.putIfAbsent(fact.key(), fact);
            if (previous != null && !previous.equals(fact)) {
                throw new IllegalArgumentException("observation authority roles disagree on one exact key");
            }
        }
    }

    public List<AuthorityFactV1> authorityFacts() {
        return predecessorOwnerFenced.isPresent()
                ? List.of(coordinatorOwner, capability, predecessorOwnerFenced.orElseThrow())
                : List.of(coordinatorOwner, capability);
    }

    private static CanonicalBytes encodeFact(AuthorityFactV1 fact) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                DeleteEligibilityCodecV2.writeFact(out, fact);
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException error) {
            throw new IllegalArgumentException("observation authority cannot be encoded", error);
        }
    }
}
