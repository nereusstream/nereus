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
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.GenerationObject;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.ProviderObjectOutcome;
import com.nereusstream.storage.object.provider.ProviderObjectResult;
import com.nereusstream.storage.object.provider.ProviderReconciliationResult;
import com.nereusstream.storage.object.provider.RepeatableObjectBody;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Cell-scoped deterministic output creation and response-loss convergence over the existing C1 session. */
public final class M5MaterializationObjectSessionV1 {
    public record Candidate(GenerationObject descriptor, CanonicalBytes canonicalBody) {
        public Candidate {
            Objects.requireNonNull(descriptor, "descriptor");
            canonicalBody = CanonicalBytes.copyOf(
                    Objects.requireNonNull(canonicalBody, "canonicalBody").toByteArray());
            if (canonicalBody.length() != descriptor.identity().bodyLength()
                    || !Sha256Digest.hash(canonicalBody)
                            .equals(descriptor.identity().bodySha256())) {
                throw new IllegalArgumentException("M5 output candidate body identity differs");
            }
        }
    }

    public record CreationResult(List<GenerationObject> exactObjects, int createdOrAdopted, int reconciledUnknowns) {
        public CreationResult {
            exactObjects = List.copyOf(Objects.requireNonNull(exactObjects, "exactObjects"));
            if (createdOrAdopted != exactObjects.size() || reconciledUnknowns < 0) {
                throw new IllegalArgumentException("M5 creation summary is inconsistent");
            }
        }
    }

    private final C1ObjectProviderSession provider;
    private final int maximumListPages;
    private final long maximumListKeys;
    private final long maximumListKeyBytes;
    private final int maximumSingleKeyBytes;

    public M5MaterializationObjectSessionV1(
            C1ObjectProviderSession provider,
            int maximumListPages,
            long maximumListKeys,
            long maximumListKeyBytes,
            int maximumSingleKeyBytes) {
        this.provider = Objects.requireNonNull(provider, "provider");
        if (maximumListPages <= 0 || maximumListKeys <= 0 || maximumListKeyBytes <= 0 || maximumSingleKeyBytes <= 0) {
            throw new IllegalArgumentException("M5 reconciliation bounds must be positive");
        }
        this.maximumListPages = maximumListPages;
        this.maximumListKeys = maximumListKeys;
        this.maximumListKeyBytes = maximumListKeyBytes;
        this.maximumSingleKeyBytes = maximumSingleKeyBytes;
    }

    public CreationResult createExact(List<Candidate> candidates) throws IOException {
        candidates = List.copyOf(Objects.requireNonNull(candidates, "candidates"));
        List<Candidate> sorted = candidates.stream()
                .sorted(Comparator.comparingInt(value -> value.descriptor().ordinal()))
                .toList();
        if (candidates.isEmpty()
                || !candidates.equals(sorted)
                || candidates.stream()
                                .map(value -> value.descriptor().identity().key())
                                .distinct()
                                .count()
                        != candidates.size()) {
            throw new IllegalArgumentException("M5 output candidates are empty, reordered, or duplicate");
        }
        List<GenerationObject> exact = new ArrayList<>(candidates.size());
        int reconciled = 0;
        for (Candidate candidate : candidates) {
            ProviderObjectResult result = provider.conditionalCreate(new Body(candidate));
            if (result.outcome() == ProviderObjectOutcome.OUTCOME_UNKNOWN) {
                ProviderReconciliationResult reconciliation = provider.reconcileUnknown(
                        candidate.descriptor().identity(),
                        leafPrefix(candidate.descriptor().identity().key()),
                        maximumListPages,
                        maximumListKeys,
                        maximumListKeyBytes,
                        maximumSingleKeyBytes);
                result = reconciliation.objectResult();
                reconciled++;
            }
            if (result.outcome() != ProviderObjectOutcome.APPLIED_EXACT
                    && result.outcome() != ProviderObjectOutcome.EXISTING_EXACT) {
                throw new IllegalStateException(
                        "M5 deterministic Object create did not converge exactly: " + result.outcome());
            }
            GenerationObject descriptor = candidate.descriptor();
            exact.add(new GenerationObject(
                    descriptor.ordinal(),
                    descriptor.indexKind(),
                    descriptor.coverage(),
                    descriptor.identity(),
                    result.versionToken()));
        }
        return new CreationResult(exact, exact.size(), reconciled);
    }

    public M5MaterializationValidatorV1.VerifiedObjectRead readExact(ObjectIdentity identity) throws IOException {
        C1ObjectProviderSession.VerifiedObjectRead read = provider.readVerifiedObjectWithVersion(identity);
        return new M5MaterializationValidatorV1.VerifiedObjectRead(read.canonicalBody(), read.immutableVersionToken());
    }

    private static String leafPrefix(String key) {
        int slash = key.lastIndexOf('/');
        if (slash <= 0) {
            throw new IllegalArgumentException("M5 Object key has no deterministic parent prefix");
        }
        return key.substring(0, slash + 1);
    }

    private record Body(Candidate candidate) implements RepeatableObjectBody {
        @Override
        public ObjectIdentity identity() {
            return candidate.descriptor().identity();
        }

        @Override
        public InputStream openStream() {
            return new ByteArrayInputStream(candidate.canonicalBody().toByteArray());
        }
    }
}
