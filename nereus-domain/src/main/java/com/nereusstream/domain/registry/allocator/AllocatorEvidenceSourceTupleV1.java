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

package com.nereusstream.domain.registry.allocator;

import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.Objects;
import java.util.regex.Pattern;

/** Exact formal allocator source/executor identity shared by every raw attachment. */
public record AllocatorEvidenceSourceTupleV1(
        String nereusSourceCommit,
        String pulsarSourceCommit,
        String oxiaClientSourceCommit,
        String oxiaServerSourceCommit,
        Sha256Digest oxiaClientJarSha256,
        Sha256Digest testedEvidenceArtifactSha256,
        Sha256Digest runtimeDomainArtifactSha256,
        Sha256Digest runtimeMetadataSpiArtifactSha256,
        Sha256Digest runtimeMetadataOxiaArtifactSha256,
        Sha256Digest sourceLocksSha256,
        Sha256Digest executorManifestSha256) {
    private static final Pattern COMMIT = Pattern.compile("[0-9a-f]{40}");
    private static final String ZERO_COMMIT = "0".repeat(40);

    public AllocatorEvidenceSourceTupleV1 {
        Objects.requireNonNull(nereusSourceCommit, "nereusSourceCommit");
        Objects.requireNonNull(pulsarSourceCommit, "pulsarSourceCommit");
        Objects.requireNonNull(oxiaClientSourceCommit, "oxiaClientSourceCommit");
        Objects.requireNonNull(oxiaServerSourceCommit, "oxiaServerSourceCommit");
        Objects.requireNonNull(oxiaClientJarSha256, "oxiaClientJarSha256");
        Objects.requireNonNull(testedEvidenceArtifactSha256, "testedEvidenceArtifactSha256");
        Objects.requireNonNull(runtimeDomainArtifactSha256, "runtimeDomainArtifactSha256");
        Objects.requireNonNull(runtimeMetadataSpiArtifactSha256, "runtimeMetadataSpiArtifactSha256");
        Objects.requireNonNull(runtimeMetadataOxiaArtifactSha256, "runtimeMetadataOxiaArtifactSha256");
        Objects.requireNonNull(sourceLocksSha256, "sourceLocksSha256");
        Objects.requireNonNull(executorManifestSha256, "executorManifestSha256");
        if (!COMMIT.matcher(nereusSourceCommit).matches()
                || !COMMIT.matcher(pulsarSourceCommit).matches()
                || !COMMIT.matcher(oxiaClientSourceCommit).matches()
                || !COMMIT.matcher(oxiaServerSourceCommit).matches()
                || ZERO_COMMIT.equals(nereusSourceCommit)
                || ZERO_COMMIT.equals(pulsarSourceCommit)
                || ZERO_COMMIT.equals(oxiaClientSourceCommit)
                || ZERO_COMMIT.equals(oxiaServerSourceCommit)
                || oxiaClientJarSha256.isZero()
                || testedEvidenceArtifactSha256.isZero()
                || runtimeDomainArtifactSha256.isZero()
                || runtimeMetadataSpiArtifactSha256.isZero()
                || runtimeMetadataOxiaArtifactSha256.isZero()
                || sourceLocksSha256.isZero()
                || executorManifestSha256.isZero()) {
            throw new AllocatorProtocolException(
                    AllocatorProtocolException.Code.SELECTION_NOT_ELIGIBLE,
                    "allocator evidence source/executor tuple is incomplete or non-canonical");
        }
    }
}
