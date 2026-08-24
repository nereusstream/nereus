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
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/** Exact regular files whose bytes must match every digest in the NAEA1 source/executor tuple. */
public record AllocatorEvidenceSourceArtifactsV1(
        Path oxiaClientJar,
        Path testedEvidenceArtifact,
        Path runtimeDomainArtifact,
        Path runtimeMetadataSpiArtifact,
        Path runtimeMetadataOxiaArtifact,
        Path sourceLocks,
        Path executorManifest) {
    public AllocatorEvidenceSourceArtifactsV1 {
        Objects.requireNonNull(oxiaClientJar, "oxiaClientJar");
        Objects.requireNonNull(testedEvidenceArtifact, "testedEvidenceArtifact");
        Objects.requireNonNull(runtimeDomainArtifact, "runtimeDomainArtifact");
        Objects.requireNonNull(runtimeMetadataSpiArtifact, "runtimeMetadataSpiArtifact");
        Objects.requireNonNull(runtimeMetadataOxiaArtifact, "runtimeMetadataOxiaArtifact");
        Objects.requireNonNull(sourceLocks, "sourceLocks");
        Objects.requireNonNull(executorManifest, "executorManifest");
    }

    void requireExact(AllocatorEvidenceSourceTupleV1 tuple) {
        if (!sha256(oxiaClientJar).equals(tuple.oxiaClientJarSha256())
                || !sha256(testedEvidenceArtifact).equals(tuple.testedEvidenceArtifactSha256())
                || !sha256(runtimeDomainArtifact).equals(tuple.runtimeDomainArtifactSha256())
                || !sha256(runtimeMetadataSpiArtifact).equals(tuple.runtimeMetadataSpiArtifactSha256())
                || !sha256(runtimeMetadataOxiaArtifact).equals(tuple.runtimeMetadataOxiaArtifactSha256())
                || !sha256(sourceLocks).equals(tuple.sourceLocksSha256())
                || !sha256(executorManifest).equals(tuple.executorManifestSha256())) {
            throw AllocatorSelectionReceiptV1.invalid(
                    "allocator evidence source/executor tuple differs from one exact supplied artifact");
        }
    }

    static Sha256Digest sha256(Path path) {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw AllocatorSelectionReceiptV1.invalid(
                    "allocator evidence source artifact must be an exact non-symlink regular file");
        }
        try {
            if (Files.size(path) == 0) {
                throw AllocatorSelectionReceiptV1.invalid("allocator evidence source artifact must be non-empty");
            }
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream input = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) {
                byte[] buffer = new byte[64 * 1024];
                int read;
                while ((read = input.read(buffer)) >= 0) {
                    if (read > 0) {
                        digest.update(buffer, 0, read);
                    }
                }
            }
            return Sha256Digest.copyOf(digest.digest());
        } catch (IOException | NoSuchAlgorithmException error) {
            throw new AllocatorProtocolException(
                    AllocatorProtocolException.Code.SELECTION_NOT_ELIGIBLE,
                    "allocator evidence source artifact could not be hashed",
                    error);
        }
    }
}
