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
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IdentityEnvelope;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PayloadKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Canonical logical NMS1 payload value. Its Object identity is the full encoded body SHA-256. */
public record Nms1ObjectV1(
        IdentityEnvelope identity,
        PayloadKind payloadKind,
        Sha256Digest taskIdSha256,
        Sha256Digest outputIdentitySha256,
        ProtocolCoverage coverage,
        int partOrdinal,
        int partCount,
        Sha256Digest encryptionGenerationSha256,
        Sha256Digest compressionPolicySha256,
        Sha256Digest checksumPolicySha256,
        List<SourceContribution> sources,
        List<ExtentRow> extents,
        CanonicalBytes payload,
        List<IndexSection> indexes) {
    public static final int MAX_SOURCE_ROWS = 256;
    public static final int MAX_EXTENT_ROWS = 65_536;
    public static final int MAX_INDEX_SECTIONS = 32;
    public static final int MAX_INDEX_BYTES = 64 * 1024 * 1024;
    public static final int KNOWN_FLAGS_MASK = 0x7;

    public Nms1ObjectV1 {
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(payloadKind, "payloadKind");
        if (payloadKind == PayloadKind.NATIVE_EXTENT_REFERENCE_V1) {
            throw new IllegalArgumentException("NMS1 cannot encode a descriptor-only native extent reference");
        }
        requireDigest(taskIdSha256, "taskIdSha256");
        requireDigest(outputIdentitySha256, "outputIdentitySha256");
        Objects.requireNonNull(coverage, "coverage");
        if (partOrdinal < 0 || partCount <= 0 || partOrdinal >= partCount || partCount > 256) {
            throw new IllegalArgumentException("NMS1 part identity is outside its cap");
        }
        requireDigest(encryptionGenerationSha256, "encryptionGenerationSha256");
        requireDigest(compressionPolicySha256, "compressionPolicySha256");
        requireDigest(checksumPolicySha256, "checksumPolicySha256");
        sources = List.copyOf(Objects.requireNonNull(sources, "sources"));
        extents = List.copyOf(Objects.requireNonNull(extents, "extents"));
        payload =
                CanonicalBytes.copyOf(Objects.requireNonNull(payload, "payload").toByteArray());
        indexes = List.copyOf(Objects.requireNonNull(indexes, "indexes"));
        if (sources.isEmpty() || sources.size() > MAX_SOURCE_ROWS) {
            throw new IllegalArgumentException("NMS1 source table count is outside its cap");
        }
        if (extents.isEmpty() || extents.size() > MAX_EXTENT_ROWS || payload.isEmpty()) {
            throw new IllegalArgumentException("NMS1 extent/payload section is empty or oversized");
        }
        if (indexes.size() > MAX_INDEX_SECTIONS) {
            throw new IllegalArgumentException("NMS1 index count is outside its cap");
        }
        requireSortedUniqueSources(sources);
        requireExtentCoverage(coverage, extents, payload.length());
        requireSortedUniqueIndexes(indexes, coverage);
    }

    public record SourceContribution(
            Sha256Digest sourceIdentitySha256, ProtocolCoverage coverage, Sha256Digest contributedBytesSha256) {
        public SourceContribution {
            requireDigest(sourceIdentitySha256, "sourceIdentitySha256");
            Objects.requireNonNull(coverage, "coverage");
            requireDigest(contributedBytesSha256, "contributedBytesSha256");
        }
    }

    public record ExtentRow(
            ProtocolCoverage coverage,
            int payloadOffset,
            int payloadLength,
            int recordCount,
            long minimumTimestamp,
            long maximumTimestamp,
            Sha256Digest payloadSha256,
            int protocolFlags) {
        public ExtentRow {
            Objects.requireNonNull(coverage, "coverage");
            if (payloadOffset < 0 || payloadLength <= 0 || recordCount < 0) {
                throw new IllegalArgumentException("NMS1 extent payload range/count is invalid");
            }
            if (recordCount == 0 && (minimumTimestamp != -1 || maximumTimestamp != -1)) {
                throw new IllegalArgumentException("empty NMS1 extent must use timestamp sentinel -1");
            }
            if (recordCount > 0 && (minimumTimestamp < 0 || maximumTimestamp < minimumTimestamp)) {
                throw new IllegalArgumentException("NMS1 extent timestamp range is invalid");
            }
            requireDigest(payloadSha256, "payloadSha256");
            if (protocolFlags < 0 || (protocolFlags & ~KNOWN_FLAGS_MASK) != 0) {
                throw new IllegalArgumentException("NMS1 extent carries unknown protocol flags");
            }
        }
    }

    public record IndexSection(
            IndexKind kind,
            ProtocolCoverage coverage,
            int parserVersion,
            CanonicalBytes body,
            Sha256Digest bodySha256) {
        public IndexSection {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(coverage, "coverage");
            if (parserVersion != 1) {
                throw new IllegalArgumentException("NMS1 index parser version is unsupported");
            }
            body = CanonicalBytes.copyOf(Objects.requireNonNull(body, "body").toByteArray());
            if (body.isEmpty() || body.length() > MAX_INDEX_BYTES) {
                throw new IllegalArgumentException("NMS1 index body length is outside its cap");
            }
            requireDigest(bodySha256, "bodySha256");
            if (!Sha256Digest.hash(body).equals(bodySha256)) {
                throw new IllegalArgumentException("NMS1 index body digest differs");
            }
        }
    }

    private static void requireSortedUniqueSources(List<SourceContribution> values) {
        Comparator<SourceContribution> comparator = Comparator.comparing((SourceContribution value) ->
                        value.sourceIdentitySha256().toHex())
                .thenComparingLong(value -> value.coverage().inclusiveStart());
        if (!values.equals(values.stream().sorted(comparator).toList())
                || values.stream()
                                .map(SourceContribution::sourceIdentitySha256)
                                .distinct()
                                .count()
                        != values.size()) {
            throw new IllegalArgumentException("NMS1 source table is not sorted unique");
        }
    }

    private static void requireExtentCoverage(ProtocolCoverage coverage, List<ExtentRow> values, int payloadLength) {
        if (values.get(0).coverage().inclusiveStart() != coverage.inclusiveStart()
                || values.get(values.size() - 1).coverage().exclusiveEnd() != coverage.exclusiveEnd()) {
            throw new IllegalArgumentException("NMS1 extents do not cover the part");
        }
        int expectedOffset = 0;
        for (int index = 0; index < values.size(); index++) {
            ExtentRow value = values.get(index);
            if (value.coverage().domain() != coverage.domain()
                    || !coverage.contains(value.coverage())
                    || value.payloadOffset() != expectedOffset
                    || (index > 0 && !values.get(index - 1).coverage().adjacentTo(value.coverage()))) {
                throw new IllegalArgumentException("NMS1 extent directory has a gap, overlap, or byte mismatch");
            }
            expectedOffset = Math.addExact(expectedOffset, value.payloadLength());
        }
        if (expectedOffset != payloadLength) {
            throw new IllegalArgumentException("NMS1 extent directory does not consume the payload exactly");
        }
    }

    private static void requireSortedUniqueIndexes(List<IndexSection> values, ProtocolCoverage coverage) {
        if (!values.equals(values.stream()
                        .sorted(Comparator.comparingInt(value -> value.kind().ordinal()))
                        .toList())
                || values.stream().map(IndexSection::kind).distinct().count() != values.size()) {
            throw new IllegalArgumentException("NMS1 index directory is not sorted unique");
        }
        if (values.stream().anyMatch(value -> !coverage.contains(value.coverage()))) {
            throw new IllegalArgumentException("NMS1 index coverage escapes the payload part");
        }
    }

    private static void requireDigest(Sha256Digest value, String label) {
        Objects.requireNonNull(value, label);
        if (value.isZero()) {
            throw new IllegalArgumentException(label + " is the zero digest");
        }
    }
}
