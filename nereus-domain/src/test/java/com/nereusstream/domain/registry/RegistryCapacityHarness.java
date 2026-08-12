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

package com.nereusstream.domain.registry;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/** Test-scope-only M1.1c-R0 Registry capacity model. */
final class RegistryCapacityHarness {
    static final int MAX_REGISTRY_BYTES = 65_536;
    static final int MAX_ASSIGNMENTS_EVER = 256;
    static final int MAX_ASSIGNMENT_ROW_BYTES = 192;
    static final int WRITER_ROW_BYTES = 120;
    static final int FIXED_HEADER_BYTES = 184;
    static final int WRITER_KIND_COUNT = 2;
    static final int COHORT_SLOTS_PER_KIND = 7;
    static final int MAX_WRITER_COUNT = WRITER_KIND_COUNT * COHORT_SLOTS_PER_KIND;
    static final int MAX_CANONICAL_REGISTRY_BYTES = 51_016;
    static final int REGISTRY_ENVELOPE_MARGIN_BYTES = 14_520;
    static final int OXIA_EXPECTED_VERSION_BYTES = Long.BYTES;
    static final int MAX_OXIA_CAS_OPERANDS_BYTES = 51_024;
    static final int EXPECTED_FOCUSED_TESTS = 18;
    static final String BASELINE_COMMIT = "26728bec826ac72e5b893d4d19983d588feaca4f";

    private static final Pattern COMMIT = Pattern.compile("[0-9a-f]{40}");
    private static final Comparator<WriterRow> WRITER_ORDER = Comparator.<WriterRow>comparingInt(
                    row -> row.writerKind().code())
            .thenComparingLong(WriterRow::principalGeneration)
            .thenComparing(row -> row.principalSha256().toHex());

    private RegistryCapacityHarness() {}

    enum WriterKind {
        NATIVE_BOOKKEEPER_LEDGER_ID(1),
        NEREUS_VIRTUAL_LEDGER_ID(2);

        private final int code;

        WriterKind(int code) {
            this.code = code;
        }

        int code() {
            return code;
        }

        static WriterKind fromCode(int code) {
            for (WriterKind value : values()) {
                if (value.code == code) {
                    return value;
                }
            }
            throw new CapacityRejectedException(
                    RejectionCode.REGISTRY_UNAUTHORIZED_WRITER, "unknown writer kind " + code);
        }
    }

    enum CohortState {
        PRESTART_COMMITTED,
        ACTIVE,
        FENCED_DRAINING,
        REVOKED_PENDING_REMOVAL
    }

    enum RejectionCode {
        REGISTRY_WRITER_COUNT_EXCEEDED,
        REGISTRY_CANONICAL_BYTES_EXCEEDED,
        REGISTRY_ASSIGNMENT_COUNT_EXCEEDED,
        REGISTRY_ASSIGNMENT_ROW_BYTES_EXCEEDED,
        REGISTRY_OMITTED_AUTHORIZED_WRITER,
        REGISTRY_UNAUTHORIZED_WRITER,
        REGISTRY_WRITER_LIFECYCLE_VIOLATION
    }

    static final class CapacityRejectedException extends IllegalArgumentException {
        private final RejectionCode code;

        CapacityRejectedException(RejectionCode code, String detail) {
            super(code + ": " + detail);
            this.code = Objects.requireNonNull(code, "code");
        }

        RejectionCode code() {
            return code;
        }
    }

    record HeaderField(String name, int bytes) {
        HeaderField {
            Objects.requireNonNull(name, "name");
            if (bytes <= 0) {
                throw new IllegalArgumentException("header field width must be positive");
            }
        }
    }

    record WriterRow(
            WriterKind writerKind,
            int exclusionContractVersion,
            long principalGeneration,
            Sha256Digest principalSha256,
            long interlockGeneration,
            Sha256Digest interlockSha256,
            int evidenceKind,
            int evidenceVersion,
            Sha256Digest admissionEvidenceSha256) {
        WriterRow {
            Objects.requireNonNull(writerKind, "writerKind");
            if (exclusionContractVersion != 1 || evidenceKind != 1 || evidenceVersion != 1) {
                throw new CapacityRejectedException(
                        RejectionCode.REGISTRY_UNAUTHORIZED_WRITER, "unknown writer contract or evidence code");
            }
            if (principalGeneration <= 0 || interlockGeneration <= 0) {
                throw new CapacityRejectedException(
                        RejectionCode.REGISTRY_UNAUTHORIZED_WRITER, "writer generations must be positive");
            }
            requireNonZero(principalSha256, "principalSha256");
            requireNonZero(interlockSha256, "interlockSha256");
            requireNonZero(admissionEvidenceSha256, "admissionEvidenceSha256");
        }

        byte[] encode() {
            ByteBuffer output = ByteBuffer.allocate(WRITER_ROW_BYTES);
            putU16(output, writerKind.code());
            putU16(output, exclusionContractVersion);
            output.putLong(principalGeneration);
            output.put(principalSha256.bytes().toByteArray());
            output.putLong(interlockGeneration);
            output.put(interlockSha256.bytes().toByteArray());
            putU16(output, evidenceKind);
            putU16(output, evidenceVersion);
            output.put(admissionEvidenceSha256.bytes().toByteArray());
            if (output.hasRemaining()) {
                throw new IllegalStateException("writer encoder did not fill its fixed allocation");
            }
            return output.array();
        }

        private static void requireNonZero(Sha256Digest digest, String field) {
            Objects.requireNonNull(digest, field);
            if (digest.isZero()) {
                throw new CapacityRejectedException(
                        RejectionCode.REGISTRY_UNAUTHORIZED_WRITER, field + " must be non-zero");
            }
        }
    }

    record Cohort(
            String cohortId,
            WriterRow row,
            CohortState state,
            boolean allocationCapable,
            boolean sourceQualified,
            boolean registryMember) {
        Cohort {
            Objects.requireNonNull(cohortId, "cohortId");
            Objects.requireNonNull(row, "row");
            Objects.requireNonNull(state, "state");
        }
    }

    record Scenario(String id, int writerCount, int registryBytesAtMaximumAssignments) {
        Scenario {
            Objects.requireNonNull(id, "id");
        }
    }

    static final class CohortLifecycle {
        private LifecyclePhase phase = LifecyclePhase.ABSENT;

        enum LifecyclePhase {
            ABSENT,
            PRESTART_COMMITTED,
            ACTIVE,
            FENCED,
            DRAINED,
            REVOKED,
            REMOVED
        }

        LifecyclePhase phase() {
            return phase;
        }

        void commitBeforeStart() {
            requirePhase(LifecyclePhase.ABSENT);
            phase = LifecyclePhase.PRESTART_COMMITTED;
        }

        void start() {
            requirePhase(LifecyclePhase.PRESTART_COMMITTED);
            phase = LifecyclePhase.ACTIVE;
        }

        void fence() {
            requirePhase(LifecyclePhase.ACTIVE);
            phase = LifecyclePhase.FENCED;
        }

        void drain() {
            requirePhase(LifecyclePhase.FENCED);
            phase = LifecyclePhase.DRAINED;
        }

        void revoke() {
            requirePhase(LifecyclePhase.DRAINED);
            phase = LifecyclePhase.REVOKED;
        }

        void remove() {
            requirePhase(LifecyclePhase.REVOKED);
            phase = LifecyclePhase.REMOVED;
        }

        private void requirePhase(LifecyclePhase expected) {
            if (phase != expected) {
                throw new CapacityRejectedException(
                        RejectionCode.REGISTRY_WRITER_LIFECYCLE_VIOLATION,
                        "expected " + expected + " but was " + phase);
            }
        }
    }

    static List<HeaderField> headerFields() {
        return List.of(
                new HeaderField("magic", 4),
                new HeaderField("schemaVersion", 2),
                new HeaderField("deploymentId", 16),
                new HeaderField("reservationDomainId", 16),
                new HeaderField("canonicalInstanceIdAscii", 36),
                new HeaderField("ledgerIdCompatibilityNamespaceId", 32),
                new HeaderField("reservedStartInclusive", 8),
                new HeaderField("reservedEndInclusive", 8),
                new HeaderField("sliceExponent", 2),
                new HeaderField("maxRegistryBytes", 4),
                new HeaderField("maxAssignmentsEver", 2),
                new HeaderField("maxAssignmentRowBytes", 2),
                new HeaderField("maxWriterCount", 2),
                new HeaderField("writerRowBytes", 2),
                new HeaderField("registryEpoch", 8),
                new HeaderField("registryAdmissionEvidenceKind", 2),
                new HeaderField("registryAdmissionEvidenceVersion", 2),
                new HeaderField("registryAdmissionEvidenceSha256", 32),
                new HeaderField("writerCount", 2),
                new HeaderField("assignmentCount", 2));
    }

    static int calculatedHeaderBytes() {
        int result = 0;
        for (HeaderField field : headerFields()) {
            result = checkedAdd(result, field.bytes());
        }
        return result;
    }

    static WriterRow writer(WriterKind kind, long generation, String label) {
        return new WriterRow(
                kind,
                1,
                generation,
                digest("principal/" + label),
                generation,
                digest("interlock/" + label),
                1,
                1,
                digest("evidence/" + label));
    }

    static int canonicalRegistryBytes(int writerCount, List<Integer> assignmentRows) {
        Objects.requireNonNull(assignmentRows, "assignmentRows");
        if (writerCount > MAX_WRITER_COUNT) {
            throw new CapacityRejectedException(
                    RejectionCode.REGISTRY_WRITER_COUNT_EXCEEDED,
                    "writerCount=" + writerCount + " max=" + MAX_WRITER_COUNT);
        }
        if (writerCount < 0) {
            throw new IllegalArgumentException("writer count must be non-negative");
        }
        if (assignmentRows.size() > MAX_ASSIGNMENTS_EVER) {
            throw new CapacityRejectedException(
                    RejectionCode.REGISTRY_ASSIGNMENT_COUNT_EXCEEDED,
                    "assignmentCount=" + assignmentRows.size() + " max=" + MAX_ASSIGNMENTS_EVER);
        }
        int assignmentBytes = 0;
        for (int rowBytes : assignmentRows) {
            if (rowBytes < 0 || rowBytes > MAX_ASSIGNMENT_ROW_BYTES) {
                throw new CapacityRejectedException(
                        RejectionCode.REGISTRY_ASSIGNMENT_ROW_BYTES_EXCEEDED,
                        "assignmentRowBytes=" + rowBytes + " max=" + MAX_ASSIGNMENT_ROW_BYTES);
            }
            assignmentBytes = checkedAdd(assignmentBytes, rowBytes);
        }
        int writerBytes = checkedMultiply(writerCount, WRITER_ROW_BYTES);
        int total = checkedAdd(calculatedHeaderBytes(), writerBytes, assignmentBytes);
        requireCanonicalBytesWithinBound(total);
        if (total > MAX_REGISTRY_BYTES) {
            throw new CapacityRejectedException(
                    RejectionCode.REGISTRY_CANONICAL_BYTES_EXCEEDED,
                    "canonicalBytes=" + total + " envelope=" + MAX_REGISTRY_BYTES);
        }
        return total;
    }

    static void requireCanonicalBytesWithinBound(int canonicalBytes) {
        if (canonicalBytes < 0 || canonicalBytes > MAX_CANONICAL_REGISTRY_BYTES) {
            throw new CapacityRejectedException(
                    RejectionCode.REGISTRY_CANONICAL_BYTES_EXCEEDED,
                    "canonicalBytes=" + canonicalBytes + " max=" + MAX_CANONICAL_REGISTRY_BYTES);
        }
    }

    static void validateWriterRows(List<WriterRow> rows) {
        Objects.requireNonNull(rows, "rows");
        if (rows.size() > MAX_WRITER_COUNT) {
            throw new CapacityRejectedException(
                    RejectionCode.REGISTRY_WRITER_COUNT_EXCEEDED,
                    "writerCount=" + rows.size() + " max=" + MAX_WRITER_COUNT);
        }
        List<WriterRow> sorted = rows.stream().sorted(WRITER_ORDER).toList();
        if (!rows.equals(sorted)) {
            throw new CapacityRejectedException(
                    RejectionCode.REGISTRY_UNAUTHORIZED_WRITER, "writer rows are not in canonical order");
        }
        Set<String> identities = new HashSet<>();
        Set<Sha256Digest> principals = new HashSet<>();
        for (WriterRow row : rows) {
            String identity = row.writerKind().code()
                    + "/"
                    + row.exclusionContractVersion()
                    + "/"
                    + row.principalGeneration()
                    + "/"
                    + row.principalSha256().toHex();
            if (!identities.add(identity) || !principals.add(row.principalSha256())) {
                throw new CapacityRejectedException(
                        RejectionCode.REGISTRY_UNAUTHORIZED_WRITER,
                        "duplicate identity or principal reused across writer kinds");
            }
        }
    }

    static void validateInventory(List<Cohort> inventory, List<WriterRow> registryRows) {
        Objects.requireNonNull(inventory, "inventory");
        Objects.requireNonNull(registryRows, "registryRows");
        validateWriterRows(registryRows.stream().sorted(WRITER_ORDER).toList());
        Set<WriterRow> members = Set.copyOf(registryRows);
        for (Cohort cohort : inventory) {
            boolean retainedState = cohort.state() == CohortState.FENCED_DRAINING
                    || cohort.state() == CohortState.REVOKED_PENDING_REMOVAL;
            if (cohort.allocationCapable()
                    && (cohort.registryMember() || retainedState)
                    && !members.contains(cohort.row())) {
                throw new CapacityRejectedException(
                        RejectionCode.REGISTRY_OMITTED_AUTHORIZED_WRITER,
                        "missing allocation-capable cohort " + cohort.cohortId());
            }
            if (members.contains(cohort.row()) && (!cohort.sourceQualified() || !cohort.registryMember())) {
                throw new CapacityRejectedException(
                        RejectionCode.REGISTRY_UNAUTHORIZED_WRITER,
                        "unqualified or unauthorized cohort " + cohort.cohortId());
            }
        }
    }

    static List<Scenario> scenarios() {
        return List.of(
                scenario("steady-native-bookkeeper", 1),
                scenario("steady-nereus-virtual-ledger", 1),
                scenario("combined-steady", 2),
                scenario("old-new-binary-coexistence", 4),
                scenario("binary-credential-two-by-two", 8),
                scenario("rollback-fresh-generations", 10),
                scenario("fenced-not-cleaned-residue", 12),
                scenario("allocation-capable-bootstrap-admin", 14));
    }

    static List<WriterRow> maximumRows() {
        List<WriterRow> rows = new ArrayList<>();
        long generation = 1;
        for (WriterKind kind : WriterKind.values()) {
            for (int slot = 1; slot <= COHORT_SLOTS_PER_KIND; slot++) {
                rows.add(writer(kind, generation++, kind.name().toLowerCase() + "/slot-" + slot));
            }
        }
        return rows.stream().sorted(WRITER_ORDER).toList();
    }

    static List<Integer> maximumAssignmentRows() {
        return java.util.Collections.nCopies(MAX_ASSIGNMENTS_EVER, MAX_ASSIGNMENT_ROW_BYTES);
    }

    static String renderEvidenceJson(String sourceCommit) {
        requireCommit(sourceCommit);
        String scenarioJson = scenarios().stream()
                .map(scenario -> """
                        {"id":"%s","writerCount":%d,"registryBytesAt256Rows":%d}""".formatted(
                                scenario.id(), scenario.writerCount(), scenario.registryBytesAtMaximumAssignments()))
                .collect(java.util.stream.Collectors.joining(",\n      "));
        return """
                {
                  "schemaVersion": 1,
                  "sourceTupleId": "v2-m0",
                  "result": "REGISTRY_CAPACITY_READINESS_ONLY",
                  "promotionEligible": false,
                  "registryConformance": false,
                  "productionRegistryAuthorityImplemented": false,
                  "allocatorModeSelected": false,
                  "runtimeActivated": false,
                  "scenarioPromotion": false,
                  "source": {
                    "nereusCommit": "%s",
                    "requiredBaselineCommit": "%s"
                  },
                  "fixedInputs": {
                    "writerKinds": 2,
                    "writerRowBytes": 120,
                    "fixedHeaderBytes": 184,
                    "maxRegistryEnvelopeBytes": 65536,
                    "maxAssignmentsEver": 256,
                    "maxAssignmentRowBytes": 192,
                    "sliceExponent": 40,
                    "oxiaExpectedVersionBytes": 8
                  },
                  "cohortBound": {
                    "slotsPerWriterKind": 7,
                    "maxWriterCount": 14,
                    "binaryCredentialMatrixPerKind": 4,
                    "rollbackPerKind": 1,
                    "fencedResiduePerKind": 1,
                    "allocationCapableBootstrapAdminPerKind": 1,
                    "rowMeaning": "SOURCE_QUALIFIED_COHORT_AND_INDEPENDENTLY_REVOCABLE_PRINCIPAL_GENERATION"
                  },
                  "formula": "184 + writerCount * 120 + sum(assignmentRowCanonicalBytes)",
                  "maximumBreakdown": {
                    "fixedHeaderBytes": 184,
                    "writerBytes": 1680,
                    "assignmentBytes": 49152,
                    "canonicalRegistryBytes": 51016,
                    "inheritedEnvelopeBytes": 65536,
                    "reservedMarginBytes": 14520,
                    "oxiaValueBytes": 51016,
                    "oxiaCasCandidateValueBytes": 51016,
                    "oxiaCasExpectedVersionBytes": 8,
                    "oxiaValuePlusVersionOperandsBytes": 51024,
                    "keyAndRpcFramingIncluded": false
                  },
                  "scenarios": [
                    %s
                  ],
                  "rejectionCodes": [
                    "REGISTRY_WRITER_COUNT_EXCEEDED",
                    "REGISTRY_CANONICAL_BYTES_EXCEEDED",
                    "REGISTRY_ASSIGNMENT_COUNT_EXCEEDED",
                    "REGISTRY_ASSIGNMENT_ROW_BYTES_EXCEEDED",
                    "REGISTRY_OMITTED_AUTHORIZED_WRITER",
                    "REGISTRY_UNAUTHORIZED_WRITER",
                    "REGISTRY_WRITER_LIFECYCLE_VIOLATION"
                  ],
                  "testEvidence": {
                    "expectedFocusedTests": 18,
                    "expectedFailures": 0,
                    "expectedErrors": 0,
                    "expectedSkipped": 0,
                    "dynamicTests": false,
                    "internalRetries": false
                  },
                  "modelSha256": "%s",
                  "limitations": [
                    "NO_R1_PRODUCTION_CODEC_STORE_OR_INTERLOCK",
                    "NO_REAL_OXIA_CONFORMANCE",
                    "NO_ALLOCATOR_MODE_SELECTION",
                    "NO_SCENARIO_PROMOTION",
                    "NO_10K_OR_100K_SCALE_BENCHMARK"
                  ]
                }
                """.formatted(sourceCommit, BASELINE_COMMIT, scenarioJson, modelSha256());
    }

    static String renderEvidenceMarkdown(String sourceCommit, String jsonSha256) {
        requireCommit(sourceCommit);
        if (!Pattern.matches("[0-9a-f]{64}", jsonSha256)) {
            throw new IllegalArgumentException("JSON SHA-256 must be lowercase hexadecimal");
        }
        return """
                # M1.1c-R0 Registry capacity readiness evidence

                ## Result boundary

                `REGISTRY_CAPACITY_READINESS_ONLY`; `promotionEligible=false`; `registryConformance=false`.

                This deterministic test/evidence-only artifact binds Nereus `%s`, 18 focused tests, the exact
                184 + writerCount * 120 + assignment-row-sum formula, and the full bounded cohort lifecycle. It does
                not implement R1 production authority, select an allocator, run real Oxia, promote any V2-POSITION
                scenario, or emit `REGISTRY_CONFORMANCE`/`HARNESS_CONFORMANCE_ONLY`.

                ## Derived boundary

                - writer kinds: 2;
                - source-qualified independently revocable cohort slots per kind: 7;
                - `maxWriterCount=14`;
                - maximum canonical Registry value and Oxia CAS candidate value: 51,016 bytes;
                - expected-version operand: 8 bytes; combined value/version operands: 51,024 bytes;
                - inherited envelope: 65,536 bytes; reserved margin: 14,520 bytes;
                - exact boundary errors: `REGISTRY_WRITER_COUNT_EXCEEDED` and
                  `REGISTRY_CANONICAL_BYTES_EXCEEDED`.

                The 120-byte writer row, 192-byte full assignment-row contribution, and 256 lifetime-assignment limit
                are unchanged. The margin cannot admit a fifteenth writer or hidden field.

                ## Artifact identity

                - JSON: `registry-capacity.json`
                - JSON SHA-256: `%s`
                - required baseline: `%s`
                - expected focused tests: 18, failures/errors/skipped: 0/0/0
                """.formatted(sourceCommit, jsonSha256, BASELINE_COMMIT);
    }

    static String sha256(String value) {
        return digest(value).toHex();
    }

    static int checkedAdd(int... values) {
        int result = 0;
        for (int value : values) {
            result = Math.addExact(result, value);
        }
        return result;
    }

    static int checkedMultiply(int left, int right) {
        return Math.multiplyExact(left, right);
    }

    private static Scenario scenario(String id, int writerCount) {
        return new Scenario(id, writerCount, canonicalRegistryBytes(writerCount, maximumAssignmentRows()));
    }

    private static String modelSha256() {
        String scenarios = RegistryCapacityHarness.scenarios().stream()
                .map(value -> value.id() + ":" + value.writerCount() + ":" + value.registryBytesAtMaximumAssignments())
                .collect(java.util.stream.Collectors.joining("|"));
        String errors = java.util.Arrays.stream(RejectionCode.values())
                .map(Enum::name)
                .collect(java.util.stream.Collectors.joining("|"));
        return sha256("header=184;writer=120;assignment=192;assignments=256;envelope=65536;maxWriters=14;"
                + scenarios
                + ";errors="
                + errors);
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static void putU16(ByteBuffer output, int value) {
        if (value < 0 || value > 0xffff) {
            throw new IllegalArgumentException("u16 value is out of range");
        }
        output.putShort((short) value);
    }

    private static void requireCommit(String sourceCommit) {
        Objects.requireNonNull(sourceCommit, "sourceCommit");
        if (!COMMIT.matcher(sourceCommit).matches()) {
            throw new IllegalArgumentException("source commit must be 40 lowercase hexadecimal characters");
        }
    }
}
