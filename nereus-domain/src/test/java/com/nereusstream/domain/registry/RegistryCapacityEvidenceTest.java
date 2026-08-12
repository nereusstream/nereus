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

import static com.nereusstream.domain.registry.RegistryCapacityHarness.MAX_CANONICAL_REGISTRY_BYTES;
import static com.nereusstream.domain.registry.RegistryCapacityHarness.MAX_WRITER_COUNT;
import static com.nereusstream.domain.registry.RegistryCapacityHarness.RejectionCode.REGISTRY_ASSIGNMENT_COUNT_EXCEEDED;
import static com.nereusstream.domain.registry.RegistryCapacityHarness.RejectionCode.REGISTRY_ASSIGNMENT_ROW_BYTES_EXCEEDED;
import static com.nereusstream.domain.registry.RegistryCapacityHarness.RejectionCode.REGISTRY_CANONICAL_BYTES_EXCEEDED;
import static com.nereusstream.domain.registry.RegistryCapacityHarness.RejectionCode.REGISTRY_OMITTED_AUTHORIZED_WRITER;
import static com.nereusstream.domain.registry.RegistryCapacityHarness.RejectionCode.REGISTRY_UNAUTHORIZED_WRITER;
import static com.nereusstream.domain.registry.RegistryCapacityHarness.RejectionCode.REGISTRY_WRITER_COUNT_EXCEEDED;
import static com.nereusstream.domain.registry.RegistryCapacityHarness.RejectionCode.REGISTRY_WRITER_LIFECYCLE_VIOLATION;
import static com.nereusstream.domain.registry.RegistryCapacityHarness.WriterKind.NATIVE_BOOKKEEPER_LEDGER_ID;
import static com.nereusstream.domain.registry.RegistryCapacityHarness.WriterKind.NEREUS_VIRTUAL_LEDGER_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.RegistryCapacityHarness.CapacityRejectedException;
import com.nereusstream.domain.registry.RegistryCapacityHarness.Cohort;
import com.nereusstream.domain.registry.RegistryCapacityHarness.CohortLifecycle;
import com.nereusstream.domain.registry.RegistryCapacityHarness.CohortState;
import com.nereusstream.domain.registry.RegistryCapacityHarness.WriterKind;
import com.nereusstream.domain.registry.RegistryCapacityHarness.WriterRow;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class RegistryCapacityEvidenceTest {
    @Test
    void writerRowEncodingIsExactly120BytesAndDeterministic() {
        WriterRow row = RegistryCapacityHarness.writer(NATIVE_BOOKKEEPER_LEDGER_ID, 1, "steady-native");

        assertThat(row.encode()).hasSize(120).containsExactly(row.encode());
        assertThat(RegistryCapacityHarness.sha256(new String(row.encode(), StandardCharsets.ISO_8859_1)))
                .hasSize(64);
    }

    @Test
    void fixedHeaderBreakdownIsExactly184Bytes() {
        assertThat(RegistryCapacityHarness.headerFields()).hasSize(20);
        assertThat(RegistryCapacityHarness.calculatedHeaderBytes()).isEqualTo(184);
        assertThat(RegistryCapacityHarness.headerFields())
                .extracting(RegistryCapacityHarness.HeaderField::name)
                .containsExactly(
                        "magic",
                        "schemaVersion",
                        "deploymentId",
                        "reservationDomainId",
                        "canonicalInstanceIdAscii",
                        "ledgerIdCompatibilityNamespaceId",
                        "reservedStartInclusive",
                        "reservedEndInclusive",
                        "sliceExponent",
                        "maxRegistryBytes",
                        "maxAssignmentsEver",
                        "maxAssignmentRowBytes",
                        "maxWriterCount",
                        "writerRowBytes",
                        "registryEpoch",
                        "registryAdmissionEvidenceKind",
                        "registryAdmissionEvidenceVersion",
                        "registryAdmissionEvidenceSha256",
                        "writerCount",
                        "assignmentCount");
    }

    @Test
    void derivesEveryCohortScenarioWithoutAssumingEight() {
        assertThat(RegistryCapacityHarness.scenarios())
                .extracting(RegistryCapacityHarness.Scenario::writerCount)
                .containsExactly(1, 1, 2, 4, 8, 10, 12, 14);
        assertThat(RegistryCapacityHarness.scenarios())
                .extracting(RegistryCapacityHarness.Scenario::id)
                .containsExactly(
                        "steady-native-bookkeeper",
                        "steady-nereus-virtual-ledger",
                        "combined-steady",
                        "old-new-binary-coexistence",
                        "binary-credential-two-by-two",
                        "rollback-fresh-generations",
                        "fenced-not-cleaned-residue",
                        "allocation-capable-bootstrap-admin");
    }

    @Test
    void derivesWorstCaseRegistryAndOxiaOperands() {
        int canonicalBytes = RegistryCapacityHarness.canonicalRegistryBytes(
                MAX_WRITER_COUNT, RegistryCapacityHarness.maximumAssignmentRows());

        assertThat(canonicalBytes).isEqualTo(51_016);
        assertThat(MAX_CANONICAL_REGISTRY_BYTES).isEqualTo(51_016);
        assertThat(RegistryCapacityHarness.MAX_REGISTRY_BYTES - canonicalBytes).isEqualTo(14_520);
        assertThat(canonicalBytes + RegistryCapacityHarness.OXIA_EXPECTED_VERSION_BYTES)
                .isEqualTo(51_024);
    }

    @Test
    void admitsFourteenAndRejectsFifteenthWithStableCode() {
        assertThat(RegistryCapacityHarness.maximumRows()).hasSize(14);
        RegistryCapacityHarness.validateWriterRows(RegistryCapacityHarness.maximumRows());

        assertRejected(
                () -> RegistryCapacityHarness.canonicalRegistryBytes(
                        15, RegistryCapacityHarness.maximumAssignmentRows()),
                REGISTRY_WRITER_COUNT_EXCEEDED);
        assertThat(184 + 15 * 120 + 256 * 192).isLessThan(RegistryCapacityHarness.MAX_REGISTRY_BYTES);
    }

    @Test
    void admits51016AndRejects51017WithStableCode() {
        RegistryCapacityHarness.requireCanonicalBytesWithinBound(51_016);

        assertRejected(
                () -> RegistryCapacityHarness.requireCanonicalBytesWithinBound(51_017),
                REGISTRY_CANONICAL_BYTES_EXCEEDED);
        assertRejected(
                () -> RegistryCapacityHarness.requireCanonicalBytesWithinBound(65_536),
                REGISTRY_CANONICAL_BYTES_EXCEEDED);
    }

    @Test
    void enforcesAssignmentCount256And257() {
        assertThat(RegistryCapacityHarness.canonicalRegistryBytes(0, maximumRows(256, 1)))
                .isEqualTo(440);

        assertRejected(
                () -> RegistryCapacityHarness.canonicalRegistryBytes(0, maximumRows(257, 1)),
                REGISTRY_ASSIGNMENT_COUNT_EXCEEDED);
    }

    @Test
    void enforcesAssignmentRow192And193() {
        assertThat(RegistryCapacityHarness.canonicalRegistryBytes(0, List.of(192)))
                .isEqualTo(376);

        assertRejected(
                () -> RegistryCapacityHarness.canonicalRegistryBytes(0, List.of(193)),
                REGISTRY_ASSIGNMENT_ROW_BYTES_EXCEEDED);
    }

    @Test
    void checkedArithmeticRejectsOverflow() {
        assertThatThrownBy(() -> RegistryCapacityHarness.checkedAdd(Integer.MAX_VALUE, 1))
                .isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> RegistryCapacityHarness.checkedMultiply(Integer.MAX_VALUE, 2))
                .isInstanceOf(ArithmeticException.class);
    }

    @Test
    void rejectsDuplicateIdentityAndCrossKindPrincipalReuse() {
        WriterRow nativeRow = RegistryCapacityHarness.writer(NATIVE_BOOKKEEPER_LEDGER_ID, 1, "shared");
        assertRejected(
                () -> RegistryCapacityHarness.validateWriterRows(List.of(nativeRow, nativeRow)),
                REGISTRY_UNAUTHORIZED_WRITER);

        WriterRow virtualRow = new WriterRow(
                NEREUS_VIRTUAL_LEDGER_ID,
                1,
                2,
                nativeRow.principalSha256(),
                2,
                RegistryCapacityHarness.writer(NEREUS_VIRTUAL_LEDGER_ID, 2, "virtual")
                        .interlockSha256(),
                1,
                1,
                RegistryCapacityHarness.writer(NEREUS_VIRTUAL_LEDGER_ID, 2, "virtual")
                        .admissionEvidenceSha256());
        assertRejected(
                () -> RegistryCapacityHarness.validateWriterRows(List.of(nativeRow, virtualRow)),
                REGISTRY_UNAUTHORIZED_WRITER);
    }

    @Test
    void rejectsZeroGenerationDigestAndUnknownCodes() {
        WriterRow valid = RegistryCapacityHarness.writer(NATIVE_BOOKKEEPER_LEDGER_ID, 1, "valid");
        assertRejected(
                () -> new WriterRow(
                        valid.writerKind(),
                        1,
                        0,
                        valid.principalSha256(),
                        1,
                        valid.interlockSha256(),
                        1,
                        1,
                        valid.admissionEvidenceSha256()),
                REGISTRY_UNAUTHORIZED_WRITER);
        assertRejected(
                () -> new WriterRow(
                        valid.writerKind(),
                        1,
                        1,
                        Sha256Digest.copyOf(new byte[32]),
                        1,
                        valid.interlockSha256(),
                        1,
                        1,
                        valid.admissionEvidenceSha256()),
                REGISTRY_UNAUTHORIZED_WRITER);
        assertRejected(() -> WriterKind.fromCode(3), REGISTRY_UNAUTHORIZED_WRITER);
    }

    @Test
    void rejectsOmittedAuthorizedAndUnqualifiedWriter() {
        WriterRow row = RegistryCapacityHarness.writer(NATIVE_BOOKKEEPER_LEDGER_ID, 1, "native");
        Cohort omitted = new Cohort("native", row, CohortState.ACTIVE, true, true, true);
        assertRejected(
                () -> RegistryCapacityHarness.validateInventory(List.of(omitted), List.of()),
                REGISTRY_OMITTED_AUTHORIZED_WRITER);

        Cohort unqualified = new Cohort("native", row, CohortState.ACTIVE, true, false, true);
        assertRejected(
                () -> RegistryCapacityHarness.validateInventory(List.of(unqualified), List.of(row)),
                REGISTRY_UNAUTHORIZED_WRITER);
    }

    @Test
    void requiresAddBeforeStart() {
        CohortLifecycle lifecycle = new CohortLifecycle();
        assertRejected(lifecycle::start, REGISTRY_WRITER_LIFECYCLE_VIOLATION);

        lifecycle.commitBeforeStart();
        lifecycle.start();
        assertThat(lifecycle.phase()).isEqualTo(CohortLifecycle.LifecyclePhase.ACTIVE);
    }

    @Test
    void requiresFenceDrainRevokeBeforeRemove() {
        CohortLifecycle lifecycle = activeLifecycle();
        assertRejected(lifecycle::remove, REGISTRY_WRITER_LIFECYCLE_VIOLATION);
        lifecycle.fence();
        assertRejected(lifecycle::remove, REGISTRY_WRITER_LIFECYCLE_VIOLATION);
        lifecycle.drain();
        assertRejected(lifecycle::remove, REGISTRY_WRITER_LIFECYCLE_VIOLATION);
        lifecycle.revoke();
        lifecycle.remove();
        assertThat(lifecycle.phase()).isEqualTo(CohortLifecycle.LifecyclePhase.REMOVED);
    }

    @Test
    void retainsFencedAndRevokedResidueUntilRemoval() {
        WriterRow row = RegistryCapacityHarness.writer(NEREUS_VIRTUAL_LEDGER_ID, 1, "residue");
        Cohort fenced = new Cohort("fenced", row, CohortState.FENCED_DRAINING, true, true, true);
        RegistryCapacityHarness.validateInventory(List.of(fenced), List.of(row));

        Cohort revoked = new Cohort("revoked", row, CohortState.REVOKED_PENDING_REMOVAL, true, true, true);
        RegistryCapacityHarness.validateInventory(List.of(revoked), List.of(row));
    }

    @Test
    void classifiesBootstrapAdminByAllocationCapability() {
        WriterRow row = RegistryCapacityHarness.writer(NATIVE_BOOKKEEPER_LEDGER_ID, 1, "bootstrap-admin");
        Cohort controlOnly = new Cohort("control-only", row, CohortState.ACTIVE, false, true, false);
        RegistryCapacityHarness.validateInventory(List.of(controlOnly), List.of());

        Cohort allocationCapable = new Cohort("allocation-admin", row, CohortState.ACTIVE, true, true, true);
        RegistryCapacityHarness.validateInventory(List.of(allocationCapable), List.of(row));
        assertRejected(
                () -> RegistryCapacityHarness.validateInventory(List.of(allocationCapable), List.of()),
                REGISTRY_OMITTED_AUTHORIZED_WRITER);
    }

    @Test
    void rollbackUsesFreshPrincipalGeneration() {
        WriterRow revoked = RegistryCapacityHarness.writer(NATIVE_BOOKKEEPER_LEDGER_ID, 41, "old-binary");
        WriterRow rollback = RegistryCapacityHarness.writer(NATIVE_BOOKKEEPER_LEDGER_ID, 42, "rollback-binary");

        assertThat(rollback.principalGeneration()).isGreaterThan(revoked.principalGeneration());
        assertThat(rollback.principalSha256()).isNotEqualTo(revoked.principalSha256());
        assertThat(rollback.admissionEvidenceSha256()).isNotEqualTo(revoked.admissionEvidenceSha256());
    }

    @Test
    void rendersDeterministicJsonAndMarkdownEvidence() throws Exception {
        String sourceCommit =
                Files.readString(sourceCommitPath(), StandardCharsets.US_ASCII).trim();
        String firstJson = RegistryCapacityHarness.renderEvidenceJson(sourceCommit);
        String secondJson = RegistryCapacityHarness.renderEvidenceJson(sourceCommit);
        String jsonSha256 = RegistryCapacityHarness.sha256(firstJson);
        String firstMarkdown = RegistryCapacityHarness.renderEvidenceMarkdown(sourceCommit, jsonSha256);
        String secondMarkdown = RegistryCapacityHarness.renderEvidenceMarkdown(sourceCommit, jsonSha256);

        assertThat(firstJson)
                .isEqualTo(secondJson)
                .contains("\"result\": \"REGISTRY_CAPACITY_READINESS_ONLY\"")
                .contains("\"promotionEligible\": false")
                .contains("\"registryConformance\": false")
                .contains("\"maxWriterCount\": 14")
                .contains("\"canonicalRegistryBytes\": 51016");
        assertThat(firstMarkdown).isEqualTo(secondMarkdown).contains(jsonSha256);

        Path reportRoot = Path.of("build/reports/v2-m1-registry-capacity");
        Files.createDirectories(reportRoot);
        Files.writeString(reportRoot.resolve("registry-capacity.json"), firstJson, StandardCharsets.UTF_8);
        Files.writeString(reportRoot.resolve("README.md"), firstMarkdown, StandardCharsets.UTF_8);
    }

    private static Path sourceCommitPath() {
        return Path.of("..", "docs", "v2", "evidence", "v2-m0", "m1.1c-r0", "source-commit.txt")
                .normalize();
    }

    private static List<Integer> maximumRows(int count, int rowBytes) {
        List<Integer> rows = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            rows.add(rowBytes);
        }
        return rows;
    }

    private static CohortLifecycle activeLifecycle() {
        CohortLifecycle lifecycle = new CohortLifecycle();
        lifecycle.commitBeforeStart();
        lifecycle.start();
        return lifecycle;
    }

    private static void assertRejected(Runnable operation, RegistryCapacityHarness.RejectionCode expected) {
        assertThatThrownBy(operation::run)
                .isInstanceOf(CapacityRejectedException.class)
                .extracting(error -> ((CapacityRejectedException) error).code())
                .isEqualTo(expected);
    }
}
