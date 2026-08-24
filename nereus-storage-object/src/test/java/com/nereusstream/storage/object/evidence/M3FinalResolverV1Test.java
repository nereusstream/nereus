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

package com.nereusstream.storage.object.evidence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.storage.object.evidence.M3FinalReceiptV1.AllocatorMode;
import com.nereusstream.storage.object.evidence.M3FinalReceiptV1.AllocatorSelection;
import com.nereusstream.storage.object.evidence.M3FinalReceiptV1.AttachmentKind;
import com.nereusstream.storage.object.evidence.M3FinalReceiptV1.AttachmentRef;
import com.nereusstream.storage.object.evidence.M3FinalReceiptV1.ChildKind;
import com.nereusstream.storage.object.evidence.M3FinalReceiptV1.ChildReceiptRef;
import com.nereusstream.storage.object.evidence.M3FinalReceiptV1.ChildSourceTuple;
import com.nereusstream.storage.object.evidence.M3FinalReceiptV1.Exclusion;
import com.nereusstream.storage.object.evidence.M3FinalReceiptV1.ProviderEvidence;
import com.nereusstream.storage.object.evidence.M3FinalReceiptV1.Receipt;
import com.nereusstream.storage.object.evidence.M3FinalReceiptV1.ReceiptKind;
import com.nereusstream.storage.object.evidence.M3FinalReceiptV1.ReceiptRejectedException;
import com.nereusstream.storage.object.evidence.M3FinalReceiptV1.ReceiptResult;
import com.nereusstream.storage.object.evidence.M3FinalReceiptV1.RejectionCode;
import com.nereusstream.storage.object.evidence.M3FinalReceiptV1.RootSourceTuple;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class M3FinalResolverV1Test {
    private static final String COMMIT = "1234567890123456789012345678901234567890";
    private static final String LOCKS_SHA = "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a";

    @TempDir
    Path temporaryDirectory;

    private Receipt receipt;
    private Path receiptFile;

    @BeforeEach
    void setUp() throws Exception {
        receipt = fixture();
        receiptFile = temporaryDirectory.resolve("m3-final.json");
        Files.write(receiptFile, M3FinalReceiptV1.canonicalBytes(receipt));
    }

    @Test
    void resolvesCompleteExactSourceFinal() {
        M3FinalResolverV1.Resolution resolution = M3FinalResolverV1.resolve(temporaryDirectory, receiptFile);

        assertThat(resolution.sourceTuple().nereusCommit()).isEqualTo(COMMIT);
        assertThat(resolution.promotedScenarios())
                .containsExactlyInAnyOrderElementsOf(M3FinalReceiptV1.requiredScenarios());
        assertThat(resolution.childReceipts()).isEqualTo(ChildKind.values().length);
        assertThat(resolution.attachments()).isGreaterThanOrEqualTo(ChildKind.values().length);
        assertThat(resolution.tests()).isPositive();
        assertThat(resolution.verifiedBytes()).isPositive();
    }

    @Test
    void roundTripsExactCanonicalBytes() {
        byte[] canonical = M3FinalReceiptV1.canonicalBytes(receipt);
        assertThat(M3FinalReceiptV1.canonicalBytes(M3FinalReceiptV1.parseCanonical(canonical)))
                .isEqualTo(canonical);
    }

    @Test
    void rejectsWhitespaceOrTrailingBytes() {
        assertRejected(
                () -> M3FinalReceiptV1.parseCanonical(
                        (new String(M3FinalReceiptV1.canonicalBytes(receipt), StandardCharsets.UTF_8) + "\n")
                                .getBytes(StandardCharsets.UTF_8)),
                RejectionCode.MALFORMED_OR_NON_CANONICAL);
    }

    @Test
    void rejectsMissingOrReorderedChild() {
        List<ChildReceiptRef> missing = new ArrayList<>(receipt.childReceipts());
        missing.remove(0);
        assertRejected(() -> M3FinalReceiptV1.canonicalBytes(copy(receipt, missing)), RejectionCode.CHILD_SET_INVALID);

        List<ChildReceiptRef> reordered = new ArrayList<>(receipt.childReceipts());
        ChildReceiptRef first = reordered.get(0);
        reordered.set(0, reordered.get(1));
        reordered.set(1, first);
        assertRejected(
                () -> M3FinalReceiptV1.canonicalBytes(copy(receipt, reordered)), RejectionCode.CHILD_SET_INVALID);
    }

    @Test
    void rejectsChildSourceThatDiffersFromFinal() {
        ChildReceiptRef child = receipt.childReceipts().get(0);
        ChildReceiptRef changed = new ChildReceiptRef(
                child.attachments(),
                child.bytes(),
                child.errors(),
                child.exclusions(),
                child.failures(),
                child.kind(),
                child.path(),
                child.promotionEligible(),
                child.result(),
                child.sha256(),
                child.skipped(),
                new ChildSourceTuple(
                        "f".repeat(40),
                        child.sourceTuple().sourceTupleId(),
                        child.sourceTuple().sourceTupleSha256()),
                child.tests());

        assertRejected(
                () -> M3FinalReceiptV1.canonicalBytes(replaceChild(receipt, 0, changed)),
                RejectionCode.SOURCE_TUPLE_INVALID);
    }

    @Test
    void rejectsZeroTestsAndEveryNonzeroBadCounter() {
        ChildReceiptRef child = receipt.childReceipts().get(0);
        for (int variant = 0; variant < 4; variant++) {
            long tests = variant == 0 ? 0 : child.tests();
            long errors = variant == 1 ? 1 : 0;
            long failures = variant == 2 ? 1 : 0;
            long skipped = variant == 3 ? 1 : 0;
            ChildReceiptRef changed = new ChildReceiptRef(
                    child.attachments(),
                    child.bytes(),
                    errors,
                    child.exclusions(),
                    failures,
                    child.kind(),
                    child.path(),
                    false,
                    child.result(),
                    child.sha256(),
                    skipped,
                    child.sourceTuple(),
                    tests);
            assertRejected(
                    () -> M3FinalReceiptV1.canonicalBytes(replaceChild(receipt, 0, changed)),
                    RejectionCode.CHILD_RESULT_NOT_PASS);
        }
    }

    @Test
    void rejectsPromotableChild() {
        ChildReceiptRef child = receipt.childReceipts().get(0);
        ChildReceiptRef changed = new ChildReceiptRef(
                child.attachments(),
                child.bytes(),
                child.errors(),
                child.exclusions(),
                child.failures(),
                child.kind(),
                child.path(),
                true,
                child.result(),
                child.sha256(),
                child.skipped(),
                child.sourceTuple(),
                child.tests());
        assertRejected(
                () -> M3FinalReceiptV1.canonicalBytes(replaceChild(receipt, 0, changed)),
                RejectionCode.CHILD_RESULT_NOT_PASS);
    }

    @Test
    void rejectsFakeProviderOrKmsAndC2Promotion() {
        for (ProviderEvidence provider : List.of(
                new ProviderEvidence(false, false, true),
                new ProviderEvidence(false, true, false),
                new ProviderEvidence(true, true, true))) {
            Receipt changed = new Receipt(
                    receipt.allocatorSelection(),
                    receipt.childReceipts(),
                    receipt.exclusions(),
                    receipt.kind(),
                    receipt.promotionEligible(),
                    provider,
                    receipt.result(),
                    receipt.scenarios(),
                    receipt.schema(),
                    receipt.sourceTuple());
            assertRejected(() -> M3FinalReceiptV1.canonicalBytes(changed), RejectionCode.PROVIDER_EVIDENCE_INVALID);
        }
    }

    @Test
    void rejectsIncompleteAllocatorEvidence() {
        for (AllocatorSelection allocator : List.of(
                new AllocatorSelection(false, AllocatorMode.STRICT, true, true, true),
                new AllocatorSelection(true, AllocatorMode.STRICT, false, true, true),
                new AllocatorSelection(true, AllocatorMode.RANGE, true, false, true),
                new AllocatorSelection(true, AllocatorMode.RANGE, true, true, false))) {
            Receipt changed = new Receipt(
                    allocator,
                    receipt.childReceipts(),
                    receipt.exclusions(),
                    receipt.kind(),
                    receipt.promotionEligible(),
                    receipt.providerEvidence(),
                    receipt.result(),
                    receipt.scenarios(),
                    receipt.schema(),
                    receipt.sourceTuple());
            assertRejected(() -> M3FinalReceiptV1.canonicalBytes(changed), RejectionCode.ALLOCATOR_EVIDENCE_INVALID);
        }
    }

    @Test
    void rejectsScenarioBorrowingOrOmission() {
        List<String> borrowed = new ArrayList<>(receipt.scenarios());
        borrowed.set(0, "V2-POLICY-001");
        assertRejected(
                () -> M3FinalReceiptV1.canonicalBytes(copyScenarios(receipt, borrowed)),
                RejectionCode.SCENARIO_SET_INVALID);
        List<String> missing = new ArrayList<>(receipt.scenarios());
        missing.remove(0);
        assertRejected(
                () -> M3FinalReceiptV1.canonicalBytes(copyScenarios(receipt, missing)),
                RejectionCode.SCENARIO_SET_INVALID);
    }

    @Test
    void rejectsRemovedM6OrM8Exclusion() {
        Receipt changed = new Receipt(
                receipt.allocatorSelection(),
                receipt.childReceipts(),
                List.of(Exclusion.M6_PROCESS_ACTIVATION),
                receipt.kind(),
                receipt.promotionEligible(),
                receipt.providerEvidence(),
                receipt.result(),
                receipt.scenarios(),
                receipt.schema(),
                receipt.sourceTuple());
        assertRejected(() -> M3FinalReceiptV1.canonicalBytes(changed), RejectionCode.EXCLUSION_SET_INVALID);
    }

    @Test
    void rejectsC2ThatCanSubstituteForC1() {
        ChildReceiptRef child = receipt.childReceipts().get(5);
        ChildReceiptRef changed = new ChildReceiptRef(
                child.attachments(),
                child.bytes(),
                child.errors(),
                List.of(Exclusion.M3_FINAL_AGGREGATE, Exclusion.SCENARIO_PROMOTION),
                child.failures(),
                child.kind(),
                child.path(),
                child.promotionEligible(),
                child.result(),
                child.sha256(),
                child.skipped(),
                child.sourceTuple(),
                child.tests());
        assertRejected(
                () -> M3FinalReceiptV1.canonicalBytes(replaceChild(receipt, 5, changed)),
                RejectionCode.EXCLUSION_SET_INVALID);
    }

    @Test
    void rejectsMissingTypedRealOrAllocatorAttachment() {
        for (int childIndex : List.of(3, 4, 10)) {
            ChildReceiptRef child = receipt.childReceipts().get(childIndex);
            AttachmentKind removedKind =
                    switch (childIndex) {
                        case 3 -> AttachmentKind.LOCAL_CAP_RESULT;
                        case 4 -> AttachmentKind.KMS_REAL_RECEIPT;
                        default -> AttachmentKind.ALLOCATOR_FAULT_SUMMARY;
                    };
            List<AttachmentRef> reduced = child.attachments().stream()
                    .filter(attachment -> attachment.kind() != removedKind)
                    .toList();
            ChildReceiptRef changed = new ChildReceiptRef(
                    reduced,
                    child.bytes(),
                    child.errors(),
                    child.exclusions(),
                    child.failures(),
                    child.kind(),
                    child.path(),
                    child.promotionEligible(),
                    child.result(),
                    child.sha256(),
                    child.skipped(),
                    child.sourceTuple(),
                    child.tests());
            assertRejected(
                    () -> M3FinalReceiptV1.canonicalBytes(replaceChild(receipt, childIndex, changed)),
                    RejectionCode.ATTACHMENT_SET_INVALID);
        }
    }

    @Test
    void rejectsTamperedChildReceipt() throws Exception {
        ChildReceiptRef child = receipt.childReceipts().get(0);
        Path path = temporaryDirectory.resolve(child.path());
        Files.writeString(path, "tampered", StandardCharsets.UTF_8);

        assertRejected(
                () -> M3FinalResolverV1.resolve(temporaryDirectory, receiptFile), RejectionCode.ATTACHMENT_SET_INVALID);
    }

    @Test
    void rejectsReboundArbitraryChildBytesThatPreviouslyPassedHashOnlyResolution() throws Exception {
        int index = 1;
        ChildReceiptRef child = receipt.childReceipts().get(index);
        byte[] fake = "{\"promotionEligible\":false,\"schema\":\"FAKE_CHILD\"}".getBytes(StandardCharsets.UTF_8);
        Files.write(temporaryDirectory.resolve(child.path()), fake);
        ChildReceiptRef rebound = new ChildReceiptRef(
                child.attachments(),
                fake.length,
                child.errors(),
                child.exclusions(),
                child.failures(),
                child.kind(),
                child.path(),
                child.promotionEligible(),
                child.result(),
                sha(fake),
                child.skipped(),
                child.sourceTuple(),
                child.tests());
        Receipt reboundReceipt = replaceChild(receipt, index, rebound);
        Files.write(receiptFile, M3FinalReceiptV1.canonicalBytes(reboundReceipt));

        assertRejected(
                () -> M3FinalResolverV1.resolve(temporaryDirectory, receiptFile),
                RejectionCode.MALFORMED_OR_NON_CANONICAL);
    }

    @Test
    void rejectsSourceLocksThatDifferFromFinalSourceTuple() throws Exception {
        Files.writeString(
                temporaryDirectory.resolve("docs/v2/source-locks.json"),
                "{\"callerClaimedRealProvider\":true}",
                StandardCharsets.UTF_8);

        assertRejected(
                () -> M3FinalResolverV1.resolve(temporaryDirectory, receiptFile), RejectionCode.SOURCE_TUPLE_INVALID);
    }

    @Test
    void rejectsSymlinkInAttachmentPath() throws Exception {
        ChildReceiptRef child = receipt.childReceipts().get(0);
        AttachmentRef attachment = child.attachments().get(0);
        Path path = temporaryDirectory.resolve(attachment.path());
        Path moved = temporaryDirectory.resolve("moved-evidence.txt");
        Files.move(path, moved);
        Files.createSymbolicLink(path, moved);

        assertRejected(
                () -> M3FinalResolverV1.resolve(temporaryDirectory, receiptFile), RejectionCode.ATTACHMENT_SET_INVALID);
    }

    private Receipt fixture() throws Exception {
        write("docs/v2/source-locks.json", "{}".getBytes(StandardCharsets.UTF_8));
        List<ChildReceiptRef> children = new ArrayList<>();
        ChildKind[] kinds = ChildKind.values();
        for (int index = 0; index < kinds.length; index++) {
            ChildKind kind = kinds[index];
            List<AttachmentRef> attachments = new ArrayList<>();
            String childPath;
            long tests;
            String sourceId;
            String sourceSha;
            byte[] childBytes;
            if (kind == ChildKind.W1_CURRENT_SOURCE_M2_REGRESSION) {
                childPath = "docs/v2/evidence/v2-m3/w1/m2-regression/receipt.json";
                for (String gate : w1Gates()) {
                    String attachmentPath = "docs/v2/evidence/v2-m3/w1/m2-regression/attachments/" + gate + ".json";
                    byte[] bytes = ("trusted:" + gate).getBytes(StandardCharsets.UTF_8);
                    write(attachmentPath, bytes);
                    attachments.add(new AttachmentRef(
                            bytes.length, AttachmentKind.CURRENT_SOURCE_M2_GATE_RESULT, attachmentPath, sha(bytes)));
                }
                attachments.sort(Comparator.comparing(AttachmentRef::path));
                String sources = "{\"providerAdapter\":{\"nereusCommit\":\"" + COMMIT + "\"}}";
                tests = w1Gates().size();
                sourceId = "SOURCES";
                sourceSha = sha(sources.getBytes(StandardCharsets.UTF_8));
                childBytes = w1ChildBytes(attachments, sources);
            } else {
                childPath = String.format("docs/v2/evidence/v2-m3/children/%02d-%s.json", index, kind.name());
                List<AttachmentKind> kindsForChild = requiredAttachments(kind);
                for (int attachmentIndex = 0; attachmentIndex < kindsForChild.size(); attachmentIndex++) {
                    AttachmentKind attachmentKind = kindsForChild.get(attachmentIndex);
                    String attachmentPath = String.format(
                            "docs/v2/evidence/v2-m3/attachments/%02d-%02d-%s.txt",
                            index, attachmentIndex, attachmentKind.name());
                    byte[] bytes = ("attachment:" + kind.name() + ":" + attachmentKind.name())
                            .getBytes(StandardCharsets.UTF_8);
                    write(attachmentPath, bytes);
                    attachments.add(new AttachmentRef(bytes.length, attachmentKind, attachmentPath, sha(bytes)));
                }
                attachments.sort(Comparator.comparing(AttachmentRef::path));
                tests = index + 1L;
                sourceId = "SOURCE_TUPLE";
                String source = "{\"nereusCommit\":\"" + COMMIT + "\",\"sourceLocksSha256\":\"" + LOCKS_SHA + "\"}";
                sourceSha = sha(source.getBytes(StandardCharsets.UTF_8));
                childBytes = genericChildBytes(kind, attachments, exclusions(kind), tests);
            }
            write(childPath, childBytes);
            children.add(new ChildReceiptRef(
                    attachments,
                    childBytes.length,
                    0,
                    exclusions(kind),
                    0,
                    kind,
                    childPath,
                    false,
                    kind.requiredResult(),
                    sha(childBytes),
                    0,
                    new ChildSourceTuple(COMMIT, sourceId, sourceSha),
                    tests));
        }
        return new Receipt(
                new AllocatorSelection(true, AllocatorMode.STRICT, true, true, true),
                children,
                List.of(Exclusion.M6_PROCESS_ACTIVATION, Exclusion.M8_NATIVE_PARITY),
                ReceiptKind.V2_M3_FINAL,
                true,
                new ProviderEvidence(false, true, true),
                ReceiptResult.PASS_V2_M3_FINAL,
                M3FinalReceiptV1.requiredScenarios(),
                M3FinalReceiptV1.SCHEMA,
                new RootSourceTuple(COMMIT, LOCKS_SHA));
    }

    private static byte[] genericChildBytes(
            ChildKind kind, List<AttachmentRef> attachments, List<Exclusion> exclusions, long tests) {
        StringJoiner attachmentRows = new StringJoiner(",");
        for (AttachmentRef attachment : attachments) {
            attachmentRows.add("{\"bytes\":"
                    + attachment.bytes()
                    + ",\"kind\":\""
                    + attachment.kind().name()
                    + "\",\"path\":\""
                    + attachment.path()
                    + "\",\"sha256\":\""
                    + attachment.sha256()
                    + "\"}");
        }
        StringJoiner exclusionRows = new StringJoiner(",");
        for (Exclusion exclusion : exclusions) {
            exclusionRows.add("\"" + exclusion.name() + "\"");
        }
        String raw = "{\"attachments\":["
                + attachmentRows
                + "],\"exclusions\":["
                + exclusionRows
                + "],\"kind\":\""
                + kind.name()
                + "\",\"promotionEligible\":false,\"result\":\""
                + kind.requiredResult()
                + "\",\"schema\":\"NEREUS_V2_M3_CHILD_EVIDENCE_V1\","
                + "\"sourceTuple\":{\"nereusCommit\":\""
                + COMMIT
                + "\",\"sourceLocksSha256\":\""
                + LOCKS_SHA
                + "\"},\"testSummary\":{\"errors\":0,\"failures\":0,\"skipped\":0,"
                + "\"tests\":"
                + tests
                + "}}";
        return raw.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] w1ChildBytes(List<AttachmentRef> sortedAttachments, String sources) {
        java.util.Map<String, AttachmentRef> byPath = new java.util.HashMap<>();
        for (AttachmentRef attachment : sortedAttachments) {
            byPath.put(attachment.path(), attachment);
        }
        StringJoiner gates = new StringJoiner(",");
        for (String gate : w1Gates()) {
            AttachmentRef attachment =
                    byPath.get("docs/v2/evidence/v2-m3/w1/m2-regression/attachments/" + gate + ".json");
            gates.add("{\"attachment\":{\"bytes\":"
                    + attachment.bytes()
                    + ",\"path\":\""
                    + attachment.path()
                    + "\",\"sha256\":\""
                    + attachment.sha256()
                    + "\"},\"errors\":0,\"failures\":0,\"gateId\":\""
                    + gate
                    + "\",\"result\":\"PASS\",\"skipped\":0,\"tests\":1}");
        }
        String raw = "{\"childGates\":["
                + gates
                + "],\"evidenceClass\":\"TRUSTED_FULL_CURRENT_SOURCE_M2\",\"exclusions\":["
                + "\"M3_IMPLEMENTATION_AND_FINAL\",\"M6_PROCESS_ACTIVATION\",\"M8_NATIVE_PARITY\","
                + "\"SCENARIO_PROMOTION\"],\"historicalFinal\":{\"bytes\":1927,"
                + "\"path\":\"docs/v2/evidence/v2-m2/final/m2-final.json\","
                + "\"publishedNereusCommit\":\"0349fd68e04d94085d9c722c7ebc448cbb810d72\","
                + "\"sha256\":\"2ba2d1cab0547c456ec7e492edaf9b953e9e0d71707770d3c4b4fe8a4d6217dd\","
                + "\"testedNereusCommit\":\"4af3278234d84df7a2fdce4fc6b3e4e227916d56\"},"
                + "\"kind\":\"CURRENT_SOURCE_M2_REGRESSION\",\"m2AmendmentLineage\":[],"
                + "\"promotionEligible\":false,\"result\":\"PASS_CURRENT_SOURCE_M2_REGRESSION_ONLY\","
                + "\"scenarioPromotion\":false,\"schema\":\"NEREUS_V2_M3_CURRENT_SOURCE_M2_REGRESSION_V1\","
                + "\"sources\":"
                + sources
                + ",\"testedNereusCommit\":\""
                + COMMIT
                + "\"}";
        return raw.getBytes(StandardCharsets.UTF_8);
    }

    private static List<String> w1Gates() {
        return List.of(
                "KAFKA_K0",
                "KAFKA_K1",
                "KAFKA_K2",
                "KAFKA_K3",
                "KAFKA_K4",
                "KAFKA_K5",
                "KAFKA_K6",
                "KAFKA_K7",
                "KAFKA_K8",
                "KAFKA_K9",
                "KAFKA_K10",
                "KAFKA_EXACT",
                "KAFKA_REAL_BOOKKEEPER",
                "KAFKA_SCALE_10000",
                "KAFKA_SCALE_100000",
                "PULSAR_P0",
                "PULSAR_P1",
                "PULSAR_P2",
                "PULSAR_P3",
                "PULSAR_P4",
                "PULSAR_P5",
                "PULSAR_P6",
                "PULSAR_NATIVE",
                "PULSAR_P6_PROVIDER",
                "PULSAR_FINAL_PARSER_POLICY");
    }

    private static List<AttachmentKind> requiredAttachments(ChildKind kind) {
        Set<AttachmentKind> kinds = EnumSet.of(AttachmentKind.JUNIT_SUMMARY);
        switch (kind) {
            case W1_CURRENT_SOURCE_M2_REGRESSION -> kinds.add(AttachmentKind.CURRENT_SOURCE_M2_GATE_RESULT);
            case AB_NWG1_WIRE ->
                kinds.addAll(EnumSet.of(
                        AttachmentKind.MUTATION_MANIFEST,
                        AttachmentKind.NWG1_VECTOR_MANIFEST,
                        AttachmentKind.WIRE_ARTIFACT,
                        AttachmentKind.ZSTD_INTEROPERABILITY_FIXTURE));
            case C_OBJECT_WAL_STATE_TRACE -> kinds.add(AttachmentKind.TRACE_MANIFEST);
            case D_LOCAL_CAP -> kinds.add(AttachmentKind.LOCAL_CAP_RESULT);
            case C1_REAL_PROVIDER_KMS ->
                kinds.addAll(EnumSet.of(AttachmentKind.KMS_REAL_RECEIPT, AttachmentKind.PROVIDER_REAL_RECEIPT));
            case R_CONTROL_RECOVERY -> kinds.add(AttachmentKind.RECOVERY_MANIFEST);
            case K_NWKCP1 -> kinds.add(AttachmentKind.PROTOCOL_FIXTURE);
            case U_KAFKA_OBJECT_WAL, P_PULSAR_OBJECT_WAL -> kinds.add(AttachmentKind.NATIVE_RESULT);
            case ALLOCATOR_SELECTION ->
                kinds.addAll(EnumSet.of(
                        AttachmentKind.ALLOCATOR_FAULT_SUMMARY,
                        AttachmentKind.ALLOCATOR_NATIVE_RELATIVE_SUMMARY,
                        AttachmentKind.ALLOCATOR_RAW_VERIFICATION,
                        AttachmentKind.ALLOCATOR_SCALE_10000_SUMMARY,
                        AttachmentKind.ALLOCATOR_SCALE_100000_SUMMARY));
            default -> {
                // The common non-empty JUnit summary is sufficient for this focused child kind.
            }
        }
        return kinds.stream().sorted().toList();
    }

    private static List<Exclusion> exclusions(ChildKind kind) {
        Set<Exclusion> exclusions = EnumSet.of(Exclusion.M3_FINAL_AGGREGATE, Exclusion.SCENARIO_PROMOTION);
        if (kind == ChildKind.D_LOCAL_CAP) {
            exclusions.add(Exclusion.REAL_KMS);
            exclusions.add(Exclusion.REAL_PROVIDER);
        }
        if (kind == ChildKind.C2_SEGMENTED_PREFIX) {
            exclusions.add(Exclusion.C1_EVIDENCE_SUBSTITUTE);
            exclusions.add(Exclusion.PRODUCTION_ALLOWLIST);
        }
        return exclusions.stream().sorted().toList();
    }

    private void write(String relative, byte[] bytes) throws Exception {
        Path path = temporaryDirectory.resolve(relative);
        Files.createDirectories(path.getParent());
        Files.write(path, bytes);
    }

    private static String sha(byte[] bytes) throws Exception {
        return java.util.HexFormat.of()
                .formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static Receipt copy(Receipt receipt, List<ChildReceiptRef> children) {
        return new Receipt(
                receipt.allocatorSelection(),
                children,
                receipt.exclusions(),
                receipt.kind(),
                receipt.promotionEligible(),
                receipt.providerEvidence(),
                receipt.result(),
                receipt.scenarios(),
                receipt.schema(),
                receipt.sourceTuple());
    }

    private static Receipt copyScenarios(Receipt receipt, List<String> scenarios) {
        return new Receipt(
                receipt.allocatorSelection(),
                receipt.childReceipts(),
                receipt.exclusions(),
                receipt.kind(),
                receipt.promotionEligible(),
                receipt.providerEvidence(),
                receipt.result(),
                scenarios,
                receipt.schema(),
                receipt.sourceTuple());
    }

    private static Receipt replaceChild(Receipt receipt, int index, ChildReceiptRef child) {
        List<ChildReceiptRef> children = new ArrayList<>(receipt.childReceipts());
        children.set(index, child);
        return copy(receipt, children);
    }

    private static void assertRejected(ThrowingAction action, RejectionCode code) {
        assertThatThrownBy(action::run)
                .isInstanceOfSatisfying(ReceiptRejectedException.class, rejection -> assertThat(rejection.code())
                        .isEqualTo(code));
    }

    @FunctionalInterface
    private interface ThrowingAction {
        void run() throws Exception;
    }
}
