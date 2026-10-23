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

package com.nereusstream.metadata.oxia.v2.retention;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.oxia.v2.retention.M5GcQuotaOxiaIntegrationTest.QFixture;
import com.nereusstream.metadata.oxia.v2.retention.M5PermanentDoneOxiaIntegrationTest.Fixture;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2.BirthResult;
import com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2.Result;
import com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Same native container/data, separate JVMs; checkpoint carries identities/hashes, never reconstructed authority. */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class M5GcQuotaOxiaRestartTest {
    @Test
    void writeBeforeServerRestart() throws Exception {
        String root = Fixture.root();
        try (var first = new QFixture(root);
                var inspector = new QFixture(root)) {
            first.initialize(3);
            first.complete(1);
            assertThat(first.settle(1)).isEqualTo(Result.SETTLED);
            var intent = first.intent(2);
            first.faults.stopAfterWrites = 1;
            assertThatThrownBy(
                            () -> Fixture.await(first.route.quota().reserve(M5GcQuotaOxiaIntegrationTest.resource(3))))
                    .hasRootCauseMessage("injected quota client stopped");
            var head = Fixture.await(inspector.route.quota().snapshot()).head();
            assertThat(head.pending()).isPresent();
            var fields = new java.util.ArrayList<String>(List.of(
                    root,
                    Sha256Digest.hash(head.encode()).toHex(),
                    intent.canonicalStoredSha256().toHex(),
                    intent.metadataVersion().value().toHex(),
                    Fixture.await(inspector.route.read(
                                    M5GcQuotaOxiaIntegrationTest.resource(1).authorityKey()))
                            .orElseThrow()
                            .canonicalStoredSha256()
                            .toHex(),
                    Sha256Digest.hash(Fixture.await(inspector.faults.client.read(inspector.layout.nativeKey(
                                            M5GcQuotaRecordsV2.entryKey(M5GcQuotaOxiaIntegrationTest.resource(2)
                                                    .sha256()))))
                                    .orElseThrow()
                                    .storedBytes())
                            .toHex()));
            fields.addAll(writeMemberCheckpoint(root));
            fields.addAll(writeBirthCheckpoint(root));
            Files.write(checkpoint(), fields);
        }
    }

    @Test
    void readAfterServerRestart() throws Exception {
        var lines = Files.readAllLines(checkpoint());
        assertThat(lines).hasSize(24);
        try (var q = new QFixture(lines.get(0))) {
            var before = Fixture.await(q.route.quota().snapshot());
            assertThat(Sha256Digest.hash(before.head().encode()).toHex()).isEqualTo(lines.get(1));
            var intent = Fixture.await(q.route.read(
                            M5GcQuotaOxiaIntegrationTest.resource(2).authorityKey()))
                    .orElseThrow();
            assertThat(intent.canonicalStoredSha256().toHex()).isEqualTo(lines.get(2));
            assertThat(intent.metadataVersion().value().toHex()).isEqualTo(lines.get(3));
            assertThat(Fixture.await(q.route.read(
                                    M5GcQuotaOxiaIntegrationTest.resource(1).authorityKey()))
                            .orElseThrow()
                            .canonicalStoredSha256()
                            .toHex())
                    .isEqualTo(lines.get(4));
            assertThat(Sha256Digest.hash(Fixture.await(q.faults.client.read(q.layout.nativeKey(
                                            M5GcQuotaRecordsV2.entryKey(M5GcQuotaOxiaIntegrationTest.resource(2)
                                                    .sha256()))))
                                    .orElseThrow()
                                    .storedBytes())
                            .toHex())
                    .isEqualTo(lines.get(5));
            // This exact existing intent completes while the recovered head is still full with another pending grant.
            q.complete(2);
            assertThat(Fixture.await(q.route.quota().snapshot()).head().pending())
                    .isEqualTo(before.head().pending());
            assertThat(q.reserve(4)).isEqualTo(Result.EXHAUSTED);
            assertThat(q.settle(2)).isEqualTo(Result.SETTLED);
            assertThat(q.reserve(1)).isEqualTo(Result.SETTLED);
            assertThat(q.reserve(2)).isEqualTo(Result.SETTLED);
            assertThat(q.reserve(4)).isEqualTo(Result.GRANTED);
            assertThat(q.reserve(5)).isEqualTo(Result.EXHAUSTED);
            assertThat(Fixture.await(
                            q.route.quota().expand(before.head().capacityBytes() + q.layout.reservationCharge())))
                    .isTrue();
            assertThat(q.reserve(5)).isEqualTo(Result.GRANTED);
            assertThat(Fixture.await(q.route.quota().snapshot()).head().settledResources())
                    .isEqualTo(2);
            assertThat(Fixture.await(q.route.quota().snapshot()).head().reservedResources())
                    .isEqualTo(3);
            assertThat(Fixture.await(q.route.quota().snapshot()).head().pending())
                    .isEmpty();
        }
        readMemberCheckpoint(lines);
        readBirthCheckpoint(lines);
    }

    /** Native metadata persistence only. These declared workers/versions do not qualify Provider output. */
    private static List<String> writeBirthCheckpoint(String root) throws Exception {
        var fields = new java.util.ArrayList<String>();
        for (int phase = 0; phase < 3; phase++) {
            var layout = M5GcQuotaRecordsV2.Layout.withPhysicalMembers(
                    root + "/birth-quota-" + phase,
                    M5GcQuotaOxiaIntegrationTest.resource(0).namespace());
            try (var q = new QFixture(layout.nativeRoot(), layout);
                    var inspector = new QFixture(layout.nativeRoot(), layout)) {
                Fixture.await(q.route.initialize(M5GcQuotaOxiaIntegrationTest.birthCapacity(layout)));
                var claim = M5GcQuotaOxiaIntegrationTest.declaredClaim(layout);
                if (phase != 0) {
                    Fixture.await(q.route
                            .quota()
                            .reserveBirth(
                                    M5GcQuotaOxiaIntegrationTest.declaredWorker(),
                                    M5GcQuotaOxiaIntegrationTest.declaredSlot()));
                }
                if (phase == 2) {
                    var output = new com.nereusstream.storage.object.provider.ObjectIdentity(
                            M5GcQuotaOxiaIntegrationTest.declaredSlot().objectKey(),
                            8,
                            com.nereusstream.storage.object.gc.SyntheticDeleteAuthorityFixturesV2.digest(
                                    "declared-restart-output"));
                    assertThat(Fixture.await(q.route.quota().armBirth(claim.key(), output)))
                            .isEqualTo(BirthResult.WRITE_ARMED);
                }
                q.faults.stopAfterWrites = phase == 2 ? 1 : 2;
                final int originalPhase = phase;
                assertThatThrownBy(() -> {
                            if (originalPhase == 0) {
                                Fixture.await(q.route
                                        .quota()
                                        .reserveBirth(
                                                M5GcQuotaOxiaIntegrationTest.declaredWorker(),
                                                M5GcQuotaOxiaIntegrationTest.declaredSlot()));
                            } else if (originalPhase == 1) {
                                Fixture.await(q.route.quota().cancelNoDispatchBirth(claim.key()));
                            } else {
                                Fixture.await(
                                        q.route.quota().assignBirth(claim.key(), (PhysicalResourceIdV2.ObjectVersion)
                                                M5GcQuotaOxiaIntegrationTest.resource(1)));
                            }
                        })
                        .hasRootCauseMessage("injected quota client stopped");
                var head = Fixture.await(
                        inspector.nativeFixture.client.get(layout.nativeKey(M5GcQuotaRecordsV2.HEAD_KEY)));
                var credit = Fixture.await(inspector.nativeFixture.client.get(layout.nativeKey(claim.key())));
                var stored = Fixture.await(inspector.route.quota().readBirth(claim.key()))
                        .orElseThrow();
                fields.addAll(List.of(
                        Sha256Digest.hash(com.nereusstream.domain.bytes.CanonicalBytes.copyOf(head.value()))
                                .toHex(),
                        Long.toString(head.version().versionId()),
                        Sha256Digest.hash(com.nereusstream.domain.bytes.CanonicalBytes.copyOf(credit.value()))
                                .toHex(),
                        Long.toString(credit.version().versionId()),
                        Long.toString(stored.grantRevision())));
            }
        }
        return fields;
    }

    private static void readBirthCheckpoint(List<String> lines) throws Exception {
        for (int phase = 0; phase < 3; phase++) {
            var layout = M5GcQuotaRecordsV2.Layout.withPhysicalMembers(
                    lines.get(0) + "/birth-quota-" + phase,
                    M5GcQuotaOxiaIntegrationTest.resource(0).namespace());
            try (var q = new QFixture(layout.nativeRoot(), layout)) {
                var claim = M5GcQuotaOxiaIntegrationTest.declaredClaim(layout);
                var head = Fixture.await(q.nativeFixture.client.get(layout.nativeKey(M5GcQuotaRecordsV2.HEAD_KEY)));
                var credit = Fixture.await(q.nativeFixture.client.get(layout.nativeKey(claim.key())));
                int offset = 9 + 5 * phase;
                assertThat(Sha256Digest.hash(com.nereusstream.domain.bytes.CanonicalBytes.copyOf(head.value()))
                                .toHex())
                        .isEqualTo(lines.get(offset));
                assertThat(Long.toString(head.version().versionId())).isEqualTo(lines.get(offset + 1));
                assertThat(Sha256Digest.hash(com.nereusstream.domain.bytes.CanonicalBytes.copyOf(credit.value()))
                                .toHex())
                        .isEqualTo(lines.get(offset + 2));
                assertThat(Long.toString(credit.version().versionId())).isEqualTo(lines.get(offset + 3));
                assertThat(Fixture.await(q.route.quota().recover())).isTrue();
                var result = phase == 0
                        ? Fixture.await(q.route
                                .quota()
                                .reserveBirth(
                                        M5GcQuotaOxiaIntegrationTest.declaredWorker(),
                                        M5GcQuotaOxiaIntegrationTest.declaredSlot()))
                        : phase == 1
                                ? Fixture.await(q.route.quota().cancelNoDispatchBirth(claim.key()))
                                : Fixture.await(
                                        q.route.quota().assignBirth(claim.key(), (PhysicalResourceIdV2.ObjectVersion)
                                                M5GcQuotaOxiaIntegrationTest.resource(1)));
                assertThat(result)
                        .isEqualTo(
                                phase == 0
                                        ? BirthResult.RESERVED
                                        : phase == 1 ? BirthResult.CANCELLED_NO_DISPATCH : BirthResult.ASSIGNED);
                var snapshot = Fixture.await(q.route.quota().snapshot());
                assertThat(snapshot.head().births().orElseThrow().issued()).isEqualTo(1);
                assertThat(snapshot.head().births().orElseThrow().pending()).isEmpty();
                assertThat(snapshot.head().pending()).isEmpty();
                assertThat(snapshot.head().reservedResources()).isEqualTo(phase == 2 ? 1 : 0);
                assertThat(snapshot.availableCanonicalBytes()).isEqualTo(phase == 1 ? layout.reservationCharge() : 0);
                assertThat(Long.toString(Fixture.await(q.route.quota().readBirth(claim.key()))
                                .orElseThrow()
                                .grantRevision()))
                        .isEqualTo(lines.get(offset + 4));
            }
        }
    }

    private static List<String> writeMemberCheckpoint(String root) throws Exception {
        var layout = M5GcQuotaRecordsV2.Layout.withPhysicalMembers(
                root + "/member-profile",
                M5GcQuotaOxiaIntegrationTest.resource(0).namespace());
        try (var q = new QFixture(layout.nativeRoot(), layout)) {
            q.initialize(1);
            assertThat(q.reserve(1)).isEqualTo(Result.GRANTED);
            var bytes = M5GcQuotaOxiaIntegrationTest.memberValue(1);
            String key = layout.nativeKey(M5GcQuotaRecordsV2.physicalMemberKey(
                    M5GcQuotaOxiaIntegrationTest.resource(1).sha256()));
            Fixture.await(q.nativeFixture.client.put(key, bytes.toByteArray()));
            q.intent(1);
            q.complete(1);
            assertThat(q.settle(1)).isEqualTo(Result.SETTLED);
            var stored = Fixture.await(q.nativeFixture.client.get(key));
            return List.of(
                    Sha256Digest.hash(Fixture.await(q.route.quota().snapshot())
                                    .head()
                                    .encode())
                            .toHex(),
                    Sha256Digest.hash(bytes).toHex(),
                    Long.toString(stored.version().versionId()));
        }
    }

    private static void readMemberCheckpoint(List<String> lines) throws Exception {
        var layout = M5GcQuotaRecordsV2.Layout.withPhysicalMembers(
                lines.get(0) + "/member-profile",
                M5GcQuotaOxiaIntegrationTest.resource(0).namespace());
        try (var q = new QFixture(layout.nativeRoot(), layout)) {
            var snapshot = Fixture.await(q.route.quota().snapshot());
            assertThat(Sha256Digest.hash(snapshot.head().encode()).toHex()).isEqualTo(lines.get(6));
            String key = layout.nativeKey(M5GcQuotaRecordsV2.physicalMemberKey(
                    M5GcQuotaOxiaIntegrationTest.resource(1).sha256()));
            var stored = Fixture.await(q.nativeFixture.client.get(key));
            assertThat(Sha256Digest.hash(com.nereusstream.domain.bytes.CanonicalBytes.copyOf(stored.value()))
                            .toHex())
                    .isEqualTo(lines.get(7));
            assertThat(Long.toString(stored.version().versionId())).isEqualTo(lines.get(8));
            assertThat(snapshot.chargedCanonicalBytes()).isGreaterThanOrEqualTo(q.actualCanonicalBytes());
            assertThat(snapshot.head().settledResources()).isEqualTo(1);
            assertThat(q.reserve(1)).isEqualTo(Result.SETTLED);
            assertThat(q.reserve(2)).isEqualTo(Result.EXHAUSTED);
        }
    }

    private static Path checkpoint() {
        return Path.of(System.getProperty("nereus.m5.quota.oxia.restartCheckpoint"));
    }
}
