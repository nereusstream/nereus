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

package com.nereusstream.storage.object.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.control.ObjectWalControlTestFixtures;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.kms.KmsCellSession;
import com.nereusstream.storage.object.kms.KmsTransport;
import com.nereusstream.storage.object.kms.WrappedRunKeyEnvelope;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class C1ObjectProviderSessionTest {
    @Test
    void conditionalCreateAndExistingExactConvergeWithoutOverwrite() throws Exception {
        FakeTransport transport = new FakeTransport();
        C1ObjectProviderSession session = session(transport, "cell-a");
        TestBody body = body("cell-a/run/0/object", 1, 2, 3, 4);

        ProviderObjectResult created = session.conditionalCreate(body);
        ProviderObjectResult existing = session.conditionalCreate(body);

        assertThat(created.outcome()).isEqualTo(ProviderObjectOutcome.APPLIED_EXACT);
        assertThat(existing.outcome()).isEqualTo(ProviderObjectOutcome.EXISTING_EXACT);
        assertThat(existing.versionToken()).isPresent();
        assertThat(transport.putCalls).isEqualTo(2);
        assertThat(transport.fullGetCalls).isEqualTo(1);
        assertThat(session.acceptedOperations()).isZero();
    }

    @Test
    void createdRequiresExactLengthAndShaProofFromTheUploadedStream() throws Exception {
        FakeTransport transport = new FakeTransport();
        C1ObjectProviderSession session = session(transport, "cell-a");

        assertRejectedBody(
                session,
                new TestBody(identity("cell-a/run/0/short", 1, 2, 3), new byte[] {1, 2}),
                "exact identity length");
        assertRejectedBody(
                session,
                new TestBody(identity("cell-a/run/0/long", 1, 2), new byte[] {1, 2, 3}),
                "exact identity length");
        assertRejectedBody(session, new TestBody(identity("cell-a/run/0/sha", 1, 2), new byte[] {1, 3}), "SHA-256");

        transport.returnCreatedWithoutReading = true;
        TestBody unconsumed = body("cell-a/run/0/unconsumed", 1, 2);
        assertThatThrownBy(() -> session.conditionalCreate(unconsumed))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("before consuming");
        assertThat(session.acceptedOperations()).isEqualTo(1);
        assertThat(session.unknownObjectCount()).isEqualTo(1);
        assertThat(session.reconcileUnknown(unconsumed.identity(), "cell-a/run/0/", 10, 100, 102_400, 1024)
                        .objectResult()
                        .outcome())
                .isEqualTo(ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED);
        assertThat(session.acceptedOperations()).isZero();
        assertThat(session.unknownObjectCount()).isZero();
        assertThat(transport.putCalls).isEqualTo(1);
    }

    @Test
    void responseUnknownRequiresExactFullReadOrStrongCompleteAbsence() throws Exception {
        FakeTransport transport = new FakeTransport();
        C1ObjectProviderSession session = session(transport, "cell-a");
        TestBody body = body("cell-a/run/0/object", 7, 8, 9);
        transport.nextCreate = ObjectProviderTransport.ConditionalCreateResult.RESPONSE_UNKNOWN;
        transport.storeOnUnknown = true;

        assertThat(session.conditionalCreate(body).outcome()).isEqualTo(ProviderObjectOutcome.OUTCOME_UNKNOWN);
        assertThat(session.unknownObjectCount()).isEqualTo(1);
        assertThat(session.acceptedOperations()).isEqualTo(1);
        assertThat(session.reconcileUnknown(body.identity(), "cell-a/run/0/", 10, 100, 102_400, 1024)
                        .objectResult()
                        .outcome())
                .isEqualTo(ProviderObjectOutcome.EXISTING_EXACT);
        assertThat(session.unknownObjectCount()).isZero();
        assertThat(session.acceptedOperations()).isZero();

        ObjectIdentity absent = body("cell-a/run/0/missing", 5).identity();
        transport.objects.put("cell-a/run/0/sibling-a", new byte[] {1});
        transport.objects.put("cell-a/run/0/sibling-b", new byte[] {2});
        assertThat(session.reconcileUnknown(absent, "cell-a/run/0/", 10, 100, 102_400, 1024)
                        .objectResult()
                        .outcome())
                .isEqualTo(ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED);
        assertThat(transport.listCalls).isGreaterThanOrEqualTo(2);

        StrongListResult inventory = session.strongList("cell-a/run/0/", 10, 100, 102_400, 1024);
        assertThat(inventory.objects())
                .extracting(ObjectProviderTransport.ListedObject::key)
                .containsExactly("cell-a/run/0/object", "cell-a/run/0/sibling-a", "cell-a/run/0/sibling-b");
        assertThat(inventory.pageCount()).isEqualTo(3);
        assertThatThrownBy(() -> session.strongList("cell-a/run/0/", 1, 100, 102_400, 1024))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("page bound");
    }

    @Test
    void sameIdentityRetriesReuseOneUnknownOperationUntilExactOrAbsentTerminal() throws Exception {
        FakeTransport transport = new FakeTransport();
        C1ObjectProviderSession session = session(transport, "cell-a");
        TestBody exact = body("cell-a/run/0/exact", 7, 8, 9);
        transport.nextCreate = ObjectProviderTransport.ConditionalCreateResult.RESPONSE_UNKNOWN;
        transport.storeOnUnknown = true;

        assertThat(session.conditionalCreate(exact).outcome()).isEqualTo(ProviderObjectOutcome.OUTCOME_UNKNOWN);
        transport.nextCreate = ObjectProviderTransport.ConditionalCreateResult.RESPONSE_UNKNOWN;
        assertThat(session.conditionalCreate(exact).outcome()).isEqualTo(ProviderObjectOutcome.OUTCOME_UNKNOWN);

        assertThat(transport.putCalls).isEqualTo(2);
        assertThatThrownBy(() -> session.conditionalCreate(exact))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("PUT2");
        assertThat(transport.putCalls).isEqualTo(2);
        assertThat(session.acceptedOperations()).isEqualTo(1);
        assertThat(session.unknownObjectCount()).isEqualTo(1);
        assertThat(session.reconcileUnknown(exact.identity(), "cell-a/run/0/", 10, 100, 102_400, 1024)
                        .objectResult()
                        .outcome())
                .isEqualTo(ProviderObjectOutcome.EXISTING_EXACT);
        assertThat(session.acceptedOperations()).isZero();
        assertThat(session.unknownObjectCount()).isZero();

        TestBody absent = body("cell-a/run/0/absent", 1);
        transport.storeOnUnknown = false;
        transport.nextCreate = ObjectProviderTransport.ConditionalCreateResult.RESPONSE_UNKNOWN;
        assertThat(session.conditionalCreate(absent).outcome()).isEqualTo(ProviderObjectOutcome.OUTCOME_UNKNOWN);
        assertThat(session.reconcileUnknown(absent.identity(), "cell-a/run/0/", 10, 100, 102_400, 1024)
                        .objectResult()
                        .outcome())
                .isEqualTo(ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED);
        assertThat(session.acceptedOperations()).isZero();
        assertThat(session.unknownObjectCount()).isZero();
    }

    @Test
    void sameKeyDifferentIdentityIsLocalConflictDuringUnknownAndDrain() throws Exception {
        FakeTransport transport = new FakeTransport();
        C1ObjectProviderSession session = session(transport, "cell-a");
        TestBody admitted = body("cell-a/run/0/immutable", 1, 2, 3);
        TestBody substitution = body("cell-a/run/0/immutable", 9, 8, 7);
        transport.nextCreate = ObjectProviderTransport.ConditionalCreateResult.RESPONSE_UNKNOWN;

        assertThat(session.conditionalCreate(admitted).outcome()).isEqualTo(ProviderObjectOutcome.OUTCOME_UNKNOWN);
        assertThat(session.conditionalCreate(substitution).outcome())
                .isEqualTo(ProviderObjectOutcome.DEFINITIVE_CONFLICT);
        assertThat(transport.putCalls).isEqualTo(1);
        assertThat(session.acceptedOperations()).isEqualTo(1);
        assertThat(session.unknownObjectCount()).isEqualTo(1);

        session.drain();
        assertThat(session.conditionalCreate(substitution).outcome())
                .isEqualTo(ProviderObjectOutcome.DEFINITIVE_CONFLICT);
        assertThat(session.reconcileUnknown(substitution.identity(), "cell-a/run/0/", 10, 100, 102_400, 1024)
                        .objectResult()
                        .outcome())
                .isEqualTo(ProviderObjectOutcome.DEFINITIVE_CONFLICT);
        assertThat(transport.putCalls).isEqualTo(1);
        assertThat(transport.listCalls).isZero();
        assertThat(transport.fullGetCalls).isZero();

        assertThat(session.reconcileUnknown(admitted.identity(), "cell-a/run/0/", 10, 100, 102_400, 1024)
                        .objectResult()
                        .outcome())
                .isEqualTo(ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED);
        assertThat(session.state()).isEqualTo(C1ObjectProviderSession.State.DRAINING);
    }

    @Test
    void drainRejectsSameCandidatePutRedispatchButKeepsBoundedReconciliation() throws Exception {
        FakeTransport transport = new FakeTransport();
        C1ObjectProviderSession session = session(transport, "cell-a");
        TestBody candidate = body("cell-a/run/0/draining", 1, 2, 3);
        transport.nextCreate = ObjectProviderTransport.ConditionalCreateResult.RESPONSE_UNKNOWN;

        assertThat(session.conditionalCreate(candidate).outcome()).isEqualTo(ProviderObjectOutcome.OUTCOME_UNKNOWN);
        session.drain();

        assertThatThrownBy(() -> session.conditionalCreate(candidate))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not dispatch");
        assertThat(transport.putCalls).isEqualTo(1);
        assertThat(session.reconcileUnknown(candidate.identity(), "cell-a/run/0/", 10, 100, 102_400, 1024)
                        .objectResult()
                        .outcome())
                .isEqualTo(ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED);
        assertThat(session.acceptedOperations()).isZero();
    }

    @Test
    void concurrentDifferentIdentityCannotDispatchASecondSameKeyPut() throws Exception {
        FakeTransport transport = new FakeTransport();
        C1ObjectProviderSession session = session(transport, "cell-a");
        TestBody admitted = body("cell-a/run/0/immutable", 1, 2, 3);
        TestBody substitution = body("cell-a/run/0/immutable", 9, 8, 7);
        transport.nextCreate = ObjectProviderTransport.ConditionalCreateResult.RESPONSE_UNKNOWN;
        transport.putEntered = new CountDownLatch(1);
        transport.allowPut = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<ProviderObjectResult> first = executor.submit(() -> session.conditionalCreate(admitted));
            assertThat(transport.putEntered.await(10, TimeUnit.SECONDS)).isTrue();

            assertThat(session.conditionalCreate(substitution).outcome())
                    .isEqualTo(ProviderObjectOutcome.DEFINITIVE_CONFLICT);
            assertThat(transport.putCalls).isEqualTo(1);

            transport.allowPut.countDown();
            assertThat(first.get(10, TimeUnit.SECONDS).outcome()).isEqualTo(ProviderObjectOutcome.OUTCOME_UNKNOWN);
            assertThat(session.reconcileUnknown(admitted.identity(), "cell-a/run/0/", 10, 100, 102_400, 1024)
                            .objectResult()
                            .outcome())
                    .isEqualTo(ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED);
            assertThat(session.acceptedOperations()).isZero();
        } finally {
            transport.allowPut.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void concurrentReconcileClaimsUnknownOperationExactlyOnce() throws Exception {
        FakeTransport transport = new FakeTransport();
        C1ObjectProviderSession session = session(transport, "cell-a");
        TestBody body = body("cell-a/run/0/object", 7, 8, 9);
        transport.nextCreate = ObjectProviderTransport.ConditionalCreateResult.RESPONSE_UNKNOWN;
        transport.storeOnUnknown = true;
        session.conditionalCreate(body);
        transport.fullGetEntered = new CountDownLatch(1);
        transport.allowFullGet = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<ProviderReconciliationResult> first = executor.submit(
                    () -> session.reconcileUnknown(body.identity(), "cell-a/run/0/", 10, 100, 102_400, 1024));
            assertThat(transport.fullGetEntered.await(10, TimeUnit.SECONDS)).isTrue();

            assertThatThrownBy(() -> session.reconcileUnknown(body.identity(), "cell-a/run/0/", 10, 100, 102_400, 1024))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("already claimed");
            assertThat(session.acceptedOperations()).isEqualTo(1);

            transport.allowFullGet.countDown();
            assertThat(first.get(10, TimeUnit.SECONDS).objectResult().outcome())
                    .isEqualTo(ProviderObjectOutcome.EXISTING_EXACT);
            assertThat(session.acceptedOperations()).isZero();
            assertThat(session.unknownObjectCount()).isZero();
        } finally {
            transport.allowFullGet.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void drainAndCloseWaitForUnknownOperationWhichCanStillReconcile() throws Exception {
        FakeTransport transport = new FakeTransport();
        C1ObjectProviderSession session = session(transport, "cell-a");
        TestBody body = body("cell-a/run/0/object", 7, 8, 9);
        transport.nextCreate = ObjectProviderTransport.ConditionalCreateResult.RESPONSE_UNKNOWN;

        session.conditionalCreate(body);
        session.drain();

        assertThat(session.state()).isEqualTo(C1ObjectProviderSession.State.DRAINING);
        assertThatThrownBy(session::close)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("accepted operations");
        assertThatThrownBy(() -> session.conditionalCreate(body("cell-a/run/0/new", 1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no longer accepts");

        assertThat(session.reconcileUnknown(body.identity(), "cell-a/run/0/", 10, 100, 102_400, 1024)
                        .objectResult()
                        .outcome())
                .isEqualTo(ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED);
        assertThat(session.state()).isEqualTo(C1ObjectProviderSession.State.DRAINING);
        assertThat(session.acceptedOperations()).isZero();
        session.close();
        assertThat(session.state()).isEqualTo(C1ObjectProviderSession.State.CLOSED);
    }

    @Test
    void drainingSessionAllowsRecoveryReadsAndRejectsExactKeyRebindingBeforeFinalClose() throws Exception {
        FakeTransport transport = new FakeTransport();
        C1ObjectProviderSession session = session(transport, "cell-a");
        TestBody body = body("cell-a/run/0/object", 7, 8, 9);
        transport.nextCreate = ObjectProviderTransport.ConditionalCreateResult.RESPONSE_UNKNOWN;
        transport.storeOnUnknown = true;
        session.conditionalCreate(body);
        session.drain();

        assertThat(session.reconcileUnknown(body.identity(), "cell-a/run/0/", 10, 100, 102_400, 1024)
                        .objectResult()
                        .outcome())
                .isEqualTo(ProviderObjectOutcome.EXISTING_EXACT);
        assertThat(session.readVerifiedObject(body.identity()).toByteArray()).containsExactly(7, 8, 9);
        TestBody discovered = body("cell-a/run/0/discovered", 1, 2, 3);
        transport.objects.put(discovered.identity().key(), discovered.bytes);
        assertThat(session.readVerifiedObject(discovered.identity()).toByteArray())
                .containsExactly(1, 2, 3);
        assertThat(session.strongList("cell-a/run/0/", 10, 100, 102_400, 1024).objects())
                .hasSize(2);
        assertThatThrownBy(() -> session.readVerifiedObject(
                        body("cell-a/run/0/discovered", 9).identity()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("rebind");
        assertThat(session.state()).isEqualTo(C1ObjectProviderSession.State.DRAINING);
        session.close();
        assertThat(session.state()).isEqualTo(C1ObjectProviderSession.State.CLOSED);
    }

    @Test
    void prefixReadIsOneExactBoundedRangeAndCellNamespacesCannotCross() throws Exception {
        FakeTransport transport = new FakeTransport();
        TestBody body = body("cell-a/run/0/object", 1, 2, 3, 4, 5, 6);
        transport.objects.put(body.identity().key(), body.bytes);
        C1ObjectProviderSession session = session(transport, "cell-a");

        CanonicalBytes prefix = session.readDirectoryPrefix(body.identity(), 4, Optional.empty());
        CanonicalBytes verified = session.readVerifiedObject(body.identity());

        assertThat(prefix.toByteArray()).containsExactly(1, 2, 3, 4);
        assertThat(verified.toByteArray()).containsExactly(1, 2, 3, 4, 5, 6);
        assertThat(transport.rangeGetCalls).isEqualTo(1);
        assertThat(transport.fullGetCalls).isEqualTo(1);
        assertThatThrownBy(() -> session.readDirectoryPrefix(
                        body("cell-b/run/0/object", 1).identity(), 1, Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outside this Cell");

        transport.objects.put(body.identity().key(), new byte[] {1, 2, 3, 4, 5, 7});
        assertThatThrownBy(() -> session.readVerifiedObject(body.identity()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("SHA-256");
    }

    @Test
    void verifiedObjectReadReturnsBodyAndBoundedVersionFromTheSameFullGet() throws Exception {
        FakeTransport transport = new FakeTransport();
        TestBody body = body("cell-a/run/0/versioned", 1, 2, 3);
        transport.objects.put(body.identity().key(), body.bytes);
        C1ObjectProviderSession session = session(transport, "cell-a");

        C1ObjectProviderSession.VerifiedObjectRead read = session.readVerifiedObjectWithVersion(body.identity());

        assertThat(read.canonicalBody().toByteArray()).containsExactly(1, 2, 3);
        assertThat(read.immutableVersionToken())
                .contains(FakeTransport.version(body.identity().key()));
        assertThat(transport.fullGetCalls).isEqualTo(1);
        assertThat(transport.fullGetBodyReachedEof).isTrue();
    }

    @Test
    void verifiedObjectReadPreservesAbsentVersionToken() throws Exception {
        FakeTransport transport = new FakeTransport();
        TestBody body = body("cell-a/run/0/unversioned", 4, 5, 6);
        transport.objects.put(body.identity().key(), body.bytes);
        transport.fullGetVersionTokenOverride = Optional.empty();
        C1ObjectProviderSession session = session(transport, "cell-a");

        C1ObjectProviderSession.VerifiedObjectRead read = session.readVerifiedObjectWithVersion(body.identity());

        assertThat(read.canonicalBody().toByteArray()).containsExactly(4, 5, 6);
        assertThat(read.immutableVersionToken()).isEmpty();
        assertThat(transport.fullGetCalls).isEqualTo(1);
    }

    @Test
    void verifiedObjectReadRejectsOversizedVersionOnlyAfterExactBodyIoIsComplete() {
        FakeTransport transport = new FakeTransport();
        TestBody body = body("cell-a/run/0/oversized-version", 7, 8, 9);
        transport.objects.put(body.identity().key(), body.bytes);
        transport.fullGetVersionTokenOverride = Optional.of(CanonicalBytes.copyOf(new byte[65_536]));
        C1ObjectProviderSession session = session(transport, "cell-a");

        assertThatThrownBy(() -> session.readVerifiedObjectWithVersion(body.identity()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version token");

        assertThat(transport.fullGetCalls).isEqualTo(1);
        assertThat(transport.fullGetBodyReachedEof).isTrue();
        assertThat(session.acceptedOperations()).isZero();
    }

    @Test
    void providerCapabilitiesAndLocalCapsAreValidatedBeforeAdmission() {
        FakeTransport transport = new FakeTransport();
        assertThatThrownBy(() -> new C1ObjectProviderSession(
                        transport, scope(1), "cell-a", transport.capabilities().maximumObjectBytes() + 1, 4096))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Object cap");
        transport.capabilities =
                new ObjectProviderCapabilities("fake", true, true, true, false, true, 1024 * 1024, 4096, 1);
        assertThatThrownBy(() -> session(transport, "cell-a"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("C1");
    }

    @Test
    void c1TransportSurfaceHasNoHeadOperation() {
        assertThat(ObjectProviderTransport.class.getMethods())
                .extracting(java.lang.reflect.Method::getName)
                .doesNotContain("head");
    }

    @Test
    void absenceRequiresCompleteStrongListAndTypedFullGetNotFoundWithZeroHead() throws Exception {
        FakeTransport transport = new FakeTransport();
        C1ObjectProviderSession session = session(transport, "cell-a");
        ObjectIdentity missing = body("cell-a/run/0/missing", 1).identity();

        ProviderAbsenceProof proof = session.proveAbsent(missing, "cell-a/run/0/", 10, 100, 102_400, 1024);

        assertThat(proof.exactFullGetNotFound()).isTrue();
        assertThat(proof.headRequests()).isZero();
        assertThat(proof.listPages()).isEqualTo(1);
        assertThat(transport.listCalls).isEqualTo(1);
        assertThat(transport.fullGetCalls).isEqualTo(1);
    }

    @Test
    void absenceFirstPermanentlyClaimsExactIdentityAndRejectsEverySameKeyRebindLocally() throws Exception {
        FakeTransport transport = new FakeTransport();
        C1ObjectProviderSession session = session(transport, "cell-a");
        TestBody exact = body("cell-a/run/0/absence-first", 1, 2, 3);
        TestBody rebound = body("cell-a/run/0/absence-first", 9, 8, 7);

        assertThat(session.proveAbsent(exact.identity(), "cell-a/run/0/", 10, 100, 102_400, 1024)
                        .exactFullGetNotFound())
                .isTrue();
        int puts = transport.putCalls;
        int lists = transport.listCalls;
        int fullGets = transport.fullGetCalls;
        int rangeGets = transport.rangeGetCalls;

        assertThatThrownBy(() -> session.readVerifiedObject(rebound.identity()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("rebind");
        assertThat(session.reconcileUnknown(rebound.identity(), "cell-a/run/0/", 10, 100, 102_400, 1024)
                        .objectResult()
                        .outcome())
                .isEqualTo(ProviderObjectOutcome.DEFINITIVE_CONFLICT);
        assertThat(session.conditionalCreate(rebound).outcome()).isEqualTo(ProviderObjectOutcome.DEFINITIVE_CONFLICT);

        assertThat(transport.putCalls).isEqualTo(puts);
        assertThat(transport.listCalls).isEqualTo(lists);
        assertThat(transport.fullGetCalls).isEqualTo(fullGets);
        assertThat(transport.rangeGetCalls).isEqualTo(rangeGets);
        assertThat(session.acceptedOperations()).isZero();
        assertThat(session.unknownObjectCount()).isZero();
    }

    @Test
    void drainingSessionCanRepeatAbsenceForTheSamePermanentlyClaimedIdentity() throws Exception {
        FakeTransport transport = new FakeTransport();
        C1ObjectProviderSession session = session(transport, "cell-a");
        ObjectIdentity exact = body("cell-a/run/0/draining-absence", 4, 5, 6).identity();
        session.proveAbsent(exact, "cell-a/run/0/", 10, 100, 102_400, 1024);
        session.drain();

        ProviderAbsenceProof proof = session.proveAbsent(exact, "cell-a/run/0/", 10, 100, 102_400, 1024);

        assertThat(proof.exactFullGetNotFound()).isTrue();
        assertThat(session.state()).isEqualTo(C1ObjectProviderSession.State.DRAINING);
        assertThat(transport.listCalls).isEqualTo(2);
        assertThat(transport.fullGetCalls).isEqualTo(2);
        assertThat(session.acceptedOperations()).isZero();
    }

    @Test
    void streamingStrongListFoldsValidatedObjectsWithoutMaterializingInventory() throws Exception {
        FakeTransport transport = new FakeTransport();
        transport.objects.put("cell-a/run/0/a", new byte[] {1});
        transport.objects.put("cell-a/run/0/b", new byte[] {2});
        transport.objects.put("cell-a/run/0/c", new byte[] {3});
        C1ObjectProviderSession session = session(transport, "cell-a");
        ArrayList<String> stagedKeys = new ArrayList<>();

        C1ObjectProviderSession.StreamingListResult result = session.strongListStreaming(
                "cell-a/run/0/",
                new C1ObjectProviderSession.StreamingListBounds(10, 100, 102_400, 1024),
                object -> stagedKeys.add(object.key()));

        assertThat(result.pageCount()).isEqualTo(3);
        assertThat(result.keyCount()).isEqualTo(3);
        assertThat(result.canonicalKeyBytes())
                .isEqualTo(stagedKeys.stream().mapToInt(String::length).sum());
        assertThat(stagedKeys).containsExactly("cell-a/run/0/a", "cell-a/run/0/b", "cell-a/run/0/c");
        assertThat(session.acceptedOperations()).isZero();
    }

    @Test
    void streamingStrongListRejectsCrossPageReplayByStrictGlobalOrder() {
        FakeTransport transport = new FakeTransport();
        transport.objects.put("cell-a/run/0/a", new byte[] {1});
        transport.objects.put("cell-a/run/0/b", new byte[] {2});
        transport.replayFirstKeyOnSecondListPage = true;
        C1ObjectProviderSession session = session(transport, "cell-a");
        ArrayList<String> staging = new ArrayList<>();

        assertThatThrownBy(() -> session.strongListStreaming(
                        "cell-a/run/0/",
                        new C1ObjectProviderSession.StreamingListBounds(10, 100, 102_400, 1024),
                        object -> staging.add(object.key())))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("strict global key order");

        // The documented fold contract makes this unpublished staging value disposable as a whole.
        staging.clear();
        assertThat(staging).isEmpty();
        assertThat(session.acceptedOperations()).isZero();
    }

    @Test
    void materializingStrongListAlsoRejectsCrossPageReplayByStrictGlobalOrder() {
        FakeTransport transport = new FakeTransport();
        transport.objects.put("cell-a/run/0/a", new byte[] {1});
        transport.objects.put("cell-a/run/0/b", new byte[] {2});
        transport.replayFirstKeyOnSecondListPage = true;
        C1ObjectProviderSession session = session(transport, "cell-a");

        assertThatThrownBy(() -> session.strongList("cell-a/run/0/", 10, 100, 102_400, 1024))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("strict global key order");

        assertThat(session.acceptedOperations()).isZero();
    }

    @Test
    void streamingStrongListRejectsOversizedListedBodyBeforeConsumerStaging() {
        FakeTransport transport = new FakeTransport();
        transport.objects.put("cell-a/run/0/oversized-body", new byte[] {1});
        transport.listedBodyLengthOverride = 1024L * 1024 + 1;
        C1ObjectProviderSession session = session(transport, "cell-a");
        int[] staged = {0};

        assertThatThrownBy(() -> session.strongListStreaming(
                        "cell-a/run/0/",
                        new C1ObjectProviderSession.StreamingListBounds(10, 100, 102_400, 1024),
                        ignored -> staged[0]++))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("body cap");

        assertThat(staged[0]).isZero();
        assertThat(session.acceptedOperations()).isZero();
    }

    @Test
    void streamingStrongListRejectsOversizedListedVersionBeforeConsumerStaging() {
        FakeTransport transport = new FakeTransport();
        transport.objects.put("cell-a/run/0/oversized-version", new byte[] {1});
        transport.listedVersionTokenOverride = Optional.of(CanonicalBytes.copyOf(new byte[65_536]));
        C1ObjectProviderSession session = session(transport, "cell-a");
        int[] staged = {0};

        assertThatThrownBy(() -> session.strongListStreaming(
                        "cell-a/run/0/",
                        new C1ObjectProviderSession.StreamingListBounds(10, 100, 102_400, 1024),
                        ignored -> staged[0]++))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version token");

        assertThat(staged[0]).isZero();
        assertThat(session.acceptedOperations()).isZero();
    }

    @Test
    void streamingStrongListRejectsOversizedContinuationBeforeConsumerStaging() {
        FakeTransport transport = new FakeTransport();
        transport.listContinuationOverride = Optional.of(CanonicalBytes.copyOf(new byte[1025]));
        C1ObjectProviderSession session = session(transport, "cell-a");
        int[] staged = {0};

        assertThatThrownBy(() -> session.strongListStreaming(
                        "cell-a/run/0/",
                        new C1ObjectProviderSession.StreamingListBounds(10, 100, 102_400, 1024),
                        ignored -> staged[0]++))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("continuation");

        assertThat(staged[0]).isZero();
        assertThat(session.acceptedOperations()).isZero();
    }

    @Test
    void materializingStrongListUsesTheSameCallerBoundedContinuationCap() {
        FakeTransport transport = new FakeTransport();
        transport.listContinuationOverride = Optional.of(CanonicalBytes.copyOf(new byte[1025]));
        C1ObjectProviderSession session = session(transport, "cell-a");

        assertThatThrownBy(() -> session.strongList("cell-a/run/0/", 10, 100, 102_400, 1024))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("continuation");

        assertThat(session.acceptedOperations()).isZero();
    }

    @Test
    void walRunTransferRejectsGenericDirtyProviderWithoutAdditionalIoOrLifecycleCommit() throws Exception {
        WalRunRootRecord root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        FakeTransport transport = new FakeTransport();
        C1ObjectProviderSession raw = new C1ObjectProviderSession(
                transport,
                root.providerScopeId(),
                root.providerConfiguration().exclusiveNamespacePrefix(),
                root.providerConfiguration().maxObjectBodyBytes(),
                root.nwg1AdmissionCaps().maxDirectoryPrefixBytes());
        CanonicalBytes canonical = CanonicalBytes.copyOf(new byte[] {1, 2, 3});
        String key = root.providerConfiguration().exclusiveNamespacePrefix()
                + "/protocol/kafka/nwkcp1-v1/objects/sha256-v1-"
                + Sha256Digest.hash(canonical).toHex()
                + ".nwkcp1";
        TestBody admitted = body(key, 1, 2, 3);
        transport.nextCreate = ObjectProviderTransport.ConditionalCreateResult.RESPONSE_UNKNOWN;
        assertThat(raw.conditionalCreate(admitted).outcome()).isEqualTo(ProviderObjectOutcome.OUTCOME_UNKNOWN);
        assertThat(raw.acceptedOperations()).isEqualTo(1);
        assertThat(raw.unknownObjectCount()).isEqualTo(1);
        int puts = transport.putCalls;
        int lists = transport.listCalls;
        int fullGets = transport.fullGetCalls;
        int rangeGets = transport.rangeGetCalls;
        KmsCellSession kms = kms(root);

        assertThatThrownBy(() -> ObjectWalControlTestFixtures.openIsolatedSession(root, raw, kms, () -> 0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("pristine");

        assertThat(transport.putCalls).isEqualTo(puts);
        assertThat(transport.listCalls).isEqualTo(lists);
        assertThat(transport.fullGetCalls).isEqualTo(fullGets);
        assertThat(transport.rangeGetCalls).isEqualTo(rangeGets);
        assertThat(raw.state()).isEqualTo(C1ObjectProviderSession.State.OPEN);
        assertThat(raw.acceptedOperations()).isEqualTo(1);
        assertThat(raw.unknownObjectCount()).isEqualTo(1);
        assertThat(raw.reconcileUnknown(admitted.identity(), key, 1, 1, 1024, 1024)
                        .objectResult()
                        .outcome())
                .isEqualTo(ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED);
        raw.close();
        kms.close();
    }

    @Test
    void pristineWalRunTransferFencesRawAliasesWhileTypedLeaseUnknownReconcilesAndCloses() throws Exception {
        WalRunRootRecord root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        FakeTransport transport = new FakeTransport();
        C1ObjectProviderSession raw = new C1ObjectProviderSession(
                transport,
                root.providerScopeId(),
                root.providerConfiguration().exclusiveNamespacePrefix(),
                root.providerConfiguration().maxObjectBodyBytes(),
                root.nwg1AdmissionCaps().maxDirectoryPrefixBytes());
        WalRunObjectSession owner = ObjectWalControlTestFixtures.openIsolatedSession(root, raw, kms(root), () -> 0);
        CanonicalBytes canonical = CanonicalBytes.copyOf(new byte[] {1, 2, 3});
        String key = root.providerConfiguration().exclusiveNamespacePrefix()
                + "/protocol/kafka/nwkcp1-v1/objects/sha256-v1-"
                + Sha256Digest.hash(canonical).toHex()
                + ".nwkcp1";
        TestBody admitted = body(key, 1, 2, 3);

        int puts = transport.putCalls;
        int lists = transport.listCalls;
        int fullGets = transport.fullGetCalls;
        int rangeGets = transport.rangeGetCalls;

        assertThatThrownBy(() -> raw.conditionalCreate(body(key + "-alias", 9)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("transferred");
        assertThatThrownBy(() -> raw.readVerifiedObject(body(key, 9, 8, 7).identity()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("transferred");
        assertThatThrownBy(() -> raw.readVerifiedObject(admitted.identity()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("transferred");
        assertThatThrownBy(() -> raw.readDirectoryPrefix(admitted.identity(), 1, Optional.empty()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("transferred");
        assertThatThrownBy(() -> raw.readExactRange(admitted.identity(), 0, 1, Optional.empty()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("transferred");
        assertThatThrownBy(() -> raw.reconcileUnknown(admitted.identity(), key, 1, 1, 1024, 1024))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("transferred");
        assertThatThrownBy(() -> raw.proveAbsent(body(key + "-missing", 1).identity(), key, 1, 1, 1024, 1024))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("transferred");
        assertThatThrownBy(() -> raw.strongList(key, 1, 1, 1024, 1024))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("transferred");
        assertThatThrownBy(() -> raw.strongListStreaming(
                        key, new C1ObjectProviderSession.StreamingListBounds(1, 1, 1024, 1024), ignored -> {}))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("transferred");
        assertThatThrownBy(raw::drain).isInstanceOf(IllegalStateException.class).hasMessageContaining("transferred");
        assertThatThrownBy(raw::close).isInstanceOf(IllegalStateException.class).hasMessageContaining("transferred");

        assertThat(transport.putCalls).isEqualTo(puts);
        assertThat(transport.listCalls).isEqualTo(lists);
        assertThat(transport.fullGetCalls).isEqualTo(fullGets);
        assertThat(transport.rangeGetCalls).isEqualTo(rangeGets);
        assertThat(raw.state()).isEqualTo(C1ObjectProviderSession.State.OPEN);
        assertThat(raw.acceptedOperations()).isZero();
        assertThat(raw.unknownObjectCount()).isZero();

        WalRunObjectSession.ValidatedKafkaProtocolObject typed =
                owner.validateKafkaProtocolObject(admitted.identity(), canonical);
        transport.nextCreate = ObjectProviderTransport.ConditionalCreateResult.RESPONSE_UNKNOWN;
        assertThat(owner.conditionalCreateKafkaProtocolObject(typed).outcome())
                .isEqualTo(ProviderObjectOutcome.OUTCOME_UNKNOWN);
        assertThat(raw.acceptedOperations()).isEqualTo(1);
        assertThat(raw.unknownObjectCount()).isEqualTo(1);

        assertThat(owner.reconcileUnknownProtocolObject(admitted.identity()).outcome())
                .isEqualTo(ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED);
        owner.close();
        assertThat(raw.state()).isEqualTo(C1ObjectProviderSession.State.CLOSED);
        assertThat(raw.acceptedOperations()).isZero();
        assertThat(raw.unknownObjectCount()).isZero();
    }

    private static KmsCellSession kms(WalRunRootRecord root) {
        return new KmsCellSession(
                new NoIoKmsTransport(),
                root.providerScopeId(),
                root.wrappedRunKey().wrappingKeyId(),
                2,
                new SecureRandom(new byte[] {1, 2, 3}));
    }

    private static C1ObjectProviderSession session(FakeTransport transport, String prefix) {
        return new C1ObjectProviderSession(transport, scope(1), prefix, 1024 * 1024, 4096);
    }

    private static CellProviderScopeId scope(int seed) {
        byte[] value = new byte[Sha256Digest.LENGTH];
        Arrays.fill(value, (byte) seed);
        return new CellProviderScopeId(Sha256Digest.copyOf(value));
    }

    private static TestBody body(String key, int... values) {
        byte[] bytes = new byte[values.length];
        for (int index = 0; index < values.length; index++) {
            bytes[index] = (byte) values[index];
        }
        CanonicalBytes canonical = CanonicalBytes.copyOf(bytes);
        return new TestBody(new ObjectIdentity(key, bytes.length, Sha256Digest.hash(canonical)), bytes);
    }

    private static ObjectIdentity identity(String key, int... values) {
        return body(key, values).identity();
    }

    private static void assertRejectedBody(C1ObjectProviderSession session, TestBody body, String message)
            throws Exception {
        assertThatThrownBy(() -> session.conditionalCreate(body))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(message);
        assertThat(session.acceptedOperations()).isZero();
        assertThat(session.unknownObjectCount()).isZero();
    }

    private record TestBody(ObjectIdentity identity, byte[] bytes) implements RepeatableObjectBody {
        private TestBody {
            bytes = bytes.clone();
        }

        @Override
        public InputStream openStream() {
            return new ByteArrayInputStream(bytes);
        }
    }

    private static final class FakeTransport implements ObjectProviderTransport {
        private ObjectProviderCapabilities capabilities =
                new ObjectProviderCapabilities("fake", true, true, true, true, true, 1024 * 1024, 4096, 1);
        private final Map<String, byte[]> objects = new LinkedHashMap<>();
        private ConditionalCreateResult nextCreate;
        private boolean storeOnUnknown;
        private boolean returnCreatedWithoutReading;
        private boolean replayFirstKeyOnSecondListPage;
        private Long listedBodyLengthOverride;
        private Optional<CanonicalBytes> listedVersionTokenOverride;
        private Optional<CanonicalBytes> listContinuationOverride;
        private Optional<CanonicalBytes> fullGetVersionTokenOverride;
        private boolean fullGetBodyReachedEof;
        private int putCalls;
        private int fullGetCalls;
        private int rangeGetCalls;
        private int listCalls;
        private CountDownLatch fullGetEntered;
        private CountDownLatch allowFullGet;
        private CountDownLatch putEntered;
        private CountDownLatch allowPut;

        @Override
        public ObjectProviderCapabilities capabilities() {
            return capabilities;
        }

        @Override
        public ConditionalCreateResult putIfAbsent(ObjectIdentity identity, InputStream body) throws IOException {
            putCalls++;
            awaitPutPermit();
            if (returnCreatedWithoutReading) {
                returnCreatedWithoutReading = false;
                return ConditionalCreateResult.CREATED;
            }
            byte[] value = readAll(body);
            ConditionalCreateResult configured = nextCreate;
            nextCreate = null;
            if (configured != null) {
                if (configured == ConditionalCreateResult.RESPONSE_UNKNOWN && storeOnUnknown) {
                    objects.putIfAbsent(identity.key(), value);
                }
                return configured;
            }
            return objects.putIfAbsent(identity.key(), value) == null
                    ? ConditionalCreateResult.CREATED
                    : ConditionalCreateResult.ALREADY_EXISTS;
        }

        @Override
        public StreamingObject get(String key, Optional<CanonicalBytes> exactVersionToken) throws IOException {
            fullGetCalls++;
            awaitFullGetPermit();
            byte[] value = required(key);
            return new StreamingObject(
                    value.length,
                    0,
                    value.length,
                    fullGetVersionTokenOverride == null ? Optional.of(version(key)) : fullGetVersionTokenOverride,
                    trackedFullGetBody(value));
        }

        @Override
        public StreamingObject getRange(
                String key, long inclusiveStart, long exclusiveEnd, Optional<CanonicalBytes> versionToken)
                throws IOException {
            rangeGetCalls++;
            byte[] value = required(key);
            byte[] range = Arrays.copyOfRange(value, Math.toIntExact(inclusiveStart), Math.toIntExact(exclusiveEnd));
            return new StreamingObject(
                    value.length,
                    inclusiveStart,
                    exclusiveEnd,
                    Optional.of(version(key)),
                    new ByteArrayInputStream(range));
        }

        @Override
        public ListPage list(String prefix, Optional<CanonicalBytes> continuationToken, int maximumKeys) {
            listCalls++;
            List<String> keys = objects.keySet().stream()
                    .filter(key -> key.startsWith(prefix))
                    .sorted()
                    .toList();
            int start = continuationToken
                    .map(value -> ByteBuffer.wrap(value.toByteArray()).getInt())
                    .orElse(0);
            if (replayFirstKeyOnSecondListPage && listCalls == 2) {
                start = 0;
            }
            int end = Math.min(keys.size(), Math.addExact(start, Math.min(maximumKeys, 1)));
            ArrayList<ListedObject> page = new ArrayList<>();
            for (int index = start; index < end; index++) {
                String key = keys.get(index);
                page.add(new ListedObject(
                        key,
                        listedBodyLengthOverride == null ? objects.get(key).length : listedBodyLengthOverride,
                        listedVersionTokenOverride == null ? Optional.empty() : listedVersionTokenOverride));
            }
            Optional<CanonicalBytes> next = end < keys.size()
                    ? Optional.of(CanonicalBytes.copyOf(
                            ByteBuffer.allocate(4).putInt(end).array()))
                    : Optional.empty();
            if (listContinuationOverride != null) {
                next = listContinuationOverride;
            }
            return new ListPage(page, next);
        }

        @Override
        public FailureKind classifyFailure(IOException failure) {
            return failure instanceof FakeNotFoundException ? FailureKind.NOT_FOUND : FailureKind.FATAL;
        }

        private byte[] required(String key) throws FakeNotFoundException {
            byte[] value = objects.get(key);
            if (value == null) {
                throw new FakeNotFoundException(key);
            }
            return value;
        }

        private InputStream trackedFullGetBody(byte[] value) {
            return new ByteArrayInputStream(value) {
                @Override
                public synchronized int read() {
                    int read = super.read();
                    if (read < 0) {
                        fullGetBodyReachedEof = true;
                    }
                    return read;
                }

                @Override
                public synchronized int read(byte[] target, int offset, int length) {
                    int read = super.read(target, offset, length);
                    if (read < 0) {
                        fullGetBodyReachedEof = true;
                    }
                    return read;
                }
            };
        }

        private void awaitFullGetPermit() throws IOException {
            CountDownLatch entered = fullGetEntered;
            CountDownLatch allowed = allowFullGet;
            if (entered == null || allowed == null) {
                return;
            }
            entered.countDown();
            try {
                if (!allowed.await(10, TimeUnit.SECONDS)) {
                    throw new IOException("timed out waiting for fake full GET permit");
                }
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted waiting for fake full GET permit", failure);
            }
        }

        private void awaitPutPermit() throws IOException {
            CountDownLatch entered = putEntered;
            CountDownLatch allowed = allowPut;
            if (entered == null || allowed == null) {
                return;
            }
            entered.countDown();
            try {
                if (!allowed.await(10, TimeUnit.SECONDS)) {
                    throw new IOException("timed out waiting for fake PUT permit");
                }
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted waiting for fake PUT permit", failure);
            }
        }

        private static CanonicalBytes version(String key) {
            return CanonicalBytes.copyOf(
                    Sha256Digest.hash(CanonicalBytes.copyOf(key.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
                            .bytes()
                            .toByteArray());
        }

        private static byte[] readAll(InputStream input) throws IOException {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            input.transferTo(output);
            return output.toByteArray();
        }

        private static final class FakeNotFoundException extends IOException {
            private FakeNotFoundException(String key) {
                super("missing fake Object: " + key);
            }
        }
    }

    private static final class NoIoKmsTransport implements KmsTransport {
        @Override
        public WrappedRunKeyEnvelope wrap(String keyIdentity, byte[] plaintextRunKey) {
            throw new AssertionError("unexpected KMS wrap");
        }

        @Override
        public byte[] unwrap(WrappedRunKeyEnvelope envelope) {
            throw new AssertionError("unexpected KMS unwrap");
        }
    }
}
