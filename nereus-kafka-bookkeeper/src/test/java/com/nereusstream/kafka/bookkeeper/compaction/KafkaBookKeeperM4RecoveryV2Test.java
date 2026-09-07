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

package com.nereusstream.kafka.bookkeeper.compaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.RunLedgerReadResultV1;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.CaptureOutcome;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.CaptureResult;
import com.nereusstream.storage.object.read.BindingReadAuthorityV1;
import com.nereusstream.storage.object.read.BindingReadHazardPoolV1;
import com.nereusstream.storage.object.read.BindingReadHazardPoolV1.ScanOutcome;
import com.nereusstream.storage.object.read.BindingReadPublicationCellV1;
import com.nereusstream.storage.object.read.BindingReadRouteTableV1;
import com.nereusstream.storage.object.read.control.BindingReadSelectorRuntimeV1;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1.Outcome;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/** Uses real M4 kernel/control code with explicitly synthetic metadata/source and native provider fixtures. */
class KafkaBookKeeperM4RecoveryV2Test {
    @Test
    void capturesCompleteDescriptorSourcePlanWithoutControlMetadataIoOrObsoleteFallback() {
        for (boolean empty : List.of(false, true)) {
            var fixture = new Fixture(empty);
            int operations = fixture.physical.store.operations.size();
            var view = fixture.recovery.recover(fixture.descriptor()).join().view();
            assertThat(view.descriptor()).isEqualTo(fixture.descriptor());
            assertThat(view.allowsPredecessorOffset(0)).isFalse();
            assertThat(fixture.physical.store.operations).hasSize(operations);
            var route = fixture.runtime
                    .currentAuthority()
                    .get()
                    .publicationCell()
                    .routes()
                    .route(0);
            assertThat(route.fallback()).isNull();
            assertThat(fixture.scan(fixture.selector.sourceGeneration())).isEqualTo(ScanOutcome.CLEAN);
        }
    }

    @Test
    void cancellationKeepsExactGenerationPinnedUntilActualNativeCompletion() {
        var fixture = new Fixture(false);
        fixture.delayNextRead = true;
        var result = fixture.recovery.recover(fixture.descriptor());
        assertThat(fixture.scan(fixture.selector.sourceGeneration())).isEqualTo(ScanOutcome.PINNED);
        assertThat(result.cancel(true)).isTrue();
        assertThat(fixture.pending.isCancelled()).isFalse();
        assertThat(fixture.scan(fixture.selector.sourceGeneration())).isEqualTo(ScanOutcome.PINNED);
        fixture.pending.complete(fixture.pendingValue);
        assertThat(result.isCancelled()).isTrue();
        assertThat(fixture.scan(fixture.selector.sourceGeneration())).isEqualTo(ScanOutcome.CLEAN);
    }

    @Test
    void unknownSelectorClosureStopsNewRecoveryWhileOldLeaseSurvivesExactRetryAndDrain() {
        var fixture = new Fixture(false);
        fixture.delayNextRead = true;
        var oldRead = fixture.recovery.recover(fixture.descriptor());
        var old = fixture.runtime.currentAuthority().get();
        var nextSelector = successor(fixture.selector);
        var next = KafkaBookKeeperM4RecoveryV2.project(fixture.descriptor(), nextSelector);
        fixture.physical.store.dropNextSelectorCas = true;
        assertThat(fixture.runtime.closeFallback(
                        fixture.selector,
                        old,
                        next,
                        next.selectedViewSha256(),
                        next.sourceGeneration(),
                        fixture.physical.sources))
                .isEqualTo(Outcome.RETRY_EXACT_PREDECESSOR);
        assertThat(fixture.runtime.currentAuthority().get().admitting()).isFalse();
        int reads = fixture.nativeReads;
        assertThatThrownBy(() -> fixture.recovery.recover(fixture.descriptor()).join())
                .hasRootCauseMessage("M4 read admission failed before source I/O: ADMISSION_CLOSED");
        assertThat(fixture.nativeReads).isEqualTo(reads);
        assertThat(fixture.scan(old.sourceGeneration())).isEqualTo(ScanOutcome.PINNED);
        assertThat(fixture.runtime.closeFallback(
                        fixture.selector,
                        old,
                        next,
                        next.selectedViewSha256(),
                        next.sourceGeneration(),
                        fixture.physical.sources))
                .isEqualTo(Outcome.APPLIED);
        assertThat(fixture.runtime.currentAuthority().get()).isEqualTo(next);
        assertThat(fixture.scan(old.sourceGeneration())).isEqualTo(ScanOutcome.PINNED);
        fixture.pending.complete(fixture.pendingValue);
        assertThat(oldRead.join().view().descriptor()).isEqualTo(fixture.descriptor());
        assertThat(oldRead.join().capturedAuthority()).isSameAs(old);
        assertThat(oldRead.join().capturedAuthority())
                .isNotSameAs(fixture.runtime.currentAuthority().get());
        assertThat(fixture.scan(old.sourceGeneration())).isEqualTo(ScanOutcome.CLEAN);
        assertThat(fixture.recovery.recover(fixture.descriptor()).join().view().descriptor())
                .isEqualTo(fixture.descriptor());
    }

    @Test
    void hazardExhaustionRejectsBeforeProviderIoAndCapacityReturnsOnlyAfterDrain() {
        var fixture = new Fixture(false, 1);
        fixture.delayNextRead = true;
        var first = fixture.recovery.recover(fixture.descriptor());
        int reads = fixture.nativeReads;
        assertThatThrownBy(() -> fixture.recovery.recover(fixture.descriptor()).join())
                .hasRootCauseMessage("M4 read admission failed before source I/O: EXHAUSTED");
        assertThat(fixture.nativeReads).isEqualTo(reads);
        fixture.pending.complete(fixture.pendingValue);
        first.join();
        fixture.recovery.recover(fixture.descriptor()).join();
        assertThat(fixture.scan(fixture.selector.sourceGeneration())).isEqualTo(ScanOutcome.CLEAN);
    }

    @Test
    void differentSelectedViewAndMalformedSourcePlanFailBeforeAnyProviderIo() {
        var fixture = new Fixture(false);
        var original = fixture.runtime.currentAuthority().get();
        var foreign = new KafkaSealedBookKeeperDescriptorV2Test.Fixture(true).descriptor;
        assertThatThrownBy(() -> fixture.recovery.recover(foreign).join())
                .hasRootCauseMessage("BK recovery descriptor differs from the captured M4 authority");
        var cell = original.publicationCell();
        var gap = new BindingReadPublicationCellV1(
                cell.sourceGeneration(),
                cell.readableUpperBound(),
                0,
                new BindingReadRouteTableV1(List.of()),
                cell.protocolStateReference());
        fixture.runtime.currentAuthority().set(withCell(original, gap));
        assertThatThrownBy(() -> fixture.recovery.recover(fixture.descriptor()).join())
                .hasRootCauseMessage("BK recovery lacks the exact complete M4 source plan");
        assertThat(fixture.nativeReads).isZero();
        assertThat(fixture.nativeCaptures).isZero();
        assertThat(fixture.scan(fixture.selector.sourceGeneration())).isEqualTo(ScanOutcome.CLEAN);
    }

    @Test
    void nativeFailureDrainsLeaseWithoutFallbackOrFurtherSourceReads() {
        var fixture = new Fixture(false);
        fixture.delayNextRead = true;
        var result = fixture.recovery.recover(fixture.descriptor());
        assertThat(fixture.scan(fixture.selector.sourceGeneration())).isEqualTo(ScanOutcome.PINNED);
        fixture.pending.completeExceptionally(new IllegalStateException("native read failed"));
        assertThatThrownBy(result::join).hasRootCauseMessage("native read failed");
        assertThat(fixture.nativeReads).isEqualTo(1);
        assertThat(fixture.scan(fixture.selector.sourceGeneration())).isEqualTo(ScanOutcome.CLEAN);
    }

    @Test
    void projectionDoesNotAcceptPredecessorSelectionOrReopenStoppedAdmission() {
        var fixture = new Fixture(false);
        assertThatThrownBy(() -> KafkaBookKeeperM4RecoveryV2.project(
                        fixture.descriptor(),
                        fixture.physical.input.plan().sourceCut().predecessorSelector()))
                .isInstanceOf(IllegalArgumentException.class);
        var original = fixture.selector;
        var stopped = new BindingReadSelector(
                original.binding(),
                original.selectedViewSha256(),
                original.ownerEpoch(),
                original.readAdmissionEpoch(),
                original.sourceGeneration(),
                original.mode(),
                AdmissionState.STOPPED,
                original.fallbackSetSha256(),
                original.capability(),
                original.pendingAnchors(),
                original.activeBatches());
        fixture.runtime.currentAuthority().set(KafkaBookKeeperM4RecoveryV2.project(fixture.descriptor(), stopped));
        assertThatThrownBy(() -> fixture.recovery.recover(fixture.descriptor()).join())
                .hasRootCauseMessage("M4 read admission failed before source I/O: ADMISSION_CLOSED");
        assertThat(fixture.nativeReads).isZero();
        assertThat(fixture.nativeCaptures).isZero();
    }

    private static BindingReadSelector successor(BindingReadSelector old) {
        return new BindingReadSelector(
                old.binding(),
                old.selectedViewSha256(),
                old.ownerEpoch(),
                old.readAdmissionEpoch() + 1,
                old.sourceGeneration() + 1,
                SelectorMode.PREFERRED_ONLY,
                AdmissionState.ADMITTING,
                Optional.empty(),
                old.capability(),
                List.of(),
                List.of());
    }

    private static BindingReadAuthorityV1 withCell(
            BindingReadAuthorityV1 authority, BindingReadPublicationCellV1 cell) {
        return new BindingReadAuthorityV1(
                authority.bindingId(),
                authority.topicIncarnationIdentity(),
                authority.storageEpochId(),
                authority.protocol(),
                authority.selectedViewSha256(),
                authority.ownerEpoch(),
                authority.readAdmissionEpoch(),
                authority.admitting(),
                authority.capabilityGeneration(),
                authority.capabilityEvidenceSha256(),
                cell);
    }

    private static final class Fixture {
        final KafkaSealedBookKeeperDescriptorV2Test.Fixture physical;
        final BindingReadSelector selector;
        final BindingReadSelectorRuntimeV1 runtime;
        final BindingReadHazardPoolV1 hazards;
        final KafkaBookKeeperM4RecoveryV2 recovery;
        boolean delayNextRead;
        CompletableFuture<RunLedgerReadResultV1> pending;
        RunLedgerReadResultV1 pendingValue;
        int nativeReads;
        int nativeCaptures;

        Fixture(boolean empty) {
            this(empty, 2);
        }

        Fixture(boolean empty, int capacity) {
            physical = new KafkaSealedBookKeeperDescriptorV2Test.Fixture(empty);
            physical.publish();
            selector = physical.m4.readSelector().orElseThrow();
            var authority = KafkaBookKeeperM4RecoveryV2.project(descriptor(), selector);
            runtime = new BindingReadSelectorRuntimeV1(selector.binding(), physical.m4, selector, authority);
            runtime.installExactDurable(authority);
            hazards = new BindingReadHazardPoolV1(capacity, 4);
            var provider = (BookKeeperCellSession) Proxy.newProxyInstance(
                    BookKeeperCellSession.class.getClassLoader(),
                    new Class<?>[] {BookKeeperCellSession.class},
                    (proxy, method, args) -> {
                        Object result;
                        try {
                            result = method.invoke(physical.session, args);
                        } catch (InvocationTargetException failure) {
                            throw failure.getCause();
                        }
                        if (method.getName().equals("readExactEntry")) {
                            nativeReads++;
                            if (delayNextRead) {
                                delayNextRead = false;
                                pendingValue = (RunLedgerReadResultV1) ((CompletableFuture<?>) result).join();
                                pending = new CompletableFuture<>();
                                return pending;
                            }
                        }
                        return result;
                    });
            var reader = new KafkaSealedBookKeeperReaderV2(
                    provider,
                    handle -> {
                        nativeCaptures++;
                        return CompletableFuture.completedFuture(new CaptureResult(
                                CaptureOutcome.EXACT_TARGET,
                                Optional.of(physical.seals.get(
                                        handle.ledgerIdentity().ledgerId()))));
                    },
                    1_000_000);
            recovery = new KafkaBookKeeperM4RecoveryV2(runtime.currentAuthority(), hazards, Runnable::run, reader);
        }

        KafkaSealedBookKeeperDescriptorV2 descriptor() {
            return physical.descriptor;
        }

        ScanOutcome scan(long generation) {
            return hazards.scan(selector.binding().bindingId(), generation);
        }
    }
}
