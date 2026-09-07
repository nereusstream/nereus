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
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.object.read.BindingReadHazardPoolV1;
import com.nereusstream.storage.object.read.BindingReadHazardPoolV1.ScanOutcome;
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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Locked native BK IO and actual M4 kernel; durable control and protocol source admission remain synthetic. */
class KafkaBookKeeperM4RecoveryV2RealTest {
    @BeforeAll
    static void connect() throws Exception {
        KafkaSealedBookKeeperDescriptorV2RealTest.connect();
    }

    @AfterAll
    static void closeClient() throws Exception {
        KafkaSealedBookKeeperDescriptorV2RealTest.closeClient();
    }

    @Test
    void realReadCallbackDelayRetainsOldGenerationAcrossDurableSelectorClosure() throws Exception {
        var context = new KafkaSealedBookKeeperDescriptorV2RealTest.Context(
                KafkaBookKeeperCompactionTestSupportV2.input(false, 501));
        var owner = Executors.newSingleThreadExecutor();
        var release = new CompletableFuture<Void>();
        var fresh = context.session();
        try {
            context.publish();
            var selected = context.m4.readSelector().orElseThrow();
            var authority = KafkaBookKeeperM4RecoveryV2.project(context.descriptor, selected);
            var runtime = new BindingReadSelectorRuntimeV1(selected.binding(), context.m4, selected, authority);
            runtime.installExactDurable(authority);
            var observed = new CountDownLatch(1);
            var first = new AtomicBoolean(true);
            var delayed = (BookKeeperCellSession) Proxy.newProxyInstance(
                    BookKeeperCellSession.class.getClassLoader(),
                    new Class<?>[] {BookKeeperCellSession.class},
                    (proxy, method, args) -> {
                        final Object result;
                        try {
                            result = method.invoke(fresh, args);
                        } catch (InvocationTargetException failure) {
                            throw failure.getCause();
                        }
                        if (method.getName().equals("readExactEntry") && first.compareAndSet(true, false)) {
                            return ((CompletionStage<?>) result).thenCompose(value -> {
                                observed.countDown();
                                return release.thenApply(ignored -> value);
                            });
                        }
                        return result;
                    });
            var hazards = new BindingReadHazardPoolV1(2, 4);
            var recovery = new KafkaBookKeeperM4RecoveryV2(
                    runtime.currentAuthority(), hazards, owner, context.reader(delayed));
            var pending = recovery.recover(context.descriptor);
            assertThat(observed.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(hazards.scan(selected.binding().bindingId(), selected.sourceGeneration()))
                    .isEqualTo(ScanOutcome.PINNED);
            var successor = new BindingReadSelector(
                    selected.binding(),
                    selected.selectedViewSha256(),
                    selected.ownerEpoch(),
                    selected.readAdmissionEpoch() + 1,
                    selected.sourceGeneration() + 1,
                    SelectorMode.PREFERRED_ONLY,
                    AdmissionState.ADMITTING,
                    Optional.empty(),
                    selected.capability(),
                    List.of(),
                    List.of());
            var next = KafkaBookKeeperM4RecoveryV2.project(context.descriptor, successor);
            assertThat(owner.submit(() -> runtime.closeFallback(
                                    selected,
                                    authority,
                                    next,
                                    next.selectedViewSha256(),
                                    next.sourceGeneration(),
                                    context.protections))
                            .get(10, TimeUnit.SECONDS))
                    .isEqualTo(Outcome.APPLIED);
            assertThat(runtime.currentAuthority().get()).isSameAs(next);
            assertThat(hazards.scan(selected.binding().bindingId(), selected.sourceGeneration()))
                    .isEqualTo(ScanOutcome.PINNED);
            release.complete(null);
            var result = pending.get(30, TimeUnit.SECONDS);
            assertThat(result.capturedAuthority()).isSameAs(authority);
            assertThat(result.view().allowsPredecessorOffset(0)).isFalse();
            assertThat(result.view().lookup(0).orElseThrow().coverage().inclusiveStart())
                    .isEqualTo(1);
            assertThat(hazards.scan(selected.binding().bindingId(), selected.sourceGeneration()))
                    .isEqualTo(ScanOutcome.CLEAN);
        } finally {
            release.complete(null);
            owner.shutdown();
            assertThat(owner.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            fresh.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
            context.close();
        }
    }

    @Test
    void emptyNativeOutputRecoversUnderM4WithIndexOnlyRouteAndNoControlIo() throws Exception {
        var context = new KafkaSealedBookKeeperDescriptorV2RealTest.Context(
                KafkaBookKeeperCompactionTestSupportV2.input(true, 502));
        var owner = Executors.newSingleThreadExecutor();
        var fresh = context.session();
        try {
            context.publish();
            var selected = context.m4.readSelector().orElseThrow();
            var authority = KafkaBookKeeperM4RecoveryV2.project(context.descriptor, selected);
            var runtime = new BindingReadSelectorRuntimeV1(selected.binding(), context.m4, selected, authority);
            runtime.installExactDurable(authority);
            var hazards = new BindingReadHazardPoolV1(1, 4);
            var recovery =
                    new KafkaBookKeeperM4RecoveryV2(runtime.currentAuthority(), hazards, owner, context.reader(fresh));
            int metadataOperations = context.store.operations.size();
            var result = recovery.recover(context.descriptor).get(30, TimeUnit.SECONDS);
            assertThat(result.view().descriptor().batchCount()).isZero();
            assertThat(result.view().lookup(0)).isEmpty();
            assertThat(result.view().allowsPredecessorOffset(0)).isFalse();
            assertThat(result.view().descriptor().task().parts())
                    .allMatch(part -> part.kind() == KafkaBookKeeperInventoryV2.PartKind.INDEX);
            assertThat(context.store.operations).hasSize(metadataOperations);
            assertThat(hazards.scan(selected.binding().bindingId(), selected.sourceGeneration()))
                    .isEqualTo(ScanOutcome.CLEAN);
        } finally {
            owner.shutdown();
            assertThat(owner.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            fresh.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
            context.close();
        }
    }
}
