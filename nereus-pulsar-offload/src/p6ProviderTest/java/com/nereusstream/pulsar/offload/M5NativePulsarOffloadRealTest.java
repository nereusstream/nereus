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

package com.nereusstream.pulsar.offload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.aggregate.FrameEncodingPolicyValueV1;
import com.nereusstream.domain.aggregate.InitialStorageEpochV1;
import com.nereusstream.domain.aggregate.PolicyCatalogDigest;
import com.nereusstream.domain.aggregate.ProfileOriginV1;
import com.nereusstream.domain.aggregate.StorageProfileV1;
import com.nereusstream.domain.aggregate.TopicBindingAggregateV1;
import com.nereusstream.domain.aggregate.TopicBindingV1;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.DeterministicTopicIdsV1;
import com.nereusstream.domain.codec.Nta1CodecV1;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.PulsarCellId;
import com.nereusstream.domain.identity.ReservationDomainId;
import com.nereusstream.domain.protocol.ProtocolKindV1;
import com.nereusstream.domain.protocol.PulsarBindingGeneration;
import com.nereusstream.domain.protocol.PulsarPersistenceName;
import com.nereusstream.domain.protocol.PulsarProtocolCellIdentity;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.PulsarTopicName;
import com.nereusstream.metadata.oxia.v2.capability.OxiaPulsarTopicGenerationSelectorStore;
import com.nereusstream.metadata.oxia.v2.capability.OxiaTopicBindingAggregatePublisher;
import com.nereusstream.metadata.oxia.v2.capability.OxiaTopicBindingAggregateReader;
import com.nereusstream.metadata.oxia.v2.capability.PulsarTopicAuthorityCoordinator;
import com.nereusstream.metadata.oxia.v2.codec.OxiaV2CodecSet;
import com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys;
import com.nereusstream.metadata.oxia.v2.mutation.AsyncOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.mutation.ConditionalMutationEngine;
import com.nereusstream.metadata.oxia.v2.mutation.MutationFailureClassifier;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMetadataNamespaceV2;
import com.nereusstream.metadata.spi.model.AggregatePublicationCandidate;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.pulsar.offload.NereusPulsarLedgerOffloaderV1.AttemptKeyManager;
import com.nereusstream.pulsar.offload.NereusPulsarLedgerOffloaderV1.PreparedAttemptKey;
import com.nereusstream.pulsar.offload.NereusPulsarLedgerOffloaderV1.RuntimePolicy;
import com.nereusstream.pulsar.offload.PulsarOffloadBlockPolicyV1.BlockClass;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.RetentionClass;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.CompressionPolicy;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.AttemptKeyEnvelope;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperDigestTypeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperProtocolModeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperTimeoutClassV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace;
import com.nereusstream.storage.bookkeeper.BookKeeperV3Crc32cAddPayloadLimitV1;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.read.BindingReadHazardPoolV1.ScanOutcome;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProtectionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1;
import com.nereusstream.storage.object.s3.S3M5PhysicalNamespaceAuthorityV2;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.Notification;
import io.oxia.client.api.NotificationContinuityRegistration;
import io.oxia.client.api.NotificationContinuitySnapshot;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.RangeScanConsumer;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.DeleteRangeOption;
import io.oxia.client.api.options.GetOption;
import io.oxia.client.api.options.GetSequenceUpdatesOption;
import io.oxia.client.api.options.ListOption;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.RangeScanOption;
import java.io.Closeable;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperClientStats;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.CreateAdvBuilder;
import org.apache.bookkeeper.client.api.CreateBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.UpdatePropertiesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.LedgerMetadataUtils;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.CapturedReadBatch;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.CapturedReadLimits;
import org.apache.bookkeeper.mledger.offload.OffloadUtils;
import org.apache.bookkeeper.mledger.util.Futures;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsData;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.OffloadedReadPriority;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.PutBucketVersioningRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

/** Actual Pulsar/BK/S3 source prerequisite; this supplies no M4 release or M5 deletion qualification. */
class M5NativePulsarOffloadRealTest {
    private static final String M6_BROKER_PROVIDER_SCOPE = System.getProperty("nereus.m6.brokerDeleteProviderScope");

    private static final String SDK_NAMESPACE_SUFFIX = System.getProperty("nereus.m5.nativeM4RuntimeOutput") != null
                    && "DELETE_AFTER_VERIFIED".equals(System.getProperty("nereus.m5.nativeOffloadRetentionClass"))
            ? "-sdk-delete"
            : "";
    private static final String BUCKET = "nereus-m5-native-offload" + SDK_NAMESPACE_SUFFIX;
    private static final String LEDGER = "m5-native/pulsar-offload" + SDK_NAMESPACE_SUFFIX;
    private static final String SUBSCRIPTION = "retained-subscription";
    private static final int SEALED_ENTRIES = 17;
    private static final int ENTRY_BYTES = 4 * PulsarOffloadLimitCandidateV1.MIB;
    private static final long NATIVE_BK_CANONICAL_CAPACITY = 500_000_000L;
    private static final long NATIVE_BK_LIFETIME_RESOURCE_BOUND = 444;
    private static final AtomicReference<com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Layout>
            NATIVE_BK_LAYOUT = new AtomicReference<>();
    private static final AtomicBoolean NATIVE_BK_INITIAL_OBSERVED = new AtomicBoolean();
    private static final PulsarOffloadLimitCandidateV1 LIMITS =
            PulsarOffloadLimitCandidateV1.adr0056EvidenceCandidate();
    private static final CapturedReadLimits OWNED_READ_LIMITS =
            new CapturedReadLimits(LIMITS.maxEntriesPerBlock(), LIMITS.maxEntryBytes(), LIMITS.maxDecodedBlockBytes());
    private static final SecretKey KEY = new SecretKeySpec(new byte[32], "AES");
    private static final SecretKey WRAPPING_KEY = new SecretKeySpec(new byte[32], "AES");
    private static final AttemptKeyEnvelope ENVELOPE = wrappedKey();

    @TempDir
    Path temporaryDirectory;

    @AfterAll
    static void recordFinalNativeBkQuota() throws Exception {
        boolean owner = System.getProperty("nereus.m5.nativeM4AdmissionFile") != null;
        if (NATIVE_BK_LAYOUT.get() == null) {
            return;
        }
        if (owner) {
            assertThat(NATIVE_BK_INITIAL_OBSERVED.get()).isTrue();
        }
        var observed = observeActualQuota(owner ? "bk-final" : "bk-sdk-completion", NATIVE_BK_LAYOUT.get(), null);
        if (observed == null) {
            return; // The SDK prerequisite phase precedes the first Native BK quota initialization.
        }
        assertThat(observed.head().capacityBytes()).isEqualTo(NATIVE_BK_CANONICAL_CAPACITY);
        assertThat(observed.head().reservedResources() + observed.head().settledResources())
                .isLessThanOrEqualTo(NATIVE_BK_LIFETIME_RESOURCE_BOUND);
        assertThat(observed.head().pending()).isEmpty();
        assertThat(observed.head().births()).isEmpty();
    }

    private static com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2.Snapshot observeActualQuota(
            String phase,
            com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Layout layout,
            PhysicalResourceIdV2 target)
            throws Exception {
        try (var client = OxiaClientBuilder.create(System.getProperty("nereus.m5.nativeOffloadOxia"))
                .namespace("default")
                .requestTimeout(Duration.ofSeconds(10))
                .asyncClient()
                .get(30, TimeUnit.SECONDS)) {
            String key = layout.nativeKey(com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.HEAD_KEY);
            var actual = client.get(key).get(30, TimeUnit.SECONDS);
            if (actual == null) {
                assertThat(phase).isEqualTo("bk-sdk-completion");
                return null;
            }
            var head = com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Head.decode(
                    CanonicalBytes.copyOf(actual.value()));
            layout.verify(head);
            String entry = "none";
            String resource = "none";
            String done = "none";
            if (target != null) {
                layout.requireResource(target);
                resource = target.sha256().toHex();
                var stored = java.util.Objects.requireNonNull(client.get(layout.nativeKey(
                                com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.entryKey(target.sha256())))
                        .get(30, TimeUnit.SECONDS));
                entry = java.util.Base64.getEncoder().encodeToString(stored.value());
                var authority = java.util.Objects.requireNonNull(
                        client.get(layout.nativeKey(target.authorityKey())).get(30, TimeUnit.SECONDS));
                if (com.nereusstream.storage.object.gc.M5TargetDeleteDoneV2.isCompactDone(
                        CanonicalBytes.copyOf(authority.value()))) {
                    done = java.util.Base64.getEncoder().encodeToString(authority.value());
                }
            }
            System.out.println("NATIVE_PULSAR_QUOTA phase=" + phase + " nativeKey=" + key
                    + " version=" + actual.version().versionId()
                    + " provider=" + layout.namespace().providerKind()
                    + " service="
                    + java.util.Base64.getEncoder()
                            .encodeToString(
                                    layout.namespace().serviceIdentity().bytes().toByteArray())
                    + " container="
                    + java.util.Base64.getEncoder()
                            .encodeToString(layout.namespace()
                                    .containerIdentity()
                                    .bytes()
                                    .toByteArray())
                    + " head=" + java.util.Base64.getEncoder().encodeToString(actual.value())
                    + " charged=" + head.usedBytes(layout) + " available=" + head.availableBytes(layout)
                    + " resource=" + resource + " entry=" + entry + " done=" + done);
            return new com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2.Snapshot(
                    head, head.usedBytes(layout), head.availableBytes(layout));
        }
    }

    @Test
    void nativeBindingCapturesExactP1OwnerAndObjectNamespace() throws Exception {
        nativeBindingCapture(0);
    }

    @Test
    void nativeBindingForM4Conformance() throws Exception {
        nativeBindingCapture(3);
    }

    @Test
    void nativePreferredOnlySourceSurvivesMissingBookKeeperMetadata() throws Exception {
        nativeBindingCapture(5);
    }

    @Test
    void nativeM4PreferredOnlyOwnerRejectsBookKeeperFallback() throws Exception {
        nativeBindingCapture(6);
    }

    @Test
    void nativeM4OwnerHoldsActualCleanupAndBorrowedAliases() throws Exception {
        nativeBindingCapture(1);
    }

    @Test
    void nativeM4CleanupFailureRetainsHazard() throws Exception {
        nativeBindingCapture(2);
    }

    @Test
    void nativeM4OrdinaryReadCannotFallbackAfterCutChanges() throws Exception {
        nativeBindingCapture(4);
    }

    @Test
    void nativeM4FallbackRetiresAfterActualGetAndAliasDrain() throws Exception {
        nativeBindingCapture(7);
    }

    @Test
    void nativeM4FallbackUnknownRetainsProtectionAndRetriesExact() throws Exception {
        nativeBindingCapture(8);
    }

    @Test
    void nativeM4SourceMapUnknownCannotPublishSelector() throws Exception {
        nativeBindingCapture(9);
    }

    @Test
    void nativeM4ColdSourceUsesPublishedObjectVersions() throws Exception {
        nativeBindingCapture(10);
    }

    @Test
    void nativeM4ColdOwnerRequiresActualDurableDrain() throws Exception {
        nativeBindingCapture(11);
    }

    @Test
    void nativeM4ColdOwnerUnknownRetainsUniqueAttempt() throws Exception {
        nativeBindingCapture(12);
    }

    @Test
    void nativeM4CellSharesRetainUnknownReaderAndHealthyBinding() throws Exception {
        nativeBindingCapture(13);
    }

    @Test
    void nativeSourcePreparationRetainsHeldRootOpen() throws Exception {
        nativeBindingCapture(14);
    }

    @Test
    void nativePreparedSourceRequiresGrantAndDrainsWithOwner() throws Exception {
        nativeBindingCapture(15);
    }

    @Test
    void nativeM4VerificationUsesCachedVersionsAndPrivateDrain() throws Exception {
        nativeBindingCapture(16);
    }

    @Test
    void nativeM4CancelledVerificationCannotClearSdkLease() throws Exception {
        nativeBindingCapture(17);
    }

    @Test
    void nativeM4CompletedVerificationObserverCannotClearSdkLease() throws Exception {
        nativeBindingCapture(18);
    }

    @Test
    void nativeCellAllocatorRejectsIndependentOwnerUntilActualDrain() throws Exception {
        nativeBindingCapture(19);
    }

    @Test
    void nativeCellAllocatorUnknownDrainRetainsOriginalOwner() throws Exception {
        nativeBindingCapture(20);
    }

    @Test
    void nativeCellReservationUnknownDoesNotDispatchSource() throws Exception {
        nativeBindingCapture(21);
    }

    @Test
    void nativeCellReservationReleaseUnknownRetainsLocalCharge() throws Exception {
        nativeBindingCapture(22);
    }

    @Test
    void nativeInitialOwnerUnknownRetainsExactCleanup() throws Exception {
        nativeBindingCapture(23);
    }

    @Test
    void nativeSuccessorOwnerUnknownRetainsExactCleanup() throws Exception {
        nativeBindingCapture(24);
    }

    @Test
    void nativeCancelledInitialOwnerRetainsOriginalGrantCleanup() throws Exception {
        nativeBindingCapture(27);
    }

    @Test
    void nativeCompletedInitialOwnerRetainsOriginalGrantCleanup() throws Exception {
        nativeBindingCapture(28);
    }

    @Test
    void nativeInitialGrantUnknownWithoutActorRetainsQuarantine() throws Exception {
        nativeBindingCapture(29);
    }

    @Test
    void nativeInitialClaimRejectsConcurrentInvocation() throws Exception {
        nativeBindingCapture(30);
    }

    @Test
    void nativeInitialClaimUnknownAndAbortUnknownKeepOriginalCharge() throws Exception {
        nativeBindingCapture(31);
    }

    @Test
    void nativeInitialClaimMissingHeadCannotReinitialize() throws Exception {
        nativeBindingCapture(32);
    }

    @Test
    void nativeActorlessGrantReconcilesActualDrainAndColdOwner() throws Exception {
        nativeBindingCapture(33);
    }

    @Test
    void nativeGuardedPulsarCreatesDurableBirthsAndMembers() throws Exception {
        nativeBindingCapture(25);
    }

    @Test
    void nativeGuardedPulsarBirthUnknownCannotCreatePhysicalLedger() throws Exception {
        nativeBindingCapture(26);
    }

    @Test
    void nativeMetadataCleanupUnknownRetainsOriginalSdkCapacity() throws Exception {
        nativeBindingCapture(34);
    }

    @Test
    void nativeMetadataTicketUnknownRetainsOriginalNoDispatchCleanup() throws Exception {
        nativeBindingCapture(35);
    }

    @Test
    void nativeMetadataWriteUnknownUsesActualVersionFence() throws Exception {
        nativeBindingCapture(36);
    }

    @Test
    void nativeMetadataWriteUnknownKeepsOriginalProofAcrossCleanupLoss() throws Exception {
        nativeBindingCapture(37);
    }

    @Test
    void nativeMetadataWriteUnknownRejectsChangedCreationIdentity() throws Exception {
        nativeBindingCapture(38);
    }

    @Test
    void nativeMetadataCreateUnknownReconcilesOriginalCreatedIdentity() throws Exception {
        nativeBindingCapture(39);
    }

    @Test
    void nativeMetadataCreateFencedAbsenceExcludesLateCreation() throws Exception {
        nativeBindingCapture(40);
    }

    @Test
    void nativeMetadataCreateUnknownRejectsForeignCreator() throws Exception {
        nativeBindingCapture(41);
    }

    @Test
    void nativeMetadataCreatePreDispatchReadFailureReconcilesOriginalTicket() throws Exception {
        nativeBindingCapture(42);
    }

    @Test
    void nativeMaterializationSourceUsesActualChainAndEveryEntry() throws Exception {
        nativeBindingCapture(43);
    }

    @Test
    void nativeMaterializationSourceClosesFromOriginalDeliveryCallback() throws Exception {
        nativeBindingCapture(44);
    }

    @Test
    void nativeMaterializationQualificationGetRetainsWorkspaceThroughClose() throws Exception {
        nativeBindingCapture(45);
    }

    @Test
    void nativeMaterializationConnectionRetainsOriginalSdkUntilActualGetDrain() throws Exception {
        nativeBindingCapture(46);
    }

    @Test
    void nativeMaterializationInputPersistsExactP1ViewAndRecoveryReferences() throws Exception {
        nativeBindingCapture(47);
    }

    @Test
    void nativeMaterializationInputUnknownRetainsOriginalCellChargeUntilExactRecovery() throws Exception {
        nativeBindingCapture(48);
    }

    @Test
    void nativeMaterializationProviderScopeUsesOriginalSdkAndFrozenCellPrefix() throws Exception {
        nativeBindingCapture(49);
    }

    @Test
    void nativeMaterializationProviderScopeUnknownRetainsOriginalSourceAndCellCharge() throws Exception {
        nativeBindingCapture(50);
    }

    @Test
    void nativeMaterializationTaskBindsOriginalPlanScopeOwnerAndSharedCellCharge() throws Exception {
        nativeBindingCapture(51);
    }

    @Test
    void nativeMaterializationTaskPlanUnknownRetainsOriginalSourceAndCellCharge() throws Exception {
        nativeBindingCapture(52);
    }

    @Test
    void nativeMaterializationTaskCapacityRejectsBeforePlanAndOutput() throws Exception {
        nativeBindingCapture(53);
    }

    @Test
    void nativeMaterializationTaskCurrentReadRetainsOriginalSourceUntilDrain() throws Exception {
        nativeBindingCapture(54);
    }

    @Test
    void nativeMaterializationTaskUnknownRetainsExclusiveSourceThenAdvancesOriginalWorker() throws Exception {
        nativeBindingCapture(55);
    }

    @Test
    void nativeMaterializationPendingInventoryUnknownRetainsOriginalKeysAndCharge() throws Exception {
        nativeBindingCapture(56);
    }

    @Test
    void nativeMaterializationBirthCreditUnknownRetainsOriginalCapacityUntilNoDispatchDrain() throws Exception {
        nativeBindingCapture(57);
    }

    @Test
    void nativeMaterializationBirthCapacityRejectsBeforeAnyProviderOutput() throws Exception {
        nativeBindingCapture(58);
    }

    @Test
    void nativeMaterializationWritesRealVersionsAndIndependentlyVerifiesEveryEntry() throws Exception {
        nativeBindingCapture(59);
    }

    @Test
    void nativeMaterializationWriteIntentUnknownCannotDispatchProviderOutput() throws Exception {
        nativeBindingCapture(60);
    }

    @Test
    void nativeMaterializationCancelledOutputObserverRetainsActualPutUntilDrain() throws Exception {
        nativeBindingCapture(61);
    }

    @Test
    void nativeMaterializationActualPutReplyLossUsesOriginalImmutableVersion() throws Exception {
        nativeBindingCapture(62);
    }

    @Test
    void nativeMaterializationPartialCutRetainsOutsideEntryAndActiveTailRoutes() throws Exception {
        nativeBindingCapture(63);
    }

    @Test
    void nativePartialMaterializationColdFactoryRetainsOriginalSourceDependencies() throws Exception {
        nativeBindingCapture(79);
    }

    @Test
    void nativeMaterializationMappedGetObserverCancelRetainsSdkHazardAndCellUntilDrain() throws Exception {
        nativeBindingCapture(64);
    }

    @Test
    void nativeMaterializationPublishesActualManifestAfterOriginalOwnerDrain() throws Exception {
        nativeBindingCapture(65);
    }

    @Test
    void nativeMaterializationAppliedPublicationUnknownRetainsOriginalCandidateUntilHandoff() throws Exception {
        nativeBindingCapture(66);
    }

    @Test
    void nativeMaterializationStopBeforeSelectorDrainsOriginalPhysicalTickets() throws Exception {
        nativeBindingCapture(67);
    }

    @Test
    void nativeRecoveryAddReplyLossRetainsOriginalSdkReferencesAfterFence() throws Exception {
        nativeBindingCapture(68);
    }

    @Test
    void nativeExplicitLacReplyLossRetainsOriginalSdkReferencesAfterFence() throws Exception {
        nativeBindingCapture(69);
    }

    @Test
    void nativeCursorResetReplyLossRetainsOriginalCAndDReferences() throws Exception {
        nativeBindingCapture(70);
    }

    @Test
    void nativeCursorResetRejectsCommittedTrimBeforeOriginalSdkDispatch() throws Exception {
        nativeBindingCapture(71);
    }

    @Test
    void nativeReadOwnerPinsCommittedTrimThroughOriginalEntryDrain() throws Exception {
        nativeBindingCapture(72);
    }

    @Test
    void nativeEnrolledReadOwnerRejectsMissingControlWriterBeforeGrant() throws Exception {
        nativeBindingCapture(73);
    }

    @Test
    void nativeCursorFloorScansActualCheckpointInsideSingleAuthorityFence() throws Exception {
        nativeBindingCapture(74);
    }

    @Test
    void nativeCursorTrimColdCommitRecoversDurableSnapshotAfterAppliedReplyLoss() throws Exception {
        nativeBindingCapture(75);
    }

    @Test
    void nativeEnrolledColdOwnerKeepsActualP1ReadTicketUntilEntryDrain() throws Exception {
        nativeBindingCapture(76);
    }

    @Test
    void nativeEnrolledColdOwnerUnknownKeepsOriginalGrantAndCell() throws Exception {
        nativeBindingCapture(77);
    }

    @Test
    void nativeObjectOnlyCursorAndGenerationFloorsSurviveMissingOriginalBookKeeper() throws Exception {
        nativeBindingCapture(78);
    }

    @Test
    void nativeTimedOutDataCreationRetainsActualSdkOwnership() throws Exception {
        verifyActualTimedOutCreation(false);
    }

    @Test
    void nativeTimedOutCursorCreationRetainsActualSdkOwnership() throws Exception {
        verifyActualTimedOutCreation(true);
    }

    private void verifyActualTimedOutCreation(boolean cursor) throws Exception {
        var objectExecutor = Executors.newSingleThreadExecutor();
        var fileExecutor = Executors.newSingleThreadExecutor();
        try (var metadata = metadata();
                var sourceClient = client(new AtomicInteger());
                var actualBk = new CreateReplyHoldBookKeeper()) {
            String bucket = "m5-native-create-" + UUID.randomUUID();
            sourceClient.createBucket(
                    CreateBucketRequest.builder().bucket(bucket).build());
            var store = new S3PulsarOffloadObjectStoreV1(sourceClient, bucket, objectExecutor, false);
            var offloader = offloader(store, fileExecutor);
            var factory = factory(metadata, actualBk);
            ManagedLedgerImpl ledger = null;
            try {
                var config = config(offloader)
                        .setMaxEntriesPerLedger(cursor ? 100 : 1)
                        .setMetadataOperationsTimeoutSeconds(1);
                ledger = (ManagedLedgerImpl) factory.open("native-create-drain-" + UUID.randomUUID(), config);
                var caller = new CompletableFuture<Object>();
                if (cursor) {
                    var opened = ledger.openCursor("held-cursor", InitialPosition.Earliest);
                    var first = ledger.addEntry(payload(0));
                    ledger.addEntry(payload(1));
                    actualBk.arm(true);
                    opened.asyncMarkDelete(
                            first,
                            new org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback() {
                                @Override
                                public void markDeleteComplete(Object ctx) {
                                    caller.complete(null);
                                }

                                @Override
                                public void markDeleteFailed(ManagedLedgerException failure, Object ctx) {
                                    caller.completeExceptionally(failure);
                                }
                            },
                            null);
                } else {
                    actualBk.arm(false);
                    ledger.addEntry(payload(0));
                    ledger.asyncAddEntry(
                            payload(1),
                            new org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback() {
                                @Override
                                public void addComplete(
                                        org.apache.bookkeeper.mledger.Position position,
                                        io.netty.buffer.ByteBuf data,
                                        Object ctx) {
                                    caller.complete(position);
                                }

                                @Override
                                public void addFailed(ManagedLedgerException failure, Object ctx) {
                                    caller.completeExceptionally(failure);
                                }
                            },
                            null);
                }
                assertThat(actualBk.created.await(30, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(() -> caller.get(30, TimeUnit.SECONDS))
                        .isInstanceOf(java.util.concurrent.ExecutionException.class);
                var closing = closeNativeLedger(ledger);
                var repeated = closeNativeLedger(ledger);
                repeated.complete(null);
                assertThatThrownBy(() -> closing.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
                actualBk.acknowledge.complete(null);
                closing.get(30, TimeUnit.SECONDS);
                long lateId = actualBk.handle.get().getId();
                var physical =
                        actualBk.getLedgerManager().readLedgerMetadata(lateId).get(30, TimeUnit.SECONDS);
                assertThat(physical.getValue().isClosed()).isTrue();
                assertThat(ledger.getLedgersInfo()).doesNotContainKey(lateId);
                assertThat(caller).isCompletedExceptionally();
                System.out.println("NATIVE_CREATE_DRAIN actualSdkCreated=true callerTimeout=true cursor=" + cursor
                        + " closeWaitsForActualResponse=true observerCannotFree=true lateHandleClosed=true"
                        + " orphanRetained=true physicalDeleteAuthority=false wholeM5Authority=false");
            } finally {
                actualBk.acknowledge.complete(null);
                if (ledger != null) {
                    closeNativeLedger(ledger).get(30, TimeUnit.SECONDS);
                }
                factory.shutdown();
                offloader.close();
                store.close().toCompletableFuture().get(30, TimeUnit.SECONDS);
            }
        } finally {
            objectExecutor.shutdown();
            fileExecutor.shutdown();
            assertThat(objectExecutor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            assertThat(fileExecutor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    /** Actual BK dispatch and metadata; only delivery of one completed SDK create response is held. */
    private static final class CreateReplyHoldBookKeeper extends BookKeeper {
        private final AtomicBoolean armed = new AtomicBoolean();
        private boolean cursor;
        private final CountDownLatch created = new CountDownLatch(1);
        private final CompletableFuture<Void> acknowledge = new CompletableFuture<>();
        private final AtomicReference<org.apache.bookkeeper.client.LedgerHandle> handle = new AtomicReference<>();

        private CreateReplyHoldBookKeeper() throws Exception {
            super(new ClientConfiguration()
                    .setUseV2WireProtocol(false)
                    .setBatchReadEnabled(false)
                    .setMetadataServiceUri("zk://" + System.getProperty("nereus.m5.nativeOffloadZk") + "/ledgers"));
        }

        private void arm(boolean holdCursor) {
            cursor = holdCursor;
            armed.set(true);
        }

        @Override
        public CreateBuilder newCreateLedgerOp() {
            var delegate = super.newCreateLedgerOp();
            return new CreateBuilder() {
                private boolean matches;

                @Override
                public CreateBuilder withEnsembleSize(int value) {
                    delegate.withEnsembleSize(value);
                    return this;
                }

                @Override
                public CreateBuilder withWriteQuorumSize(int value) {
                    delegate.withWriteQuorumSize(value);
                    return this;
                }

                @Override
                public CreateBuilder withAckQuorumSize(int value) {
                    delegate.withAckQuorumSize(value);
                    return this;
                }

                @Override
                public CreateBuilder withPassword(byte[] value) {
                    delegate.withPassword(value);
                    return this;
                }

                @Override
                public CreateBuilder withWriteFlags(java.util.EnumSet<WriteFlag> value) {
                    delegate.withWriteFlags(value);
                    return this;
                }

                @Override
                public CreateBuilder withDigestType(org.apache.bookkeeper.client.api.DigestType value) {
                    delegate.withDigestType(value);
                    return this;
                }

                @Override
                public CreateBuilder withLoggerContext(io.github.merlimat.slog.Logger value) {
                    delegate.withLoggerContext(value);
                    return this;
                }

                @Override
                public CreateAdvBuilder makeAdv() {
                    return delegate.makeAdv();
                }

                @Override
                public CreateBuilder withCustomMetadata(Map<String, byte[]> value) {
                    matches = value.containsKey("pulsar/cursor") == cursor;
                    delegate.withCustomMetadata(value);
                    return this;
                }

                @Override
                public CompletableFuture<WriteHandle> execute() {
                    var actual = delegate.execute();
                    if (!matches || !armed.compareAndSet(true, false)) {
                        return actual;
                    }
                    return actual.thenCompose(createdHandle -> {
                        handle.set((org.apache.bookkeeper.client.LedgerHandle) createdHandle);
                        created.countDown();
                        return acknowledge.thenApply(ignored -> createdHandle);
                    });
                }
            };
        }
    }

    /** Forwards every operation to the actual SDK; only completed selector responses are lost. */
    private static final class SelectorResponseFault implements AsyncOxiaClient {
        private final AsyncOxiaClient delegate;
        private final AtomicBoolean armed = new AtomicBoolean();
        private final AtomicBoolean blockedReads = new AtomicBoolean();
        private final AtomicInteger putLosses = new AtomicInteger();
        private final AtomicInteger getLosses = new AtomicInteger();
        private final AtomicInteger watchedPutAttempts = new AtomicInteger();
        private final AtomicBoolean delayNextPut = new AtomicBoolean();
        private volatile DelayedPut delayedPut;

        private record DelayedPut(String key, byte[] value, Set<PutOption> options) {}

        private volatile String selector;
        private Predicate<byte[]> candidate = value -> true;
        private volatile Predicate<byte[]> heldCandidate;
        private volatile String heldKey;
        private volatile String heldActualKey;
        private final AtomicInteger heldRemaining = new AtomicInteger();
        private volatile CountDownLatch heldApplied;
        private volatile CompletableFuture<Void> heldRelease = CompletableFuture.completedFuture(null);
        private volatile String heldReadKey;
        private final AtomicBoolean holdRead = new AtomicBoolean();
        private volatile CountDownLatch heldReadEntered;
        private volatile CompletableFuture<Void> heldReadRelease = CompletableFuture.completedFuture(null);

        private SelectorResponseFault(AsyncOxiaClient delegate) {
            this.delegate = delegate;
        }

        void arm(String key) {
            selector = key;
            candidate = value -> true;
            armed.set(true);
        }

        void arm(String key, Predicate<byte[]> expectedCandidate) {
            selector = key;
            candidate = expectedCandidate;
            armed.set(true);
        }

        void armCandidate(Predicate<byte[]> expectedCandidate) {
            selector = null;
            candidate = expectedCandidate;
            armed.set(true);
        }

        void delayUnknownPut(String key, Predicate<byte[]> expectedCandidate) {
            if (delayedPut != null) {
                throw new IllegalStateException("Original delayed PUT has not been settled");
            }
            selector = key;
            candidate = expectedCandidate;
            delayNextPut.set(true);
        }

        CompletableFuture<PutResult> dispatchDelayedPut() {
            var original = java.util.Objects.requireNonNull(delayedPut, "original delayed PUT");
            delayedPut = null;
            return delegate.put(original.key(), original.value(), original.options());
        }

        void clear() {
            armed.set(false);
            blockedReads.set(false);
            heldCandidate = null;
            heldKey = null;
            heldActualKey = null;
            heldRemaining.set(0);
            heldRelease.complete(null);
            holdRead.set(false);
            heldReadRelease.complete(null);
        }

        void holdOneGet(String key) {
            heldReadKey = key;
            heldReadEntered = new CountDownLatch(1);
            heldReadRelease = new CompletableFuture<>();
            holdRead.set(true);
        }

        void holdTwoAppliedPuts(Predicate<byte[]> expectedCandidate) {
            heldKey = null;
            heldRelease = new CompletableFuture<>();
            heldApplied = new CountDownLatch(2);
            heldRemaining.set(2);
            heldCandidate = expectedCandidate;
        }

        void holdAppliedPut(String key, Predicate<byte[]> expectedCandidate) {
            heldKey = key;
            heldActualKey = null;
            heldRelease = new CompletableFuture<>();
            heldApplied = new CountDownLatch(1);
            heldRemaining.set(1);
            heldCandidate = expectedCandidate;
        }

        void loseHeldPutReply() {
            var originalKey = heldKey == null ? heldActualKey : heldKey;
            if (originalKey == null || heldApplied.getCount() != 0 || heldRemaining.get() != 0) {
                throw new IllegalStateException("Original held PUT has not actually applied");
            }
            selector = originalKey;
            blockedReads.set(true);
            putLosses.incrementAndGet();
            heldRelease.completeExceptionally(new TimeoutException("actual selector CAS response lost"));
        }

        @Override
        public CompletableFuture<PutResult> put(String key, byte[] value, Set<PutOption> options) {
            if (key.equals(selector)) {
                watchedPutAttempts.incrementAndGet();
            }
            if (key.equals(selector) && candidate.test(value) && delayNextPut.compareAndSet(true, false)) {
                delayedPut = new DelayedPut(key, value.clone(), Set.copyOf(options));
                blockedReads.set(true);
                putLosses.incrementAndGet();
                return CompletableFuture.failedFuture(new TimeoutException("original ticket CAS dispatch delayed"));
            }
            var actual = delegate.put(key, value, options);
            var holdCandidate = heldCandidate;
            if (holdCandidate != null && (heldKey == null || heldKey.equals(key)) && holdCandidate.test(value)) {
                int remaining = heldRemaining.get();
                while (remaining > 0) {
                    if (heldRemaining.compareAndSet(remaining, remaining - 1)) {
                        var release = heldRelease;
                        var applied = heldApplied;
                        return actual.thenCompose(result -> {
                            heldActualKey = key;
                            applied.countDown();
                            return release.thenApply(ignored -> result);
                        });
                    }
                    remaining = heldRemaining.get();
                }
            }
            if ((selector == null || key.equals(selector))
                    && candidate.test(value)
                    && armed.compareAndSet(true, false)) {
                selector = key;
                return actual.thenCompose(result -> {
                    putLosses.incrementAndGet();
                    blockedReads.set(true);
                    return CompletableFuture.failedFuture(new TimeoutException("actual selector CAS response lost"));
                });
            }
            return actual;
        }

        @Override
        public CompletableFuture<PutResult> put(String key, byte[] value) {
            return put(key, value, Set.of());
        }

        @Override
        public CompletableFuture<GetResult> get(String key, Set<GetOption> options) {
            return delegate.get(key, options).thenCompose(result -> {
                if (key.equals(heldReadKey) && holdRead.compareAndSet(true, false)) {
                    heldReadEntered.countDown();
                    return heldReadRelease.thenApply(ignored -> result);
                }
                if (key.equals(selector) && blockedReads.get()) {
                    getLosses.incrementAndGet();
                    return CompletableFuture.failedFuture(
                            new TimeoutException("actual selector observation unavailable"));
                }
                return CompletableFuture.completedFuture(result);
            });
        }

        @Override
        public CompletableFuture<GetResult> get(String key) {
            return get(key, Set.of());
        }

        @Override
        public CompletableFuture<Boolean> delete(String key, Set<DeleteOption> options) {
            return delegate.delete(key, options);
        }

        @Override
        public CompletableFuture<Boolean> delete(String key) {
            return delegate.delete(key);
        }

        @Override
        public CompletableFuture<Void> deleteRange(String first, String last) {
            return delegate.deleteRange(first, last);
        }

        @Override
        public CompletableFuture<Void> deleteRange(String first, String last, Set<DeleteRangeOption> options) {
            return delegate.deleteRange(first, last, options);
        }

        @Override
        public CompletableFuture<List<String>> list(String first, String last) {
            return delegate.list(first, last);
        }

        @Override
        public CompletableFuture<List<String>> list(String first, String last, Set<ListOption> options) {
            return delegate.list(first, last, options);
        }

        @Override
        public void rangeScan(String first, String last, RangeScanConsumer consumer) {
            delegate.rangeScan(first, last, consumer);
        }

        @Override
        public void rangeScan(String first, String last, RangeScanConsumer consumer, Set<RangeScanOption> options) {
            delegate.rangeScan(first, last, consumer, options);
        }

        @Override
        public void notifications(Consumer<Notification> consumer) {
            delegate.notifications(consumer);
        }

        @Override
        public NotificationContinuityRegistration notificationContinuity(
                Consumer<NotificationContinuitySnapshot> consumer) {
            return delegate.notificationContinuity(consumer);
        }

        @Override
        public Closeable getSequenceUpdates(
                String key, Consumer<String> consumer, Set<GetSequenceUpdatesOption> options) {
            return delegate.getSequenceUpdates(key, consumer, options);
        }

        @Override
        public void close() {
            /* Borrowed SDK client is closed by the owning fixture. */
        }
    }

    @Test
    void nativeOnlyFirstAdmissionPersistsPolicyAndFencesKafkaFloors() throws Exception {
        nativeBindingCapture(80);
    }

    @Test
    void nativeOnlyTaskTerminalReferencesRequireOriginalCellDrain() throws Exception {
        nativeBindingCapture(81);
    }

    @Test
    void nativeBookKeeperReplacementRequiresExactReleaseAndReconcilesUnknown() throws Exception {
        nativeBindingCapture(82);
    }

    @Test
    void nativeBookKeeperExpiryRequiresTypedTrimAndRetainsActiveBacklog() throws Exception {
        nativeBindingCapture(83);
    }

    private void nativeBindingCapture(int m4Mode) throws Exception {
        boolean bookKeeperLifecycle = m4Mode == 82 || m4Mode == 83;
        String modeSuffix = m4Mode == 0 ? "" : "-m4-owner-" + m4Mode;
        String suffix = modeSuffix + SDK_NAMESPACE_SUFFIX;
        String bucket = BUCKET + "-binding" + modeSuffix;
        String root = "/nereus/v2/m5/native-pulsar-binding" + suffix;
        var incarnation = new PulsarTopicIncarnationIdentity(
                PulsarPersistenceName.fromString("tenant/ns/persistent/native-binding" + suffix),
                PulsarTopicName.fromString("persistent://tenant/ns/native-binding" + suffix),
                new PulsarBindingGeneration(1));
        var reads = new AtomicInteger();
        var heldGet = new AtomicReference<SdkHold>();
        var heldOutputPut = new AtomicReference<SdkHold>();
        var objectExecutor = Executors.newFixedThreadPool(2);
        var fileExecutor = Executors.newFixedThreadPool(2);
        try (var oxia = OxiaClientBuilder.create(System.getProperty("nereus.m5.nativeOffloadOxia"))
                        .namespace("default")
                        .requestTimeout(Duration.ofSeconds(10))
                        .asyncClient()
                        .get(30, TimeUnit.SECONDS);
                var independent = client(null);
                var actual = client(reads, heldGet, heldOutputPut, null);
                var metadata = metadata();
                var independentMetadata = metadata();
                var bk = bookKeeper()) {
            var namespace = OxiaPhysicalMetadataNamespaceV2.provision(oxia)
                    .toCompletableFuture()
                    .join();
            var selectorFault = new SelectorResponseFault(oxia);
            var conditional = new AsyncOxiaConditionalClient(selectorFault);
            var keys = new OxiaV2AuthorityKeys(root);
            var codecs = OxiaV2CodecSet.productionP1();
            var mutations = new ConditionalMutationEngine(conditional, new MutationFailureClassifier());
            var selectors = new OxiaPulsarTopicGenerationSelectorStore(
                    () -> {}, keys, codecs.selector(), conditional, mutations);
            var coordinator = new PulsarTopicAuthorityCoordinator(
                    new OxiaTopicBindingAggregatePublisher(() -> {}, keys, codecs.aggregate(), mutations),
                    new OxiaTopicBindingAggregateReader(() -> {}, keys, codecs.aggregate(), conditional),
                    selectors);
            var cell = new PulsarProtocolCellIdentity(
                    new DeploymentId(new Id128(1, 2)),
                    new ReservationDomainId(new Id128(3, 4)),
                    new PulsarCellId(new Id128(5, 6L + m4Mode)));
            var bindingId = DeterministicTopicIdsV1.deriveBindingId(cell, incarnation);
            var aggregate = new TopicBindingAggregateV1(
                    TopicBindingAggregateV1.SCHEMA_VERSION,
                    new TopicBindingV1(ProtocolKindV1.PULSAR, bindingId, cell, incarnation),
                    new InitialStorageEpochV1(
                            DeterministicTopicIdsV1.deriveStorageEpochId(bindingId, 0),
                            0,
                            StorageProfileV1.BOOKKEEPER_WAL_ASYNC_OBJECT,
                            ProfileOriginV1.TOPIC_EXPLICIT,
                            new PolicyCatalogDigest(Sha256Digest.hash(
                                    CanonicalBytes.copyOf("m5-native-real".getBytes(StandardCharsets.UTF_8)))),
                            FrameEncodingPolicyValueV1.none()));
            var bytes = Nta1CodecV1.encode(aggregate);
            PulsarNativeBindingV1 binding;
            com.nereusstream.metadata.oxia.v2.capability.PulsarActiveTopicAuthority active;
            if (m4Mode == 80 || m4Mode == 81 || bookKeeperLifecycle) {
                var policyCapability =
                        new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding(
                                1, bookKeeperCapability().configurationDigest());
                binding = PulsarNativeBindingV1.createNativeOnly(
                                selectorFault,
                                namespace.identity(),
                                root,
                                new AggregatePublicationCandidate(aggregate, bytes, Sha256Digest.hash(bytes)),
                                nativeExpectedWriterRegistry(policyCapability),
                                m4Mode == 81 || bookKeeperLifecycle
                                        ? com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityRecordsV1
                                                .PulsarRetentionPolicyV1.NATIVE_PULSAR_ONLY_WITHOUT_ACCESS_PROJECTION_V2
                                        : com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityRecordsV1
                                                .PulsarRetentionPolicyV1.NATIVE_PULSAR_ONLY_V1,
                                m4Mode == 81 || bookKeeperLifecycle
                                        ? Optional.of(
                                                com.nereusstream.storage.object.retention.M5PulsarAuditDomainV1.empty())
                                        : Optional.empty())
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS);
                active = binding.authority();
            } else {
                active = coordinator
                        .activate(new AggregatePublicationCandidate(aggregate, bytes, Sha256Digest.hash(bytes)))
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS);
                binding = PulsarNativeBindingV1.read(selectorFault, namespace.identity(), root, incarnation)
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS);
            }
            if (m4Mode == 65 || m4Mode == 66 || m4Mode == 67 || m4Mode == 79) {
                var p1Store = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(
                        selectorFault);
                var original = p1Store.read(keys.aggregateKey(incarnation))
                        .toCompletableFuture()
                        .join()
                        .orElseThrow();
                var p1Retirement =
                        new com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1(p1Store);
                var capability =
                        new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding(
                                1, bookKeeperCapability().configurationDigest());
                assertThat(p1Retirement
                                .migrateLegacy(new com.nereusstream.storage.object.retention
                                        .M5PulsarAggregateRetirementCoordinatorV1.MigrationRequest(
                                        original.key(), original, capability))
                                .toCompletableFuture()
                                .join())
                        .isEqualTo(
                                com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1
                                        .Outcome.APPLIED_EXACT);
                var envelope = p1Store.read(original.key())
                        .toCompletableFuture()
                        .join()
                        .orElseThrow();
                var value = com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                        envelope.canonicalStoredBytes());
                var next = com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.successor(
                        value, value.state(), Optional.empty(), List.of());
                assertThat(p1Store.compareAndSet(
                                        Optional.of(envelope),
                                        envelope.key(),
                                        com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1
                                                .encodeAuthority(next))
                                .toCompletableFuture()
                                .join())
                        .isEqualTo(
                                com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome
                                        .APPLIED_EXACT);
                binding.revalidate().toCompletableFuture().get(30, TimeUnit.SECONDS);
                var reread = new OxiaTopicBindingAggregateReader(() -> {}, keys, codecs.aggregate(), conditional)
                        .readAggregate(incarnation)
                        .toCompletableFuture()
                        .join()
                        .orElseThrow();
                assertThat(reread.metadataVersion())
                        .isNotEqualTo(active.aggregate().metadataVersion());
                assertThat(reread.aggregateSourceVersion())
                        .isEqualTo(active.aggregate().metadataVersion());
                System.out.println("NATIVE_PULSAR_P1_PROJECTION actualOxiaMigrationAndSuccessor=true "
                        + "originalNtaBytesAndVersion=true currentPhysicalCasVersion=true wholeM5Authority=false");
            }
            assertThat(binding.authority()).isEqualTo(active);
            independent.createBucket(
                    CreateBucketRequest.builder().bucket(bucket).build());
            independent.putBucketVersioning(PutBucketVersioningRequest.builder()
                    .bucket(bucket)
                    .versioningConfiguration(value -> value.status(BucketVersioningStatus.ENABLED))
                    .build());
            var objectNamespace = S3M5PhysicalNamespaceAuthorityV2.provision(independent, bucket, namespace.identity());
            if (m4Mode == 25 || m4Mode >= 51 && m4Mode <= 83) {
                // Explicit fixture provisioning precedes Task admission. The original Task may only reopen it.
                var objectQuota = OxiaPhysicalMetadataNamespaceV2.connect(selectorFault, namespace.identity())
                        .thenCompose(metadataAuthority -> metadataAuthority.openAuthorityRoute(
                                objectNamespace,
                                new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(
                                        selectorFault)))
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS);
                var objectLayout = com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Layout.withPhysicalMembers(
                        objectNamespace
                                .bind(namespace.identity())
                                .toCompletableFuture()
                                .join()
                                .authorityRoot(),
                        objectNamespace.namespace());
                objectQuota
                        .initialize(m4Mode == 58 ? objectLayout.headCharge() : 64L * 1024 * 1024)
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS);
            }
            var bkCapability = bookKeeperCapability();
            assertThat(Sha256Digest.hash(CanonicalBytes.copyOf(Files.readAllBytes(Path.of(BookKeeper.class
                            .getProtectionDomain()
                            .getCodeSource()
                            .getLocation()
                            .toURI())))))
                    .isEqualTo(bkCapability.clientArtifactSha256());
            try (var nativeBookKeeper = M5BookKeeperNamespaceAuthorityV2.connect(
                    "zk://" + System.getProperty("nereus.m5.nativeOffloadZk") + "/ledgers", bkCapability)) {
                var physicalBinding = nativeBookKeeper
                        .bind(namespace.identity())
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS);
                // The two real BK lifecycle targets share this exact permanent native epoch/intent budget.
                // Bootstrap precedes every guarded ledger reservation, including prerequisite SDK cases.
                var nativeDeleteQuota = nativeBookKeeper.nativeDeleteQuota();
                nativeDeleteQuota
                        .initialize(nativeDeleteQuota.capacityForResources(2))
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS);
                var nativeLayout = com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Layout.withPhysicalMembers(
                        physicalBinding.authorityRoot(), nativeBookKeeper.namespace());
                var originalLayout = NATIVE_BK_LAYOUT.compareAndExchange(null, nativeLayout);
                if (originalLayout != null) {
                    assertThat(nativeLayout).isEqualTo(originalLayout);
                }
                boolean firstNativeBkAdmission = m4Mode >= 25
                        && System.getProperty("nereus.m5.nativeM4AdmissionFile") != null
                        && !NATIVE_BK_INITIAL_OBSERVED.get();
                if (firstNativeBkAdmission
                        || m4Mode == 65
                        || m4Mode == 66
                        || m4Mode == 67
                        || m4Mode == 79
                        || m4Mode == 80
                        || m4Mode == 81
                        || bookKeeperLifecycle) {
                    OxiaPhysicalMetadataNamespaceV2.connect(selectorFault, namespace.identity())
                            .thenCompose(physical -> physical.openAuthorityRoute(
                                    nativeBookKeeper,
                                    new com.nereusstream.metadata.oxia.v2.retention
                                            .Oxia09ExactMetadataTransactionStoreV1(selectorFault)))
                            .thenCompose(route -> route.initialize(NATIVE_BK_CANONICAL_CAPACITY))
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS);
                    if (firstNativeBkAdmission) {
                        var initialQuota = observeActualQuota("bk-initial", nativeLayout, null);
                        assertThat(initialQuota.head().capacityBytes()).isEqualTo(NATIVE_BK_CANONICAL_CAPACITY);
                        assertThat(initialQuota.head().reservedResources()).isZero();
                        assertThat(initialQuota.head().settledResources()).isZero();
                        assertThat(NATIVE_BK_INITIAL_OBSERVED.compareAndSet(false, true))
                                .isTrue();
                    }
                }
                var providerConfiguration = m4Mode >= 49 && m4Mode <= 83
                        ? new S3PulsarOffloadObjectStoreV1.MaterializationConfiguration(
                                new com.nereusstream.storage.object.materialization.M5CellProviderProfileV2.References(
                                        "minio/security/nereus-m5-drain",
                                        "minio/credential/nereus-m5-drain",
                                        "NONE",
                                        "nereus/m5/native-cell-admission",
                                        "nereus/m5/owned-runner",
                                        "nereus/m5/version-match-delete-v1"),
                                actual.serviceClientConfiguration().credentialsProvider(),
                                m4Mode == 81 || bookKeeperLifecycle
                                        ? Optional.of(new com.nereusstream.storage.object.materialization
                                                .M5CellProviderProfileV2.GcGracePolicyV1(1, 1, 25))
                                        : Optional.empty())
                        : null;
                var store =
                        new S3PulsarOffloadObjectStoreV1(actual, bucket, objectExecutor, false, providerConfiguration);
                var offloader = bookKeeperLifecycle
                        ? offloader(store, fileExecutor, RetentionClass.DELETE_AFTER_VERIFIED)
                        : offloader(store, fileExecutor);
                if (bookKeeperLifecycle) {
                    verifyNativeBookKeeperLifecycle(
                            binding,
                            nativeBookKeeper,
                            offloader,
                            objectNamespace.namespace(),
                            selectorFault,
                            metadata,
                            m4Mode == 83);
                    return;
                }
                if (m4Mode == 80 || m4Mode == 81) {
                    verifyNativeOnlyFirstAdmission(
                            binding,
                            nativeBookKeeper,
                            offloader,
                            objectNamespace.namespace(),
                            selectorFault,
                            metadata,
                            m4Mode == 81);
                    return;
                }
                String name = incarnation.persistenceName().value().value();
                String nativePath = "/managed-ledgers/" + name;
                var holdPropertyAcknowledgement = new AtomicBoolean();
                var propertyWritten = new CountDownLatch(1);
                var propertyAcknowledgement = new CompletableFuture<Void>();
                var qualificationGet = new QualificationGetHold();
                var heldMetadata = new FaultInjectionMetadataStore(metadata) {
                    @Override
                    public CompletableFuture<Optional<org.apache.pulsar.metadata.api.GetResult>> get(String path) {
                        var original = super.get(path);
                        if (path.equals(nativePath) && qualificationGet.armed.compareAndSet(true, false)) {
                            return original.thenCompose(value -> {
                                qualificationGet.entered.countDown();
                                return qualificationGet.release.thenApply(ignored -> value);
                            });
                        }
                        return original;
                    }

                    @Override
                    public CompletableFuture<Stat> put(
                            String path, byte[] value, Optional<Long> expectedVersion, Set<Option> options) {
                        var actualWrite = super.put(path, value, expectedVersion, options);
                        if (path.equals(nativePath) && holdPropertyAcknowledgement.compareAndSet(true, false)) {
                            return actualWrite.thenCompose(stat -> {
                                propertyWritten.countDown();
                                return propertyAcknowledgement.thenApply(ignored -> stat);
                            });
                        }
                        return actualWrite;
                    }
                };
                var factory = factory(heldMetadata, nativeBookKeeper.borrowClient());
                ManagedLedgerImpl ledger = null;
                long sealedLedger = -1;
                var callbackFailureIssued = new AtomicBoolean();
                Throwable bindingFailure = null;
                try {
                    var config = config(offloader).setMaxEntriesPerLedger(3);
                    if (m4Mode == 25) {
                        config.setWriteQuorumSize(2); // Existing cross-quorum legacy source admission coverage.
                    }
                    config.setProperties(Map.of("user-property", "preserved"));
                    binding.configureCreate(config);
                    ledger = (ManagedLedgerImpl) factory.open(name, config);
                    var cursor = ledger.openCursor(SUBSCRIPTION, InitialPosition.Earliest);
                    var first = ledger.addEntry(payload(0));
                    sealedLedger = first.getLedgerId();
                    ledger.addEntry(payload(1));
                    ledger.addEntry(payload(2));
                    var plainSealed = ledger.addEntry(payload(3));
                    ledger.addEntry(payload(4));
                    ledger.addEntry(payload(5));
                    var activeTail = ledger.addEntry(payload(6));
                    assertThat(cursor.getNumberOfEntriesInBacklog(false)).isEqualTo(7);
                    ledger.offloadPrefix(plainSealed);
                    assertThat(ledger.getLedgersInfo()
                                    .get(sealedLedger)
                                    .getOffloadContext()
                                    .isComplete())
                            .isTrue();
                    var captured = binding.capture(
                                    ledger, sealedLedger, nativeBookKeeper, offloader, objectNamespace.namespace())
                            .get(30, TimeUnit.SECONDS);
                    assertThat(captured.binding().bindingIdentity()).isEqualTo(binding.bindingIdentity());
                    assertThat(captured.nativeSource().managedLedgerProperties())
                            .containsEntry("user-property", "preserved");
                    assertThat(captured.members()).hasSize(3);
                    assertThat(captured.members().stream().filter(PhysicalResourceIdV2.ObjectVersion.class::isInstance))
                            .hasSize(2)
                            .allSatisfy(
                                    member -> assertThat(member.namespace()).isEqualTo(objectNamespace.namespace()));
                    assertThat(captured.members())
                            .contains(new PhysicalResourceIdV2.BookKeeperLedger(
                                    nativeBookKeeper.namespace(), sealedLedger));
                    var bkFact = bk.getLedgerManager()
                            .readLedgerMetadata(sealedLedger)
                            .get(30, TimeUnit.SECONDS);
                    assertThat(captured.bookKeeperMetadataVersion())
                            .isEqualTo(((org.apache.bookkeeper.versioning.LongVersion) bkFact.getVersion())
                                    .getLongVersion());
                    assertThat(captured.bookKeeperBinding().metadataNamespace()).isEqualTo(namespace.identity());
                    var view = captured.readView();
                    assertThat(view.ledgerId()).isEqualTo(sealedLedger);
                    assertThat(view.bindingIdentity()).isEqualTo(binding.bindingIdentity());
                    assertThat(view.members()).isEqualTo(captured.members());
                    assertThat(view.lastAddConfirmed()).isEqualTo(2);
                    var repeated = binding.capture(
                                    ledger, sealedLedger, nativeBookKeeper, offloader, objectNamespace.namespace())
                            .get(30, TimeUnit.SECONDS);
                    assertThat(repeated.readView().canonicalBytes()).isEqualTo(view.canonicalBytes());
                    assertThat(repeated.readView().sha256()).isEqualTo(view.sha256());
                    int beforeBadRoute = reads.get();
                    long viewLedgerId = sealedLedger;
                    assertThatThrownBy(() -> view.readOwnedAsync(viewLedgerId + 1, 0, 0, OWNED_READ_LIMITS, () -> true)
                                    .join())
                            .hasRootCauseMessage("read differs from captured native ledger");
                    assertThatThrownBy(() -> view.readOwnedAsync(viewLedgerId, 0, 3, OWNED_READ_LIMITS, () -> true)
                                    .join())
                            .hasRootCauseMessage("captured range exceeds its admitted bounds");
                    assertThatThrownBy(() -> view.readOwnedAsync(viewLedgerId, 0, 0, OWNED_READ_LIMITS, () -> false)
                                    .join())
                            .hasRootCauseMessage("captured source use is closed");
                    assertThat(reads.get()).isEqualTo(beforeBadRoute);
                    var bindingView = binding.captureReadView(
                                    ledger, nativeBookKeeper, offloader, objectNamespace.namespace())
                            .get(30, TimeUnit.SECONDS);
                    assertThat(bindingView.ledgerIds())
                            .containsExactly(sealedLedger, plainSealed.getLedgerId(), activeTail.getLedgerId());
                    assertThat(bindingView.members()).hasSize(5);
                    assertThat(bindingView.confirmedPosition()).isEqualTo(activeTail);
                    if (m4Mode >= 68 && m4Mode <= 78) {
                        var trimChain = bindingView.materializationChain();
                        var trimCoverage = com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.of(
                                binding.bindingIdentity(),
                                bindingView.sha256(),
                                trimChain,
                                trimChain.stream()
                                        .filter(value -> !value.active() && value.entryEndExclusive() > 0)
                                        .map(value ->
                                                new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2
                                                        .EntryRange(value.ledgerId(), 0, value.entryEndExclusive()))
                                        .toList());
                        closeNativeLedger(ledger).get(30, TimeUnit.SECONDS);
                        verifyUnfenceableSdkReplyLoss(
                                binding,
                                nativeBookKeeper,
                                offloader,
                                objectNamespace.namespace(),
                                selectorFault,
                                m4Mode,
                                trimCoverage);
                        return;
                    }
                    if (m4Mode == 46) {
                        verifyMaterializationConnectionDrain(
                                binding, captured, offloader, store, objectNamespace.namespace(), heldGet);
                        return;
                    }
                    if (m4Mode == 43 || m4Mode == 44 || m4Mode == 45 || m4Mode >= 47 && m4Mode <= 67 || m4Mode == 79) {
                        verifyNativeMaterializationSource(
                                binding,
                                bindingView,
                                ledger,
                                nativeBookKeeper,
                                offloader,
                                objectNamespace.namespace(),
                                sealedLedger,
                                plainSealed.getLedgerId(),
                                activeTail.getLedgerId(),
                                independentMetadata,
                                nativePath,
                                m4Mode == 44,
                                m4Mode == 45 ? qualificationGet : null,
                                m4Mode == 47,
                                m4Mode == 48 ? selectorFault : null,
                                m4Mode >= 49 ? selectorFault : null,
                                m4Mode == 50,
                                m4Mode >= 51 ? m4Mode : 0,
                                bucket,
                                heldOutputPut,
                                heldGet);
                        return;
                    }
                    if (m4Mode == 25 || m4Mode == 26 || m4Mode >= 34 && m4Mode <= 42) {
                        closeNativeLedger(ledger).get(30, TimeUnit.SECONDS);
                        verifyNativeGuardedBirths(
                                binding,
                                nativeBookKeeper,
                                objectNamespace.namespace(),
                                bucket,
                                name,
                                objectExecutor,
                                fileExecutor,
                                reads,
                                selectorFault,
                                m4Mode == 26,
                                m4Mode >= 34 ? m4Mode : 0);
                        return;
                    }
                    if (m4Mode >= 21 && m4Mode <= 23 || m4Mode >= 27 && m4Mode <= 33) {
                        closeNativeLedger(ledger).get(30, TimeUnit.SECONDS);
                        verifyNativeReservationInventory(
                                binding,
                                nativeBookKeeper,
                                objectNamespace.namespace(),
                                bucket,
                                name,
                                objectExecutor,
                                fileExecutor,
                                reads,
                                selectorFault,
                                m4Mode);
                        return;
                    }
                    if (m4Mode == 19 || m4Mode == 20) {
                        closeNativeLedger(ledger).get(30, TimeUnit.SECONDS);
                        verifyNativeCellAllocator(
                                binding,
                                nativeBookKeeper,
                                objectNamespace.namespace(),
                                bucket,
                                name,
                                root,
                                incarnation,
                                objectExecutor,
                                fileExecutor,
                                reads,
                                selectorFault,
                                m4Mode == 20);
                        return;
                    }
                    if (m4Mode >= 16 && m4Mode <= 18) {
                        verifyNativeOwnedVerification(
                                binding,
                                bindingView,
                                ledger,
                                nativeBookKeeper,
                                offloader,
                                objectNamespace.namespace(),
                                independent,
                                bucket,
                                sealedLedger,
                                plainSealed.getLedgerId(),
                                heldGet,
                                reads,
                                m4Mode);
                        return;
                    }
                    if (m4Mode == 14 || m4Mode == 15) {
                        closeNativeLedger(ledger).get(30, TimeUnit.SECONDS);
                        verifyNativeSourcePreparation(
                                binding,
                                nativeBookKeeper,
                                objectNamespace.namespace(),
                                bucket,
                                name,
                                sealedLedger,
                                objectExecutor,
                                fileExecutor,
                                reads,
                                m4Mode == 14);
                        return;
                    }
                    if (m4Mode == 13) {
                        verifyNativeCellShares(
                                binding,
                                bindingView,
                                ledger,
                                nativeBookKeeper,
                                offloader,
                                objectNamespace.namespace(),
                                factory,
                                coordinator,
                                selectorFault,
                                root,
                                sealedLedger,
                                activeTail.getLedgerId(),
                                heldGet,
                                reads);
                        return;
                    }
                    if (m4Mode >= 7 && m4Mode <= 12 || m4Mode == 24) {
                        verifyNativeM4Retirement(
                                binding,
                                bindingView,
                                ledger,
                                nativeBookKeeper,
                                offloader,
                                objectNamespace.namespace(),
                                bucket,
                                name,
                                sealedLedger,
                                activeTail.getLedgerId(),
                                objectExecutor,
                                fileExecutor,
                                reads,
                                heldGet,
                                selectorFault,
                                oxia,
                                m4Mode);
                        return;
                    }
                    if (m4Mode == 5 || m4Mode == 6) {
                        ledger.close();
                        if (m4Mode == 5) {
                            // Controlled SDK absence fixture, not an M5-authorized physical-delete receipt.
                            nativeBookKeeper.borrowClient().deleteLedger(sealedLedger);
                            var missing = nativeBookKeeper
                                    .borrowClient()
                                    .getLedgerManager()
                                    .readLedgerMetadata(sealedLedger);
                            assertThatThrownBy(missing::join)
                                    .isInstanceOf(java.util.concurrent.CompletionException.class);
                        }
                        verifyPreferredOnlySource(
                                binding,
                                nativeBookKeeper,
                                objectNamespace.namespace(),
                                bucket,
                                name,
                                sealedLedger,
                                plainSealed.getLedgerId(),
                                activeTail.getLedgerId(),
                                objectExecutor,
                                fileExecutor,
                                reads,
                                heldGet,
                                m4Mode);
                        return;
                    }
                    if (m4Mode == 1 || m4Mode == 2 || m4Mode == 4) {
                        verifyNativeM4Owner(
                                binding,
                                bindingView,
                                ledger,
                                nativeBookKeeper,
                                offloader,
                                sealedLedger,
                                plainSealed.getLedgerId(),
                                activeTail.getLedgerId(),
                                heldGet,
                                reads,
                                m4Mode,
                                callbackFailureIssued);
                        return;
                    }
                    String runtimeOutput = System.getProperty("nereus.m5.nativeM4RuntimeOutput");
                    if (runtimeOutput != null) {
                        var runtime = new Properties();
                        PulsarNativeM4DrainAdmissionV1.runtimeFingerprint()
                                .forEach((key, value) -> runtime.setProperty("runtime." + key, value));
                        runtime.setProperty(
                                "configuration",
                                bindingView
                                        .readAdmissionConfiguration(nativeBookKeeper, offloader)
                                        .toHex());
                        try (var output = Files.newOutputStream(Path.of(runtimeOutput))) {
                            runtime.store(output, "Actual current Native runtime and backend configuration");
                        }
                    }
                    var m4 = binding.m4Control();
                    int beforeMissingM4 = reads.get();
                    assertThat(m4.readFacts(bindingView).get(30, TimeUnit.SECONDS))
                            .isEmpty();
                    assertThat(conditional
                                    .read(m4.selectorKey())
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS))
                            .isEmpty();
                    assertThat(conditional
                                    .read(m4.capabilityKey(1))
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS))
                            .isEmpty();
                    assertThat(reads.get()).isEqualTo(beforeMissingM4);
                    assertThat(bindingView
                                    .readOwnedAsync(sealedLedger, 0, 0, OWNED_READ_LIMITS, () -> true)
                                    .get(30, TimeUnit.SECONDS)
                                    .entries()
                                    .get(0)
                                    .payload())
                            .containsExactly(payload(0));
                    assertThat(bindingView
                                    .readOwnedAsync(plainSealed.getLedgerId(), 0, 0, OWNED_READ_LIMITS, () -> true)
                                    .get(30, TimeUnit.SECONDS)
                                    .entries()
                                    .get(0)
                                    .payload())
                            .containsExactly(payload(3));
                    assertThat(bindingView
                                    .readOwnedAsync(activeTail.getLedgerId(), 0, 0, OWNED_READ_LIMITS, () -> true)
                                    .get(30, TimeUnit.SECONDS)
                                    .entries()
                                    .get(0)
                                    .payload())
                            .containsExactly(payload(6));
                    var advancedTail = ledger.addEntry(payload(7));
                    assertThat(advancedTail.getLedgerId()).isEqualTo(activeTail.getLedgerId());
                    assertThatThrownBy(() -> bindingView
                                    .readOwnedAsync(activeTail.getLedgerId(), 1, 1, OWNED_READ_LIMITS, () -> true)
                                    .join())
                            .hasRootCauseMessage("captured range exceeds its admitted bounds");
                    assertThat(bindingView
                                    .readOwnedAsync(activeTail.getLedgerId(), 0, 0, OWNED_READ_LIMITS, () -> true)
                                    .get(30, TimeUnit.SECONDS)
                                    .entries()
                                    .get(0)
                                    .payload())
                            .containsExactly(payload(6));
                    var advancedView = binding.captureReadView(
                                    ledger, nativeBookKeeper, offloader, objectNamespace.namespace())
                            .get(30, TimeUnit.SECONDS);
                    assertThat(advancedView.confirmedPosition()).isEqualTo(advancedTail);
                    assertThat(advancedView.sha256()).isNotEqualTo(bindingView.sha256());
                    assertThat(m4.readFacts(advancedView).get(30, TimeUnit.SECONDS))
                            .isEmpty();
                    assertThat(advancedView
                                    .readOwnedAsync(advancedTail.getLedgerId(), 1, 1, OWNED_READ_LIMITS, () -> true)
                                    .get(30, TimeUnit.SECONDS)
                                    .entries()
                                    .get(0)
                                    .payload())
                            .containsExactly(payload(7));
                    var tailHeader = bk.getLedgerManager()
                            .readLedgerMetadata(activeTail.getLedgerId())
                            .get(30, TimeUnit.SECONDS);
                    var recoveringTail = bk.getLedgerManager()
                            .writeLedgerMetadata(
                                    activeTail.getLedgerId(),
                                    LedgerMetadataBuilder.from(tailHeader.getValue())
                                            .withInRecoveryState()
                                            .build(),
                                    tailHeader.getVersion())
                            .get(30, TimeUnit.SECONDS);
                    int beforeRecoveringTail = reads.get();
                    var actualOwner = ledger;
                    assertThatThrownBy(() -> binding.captureReadView(
                                            actualOwner, nativeBookKeeper, offloader, objectNamespace.namespace())
                                    .join())
                            .hasRootCauseMessage("native BookKeeper metadata differs from sealed source");
                    assertThat(reads.get()).isEqualTo(beforeRecoveringTail);
                    bk.getLedgerManager()
                            .writeLedgerMetadata(
                                    activeTail.getLedgerId(), tailHeader.getValue(), recoveringTail.getVersion())
                            .get(30, TimeUnit.SECONDS);
                    var changedHeader =
                            new java.util.HashMap<>(bkFact.getValue().getCustomMetadata());
                    changedHeader.put(
                            LedgerMetadataUtils.METADATA_PROPERTY_MANAGED_LEDGER_NAME,
                            "tenant/ns/persistent/foreign-owner".getBytes(StandardCharsets.UTF_8));
                    var foreignHeader = bk.getLedgerManager()
                            .writeLedgerMetadata(
                                    sealedLedger,
                                    LedgerMetadataBuilder.from(bkFact.getValue())
                                            .withCustomMetadata(changedHeader)
                                            .build(),
                                    bkFact.getVersion())
                            .get(30, TimeUnit.SECONDS);
                    var headerOwner = ledger;
                    long headerSourceId = sealedLedger;
                    int beforeForeignHeader = reads.get();
                    assertThat(captured.nativeSource().isCurrent()).isTrue();
                    assertThatThrownBy(() -> binding.capture(
                                            headerOwner,
                                            headerSourceId,
                                            nativeBookKeeper,
                                            offloader,
                                            objectNamespace.namespace())
                                    .join())
                            .hasRootCauseMessage("native BookKeeper metadata differs from sealed source");
                    assertThat(reads.get()).isEqualTo(beforeForeignHeader);
                    var restoredHeader = bk.getLedgerManager()
                            .writeLedgerMetadata(sealedLedger, bkFact.getValue(), foreignHeader.getVersion())
                            .get(30, TimeUnit.SECONDS);
                    var cursorHeader = new java.util.HashMap<>(bkFact.getValue().getCustomMetadata());
                    cursorHeader.put(
                            LedgerMetadataUtils.METADATA_PROPERTY_CURSOR_NAME,
                            "foreign-cursor".getBytes(StandardCharsets.UTF_8));
                    var wrongRole = bk.getLedgerManager()
                            .writeLedgerMetadata(
                                    sealedLedger,
                                    LedgerMetadataBuilder.from(bkFact.getValue())
                                            .withCustomMetadata(cursorHeader)
                                            .build(),
                                    restoredHeader.getVersion())
                            .get(30, TimeUnit.SECONDS);
                    assertThatThrownBy(() -> binding.capture(
                                            headerOwner,
                                            headerSourceId,
                                            nativeBookKeeper,
                                            offloader,
                                            objectNamespace.namespace())
                                    .join())
                            .hasRootCauseMessage("native BookKeeper metadata differs from sealed source");
                    assertThat(reads.get()).isEqualTo(beforeForeignHeader);
                    bk.getLedgerManager()
                            .writeLedgerMetadata(sealedLedger, bkFact.getValue(), wrongRole.getVersion())
                            .get(30, TimeUnit.SECONDS);
                    ledger.reopenReadHandle(plainSealed.getLedgerId()).get(30, TimeUnit.SECONDS);
                    assertThat(bindingView.isCurrent()).isFalse();
                    int beforeStaleM4 = reads.get();
                    assertThatThrownBy(() -> m4.readFacts(bindingView).join())
                            .hasRootCauseMessage("native M4 capture requires a current Native view");
                    assertThatThrownBy(() -> bindingView
                                    .readOwnedAsync(plainSealed.getLedgerId(), 0, 0, OWNED_READ_LIMITS, () -> true)
                                    .join())
                            .hasRootCauseMessage("captured plain BookKeeper source is stale");
                    assertThat(reads.get()).isEqualTo(beforeStaleM4);
                    var newHeaderVersion = binding.capture(
                                    ledger, sealedLedger, nativeBookKeeper, offloader, objectNamespace.namespace())
                            .get(30, TimeUnit.SECONDS);
                    assertThat(newHeaderVersion.objects()).isEqualTo(captured.objects());
                    assertThat(newHeaderVersion.members()).isEqualTo(captured.members());
                    assertThat(newHeaderVersion.readView().sha256()).isNotEqualTo(view.sha256());
                    assertThat(view.sha256()).isEqualTo(Sha256Digest.hash(view.canonicalBytes()));
                    assertThat(captured.objects().ledgerId()).isEqualTo(sealedLedger);
                    var nativeFact = independentMetadata
                            .get(nativePath)
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow();
                    assertThat(captured.nativeSource().sourceState().metadataVersion())
                            .isEqualTo(nativeFact.getStat().getVersion());
                    var persisted = ((org.apache.bookkeeper.mledger.impl.MetaStoreImpl) factory.getMetaStore())
                            .parseManagedLedgerInfo(nativeFact.getValue());
                    var actualProperties = new java.util.HashMap<String, String>();
                    persisted
                            .getPropertiesList()
                            .forEach(property -> actualProperties.put(property.getKey(), property.getValue()));
                    assertThat(captured.nativeSource().managedLedgerProperties())
                            .containsExactlyInAnyOrderEntriesOf(actualProperties);
                    var actualLedgerInfo = persisted.getLedgerInfosList().stream()
                            .filter(info -> info.getLedgerId() == headerSourceId)
                            .findFirst()
                            .orElseThrow()
                            .toByteArray();
                    var capturedLedgerInfo = captured.nativeSource().sealedLedgerInfoBytes();
                    assertThat(capturedLedgerInfo).containsExactly(actualLedgerInfo);
                    capturedLedgerInfo[0] ^= 1;
                    assertThat(captured.nativeSource().sealedLedgerInfoBytes()).containsExactly(actualLedgerInfo);
                    assertThatThrownBy(() -> captured.nativeSource()
                                    .managedLedgerProperties()
                                    .put("user-property", "changed"))
                            .isInstanceOf(UnsupportedOperationException.class);
                    var owned = view.readOwnedAsync(sealedLedger, 0, 0, OWNED_READ_LIMITS, () -> true)
                            .get(30, TimeUnit.SECONDS);
                    assertThat(owned.entries().get(0).payload()).containsExactly(payload(0));

                    var updatingProperty = new CompletableFuture<Void>();
                    holdPropertyAcknowledgement.set(true);
                    ledger.asyncSetProperty(
                            "user-property",
                            "preserved",
                            new UpdatePropertiesCallback() {
                                @Override
                                public void updatePropertiesComplete(Map<String, String> properties, Object ctx) {
                                    updatingProperty.complete(null);
                                }

                                @Override
                                public void updatePropertiesFailed(ManagedLedgerException exception, Object ctx) {
                                    updatingProperty.completeExceptionally(exception);
                                }
                            },
                            null);
                    assertThat(propertyWritten.await(30, TimeUnit.SECONDS)).isTrue();
                    var committedProperty = independentMetadata
                            .get(nativePath)
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow();
                    assertThat(committedProperty.getStat().getVersion())
                            .isGreaterThan(captured.nativeSource().sourceState().metadataVersion());
                    assertThat(updatingProperty).isNotDone();
                    assertThat(captured.nativeSource().isCurrent()).isTrue();
                    var heldOwner = ledger;
                    long heldSourceId = sealedLedger;
                    int beforeUnacknowledged = reads.get();
                    assertThatThrownBy(() -> binding.capture(
                                            heldOwner,
                                            heldSourceId,
                                            nativeBookKeeper,
                                            offloader,
                                            objectNamespace.namespace())
                                    .join())
                            .hasRootCauseMessage("captured native metadata differs from stored source");
                    assertThat(reads.get()).isEqualTo(beforeUnacknowledged);
                    propertyAcknowledgement.complete(null);
                    updatingProperty.get(30, TimeUnit.SECONDS);
                    assertThat(captured.nativeSource().isCurrent()).isFalse();
                    assertThat(view.canonicalBytes())
                            .isEqualTo(repeated.readView().canonicalBytes());
                    int beforeStaleView = reads.get();
                    assertThatThrownBy(() -> view.readOwnedAsync(heldSourceId, 0, 0, OWNED_READ_LIMITS, () -> true)
                                    .join())
                            .hasRootCauseMessage("captured native source is stale");
                    assertThat(reads.get()).isEqualTo(beforeStaleView);

                    String identityKey = actualProperties.keySet().stream()
                            .filter(key -> key.endsWith("bindingId"))
                            .findFirst()
                            .orElseThrow();
                    ledger.setProperty(identityKey, "foreign-binding");
                    int before = reads.get();
                    var owner = ledger;
                    long sourceId = sealedLedger;
                    assertThatThrownBy(() -> binding.capture(
                                            owner, sourceId, nativeBookKeeper, offloader, objectNamespace.namespace())
                                    .join())
                            .hasRootCauseMessage("native source lacks its exact persisted P1 Binding");
                    assertThat(reads.get()).isEqualTo(before);
                    ledger.setProperty(identityKey, actualProperties.get(identityKey));
                    closeNativeLedger(ledger).get(30, TimeUnit.SECONDS);
                    ledger = (ManagedLedgerImpl) factory.open(name, config(offloader));
                    var recovered = binding.capture(
                                    ledger, sealedLedger, nativeBookKeeper, offloader, objectNamespace.namespace())
                            .get(30, TimeUnit.SECONDS);
                    var recoveredBindingView = binding.captureReadView(
                                    ledger, nativeBookKeeper, offloader, objectNamespace.namespace())
                            .get(30, TimeUnit.SECONDS);
                    assertThat(m4.readFacts(recoveredBindingView).get(30, TimeUnit.SECONDS))
                            .isEmpty();
                    conditional
                            .createIfAbsent(m4.selectorKey(), CanonicalBytes.copyOf(new byte[] {0}))
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS);
                    assertThatThrownBy(() -> m4.readFacts(recoveredBindingView).join())
                            .hasCauseInstanceOf(IllegalArgumentException.class);
                    assertThat(recoveredBindingView.ledgerIds()).containsAll(bindingView.ledgerIds());
                    assertThat(recoveredBindingView
                                    .readOwnedAsync(advancedTail.getLedgerId(), 1, 1, OWNED_READ_LIMITS, () -> true)
                                    .get(30, TimeUnit.SECONDS)
                                    .entries()
                                    .get(0)
                                    .payload())
                            .containsExactly(payload(7));
                    assertThat(recovered.nativeSource().managedLedgerProperties())
                            .containsExactlyInAnyOrderEntriesOf(actualProperties);
                    assertThat(recovered.objects()).isEqualTo(captured.objects());
                    assertThat(recovered.readView().sha256()).isNotEqualTo(view.sha256());
                    var read = recovered
                            .readView()
                            .readOwnedAsync(sealedLedger, 0, 0, OWNED_READ_LIMITS, () -> true)
                            .get(30, TimeUnit.SECONDS);
                    assertThat(read.entries().get(0).payload()).containsExactly(payload(0));
                    String foreignBucket = bucket + "-other";
                    independent.createBucket(
                            CreateBucketRequest.builder().bucket(foreignBucket).build());
                    independent.putBucketVersioning(PutBucketVersioningRequest.builder()
                            .bucket(foreignBucket)
                            .versioningConfiguration(value -> value.status(BucketVersioningStatus.ENABLED))
                            .build());
                    var foreignNamespace = S3M5PhysicalNamespaceAuthorityV2.provision(
                            independent, foreignBucket, namespace.identity());
                    assertThatThrownBy(() -> offloader
                                    .observePhysicalNamespace(foreignNamespace.namespace(), namespace.identity())
                                    .join())
                            .hasRootCauseMessage("S3 physical namespace incarnation differs");
                    var otherProvider = offloader(store, fileExecutor);
                    try {
                        var recoveredOwner = ledger;
                        int beforeForeign = reads.get();
                        assertThatThrownBy(() -> binding.capture(
                                                recoveredOwner,
                                                sourceId,
                                                nativeBookKeeper,
                                                otherProvider,
                                                objectNamespace.namespace())
                                        .join())
                                .hasRootCauseMessage("native source lacks its exact persisted P1 Binding");
                        assertThat(reads.get()).isEqualTo(beforeForeign);
                    } finally {
                        otherProvider.close();
                    }
                    try (var otherBookKeeper = M5BookKeeperNamespaceAuthorityV2.connect(
                            "zk://" + System.getProperty("nereus.m5.nativeOffloadZk") + "/ledgers",
                            bookKeeperCapability())) {
                        assertThat(otherBookKeeper.namespace()).isEqualTo(nativeBookKeeper.namespace());
                        var recoveredOwner = ledger;
                        int beforeForeignClient = reads.get();
                        assertThatThrownBy(() -> binding.capture(
                                                recoveredOwner,
                                                sourceId,
                                                otherBookKeeper,
                                                offloader,
                                                objectNamespace.namespace())
                                        .join())
                                .hasRootCauseMessage("native source lacks its exact persisted P1 Binding");
                        assertThat(reads.get()).isEqualTo(beforeForeignClient);
                    }
                    if (m4Mode != 3) {
                        var zk = ((org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver)
                                        nativeBookKeeper.borrowClient().getMetadataClientDriver())
                                .getZk();
                        String gatePath = "/ledgers/nereus-m5-native-v2/authority-namespace";
                        var gateStat = new org.apache.zookeeper.data.Stat();
                        byte[] gateBytes = zk.getData(gatePath, false, gateStat);
                        zk.setData(gatePath, gateBytes, gateStat.getVersion());
                        var gateOwner = ledger;
                        int beforeGateRewrite = reads.get();
                        assertThatThrownBy(() -> binding.capture(
                                                gateOwner,
                                                sourceId,
                                                nativeBookKeeper,
                                                offloader,
                                                objectNamespace.namespace())
                                        .join())
                                .hasRootCauseMessage(
                                        "native physical namespace gate is not permanent at its exact version");
                        assertThat(reads.get()).isEqualTo(beforeGateRewrite);
                        var deleting = coordinator
                                .beginDeletion(incarnation.persistenceName())
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS);
                        assertThatThrownBy(() -> binding.revalidate()
                                        .toCompletableFuture()
                                        .join())
                                .hasRootCauseMessage("active authority requires an ACTIVE selector");
                        assertThat(deleting).isNotEqualTo(active.selector());
                        var marker = conditional
                                .read(OxiaPhysicalMetadataNamespaceV2.MARKER_KEY)
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS)
                                .orElseThrow();
                        conditional
                                .compareAndSet(marker.key(), marker.storedBytes(), marker.versionId())
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS);
                        assertThatThrownBy(() -> binding.revalidate()
                                        .toCompletableFuture()
                                        .join())
                                .hasRootCauseMessage(
                                        "actual metadata backend namespace differs from the physical binding");
                        recoveredBindingView.fenceOwnerReadAdmission().get(30, TimeUnit.SECONDS);
                        int beforeFencedBinding = reads.get();
                        assertThatThrownBy(() -> recoveredBindingView
                                        .readOwnedAsync(advancedTail.getLedgerId(), 1, 1, OWNED_READ_LIMITS, () -> true)
                                        .join())
                                .hasRootCauseMessage("captured plain BookKeeper source is stale");
                        assertThat(reads.get()).isEqualTo(beforeFencedBinding);
                    }
                    System.out.println("NATIVE_PULSAR_BINDING_SOURCE ledger=" + sealedLedger
                            + " binding=" + bindingId.digest().toHex()
                            + " metadataNamespace="
                            + namespace.identity().instanceId().toHex()
                            + " nativeVersion="
                            + recovered.nativeSource().sourceState().metadataVersion()
                            + " physicalMembers=" + recovered.members().size()
                            + " nativeReadViewSha256="
                            + recovered.readView().sha256().toHex()
                            + " exactNativeReadView=true badNativeRouteBeforeIo=true sameBytesBkVersionChangesView=true"
                            + " completeNativeDataView=true sealedPlainBk=true activeTail=true capturedFrontier=true"
                            + " coldProperties=true deletingRejected=" + (m4Mode != 3)
                            + " foreignProviderRejected=true"
                            + " metadataMarkerRewriteRejected=" + (m4Mode != 3));
                } catch (Exception | Error failure) {
                    bindingFailure = failure;
                    try {
                        System.out.println("NATIVE_BINDING_FAILED_QUOTA mode=" + m4Mode + " "
                                + binding.materializationQuota(nativeBookKeeper)
                                        .thenCompose(route -> route.quota().describeUsage())
                                        .get(30, TimeUnit.SECONDS));
                    } catch (Exception diagnosticFailure) {
                        failure.addSuppressed(diagnosticFailure);
                    }
                    throw failure;
                } finally {
                    propertyAcknowledgement.complete(null);
                    try {
                        try {
                            if (ledger != null) {
                                if (callbackFailureIssued.get()) {
                                    ManagedLedgerImpl failedOwner = ledger;
                                    assertThatThrownBy(() -> closeNativeLedger(failedOwner)
                                                    .get(30, TimeUnit.SECONDS))
                                            .hasRootCauseMessage("actual Native M4 callback failed");
                                } else {
                                    closeNativeLedger(ledger).get(30, TimeUnit.SECONDS);
                                }
                            }
                        } finally {
                            factory.shutdown();
                            if (m4Mode == 70) {
                                // The unresolved reset owns its original workspace, including this provider connection.
                                assertThat(offloader.closeAsync()).isNotDone();
                                assertThat(store.close().toCompletableFuture()).isNotDone();
                            } else {
                                offloader.closeAsync().get(30, TimeUnit.SECONDS);
                                store.close().toCompletableFuture().get(30, TimeUnit.SECONDS);
                            }
                        }
                    } catch (Exception | Error cleanupFailure) {
                        if (bindingFailure == null) {
                            throw cleanupFailure;
                        } else {
                            bindingFailure.addSuppressed(cleanupFailure);
                        }
                    }
                }
            }
        } finally {
            fileExecutor.shutdown();
            objectExecutor.shutdown();
            assertThat(fileExecutor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            assertThat(objectExecutor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    private void verifyNativeMaterializationSource(
            PulsarNativeBindingV1 binding,
            PulsarNativeBindingReadViewV1 diagnostic,
            ManagedLedgerImpl ledger,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace objectNamespace,
            long offloaded,
            long plain,
            long tail,
            MetadataStoreExtended independentMetadata,
            String nativePath,
            boolean closeFromDelivery,
            QualificationGetHold heldQualification,
            boolean verifyPersistentInput,
            SelectorResponseFault inputFault,
            SelectorResponseFault providerFault,
            boolean providerUnknown,
            int taskMode,
            String bucket,
            AtomicReference<SdkHold> heldOutputPut,
            AtomicReference<SdkHold> heldMappedGet)
            throws Exception {
        if (taskMode < 65) {
            verifyNativeMaterializationSourceOnOwner(
                    binding,
                    diagnostic,
                    ledger,
                    bookKeeper,
                    offloader,
                    objectNamespace,
                    offloaded,
                    plain,
                    tail,
                    independentMetadata,
                    nativePath,
                    closeFromDelivery,
                    heldQualification,
                    verifyPersistentInput,
                    inputFault,
                    providerFault,
                    providerUnknown,
                    taskMode,
                    bucket,
                    heldOutputPut,
                    heldMappedGet);
            return;
        }
        // Diagnostics above intentionally opened fallback composites. An Object-only successor must use a real
        // fresh SDK owner after original SDK/handle termination, never flip the cached composite's source flags.
        closeNativeLedger(ledger).get(30, TimeUnit.SECONDS);
        try (var coldMetadata = metadata()) {
            var coldFactory = factory(coldMetadata, bookKeeper.borrowClient());
            ManagedLedgerImpl cold = null;
            try {
                var checkpointConfig = config(offloader);
                if (taskMode == 65) {
                    checkpointConfig.setMaxUnackedRangesToPersistInMetadataStore(0);
                }
                cold = (ManagedLedgerImpl) coldFactory.open(ledger.getName(), checkpointConfig);
                if (taskMode == 65) {
                    var checkpointCursor = cold.openCursor("checkpoint-observation", InitialPosition.Earliest);
                    checkpointCursor.markDelete(PositionFactory.create(offloaded, 0));
                    var retainedCheckpoint = cold.openCursor("sealed-checkpoint-import", InitialPosition.Earliest);
                    retainedCheckpoint.markDelete(PositionFactory.create(offloaded, 0));
                    retainedCheckpoint.delete(PositionFactory.create(offloaded, 2));
                    assertThat(((org.apache.bookkeeper.mledger.impl.ManagedCursorImpl) checkpointCursor)
                                    .getCursorLedger())
                            .isNotNegative();
                }
                verifyNativeMaterializationSourceOnOwner(
                        binding,
                        diagnostic,
                        cold,
                        bookKeeper,
                        offloader,
                        objectNamespace,
                        offloaded,
                        plain,
                        tail,
                        independentMetadata,
                        nativePath,
                        closeFromDelivery,
                        heldQualification,
                        verifyPersistentInput,
                        inputFault,
                        providerFault,
                        providerUnknown,
                        taskMode,
                        bucket,
                        heldOutputPut,
                        heldMappedGet);
            } finally {
                try {
                    if (cold != null) {
                        closeNativeLedger(cold).get(30, TimeUnit.SECONDS);
                    }
                } finally {
                    coldFactory.shutdown();
                }
            }
        }
    }

    private void verifyNativeMaterializationSourceOnOwner(
            PulsarNativeBindingV1 binding,
            PulsarNativeBindingReadViewV1 diagnostic,
            ManagedLedgerImpl ledger,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace objectNamespace,
            long offloaded,
            long plain,
            long tail,
            MetadataStoreExtended independentMetadata,
            String nativePath,
            boolean closeFromDelivery,
            QualificationGetHold heldQualification,
            boolean verifyPersistentInput,
            SelectorResponseFault inputFault,
            SelectorResponseFault providerFault,
            boolean providerUnknown,
            int taskMode,
            String bucket,
            AtomicReference<SdkHold> heldOutputPut,
            AtomicReference<SdkHold> heldMappedGet)
            throws Exception {
        var capsule = Path.of(System.getProperty("nereus.m5.nativeM4AdmissionFile"));
        var expected = Sha256Digest.copyOf(
                java.util.HexFormat.of().parseHex(System.getProperty("nereus.m5.nativeM4AdmissionSha256")));
        var admission = PulsarNativeM4DrainAdmissionV1.load(capsule, expected);
        var control = binding.m4Control();
        var budget =
                readBudget(binding, taskMode == 65 || taskMode == 79 ? 6 : taskMode == 66 || taskMode == 67 ? 4 : 3);
        var view = preparedView(binding, ledger, bookKeeper, offloader, objectNamespace, budget, taskMode >= 65);
        var event = Executors.newSingleThreadExecutor();
        var executor = Executors.newSingleThreadExecutor();
        PulsarNativeM4ReadOwnerV1 owner = null;
        PulsarNativeMaterializationSourceV2 source = null;
        Throwable sourceFailure = null;
        try {
            if (taskMode == 65 || taskMode == 66 || taskMode == 67 || taskMode == 79) {
                var exact = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(
                        providerFault);
                String key = new OxiaV2AuthorityKeys(binding.authorityRoot())
                        .aggregateKey(binding.authority().aggregate().binding().incarnationIdentity());
                var authority =
                        new com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1(exact);
                var capability =
                        new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding(
                                1, bookKeeper.capability().configurationDigest());
                var registry = nativeExpectedWriterRegistry(capability);
                var predecessor = authority
                        .read(key)
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS)
                        .orElseThrow();
                assertThat(authority
                                .enrollWriters(new com.nereusstream.storage.object.retention
                                        .M5PulsarAggregateRetirementCoordinatorV1.EnrollmentRequest(
                                        key, predecessor, registry.enrollment()))
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS))
                        .isEqualTo(
                                com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1
                                        .Outcome.APPLIED_EXACT);
                var beforeRejectedRegistry = budget.usage();
                var originalAuthority =
                        authority.read(key).toCompletableFuture().get(30, TimeUnit.SECONDS);
                var invalidRegistry = registryWithMisassignedFallback(registry);
                assertThatThrownBy(() -> control.installControlWriter(invalidRegistry, authority, key, budget)
                                .get(30, TimeUnit.SECONDS))
                        .hasRootCauseMessage("Native writer registry assigns a class to a different production owner");
                assertThat(budget.usage()).isEqualTo(beforeRejectedRegistry);
                assertThat(authority.read(key).toCompletableFuture().get(30, TimeUnit.SECONDS))
                        .isEqualTo(originalAuthority);
                control.installControlWriter(registry, authority, key, budget).get(30, TimeUnit.SECONDS);
            }
            owner = control.createReadOwner(view, admission, bookKeeper, offloader, executor, event, 2, budget)
                    .get(30, TimeUnit.SECONDS);
            var before = budget.usage();
            if (taskMode == 65) {
                verifyActualCursorMetadata(owner, budget, executor, ledger, bookKeeper, control);
            }
            var ranges = List.of(
                    new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.EntryRange(
                            offloaded, taskMode >= 63 ? 1 : 0, 3),
                    new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.EntryRange(
                            plain,
                            0,
                            taskMode == 65
                                    ? Math.addExact(
                                            view.materializationDirectory().stream()
                                                    .filter(slot -> slot.ledgerId() == plain)
                                                    .findFirst()
                                                    .orElseThrow()
                                                    .lastEntry(),
                                            1)
                                    : taskMode >= 63 ? 2 : 3));
            if (inputFault != null) {
                inputFault.armCandidate(value ->
                        value.length >= 8 && java.nio.ByteBuffer.wrap(value).getInt() == 0x4e504d49);
                var capture =
                        PulsarNativeMaterializationSourceV2.capture(owner, bookKeeper, offloader, ranges, executor);
                assertThatThrownBy(() -> capture.get(60, TimeUnit.SECONDS))
                        .hasRootCauseMessage("actual selector observation unavailable");
                assertThat(inputFault.putLosses.get()).isEqualTo(1);
                assertThat(inputFault.getLosses.get()).isGreaterThan(0);
                var retained = budget.usage();
                assertThat(retained.owners()).isEqualTo(before.owners() + 1);
                assertThat(budget.durableUsage().get(30, TimeUnit.SECONDS)).isEqualTo(retained);
                assertThatThrownBy(() -> budget.reconcileUndeliveredSdkCleanup().get(30, TimeUnit.SECONDS))
                        .hasRootCauseMessage("actual selector observation unavailable");
                assertThat(budget.usage()).isEqualTo(retained);
                owner.fence().get(30, TimeUnit.SECONDS);
                var closing = budget.closeCell();
                assertThatThrownBy(() -> closing.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
                inputFault.clear();
                budget.reconcileUndeliveredSdkCleanup().get(30, TimeUnit.SECONDS);
                budget.closeCell().get(30, TimeUnit.SECONDS);
                assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
                closing.get(30, TimeUnit.SECONDS);
                System.out.println("NATIVE_PULSAR_MATERIALIZATION_INPUT_UNKNOWN actualPutAppliedReplyLost=true "
                        + "originalCandidateAndDurableCellChargeRetained=true noTaskOutput=true "
                        + "cellCloseWaits=true exactOriginalRecoverySettles=true wholeM5Authority=false");
                return;
            }
            if (closeFromDelivery) {
                var delivered = new CountDownLatch(1);
                PulsarNativeMaterializationSourceV2.capture(owner, bookKeeper, offloader, ranges, executor)
                        .thenAccept(value -> {
                            delivered.countDown();
                            try {
                                value.close();
                            } catch (Exception failure) {
                                throw new java.util.concurrent.CompletionException(failure);
                            }
                        })
                        .get(60, TimeUnit.SECONDS);
                assertThat(delivered.getCount()).isZero();
                assertThat(budget.usage()).isEqualTo(before);
                System.out.println("NATIVE_PULSAR_SOURCE_DELIVERY_CLOSE actualOriginalCallback=true "
                        + "nativeScanAndCloseSettled=true workspaceRefunded=true wholeM5Authority=false");
                return;
            }
            source = PulsarNativeMaterializationSourceV2.capture(owner, bookKeeper, offloader, ranges, executor)
                    .get(60, TimeUnit.SECONDS);
            if (taskMode == 55) {
                var charged = budget.usage();
                var originalOwner = owner;
                assertThatThrownBy(() -> PulsarNativeMaterializationSourceV2.capture(
                                        originalOwner, bookKeeper, offloader, ranges, executor)
                                .get(30, TimeUnit.SECONDS))
                        .hasRootCauseMessage("Native Binding already owns its original materialization source");
                assertThat(budget.usage()).isEqualTo(charged);
                var original = source;
                var cell = original.nativeInput().protocolCellSha256();
                original.qualifyProvider(64L * 1024 * 1024, executor).get(60, TimeUnit.SECONDS);
                var caps = new com.nereusstream.storage.object.materialization.M5MaterializationAdmissionV1.Caps(
                        4, 1024L * 1024 * 1024, 1024L * 1024 * 1024, 1024, 1024, 128, 128);
                var indexes = List.of(
                        com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind
                                .OFFSET_OR_POSITION);
                providerFault.armCandidate(bytes ->
                        bytes.length >= 8 && java.nio.ByteBuffer.wrap(bytes).getInt() == 0x4d35504c);
                assertThatThrownBy(() -> original.prepareTask(true, indexes, caps, executor)
                                .get(60, TimeUnit.SECONDS))
                        .hasRootCauseMessage("actual selector observation unavailable");
                assertThatThrownBy(() -> original.closeAsync().get(30, TimeUnit.SECONDS))
                        .hasRootCauseMessage("actual selector observation unavailable");
                assertThatThrownBy(() -> PulsarNativeMaterializationSourceV2.capture(
                                        originalOwner, bookKeeper, offloader, ranges, executor)
                                .get(30, TimeUnit.SECONDS))
                        .hasRootCauseMessage("Native Binding already owns its original materialization source");
                assertThat(control.materializationAdmission(cell)
                                .read()
                                .orElseThrow()
                                .reservations())
                        .hasSize(1);
                providerFault.clear();
                original.closeAsync().get(30, TimeUnit.SECONDS);
                assertThat(budget.usage()).isEqualTo(before);
                assertThat(control.materializationAdmission(cell)
                                .read()
                                .orElseThrow()
                                .reservations())
                        .isEmpty();
                source = PulsarNativeMaterializationSourceV2.capture(owner, bookKeeper, offloader, ranges, executor)
                        .get(60, TimeUnit.SECONDS);
                source.qualifyProvider(64L * 1024 * 1024, executor).get(60, TimeUnit.SECONDS);
                var resumed = source.prepareTask(true, indexes, caps, executor).get(60, TimeUnit.SECONDS);
                resumed.requireCurrent();
                assertThat(resumed.plan().sourceCut().identity().workerEpoch()).isEqualTo(2);
                assertThat(resumed.plan().sourceCut().identity().ownerEpoch())
                        .isEqualTo(owner.selector().ownerEpoch());
                source.closeAsync().get(30, TimeUnit.SECONDS);
                source = null;
                assertThat(budget.usage()).isEqualTo(before);
                System.out.println("NATIVE_PULSAR_TASK_EXCLUSIVE_WARM_RESUME actualSameCellOwner=true "
                        + "secondSourceRejectedBeforeCharge=true unknownRetainsOriginalSourceSlot=true "
                        + "originalGrantSettledBeforeCancellation=true nextOriginalWorkerEpoch=2 "
                        + "sameOwnerNoCancelledTaskCollision=true noOutputAuthority=true wholeM5Authority=false");
                return;
            }
            if (heldQualification != null) {
                var original = source;
                heldQualification.armed.set(true);
                var verify = CompletableFuture.runAsync(
                        () -> {
                            try {
                                original.verifyPulsarSource(original.sources().get(0));
                            } catch (java.io.IOException failure) {
                                throw new java.util.concurrent.CompletionException(failure);
                            }
                        },
                        executor);
                try {
                    assertThat(heldQualification.entered.await(30, TimeUnit.SECONDS))
                            .isTrue();
                    var charged = budget.usage();
                    var close = original.closeAsync();
                    assertThat(close).isNotDone();
                    assertThatThrownBy(() -> close.get(5, TimeUnit.SECONDS))
                            .isInstanceOf(java.util.concurrent.TimeoutException.class);
                    assertThat(budget.usage()).isEqualTo(charged);
                    heldQualification.release.complete(null);
                    assertThatThrownBy(() -> verify.get(30, TimeUnit.SECONDS))
                            .isInstanceOf(java.util.concurrent.ExecutionException.class);
                    close.get(30, TimeUnit.SECONDS);
                    assertThat(budget.usage()).isEqualTo(before);
                    assertThatThrownBy(() -> original.verifyPulsarSource(
                                    original.sources().get(0)))
                            .hasMessageContaining("qualification remains unresolved");
                    source = null;
                    System.out.println("NATIVE_SOURCE_METADATA_GET_CLOSE actualGetHeld=true "
                            + "metadataDrainKeepsWorkspace=true "
                            + "refundAfterReply=true wholeM5Authority=false");
                    return;
                } finally {
                    heldQualification.release.complete(null);
                    verify.handle((ignored, failure) -> null).get(30, TimeUnit.SECONDS);
                }
            }
            if (providerFault != null) {
                if (taskMode != 0) {
                    verifyActualNativeTask(
                            source,
                            control,
                            binding,
                            budget,
                            owner,
                            ledger,
                            bookKeeper,
                            offloader,
                            executor,
                            providerFault,
                            taskMode,
                            bucket,
                            heldOutputPut,
                            heldMappedGet);
                    source = null;
                    assertThat(budget.usage())
                            .isEqualTo(
                                    taskMode == 52 || taskMode == 56 || taskMode == 57 || taskMode >= 64
                                            ? new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0)
                                            : before);
                    return;
                }
                if (providerUnknown) {
                    providerFault.armCandidate(value ->
                            value.length >= 8 && java.nio.ByteBuffer.wrap(value).getInt() == 0x4d355352);
                    var original = source;
                    assertThatThrownBy(() -> original.qualifyProvider(64L * 1024 * 1024, executor)
                                    .get(60, TimeUnit.SECONDS))
                            .hasRootCauseMessage("actual selector observation unavailable");
                    assertThat(providerFault.putLosses.get()).isEqualTo(1);
                    var retained = budget.usage();
                    var closing = original.closeAsync();
                    assertThatThrownBy(() -> closing.get(30, TimeUnit.SECONDS))
                            .isInstanceOf(java.util.concurrent.ExecutionException.class);
                    assertThat(budget.usage()).isEqualTo(retained);
                    assertThat(budget.durableUsage().get(30, TimeUnit.SECONDS)).isEqualTo(retained);
                    owner.fence().get(30, TimeUnit.SECONDS);
                    var cellClose = budget.closeCell();
                    assertThatThrownBy(() -> cellClose.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
                    providerFault.clear();
                    original.closeAsync().get(30, TimeUnit.SECONDS);
                    source = null;
                    budget.closeCell().get(30, TimeUnit.SECONDS);
                    cellClose.get(30, TimeUnit.SECONDS);
                    System.out.println("NATIVE_PULSAR_PROVIDER_SCOPE_UNKNOWN actualOriginalPutAppliedReplyLost=true "
                            + "sourceAndCellChargeRetained=true closeCannotFreeBeforeExactRecovery=true "
                            + "sameOriginalScopeRecovered=true nativeTaskOutputAuthority=false wholeM5Authority=false");
                } else {
                    var connection =
                            source.qualifyProvider(64L * 1024 * 1024, executor).get(60, TimeUnit.SECONDS);
                    connection.requireProfileCurrent().get(30, TimeUnit.SECONDS);
                    assertThat(connection.profile().protocolCell())
                            .isEqualTo(binding.authority().aggregate().binding().cellIdentity());
                    assertThat(connection.qualifiedProvider().providerScopeId())
                            .isEqualTo(connection.profile().scopeId());
                    var data = source.sources().get(0);
                    var consumed = connection
                            .qualifiedProvider()
                            .consumeVerifiedObject(
                                    new com.nereusstream.storage.object.provider.ObjectIdentity(
                                            data.physicalKey(), data.canonicalLength(), data.bodySha256()),
                                    data.immutableProviderVersionToken(),
                                    java.io.InputStream::readAllBytes);
                    assertThat(consumed.immutableVersionToken()).isEqualTo(data.immutableProviderVersionToken());
                    assertThat(Sha256Digest.hash(CanonicalBytes.copyOf(consumed.value())))
                            .isEqualTo(data.bodySha256());
                    verifyActualProviderScopeRegistry(binding, connection);
                    source.close();
                    source = null;
                    assertThat(budget.usage()).isEqualTo(before);
                    System.out.println("NATIVE_PULSAR_PROVIDER_SCOPE actualCellNamespaceCredentialContext=true "
                            + "fullStableReferencesPersisted=true independentReconnect=true "
                            + "foreignCellOverlappingPrefixRejected=true "
                            + "nativeTaskOutputAuthority=false wholeM5Authority=false");
                }
                return;
            }
            assertThat(source.coverage().chain().stream()
                            .map(value -> value.ledgerId())
                            .toList())
                    .containsExactlyElementsOf(diagnostic.ledgerIds());
            assertThat(source.coverage().chain().get(2).active()).isTrue();
            assertThat(source.coverage().chain().get(2).ledgerId()).isEqualTo(tail);
            assertThat(source.coverage().entryCount()).isEqualTo(6);
            assertThat(source.sources()).hasSize(2);
            assertThat(source.sources().get(0).kind())
                    .isEqualTo(
                            com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind
                                    .PULSAR_NPD1_DATA);
            assertThat(source.sources().get(1).kind())
                    .isEqualTo(
                            com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind
                                    .BOOKKEEPER_LEDGER);
            assertThat(budget.usage().owners()).isEqualTo(before.owners() + 1);
            for (int index = 0; index < source.sources().size(); index++) {
                var extent = source.sources().get(index);
                source.verifyPulsarSource(extent);
                var commitment = new com.nereusstream.storage.object.materialization.M5PulsarEntriesV2.Commitment(
                        extent.coverage().pulsarCoverage().orElseThrow());
                try (var cursor = source.openPulsarEntries(extent)) {
                    for (int entry = 0; entry < 3; entry++) {
                        var actual = cursor.next().orElseThrow();
                        assertThat(actual.position().ledgerId()).isEqualTo(index == 0 ? offloaded : plain);
                        assertThat(actual.position().entryId()).isEqualTo(entry);
                        assertThat(actual.payload().toByteArray()).containsExactly(payload(index * 3 + entry));
                        assertThat(actual.timestamp()).isEqualTo(1_791_177_600_000L + index * 3 + entry);
                        commitment.accept(actual);
                    }
                    assertThat(cursor.next()).isEmpty();
                }
                assertThat(commitment.finish())
                        .isEqualTo(extent.pulsarEntryRootSha256().orElseThrow());
            }
            if (verifyPersistentInput) {
                verifyPersistedMaterializationInput(source, control, binding);
                source.close();
                source = null;
                assertThat(budget.usage()).isEqualTo(before);
                return;
            }
            var stored = independentMetadata
                    .get(nativePath)
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            independentMetadata
                    .put(
                            nativePath,
                            stored.getValue(),
                            Optional.of(stored.getStat().getVersion()))
                    .get(30, TimeUnit.SECONDS);
            var original = source;
            assertThatThrownBy(
                            () -> original.verifyPulsarSource(original.sources().get(0)))
                    .hasMessageContaining("qualification remains unresolved");
            source.close();
            source = null;
            assertThat(budget.usage()).isEqualTo(before);
            System.out.println("NATIVE_PULSAR_MATERIALIZATION_SOURCE actualP1OwnerCellChain=true sealedLedgers=2 "
                    + "entries=6 entryIdsRestartAtZero=true activeTailExcluded=true nativeObjectAndBk=true "
                    + "independentMetadataVersionChangeRejected=true wholeM5Authority=false");
        } catch (Exception | AssertionError failure) {
            sourceFailure = failure;
            // Preserve the original failure in the raw evidence if a retained Cell close also fails.
            failure.printStackTrace(System.err);
            throw failure;
        } finally {
            try {
                if (inputFault != null) {
                    inputFault.clear();
                }
                if (providerFault != null) {
                    providerFault.clear();
                }
                if (source != null) {
                    source.close();
                }
                if (owner != null) {
                    owner.fence().get(30, TimeUnit.SECONDS);
                } else {
                    view.closePreparation().get(30, TimeUnit.SECONDS);
                }
                control.closeControlWriter().get(30, TimeUnit.SECONDS);
                budget.closeCell().get(30, TimeUnit.SECONDS);
                executor.shutdown();
                event.shutdown();
                assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
                assertThat(event.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            } catch (Exception | Error cleanupFailure) {
                if (sourceFailure == null) {
                    throw cleanupFailure;
                }
                sourceFailure.addSuppressed(cleanupFailure);
            }
        }
    }

    private static void verifyActualCursorMetadata(
            PulsarNativeM4ReadOwnerV1 owner,
            PulsarNativeReadCellBudgetV1 budget,
            java.util.concurrent.Executor control,
            ManagedLedgerImpl ledger,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            PulsarNativeM4ControlV1 nativeControl)
            throws Exception {
        var before = budget.usage();
        try (var observation =
                        PulsarNativeCursorMetadataV2.capture(owner, control).get(30, TimeUnit.SECONDS);
                var independent = metadata()) {
            var sourceSelection = nativeControl.captureSelectedSourceMap().get(30, TimeUnit.SECONDS);
            assertThat(sourceSelection.selector()).isPresent();
            if (sourceSelection.manifest()) {
                var manifest = com.nereusstream.storage.object.materialization.M5MaterializationCodecV1.decodeManifest(
                        sourceSelection.source().orElseThrow().canonicalStoredBytes());
                assertThat(com.nereusstream.storage.object.materialization.M5MaterializationCodecV1.manifestSha256(
                                manifest))
                        .isEqualTo(owner.selector().selectedViewSha256());
                assertThat(manifest.identity().binding())
                        .isEqualTo(owner.selector().binding());
            } else {
                assertThat(sourceSelection.map().orElseThrow().actualViewBytes())
                        .isEqualTo(owner.materializationView().canonicalBytes());
            }
            nativeControl.requireSelectedSourceSnapshot(sourceSelection).get(30, TimeUnit.SECONDS);
            assertThat(observation.cursors()).isNotEmpty();
            assertThat(
                            independent
                                    .getChildrenFromStore("/managed-ledgers/" + ledger.getName())
                                    .get(30, TimeUnit.SECONDS)
                                    .stream()
                                    .sorted()
                                    .toList())
                    .isEqualTo(observation.cursors().stream()
                            .map(row -> row.cursorName())
                            .toList());
            assertThat(observation.metadataNamespace())
                    .isEqualTo(owner.materializationView().binding().metadataNamespace());
            assertThat(observation.binding()).isEqualTo(owner.selector().binding());
            var admitted = budget.usage();
            observation.revalidate(control).get(30, TimeUnit.SECONDS);
            assertThat(budget.usage()).isEqualTo(admitted);
            for (var row : observation.cursors()) {
                var stored = independent
                        .get("/managed-ledgers/" + ledger.getName() + "/" + row.cursorName())
                        .get(30, TimeUnit.SECONDS)
                        .orElseThrow();
                assertThat(stored.getStat().getVersion()).isEqualTo(row.metadataVersion());
                assertThat(stored.getValue()).isEqualTo(row.storedBytes());
                var info = observation.cursorInfo(row);
                assertThat(info.getCursorsLedgerId()).isGreaterThanOrEqualTo(-1);
                assertThat(info.hasMarkDeleteLedgerId()).isTrue();
                assertThat(info.hasMarkDeleteEntryId()).isTrue();
                assertThat(observation.canonicalBytes().isEmpty()).isFalse();
            }
            int bkCheckpoints = 0;
            assertThat(owner.materializationView().bookKeeperNamespaceBinding())
                    .isEqualTo(bookKeeper
                            .readBinding()
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow());
            assertThat(observation.checkpoints()).hasSize(observation.cursors().size());
            for (var checkpoint : observation.checkpoints()) {
                if (checkpoint.cursorLedgerId() < 0) {
                    assertThat(checkpoint.positionInfoBytes()).isEmpty();
                    continue;
                }
                bkCheckpoints++;
                var reference = observation.checkpointReferences().stream()
                        .filter(value -> value.cursorName().equals(checkpoint.cursorName()))
                        .findFirst()
                        .orElseThrow();
                var row = observation.cursors().stream()
                        .filter(value -> value.cursorName().equals(checkpoint.cursorName()))
                        .findFirst()
                        .orElseThrow();
                assertThat(reference.resource())
                        .isEqualTo(new PhysicalResourceIdV2.BookKeeperLedger(
                                bookKeeper.namespace(), checkpoint.cursorLedgerId()));
                assertThat(reference.cursorMetadataVersion()).isEqualTo(row.metadataVersion());
                assertThat(reference.cursorStoredBytesSha256())
                        .isEqualTo(Sha256Digest.hash(CanonicalBytes.copyOf(row.storedBytes())));
                assertThat(reference.checkpointEntryId()).isEqualTo(checkpoint.checkpointEntryId());
                assertThat(reference.positionInfoSha256())
                        .isEqualTo(Sha256Digest.hash(CanonicalBytes.copyOf(checkpoint.positionInfoBytes())));
                try (var handle = bookKeeper
                                .borrowClient()
                                .newOpenLedgerOp()
                                .withRecovery(false)
                                .withLedgerId(checkpoint.cursorLedgerId())
                                .withDigestType(ledger.getConfig().getDigestType())
                                .withPassword(ledger.getConfig().getPassword())
                                .execute()
                                .get(30, TimeUnit.SECONDS);
                        var entries = handle.readAsync(checkpoint.checkpointEntryId(), checkpoint.checkpointEntryId())
                                .get(30, TimeUnit.SECONDS)) {
                    assertThat(handle.getLastAddConfirmed()).isEqualTo(checkpoint.checkpointEntryId());
                    assertThat(entries.getEntry(checkpoint.checkpointEntryId()).getEntryBytes())
                            .isEqualTo(checkpoint.positionInfoBytes());
                    var info = observation.positionInfo(checkpoint);
                    assertThat(info.hasLedgerId()).isTrue();
                    assertThat(info.hasEntryId()).isTrue();
                }
            }
            if (!owner.materializationView().materialized()) {
                assertThat(bkCheckpoints).isPositive();
            }
            assertThat(observation.checkpointReferences()).hasSize(bkCheckpoints);
            assertThat(observation.cursorFloors()).hasSize(observation.cursors().size());
            var chain = owner.materializationView().materializationChain();
            for (int i = 0; i < observation.cursorFloors().size(); i++) {
                var floor = observation.cursorFloors().get(i);
                assertThat(floor.cursorName())
                        .isEqualTo(observation.cursors().get(i).cursorName());
                assertThat(chain.stream()
                                .anyMatch(item ->
                                        item.ledgerId() == floor.firstRequired().ledgerId()))
                        .isTrue();
                var checkpoint = observation.checkpoints().get(i);
                if (checkpoint.cursorLedgerId() >= 0) {
                    var actual = observation.positionInfo(checkpoint);
                    var same = chain.stream()
                            .filter(item -> item.ledgerId() == actual.getLedgerId())
                            .findFirst();
                    if (same.isPresent()
                            && actual.getEntryId() + 1 < same.orElseThrow().entryEndExclusive()) {
                        assertThat(floor.positionResolved()).isTrue();
                        assertThat(floor.firstRequired())
                                .isEqualTo(
                                        new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                                                actual.getLedgerId(), actual.getEntryId() + 1));
                    }
                }
            }
            var first = chain.get(0);
            assertThat(observation.cursorRetains(
                            new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.EntryRange(
                                    first.ledgerId(), 0, first.entryEndExclusive())))
                    .isTrue();
            var selected = owner.materializationView().materialized()
                    ? nativeControl.readSelectedMaterialization(control).get(30, TimeUnit.SECONDS)
                    : null;
            if (selected != null) {
                var snapshot = nativeControl
                        .readSelectedMaterializationSnapshot(control)
                        .get(30, TimeUnit.SECONDS);
                nativeControl
                        .requireSelectedMaterializationSnapshot(snapshot, control)
                        .get(30, TimeUnit.SECONDS);
                try (var sdk = client(null)) {
                    var physical = selected.providerProfile().physicalBinding().physicalNamespace();
                    var backend = S3M5PhysicalNamespaceAuthorityV2.connect(
                            sdk, physical.containerIdentity().value(), physical);
                    var quota = owner.materializationView()
                            .binding()
                            .materializationQuota(backend)
                            .get(30, TimeUnit.SECONDS);
                    for (var credit : selected.readMap().readableOutputs()) {
                        var resource = credit.resource().orElseThrow();
                        quota.readResourceFact(resource).toCompletableFuture().get(30, TimeUnit.SECONDS);
                        var member = quota.members()
                                .readFact(resource)
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS)
                                .inventory();
                        assertThat(member.protocolCell())
                                .isEqualTo(selected.providerProfile().protocolCellSha256());
                        assertThat(member.members())
                                .contains(
                                        new com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2
                                                .Member(
                                                com.nereusstream.storage.object.materialization
                                                        .M5MaterializationRecordsV1.PositionDomain.PULSAR_ENTRY,
                                                selected.providerProfile()
                                                        .scopeId()
                                                        .digest(),
                                                owner.selector().binding()));
                    }
                }
            }
            for (var item : chain) {
                var expectedPhysical = new java.util.TreeSet<PhysicalResourceIdV2>();
                if (item.entryEndExclusive() == 0) {
                    var actualHeader = bookKeeper
                            .borrowClient()
                            .getLedgerManager()
                            .readLedgerMetadata(item.ledgerId())
                            .get(30, TimeUnit.SECONDS)
                            .getValue();
                    assertThat(actualHeader.getCustomMetadata().get("pulsar/managed-ledger"))
                            .isEqualTo(ledger.getName().getBytes(StandardCharsets.UTF_8));
                    expectedPhysical.add(
                            new PhysicalResourceIdV2.BookKeeperLedger(bookKeeper.namespace(), item.ledgerId()));
                }
                for (long id = 0; id < item.entryEndExclusive(); id++) {
                    var position = new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                            item.ledgerId(), id);
                    var unit = new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.EntryRange(
                            item.ledgerId(), id, id + 1);
                    expectedPhysical.addAll(owner.materializationView().physicalDataReferences(unit));
                    var matching = observation.dataReferences().stream()
                            .filter(ref -> ref.range().ledgerId() == position.ledgerId()
                                    && ref.range().startEntryInclusive() <= position.entryId()
                                    && ref.range().endEntryExclusive() > position.entryId())
                            .toList();
                    assertThat(matching).hasSize(observation.cursorRetains(unit) ? 1 : 0);
                    if (matching.isEmpty()) {
                        continue;
                    }
                    var resources = matching.get(0).resources();
                    if (selected != null && selected.readMap().rewrites(position)) {
                        assertThat(resources)
                                .containsExactly(selected.readMap()
                                        .locate(position)
                                        .rewrittenPayload()
                                        .orElseThrow());
                    } else {
                        assertThat(resources)
                                .allMatch(owner.materializationView().members()::contains);
                        for (var resource : resources) {
                            if (resource instanceof PhysicalResourceIdV2.BookKeeperLedger bk) {
                                assertThat(bk.namespace()).isEqualTo(bookKeeper.namespace());
                                assertThat(bk.ledgerId()).isEqualTo(position.ledgerId());
                            }
                        }
                    }
                }
                var actualPhysical = selected == null
                        ? sourceSelection
                                .map()
                                .orElseThrow()
                                .physicalLedgerReferences(
                                        owner.materializationView().binding(),
                                        owner.materializationView().bookKeeperNamespaceBinding(),
                                        owner.materializationView().objectBinding(),
                                        item.ledgerId(),
                                        item.entryEndExclusive())
                        : selected.readMap()
                                .physicalLedgerReferences(
                                        owner.materializationView().binding(),
                                        owner.materializationView().bookKeeperNamespaceBinding(),
                                        owner.materializationView().objectBinding(),
                                        item.ledgerId(),
                                        item.entryEndExclusive());
                if (actualPhysical.isPresent()) {
                    assertThat(actualPhysical.orElseThrow()).containsExactlyElementsOf(expectedPhysical);
                } else {
                    assertThat(selected).isNotNull();
                    assertThat(selected.readMap()
                                    .plan()
                                    .sourceCut()
                                    .coverage()
                                    .pulsarCoverage()
                                    .orElseThrow()
                                    .chain())
                            .noneMatch(row -> row.ledgerId() == item.ledgerId());
                    var header = bookKeeper
                            .borrowClient()
                            .getLedgerManager()
                            .readLedgerMetadata(item.ledgerId())
                            .get(30, TimeUnit.SECONDS)
                            .getValue();
                    assertThat(header.getCustomMetadata().get("pulsar/managed-ledger"))
                            .isEqualTo(ledger.getName().getBytes(StandardCharsets.UTF_8));
                    assertThat(expectedPhysical)
                            .containsExactly(
                                    new PhysicalResourceIdV2.BookKeeperLedger(bookKeeper.namespace(), item.ledgerId()));
                }
            }
            assertThat(budget.usage().owners()).isGreaterThan(before.owners());
            var referenceCoverage = observation.referenceCoverage();
            for (var checkpoint : observation.checkpointReferences()) {
                verifyNativeReferenceVeto(
                        observation,
                        com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                .RECOVERY_CHECKPOINT_OR_SNAPSHOT,
                        checkpoint.resource(),
                        referenceCoverage,
                        control);
            }
            if (observation.minimumCursorFloor().isPresent()) {
                for (var reference : observation.dataReferences()) {
                    for (var resource : reference.resources()) {
                        verifyNativeReferenceVeto(
                                observation,
                                com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                        .PULSAR_SUBSCRIPTION_OR_REPLICATION_CURSOR,
                                resource,
                                referenceCoverage,
                                control);
                    }
                }
            }
            assertThatThrownBy(() -> observation
                            .scanReferences(
                                    com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                            .RECOVERY_CHECKPOINT_OR_SNAPSHOT,
                                    observation.sha256(),
                                    referenceCoverage,
                                    control)
                            .get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("Native cursor reference absence requires closed writer/directory evidence");
            assertThat(budget.usage()).isEqualTo(admitted);
            if (!owner.materializationView().materialized()) {
                var cursor = ledger.openCursor("checkpoint-observation");
                var advanced = PositionFactory.create(first.ledgerId(), first.entryEndExclusive() - 1);
                assertThat(advanced.compareTo(cursor.getMarkDeletedPosition())).isPositive();
                cursor.markDelete(advanced);
                assertThatThrownBy(() -> observation.revalidate(control).get(30, TimeUnit.SECONDS))
                        .hasRootCauseMessage("actual Native cursor observation changed");
                if (!observation.checkpointReferences().isEmpty()) {
                    assertThatThrownBy(() -> observation
                                    .scanReferences(
                                            com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                                    .ReferenceKindV1.RECOVERY_CHECKPOINT_OR_SNAPSHOT,
                                            observation
                                                    .checkpointReferences()
                                                    .get(0)
                                                    .resource()
                                                    .sha256(),
                                            referenceCoverage,
                                            control)
                                    .get(30, TimeUnit.SECONDS))
                            .hasRootCauseMessage("actual Native cursor observation changed");
                }
                assertThat(budget.usage()).isEqualTo(admitted);
                var queued = new AtomicReference<Runnable>();
                var modifiedObserver = observation.revalidate(command -> {
                    if (!queued.compareAndSet(null, command)) {
                        throw new IllegalStateException("another revalidation was scheduled");
                    }
                });
                assertThat(queued.get()).isNotNull();
                assertThat(modifiedObserver.complete(null)).isTrue();
                var originalClose = observation.closeAsync();
                assertThat(originalClose).isNotDone();
                assertThat(budget.usage()).isEqualTo(admitted);
                queued.get().run();
                originalClose.get(30, TimeUnit.SECONDS);
                assertThat(budget.usage()).isEqualTo(before);
            }
        }
        assertThat(budget.usage()).isEqualTo(before);
        System.out.println(
                "NATIVE_PULSAR_CURSOR_METADATA actualCompleteCurrentDirectoryReread=true backendDirectory=true "
                        + "originalStoredBytesVersions=true independentNativeMetadata=true "
                        + "actualBkPositionInfo=true typedCursorFloors=true exactPhysicalCheckpointReferences=true "
                        + "cursorDataReferencesMatchSelectedPhysicalRoutes=true "
                        + "freshNativeObservationAndExactDrain=true "
                        + "ownerHazardAndWorkspace=true referenceFreeAuthority=false wholeM5Authority=false");
    }

    private static void verifyNativeReferenceVeto(
            PulsarNativeCursorMetadataV2 observation,
            com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1 kind,
            PhysicalResourceIdV2 resource,
            com.nereusstream.storage.object.materialization.M5PulsarCoverageV2 coverage,
            java.util.concurrent.Executor control)
            throws Exception {
        var result = observation.scanReferences(kind, resource.sha256(), coverage, control);
        assertThatThrownBy(() -> result.get(30, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(PulsarNativeCursorMetadataV2.NativeReferencePresentException.class);
        Throwable failure = result.handle((value, error) -> error).get(30, TimeUnit.SECONDS);
        while (failure.getCause() != null) {
            failure = failure.getCause();
        }
        var present = (PulsarNativeCursorMetadataV2.NativeReferencePresentException) failure;
        assertThat(present.kind()).isEqualTo(kind);
        assertThat(present.target()).isEqualTo(resource.sha256());
        assertThat(present.coverage()).isEqualTo(coverage);
        assertThat(present.nativeSnapshot()).isEqualTo(observation.canonicalBytes());
        assertThat(present.resources()).containsExactly(resource);
        System.out.println("NATIVE_CURSOR_REFERENCE_VETO kind=" + kind + " target="
                + resource.sha256().toHex()
                + " actualSnapshotReread=true actualPhysicalWitness=true noScalarCoverage=true"
                + " referenceAbsenceQualified=false wholeM5Authority=false");
    }

    private static void verifyActualNativeTask(
            PulsarNativeMaterializationSourceV2 source,
            PulsarNativeM4ControlV1 control,
            PulsarNativeBindingV1 binding,
            PulsarNativeReadCellBudgetV1 budget,
            PulsarNativeM4ReadOwnerV1 owner,
            ManagedLedgerImpl nativeLedger,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            java.util.concurrent.Executor executor,
            SelectorResponseFault fault,
            int mode,
            String bucket,
            AtomicReference<SdkHold> heldOutputPut,
            AtomicReference<SdkHold> heldMappedGet)
            throws Exception {
        var connection = source.qualifyProvider(64L * 1024 * 1024, executor).get(60, TimeUnit.SECONDS);
        var caps = new com.nereusstream.storage.object.materialization.M5MaterializationAdmissionV1.Caps(
                4, 1024L * 1024 * 1024, mode == 53 ? 1 : 1024L * 1024 * 1024, 1024, 1024, 128, 128);
        var indexes = List.of(
                com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind.OFFSET_OR_POSITION,
                com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind.CHECKSUM_COVERAGE);
        if (mode == 52 || mode == 56 || mode == 57) {
            var cell = source.nativeInput().protocolCellSha256();
            fault.armCandidate(bytes -> bytes.length >= 8
                    && java.nio.ByteBuffer.wrap(bytes).getInt()
                            == (mode == 57 ? 0x4d354243 : mode == 56 ? 0x4d35504f : 0x4d35504c));
            assertThatThrownBy(() ->
                            source.prepareTask(true, indexes, caps, executor).get(60, TimeUnit.SECONDS))
                    .hasRootCauseMessage("actual selector observation unavailable");
            assertThat(fault.putLosses.get()).isEqualTo(1);
            var retained = budget.usage();
            assertThatThrownBy(() -> source.closeAsync().get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("actual selector observation unavailable");
            assertThat(budget.usage()).isEqualTo(retained);
            assertThat(budget.durableUsage().get(30, TimeUnit.SECONDS)).isEqualTo(retained);
            assertThat(control.materializationAdmission(cell)
                            .read()
                            .orElseThrow()
                            .reservations())
                    .hasSize(1);
            owner.fence().get(30, TimeUnit.SECONDS);
            var closing = budget.closeCell();
            assertThatThrownBy(() -> closing.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
            fault.clear();
            source.closeAsync().get(30, TimeUnit.SECONDS);
            budget.closeCell().get(30, TimeUnit.SECONDS);
            closing.get(30, TimeUnit.SECONDS);
            System.out.println((mode == 57
                            ? "NATIVE_PULSAR_BIRTH_CREDIT_UNKNOWN actualOriginalCreditAppliedReplyLost=true "
                            : mode == 56
                                    ? "NATIVE_PULSAR_PENDING_INVENTORY_UNKNOWN "
                                            + "actualOriginalPendingKeysAppliedReplyLost=true "
                                    : "NATIVE_PULSAR_TASK_PLAN_UNKNOWN actualOriginalPlanAppliedReplyLost=true ")
                    + "originalPlanAndSharedChargeRetained=true sourceCloseAndCellCloseWait=true "
                    + "exactUnstartedCancellationThenRefund=true outputCreateAuthority=false wholeM5Authority=false");
            return;
        }
        if (mode == 58) {
            var quota = binding.materializationQuota(connection.physical()).get(30, TimeUnit.SECONDS);
            var before = quota.quota().snapshot().toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertThatThrownBy(() ->
                            source.prepareTask(true, indexes, caps, executor).get(60, TimeUnit.SECONDS))
                    .hasRootCauseMessage("Native original Object birth capacity remains unresolved: EXHAUSTED");
            source.closeAsync().get(30, TimeUnit.SECONDS);
            assertThat(quota.quota().snapshot().toCompletableFuture().get(30, TimeUnit.SECONDS))
                    .isEqualTo(before);
            assertThat(before.head().births()).isEmpty();
            System.out.println("NATIVE_PULSAR_BIRTH_CAP_REJECT actualAssignedQuota=true "
                    + "noCreditOrPhysicalVersionInvented=true noProviderOutput=true unchangedCapacity=true "
                    + "originalNoDispatchTaskCancelled=true wholeM5Authority=false");
            return;
        }
        if (mode == 53) {
            assertThatThrownBy(() ->
                            source.prepareTask(true, indexes, caps, executor).get(60, TimeUnit.SECONDS))
                    .hasRootCauseMessage("Native original Task Cell reservation remains unresolved: REJECTED_CAP");
            assertThat(control.materializationAdmission(source.nativeInput().protocolCellSha256())
                            .read()
                            .orElseThrow()
                            .reservations())
                    .isEmpty();
            source.closeAsync().get(30, TimeUnit.SECONDS);
            System.out.println("NATIVE_PULSAR_TASK_CAP_REJECT actualSharedCellCap=true "
                    + "noCompletePlanTaskOrOutputCreated=true sourceWorkspaceRefunded=true wholeM5Authority=false");
            return;
        }
        var task = source.prepareTask(true, indexes, caps, executor).get(60, TimeUnit.SECONDS);
        task.requireCurrent();
        var plan = task.plan();
        if (mode >= 59 && mode <= 67 || mode == 79) {
            verifyActualNativeOutputs(
                    source,
                    task,
                    control,
                    binding,
                    budget,
                    owner,
                    nativeLedger,
                    bookKeeper,
                    offloader,
                    executor,
                    fault,
                    mode,
                    bucket,
                    heldOutputPut,
                    heldMappedGet,
                    connection.physical());
            return;
        }
        if (mode == 54) {
            fault.holdOneGet(control.selectorKey());
            var checking = CompletableFuture.runAsync(task::requireCurrent, executor);
            try {
                assertThat(fault.heldReadEntered.await(30, TimeUnit.SECONDS)).isTrue();
                var retained = budget.usage();
                var closing = source.closeAsync();
                assertThat(closing).isNotDone();
                assertThatThrownBy(() -> closing.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
                assertThat(budget.usage()).isEqualTo(retained);
                assertThat(budget.durableUsage().get(30, TimeUnit.SECONDS)).isEqualTo(retained);
                fault.heldReadRelease.complete(null);
                assertThatThrownBy(() -> checking.get(30, TimeUnit.SECONDS))
                        .isInstanceOf(java.util.concurrent.ExecutionException.class);
                closing.get(30, TimeUnit.SECONDS);
            } finally {
                fault.heldReadRelease.complete(null);
                checking.handle((ignored, failure) -> null).get(30, TimeUnit.SECONDS);
            }
            System.out.println("NATIVE_PULSAR_TASK_CURRENT_READ_DRAIN actualOriginalGetHeld=true "
                    + "sourceWorkspaceAndSdkLoanRetained=true originalReadDrainPrecedesRefund=true "
                    + "wholeM5Authority=false");
            return;
        }
        assertThat(plan.sourceCut().sources()).isEqualTo(source.sources());
        assertThat(plan.sourceCut().identity().providerScopeSha256())
                .isEqualTo(connection.profile().scopeId().digest());
        assertThat(plan.sourceCut().identity().protocolCellSha256())
                .isEqualTo(source.nativeInput().protocolCellSha256());
        assertThat(plan.sourceCut().identity().ownerEpoch())
                .isEqualTo(owner.selector().ownerEpoch());
        assertThat(plan.sourceCut().identity().workerEpoch()).isEqualTo(1);
        assertThat(plan.sourceCut().identity().storageFence())
                .isEqualTo(source.nativeInput().storageFence());
        assertThat(plan.sourceCut().protocolStateRootSha256())
                .isEqualTo(source.nativeInput().protocolStateRootSha256());
        assertThat(plan.sourceCut().recoveryCheckpointRootSha256())
                .isEqualTo(source.recoveryInput().sha256());
        assertThat(plan.sourceCut().pulsarFrontiers().orElseThrow().recovery()).isEmpty();
        try (var independent = OxiaClientBuilder.create(System.getProperty("nereus.m5.nativeOffloadOxia"))
                .namespace("default")
                .clientIdentifier("native-task-complete-plan-independent")
                .asyncClient()
                .get(30, TimeUnit.SECONDS)) {
            var exact =
                    new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(independent);
            var storedPlan = exact.read(control.materializationPlanKey(plan.taskIdSha256()))
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            assertThat(storedPlan.canonicalStoredBytes())
                    .isEqualTo(
                            com.nereusstream.storage.object.materialization.M5MaterializationCodecV1.encodePlan(plan));
            var authority = com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1.decodeAuthority(
                    exact.read(control.selectorKey())
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow()
                            .canonicalStoredBytes());
            var worker = authority.materializationWorker().orElseThrow();
            assertThat(worker.taskIdSha256()).isEqualTo(plan.taskIdSha256());
            assertThat(worker.planRootSha256()).isEqualTo(Sha256Digest.hash(storedPlan.canonicalStoredBytes()));
            var pending = exact.read(control.materializationPendingOutputsKey(plan.taskIdSha256()))
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            var inventory = com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2.decode(
                    pending.canonicalStoredBytes());
            inventory.requirePlan(plan);
            assertThat(inventory.worker()).isEqualTo(worker);
            assertThat(inventory.profile()).isEqualTo(connection.profile());
            assertThat(inventory.outputs().stream()
                            .map(value -> value.objectKey())
                            .toList())
                    .containsExactlyElementsOf(java.util.stream.Stream.concat(
                                    plan.outputParts().stream().map(value -> value.objectKey()),
                                    plan.indexes().stream().map(value -> value.objectKey()))
                            .toList());
            assertThat(inventory.state())
                    .isEqualTo(
                            com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2.State.PREPARED);
            var profileBinding = inventory.profile().physicalBinding();
            var layout = com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Layout.withPhysicalMembers(
                    profileBinding.authorityRoot(), profileBinding.physicalNamespace());
            var beforeBirths = com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Head.decode(
                    exact.read(layout.nativeKey(com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.HEAD_KEY))
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow()
                            .canonicalStoredBytes());
            assertThat(beforeBirths.births().orElseThrow().issued())
                    .isEqualTo(inventory.outputs().size());
            assertThat(beforeBirths.births().orElseThrow().unassigned())
                    .isEqualTo(inventory.outputs().size());
            assertThat(beforeBirths.reservedResources()).isZero();
            for (var slot : inventory.outputs()) {
                var claim = new com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3(
                        layout.scope(),
                        worker,
                        slot,
                        2,
                        com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3.State.RESERVED,
                        Optional.empty());
                var credit = com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3.decode(
                        exact.read(layout.nativeKey(claim.key()))
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS)
                                .orElseThrow()
                                .canonicalStoredBytes());
                assertThat(credit.worker()).isEqualTo(worker);
                assertThat(credit.slot()).isEqualTo(slot);
                assertThat(credit.resource()).isEmpty();
            }
            source.closeAsync().get(30, TimeUnit.SECONDS);
            assertThatThrownBy(task::requireCurrent).hasMessageContaining("no usable complete-plan worker grant");
            var terminal = exact.read(control.materializationTaskKey(plan.taskIdSha256()))
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            assertThat(com.nereusstream.storage.object.materialization.M5MaterializationCodecV1.decodeTask(
                                    terminal.canonicalStoredBytes())
                            .state())
                    .isEqualTo(
                            com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.TaskState
                                    .CANCELLED_STALE);
            var cancelled = exact.read(control.materializationPendingOutputsKey(plan.taskIdSha256()))
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            assertThat(com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2.decode(
                            cancelled.canonicalStoredBytes()))
                    .isEqualTo(inventory.cancelUnstarted());
            var afterBirths = com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Head.decode(
                    exact.read(layout.nativeKey(com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.HEAD_KEY))
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow()
                            .canonicalStoredBytes());
            assertThat(afterBirths.births().orElseThrow().cancelled())
                    .isEqualTo(inventory.outputs().size());
            assertThat(afterBirths.births().orElseThrow().unassigned()).isZero();
            assertThat(beforeBirths.usedBytes(layout) - afterBirths.usedBytes(layout))
                    .isEqualTo(inventory.outputs().size() * layout.reservationCharge());
            for (var slot : inventory.outputs()) {
                var claim = new com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3(
                        layout.scope(),
                        worker,
                        slot,
                        2,
                        com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3.State.RESERVED,
                        Optional.empty());
                var credit = com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3.decode(
                        exact.read(layout.nativeKey(claim.key()))
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS)
                                .orElseThrow()
                                .canonicalStoredBytes());
                assertThat(credit.state())
                        .isEqualTo(
                                com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3.State.CANCELLED_NO_DISPATCH);
            }
            var charged = exact.read(control.materializationAdmissionKey(
                            plan.sourceCut().identity().protocolCellSha256()))
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            assertThat(com.nereusstream.storage.object.materialization.M5MaterializationAdmissionV1.decode(
                                    charged.canonicalStoredBytes())
                            .reservations())
                    .isEmpty();
        }
        if (mode == 51) {
            var exact = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(fault);
            String key = new OxiaV2AuthorityKeys(binding.authorityRoot())
                    .aggregateKey(binding.authority().aggregate().binding().incarnationIdentity());
            var legacy = exact.read(key)
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            assertThat(com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.isAuthorityValue(
                            legacy.canonicalStoredBytes()))
                    .isFalse();
            assertThatThrownBy(() -> binding.logicalTrimCoordinator()
                            .beginSourceScan(
                                    legacy,
                                    Sha256Digest.hash(CanonicalUtf8.fromString("unqualified-legacy-task-roots")
                                            .bytes())))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(exact.read(key).toCompletableFuture().get(30, TimeUnit.SECONDS))
                    .contains(legacy);
        }
        System.out.println("NATIVE_PULSAR_TASK_PREPARATION actualOriginalOwnerScopeCellInput=true "
                + "independentCompletePlanAndWorkerGrant=true sharedCellReservation=true "
                + "unstartedOriginalTaskCancelledBeforeRefund=true outputCreateAuthority=false wholeM5Authority=false");
    }

    private static void verifyActualNativeOutputs(
            PulsarNativeMaterializationSourceV2 source,
            PulsarNativeMaterializationTaskV2 task,
            PulsarNativeM4ControlV1 control,
            PulsarNativeBindingV1 binding,
            PulsarNativeReadCellBudgetV1 budget,
            PulsarNativeM4ReadOwnerV1 owner,
            ManagedLedgerImpl nativeLedger,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            java.util.concurrent.Executor executor,
            SelectorResponseFault fault,
            int mode,
            String bucket,
            AtomicReference<SdkHold> heldPut,
            AtomicReference<SdkHold> heldMappedGet,
            com.nereusstream.storage.api.lifecycle.NativePhysicalNamespaceAuthorityV2 objectBackend)
            throws Exception {
        var plan = task.plan();
        Sha256Digest originalPublishedManifest = null;
        var staging = Files.createTempDirectory("nereus-native-materialization-output-");
        SdkHold put = mode == 61 || mode == 62 ? hold() : null;
        try (var independent = client(null);
                var metadata = OxiaClientBuilder.create(System.getProperty("nereus.m5.nativeOffloadOxia"))
                        .namespace("default")
                        .asyncClient()
                        .get(30, TimeUnit.SECONDS)) {
            if (mode == 60) {
                fault.armCandidate(value -> value.length >= 8
                        && java.nio.ByteBuffer.wrap(value).getInt() == 0x4d354243
                        && com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3.decode(CanonicalBytes.copyOf(value))
                                        .state()
                                == com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3.State.WRITE_ARMED);
            }
            if (put != null) {
                if (mode == 62) {
                    put.failure().set(SdkClientException.create("actual materialization PUT reply lost"));
                    put.released().countDown();
                }
                heldPut.set(put);
            }
            var writing = task.writeAndVerify(staging, 64L * 1024 * 1024, executor);
            if (mode == 60) {
                assertThatThrownBy(() -> writing.get(60, TimeUnit.SECONDS))
                        .hasRootCauseMessage("actual selector observation unavailable");
                var retained = budget.usage();
                assertThatThrownBy(() -> source.closeAsync().get(30, TimeUnit.SECONDS))
                        .hasRootCauseMessage("actual selector observation unavailable");
                assertThat(budget.usage()).isEqualTo(retained);
                assertThat(budget.durableUsage().get(30, TimeUnit.SECONDS)).isEqualTo(retained);
                assertThat(independent
                                .listObjectsV2(request -> request.bucket(bucket)
                                        .prefix(plan.outputParts().get(0).objectKey()))
                                .contents())
                        .isEmpty();
                fault.clear();
                source.closeAsync().get(30, TimeUnit.SECONDS);
                assertThat(independent
                                .listObjectsV2(request -> request.bucket(bucket)
                                        .prefix(plan.outputParts().get(0).objectKey()))
                                .contents())
                        .isEmpty();
                System.out.println("NATIVE_PULSAR_OUTPUT_INTENT_UNKNOWN actualBodyIntentAppliedReplyLost=true "
                        + "actualProviderOutputAbsent=true workspaceSourceCellRetained=true "
                        + "originalNoDispatchCancellation=true wholeM5Authority=false");
                return;
            }
            if (mode == 61) {
                try {
                    assertThat(put.started().await(30, TimeUnit.SECONDS)).isTrue();
                    var retained = budget.usage();
                    assertThat(writing.cancel(false)).isTrue();
                    var closing = source.closeAsync();
                    assertThatThrownBy(() -> closing.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
                    assertThat(budget.usage()).isEqualTo(retained);
                    assertThat(independent
                                    .headObject(request -> request.bucket(bucket)
                                            .key(put.key().get())
                                            .versionId(put.version().get()))
                                    .versionId())
                            .isEqualTo(put.version().get());
                    put.released().countDown();
                    closing.get(30, TimeUnit.SECONDS);
                    var terminal = metadata.get(control.materializationTaskKey(plan.taskIdSha256()))
                            .get(30, TimeUnit.SECONDS);
                    assertThat(com.nereusstream.storage.object.materialization.M5MaterializationCodecV1.decodeTask(
                                            CanonicalBytes.copyOf(terminal.value()))
                                    .state())
                            .isEqualTo(
                                    com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.TaskState
                                            .QUARANTINED);
                    assertThat(independent
                                    .headObject(request -> request.bucket(bucket)
                                            .key(put.key().get())
                                            .versionId(put.version().get()))
                                    .versionId())
                            .isEqualTo(put.version().get());
                    System.out.println("NATIVE_PULSAR_OUTPUT_PUT_DRAIN actualPutAppliedReplyHeld=true "
                            + "observerCancelled=true sourceCloseWaitsActualSdk=true "
                            + "quarantinedActualOutputPreserved=true wholeM5Authority=false");
                    return;
                } finally {
                    put.released().countDown();
                }
            }
            var verified = writing.get(60, TimeUnit.SECONDS);
            assertThat(verified.plan()).isEqualTo(plan);
            assertThat(verified.payloads()).hasSize(2);
            assertThat(verified.indexes()).hasSize(2);
            var exact = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(metadata);
            var inventory = com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2.decode(
                    exact.read(control.materializationPendingOutputsKey(plan.taskIdSha256()))
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow()
                            .canonicalStoredBytes());
            var physical = inventory.profile().physicalBinding();
            var layout = com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Layout.withPhysicalMembers(
                    physical.authorityRoot(), physical.physicalNamespace());
            var descriptors = java.util.stream.Stream.concat(verified.payloads().stream(), verified.indexes().stream())
                    .toList();
            var assigned = new java.util.ArrayList<com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3>();
            for (var descriptor : descriptors) {
                String version = new String(
                        descriptor.immutableProviderVersionToken().orElseThrow().toByteArray(), StandardCharsets.UTF_8);
                var body = independent.getObjectAsBytes(request ->
                        request.bucket(bucket).key(descriptor.identity().key()).versionId(version));
                assertThat(body.response().versionId()).isEqualTo(version);
                var bytes = CanonicalBytes.copyOf(body.asByteArray());
                assertThat(bytes.length()).isEqualTo(descriptor.identity().bodyLength());
                assertThat(Sha256Digest.hash(bytes))
                        .isEqualTo(descriptor.identity().bodySha256());
                if (descriptor.payload()) {
                    var value = com.nereusstream.storage.object.materialization.Nms1CodecV1.decode(bytes);
                    assertThat(value.taskIdSha256()).isEqualTo(plan.taskIdSha256());
                    var selected = descriptor
                            .coverage()
                            .pulsarCoverage()
                            .orElseThrow()
                            .ranges()
                            .get(0);
                    int entryCount = Math.toIntExact(selected.endEntryExclusive() - selected.startEntryInclusive());
                    assertThat(value.extents()).hasSize(entryCount);
                    for (int entry = 0; entry < entryCount; entry++) {
                        var extent = value.extents().get(entry);
                        int originalEntry = Math.toIntExact(selected.startEntryInclusive() + entry);
                        assertThat(extent.coverage()
                                        .pulsarCoverage()
                                        .orElseThrow()
                                        .ranges()
                                        .get(0)
                                        .ledgerId())
                                .isEqualTo(plan.sourceCut()
                                        .sources()
                                        .get(descriptor.ordinal())
                                        .coverage()
                                        .pulsarCoverage()
                                        .orElseThrow()
                                        .ranges()
                                        .get(0)
                                        .ledgerId());
                        assertThat(extent.coverage()
                                        .pulsarCoverage()
                                        .orElseThrow()
                                        .ranges()
                                        .get(0)
                                        .startEntryInclusive())
                                .isEqualTo(originalEntry);
                        assertThat(extent.minimumTimestamp())
                                .isEqualTo(1_791_177_600_000L + descriptor.ordinal() * 3 + originalEntry);
                        assertThat(java.util.Arrays.copyOfRange(
                                        value.payload().toByteArray(),
                                        extent.payloadOffset(),
                                        extent.payloadOffset() + extent.payloadLength()))
                                .containsExactly(payload(descriptor.ordinal() * 3 + originalEntry));
                    }
                } else {
                    assertThat(com.nereusstream.storage.object.materialization.M5LookupIndexV1.decode(bytes)
                                    .rows())
                            .hasSize(Math.toIntExact(source.coverage().entryCount()));
                }
                var slot = inventory.outputs().stream()
                        .filter(value ->
                                value.objectKey().equals(descriptor.identity().key()))
                        .findFirst()
                        .orElseThrow();
                var claim = new com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3(
                        layout.scope(),
                        inventory.worker(),
                        slot,
                        2,
                        com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3.State.RESERVED,
                        Optional.empty());
                var credit = com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3.decode(
                        exact.read(layout.nativeKey(claim.key()))
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS)
                                .orElseThrow()
                                .canonicalStoredBytes());
                assertThat(credit.output()).contains(descriptor.identity());
                assertThat(credit.resource().orElseThrow().immutableIdentity().value())
                        .isEqualTo(version);
                assigned.add(credit);
            }
            var map = task.readMap();
            assertThat(map.outputs()).containsExactlyElementsOf(assigned);
            assertThat(map.plan()).isEqualTo(plan);
            var original = exact.read(control.sourceMapKey(
                            plan.sourceCut().predecessorSelector().selectedViewSha256()))
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            assertThat(map.predecessor().encode()).isEqualTo(original.canonicalStoredBytes());
            var decodedMap = PulsarNativeMaterializationReadMapV2.decode(map.encode());
            assertThat(decodedMap.sha256()).isEqualTo(map.sha256());
            if (mode == 67) {
                var beforeSelector = exact.read(control.selectorKey())
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS)
                        .orElseThrow();
                fault.holdAppliedPut(null, bytes -> {
                    try {
                        var value = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                                CanonicalBytes.copyOf(bytes));
                        return map.readableOutputs().stream().anyMatch(credit -> credit.resource()
                                        .orElseThrow()
                                        .equals(value.target().resourceId()))
                                && value.activeWriterTickets().stream()
                                        .anyMatch(ticket -> ticket.writerClass()
                                                == com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                                        .ProofBoundWriterClassV1
                                                        .MANIFEST_SELECTOR_GENERATION_REPRESENTATION_V1);
                    } catch (RuntimeException notTarget) {
                        return false;
                    }
                });
                var publication = task.publish(executor);
                try {
                    assertThat(fault.heldApplied.await(30, TimeUnit.SECONDS)).isTrue();
                    var originalCharge = budget.usage();
                    var closing = source.closeAsync();
                    assertThat(closing).isNotDone();
                    assertThat(budget.usage()).isEqualTo(originalCharge);
                    fault.heldRelease.complete(null);
                    assertThatThrownBy(() -> publication.get(60, TimeUnit.SECONDS))
                            .hasRootCauseMessage("Native original physical publication ticket remains unresolved");
                    closing.get(60, TimeUnit.SECONDS);
                } finally {
                    fault.clear();
                }
                assertThat(exact.read(control.selectorKey())
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS)
                                .orElseThrow())
                        .isEqualTo(beforeSelector);
                assertThat(com.nereusstream.storage.object.materialization.M5MaterializationCodecV1.decodeTask(
                                        exact.read(control.materializationTaskKey(plan.taskIdSha256()))
                                                .toCompletableFuture()
                                                .get(30, TimeUnit.SECONDS)
                                                .orElseThrow()
                                                .canonicalStoredBytes())
                                .state())
                        .isEqualTo(
                                com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.TaskState
                                        .QUARANTINED);
                for (var credit : map.readableOutputs()) {
                    var stored = exact.read(layout.nativeKey(
                                    credit.resource().orElseThrow().authorityKey()))
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow();
                    assertThat(com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                                            stored.canonicalStoredBytes())
                                    .activeWriterTickets())
                            .isEmpty();
                }
                assertThat(budget.usage())
                        .isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(1, 0, 0, 24L * 1024 * 1024));
                control.closeControlWriter().get(30, TimeUnit.SECONDS);
                assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
                String aggregateKey = new OxiaV2AuthorityKeys(binding.authorityRoot())
                        .aggregateKey(binding.authority().aggregate().binding().incarnationIdentity());
                var trim = binding.logicalTrimCoordinator();
                var attempt = Sha256Digest.hash(CanonicalUtf8.fromString("unselected-guarded-native-task-roots")
                        .bytes());
                var scan = trim.beginSourceScan(
                                exact.read(aggregateKey)
                                        .toCompletableFuture()
                                        .get(30, TimeUnit.SECONDS)
                                        .orElseThrow(),
                                attempt)
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS)
                        .exactAuthority()
                        .orElseThrow();
                try {
                    verifyMaterializationRetentionRoots(
                            binding, control, budget, executor, fault, scan, plan, bookKeeper, objectBackend);
                    try (var roots = control.captureMaterializationTaskRoots(scan, budget, executor)
                            .get(30, TimeUnit.SECONDS)) {
                        var actual =
                                com.nereusstream.storage.object.materialization.M5MaterializationCodecV1.decodeTask(
                                        roots.facts()
                                                .get(control.materializationTaskKey(plan.taskIdSha256()))
                                                .canonicalStoredBytes());
                        assertThat(actual.state())
                                .isEqualTo(
                                        com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1
                                                .TaskState.QUARANTINED);
                        assertThat(roots.plans()).contains(plan);
                    }
                } finally {
                    trim.abortSourceScan(scan, attempt).toCompletableFuture().get(30, TimeUnit.SECONDS);
                }
                assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
                System.out.println("NATIVE_PUBLICATION_STOP originalDReplyHeld=true selectorNeverDispatched=true"
                        + " originalTaskQuarantined=true originalTicketsCleared=true"
                        + " taskActorDrained=true wholeM5Authority=false");
                return;
            }
            if (mode == 79) {
                var expected = new java.util.LinkedHashMap<org.apache.bookkeeper.mledger.Position, byte[]>();
                var chain = plan.sourceCut()
                        .coverage()
                        .pulsarCoverage()
                        .orElseThrow()
                        .chain();
                for (var row : chain) {
                    assertThat(map.fullyReplaces(row.ledgerId(), row.entryEndExclusive() - 1))
                            .isFalse();
                    for (long id = 0; id < row.entryEndExclusive(); id++) {
                        var position = PositionFactory.create(row.ledgerId(), id);
                        var entry = readOrdinaryEntry(nativeLedger, position).get(30, TimeUnit.SECONDS);
                        try {
                            expected.put(position, entry.getData());
                        } finally {
                            entry.release();
                        }
                    }
                }
                var published = task.publish(executor).get(60, TimeUnit.SECONDS);
                assertThat(control.readSelectedMaterialization(executor)
                                .get(30, TimeUnit.SECONDS)
                                .readMap()
                                .encode())
                        .isEqualTo(published.readMap().encode());
                source.closeAsync().get(30, TimeUnit.SECONDS);
                control.closeControlWriter().get(30, TimeUnit.SECONDS);
                assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
                budget.closeCell().get(30, TimeUnit.SECONDS);
                closeNativeLedger(nativeLedger).get(30, TimeUnit.SECONDS);
                verifyMappedColdFactoryAfterOriginalRemoval(
                        binding, bookKeeper, offloader, executor, fault, expected, true);
                return;
            }
            if (mode == 65 || mode == 66) {
                var originalInput = source.nativeInput();
                var originalRecovery = source.recoveryInput();
                var ordinaryExpected = new java.util.LinkedHashMap<org.apache.bookkeeper.mledger.Position, byte[]>();
                if (mode == 65) {
                    for (var originalLedger : plan.sourceCut()
                            .coverage()
                            .pulsarCoverage()
                            .orElseThrow()
                            .chain()) {
                        for (long id = 0; id < originalLedger.entryEndExclusive(); id++) {
                            var position = PositionFactory.create(originalLedger.ledgerId(), id);
                            var entry =
                                    readOrdinaryEntry(nativeLedger, position).get(30, TimeUnit.SECONDS);
                            ordinaryExpected.put(position, entry.getData());
                            entry.release();
                        }
                    }
                }
                if (mode == 66) {
                    fault.armCandidate(bytes -> M5BindingAuthorityCodecV1.isAuthorityValue(CanonicalBytes.copyOf(bytes))
                            && M5BindingAuthorityCodecV1.decodeAuthority(CanonicalBytes.copyOf(bytes))
                                            .selectorProjection()
                                            .sourceGeneration()
                                    == plan.sourceCut().predecessorSelector().sourceGeneration() + 1);
                    assertThatThrownBy(() -> task.publish(executor).get(60, TimeUnit.SECONDS))
                            .isInstanceOf(java.util.concurrent.ExecutionException.class);
                    assertThat(fault.putLosses.get()).isEqualTo(1);
                    var retained = budget.usage();
                    assertThatThrownBy(() -> source.closeAsync().get(30, TimeUnit.SECONDS))
                            .isInstanceOf(java.util.concurrent.ExecutionException.class);
                    assertThat(budget.usage()).isEqualTo(retained);
                    assertThat(budget.durableUsage().get(30, TimeUnit.SECONDS)).isEqualTo(retained);
                    var cellClose = budget.closeCell();
                    assertThatThrownBy(() -> cellClose.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
                    fault.clear();
                    source.closeAsync().get(30, TimeUnit.SECONDS);
                    control.closeControlWriter().get(30, TimeUnit.SECONDS);
                    cellClose.get(30, TimeUnit.SECONDS);
                } else {
                    var sourceResources = new java.util.TreeSet<PhysicalResourceIdV2>();
                    var originalView = owner.materializationView();
                    for (var range : source.coverage().ranges()) {
                        for (var slice : originalView.materializationSlices(List.of(range))) {
                            sourceResources.addAll(originalView.physicalDataReferences(slice));
                        }
                    }
                    fault.holdAppliedPut(null, bytes -> {
                        try {
                            return M4ReadControlCodecV1.decodeProtection(CanonicalBytes.copyOf(bytes))
                                            .state()
                                    == ProtectionState.PROTECTED;
                        } catch (RuntimeException otherRecord) {
                            return false;
                        }
                    });
                    var sourcePublication = task.publish(executor);
                    var sourceReply = fault.heldRelease;
                    try {
                        boolean protectionHeld = fault.heldApplied.await(30, TimeUnit.SECONDS);
                        if (!protectionHeld && sourcePublication.isCompletedExceptionally()) {
                            sourcePublication.get(1, TimeUnit.SECONDS);
                        }
                        assertThat(protectionHeld).isTrue();
                        String p1Key = new OxiaV2AuthorityKeys(binding.authorityRoot())
                                .aggregateKey(binding.authority()
                                        .aggregate()
                                        .binding()
                                        .incarnationIdentity());
                        var p1 = exact.read(p1Key)
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS)
                                .orElseThrow();
                        var heldTickets = com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1
                                .decodeAuthority(p1.canonicalStoredBytes())
                                .referenceMutationTickets();
                        assertThat(heldTickets).hasSize(2);
                        assertThat(heldTickets)
                                .filteredOn(ticket -> ticket.referenceKind()
                                        == com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                                .ReferenceKindV1.SOURCE_PROTECTION)
                                .hasSize(1);
                        assertThat(heldTickets)
                                .filteredOn(ticket -> ticket.referenceKind()
                                        == com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                                .ReferenceKindV1.READ_GENERATION_PIN_OR_OPEN_HANDLE)
                                .hasSize(1);
                        var operations = new java.util.HashSet<Sha256Digest>();
                        var roots = new java.util.HashSet<Sha256Digest>();
                        var bkQuota = binding.materializationQuota(bookKeeper).get(30, TimeUnit.SECONDS);
                        for (var resource : sourceResources) {
                            var actual = (resource instanceof PhysicalResourceIdV2.ObjectVersion
                                            ? exact.read(layout.nativeKey(resource.authorityKey()))
                                            : bkQuota.read(resource.authorityKey()))
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS)
                                    .orElseThrow();
                            var sourceAuthority =
                                    com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                                            actual.canonicalStoredBytes());
                            assertThat(sourceAuthority.activeWriterTickets())
                                    .singleElement()
                                    .satisfies(ticket -> {
                                        assertThat(ticket.writerClass())
                                                .isEqualTo(
                                                        com.nereusstream.storage.object.gc
                                                                .M5TargetDeleteAuthorityRecordsV1
                                                                .ProofBoundWriterClassV1
                                                                .M4_SOURCE_PROTECTION_RELEASE_V1);
                                        operations.add(ticket.operationIdSha256());
                                        roots.add(ticket.externalFactsRootSha256());
                                    });
                            var fact =
                                    new com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1(
                                            p1.key(), p1.metadataVersion(), p1.canonicalStoredSha256());
                            var context = new com.nereusstream.storage.object.gc.DeleteObservationContextV2(
                                    1, fact, fact, Optional.empty());
                            assertThatThrownBy(() ->
                                            com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityStateMachineV1
                                                    .prepareIdentityRead(sourceAuthority, plan.taskIdSha256(), context))
                                    .hasMessage("active or unresolved writer ticket vetoes CAS-1");
                        }
                        assertThat(operations).hasSize(1);
                        assertThat(roots).hasSize(1);
                        assertThat(sourcePublication).isNotDone();
                        System.out.println("NATIVE_SOURCE_PROTECTION_ORIGINAL_C_D heldActualSdkReply=true"
                                + " originalMixedSourceTickets=true deleteReadFenceVeto=true wholeM5Authority=false");
                    } catch (Throwable failure) {
                        sourceReply.complete(null);
                        fault.clear();
                        throw failure;
                    }
                    String originalProtectionKey = fault.heldActualKey;
                    var originalProtection = exact.read(originalProtectionKey)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow();
                    var originalSourceCharge = budget.usage();
                    String originalSourceP1Key = new OxiaV2AuthorityKeys(binding.authorityRoot())
                            .aggregateKey(
                                    binding.authority().aggregate().binding().incarnationIdentity());
                    var originalSourceCTickets =
                            com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                            exact.read(originalSourceP1Key)
                                                    .toCompletableFuture()
                                                    .get(30, TimeUnit.SECONDS)
                                                    .orElseThrow()
                                                    .canonicalStoredBytes())
                                    .referenceMutationTickets();
                    assertThat(originalSourceCTickets).hasSize(2);
                    var originalReaderTickets = originalSourceCTickets.stream()
                            .filter(ticket -> ticket.referenceKind()
                                    == com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                            .READ_GENERATION_PIN_OR_OPEN_HANDLE)
                            .toList();
                    assertThat(originalReaderTickets).hasSize(1);
                    assertThat(originalSourceCTickets.stream()
                                    .filter(ticket -> ticket.referenceKind()
                                            == com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                                    .ReferenceKindV1.SOURCE_PROTECTION))
                            .hasSize(1);
                    fault.loseHeldPutReply();
                    int originalSdkAttempts = fault.watchedPutAttempts.get();
                    assertThatThrownBy(() -> sourcePublication.get(60, TimeUnit.SECONDS))
                            .hasRootCauseInstanceOf(TimeoutException.class)
                            .hasRootCauseMessage("actual selector observation unavailable");
                    assertThatThrownBy(() ->
                                    control.reconcileControlWriterCleanup().get(30, TimeUnit.SECONDS))
                            .isInstanceOf(java.util.concurrent.ExecutionException.class);
                    assertThat(budget.usage()).isEqualTo(originalSourceCharge);
                    assertThat(
                                    com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1
                                            .decodeAuthority(exact.read(originalSourceP1Key)
                                                    .toCompletableFuture()
                                                    .get(30, TimeUnit.SECONDS)
                                                    .orElseThrow()
                                                    .canonicalStoredBytes())
                                            .referenceMutationTickets())
                            .containsExactlyElementsOf(originalSourceCTickets);
                    var bkSourceQuota = binding.materializationQuota(bookKeeper).get(30, TimeUnit.SECONDS);
                    for (var resource : sourceResources) {
                        var actual = (resource instanceof PhysicalResourceIdV2.ObjectVersion
                                        ? exact.read(layout.nativeKey(resource.authorityKey()))
                                        : bkSourceQuota.read(resource.authorityKey()))
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS)
                                .orElseThrow();
                        assertThat(com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                                                actual.canonicalStoredBytes())
                                        .activeWriterTickets())
                                .singleElement()
                                .satisfies(ticket -> assertThat(ticket.writerClass())
                                        .isEqualTo(
                                                com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                                        .ProofBoundWriterClassV1.M4_SOURCE_PROTECTION_RELEASE_V1));
                    }
                    fault.clear();
                    control.reconcileControlWriterCleanup().get(30, TimeUnit.SECONDS);
                    assertThat(fault.watchedPutAttempts.get()).isEqualTo(originalSdkAttempts);
                    assertThat(
                                    com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1
                                            .decodeAuthority(exact.read(originalSourceP1Key)
                                                    .toCompletableFuture()
                                                    .get(30, TimeUnit.SECONDS)
                                                    .orElseThrow()
                                                    .canonicalStoredBytes())
                                            .referenceMutationTickets())
                            .containsExactlyElementsOf(originalReaderTickets);
                    assertThat(exact.read(originalProtectionKey)
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS)
                                    .orElseThrow())
                            .isEqualTo(originalProtection);
                    for (var resource : sourceResources) {
                        var actual = (resource instanceof PhysicalResourceIdV2.ObjectVersion
                                        ? exact.read(layout.nativeKey(resource.authorityKey()))
                                        : bkSourceQuota.read(resource.authorityKey()))
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS)
                                .orElseThrow();
                        assertThat(com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                                                actual.canonicalStoredBytes())
                                        .activeWriterTickets())
                                .isEmpty();
                    }
                    System.out.println("NATIVE_SOURCE_PROTECTION_SDK_UNKNOWN originalCandidateVersionPreserved=true"
                            + " originalDTicketsRetained=true metadataOnlyCleanup=true sdkRedispatch=false"
                            + " wholeM5Authority=false");
                    fault.holdAppliedPut(
                            control.selectorKey(),
                            bytes -> M5BindingAuthorityCodecV1.isAuthorityValue(CanonicalBytes.copyOf(bytes))
                                    && M5BindingAuthorityCodecV1.decodeAuthority(CanonicalBytes.copyOf(bytes))
                                                    .selectorProjection()
                                                    .sourceGeneration()
                                            == plan.sourceCut()
                                                            .predecessorSelector()
                                                            .sourceGeneration()
                                                    + 1);
                    var publication = task.publish(executor);
                    com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue
                            appliedSelection;
                    try {
                        assertThat(fault.heldApplied.await(30, TimeUnit.SECONDS))
                                .isTrue();
                        String p1Key = new OxiaV2AuthorityKeys(binding.authorityRoot())
                                .aggregateKey(binding.authority()
                                        .aggregate()
                                        .binding()
                                        .incarnationIdentity());
                        var p1 = exact.read(p1Key)
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS)
                                .orElseThrow();
                        var tickets = com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1
                                .decodeAuthority(p1.canonicalStoredBytes())
                                .referenceMutationTickets();
                        assertThat(tickets).hasSize(1);
                        assertThat(tickets.get(0).referenceKind())
                                .isEqualTo(
                                        com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                                .MANIFEST_SELECTED);
                        assertThat(publication).isNotDone();
                        var operations = new java.util.HashSet<Sha256Digest>();
                        var roots = new java.util.HashSet<Sha256Digest>();
                        for (var credit : map.readableOutputs()) {
                            var resource = credit.resource().orElseThrow();
                            var target = exact.read(layout.nativeKey(resource.authorityKey()))
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS)
                                    .orElseThrow();
                            var physicalTickets =
                                    com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                                                    target.canonicalStoredBytes())
                                            .activeWriterTickets();
                            assertThat(physicalTickets).hasSize(1);
                            var ticket = physicalTickets.get(0);
                            assertThat(ticket.writerClass())
                                    .isEqualTo(
                                            com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                                    .ProofBoundWriterClassV1
                                                    .MANIFEST_SELECTOR_GENERATION_REPRESENTATION_V1);
                            assertThat(ticket.capabilitySha256())
                                    .isEqualTo(inventory.profile().providerConfigurationSha256());
                            operations.add(ticket.operationIdSha256());
                            roots.add(ticket.externalFactsRootSha256());
                        }
                        assertThat(operations).hasSize(1);
                        assertThat(roots).hasSize(1);
                        appliedSelection = exact.read(control.selectorKey())
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS)
                                .orElseThrow();
                        var originalCharge = budget.usage();
                        int losses = fault.putLosses.get();
                        fault.armCandidate(
                                bytes -> {
                                    try {
                                        var value = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1
                                                .decodeAuthority(CanonicalBytes.copyOf(bytes));
                                        return map.readableOutputs().stream().anyMatch(credit -> credit.resource()
                                                        .orElseThrow()
                                                        .equals(value.target().resourceId()))
                                                && value.activeWriterTickets().isEmpty();
                                    } catch (RuntimeException notTarget) {
                                        return false;
                                    }
                                });
                        fault.heldRelease.complete(null);
                        assertThatThrownBy(() -> publication.get(60, TimeUnit.SECONDS))
                                .hasRootCauseMessage("Native original physical publication ticket remains unresolved");
                        assertThat(fault.putLosses.get()).isEqualTo(losses + 1);
                        assertThat(budget.usage()).isEqualTo(originalCharge);
                    } finally {
                        fault.clear();
                    }
                    var published = task.publish(executor).get(60, TimeUnit.SECONDS);
                    for (var credit : map.readableOutputs()) {
                        var target = exact.read(layout.nativeKey(
                                        credit.resource().orElseThrow().authorityKey()))
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS)
                                .orElseThrow();
                        assertThat(com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                                                target.canonicalStoredBytes())
                                        .activeWriterTickets())
                                .isEmpty();
                    }
                    var selection = exact.read(control.selectorKey())
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow();
                    assertThat(selection).isEqualTo(appliedSelection);
                    assertThat(selection).isEqualTo(published.selectorFact());
                    var selected = M5BindingAuthorityCodecV1.decodeAuthority(selection.canonicalStoredBytes())
                            .selectorProjection();
                    assertThat(selected).isEqualTo(published.selector());
                    var validationSha =
                            com.nereusstream.storage.object.materialization.M5MaterializationCodecV1
                                    .validationRootSha256(published.validated().validationRoot());
                    var generationSha =
                            com.nereusstream.storage.object.materialization.M5MaterializationCodecV1.generationSha256(
                                    published.validated().generation());
                    var manifestSha =
                            com.nereusstream.storage.object.materialization.M5MaterializationCodecV1.manifestSha256(
                                    published.validated().manifestView());
                    originalPublishedManifest = manifestSha;
                    assertThat(selected.selectedViewSha256()).isEqualTo(manifestSha);
                    assertThat(selected.readAdmissionEpoch())
                            .isEqualTo(plan.sourceCut().predecessorSelector().readAdmissionEpoch() + 1);
                    assertThat(selected.sourceGeneration())
                            .isEqualTo(plan.sourceCut().predecessorSelector().sourceGeneration() + 1);
                    assertThat(exact.read(control.materializationValidationKey(validationSha))
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS)
                                    .orElseThrow()
                                    .canonicalStoredBytes())
                            .isEqualTo(
                                    com.nereusstream.storage.object.materialization.M5MaterializationCodecV1
                                            .encodeValidationRoot(
                                                    published.validated().validationRoot()));
                    assertThat(exact.read(control.materializationGenerationKey(generationSha))
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS)
                                    .orElseThrow()
                                    .canonicalStoredBytes())
                            .isEqualTo(
                                    com.nereusstream.storage.object.materialization.M5MaterializationCodecV1
                                            .encodeGeneration(
                                                    published.validated().generation()));
                    assertThat(exact.read(control.materializationManifestKey(manifestSha))
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS)
                                    .orElseThrow()
                                    .canonicalStoredBytes())
                            .isEqualTo(
                                    com.nereusstream.storage.object.materialization.M5MaterializationCodecV1
                                            .encodeManifest(
                                                    published.validated().manifestView()));
                    for (var protectedSource : published.protectedSources()) {
                        var protection =
                                M4ReadControlCodecV1.decodeProtection(exact.read(control.protectionKey(protectedSource))
                                        .toCompletableFuture()
                                        .get(30, TimeUnit.SECONDS)
                                        .orElseThrow()
                                        .canonicalStoredBytes());
                        assertThat(protection.identity()).isEqualTo(protectedSource);
                        assertThat(protection.state()).isEqualTo(ProtectionState.PROTECTED);
                        assertThat(plan.sourceCut().sources().stream()
                                        .map(value -> value.sourceIdentitySha256())
                                        .toList())
                                .contains(protectedSource.sourceIdentitySha256());
                    }
                    assertThat(owner.authority().admitting()).isFalse();
                    assertThat(published.readMap().outputs()).containsExactlyElementsOf(assigned);
                    // A new control instance reconstructs from the actual selected metadata, without the Task map.
                    var restoredControl = binding.m4Control();
                    var restored = restoredControl
                            .readSelectedMaterialization(executor)
                            .get(30, TimeUnit.SECONDS);
                    assertThat(restored.selector()).isEqualTo(selected);
                    assertThat(restored.selectorFact()).isEqualTo(selection);
                    assertThat(restored.readMap().encode())
                            .isEqualTo(published.readMap().encode());
                    assertThat(restored.nativeInput()).isEqualTo(originalInput);
                    assertThat(restored.recoveryInput()).isEqualTo(originalRecovery);
                    assertThat(Sha256Digest.hash(restored.providerProfile().encode()))
                            .isEqualTo(plan.encryptionGenerationSha256());

                    // Same-byte actual selector CAS still changes the native version during the held manifest GET.
                    fault.holdOneGet(control.materializationManifestKey(manifestSha));
                    var changing = restoredControl.readSelectedMaterialization(executor);
                    assertThat(fault.heldReadEntered.await(30, TimeUnit.SECONDS))
                            .isTrue();
                    try {
                        exact.compareAndSet(Optional.of(selection), selection.key(), selection.canonicalStoredBytes())
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS);
                    } finally {
                        fault.heldReadRelease.complete(null);
                    }
                    assertThatThrownBy(() -> changing.get(30, TimeUnit.SECONDS))
                            .hasRootCauseMessage(
                                    "Native selected materialization native version changed during capture");
                    fault.clear();
                    var current = restoredControl
                            .readSelectedMaterialization(executor)
                            .get(30, TimeUnit.SECONDS);
                    assertThat(current.readMap().encode())
                            .isEqualTo(published.readMap().encode());
                    assertThat(current.selectorFact().canonicalStoredBytes())
                            .isEqualTo(selection.canonicalStoredBytes());
                    assertThat(current.selectorFact().metadataVersion()).isNotEqualTo(selection.metadataVersion());
                    source.closeAsync().get(30, TimeUnit.SECONDS);
                    verifyOrdinaryMaterializedReads(
                            binding,
                            control,
                            nativeLedger,
                            bookKeeper,
                            offloader,
                            budget,
                            executor,
                            ordinaryExpected,
                            plan,
                            fault);
                    control.closeControlWriter().get(30, TimeUnit.SECONDS);
                    assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
                    budget.closeCell().get(30, TimeUnit.SECONDS);
                    // The preceding contract test closed the original allocator. Admit a real successor owner
                    // against the same durable profile and its confirmed drain before the read-only floor scan.
                    var floorBudget = readBudget(binding, 6);
                    try {
                        verifyActualGenerationFloor(
                                binding,
                                control,
                                floorBudget,
                                executor,
                                fault,
                                bookKeeper,
                                nativeLedger.getConfig(),
                                objectBackend);
                    } finally {
                        floorBudget.closeCell().get(30, TimeUnit.SECONDS);
                    }
                    verifyMappedColdFactoryAfterOriginalRemoval(
                            binding, bookKeeper, offloader, executor, fault, ordinaryExpected, false);
                    budget.closeCell().get(30, TimeUnit.SECONDS);
                }
                var storedTask = com.nereusstream.storage.object.materialization.M5MaterializationCodecV1.decodeTask(
                        exact.read(control.materializationTaskKey(plan.taskIdSha256()))
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS)
                                .orElseThrow()
                                .canonicalStoredBytes());
                assertThat(storedTask.state())
                        .isEqualTo(
                                com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.TaskState
                                        .PUBLISHED);
                var selected = readNativeSelector(metadata, control.selectorKey());
                if (mode == 66) {
                    assertThat(storedTask.manifestViewSha256()).contains(selected.selectedViewSha256());
                } else {
                    assertThat(storedTask.manifestViewSha256()).contains(originalPublishedManifest);
                    assertThat(control.readSelectedMaterialization(executor)
                                    .get(30, TimeUnit.SECONDS)
                                    .readMap()
                                    .plan()
                                    .taskIdSha256())
                            .isNotEqualTo(plan.taskIdSha256());
                }
                // Independent actual predecessor drain survives publication; it is not a source RELEASED receipt.
                var drained = exact.read(control.ownerDrainKey(plan.sourceCut().predecessorSelector()))
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS)
                        .orElseThrow();
                assertThat(drained.key())
                        .isEqualTo(control.ownerDrainKey(plan.sourceCut().predecessorSelector()));
                assertThat(drained.canonicalStoredBytes().isEmpty()).isFalse();
                if (mode == 66) {
                    assertThat(exact.read(control.ownerDrainKey(selected))
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS))
                            .isEmpty();
                }
                assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
                for (var descriptor : descriptors) {
                    String version = new String(
                            descriptor
                                    .immutableProviderVersionToken()
                                    .orElseThrow()
                                    .toByteArray(),
                            StandardCharsets.UTF_8);
                    assertThat(independent
                                    .headObject(request -> request.bucket(bucket)
                                            .key(descriptor.identity().key())
                                            .versionId(version))
                                    .versionId())
                            .isEqualTo(version);
                }
                System.out.println("NATIVE_PULSAR_ACTUAL_MANIFEST_PUBLICATION actualIndependentOutputs=true "
                        + "exactOriginalSourceProtections=true originalOwnerDrained=true nativeSelectorVersion=true "
                        + "sameUnknownCandidateHandoff=" + (mode == 66)
                        + "physicalOutputPreserved=true ordinaryMappedReaderInstalled=false wholeM5Authority=false");
                return;
            }
            if (mode == 64) {
                var hold = hold(verified.payloads().get(0).identity().key());
                heldMappedGet.set(hold);
                var firstRange = source.coverage().ranges().get(0);
                var position = new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                        firstRange.ledgerId(), firstRange.startEntryInclusive());
                var reading = task.readMappedEntry(position, 8L * 1024 * 1024, executor);
                try {
                    assertThat(hold.started().await(30, TimeUnit.SECONDS)).isTrue();
                    assertThat(hold.key().get())
                            .isEqualTo(verified.payloads().get(0).identity().key());
                    assertThat(reading.cancel(false)).isTrue();
                    var retained = budget.usage();
                    var closing = source.closeAsync();
                    var fencing = owner.fence();
                    var cellClose = budget.closeCell();
                    assertThatThrownBy(() -> closing.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
                    assertThat(fencing).isNotDone();
                    assertThat(cellClose).isNotDone();
                    assertThat(budget.usage()).isEqualTo(retained);
                    assertThat(budget.durableUsage().get(30, TimeUnit.SECONDS)).isEqualTo(retained);
                    hold.released().countDown();
                    closing.get(30, TimeUnit.SECONDS);
                    fencing.get(30, TimeUnit.SECONDS);
                    cellClose.get(30, TimeUnit.SECONDS);
                    assertThat(budget.usage().owners()).isZero();
                    assertThat(independent
                                    .headObject(request -> request.bucket(bucket)
                                            .key(hold.key().get())
                                            .versionId(hold.version().get()))
                                    .versionId())
                            .isEqualTo(hold.version().get());
                    System.out.println("NATIVE_PULSAR_MAPPED_GET_DRAIN actualSameVersionFullGetHeld=true "
                            + "observerCancelled=true sourceOwnerHazardCellAndWorkspaceRetained=true "
                            + "originalSdkDrainBeforeRefund=true physicalOutputPreserved=true wholeM5Authority=false");
                    return;
                } finally {
                    hold.released().countDown();
                }
            }
            for (int part = 0; part < plan.outputParts().size(); part++) {
                var selected = plan.outputParts()
                        .get(part)
                        .coverage()
                        .pulsarCoverage()
                        .orElseThrow()
                        .ranges()
                        .get(0);
                for (long entry = selected.startEntryInclusive(); entry < selected.endEntryExclusive(); entry++) {
                    var route = decodedMap.locate(
                            new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                                    selected.ledgerId(), entry));
                    assertThat(route.rewrittenPayload())
                            .isEqualTo(assigned.get(part).resource());
                    assertThat(route.retainedNativeSource()).isEmpty();
                    if (mode == 63) {
                        var beforeRead = budget.usage();
                        var position = new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                                selected.ledgerId(), entry);
                        var actual = task.readMappedEntry(position, 8L * 1024 * 1024, executor)
                                .get(30, TimeUnit.SECONDS);
                        assertThat(actual.position()).isEqualTo(position);
                        assertThat(actual.payload().toByteArray())
                                .containsExactly(payload(Math.toIntExact(part * 3 + entry)));
                        assertThat(actual.timestamp()).isEqualTo(1_791_177_600_000L + part * 3 + entry);
                        assertThat(budget.usage()).isEqualTo(beforeRead);
                    }
                }
            }
            var chain = source.coverage().chain();
            for (var ledger : chain) {
                assertThatThrownBy(() -> decodedMap.locate(
                                new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                                        ledger.ledgerId(), ledger.entryEndExclusive())))
                        .hasMessageContaining("captured Native frontier");
            }
            if (mode == 63) {
                for (var position : List.of(
                        new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                                chain.get(0).ledgerId(), 0),
                        new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                                chain.get(1).ledgerId(), 2),
                        new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                                chain.get(2).ledgerId(), 0))) {
                    var route = decodedMap.locate(position);
                    assertThat(route.rewrittenPayload()).isEmpty();
                    assertThat(Sha256Digest.hash(route.retainedNativeSource().orElseThrow()))
                            .isEqualTo(route.originalSourceSha256());
                    int ordinal = chain.stream()
                            .map(value -> value.ledgerId())
                            .toList()
                            .indexOf(position.ledgerId());
                    var actual = task.readMappedEntry(position, 8L * 1024 * 1024, executor)
                            .get(30, TimeUnit.SECONDS);
                    int originalEntry = Math.toIntExact(ordinal * 3 + position.entryId());
                    assertThat(actual.payload().toByteArray()).containsExactly(payload(originalEntry));
                    assertThat(actual.timestamp()).isEqualTo(1_791_177_600_000L + originalEntry);
                }
                assertThatThrownBy(() -> decodedMap.locate(
                                new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                                        Long.MAX_VALUE, 0)))
                        .hasMessageContaining("outside the complete Native map");
                var trailing = java.util.Arrays.copyOf(
                        map.encode().toByteArray(), map.encode().length() + 1);
                assertThatThrownBy(() -> PulsarNativeMaterializationReadMapV2.decode(CanonicalBytes.copyOf(trailing)))
                        .hasMessageContaining("trailing bytes");
            }
            if (mode == 62) {
                assertThat(put.started().getCount()).isZero();
                assertThat(descriptors.stream()
                                .filter(value ->
                                        value.identity().key().equals(put.key().get()))
                                .findFirst()
                                .orElseThrow()
                                .immutableProviderVersionToken())
                        .contains(CanonicalBytes.copyOf(put.version().get().getBytes(StandardCharsets.UTF_8)));
            }
            source.closeAsync().get(30, TimeUnit.SECONDS);
            var terminal = metadata.get(control.materializationTaskKey(plan.taskIdSha256()))
                    .get(30, TimeUnit.SECONDS);
            assertThat(com.nereusstream.storage.object.materialization.M5MaterializationCodecV1.decodeTask(
                                    CanonicalBytes.copyOf(terminal.value()))
                            .state())
                    .isEqualTo(
                            com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.TaskState
                                    .QUARANTINED);
            assertThat(com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2.decode(
                            exact.read(control.materializationPendingOutputsKey(plan.taskIdSha256()))
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS)
                                    .orElseThrow()
                                    .canonicalStoredBytes()))
                    .isEqualTo(inventory);
            var head = com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Head.decode(
                    exact.read(layout.nativeKey(com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.HEAD_KEY))
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow()
                            .canonicalStoredBytes());
            assertThat(head.reservedResources()).isEqualTo(4);
            assertThat(head.births().orElseThrow().assigned()).isEqualTo(4);
            assertThat(head.births().orElseThrow().unassigned()).isZero();
            System.out.println("NATIVE_PULSAR_REAL_OUTPUT actualSdkImmutableVersions=true independentFullGet=true "
                    + "typedTwoLedgerEntryBytesPositionsTimestamps=true entryCount="
                    + source.coverage().entryCount()
                    + " completeNativeReadMap=true partialCutOutsideRoutes=" + (mode == 63)
                    + " realNms1Payloads=2 realIndexes=2 "
                    + "originalQuotaTransferred=true actualPutReplyLoss=" + (mode == 62)
                    + "unpublishedQuarantinePreservesOutputs=true publicationAuthority=false wholeM5Authority=false");
        } finally {
            if (put != null) {
                put.released().countDown();
            }
            fault.clear();
            source.closeAsync().get(30, TimeUnit.SECONDS);
            try (var paths = Files.walk(staging)) {
                for (var owned :
                        paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                    Files.delete(owned);
                }
            }
        }
    }

    private static void verifyActualProviderScopeRegistry(
            PulsarNativeBindingV1 binding, S3PulsarOffloadObjectStoreV1.MaterializationConnection connection)
            throws Exception {
        try (var independent = OxiaClientBuilder.create(System.getProperty("nereus.m5.nativeOffloadOxia"))
                .namespace("default")
                .asyncClient()
                .get(30, TimeUnit.SECONDS)) {
            var namespace = OxiaPhysicalMetadataNamespaceV2.connect(independent, binding.metadataNamespace())
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS);
            var registry = namespace
                    .openCellProviderScopes(connection.physical())
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS);
            assertThat(registry.key()).startsWith(connection.binding().authorityRoot() + "/");
            assertThat(registry.prepare(connection.profile())
                            .admit()
                            .get(30, TimeUnit.SECONDS)
                            .profile())
                    .isEqualTo(connection.profile());
            var exact =
                    new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(independent);
            var stored = exact.read(registry.key())
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            var decoded = com.nereusstream.metadata.oxia.v2.retention.OxiaCellProviderScopeRegistryV2.Catalogue.decode(
                    stored.canonicalStoredBytes());
            assertThat(decoded.profiles()).containsExactly(connection.profile());
            assertThat(new String(stored.canonicalStoredBytes().toByteArray(), StandardCharsets.ISO_8859_1))
                    .doesNotContain("nereus-m5-drain-secret");
            var peer = new PulsarProtocolCellIdentity(
                    new DeploymentId(new Id128(1, 2)),
                    new ReservationDomainId(new Id128(3, 4)),
                    new PulsarCellId(new Id128(75, 76)));
            var profile = connection.profile();
            var foreign = new com.nereusstream.storage.object.materialization.M5CellProviderProfileV2(
                    profile.physicalBinding(),
                    peer,
                    profile.exclusiveNamespacePrefix(),
                    profile.references(),
                    profile.adapterVersion(),
                    profile.providerConfigurationSha256(),
                    profile.maximumObjectBytes(),
                    profile.maximumRangeBytes(),
                    profile.maximumListPageKeys(),
                    profile.maximumVersionTokenBytes());
            assertThatThrownBy(() -> registry.prepare(foreign).admit().get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("Cell Provider catalogue has duplicate Cell or overlapping prefix");
            assertThat(exact.read(registry.key()).toCompletableFuture().get(30, TimeUnit.SECONDS))
                    .isEqualTo(Optional.of(stored));
        }
    }

    private static void verifyPersistedMaterializationInput(
            PulsarNativeMaterializationSourceV2 source, PulsarNativeM4ControlV1 control, PulsarNativeBindingV1 binding)
            throws Exception {
        var input = source.nativeInput();
        try (var independent = OxiaClientBuilder.create(System.getProperty("nereus.m5.nativeOffloadOxia"))
                .namespace("default")
                .clientIdentifier("native-materialization-input-independent")
                .asyncClient()
                .get(30, TimeUnit.SECONDS)) {
            var exact =
                    new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(independent);
            var stored = exact.read(control.materializationInputKey(input.protocolStateRootSha256()))
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            var recovered = com.nereusstream.metadata.oxia.v2.retention.OxiaPulsarMaterializationInputV2.decode(
                    stored.canonicalStoredBytes());
            assertThat(recovered).isEqualTo(input);
            var view = exact.read(recovered.sourceViewKey())
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            assertThat(recovered.verifySourceView(view)).isEqualTo(source.recoveryInput());
            assertThat(recovered.coverage()).isEqualTo(source.coverage());
            assertThat(recovered.storageFence())
                    .isEqualTo(Math.addExact(
                            binding.authority().aggregate().initialEpoch().epochOrdinal(), 1));
            var aggregate = exact.read(recovered.aggregateKey())
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            var selector = exact.read(recovered.selectorKey())
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            recovered.requireCurrentP1(aggregate, selector);
            assertThat(exact.compareAndSet(Optional.of(selector), selector.key(), selector.canonicalStoredBytes())
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS))
                    .isEqualTo(
                            com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome
                                    .APPLIED_EXACT);
            var rewritten = exact.read(selector.key())
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            assertThat(rewritten.metadataVersion()).isNotEqualTo(selector.metadataVersion());
            assertThatThrownBy(() -> recovered.requireCurrentP1(aggregate, rewritten))
                    .hasMessageContaining("P1 key/version/value changed");
            assertThatThrownBy(() -> source.verifyPulsarSource(source.sources().get(0)))
                    .hasMessageContaining("qualification remains unresolved");
        }
        System.out.println("NATIVE_PULSAR_MATERIALIZATION_INPUT actualP1KeyVersionValue=true exactStoredInput=true "
                + "independentOriginalViewRecoveryRoot=true sameValueNewP1VersionRejected=true "
                + "nativeTaskOutputPublicationAuthority=false wholeM5Authority=false");
    }

    private void verifyMaterializationConnectionDrain(
            PulsarNativeBindingV1 binding,
            PulsarNativeBindingV1.CapturedSource original,
            NereusPulsarLedgerOffloaderV1 offloader,
            S3PulsarOffloadObjectStoreV1 store,
            com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace namespace,
            AtomicReference<SdkHold> heldGet)
            throws Exception {
        var scope = new CellProviderScopeId(Sha256Digest.hash(
                CanonicalBytes.copyOf("native-sdk-connection-dependency".getBytes(StandardCharsets.UTF_8))));
        var connection = offloader.borrowMaterializationConnection(
                namespace, binding.metadataNamespace(), scope, 64L * 1024 * 1024);
        var executor = Executors.newSingleThreadExecutor();
        var hold = hold();
        var data = original.objects().data();
        var identity = new com.nereusstream.storage.object.provider.ObjectIdentity(
                data.key(),
                data.length(),
                Sha256Digest.copyOf(java.util.HexFormat.of().parseHex(data.sha256())));
        var provider = connection.provider();
        heldGet.set(hold);
        var reading = CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return provider.consumeVerifiedObject(
                                identity,
                                Optional.of(CanonicalBytes.copyOf(
                                        data.version().substring(8).getBytes(StandardCharsets.UTF_8))),
                                input -> {
                                    byte[] buffer = new byte[65_536];
                                    long bytes = 0;
                                    for (int count = input.read(buffer); count != -1; count = input.read(buffer)) {
                                        bytes = Math.addExact(bytes, count);
                                    }
                                    return bytes;
                                });
                    } catch (java.io.IOException failure) {
                        throw new java.util.concurrent.CompletionException(failure);
                    }
                },
                executor);
        try {
            assertThat(hold.started().await(30, TimeUnit.SECONDS)).isTrue();
            var offloaderClose = offloader.closeAsync();
            var storeClose = store.close().toCompletableFuture();
            assertThatThrownBy(connection::close).hasMessageContaining("accepted operations");
            assertThatThrownBy(() -> offloaderClose.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(java.util.concurrent.TimeoutException.class);
            assertThat(storeClose).isNotDone();
            hold.released().countDown();
            assertThat(reading.get(30, TimeUnit.SECONDS).value()).isEqualTo(data.length());
            assertThat(provider.acceptedOperations()).isZero();
            connection.close();
            connection.close();
            offloaderClose.get(30, TimeUnit.SECONDS);
            storeClose.get(30, TimeUnit.SECONDS);
            assertThatThrownBy(connection::provider).hasMessageContaining("released");
            System.out.println("NATIVE_MATERIALIZATION_CONNECTION originalS3Client=true originalGetHeld=true "
                    + "offloaderAndStoreCloseWaitForLoan=true exactGetDrain=true noWholeM5Authority=true");
        } finally {
            hold.released().countDown();
            reading.handle((ignored, failure) -> null).get(30, TimeUnit.SECONDS);
            connection.close();
            executor.shutdown();
            assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static void verifyNativeMetadataTicketClearUnknown(
            PulsarNativeBindingV1 binding,
            PulsarNativeReadCellBudgetV1 budget,
            PulsarNativeMetadataWriterV2 writer,
            org.apache.bookkeeper.mledger.impl.MetaStoreImpl nativeStore,
            MetadataStoreExtended metadata,
            SelectorResponseFault responses,
            String aggregateKey,
            AtomicInteger nativePuts)
            throws Exception {
        String name = binding.managedLedgerName();
        var original = metadata.get("/managed-ledgers/" + name)
                .get(30, TimeUnit.SECONDS)
                .orElseThrow();
        var info = nativeStore.parseManagedLedgerInfo(original.getValue());
        int before = nativePuts.get();
        var predecessor = nativeStore
                .readStoredMutationValue(name, "/managed-ledgers/" + name, 8_388_608)
                .get(30, TimeUnit.SECONDS)
                .orElseThrow();
        assertThat(predecessor.storedBytes()).isEqualTo(original.getValue());
        assertThat(predecessor.stat().getVersion()).isEqualTo(original.getStat().getVersion());
        assertThat(predecessor.stat().getCreationTimestamp())
                .isEqualTo(original.getStat().getCreationTimestamp());
        var modifiedCopy = predecessor.storedBytes();
        java.util.Arrays.fill(modifiedCopy, (byte) 0);
        assertThat(predecessor.storedBytes()).isEqualTo(original.getValue());
        assertThatThrownBy(() -> nativeStore
                        .readStoredMutationValue(name, "/managed-ledgers/foreign", 8_388_608)
                        .get(30, TimeUnit.SECONDS))
                .hasRootCauseMessage("metadata predecessor scope/bound differs");
        assertThat(original.getValue().length).isGreaterThan(1);
        assertThatThrownBy(() -> nativeStore
                        .readStoredMutationValue(name, "/managed-ledgers/" + name, original.getValue().length - 1)
                        .get(30, TimeUnit.SECONDS))
                .hasRootCauseMessage("actual metadata predecessor exceeds its scope/bound");
        assertThat(nativePuts.get()).isEqualTo(before);
        var charge = budget.usage();
        responses.armCandidate(
                value -> {
                    try {
                        var actual = com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1
                                .decodeAuthority(CanonicalBytes.copyOf(value));
                        return actual.referenceMutationTickets().isEmpty()
                                && actual.writerEnrollment().isPresent();
                    } catch (RuntimeException notAuthority) {
                        return false;
                    }
                });
        var result = new CompletableFuture<Void>();
        nativeStore.asyncUpdateLedgerIds(
                name, info, original.getStat(), new org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback<>() {
                    @Override
                    public void operationComplete(Void value, org.apache.pulsar.metadata.api.Stat stat) {
                        result.complete(null);
                    }

                    @Override
                    public void operationFailed(
                            org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException failure) {
                        result.completeExceptionally(failure);
                    }
                });
        assertThatThrownBy(() -> result.get(30, TimeUnit.SECONDS))
                .hasRootCauseMessage("original Native metadata ticket is unresolved: path=/managed-ledgers/" + name
                        + " dispatched=true outcome=RETAINED_TICKET_STATE_UNKNOWN");
        assertThat(nativePuts.get()).isEqualTo(before + 1);
        assertThat(budget.usage()).isEqualTo(charge);
        assertThatThrownBy(() -> writer.reconcileCleanup().get(30, TimeUnit.SECONDS))
                .isInstanceOf(java.util.concurrent.ExecutionException.class);
        assertThat(budget.usage()).isEqualTo(charge);
        responses.clear();
        writer.reconcileCleanup().get(30, TimeUnit.SECONDS);
        assertThat(nativePuts.get()).isEqualTo(before + 1);
        assertThat(result).isCompletedExceptionally();
        var actual = metadata.get("/managed-ledgers/" + name)
                .get(30, TimeUnit.SECONDS)
                .orElseThrow();
        assertThat(actual.getValue()).isEqualTo(original.getValue());
        assertThat(actual.getStat().getVersion())
                .isGreaterThan(original.getStat().getVersion());
        var exact = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(responses);
        var authority = exact.read(aggregateKey).toCompletableFuture().join().orElseThrow();
        assertThat(com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                authority.canonicalStoredBytes())
                        .referenceMutationTickets())
                .isEmpty();
        assertThat(budget.usage()).isEqualTo(charge); // The admitted factory remains live after this operation cleanup.
        System.out.println("NATIVE_PULSAR_METADATA_TICKET_UNKNOWN actualNativeAck=true lostActualOxiaClearReply=true "
                + "originalLifetimeRetained=true metadataOnlyReconcile=true nativePutCount=1 wholeM5Authority=false");
    }

    private static void verifyNativeMetadataUndispatchedCleanup(
            PulsarNativeBindingV1 binding,
            PulsarNativeReadCellBudgetV1 budget,
            PulsarNativeMetadataWriterV2 writer,
            org.apache.bookkeeper.mledger.impl.MetaStoreImpl nativeStore,
            MetadataStoreExtended metadata,
            SelectorResponseFault responses,
            String aggregateKey,
            AtomicInteger nativePuts)
            throws Exception {
        String name = binding.managedLedgerName();
        var independent = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(
                responses.delegate);
        for (boolean delayed : new boolean[] {false, true}) {
            var nativeBefore = metadata.get("/managed-ledgers/" + name)
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            int putsBefore = nativePuts.get();
            var charge = budget.usage();
            Predicate<byte[]> hasTicket = value -> {
                try {
                    return !com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                    CanonicalBytes.copyOf(value))
                            .referenceMutationTickets()
                            .isEmpty();
                } catch (RuntimeException notAuthority) {
                    return false;
                }
            };
            if (delayed) {
                responses.delayUnknownPut(aggregateKey, hasTicket);
            } else {
                responses.arm(aggregateKey, hasTicket);
            }
            var result = new CompletableFuture<Void>();
            nativeStore.asyncUpdateLedgerIds(
                    name,
                    nativeStore.parseManagedLedgerInfo(nativeBefore.getValue()),
                    nativeBefore.getStat(),
                    new org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback<>() {
                        @Override
                        public void operationComplete(Void value, org.apache.pulsar.metadata.api.Stat stat) {
                            result.complete(null);
                        }

                        @Override
                        public void operationFailed(
                                org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException failure) {
                            result.completeExceptionally(failure);
                        }
                    });
            assertThatThrownBy(() -> result.get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("original Native metadata ticket is unresolved: path=/managed-ledgers/" + name
                            + " dispatched=false outcome=RETAINED_TICKET_STATE_UNKNOWN");
            assertThat(nativePuts.get()).isEqualTo(putsBefore);
            assertThat(budget.usage()).isEqualTo(charge);
            assertThatThrownBy(() -> writer.reconcileCleanup().get(30, TimeUnit.SECONDS))
                    .isInstanceOf(java.util.concurrent.ExecutionException.class);
            var before =
                    independent.read(aggregateKey).toCompletableFuture().join().orElseThrow();
            assertThat(com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                    before.canonicalStoredBytes())
                            .referenceMutationTickets())
                    .hasSize(delayed ? 0 : 1);
            responses.clear();
            writer.reconcileCleanup().get(30, TimeUnit.SECONDS);
            var after =
                    independent.read(aggregateKey).toCompletableFuture().join().orElseThrow();
            assertThat(after.metadataVersion()).isNotEqualTo(before.metadataVersion());
            assertThat(com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                    after.canonicalStoredBytes())
                            .referenceMutationTickets())
                    .isEmpty();
            if (delayed) {
                assertThatThrownBy(() -> responses.dispatchDelayedPut().get(30, TimeUnit.SECONDS))
                        .hasRootCauseInstanceOf(io.oxia.client.api.exceptions.UnexpectedVersionIdException.class);
                assertThat(independent
                                .read(aggregateKey)
                                .toCompletableFuture()
                                .join()
                                .orElseThrow())
                        .isEqualTo(after);
            }
            var nativeAfter = metadata.get("/managed-ledgers/" + name)
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            assertThat(nativeAfter.getValue()).isEqualTo(nativeBefore.getValue());
            assertThat(nativeAfter.getStat().getVersion())
                    .isEqualTo(nativeBefore.getStat().getVersion());
            assertThat(nativePuts.get()).isEqualTo(putsBefore);
            assertThat(result).isCompletedExceptionally();
            assertThat(budget.usage()).isEqualTo(charge);
        }
        System.out.println("NATIVE_PULSAR_METADATA_UNDISPATCHED actualAppliedAndDelayedOxiaAcquire=true "
                + "absentTicketVersionFenced=true lateOriginalCasRejected=true nativePutCount=0 "
                + "originalLifetimeRetainedUntilCleanup=true wholeM5Authority=false");
    }

    private static void verifyNativeMetadataDefinitiveRejection(
            PulsarNativeBindingV1 binding,
            PulsarNativeReadCellBudgetV1 budget,
            PulsarNativeMetadataWriterV2 writer,
            org.apache.bookkeeper.mledger.impl.MetaStoreImpl nativeStore,
            MetadataStoreExtended metadata,
            SelectorResponseFault responses,
            String aggregateKey,
            AtomicInteger nativePuts)
            throws Exception {
        String name = binding.managedLedgerName();
        var original = metadata.get("/managed-ledgers/" + name)
                .get(30, TimeUnit.SECONDS)
                .orElseThrow();
        int before = nativePuts.get();
        var charge = budget.usage();
        var stale = new org.apache.pulsar.metadata.api.Stat(
                original.getStat().getPath(), original.getStat().getVersion() - 1,
                original.getStat().getCreationTimestamp(), original.getStat().getModificationTimestamp(),
                original.getStat().isEphemeral(), original.getStat().isCreatedBySelf());
        responses.arm(aggregateKey, value -> {
            try {
                return com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                CanonicalBytes.copyOf(value))
                        .referenceMutationTickets()
                        .isEmpty();
            } catch (RuntimeException notAuthority) {
                return false;
            }
        });
        var result = new CompletableFuture<Void>();
        nativeStore.asyncUpdateLedgerIds(
                name,
                nativeStore.parseManagedLedgerInfo(original.getValue()),
                stale,
                new org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback<>() {
                    @Override
                    public void operationComplete(Void value, org.apache.pulsar.metadata.api.Stat stat) {
                        result.complete(null);
                    }

                    @Override
                    public void operationFailed(
                            org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException failure) {
                        result.completeExceptionally(failure);
                    }
                });
        assertThatThrownBy(() -> result.get(30, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(org.apache.zookeeper.KeeperException.BadVersionException.class);
        assertThat(nativePuts.get()).isEqualTo(before + 1);
        assertThatThrownBy(() -> writer.reconcileCleanup().get(30, TimeUnit.SECONDS))
                .isInstanceOf(java.util.concurrent.ExecutionException.class);
        assertThat(budget.usage()).isEqualTo(charge);
        responses.clear();
        writer.reconcileCleanup().get(30, TimeUnit.SECONDS);
        assertThat(nativePuts.get()).isEqualTo(before + 1);
        assertThat(result).isCompletedExceptionally();
        var actual = metadata.get("/managed-ledgers/" + name)
                .get(30, TimeUnit.SECONDS)
                .orElseThrow();
        assertThat(actual.getValue()).isEqualTo(original.getValue());
        assertThat(actual.getStat().getVersion()).isEqualTo(original.getStat().getVersion());
        var exact = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(responses);
        assertThat(com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                exact.read(aggregateKey)
                                        .toCompletableFuture()
                                        .join()
                                        .orElseThrow()
                                        .canonicalStoredBytes())
                        .referenceMutationTickets())
                .isEmpty();
        assertThat(budget.usage()).isEqualTo(charge);
        System.out.println("NATIVE_PULSAR_METADATA_REJECTED actualZooKeeperBadVersion=true "
                + "originalNotApplied=true lostClearReplyReconciledWithoutPut=true nativePutCount=1 "
                + "wholeM5Authority=false");
    }

    /** Production ownership mapping with independently hashed current implementation sources. */
    private static com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1 nativeExpectedWriterRegistry(
            com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding capability)
            throws Exception {
        var sources = new java.util.HashMap<Class<?>, Sha256Digest>();
        Path root = Path.of("").toAbsolutePath();
        while (root != null && !java.nio.file.Files.isDirectory(root.resolve("nereus-pulsar-offload"))) {
            root = root.getParent();
        }
        if (root == null) {
            throw new IllegalStateException("actual fixture source checkout is absent");
        }
        for (var owner : PulsarNativeWriterRegistryV2.implementationOwners()) {
            String module = owner.getName().contains("metadata.oxia")
                    ? "nereus-metadata-oxia"
                    : owner.getName().contains("storage.object") ? "nereus-storage-object" : "nereus-pulsar-offload";
            var source =
                    root.resolve(module + "/src/main/java/" + owner.getName().replace('.', '/') + ".java");
            sources.put(owner, Sha256Digest.hash(CanonicalBytes.copyOf(java.nio.file.Files.readAllBytes(source))));
        }
        return PulsarNativeWriterRegistryV2.create(capability, sources);
    }

    private static com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1 registryWithMisassignedFallback(
            com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1 original) {
        var declarations = new java.util.ArrayList<
                com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1.WriterDeclarationV1>();
        for (var row : original.declarations()) {
            var references = new java.util.ArrayList<>(row.referenceKinds());
            references.remove(
                    com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1.MANIFEST_FALLBACK);
            if (row.writerClass().equals(PulsarNativeBindingV1.class.getName())) {
                references.add(
                        com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                .MANIFEST_FALLBACK);
            }
            declarations.add(new com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1.WriterDeclarationV1(
                    row.writerClass(),
                    row.capability(),
                    row.floorClasses(),
                    references,
                    row.implementationSourceSha256()));
        }
        return new com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1(declarations);
    }

    private static void verifyNativeCheckpointMetadataTickets(
            PulsarNativeBindingV1 binding,
            PulsarNativeReadCellBudgetV1 budget,
            PulsarNativeMetadataWriterV2 writer,
            PulsarNativeBookKeeperCreateV1 creation,
            MetadataStoreExtended metadata,
            SelectorResponseFault responses,
            AtomicInteger nativePuts,
            long cursorLedger,
            AtomicReference<CompletableFuture<Void>> replyGate,
            AtomicReference<CompletableFuture<org.apache.pulsar.metadata.api.Stat>> stored)
            throws Exception {
        String parent = "/managed-ledgers/" + binding.managedLedgerName();
        String path = parent + "/checkpoint-ticket-observation";
        var resource =
                new PhysicalResourceIdV2.BookKeeperLedger(creation.namespace().namespace(), cursorLedger);
        var cursor = metadata.get(parent + "/guarded-cursor")
                .get(30, TimeUnit.SECONDS)
                .orElseThrow();
        writer.execute(
                        binding.managedLedgerName(),
                        path,
                        cursor.getValue(),
                        Optional.of(-1L),
                        () -> metadata.put(path, cursor.getValue(), Optional.of(-1L)))
                .get(30, TimeUnit.SECONDS);
        var original = metadata.get(path).get(30, TimeUnit.SECONDS).orElseThrow();
        var before = creation.route()
                .read(resource.authorityKey())
                .toCompletableFuture()
                .get(30, TimeUnit.SECONDS)
                .orElseThrow();
        int puts = nativePuts.get();
        var charge = budget.usage();
        var gate = new CompletableFuture<Void>();
        stored.set(new CompletableFuture<>());
        replyGate.set(gate);
        try {
            var observer = writer.execute(
                    binding.managedLedgerName(),
                    path,
                    original.getValue(),
                    Optional.of(original.getStat().getVersion()),
                    () -> metadata.put(
                            path,
                            original.getValue(),
                            Optional.of(original.getStat().getVersion())));
            stored.get().get(30, TimeUnit.SECONDS);
            assertThat(observer.complete(null)).isTrue();
            var held = creation.route()
                    .read(resource.authorityKey())
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            var authority = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                    held.canonicalStoredBytes());
            assertThat(authority.activeWriterTickets()).hasSize(1);
            assertThat(authority.activeWriterTickets().get(0).writerClass())
                    .isEqualTo(
                            com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1
                                    .REFERENCE_SHARED_PHYSICAL_MEMBER_V1);
            var pending = writer.reconcileCleanup();
            assertThat(pending).isNotDone();
            assertThat(budget.usage()).isEqualTo(charge);
            gate.complete(null);
            pending.get(30, TimeUnit.SECONDS);
            assertThat(nativePuts.get()).isEqualTo(puts + 1);
            assertThat(budget.usage()).isEqualTo(charge);
        } finally {
            replyGate.set(null);
            gate.complete(null);
        }
        var fresh = metadata.get(path).get(30, TimeUnit.SECONDS).orElseThrow();
        responses.armCandidate(bytes -> {
            try {
                var value = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                        CanonicalBytes.copyOf(bytes));
                return value.target().resourceId().equals(resource)
                        && value.activeWriterTickets().isEmpty();
            } catch (RuntimeException notTarget) {
                return false;
            }
        });
        try {
            var attempt = writer.execute(
                    binding.managedLedgerName(),
                    path,
                    fresh.getValue(),
                    Optional.of(fresh.getStat().getVersion()),
                    () -> metadata.put(
                            path, fresh.getValue(), Optional.of(fresh.getStat().getVersion())));
            assertThatThrownBy(() -> attempt.get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("original Native metadata ticket is unresolved: path=" + path
                            + " dispatched=true outcome=RETAINED_AMBIGUOUS_EXTERNAL_MUTATION");
            assertThat(nativePuts.get()).isEqualTo(puts + 2);
            assertThat(budget.usage()).isEqualTo(charge);
        } finally {
            responses.clear();
        }
        writer.reconcileCleanup().get(30, TimeUnit.SECONDS);
        assertThat(nativePuts.get()).isEqualTo(puts + 2);
        var cleared = creation.route()
                .read(resource.authorityKey())
                .toCompletableFuture()
                .get(30, TimeUnit.SECONDS)
                .orElseThrow();
        var actual = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                cleared.canonicalStoredBytes());
        assertThat(actual.activeWriterTickets()).isEmpty();
        assertThat(cleared).isNotEqualTo(before);
        assertThat(metadata.get(path).get(30, TimeUnit.SECONDS).orElseThrow().getValue())
                .isEqualTo(original.getValue());
        var compression = new org.apache.bookkeeper.mledger.proto.ManagedCursorInfoMetadata()
                .setCompressionType(org.apache.bookkeeper.mledger.proto.CompressionType.ZSTD)
                .setUncompressedSize(8_388_609)
                .toByteArray();
        var oversized = ByteBuffer.allocate(6 + compression.length)
                .putShort((short) 0x4778)
                .putInt(compression.length)
                .put(compression)
                .array();
        assertThatThrownBy(() -> writer.execute(
                                binding.managedLedgerName(),
                                path,
                                oversized,
                                Optional.empty(),
                                () -> metadata.put(path, oversized, Optional.empty()))
                        .get(30, TimeUnit.SECONDS))
                .hasRootCauseMessage("Native cursor decoded metadata exceeds its admitted bound");
        assertThat(nativePuts.get()).isEqualTo(puts + 2);
        var predecessor = metadata.get(path).get(30, TimeUnit.SECONDS).orElseThrow();
        responses.holdAppliedPut(null, value -> {
            try {
                var authority = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                        CanonicalBytes.copyOf(value));
                return authority.target().resourceId().equals(resource)
                        && !authority.activeWriterTickets().isEmpty();
            } catch (RuntimeException notTarget) {
                return false;
            }
        });
        try {
            var stale = writer.execute(
                    binding.managedLedgerName(),
                    path,
                    predecessor.getValue(),
                    Optional.empty(),
                    () -> metadata.put(path, predecessor.getValue(), Optional.empty()));
            assertThat(responses.heldApplied.await(30, TimeUnit.SECONDS)).isTrue();
            writer.execute(
                            binding.managedLedgerName(),
                            path,
                            predecessor.getValue(),
                            Optional.of(predecessor.getStat().getVersion()),
                            () -> metadata.put(
                                    path,
                                    predecessor.getValue(),
                                    Optional.of(predecessor.getStat().getVersion())))
                    .get(30, TimeUnit.SECONDS);
            responses.heldRelease.complete(null);
            assertThatThrownBy(() -> stale.get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("actual Native metadata predecessor changed before dispatch");
            assertThat(nativePuts.get()).isEqualTo(puts + 3);
            assertThat(budget.usage()).isEqualTo(charge);
        } finally {
            responses.clear();
        }
        writer.reconcileCleanup().get(30, TimeUnit.SECONDS);
        var ending = metadata.get(path).get(30, TimeUnit.SECONDS).orElseThrow();
        writer.execute(
                        binding.managedLedgerName(),
                        path,
                        null,
                        Optional.of(ending.getStat().getVersion()),
                        () -> metadata.delete(path, Optional.of(ending.getStat().getVersion())))
                .get(30, TimeUnit.SECONDS);
        assertThat(metadata.get(path).get(30, TimeUnit.SECONDS)).isEmpty();
    }

    private static CompletableFuture<org.apache.pulsar.metadata.api.Stat> updateOriginalNativeParent(
            org.apache.bookkeeper.mledger.impl.MetaStoreImpl nativeStore,
            String ledgerName,
            org.apache.pulsar.metadata.api.GetResult original)
            throws Exception {
        var result = new CompletableFuture<org.apache.pulsar.metadata.api.Stat>();
        var info = nativeStore.parseManagedLedgerInfo(original.getValue(), 8_388_608);
        nativeStore.asyncUpdateLedgerIds(
                ledgerName,
                info,
                original.getStat(),
                new org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback<Void>() {
                    @Override
                    public void operationComplete(Void ignored, org.apache.pulsar.metadata.api.Stat stat) {
                        result.complete(stat);
                    }

                    @Override
                    public void operationFailed(
                            org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException failure) {
                        result.completeExceptionally(failure);
                    }
                });
        return result;
    }

    private static void verifyNativeMixedReferenceTickets(
            PulsarNativeBindingV1 binding,
            PulsarNativeReadCellBudgetV1 budget,
            PulsarNativeMetadataWriterV2 writer,
            PulsarNativeBookKeeperCreateV1 creation,
            com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2 objectRoute,
            List<PhysicalResourceIdV2> objects,
            org.apache.bookkeeper.mledger.impl.MetaStoreImpl nativeStore,
            MetadataStoreExtended metadata,
            SelectorResponseFault responses,
            AtomicInteger nativePuts,
            AtomicReference<CompletableFuture<Void>> replyGate,
            AtomicReference<CompletableFuture<org.apache.pulsar.metadata.api.Stat>> stored,
            Optional<List<PhysicalResourceIdV2>> resolvedTargets,
            boolean originalSdkApi)
            throws Exception {
        String path = "/managed-ledgers/" + binding.managedLedgerName();
        var original = metadata.get(path).get(30, TimeUnit.SECONDS).orElseThrow();
        var info = nativeStore.parseManagedLedgerInfo(original.getValue(), 8_388_608);
        var targets = new java.util.ArrayList<PhysicalResourceIdV2>(objects);
        for (var source : info.getLedgerInfosList()) {
            if (!source.hasOffloadContext()
                    || !source.getOffloadContext().isComplete()
                    || !source.getOffloadContext().isBookkeeperDeleted()) {
                targets.add(new PhysicalResourceIdV2.BookKeeperLedger(
                        creation.namespace().namespace(), source.getLedgerId()));
            }
        }
        if (resolvedTargets.isPresent()) {
            targets = new java.util.ArrayList<>(resolvedTargets.orElseThrow());
        }
        int before = nativePuts.get();
        var charge = budget.usage();
        var heldCharge = new AtomicReference<PulsarNativeReadCellBudgetV1.Usage>();
        var gate = new CompletableFuture<Void>();
        stored.set(new CompletableFuture<>());
        replyGate.set(gate);
        try {
            var observer = originalSdkApi
                    ? updateOriginalNativeParent(nativeStore, binding.managedLedgerName(), original)
                    : writer.execute(
                            binding.managedLedgerName(),
                            path,
                            original.getValue(),
                            Optional.of(original.getStat().getVersion()),
                            () -> metadata.put(
                                    path,
                                    original.getValue(),
                                    Optional.of(original.getStat().getVersion())));
            stored.get().get(30, TimeUnit.SECONDS);
            heldCharge.set(budget.usage());
            if (originalSdkApi) {
                assertThat(heldCharge.get().owners()).isEqualTo(charge.owners() + 1);
            } else {
                assertThat(heldCharge.get()).isEqualTo(charge);
            }
            assertThat(observer.complete(null)).isTrue();
            var operations = new java.util.HashSet<Sha256Digest>();
            var roots = new java.util.HashSet<Sha256Digest>();
            for (var target : targets) {
                var route = target instanceof PhysicalResourceIdV2.ObjectVersion ? objectRoute : creation.route();
                var fact = route.read(target.authorityKey())
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS)
                        .orElseThrow();
                var authority = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                        fact.canonicalStoredBytes());
                assertThat(authority.activeWriterTickets()).hasSize(1);
                var ticket = authority.activeWriterTickets().get(0);
                operations.add(ticket.operationIdSha256());
                roots.add(ticket.externalFactsRootSha256());
                assertThat(ticket.ownerFenceSha256())
                        .isEqualTo(Sha256Digest.hash(creation.spec().allocatorOwner()));
                if (target instanceof PhysicalResourceIdV2.ObjectVersion) {
                    assertThat(ticket.capabilitySha256())
                            .isNotEqualTo(creation.spec().configuration());
                } else {
                    assertThat(ticket.capabilitySha256())
                            .isEqualTo(creation.spec().configuration());
                }
            }
            assertThat(operations).hasSize(1);
            assertThat(roots).hasSize(1);
            var cleanup = writer.reconcileCleanup();
            assertThat(cleanup).isNotDone();
            assertThat(budget.usage()).isEqualTo(heldCharge.get());
            gate.complete(null);
            cleanup.get(30, TimeUnit.SECONDS);
            assertThat(nativePuts.get()).isEqualTo(before + 1);
        } finally {
            replyGate.set(null);
            gate.complete(null);
        }
        var fresh = metadata.get(path).get(30, TimeUnit.SECONDS).orElseThrow();
        var object = objects.get(0);
        responses.armCandidate(bytes -> {
            try {
                var value = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                        CanonicalBytes.copyOf(bytes));
                return value.target().resourceId().equals(object)
                        && value.activeWriterTickets().isEmpty();
            } catch (RuntimeException other) {
                return false;
            }
        });
        try {
            var attempt = originalSdkApi
                    ? updateOriginalNativeParent(nativeStore, binding.managedLedgerName(), fresh)
                    : writer.execute(
                            binding.managedLedgerName(),
                            path,
                            fresh.getValue(),
                            Optional.of(fresh.getStat().getVersion()),
                            () -> metadata.put(
                                    path,
                                    fresh.getValue(),
                                    Optional.of(fresh.getStat().getVersion())));
            assertThatThrownBy(() -> attempt.get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("original Native metadata ticket is unresolved: path=" + path
                            + " dispatched=true outcome=RETAINED_AMBIGUOUS_EXTERNAL_MUTATION");
            assertThat(nativePuts.get()).isEqualTo(before + 2);
            assertThat(budget.usage()).isEqualTo(heldCharge.get());
        } finally {
            responses.clear();
        }
        writer.reconcileCleanup().get(30, TimeUnit.SECONDS);
        assertThat(nativePuts.get()).isEqualTo(before + 2);
        for (var target : targets) {
            var route = target instanceof PhysicalResourceIdV2.ObjectVersion ? objectRoute : creation.route();
            var fact = route.read(target.authorityKey())
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            assertThat(com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                                    fact.canonicalStoredBytes())
                            .activeWriterTickets())
                    .isEmpty();
        }
        assertThat(budget.usage()).isEqualTo(charge);
    }

    private void verifyNativeGuardedBirths(
            PulsarNativeBindingV1 binding,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            Namespace objectNamespace,
            String bucket,
            String name,
            ExecutorService objectExecutor,
            ExecutorService fileExecutor,
            AtomicInteger reads,
            SelectorResponseFault responseFault,
            boolean loseBirth,
            int metadataCleanupMode)
            throws Exception {
        var control = Executors.newSingleThreadExecutor();
        var budget = readBudget(binding);
        String uri = "zk://" + System.getProperty("nereus.m5.nativeOffloadZk") + "/ledgers";
        var nativeReplies = metadataCleanupMode >= 36
                ? new com.nereusstream.storage.bookkeeper.M5ZooKeeperReadReplyFixtureV1(uri)
                : null;
        PulsarNativeBookKeeperCreateV1 creation = null;
        ManagedLedgerImpl ledger = null;
        try (var metadata = metadata();
                var sourceClient = client(reads)) {
            var providerConfiguration = !loseBirth && metadataCleanupMode == 0
                    ? new S3PulsarOffloadObjectStoreV1.MaterializationConfiguration(
                            new com.nereusstream.storage.object.materialization.M5CellProviderProfileV2.References(
                                    "minio/security/nereus-m5-drain",
                                    "minio/credential/nereus-m5-drain",
                                    "NONE",
                                    "nereus/m5/native-cell-admission",
                                    "nereus/m5/owned-runner",
                                    "nereus/m5/version-match-delete-v1"),
                            sourceClient.serviceClientConfiguration().credentialsProvider())
                    : null;
            var store = new S3PulsarOffloadObjectStoreV1(
                    sourceClient, bucket, objectExecutor, false, providerConfiguration);
            var offloader = offloader(store, fileExecutor);
            ManagedLedgerFactoryImpl guardedFactory = null;
            PulsarNativeMetadataWriterV2 metadataWriter = null;
            Throwable workFailure = null;
            try {
                creation = binding.openGuardedBookKeeper(
                                nativeReplies == null ? uri : nativeReplies.metadataUri(),
                                bootstrap,
                                budget,
                                NATIVE_BK_CANONICAL_CAPACITY,
                                control)
                        .get(30, TimeUnit.SECONDS);
                var owned = creation;
                assertThat(creation.spec().allocatorOwner()).isEqualTo(budget.nativeOwnerIdentity(binding));
                assertThat(com.nereusstream.storage.bookkeeper.M5PulsarNativeCreateSpecV1.decode(
                                creation.spec().encode()))
                        .isEqualTo(creation.spec());
                var nativeMetadataPuts = new AtomicInteger();
                var cursorReplyGate = new AtomicReference<CompletableFuture<Void>>();
                var cursorStored = new AtomicReference<CompletableFuture<org.apache.pulsar.metadata.api.Stat>>();
                var parentReplyGate = new AtomicReference<CompletableFuture<Void>>();
                var parentStored = new AtomicReference<CompletableFuture<org.apache.pulsar.metadata.api.Stat>>();
                var countedMetadata = new FaultInjectionMetadataStore(metadata) {
                    @Override
                    public CompletableFuture<org.apache.pulsar.metadata.api.Stat> put(
                            String path,
                            byte[] value,
                            Optional<Long> expectedVersion,
                            Set<org.apache.pulsar.metadata.api.Option> options) {
                        nativeMetadataPuts.incrementAndGet();
                        return super.put(path, value, expectedVersion, options).thenCompose(stat -> {
                            var parentGate = parentReplyGate.get();
                            if (parentGate != null && path.equals("/managed-ledgers/" + name)) {
                                parentStored.get().complete(stat);
                                return parentGate.thenApply(unused -> stat);
                            }
                            var gate = cursorReplyGate.get();
                            if (gate == null
                                    || !path.equals("/managed-ledgers/" + name + "/checkpoint-ticket-observation")) {
                                return CompletableFuture.completedFuture(stat);
                            }
                            cursorStored.get().complete(stat);
                            return gate.thenApply(unused -> stat);
                        });
                    }
                };
                guardedFactory = factory(countedMetadata, creation.namespace().borrowClient());
                if (!loseBirth && metadataCleanupMode == 0) {
                    var exact = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(
                            responseFault);
                    String aggregateKey = new OxiaV2AuthorityKeys(binding.authorityRoot())
                            .aggregateKey(
                                    binding.authority().aggregate().binding().incarnationIdentity());
                    var original = exact.read(aggregateKey)
                            .toCompletableFuture()
                            .join()
                            .orElseThrow();
                    var capability =
                            new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding(
                                    1, creation.spec().configuration());
                    var coordinator =
                            new com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1(
                                    exact);
                    assertThat(coordinator
                                    .migrateLegacy(new com.nereusstream.storage.object.retention
                                            .M5PulsarAggregateRetirementCoordinatorV1.MigrationRequest(
                                            aggregateKey, original, capability))
                                    .toCompletableFuture()
                                    .join())
                            .isEqualTo(
                                    com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1
                                            .Outcome.APPLIED_EXACT);
                    var registry = nativeExpectedWriterRegistry(capability);
                    var current = exact.read(aggregateKey)
                            .toCompletableFuture()
                            .join()
                            .orElseThrow();
                    assertThat(coordinator
                                    .enrollWriters(new com.nereusstream.storage.object.retention
                                            .M5PulsarAggregateRetirementCoordinatorV1.EnrollmentRequest(
                                            aggregateKey, current, registry.enrollment()))
                                    .toCompletableFuture()
                                    .join())
                            .isEqualTo(
                                    com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1
                                            .Outcome.APPLIED_EXACT);
                    metadataWriter = PulsarNativeMetadataWriterV2.install(
                                    binding,
                                    budget,
                                    (org.apache.bookkeeper.mledger.impl.MetaStoreImpl) guardedFactory.getMetaStore(),
                                    registry,
                                    coordinator,
                                    aggregateKey,
                                    creation)
                            .get(30, TimeUnit.SECONDS);
                    metadataWriter
                            .admitObjectPhysicalScope(offloader, objectNamespace, 8_388_608)
                            .get(30, TimeUnit.SECONDS);
                    metadataWriter.requireObjectPhysicalScope().get(30, TimeUnit.SECONDS);
                    var nativeStore = (org.apache.bookkeeper.mledger.impl.MetaStoreImpl) guardedFactory.getMetaStore();
                    var parentBefore = nativeStore
                            .readStoredMutationValue(name, "/managed-ledgers/" + name, 8_388_608)
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow();
                    var oldPlain =
                            nativeStore.parseManagedLedgerInfo(parentBefore.storedBytes()).getLedgerInfosList().stream()
                                    .filter(info -> !info.hasOffloadContext()
                                            || !info.getOffloadContext().isComplete())
                                    .map(org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo::getLedgerId)
                                    .toList();
                    var offloaded =
                            nativeStore.parseManagedLedgerInfo(parentBefore.storedBytes()).getLedgerInfosList().stream()
                                    .filter(info -> info.hasOffloadContext()
                                            && info.getOffloadContext().isComplete())
                                    .toList();
                    assertThat(offloaded).isNotEmpty();
                    var objectHeads = new java.util.HashMap<
                            String, software.amazon.awssdk.services.s3.model.HeadObjectResponse>();
                    var objectResources = new java.util.ArrayList<PhysicalResourceIdV2>();
                    for (var info : offloaded) {
                        var context = info.getOffloadContext();
                        var keys = PulsarOffloadKeysV1.derive(
                                "cells/m5-native",
                                info.getLedgerId(),
                                new UUID(context.getUidMsb(), context.getUidLsb()));
                        for (var key : List.of(keys.rootKey(), keys.dataKey())) {
                            var head = sourceClient.headObject(HeadObjectRequest.builder()
                                    .bucket(bucket)
                                    .key(key)
                                    .build());
                            objectHeads.put(key, head);
                            objectResources.add(new PhysicalResourceIdV2.ObjectVersion(
                                    objectNamespace,
                                    CanonicalUtf8.fromString(key),
                                    PhysicalResourceIdV2.ObjectIdentityKind.IMMUTABLE_VERSION,
                                    CanonicalUtf8.fromString(head.versionId())));
                        }
                    }
                    assertThat(oldPlain).isNotEmpty();
                    var manager = creation.namespace().borrowClient().getLedgerManager();
                    var oldHeaders = new java.util.HashMap<
                            Long,
                            org.apache.bookkeeper.versioning.Versioned<
                                    org.apache.bookkeeper.client.api.LedgerMetadata>>();
                    for (long id : oldPlain) {
                        var header = manager.readLedgerMetadata(id).get(30, TimeUnit.SECONDS);
                        assertThat(header.getValue().isClosed()).isTrue();
                        assertThat(header.getValue().getCustomMetadata())
                                .doesNotContainKey("nereus/m5-pulsar-create-scope");
                        assertThat(creation.spec().bindingFor(header.getValue(), false))
                                .isEmpty();
                        assertThat(creation.spec().sealedSourceBindingFor(header.getValue()))
                                .isPresent();
                        oldHeaders.put(id, header);
                    }
                    int sourcePuts = nativeMetadataPuts.get();
                    var sourceCharge = budget.usage();
                    metadataWriter.admitSealedPlainSources().get(30, TimeUnit.SECONDS);
                    assertThat(nativeMetadataPuts.get()).isEqualTo(sourcePuts);
                    assertThat(budget.usage()).isEqualTo(sourceCharge);
                    for (long id : oldPlain) {
                        var before = oldHeaders.get(id);
                        var after = manager.readLedgerMetadata(id).get(30, TimeUnit.SECONDS);
                        assertThat(after.getVersion()).isEqualTo(before.getVersion());
                        assertThat(new org.apache.bookkeeper.meta.LedgerMetadataSerDe().serialize(after.getValue()))
                                .isEqualTo(new org.apache.bookkeeper.meta.LedgerMetadataSerDe()
                                        .serialize(before.getValue()));
                        var resource = new PhysicalResourceIdV2.BookKeeperLedger(
                                creation.namespace().namespace(), id);
                        assertThat(creation.route()
                                        .members()
                                        .readFact(resource)
                                        .toCompletableFuture()
                                        .get(30, TimeUnit.SECONDS)
                                        .inventory()
                                        .members())
                                .allSatisfy(
                                        member -> assertThat(member.binding()).isEqualTo(binding.bindingIdentity()));
                    }
                    var parentAfter = nativeStore
                            .readStoredMutationValue(name, "/managed-ledgers/" + name, 8_388_608)
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow();
                    assertThat(parentAfter.stat()).isEqualTo(parentBefore.stat());
                    assertThat(parentAfter.storedBytes()).isEqualTo(parentBefore.storedBytes());
                    var physicalObjects =
                            S3M5PhysicalNamespaceAuthorityV2.connect(sourceClient, bucket, objectNamespace);
                    var actualObjectRoute =
                            binding.materializationQuota(physicalObjects).get(30, TimeUnit.SECONDS);
                    for (var resource : objectResources) {
                        var object = (PhysicalResourceIdV2.ObjectVersion) resource;
                        var head = sourceClient.headObject(HeadObjectRequest.builder()
                                .bucket(bucket)
                                .key(object.objectKey().value())
                                .build());
                        assertThat(head.versionId())
                                .isEqualTo(objectHeads
                                        .get(object.objectKey().value())
                                        .versionId());
                        assertThat(head.contentLength())
                                .isEqualTo(objectHeads
                                        .get(object.objectKey().value())
                                        .contentLength());
                        actualObjectRoute
                                .readResourceFact(resource)
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS);
                        assertThat(actualObjectRoute
                                        .members()
                                        .readFact(resource)
                                        .toCompletableFuture()
                                        .get(30, TimeUnit.SECONDS)
                                        .inventory()
                                        .members())
                                .allSatisfy(
                                        member -> assertThat(member.binding()).isEqualTo(binding.bindingIdentity()));
                    }
                    verifyNativeMixedReferenceTickets(
                            binding,
                            budget,
                            metadataWriter,
                            creation,
                            actualObjectRoute,
                            objectResources,
                            nativeStore,
                            countedMetadata,
                            responseFault,
                            nativeMetadataPuts,
                            parentReplyGate,
                            parentStored,
                            Optional.empty(),
                            false);
                    verifyNativeMetadataTicketClearUnknown(
                            binding,
                            budget,
                            metadataWriter,
                            (org.apache.bookkeeper.mledger.impl.MetaStoreImpl) guardedFactory.getMetaStore(),
                            metadata,
                            responseFault,
                            aggregateKey,
                            nativeMetadataPuts);
                    verifyNativeMetadataUndispatchedCleanup(
                            binding,
                            budget,
                            metadataWriter,
                            (org.apache.bookkeeper.mledger.impl.MetaStoreImpl) guardedFactory.getMetaStore(),
                            metadata,
                            responseFault,
                            aggregateKey,
                            nativeMetadataPuts);
                    verifyNativeMetadataDefinitiveRejection(
                            binding,
                            budget,
                            metadataWriter,
                            (org.apache.bookkeeper.mledger.impl.MetaStoreImpl) guardedFactory.getMetaStore(),
                            metadata,
                            responseFault,
                            aggregateKey,
                            nativeMetadataPuts);
                }
                var cfg = config(offloader)
                        .setMaxEntriesPerLedger(100)
                        .setEnsembleSize(creation.spec().ensemble())
                        .setWriteQuorumSize(creation.spec().writeQuorum())
                        .setAckQuorumSize(creation.spec().ackQuorum());
                binding.configureCreate(cfg);
                var failedResource = new AtomicReference<PhysicalResourceIdV2.BookKeeperLedger>();
                if (loseBirth) {
                    responseFault.armCandidate(
                            bytes -> {
                                try {
                                    var authority = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1
                                            .decodeAuthority(CanonicalBytes.copyOf(bytes));
                                    if (authority.authorityRevision() == 1
                                            && authority.target().resourceId()
                                                    instanceof PhysicalResourceIdV2.BookKeeperLedger resource) {
                                        failedResource.set(resource);
                                        return true;
                                    }
                                } catch (RuntimeException notAuthority) {
                                }
                                return false;
                            });
                    var attemptFactory = guardedFactory;
                    assertThatThrownBy(() -> attemptFactory.open(name, cfg)).isInstanceOf(ManagedLedgerException.class);
                    assertThat(responseFault.putLosses.get()).isEqualTo(1);
                    var absent = failedResource.get();
                    assertThat(absent).isNotNull();
                    creation.namespace()
                            .requirePulsarReservation(absent.ledgerId())
                            .get(30, TimeUnit.SECONDS);
                    assertThatThrownBy(() -> owned.namespace()
                                    .borrowClient()
                                    .getLedgerManager()
                                    .readLedgerMetadata(absent.ledgerId())
                                    .get(30, TimeUnit.SECONDS))
                            .isInstanceOf(java.util.concurrent.ExecutionException.class);
                    responseFault.clear();
                    assertThat(creation.route()
                                    .readResourceFact(absent)
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS))
                            .isNotNull();
                }
                ledger = (ManagedLedgerImpl) guardedFactory.open(name, cfg);
                var cursor = ledger.openCursor("guarded-cursor", InitialPosition.Earliest);
                var first = ledger.addEntry(payload(0));
                ledger.addEntry(payload(1));
                cursor.markDelete(first);
                var cursorInfo = new CompletableFuture<org.apache.bookkeeper.mledger.proto.ManagedCursorInfo>();
                guardedFactory
                        .getMetaStore()
                        .asyncGetCursorInfo(
                                name,
                                "guarded-cursor",
                                new org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback<>() {
                                    @Override
                                    public void operationComplete(
                                            org.apache.bookkeeper.mledger.proto.ManagedCursorInfo info,
                                            org.apache.pulsar.metadata.api.Stat stat) {
                                        cursorInfo.complete(info);
                                    }

                                    @Override
                                    public void operationFailed(
                                            org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException
                                                    failure) {
                                        cursorInfo.completeExceptionally(failure);
                                    }
                                });
                long cursorLedger = cursorInfo.get(30, TimeUnit.SECONDS).getCursorsLedgerId();
                if (metadataWriter != null) {
                    var aggregateKey = new OxiaV2AuthorityKeys(binding.authorityRoot())
                            .aggregateKey(
                                    binding.authority().aggregate().binding().incarnationIdentity());
                    var aggregate =
                            new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(
                                    responseFault);
                    var beforeResetAuthority = aggregate
                            .read(aggregateKey)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow();
                    String cursorPath = "/managed-ledgers/" + name + "/guarded-cursor";
                    var beforeResetMetadata =
                            metadata.get(cursorPath).get(30, TimeUnit.SECONDS).orElseThrow();
                    cursor.resetCursor(first);
                    assertThat(cursor.getReadPosition()).isEqualTo(first);
                    assertThat(cursor.getMarkDeletedPosition()).isNotEqualTo(first);
                    var afterResetMetadata =
                            metadata.get(cursorPath).get(30, TimeUnit.SECONDS).orElseThrow();
                    assertThat(afterResetMetadata.getValue()).isEqualTo(beforeResetMetadata.getValue());
                    assertThat(afterResetMetadata.getStat().getVersion())
                            .isEqualTo(beforeResetMetadata.getStat().getVersion());
                    var afterResetAuthority = aggregate
                            .read(aggregateKey)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow();
                    assertThat(afterResetAuthority.metadataVersion())
                            .as("cursor rewind must invalidate the C version captured by floors")
                            .isNotEqualTo(beforeResetAuthority.metadataVersion());
                    verifyNativeCheckpointMetadataTickets(
                            binding,
                            budget,
                            metadataWriter,
                            creation,
                            countedMetadata,
                            responseFault,
                            nativeMetadataPuts,
                            cursorLedger,
                            cursorReplyGate,
                            cursorStored);
                }
                for (long id : List.of(first.getLedgerId(), cursorLedger)) {
                    creation.namespace().requirePulsarReservation(id).get(30, TimeUnit.SECONDS);
                    var nativeMetadata = creation.namespace()
                            .borrowClient()
                            .getLedgerManager()
                            .readLedgerMetadata(id)
                            .get(30, TimeUnit.SECONDS);
                    assertThat(creation.spec().bindingFor(nativeMetadata.getValue(), true))
                            .isPresent();
                    var resource = new PhysicalResourceIdV2.BookKeeperLedger(
                            creation.namespace().namespace(), id);
                    var member = creation.route()
                            .members()
                            .readFact(resource)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS);
                    assertThat(member.inventory().members()).hasSize(1);
                    assertThat(member.inventory().members().get(0).binding()).isEqualTo(binding.bindingIdentity());
                }
                if (metadataCleanupMode < 39) {
                    var pendingCellClose = budget.closeCell();
                    pendingCellClose.complete(null);
                }
                assertThat(budget.usage().owners()).isEqualTo(metadataWriter == null ? 1 : 2);
                closeNativeLedger(ledger).get(30, TimeUnit.SECONDS);
                if (!loseBirth && metadataCleanupMode == 0) {
                    var closedHeader = bootstrap
                            .borrowClient()
                            .getLedgerManager()
                            .readLedgerMetadata(first.getLedgerId())
                            .get(30, TimeUnit.SECONDS)
                            .getValue();
                    assertThatThrownBy(() -> owned.namespace()
                                    .readPriorPulsarSealedBirth(first.getLedgerId(), closedHeader)
                                    .get(30, TimeUnit.SECONDS))
                            .hasRootCauseMessage("prior Native Pulsar scope lacks its exact create fence");
                }
                if (metadataWriter != null) {
                    ledger = null;
                    guardedFactory.shutdown();
                    guardedFactory = null;
                    var originalObjectClose = offloader.closeAsync();
                    assertThat(originalObjectClose).isNotDone();
                    metadataWriter.closeAsync().get(30, TimeUnit.SECONDS);
                    originalObjectClose.get(30, TimeUnit.SECONDS);
                }
                if (metadataCleanupMode >= 39) {
                    verifyNativeMetadataCreateOutcome(
                            creation, bootstrap, first.getLedgerId(), nativeReplies, budget, metadataCleanupMode);
                    return;
                }
                if (metadataCleanupMode >= 36) {
                    verifyNativeMetadataOutcome(
                            creation,
                            bootstrap,
                            first.getLedgerId(),
                            nativeReplies,
                            responseFault,
                            budget,
                            metadataCleanupMode);
                    return;
                }
                if (metadataCleanupMode != 0) {
                    verifyNativeMetadataUnknownCleanup(
                            creation,
                            bootstrap,
                            first.getLedgerId(),
                            cursorLedger,
                            responseFault,
                            budget,
                            metadataCleanupMode == 35);
                    return;
                }
                java.util.concurrent.Future<?> pendingScopeClose = null;
                if (!loseBirth) {
                    pendingScopeClose = verifyNativeMetadataAdmission(
                            creation, List.of(first.getLedgerId(), cursorLedger), responseFault, budget, control);
                }
                creation.namespace().fencePulsarCreates().get(30, TimeUnit.SECONDS);
                var births = creation.namespace().readPulsarBirths().get(30, TimeUnit.SECONDS);
                var summaries = creation.namespace().readPulsarBirthSummaries().get(30, TimeUnit.SECONDS);
                assertThat(summaries.births().stream().map(value -> value.ledgerId()))
                        .containsExactlyElementsOf(births.births().stream()
                                .map(value -> value.ledgerId())
                                .toList());
                for (var birth : births.births()) {
                    assertThat(creation.namespace()
                                    .readPulsarBirthMetadata(summaries, birth.ledgerId())
                                    .get(30, TimeUnit.SECONDS))
                            .isEqualTo(birth);
                }
                int rawMetadataBytes = births.births().stream()
                        .mapToInt(value -> value.ledgerMetadata()
                                .map(record -> record.rawStoredBytes().length())
                                .orElse(0))
                        .sum();
                assertThatThrownBy(() -> owned.namespace()
                                .readPulsarBirths(rawMetadataBytes - 1)
                                .get(30, TimeUnit.SECONDS))
                        .hasRootCauseMessage("Native birth bulk metadata budget exhausted");
                assertThat(creation.namespace().readPulsarBirthSummaries().get(30, TimeUnit.SECONDS))
                        .isEqualTo(summaries);
                if (loseBirth) {
                    var manager = creation.namespace().borrowClient().getLedgerManager();
                    var nativeValue =
                            manager.readLedgerMetadata(first.getLedgerId()).get(30, TimeUnit.SECONDS);
                    manager.writeLedgerMetadata(first.getLedgerId(), nativeValue.getValue(), nativeValue.getVersion())
                            .get(30, TimeUnit.SECONDS);
                    assertThatThrownBy(() -> owned.namespace()
                                    .readPulsarBirthMetadata(summaries, first.getLedgerId())
                                    .get(30, TimeUnit.SECONDS))
                            .hasRootCauseMessage("Native birth metadata differs from its summary");
                    var refreshed =
                            creation.namespace().readPulsarBirthSummaries().get(30, TimeUnit.SECONDS);
                    assertThat(refreshed).isNotEqualTo(summaries);
                    births = creation.namespace().readPulsarBirths().get(30, TimeUnit.SECONDS);
                    for (var birth : births.births()) {
                        assertThat(creation.namespace()
                                        .readPulsarBirthMetadata(refreshed, birth.ledgerId())
                                        .get(30, TimeUnit.SECONDS))
                                .isEqualTo(birth);
                    }
                }
                assertThat(births.taskFence().nativeVersion()).isEqualTo(1);
                assertThat(births.births().stream().map(value -> value.ledgerId()))
                        .contains(first.getLedgerId(), cursorLedger);
                assertThat(births.births()).hasSize(loseBirth ? 3 : 2);
                assertThat(births.births().stream()
                                .filter(value -> value.ledgerMetadata().isPresent()))
                        .hasSize(2);
                if (loseBirth) {
                    assertThat(births.births().stream()
                                    .filter(value -> value.ledgerId()
                                            == failedResource.get().ledgerId())
                                    .findFirst()
                                    .orElseThrow()
                                    .ledgerMetadata())
                            .isEmpty();
                }
                try (var observer = M5BookKeeperNamespaceAuthorityV2.observePulsarScope(
                        "zk://" + System.getProperty("nereus.m5.nativeOffloadZk") + "/ledgers",
                        bootstrap.capability(),
                        creation.spec())) {
                    assertThat(observer.readPulsarBirths().get(30, TimeUnit.SECONDS))
                            .isEqualTo(births);
                    assertThatThrownBy(() -> observer.borrowClient()
                                    .newCreateLedgerOp()
                                    .withEnsembleSize(3)
                                    .withWriteQuorumSize(owned.spec().writeQuorum())
                                    .withAckQuorumSize(2)
                                    .withPassword(new byte[0])
                                    .withDigestType(DigestType.CRC32C)
                                    .withCustomMetadata(Map.of(
                                            "application", "pulsar".getBytes(StandardCharsets.UTF_8),
                                            "component", "managed-ledger".getBytes(StandardCharsets.UTF_8),
                                            "pulsar/managed-ledger", name.getBytes(StandardCharsets.UTF_8)))
                                    .execute()
                                    .get(30, TimeUnit.SECONDS))
                            .isInstanceOf(java.util.concurrent.ExecutionException.class);
                    var actualZk = ((org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver)
                                    observer.borrowClient().getMetadataClientDriver())
                            .getZk();
                    var index = births.births().stream()
                            .filter(value -> value.ledgerId() == cursorLedger)
                            .findFirst()
                            .orElseThrow()
                            .indexEntry();
                    byte[] corrupt = index.rawStoredBytes().toByteArray();
                    corrupt[0] ^= 1;
                    actualZk.setData(index.nativePath(), corrupt, index.nativeVersion());
                    assertThatThrownBy(() -> observer.readPulsarBirths().get(30, TimeUnit.SECONDS))
                            .isInstanceOf(java.util.concurrent.ExecutionException.class);
                    actualZk.setData(
                            index.nativePath(), index.rawStoredBytes().toByteArray(), index.nativeVersion() + 1);
                    assertThatThrownBy(() -> observer.readPulsarBirths().get(30, TimeUnit.SECONDS))
                            .isInstanceOf(java.util.concurrent.ExecutionException.class);
                }
                assertThatThrownBy(() -> owned.namespace()
                                .borrowClient()
                                .newCreateLedgerOp()
                                .withEnsembleSize(3)
                                .withWriteQuorumSize(owned.spec().writeQuorum())
                                .withAckQuorumSize(2)
                                .withPassword(new byte[0])
                                .withDigestType(DigestType.CRC32C)
                                .withCustomMetadata(Map.of(
                                        "application", "pulsar".getBytes(StandardCharsets.UTF_8),
                                        "component", "managed-ledger".getBytes(StandardCharsets.UTF_8),
                                        "pulsar/managed-ledger", name.getBytes(StandardCharsets.UTF_8)))
                                .execute()
                                .get(30, TimeUnit.SECONDS))
                        .isInstanceOf(java.util.concurrent.ExecutionException.class);
                if (pendingScopeClose != null) {
                    var waiting = pendingScopeClose;
                    assertThatThrownBy(() -> waiting.get(100, TimeUnit.MILLISECONDS))
                            .isInstanceOf(TimeoutException.class);
                    assertThat(budget.usage().owners()).isEqualTo(1);
                    responseFault.clear();
                    pendingScopeClose.get(30, TimeUnit.SECONDS);
                } else {
                    creation.close();
                }
                if (metadataWriter != null) {
                    metadataWriter.closeAsync().get(30, TimeUnit.SECONDS);
                    var exact = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(
                            responseFault);
                    String key = new OxiaV2AuthorityKeys(binding.authorityRoot())
                            .aggregateKey(
                                    binding.authority().aggregate().binding().incarnationIdentity());
                    var stored = exact.read(key).toCompletableFuture().join().orElseThrow();
                    var authority =
                            com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                    stored.canonicalStoredBytes());
                    assertThat(authority.referenceMutationTickets()).isEmpty();
                    assertThat(authority.authorityGeneration()).isGreaterThan(2);
                    System.out.println("NATIVE_PULSAR_METADATA_WRITER actualNativeDispatch=true actualOxiaTickets=true "
                            + "originalReadbackAndCellRelease=true completeOtherWriters=false wholeM5Authority=false");
                }
                assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
                assertThatThrownBy(() ->
                                owned.namespace().readPulsarBirthSummaries().get(30, TimeUnit.SECONDS))
                        .hasRootCauseInstanceOf(org.apache.bookkeeper.client.BKException.BKClientClosedException.class);
                budget.closeCell().get(30, TimeUnit.SECONDS);
                if (!loseBirth) {
                    verifyPriorGuardedSourceAdmission(
                            binding,
                            bootstrap,
                            metadata,
                            creation.spec(),
                            first.getLedgerId(),
                            responseFault,
                            control,
                            bucket,
                            objectNamespace,
                            objectExecutor,
                            fileExecutor);
                    verifyNativeGenerationAdmission(binding, bootstrap, control);
                    verifyNativeHeldBirthRead(binding, bootstrap, control, false);
                    verifyNativeHeldBirthRead(binding, bootstrap, control, true);
                    verifyNativeSdkOpeningCleanup(binding, bootstrap, control, responseFault);
                    verifyNativeSdkClosedCleanup(binding, bootstrap, control, responseFault);
                    verifyUndeliveredNativeSdk(binding, bootstrap, control, responseFault, false);
                    verifyUndeliveredNativeSdk(binding, bootstrap, control, responseFault, true);
                    verifyUndeliveredNativeOpeningFailure(binding, bootstrap, control, responseFault, false);
                    verifyUndeliveredNativeOpeningFailure(binding, bootstrap, control, responseFault, true);
                }
                System.out.println("NATIVE_PULSAR_GUARDED_BIRTH actualSdkProfile=true dataAndCursor=true"
                        + " nativeReservation=true quotaBackedBirth=true durableMembers=true metadataTickets=true"
                        + " nativeFenceRejectsCreate=true unknownNoPhysical=" + loseBirth
                        + " wholeM5Authority=false");
            } catch (Exception | Error failure) {
                workFailure = failure;
                throw failure;
            } finally {
                responseFault.clear();
                if (ledger != null) {
                    closeNativeLedger(ledger).get(30, TimeUnit.SECONDS);
                }
                if (guardedFactory != null) {
                    guardedFactory.shutdown();
                }
                if (metadataWriter != null) {
                    metadataWriter.closeAsync().get(30, TimeUnit.SECONDS);
                }
                if (creation != null) {
                    try {
                        creation.close();
                    } catch (Exception | Error cleanupFailure) {
                        if (workFailure == null) {
                            throw cleanupFailure;
                        }
                        workFailure.addSuppressed(cleanupFailure);
                    }
                }
                offloader.close();
                store.close().toCompletableFuture().get(30, TimeUnit.SECONDS);
            }
        } finally {
            if (nativeReplies != null) {
                nativeReplies.close();
            }
            control.shutdown();
            assertThat(control.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    private void verifyPriorGuardedSourceAdmission(
            PulsarNativeBindingV1 binding,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            MetadataStoreExtended metadata,
            com.nereusstream.storage.bookkeeper.M5PulsarNativeCreateSpecV1 originalScope,
            long originalLedger,
            SelectorResponseFault responses,
            ExecutorService executor,
            String bucket,
            Namespace objectNamespace,
            ExecutorService objectExecutor,
            ExecutorService fileExecutor)
            throws Exception {
        var before = bootstrap
                .borrowClient()
                .getLedgerManager()
                .readLedgerMetadata(originalLedger)
                .get(30, TimeUnit.SECONDS);
        var budget = readBudget(binding);
        PulsarNativeBookKeeperCreateV1 creation = null;
        ManagedLedgerFactoryImpl factory = null;
        PulsarNativeMetadataWriterV2 writer = null;
        try (var sdk = client(null)) {
            var configuration = new S3PulsarOffloadObjectStoreV1.MaterializationConfiguration(
                    new com.nereusstream.storage.object.materialization.M5CellProviderProfileV2.References(
                            "minio/security/nereus-m5-drain",
                            "minio/credential/nereus-m5-drain",
                            "NONE",
                            "nereus/m5/native-cell-admission",
                            "nereus/m5/owned-runner",
                            "nereus/m5/version-match-delete-v1"),
                    sdk.serviceClientConfiguration().credentialsProvider());
            var store = new S3PulsarOffloadObjectStoreV1(sdk, bucket, objectExecutor, false, configuration);
            var offloader = offloader(store, fileExecutor);
            try {
                creation = binding.openGuardedBookKeeper(
                                "zk://" + System.getProperty("nereus.m5.nativeOffloadZk") + "/ledgers",
                                bootstrap,
                                budget,
                                NATIVE_BK_CANONICAL_CAPACITY,
                                executor)
                        .get(30, TimeUnit.SECONDS);
                assertThat(creation.spec().allocatorOwner()).isNotEqualTo(originalScope.allocatorOwner());
                assertThat(creation.spec().sha256()).isNotEqualTo(originalScope.sha256());
                var proof = creation.namespace()
                        .readPriorPulsarSealedBirth(originalLedger, before.getValue())
                        .get(30, TimeUnit.SECONDS);
                assertThat(proof.scope()).isEqualTo(originalScope);
                assertThat(proof.facts()).hasSize(5);
                factory = factory(metadata, creation.namespace().borrowClient());
                var exact = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(
                        responses);
                var authority =
                        new com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1(exact);
                var registry = nativeExpectedWriterRegistry(
                        new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding(
                                1, creation.spec().configuration()));
                String key = new OxiaV2AuthorityKeys(binding.authorityRoot())
                        .aggregateKey(binding.authority().aggregate().binding().incarnationIdentity());
                writer = PulsarNativeMetadataWriterV2.install(
                                binding,
                                budget,
                                (org.apache.bookkeeper.mledger.impl.MetaStoreImpl) factory.getMetaStore(),
                                registry,
                                authority,
                                key,
                                creation)
                        .get(30, TimeUnit.SECONDS);
                writer.admitObjectPhysicalScope(offloader, objectNamespace, 8_388_608)
                        .get(30, TimeUnit.SECONDS);
                writer.admitSealedPlainSources().get(30, TimeUnit.SECONDS);
                var after = bootstrap
                        .borrowClient()
                        .getLedgerManager()
                        .readLedgerMetadata(originalLedger)
                        .get(30, TimeUnit.SECONDS);
                assertThat(after.getVersion()).isEqualTo(before.getVersion());
                assertThat(new org.apache.bookkeeper.meta.LedgerMetadataSerDe().serialize(after.getValue()))
                        .isEqualTo(new org.apache.bookkeeper.meta.LedgerMetadataSerDe().serialize(before.getValue()));
            } finally {
                if (factory != null) {
                    factory.shutdown();
                }
                if (writer != null) {
                    writer.closeAsync().get(30, TimeUnit.SECONDS);
                }
                if (creation != null) {
                    creation.close();
                }
                budget.closeCell().get(30, TimeUnit.SECONDS);
                offloader.close();
                store.close().toCompletableFuture().get(30, TimeUnit.SECONDS);
            }
        }
        assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
    }

    private static void verifyNativeMetadataCreateOutcome(
            PulsarNativeBookKeeperCreateV1 creation,
            M5BookKeeperNamespaceAuthorityV2 independent,
            long sourceId,
            com.nereusstream.storage.bookkeeper.M5ZooKeeperReadReplyFixtureV1 replies,
            PulsarNativeReadCellBudgetV1 budget,
            int mode)
            throws Exception {
        var manager = creation.namespace().borrowClient().getLedgerManager();
        var sample =
                manager.readLedgerMetadata(sourceId).get(30, TimeUnit.SECONDS).getValue();
        long id;
        try (var generator = creation.namespace()
                .borrowClient()
                .getMetadataClientDriver()
                .getLedgerManagerFactory()
                .newLedgerIdGenerator()) {
            var allocated = new CompletableFuture<Long>();
            generator.generateLedgerId((rc, value) -> {
                if (rc == org.apache.bookkeeper.client.BKException.Code.OK) {
                    allocated.complete(value);
                } else {
                    allocated.completeExceptionally(org.apache.bookkeeper.client.BKException.create(rc));
                }
            });
            id = allocated.get(30, TimeUnit.SECONDS);
        }
        creation.namespace().requirePulsarReservation(id).get(30, TimeUnit.SECONDS);
        var originalMetadata = Map.of(
                "application", sample.getCustomMetadata().get("application"),
                "component", sample.getCustomMetadata().get("component"),
                "pulsar/managed-ledger", sample.getCustomMetadata().get("pulsar/managed-ledger"));
        var metadata = org.apache.bookkeeper.client.LedgerMetadataBuilder.create()
                .withId(id)
                .withMetadataFormatVersion(3)
                .withDigestType(sample.getDigestType())
                .withPassword(sample.getPassword())
                .withEnsembleSize(sample.getEnsembleSize())
                .withWriteQuorumSize(sample.getWriteQuorumSize())
                .withAckQuorumSize(sample.getAckQuorumSize())
                .newEnsembleEntry(0, sample.getAllEnsembles().get(0L))
                .withCustomMetadata(originalMetadata)
                .build();
        if (mode == 42) {
            creation.namespace().fencePulsarCreates().get(30, TimeUnit.SECONDS);
            var births = creation.namespace().readPulsarBirthSummaries().get(30, TimeUnit.SECONDS);
            var reservation = births.births().stream()
                    .filter(value -> value.ledgerId() == id)
                    .findFirst()
                    .orElseThrow()
                    .reservation();
            replies.holdReservationRead(reservation.nativePath());
        } else if (mode == 40) {
            replies.observeLedgerCreate();
            creation.namespace().fencePulsarCreates().get(30, TimeUnit.SECONDS);
        } else {
            replies.holdLedgerCreate();
        }
        var creating = manager.createLedgerMetadata(id, metadata);
        var actualManager = independent.borrowClient().getLedgerManager();
        org.apache.bookkeeper.versioning.Versioned<org.apache.bookkeeper.client.api.LedgerMetadata> actual = null;
        if (mode == 42) {
            assertThat(replies.awaitHeld()).isTrue();
            assertThat(replies.targetedReadDispatches()).isEqualTo(2);
            assertThatThrownBy(() -> actualManager.readLedgerMetadata(id).get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(
                            org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsOnMetadataServerException
                                    .class);
            replies.rejectGetReply();
        } else if (mode != 40) {
            assertThat(replies.awaitHeld()).isTrue();
            actual = actualManager.readLedgerMetadata(id).get(30, TimeUnit.SECONDS);
            assertThat(creation.spec().bindingFor(actual.getValue(), true)).isPresent();
            replies.loseCreateReplyAndBlockReads();
        }
        assertThatThrownBy(() -> creating.get(30, TimeUnit.SECONDS))
                .isInstanceOf(java.util.concurrent.ExecutionException.class);
        assertThat(replies.targetedWriteDispatches()).isEqualTo(mode == 42 ? 0 : mode == 40 ? 1 : 2);
        if (mode == 42) {
            assertThatThrownBy(() -> creating.get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(
                            org.apache.bookkeeper.client.BKException.BKUnauthorizedAccessException.class);
        }
        assertThat(budget.usage().owners()).isEqualTo(1);
        if (mode == 41) {
            var changed = org.apache.bookkeeper.client.LedgerMetadataBuilder.from(actual.getValue())
                    .withCToken(actual.getValue().getCToken() + 1)
                    .build();
            var rewritten = actualManager
                    .writeLedgerMetadata(id, changed, actual.getVersion())
                    .get(30, TimeUnit.SECONDS);
            replies.allowMetadataReads();
            try {
                assertThatThrownBy(creation::close)
                        .hasRootCauseMessage("Native create terminal belongs to another invocation");
                assertThat(budget.usage().owners()).isEqualTo(1);
                assertThat(budget.closeCell()).isNotDone();
            } finally {
                actual = actualManager
                        .writeLedgerMetadata(id, actual.getValue(), rewritten.getVersion())
                        .get(30, TimeUnit.SECONDS);
            }
        }
        replies.allowMetadataReads();
        creation.close();
        if (mode == 40 || mode == 42) {
            assertThatThrownBy(() -> actualManager.readLedgerMetadata(id).get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(
                            org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsOnMetadataServerException
                                    .class);
        } else {
            var after = actualManager.readLedgerMetadata(id).get(30, TimeUnit.SECONDS);
            assertThat(after.getVersion()).isEqualTo(actual.getVersion());
            var serde = new org.apache.bookkeeper.meta.LedgerMetadataSerDe();
            assertThat(serde.serialize(after.getValue())).containsExactly(serde.serialize(actual.getValue()));
        }
        assertThat(creating).isCompletedExceptionally();
        assertThat(replies.targetedWriteDispatches()).isEqualTo(mode == 42 ? 0 : mode == 40 ? 1 : 2);
        assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
        budget.closeCell().get(30, TimeUnit.SECONDS);
        replies.requireHealthy();
        System.out.println("NATIVE_METADATA_CREATE_TERMINAL realReservationAndMulti=true mode=" + mode
                + " originalOutcomeUnchanged=true sdkCreateNotReissued=true wholeM5Authority=false");
    }

    private static void verifyNativeMetadataOutcome(
            PulsarNativeBookKeeperCreateV1 creation,
            M5BookKeeperNamespaceAuthorityV2 independent,
            long ledgerId,
            com.nereusstream.storage.bookkeeper.M5ZooKeeperReadReplyFixtureV1 replies,
            SelectorResponseFault responses,
            PulsarNativeReadCellBudgetV1 budget,
            int mode)
            throws Exception {
        var manager = creation.namespace().borrowClient().getLedgerManager();
        var before = manager.readLedgerMetadata(ledgerId).get(30, TimeUnit.SECONDS);
        creation.namespace().fencePulsarCreates().get(30, TimeUnit.SECONDS);
        var births = creation.namespace().readPulsarBirthSummaries().get(30, TimeUnit.SECONDS);
        var summary = births.births().stream()
                .filter(value -> value.ledgerId() == ledgerId)
                .findFirst()
                .orElseThrow();
        replies.holdWrite(summary.ledgerMetadata().orElseThrow().nativePath());
        var updating = manager.writeLedgerMetadata(ledgerId, before.getValue(), before.getVersion());
        assertThat(replies.awaitHeld()).isTrue();
        var independentManager = independent.borrowClient().getLedgerManager();
        var applied = independentManager.readLedgerMetadata(ledgerId).get(30, TimeUnit.SECONDS);
        assertThat(((org.apache.bookkeeper.versioning.LongVersion) applied.getVersion()).getLongVersion())
                .isEqualTo(((org.apache.bookkeeper.versioning.LongVersion) before.getVersion()).getLongVersion() + 1);
        replies.loseWriteReply();
        assertThatThrownBy(() -> updating.get(30, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(org.apache.bookkeeper.client.BKException.BKMetadataVersionException.class);
        assertThat(replies.targetedWriteDispatches()).isEqualTo(2);
        assertThat(budget.usage().owners()).isEqualTo(1);
        if (mode == 38) {
            var changed = org.apache.bookkeeper.client.LedgerMetadataBuilder.from(applied.getValue())
                    .withCToken(applied.getValue().getCToken() + 1)
                    .build();
            var changedValue = independentManager
                    .writeLedgerMetadata(ledgerId, changed, applied.getVersion())
                    .get(30, TimeUnit.SECONDS);
            try {
                assertThatThrownBy(creation::close).hasRootCauseMessage("Native metadata creation identity changed");
                assertThat(budget.usage().owners()).isEqualTo(1);
                assertThat(budget.closeCell()).isNotDone();
            } finally {
                applied = independentManager
                        .writeLedgerMetadata(ledgerId, applied.getValue(), changedValue.getVersion())
                        .get(30, TimeUnit.SECONDS);
            }
        }
        if (mode == 37) {
            var resource = new PhysicalResourceIdV2.BookKeeperLedger(
                    creation.namespace().namespace(), ledgerId);
            responses.armCandidate(bytes -> metadataAuthorityCandidate(bytes, resource, false));
            assertThatThrownBy(creation::close).hasRootCauseMessage("Native BK metadata cleanup remains unresolved");
            assertThat(budget.usage().owners()).isEqualTo(1);
            assertThatThrownBy(() -> manager.readLedgerMetadata(ledgerId).get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(org.apache.bookkeeper.client.BKException.BKClientClosedException.class);
            responses.clear();
        }
        creation.close();
        var after = independentManager.readLedgerMetadata(ledgerId).get(30, TimeUnit.SECONDS);
        assertThat(after.getVersion()).isEqualTo(applied.getVersion());
        var serde = new org.apache.bookkeeper.meta.LedgerMetadataSerDe();
        assertThat(serde.serialize(after.getValue())).containsExactly(serde.serialize(applied.getValue()));
        assertThat(updating).isCompletedExceptionally();
        assertThat(replies.targetedWriteDispatches()).isEqualTo(2);
        assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
        budget.closeCell().get(30, TimeUnit.SECONDS);
        replies.requireHealthy();
        System.out.println("NATIVE_METADATA_WRITE_UNKNOWN actualSetDataApplied=true originalFailurePreserved=true"
                + " actualVersionFence=true nativeWriteNotRepeated=true mode=" + mode + " wholeM5Authority=false");
    }

    private static void verifyNativeMetadataUnknownCleanup(
            PulsarNativeBookKeeperCreateV1 creation,
            M5BookKeeperNamespaceAuthorityV2 independent,
            long ledgerId,
            long siblingLedgerId,
            SelectorResponseFault responses,
            PulsarNativeReadCellBudgetV1 budget,
            boolean loseTicket)
            throws Exception {
        var manager = creation.namespace().borrowClient().getLedgerManager();
        var before = manager.readLedgerMetadata(ledgerId).get(30, TimeUnit.SECONDS);
        var resource =
                new PhysicalResourceIdV2.BookKeeperLedger(creation.namespace().namespace(), ledgerId);
        int losses = responses.putLosses.get();
        responses.armCandidate(bytes -> metadataAuthorityCandidate(bytes, resource, loseTicket));
        assertThatThrownBy(() -> manager.writeLedgerMetadata(ledgerId, before.getValue(), before.getVersion())
                        .get(30, TimeUnit.SECONDS))
                .isInstanceOf(java.util.concurrent.ExecutionException.class);
        assertThat(responses.putLosses.get()).isEqualTo(losses + 1);
        var actual = independent
                .borrowClient()
                .getLedgerManager()
                .readLedgerMetadata(ledgerId)
                .get(30, TimeUnit.SECONDS);
        long version = ((org.apache.bookkeeper.versioning.LongVersion) before.getVersion()).getLongVersion();
        assertThat(((org.apache.bookkeeper.versioning.LongVersion) actual.getVersion()).getLongVersion())
                .isEqualTo(version + (loseTicket ? 0 : 1));
        responses.clear();
        var sibling = manager.readLedgerMetadata(siblingLedgerId).get(30, TimeUnit.SECONDS);
        var siblingResource =
                new PhysicalResourceIdV2.BookKeeperLedger(creation.namespace().namespace(), siblingLedgerId);
        responses.armCandidate(bytes -> metadataAuthorityCandidate(bytes, siblingResource, loseTicket));
        assertThatThrownBy(() -> manager.writeLedgerMetadata(siblingLedgerId, sibling.getValue(), sibling.getVersion())
                        .get(30, TimeUnit.SECONDS))
                .isInstanceOf(java.util.concurrent.ExecutionException.class);
        assertThat(responses.putLosses.get()).isEqualTo(losses + 2);
        assertThatThrownBy(() -> manager.writeLedgerMetadata(ledgerId, actual.getValue(), actual.getVersion())
                        .get(30, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(org.apache.bookkeeper.client.BKException.BKTooManyRequestsException.class);
        assertThatThrownBy(creation::close).hasRootCauseMessage("Native BK metadata cleanup remains unresolved");
        assertThat(budget.usage().owners()).isEqualTo(1);
        assertThat(budget.closeCell()).isNotDone();
        responses.clear();
        creation.close();
        var after = independent
                .borrowClient()
                .getLedgerManager()
                .readLedgerMetadata(ledgerId)
                .get(30, TimeUnit.SECONDS);
        assertThat(after.getVersion()).isEqualTo(actual.getVersion());
        var serde = new org.apache.bookkeeper.meta.LedgerMetadataSerDe();
        assertThat(serde.serialize(after.getValue())).containsExactly(serde.serialize(actual.getValue()));
        var authority = new com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1(creation.route())
                .inspect(resource.authorityKey())
                .toCompletableFuture()
                .get(30, TimeUnit.SECONDS)
                .orElseThrow();
        assertThat(authority.fullAuthority().orElseThrow().activeWriterTickets())
                .isEmpty();
        assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
        budget.closeCell().get(30, TimeUnit.SECONDS);
        System.out.println("NATIVE_PULSAR_METADATA_UNKNOWN originalTicketCleanup=true sdkCapacityRetained=true"
                + " nativeVersionUnchangedOnRetry=true noDispatch=" + loseTicket + " wholeM5Authority=false");
    }

    private static boolean metadataAuthorityCandidate(
            byte[] bytes, PhysicalResourceIdV2.BookKeeperLedger resource, boolean loseTicket) {
        try {
            var authority = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                    CanonicalBytes.copyOf(bytes));
            return authority.target().resourceId().equals(resource)
                    && authority.activeWriterTickets().isEmpty() != loseTicket;
        } catch (RuntimeException notAuthority) {
            return false;
        }
    }

    private static java.util.concurrent.Future<?> verifyNativeMetadataAdmission(
            PulsarNativeBookKeeperCreateV1 creation,
            List<Long> ledgerIds,
            SelectorResponseFault responses,
            PulsarNativeReadCellBudgetV1 budget,
            ExecutorService control)
            throws Exception {
        var manager = creation.namespace().borrowClient().getLedgerManager();
        long cursorId = ledgerIds.get(1);
        var cursorMetadata = manager.readLedgerMetadata(cursorId).get(30, TimeUnit.SECONDS);
        var oversizedValues = new java.util.HashMap<>(cursorMetadata.getValue().getCustomMetadata());
        oversizedValues.put(
                "pulsar/cursor",
                new byte[com.nereusstream.storage.bookkeeper.M5PulsarNativeCreateSpecV1.MAX_METADATA_BYTES]);
        var oversized = org.apache.bookkeeper.client.LedgerMetadataBuilder.from(cursorMetadata.getValue())
                .withCustomMetadata(oversizedValues)
                .build();
        assertThatThrownBy(() -> manager.writeLedgerMetadata(cursorId, oversized, cursorMetadata.getVersion())
                        .get(30, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
        assertThat(manager.readLedgerMetadata(cursorId)
                        .get(30, TimeUnit.SECONDS)
                        .getVersion())
                .isEqualTo(cursorMetadata.getVersion());
        for (long unsupported : new long[] {-1L, 0xffff_ffffL, 0x1_0000_0000L}) {
            assertThatThrownBy(() -> manager.writeLedgerMetadata(
                                    cursorId,
                                    cursorMetadata.getValue(),
                                    new org.apache.bookkeeper.versioning.LongVersion(unsupported))
                            .get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(org.apache.bookkeeper.client.BKException.BKMetadataVersionException.class);
            assertThat(manager.readLedgerMetadata(cursorId)
                            .get(30, TimeUnit.SECONDS)
                            .getVersion())
                    .isEqualTo(cursorMetadata.getVersion());
        }
        responses.holdTwoAppliedPuts(bytes -> {
            try {
                var authority = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                        CanonicalBytes.copyOf(bytes));
                return authority.authorityRevision() > 1
                        && authority.activeWriterTickets().isEmpty()
                        && authority.target().resourceId() instanceof PhysicalResourceIdV2.BookKeeperLedger resource
                        && ledgerIds.contains(resource.ledgerId());
            } catch (RuntimeException notAuthority) {
                return false;
            }
        });
        var updates = new java.util.ArrayList<
                CompletableFuture<
                        org.apache.bookkeeper.versioning.Versioned<org.apache.bookkeeper.client.api.LedgerMetadata>>>();
        for (long id : ledgerIds) {
            var current = manager.readLedgerMetadata(id).get(30, TimeUnit.SECONDS);
            updates.add(manager.writeLedgerMetadata(id, current.getValue(), current.getVersion()));
        }
        assertThat(responses.heldApplied.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(updates).allMatch(value -> !value.isDone());
        var current = manager.readLedgerMetadata(cursorId).get(30, TimeUnit.SECONDS);
        assertThatThrownBy(() -> manager.writeLedgerMetadata(cursorId, current.getValue(), current.getVersion())
                        .get(30, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(org.apache.bookkeeper.client.BKException.BKTooManyRequestsException.class);
        assertThat(manager.readLedgerMetadata(cursorId)
                        .get(30, TimeUnit.SECONDS)
                        .getVersion())
                .isEqualTo(current.getVersion());
        updates.get(0).complete(current);
        updates.get(1).cancel(true);
        var driver = (com.nereusstream.storage.bookkeeper.M5PulsarNativeMetadataDriverV1)
                creation.namespace().borrowClient().getMetadataClientDriver();
        driver.drainMetadata().complete(null);
        assertThatThrownBy(() -> driver.drainMetadata().get(100, TimeUnit.MILLISECONDS))
                .isInstanceOf(TimeoutException.class);
        assertThat(budget.usage().owners()).isEqualTo(1);
        System.out.println("NATIVE_PULSAR_METADATA_ADMISSION actualSdkWrites=true heldActualOxiaCompletion=true"
                + " oversizedRejectedBeforeWrite=true fullAdmissionRejects=true modifiedObserversRetain=true"
                + " wholeM5Authority=false");
        return control.submit(() -> {
            creation.close();
            return null;
        });
    }

    private static void verifyNativeGenerationAdmission(
            PulsarNativeBindingV1 binding, M5BookKeeperNamespaceAuthorityV2 bootstrap, ExecutorService control)
            throws Exception {
        var budget = readBudget(binding);
        String uri = "zk://" + System.getProperty("nereus.m5.nativeOffloadZk") + "/ledgers";
        try (var creation = binding.openGuardedBookKeeper(uri, bootstrap, budget, NATIVE_BK_CANONICAL_CAPACITY, control)
                        .get(30, TimeUnit.SECONDS);
                var generations = new com.nereusstream.storage.bookkeeper.M5PulsarGenerationReplyFixtureV1(
                        (com.nereusstream.storage.bookkeeper.M5PulsarNativeMetadataDriverV1)
                                creation.namespace().borrowClient().getMetadataClientDriver(),
                        uri,
                        bootstrap.capability())) {
            var first = generations.allocate();
            var second = generations.allocate();
            assertThat(generations.awaitNativeReplies()).isTrue();
            assertThat(first).isNotDone();
            assertThat(second).isNotDone();
            assertThatThrownBy(() -> generations.allocate().get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(org.apache.bookkeeper.client.BKException.BKTooManyRequestsException.class);
            assertThat(generations.nativeDispatches()).isEqualTo(2);
            first.complete(Long.MAX_VALUE);
            second.cancel(true);
            var driver = (com.nereusstream.storage.bookkeeper.M5PulsarNativeMetadataDriverV1)
                    creation.namespace().borrowClient().getMetadataClientDriver();
            driver.drainMetadata().complete(null);
            budget.closeCell().complete(null);
            var closing = control.submit(() -> {
                creation.close();
                return null;
            });
            creation.namespace().fencePulsarCreates().get(30, TimeUnit.SECONDS);
            var before = creation.namespace().readPulsarBirths().get(30, TimeUnit.SECONDS);
            assertThat(before.births()).isEmpty();
            assertThatThrownBy(() -> driver.drainMetadata().get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            assertThatThrownBy(() -> closing.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
            assertThat(budget.usage().owners()).isEqualTo(1);
            assertThatThrownBy(() -> generations.allocate().get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(org.apache.bookkeeper.client.BKException.BKClientClosedException.class);
            assertThat(generations.nativeDispatches()).isEqualTo(2);
            try (var observer =
                    M5BookKeeperNamespaceAuthorityV2.observePulsarScope(uri, bootstrap.capability(), creation.spec())) {
                generations.releaseReplies();
                closing.get(30, TimeUnit.SECONDS);
                assertThat(generations.awaitDelivery()).isTrue();
                assertThat(generations.terminalCode(0))
                        .isEqualTo(org.apache.bookkeeper.client.BKException.Code.ZKException);
                assertThat(generations.terminalCode(1))
                        .isEqualTo(org.apache.bookkeeper.client.BKException.Code.ZKException);
                assertThat(observer.readPulsarBirths().get(30, TimeUnit.SECONDS))
                        .isEqualTo(before);
            }
            assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
            budget.closeCell().get(30, TimeUnit.SECONDS);
            System.out.println("NATIVE_PULSAR_ID_ADMISSION actualSdkGenerator=true actualZkCreateRepliesHeld=true"
                    + " overflowNoNativeDispatch=true modifiedObserversRetain=true sdkCloseWaits=true"
                    + " lateFenceRejectsBirth=true wholeM5Authority=false");
        } finally {
            budget.closeCell().get(30, TimeUnit.SECONDS);
        }
    }

    private static void verifyNativeHeldBirthRead(
            PulsarNativeBindingV1 binding,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            ExecutorService control,
            boolean cancel)
            throws Exception {
        var budget = readBudget(binding);
        String upstream = "zk://" + System.getProperty("nereus.m5.nativeOffloadZk") + "/ledgers";
        try (var replies = new com.nereusstream.storage.bookkeeper.M5ZooKeeperReadReplyFixtureV1(upstream)) {
            var creation = binding.openGuardedBookKeeper(
                            replies.metadataUri(), bootstrap, budget, NATIVE_BK_CANONICAL_CAPACITY, control)
                    .get(30, TimeUnit.SECONDS);
            try {
                creation.namespace().fencePulsarCreates().get(30, TimeUnit.SECONDS);
                var before = creation.namespace().readPulsarBirthSummaries().get(30, TimeUnit.SECONDS);
                assertThat(before.births()).isEmpty();
                replies.holdGet(before.inventoryHead().nativePath());
                var reading = creation.namespace().readPulsarBirthSummaries();
                assertThat(replies.awaitHeld()).isTrue();
                if (cancel) {
                    assertThat(reading.cancel(true)).isTrue();
                } else {
                    assertThat(reading.complete(null)).isTrue();
                }
                assertThatThrownBy(() ->
                                creation.namespace().readPulsarBirthSummaries().get(30, TimeUnit.SECONDS))
                        .hasRootCauseInstanceOf(
                                org.apache.bookkeeper.client.BKException.BKTooManyRequestsException.class);
                var driver = (com.nereusstream.storage.bookkeeper.M5PulsarNativeMetadataDriverV1)
                        creation.namespace().borrowClient().getMetadataClientDriver();
                driver.drainBirthReads().complete(null);
                assertThatThrownBy(() -> driver.drainBirthReads().get(100, TimeUnit.MILLISECONDS))
                        .isInstanceOf(TimeoutException.class);
                budget.closeCell().complete(null);
                var closing = control.submit(() -> {
                    creation.close();
                    return null;
                });
                assertThatThrownBy(() -> closing.get(100, TimeUnit.MILLISECONDS))
                        .isInstanceOf(TimeoutException.class);
                assertThat(budget.usage().owners()).isEqualTo(1);
                replies.releaseReply();
                closing.get(30, TimeUnit.SECONDS);
                replies.requireHealthy();
                assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
                budget.closeCell().get(30, TimeUnit.SECONDS);
                try (var observer = M5BookKeeperNamespaceAuthorityV2.observePulsarScope(
                        upstream, bootstrap.capability(), creation.spec())) {
                    assertThat(observer.readPulsarBirthSummaries().get(30, TimeUnit.SECONDS))
                            .isEqualTo(before);
                }
                System.out.println("NATIVE_PULSAR_BIRTH_READ_DRAIN actualSdk=true originalZkGetReplyHeld=true"
                        + " modifiedObserver=" + cancel + " fullAdmissionRejects=true privateDrainWaits=true"
                        + " cellChargeRetained=true actualSdkClose=true wholeM5Authority=false");
            } finally {
                replies.releaseReply();
                creation.close();
            }
        } finally {
            budget.closeCell().get(30, TimeUnit.SECONDS);
        }
    }

    private static void verifyNativeSdkOpeningCleanup(
            PulsarNativeBindingV1 binding,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            ExecutorService control,
            SelectorResponseFault responses)
            throws Exception {
        var budget = readBudget(binding);
        String upstream = "zk://" + System.getProperty("nereus.m5.nativeOffloadZk") + "/ledgers";
        PulsarNativeBookKeeperCreateV1.OpenCleanupException cleanup = null;
        try (var sdk = new com.nereusstream.storage.bookkeeper.M5ZooKeeperReadReplyFixtureV1(upstream)) {
            int originalLosses = responses.putLosses.get();
            responses.arm(budget.ownerKey(), budget::hasDurableReservationValue);
            var opening = binding.openGuardedBookKeeper(
                    sdk.metadataUri(), bootstrap, budget, NATIVE_BK_CANONICAL_CAPACITY, control);
            try {
                opening.get(30, TimeUnit.SECONDS).close();
                throw new AssertionError("Expected unresolved original SDK opening cleanup");
            } catch (java.util.concurrent.ExecutionException failure) {
                Throwable cause = failure;
                while (!(cause instanceof PulsarNativeBookKeeperCreateV1.OpenCleanupException)
                        && cause.getCause() != null) {
                    cause = cause.getCause();
                }
                assertThat(cause).isInstanceOf(PulsarNativeBookKeeperCreateV1.OpenCleanupException.class);
                cleanup = (PulsarNativeBookKeeperCreateV1.OpenCleanupException) cause;
            }
            assertThat(responses.putLosses.get()).isEqualTo(originalLosses + 1);
            assertThat(budget.usage().owners()).isEqualTo(1);
            assertThat(budget.usage().encodedBytes()).isPositive();
            assertThat(sdk.hasSdkConnection()).isFalse();
            var original = cleanup;
            assertThatThrownBy(() -> original.reconcileCleanup().get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("actual selector observation unavailable");
            assertThat(budget.usage().owners()).isEqualTo(1);
            assertThat(sdk.hasSdkConnection()).isFalse();
            responses.clear();
            cleanup.reconcileCleanup().get(30, TimeUnit.SECONDS);
            cleanup.reconcileCleanup().get(30, TimeUnit.SECONDS);
            assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
            assertThat(budget.durableUsage().get(30, TimeUnit.SECONDS)).isEqualTo(budget.usage());
            assertThat(sdk.hasSdkConnection()).isFalse();
            budget.closeCell().get(30, TimeUnit.SECONDS);
            System.out.println("NATIVE_PULSAR_OPEN_CLEANUP actualOxiaReserveUnknown=true sdkNotStarted=true"
                    + " originalCleanupRetained=true retryNoSdkStart=true exactReservationRemoval=true"
                    + " wholeM5Authority=false");
        } finally {
            responses.clear();
            if (cleanup != null) {
                cleanup.reconcileCleanup().get(30, TimeUnit.SECONDS);
            }
            budget.closeCell().get(30, TimeUnit.SECONDS);
        }
    }

    private static void verifyNativeSdkClosedCleanup(
            PulsarNativeBindingV1 binding,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            ExecutorService control,
            SelectorResponseFault responses)
            throws Exception {
        var budget = readBudget(binding);
        String uri = "zk://" + System.getProperty("nereus.m5.nativeOffloadZk") + "/ledgers";
        var creation = binding.openGuardedBookKeeper(uri, bootstrap, budget, NATIVE_BK_CANONICAL_CAPACITY, control)
                .get(30, TimeUnit.SECONDS);
        try {
            int before = responses.putLosses.get();
            responses.arm(
                    budget.ownerKey(),
                    bytes -> !budget.hasDurableReservationValue(bytes) && !budget.isDrainedOwnerValue(bytes));
            assertThatThrownBy(creation::close).isInstanceOf(java.util.concurrent.ExecutionException.class);
            assertThat(responses.putLosses.get()).isEqualTo(before + 1);
            assertThat(budget.usage().owners()).isEqualTo(1);
            assertThatThrownBy(() ->
                            creation.namespace().readPulsarBirthSummaries().get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(org.apache.bookkeeper.client.BKException.BKClientClosedException.class);
            try (var observer =
                    M5BookKeeperNamespaceAuthorityV2.observePulsarScope(uri, bootstrap.capability(), creation.spec())) {
                assertThat(observer.readPulsarBirthSummaries()
                                .get(30, TimeUnit.SECONDS)
                                .births())
                        .isEmpty();
            }
            assertThatThrownBy(creation::close).isInstanceOf(java.util.concurrent.ExecutionException.class);
            assertThat(budget.usage().owners()).isEqualTo(1);
            responses.clear();
            creation.close();
            assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
            assertThat(budget.durableUsage().get(30, TimeUnit.SECONDS)).isEqualTo(budget.usage());
            budget.closeCell().get(30, TimeUnit.SECONDS);
            System.out.println("NATIVE_PULSAR_CLOSED_CLEANUP actualSdkClose=true originalReleaseUnknown=true"
                    + " chargeRetained=true closedSdkNotReissued=true exactReservationRemoval=true"
                    + " wholeM5Authority=false");
        } finally {
            responses.clear();
            creation.close();
            budget.closeCell().get(30, TimeUnit.SECONDS);
        }
    }

    private static void verifyUndeliveredNativeSdk(
            PulsarNativeBindingV1 binding,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            ExecutorService control,
            SelectorResponseFault responses,
            boolean cancel)
            throws Exception {
        var budget = readBudget(binding);
        String uri = "zk://" + System.getProperty("nereus.m5.nativeOffloadZk") + "/ledgers";
        try (var sdk = new com.nereusstream.storage.bookkeeper.M5ZooKeeperReadReplyFixtureV1(uri)) {
            sdk.holdGet("/ledgers/INSTANCEID");
            var opening = binding.openGuardedBookKeeper(
                    sdk.metadataUri(), bootstrap, budget, NATIVE_BK_CANONICAL_CAPACITY, control);
            try {
                assertThat(sdk.awaitHeld()).isTrue();
                assertThat(sdk.hasSdkConnection()).isTrue();
                if (cancel) {
                    assertThat(opening.cancel(true)).isTrue();
                } else {
                    assertThat(opening.complete(null)).isTrue();
                }
                int losses = responses.putLosses.get();
                responses.arm(
                        budget.ownerKey(),
                        bytes -> !budget.hasDurableReservationValue(bytes) && !budget.isDrainedOwnerValue(bytes));
                budget.closeCell().complete(null);
                assertThat(budget.usage().owners()).isEqualTo(1);
                sdk.releaseReply();
                long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (responses.putLosses.get() == losses) {
                    if (System.nanoTime() >= limit) {
                        throw new AssertionError("Original undelivered SDK cleanup did not dispatch");
                    }
                    Thread.sleep(10);
                }
                assertThat(responses.putLosses.get()).isEqualTo(losses + 1);
                assertThatThrownBy(() -> budget.reconcileUndeliveredSdkCleanup().get(30, TimeUnit.SECONDS))
                        .isInstanceOf(java.util.concurrent.ExecutionException.class);
                assertThat(budget.usage().owners()).isEqualTo(1);
                assertThatThrownBy(() -> budget.closeCell().get(100, TimeUnit.MILLISECONDS))
                        .isInstanceOf(TimeoutException.class);
                responses.clear();
                var cleanup = budget.reconcileUndeliveredSdkCleanup();
                cleanup.complete(null);
                budget.reconcileUndeliveredSdkCleanup().get(30, TimeUnit.SECONDS);
                budget.closeCell().get(30, TimeUnit.SECONDS);
                assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
                assertThat(budget.durableUsage().get(30, TimeUnit.SECONDS)).isEqualTo(budget.usage());
                sdk.requireHealthy();
                System.out.println("NATIVE_PULSAR_UNDELIVERED_SDK actualConstructorReplyHeld=true observer=" + cancel
                        + " originalSdkRetained=true closedBeforeRemoval=true unknownRetainsCharge=true"
                        + " exactCellCleanup=true wholeM5Authority=false");
            } finally {
                sdk.releaseReply();
                responses.clear();
                budget.reconcileUndeliveredSdkCleanup().get(30, TimeUnit.SECONDS);
                budget.closeCell().get(30, TimeUnit.SECONDS);
            }
        }
    }

    private static void verifyUndeliveredNativeOpeningFailure(
            PulsarNativeBindingV1 binding,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            ExecutorService control,
            SelectorResponseFault responses,
            boolean cancel)
            throws Exception {
        var budget = readBudget(binding);
        String uri = "zk://" + System.getProperty("nereus.m5.nativeOffloadZk") + "/ledgers";
        try (var sdk = new com.nereusstream.storage.bookkeeper.M5ZooKeeperReadReplyFixtureV1(uri)) {
            try {
                responses.holdAppliedPut(budget.ownerKey(), budget::hasDurableReservationValue);
                var opening = binding.openGuardedBookKeeper(
                        sdk.metadataUri(), bootstrap, budget, NATIVE_BK_CANONICAL_CAPACITY, control);
                assertThat(responses.heldApplied.await(30, TimeUnit.SECONDS)).isTrue();
                if (cancel) {
                    assertThat(opening.cancel(true)).isTrue();
                } else {
                    assertThat(opening.complete(null)).isTrue();
                }
                budget.closeCell().complete(null);
                assertThat(budget.usage().owners()).isEqualTo(1);
                assertThat(sdk.hasSdkConnection()).isFalse();
                responses.loseHeldPutReply();
                // This case's existing single control executor finishes the private opening before the barrier.
                control.submit(() -> {}).get(30, TimeUnit.SECONDS);
                assertThatThrownBy(() -> budget.reconcileUndeliveredSdkCleanup().get(30, TimeUnit.SECONDS))
                        .hasRootCauseMessage("actual selector observation unavailable");
                assertThat(budget.usage().owners()).isEqualTo(1);
                assertThatThrownBy(() -> budget.closeCell().get(100, TimeUnit.MILLISECONDS))
                        .isInstanceOf(TimeoutException.class);
                responses.clear();
                budget.reconcileUndeliveredSdkCleanup().get(30, TimeUnit.SECONDS);
                budget.closeCell().get(30, TimeUnit.SECONDS);
                assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
                assertThat(budget.durableUsage().get(30, TimeUnit.SECONDS)).isEqualTo(budget.usage());
                assertThat(sdk.hasSdkConnection()).isFalse();
                sdk.requireHealthy();
                System.out.println("NATIVE_PULSAR_UNDELIVERED_OPEN_FAILURE actualReserveApplied=true observer=" + cancel
                        + " originalCleanupRetained=true unknownRetainsCharge=true sdkNeverStarted=true"
                        + " exactCellCleanup=true wholeM5Authority=false");
            } finally {
                responses.clear();
                control.submit(() -> {}).get(30, TimeUnit.SECONDS);
                budget.reconcileUndeliveredSdkCleanup().get(30, TimeUnit.SECONDS);
                budget.closeCell().get(30, TimeUnit.SECONDS);
            }
        }
    }

    private void verifyNativeReservationInventory(
            PulsarNativeBindingV1 binding,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            Namespace objectNamespace,
            String bucket,
            String name,
            ExecutorService objectExecutor,
            ExecutorService fileExecutor,
            AtomicInteger reads,
            SelectorResponseFault responseFault,
            int mode)
            throws Exception {
        try (var metadata = metadata();
                var sourceClient = client(reads, null, null, null)) {
            var store = new S3PulsarOffloadObjectStoreV1(sourceClient, bucket, objectExecutor, false);
            var offloader = offloader(store, fileExecutor);
            var factory = factory(metadata, bookKeeper.borrowClient());
            ManagedLedgerImpl ledger = null;
            PulsarNativeSourcePreparationV1 preparation = null;
            try {
                ledger = (ManagedLedgerImpl) factory.open(name, config(offloader));
                var budget = readBudget(binding);
                int before = reads.get();
                boolean loseReserve = mode == 21;
                if (loseReserve) {
                    responseFault.arm(budget.ownerKey(), budget::hasDurableReservationValue);
                }
                preparation = binding.prepareReadSources(ledger, bookKeeper, offloader, objectNamespace, budget, true)
                        .get(30, TimeUnit.SECONDS);
                if (mode == 23) {
                    verifyInitialOwnerCleanup(
                            binding,
                            preparation.view().get(30, TimeUnit.SECONDS),
                            bookKeeper,
                            offloader,
                            budget,
                            reads,
                            responseFault);
                    return;
                }
                if (mode == 27 || mode == 28) {
                    verifyUndeliveredInitialOwner(
                            binding,
                            preparation.view().get(30, TimeUnit.SECONDS),
                            bookKeeper,
                            offloader,
                            budget,
                            reads,
                            responseFault,
                            mode == 27);
                    return;
                }
                if (mode == 29) {
                    verifyActorlessInitialGrantQuarantine(
                            binding,
                            preparation.view().get(30, TimeUnit.SECONDS),
                            bookKeeper,
                            offloader,
                            budget,
                            reads,
                            responseFault);
                    return;
                }
                if (mode == 33) {
                    verifyActorlessGrantRecovery(
                            binding,
                            preparation.view().get(30, TimeUnit.SECONDS),
                            bookKeeper,
                            offloader,
                            budget,
                            reads,
                            responseFault,
                            ledger,
                            objectNamespace,
                            bucket,
                            name,
                            objectExecutor,
                            fileExecutor);
                    return;
                }
                if (mode >= 30 && mode <= 32) {
                    verifyNativeInitialClaim(
                            binding,
                            preparation.view().get(30, TimeUnit.SECONDS),
                            bookKeeper,
                            offloader,
                            budget,
                            reads,
                            responseFault,
                            mode);
                    return;
                }
                var attempt = preparation;
                if (loseReserve) {
                    assertThatThrownBy(() -> attempt.view().get(30, TimeUnit.SECONDS))
                            .isInstanceOf(java.util.concurrent.ExecutionException.class);
                    assertThat(reads.get()).isEqualTo(before);
                } else {
                    preparation.view().get(30, TimeUnit.SECONDS);
                    assertThat(budget.durableUsage().get(30, TimeUnit.SECONDS)).isEqualTo(budget.usage());
                    responseFault.arm(
                            budget.ownerKey(),
                            bytes -> !budget.hasDurableReservationValue(bytes) && !budget.isDrainedOwnerValue(bytes));
                    assertThatThrownBy(() -> attempt.close().get(30, TimeUnit.SECONDS))
                            .isInstanceOf(java.util.concurrent.ExecutionException.class);
                }
                assertThat(responseFault.putLosses.get()).isEqualTo(1);
                assertThat(budget.usage().owners()).isEqualTo(1);
                assertThat(budget.usage().encodedBytes()).isPositive();
                responseFault.clear();
                preparation.close().get(30, TimeUnit.SECONDS);
                assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
                assertThat(budget.durableUsage().get(30, TimeUnit.SECONDS)).isEqualTo(budget.usage());
                if (loseReserve) {
                    assertThat(reads.get()).isEqualTo(before);
                }
                budget.closeCell().get(30, TimeUnit.SECONDS);
                System.out.println(
                        "NATIVE_CELL_RESERVATION actualSingleHeadInventory=true admissionUnknown=" + loseReserve
                                + " noSourceOnUnknown=true releaseUnknownKeepsLocal=true exactOperationCleanup=true"
                                + " durableUsageMatches=true wholeM5Authority=false");
            } finally {
                responseFault.clear();
                try {
                    if (preparation != null) {
                        preparation.close().get(30, TimeUnit.SECONDS);
                    }
                    if (ledger != null) {
                        closeNativeLedger(ledger).get(30, TimeUnit.SECONDS);
                    }
                } finally {
                    factory.shutdown();
                    offloader.close();
                    store.close().toCompletableFuture().get(30, TimeUnit.SECONDS);
                }
            }
        }
    }

    private void verifyInitialOwnerCleanup(
            PulsarNativeBindingV1 binding,
            PulsarNativeBindingReadViewV1 view,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            PulsarNativeReadCellBudgetV1 budget,
            AtomicInteger reads,
            SelectorResponseFault responseFault)
            throws Exception {
        var capsule = Path.of(System.getProperty("nereus.m5.nativeM4AdmissionFile"));
        var expected = Sha256Digest.copyOf(
                java.util.HexFormat.of().parseHex(System.getProperty("nereus.m5.nativeM4AdmissionSha256")));
        var admission = PulsarNativeM4DrainAdmissionV1.load(capsule, expected);
        var control = binding.m4Control();
        var controlLoop = Executors.newSingleThreadExecutor();
        var ownerLoop = Executors.newSingleThreadExecutor();
        try {
            int before = reads.get();
            responseFault.arm(budget.ownerKey(), budget::hasDurableReservationValue);
            var cleanup = failedOwnerCleanup(
                    control.createReadOwner(view, admission, bookKeeper, offloader, controlLoop, ownerLoop, 2, budget));
            assertThat(responseFault.putLosses.get()).isEqualTo(1);
            assertThat(budget.usage().owners()).isEqualTo(2);
            assertThat(reads.get()).isEqualTo(before);
            assertThat(responseFault.delegate.get(control.selectorKey()).get(30, TimeUnit.SECONDS))
                    .isNull();
            assertThatThrownBy(() -> cleanup.reconcileCleanup().get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("actual selector observation unavailable");
            assertThat(budget.usage().owners()).isEqualTo(2);
            responseFault.clear();
            var cleanupObserver = cleanup.reconcileCleanup();
            cleanupObserver.complete(null);
            cleanup.reconcileCleanup().get(30, TimeUnit.SECONDS);
            assertThat(budget.usage().owners()).isEqualTo(1);
            assertThat(budget.durableUsage().get(30, TimeUnit.SECONDS)).isEqualTo(budget.usage());
            assertThat(reads.get()).isEqualTo(before);
            var owner = control.createReadOwner(
                            view, admission, bookKeeper, offloader, controlLoop, ownerLoop, 2, budget)
                    .get(30, TimeUnit.SECONDS);
            assertThatThrownBy(() -> view.closeUnregisteredPreparation().get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("Native preparation is not its original unregistered read scope");
            owner.fence().get(30, TimeUnit.SECONDS);
            assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
            budget.closeCell().get(30, TimeUnit.SECONDS);
            System.out.println("NATIVE_INITIAL_OWNER_CLEANUP reserveUnknown=true noSelectorOrSourceDispatch=true"
                    + " originalCleanupRetained=true observerCannotFree=true exactRetry=true wholeM5Authority=false");
        } finally {
            responseFault.clear();
            controlLoop.shutdown();
            ownerLoop.shutdown();
            assertThat(controlLoop.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            assertThat(ownerLoop.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    private void verifyUndeliveredInitialOwner(
            PulsarNativeBindingV1 binding,
            PulsarNativeBindingReadViewV1 view,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            PulsarNativeReadCellBudgetV1 budget,
            AtomicInteger reads,
            SelectorResponseFault responses,
            boolean cancel)
            throws Exception {
        var capsule = Path.of(System.getProperty("nereus.m5.nativeM4AdmissionFile"));
        var expected = Sha256Digest.copyOf(
                java.util.HexFormat.of().parseHex(System.getProperty("nereus.m5.nativeM4AdmissionSha256")));
        var admission = PulsarNativeM4DrainAdmissionV1.load(capsule, expected);
        var control = binding.m4Control();
        var controlLoop = Executors.newSingleThreadExecutor();
        var ownerLoop = Executors.newSingleThreadExecutor();
        CompletableFuture<PulsarNativeM4ReadOwnerV1> opening = null;
        try {
            int beforeReads = reads.get();
            int beforeLosses = responses.putLosses.get();
            responses.holdAppliedPut(control.selectorKey(), bytes -> true);
            opening =
                    control.createReadOwner(view, admission, bookKeeper, offloader, controlLoop, ownerLoop, 2, budget);
            assertThat(responses.heldApplied.await(30, TimeUnit.SECONDS)).isTrue();
            var granted = control.readFacts(view)
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow()
                    .selector();
            assertThat(granted.mode()).isEqualTo(SelectorMode.PREFERRED_ONLY);
            if (cancel) {
                assertThat(opening.cancel(true)).isTrue();
            } else {
                assertThat(opening.complete(null)).isTrue();
            }
            String drainKey = control.ownerDrainKey(granted);
            responses.arm(drainKey);
            budget.closeCell().complete(null);
            assertThat(budget.usage().owners()).isEqualTo(2);
            assertThat(responses.heldRelease.complete(null)).isTrue();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (responses.putLosses.get() == beforeLosses) {
                assertThat(System.nanoTime()).isLessThan(deadline);
                TimeUnit.MILLISECONDS.sleep(10);
            }
            assertThat(responses.putLosses.get()).isEqualTo(beforeLosses + 1);
            assertThatThrownBy(() -> budget.reconcileUndeliveredOwnerCleanup().get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("actual selector observation unavailable");
            assertThat(budget.usage().owners()).isEqualTo(1);
            assertThat(reads.get()).isEqualTo(beforeReads);
            assertThatThrownBy(() -> budget.closeCell().get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            var actualDrain = responses.delegate.get(drainKey).get(30, TimeUnit.SECONDS);
            assertThat(actualDrain).isNotNull();
            responses.clear();
            budget.reconcileUndeliveredOwnerCleanup().complete(null);
            budget.reconcileUndeliveredOwnerCleanup().get(30, TimeUnit.SECONDS);
            budget.closeCell().get(30, TimeUnit.SECONDS);
            assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
            assertThat(budget.durableUsage().get(30, TimeUnit.SECONDS)).isEqualTo(budget.usage());
            assertThat(responses
                            .delegate
                            .get(drainKey)
                            .get(30, TimeUnit.SECONDS)
                            .value())
                    .containsExactly(actualDrain.value());
            assertThat(control.readSelectedSourceMap().get(30, TimeUnit.SECONDS).selectedViewSha256())
                    .isEqualTo(granted.selectedViewSha256());
            System.out.println("NATIVE_UNDELIVERED_OWNER actualGrantReplyHeld=true observer=" + cancel
                    + " originalActorDrained=true durableDrainUnknown=true chargeRetained=true"
                    + " exactOriginalGrantCleanup=true noAdditionalSourceGet=true wholeM5Authority=false");
        } finally {
            responses.clear();
            try {
                if (opening != null) {
                    opening.cancel(false);
                    if (!opening.isCompletedExceptionally() && opening.getNow(null) != null) {
                        opening.getNow(null).fence().get(30, TimeUnit.SECONDS);
                    }
                }
                budget.reconcileUndeliveredOwnerCleanup().get(30, TimeUnit.SECONDS);
                view.closePreparation().get(30, TimeUnit.SECONDS);
                budget.closeCell().get(30, TimeUnit.SECONDS);
            } finally {
                controlLoop.shutdown();
                ownerLoop.shutdown();
                assertThat(controlLoop.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
                assertThat(ownerLoop.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    private void verifyActorlessInitialGrantQuarantine(
            PulsarNativeBindingV1 binding,
            PulsarNativeBindingReadViewV1 view,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            PulsarNativeReadCellBudgetV1 budget,
            AtomicInteger reads,
            SelectorResponseFault responses)
            throws Exception {
        var capsule = Path.of(System.getProperty("nereus.m5.nativeM4AdmissionFile"));
        var expected = Sha256Digest.copyOf(
                java.util.HexFormat.of().parseHex(System.getProperty("nereus.m5.nativeM4AdmissionSha256")));
        var admission = PulsarNativeM4DrainAdmissionV1.load(capsule, expected);
        var control = binding.m4Control();
        var controlLoop = Executors.newSingleThreadExecutor();
        var ownerLoop = Executors.newSingleThreadExecutor();
        try {
            int beforeReads = reads.get();
            int beforeLosses = responses.putLosses.get();
            responses.arm(control.selectorKey());
            var opening =
                    control.createReadOwner(view, admission, bookKeeper, offloader, controlLoop, ownerLoop, 2, budget);
            Throwable failure = opening.handle((actor, thrown) -> thrown).get(30, TimeUnit.SECONDS);
            assertThat(failure).isNotNull();
            assertThat(responses.putLosses.get()).isEqualTo(beforeLosses + 1);
            var actualSelector = responses.delegate.get(control.selectorKey()).get(30, TimeUnit.SECONDS);
            assertThat(actualSelector).isNotNull();
            System.out.println("NATIVE_ACTORLESS_GRANT_UNKNOWN actualSelectorApplied=true originalOwners="
                    + budget.usage().owners() + " noAdditionalSourceGet=" + (reads.get() == beforeReads));
            assertThat(budget.usage().owners()).isEqualTo(2);
            assertThat(budget.durableUsage().get(30, TimeUnit.SECONDS)).isEqualTo(budget.usage());
            while (!(failure instanceof PulsarNativeM4ControlV1.OwnerCleanupException) && failure.getCause() != null) {
                failure = failure.getCause();
            }
            assertThat(failure).isInstanceOf(PulsarNativeM4ControlV1.OwnerCleanupException.class);
            var cleanup = (PulsarNativeM4ControlV1.OwnerCleanupException) failure;
            responses.clear();
            var selected = control.readFacts(view)
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow()
                    .selector();
            assertThat(cleanup.unresolvedInitialSelector()).contains(selected);
            var ownedClaim =
                    responses.delegate.get(control.initialOwnerClaimKey()).get(30, TimeUnit.SECONDS);
            responses
                    .delegate
                    .put(control.initialOwnerClaimKey(), ownedClaim.value())
                    .get(30, TimeUnit.SECONDS);
            assertThat(responses.delegate.get(control.ownerDrainKey(selected)).get(30, TimeUnit.SECONDS))
                    .isNull();
            assertThatThrownBy(() -> cleanup.reconcileCleanup().get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("Original Native initial claim is unconfirmed or changed");
            view.closePreparation().get(30, TimeUnit.SECONDS);
            assertThat(budget.usage().owners()).isEqualTo(1);
            assertThat(budget.durableUsage().get(30, TimeUnit.SECONDS)).isEqualTo(budget.usage());
            assertThat(reads.get()).isEqualTo(beforeReads);
            budget.closeCell().complete(null);
            assertThatThrownBy(() -> budget.reconcileUndeliveredOwnerCleanup().get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("Original Native initial claim is unconfirmed or changed");
            assertThatThrownBy(() -> budget.closeCell().get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            assertThatThrownBy(() -> readBudget(binding))
                    .hasRootCauseMessage("Native Cell allocator has no confirmed durable drain");
            assertThat(responses
                            .delegate
                            .get(control.selectorKey())
                            .get(30, TimeUnit.SECONDS)
                            .value())
                    .containsExactly(actualSelector.value());
            assertThat(responses.delegate.get(control.ownerDrainKey(selected)).get(30, TimeUnit.SECONDS))
                    .isNull();
            System.out.println("NATIVE_ACTORLESS_GRANT_QUARANTINE actualSelectorApplied=true actorNotRegistered=true"
                    + " originalChargeRetained=true claimVersionChanged=true sourceDrainedButNoOwnerProof=true"
                    + " noReplacementAllocator=true"
                    + " noAdditionalSourceGet=true wholeM5Authority=false");
        } finally {
            responses.clear();
            try {
                view.closePreparation().get(30, TimeUnit.SECONDS);
                // Absent qualified grant/actor proof deliberately keeps this original Cell quarantined.
                budget.closeCell();
            } finally {
                controlLoop.shutdown();
                ownerLoop.shutdown();
                assertThat(controlLoop.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
                assertThat(ownerLoop.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    private void verifyActorlessGrantRecovery(
            PulsarNativeBindingV1 binding,
            PulsarNativeBindingReadViewV1 view,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            PulsarNativeReadCellBudgetV1 budget,
            AtomicInteger reads,
            SelectorResponseFault responses,
            ManagedLedgerImpl previous,
            Namespace objectNamespace,
            String bucket,
            String name,
            ExecutorService objectExecutor,
            ExecutorService fileExecutor)
            throws Exception {
        var capsule = Path.of(System.getProperty("nereus.m5.nativeM4AdmissionFile"));
        var expected = Sha256Digest.copyOf(
                java.util.HexFormat.of().parseHex(System.getProperty("nereus.m5.nativeM4AdmissionSha256")));
        var admission = PulsarNativeM4DrainAdmissionV1.load(capsule, expected);
        var control = binding.m4Control();
        var controlLoop = Executors.newSingleThreadExecutor();
        var ownerLoop = Executors.newSingleThreadExecutor();
        try {
            int beforeReads = reads.get();
            responses.arm(control.selectorKey());
            var cleanup = failedOwnerCleanup(
                    control.createReadOwner(view, admission, bookKeeper, offloader, controlLoop, ownerLoop, 2, budget));
            assertThat(budget.usage().owners()).isEqualTo(2);
            responses.clear();
            var selected = control.readFacts(view)
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow()
                    .selector();
            assertThat(cleanup.unresolvedInitialSelector()).contains(selected);
            String drainKey = control.ownerDrainKey(selected);
            responses.arm(drainKey);
            assertThatThrownBy(() -> cleanup.reconcileCleanup().get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("actual selector observation unavailable");
            view.requireUnregisteredPreparationDrain();
            assertThat(budget.usage().owners()).isEqualTo(1);
            assertThat(budget.durableUsage().get(30, TimeUnit.SECONDS)).isEqualTo(budget.usage());
            var actualProof = responses.delegate.get(drainKey).get(30, TimeUnit.SECONDS);
            assertThat(ByteBuffer.wrap(actualProof.value()).getInt()).isEqualTo(0x4e505544);
            budget.closeCell().complete(null);
            assertThatThrownBy(() -> budget.closeCell().get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            responses.clear();
            cleanup.reconcileCleanup().complete(null);
            budget.reconcileUndeliveredOwnerCleanup().get(30, TimeUnit.SECONDS);
            budget.closeCell().get(30, TimeUnit.SECONDS);
            assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
            assertThat(responses
                            .delegate
                            .get(drainKey)
                            .get(30, TimeUnit.SECONDS)
                            .value())
                    .containsExactly(actualProof.value());
            assertThat(reads.get()).isEqualTo(beforeReads);
            closeNativeLedger(previous).get(30, TimeUnit.SECONDS);
            verifyActorlessColdOwner(
                    binding,
                    bookKeeper,
                    objectNamespace,
                    bucket,
                    name,
                    objectExecutor,
                    fileExecutor,
                    reads,
                    admission,
                    selected,
                    controlLoop,
                    ownerLoop);
            System.out.println("NATIVE_ACTORLESS_GRANT_EXIT actualSelectorUnknown=true originalClaimVersionBound=true"
                    + " actualUnregisteredSdkDrain=true drainResponseUnknownRetainsCharge=true exactProofRetry=true"
                    + " noSyntheticActor=true independentColdOwnerRead=true wholeM5Authority=false");
        } finally {
            responses.clear();
            try {
                budget.reconcileUndeliveredOwnerCleanup().get(30, TimeUnit.SECONDS);
                view.closePreparation().get(30, TimeUnit.SECONDS);
                budget.closeCell().get(30, TimeUnit.SECONDS);
            } finally {
                controlLoop.shutdown();
                ownerLoop.shutdown();
                assertThat(controlLoop.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
                assertThat(ownerLoop.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    private void verifyActorlessColdOwner(
            PulsarNativeBindingV1 binding,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            Namespace objectNamespace,
            String bucket,
            String name,
            ExecutorService objectExecutor,
            ExecutorService fileExecutor,
            AtomicInteger reads,
            PulsarNativeM4DrainAdmissionV1 admission,
            BindingReadSelector predecessor,
            ExecutorService controlLoop,
            ExecutorService ownerLoop)
            throws Exception {
        var budget = readBudget(binding);
        try (var metadata = metadata();
                var client = client(reads, null, null, null)) {
            var store = new S3PulsarOffloadObjectStoreV1(client, bucket, objectExecutor, false);
            var offloader = offloader(store, fileExecutor);
            var factory = factory(metadata, bookKeeper.borrowClient());
            ManagedLedgerImpl cold = null;
            PulsarNativeSourcePreparationV1 sources = null;
            PulsarNativeM4ReadOwnerV1 owner = null;
            try {
                cold = (ManagedLedgerImpl) factory.open(name, config(offloader));
                var control = binding.m4Control();
                sources = control.preparePublishedReadSources(cold, bookKeeper, offloader, objectNamespace, budget)
                        .get(30, TimeUnit.SECONDS);
                var view = sources.view().get(30, TimeUnit.SECONDS);
                var recovery = control.prepareColdReadOwner(sources, admission, bookKeeper, offloader, controlLoop)
                        .get(30, TimeUnit.SECONDS);
                owner = recovery.recover(controlLoop, ownerLoop, 2, budget).get(30, TimeUnit.SECONDS);
                assertThat(owner.selector().ownerEpoch()).isEqualTo(predecessor.ownerEpoch() + 1);
                var actualRead =
                        view.readOwnedOperation(view.ledgerIds().getFirst(), 0, 0, OWNED_READ_LIMITS, () -> true);
                assertThat(actualRead.result().get(30, TimeUnit.SECONDS).entries())
                        .hasSize(1);
                actualRead.drained().get(30, TimeUnit.SECONDS);
                owner.fence().get(30, TimeUnit.SECONDS);
                budget.closeCell().get(30, TimeUnit.SECONDS);
                assertThat(budget.durableUsage().get(30, TimeUnit.SECONDS)).isEqualTo(budget.usage());
            } finally {
                if (owner != null) {
                    owner.fence().get(30, TimeUnit.SECONDS);
                }
                if (sources != null) {
                    sources.close().get(30, TimeUnit.SECONDS);
                }
                if (cold != null) {
                    closeNativeLedger(cold).get(30, TimeUnit.SECONDS);
                }
                factory.shutdown();
                offloader.close();
                store.close().toCompletableFuture().get(30, TimeUnit.SECONDS);
                budget.closeCell().get(30, TimeUnit.SECONDS);
            }
        }
    }

    private void verifyNativeInitialClaim(
            PulsarNativeBindingV1 binding,
            PulsarNativeBindingReadViewV1 view,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            PulsarNativeReadCellBudgetV1 budget,
            AtomicInteger reads,
            SelectorResponseFault responses,
            int mode)
            throws Exception {
        var capsule = Path.of(System.getProperty("nereus.m5.nativeM4AdmissionFile"));
        var expected = Sha256Digest.copyOf(
                java.util.HexFormat.of().parseHex(System.getProperty("nereus.m5.nativeM4AdmissionSha256")));
        var admission = PulsarNativeM4DrainAdmissionV1.load(capsule, expected);
        var control = binding.m4Control();
        var firstLoop = Executors.newSingleThreadExecutor();
        var secondLoop = Executors.newSingleThreadExecutor();
        var ownerLoop = Executors.newSingleThreadExecutor();
        PulsarNativeM4ReadOwnerV1 owner = null;
        CompletableFuture<PulsarNativeM4ReadOwnerV1> first = null;
        try {
            int beforeReads = reads.get();
            if (mode != 30) {
                responses.arm(control.initialOwnerClaimKey());
                var cleanup = failedOwnerCleanup(control.createReadOwner(
                        view, admission, bookKeeper, offloader, firstLoop, ownerLoop, 2, budget));
                assertThat(budget.usage().owners()).isEqualTo(2);
                assertThat(responses.delegate.get(control.selectorKey()).get(30, TimeUnit.SECONDS))
                        .isNull();
                var actual =
                        responses.delegate.get(control.initialOwnerClaimKey()).get(30, TimeUnit.SECONDS);
                assertThat(ByteBuffer.wrap(actual.value()).getLong(8)).isEqualTo(1);
                assertThat(ByteBuffer.wrap(actual.value()).getInt(16)).isEqualTo(1);
                responses.clear();
                responses.arm(
                        control.initialOwnerClaimKey(),
                        value -> ByteBuffer.wrap(value).getInt(16) == 2);
                assertThatThrownBy(() -> cleanup.reconcileCleanup().get(30, TimeUnit.SECONDS))
                        .isInstanceOf(java.util.concurrent.ExecutionException.class);
                assertThat(budget.usage().owners()).isEqualTo(2);
                var aborted =
                        responses.delegate.get(control.initialOwnerClaimKey()).get(30, TimeUnit.SECONDS);
                assertThat(ByteBuffer.wrap(aborted.value()).getInt(16)).isEqualTo(2);
                assertThat(ByteBuffer.wrap(aborted.value()).getLong(8)).isEqualTo(1);
                assertThat(responses.delegate.get(control.selectorKey()).get(30, TimeUnit.SECONDS))
                        .isNull();
                responses.clear();
                cleanup.reconcileCleanup().get(30, TimeUnit.SECONDS);
                assertThat(budget.usage().owners()).isEqualTo(1);
                if (mode == 32) {
                    var profile = responses
                            .delegate
                            .get(control.initialOwnerClaimKey() + "/profile")
                            .get(30, TimeUnit.SECONDS);
                    assertThat(responses
                                    .delegate
                                    .delete(control.initialOwnerClaimKey())
                                    .get(30, TimeUnit.SECONDS))
                            .isTrue();
                    assertThatThrownBy(() -> binding.m4Control()
                                    .createReadOwner(
                                            view, admission, bookKeeper, offloader, secondLoop, ownerLoop, 2, budget)
                                    .get(30, TimeUnit.SECONDS))
                            .hasRootCauseMessage(
                                    "Native initial claim head is missing or initialization belongs elsewhere");
                    assertThat(responses
                                    .delegate
                                    .get(control.initialOwnerClaimKey())
                                    .get(30, TimeUnit.SECONDS))
                            .isNull();
                    assertThat(responses
                                    .delegate
                                    .get(control.initialOwnerClaimKey() + "/profile")
                                    .get(30, TimeUnit.SECONDS)
                                    .value())
                            .containsExactly(profile.value());
                    assertThat(responses.delegate.get(control.selectorKey()).get(30, TimeUnit.SECONDS))
                            .isNull();
                    assertThat(budget.usage().owners()).isEqualTo(1);
                    assertThat(reads.get()).isEqualTo(beforeReads);
                    view.closePreparation().get(30, TimeUnit.SECONDS);
                    budget.closeCell().get(30, TimeUnit.SECONDS);
                    System.out.println(
                            "NATIVE_INITIAL_CLAIM_MISSING actualHeadRemoved=true permanentProfileRetained=true"
                                    + " noReinitialize=true noSelectorDispatch=true originalUnusedChargeOnly=true"
                                    + " wholeM5Authority=false");
                    return;
                }
                owner = control.createReadOwner(
                                view, admission, bookKeeper, offloader, secondLoop, ownerLoop, 2, budget)
                        .get(30, TimeUnit.SECONDS);
                var successor =
                        responses.delegate.get(control.initialOwnerClaimKey()).get(30, TimeUnit.SECONDS);
                assertThat(ByteBuffer.wrap(successor.value()).getLong(8)).isEqualTo(2);
                assertThat(ByteBuffer.wrap(successor.value()).getInt(16)).isEqualTo(1);
                assertThat(java.util.Arrays.copyOfRange(successor.value(), 20, 36))
                        .isNotEqualTo(java.util.Arrays.copyOfRange(actual.value(), 20, 36));
                System.out.println("NATIVE_INITIAL_CLAIM_UNKNOWN actualClaimApplied=true noSelectorDispatch=true"
                        + " abortUnknownKeepsCharge=true exactAbortThenSuccessor=true originalOperationOnly=true"
                        + " wholeM5Authority=false");
            } else {
                responses.holdAppliedPut(control.initialOwnerClaimKey(), value -> true);
                first = control.createReadOwner(
                        view, admission, bookKeeper, offloader, firstLoop, ownerLoop, 2, budget);
                assertThat(responses.heldApplied.await(30, TimeUnit.SECONDS)).isTrue();
                var claim =
                        responses.delegate.get(control.initialOwnerClaimKey()).get(30, TimeUnit.SECONDS);
                assertThat(claim).isNotNull();
                var other = binding.m4Control();
                assertThatThrownBy(() -> other.createReadOwner(
                                        view, admission, bookKeeper, offloader, secondLoop, ownerLoop, 2, budget)
                                .get(30, TimeUnit.SECONDS))
                        .hasRootCauseMessage("Native initial owner claim belongs to another invocation");
                assertThat(budget.usage().owners()).isEqualTo(2);
                assertThat(responses.delegate.get(control.selectorKey()).get(30, TimeUnit.SECONDS))
                        .isNull();
                assertThat(responses
                                .delegate
                                .get(control.initialOwnerClaimKey())
                                .get(30, TimeUnit.SECONDS)
                                .value())
                        .containsExactly(claim.value());
                assertThat(responses.heldRelease.complete(null)).isTrue();
                owner = first.get(30, TimeUnit.SECONDS);
                assertThat(responses
                                .delegate
                                .get(control.initialOwnerClaimKey())
                                .get(30, TimeUnit.SECONDS)
                                .value())
                        .containsExactly(claim.value());
                System.out.println("NATIVE_INITIAL_CLAIM_EXCLUSIVE actualClaimReplyHeld=true independentController=true"
                        + " competitorCannotCreateSelector=true originalClaimUnchanged=true originalActorDelivered=true"
                        + " wholeM5Authority=false");
            }
            assertThat(reads.get()).isEqualTo(beforeReads);
            owner.fence().get(30, TimeUnit.SECONDS);
            assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
            assertThat(budget.durableUsage().get(30, TimeUnit.SECONDS)).isEqualTo(budget.usage());
            budget.closeCell().get(30, TimeUnit.SECONDS);
        } finally {
            responses.clear();
            try {
                if (owner == null && first != null && !first.isCompletedExceptionally()) {
                    first.cancel(false);
                    if (!first.isCompletedExceptionally()) {
                        owner = first.getNow(null);
                    }
                }
                if (owner != null) {
                    owner.fence().get(30, TimeUnit.SECONDS);
                }
                budget.reconcileUndeliveredOwnerCleanup().get(30, TimeUnit.SECONDS);
                view.closePreparation().get(30, TimeUnit.SECONDS);
                budget.closeCell().get(30, TimeUnit.SECONDS);
            } finally {
                firstLoop.shutdown();
                secondLoop.shutdown();
                ownerLoop.shutdown();
                assertThat(firstLoop.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
                assertThat(secondLoop.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
                assertThat(ownerLoop.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    private static PulsarNativeM4ControlV1.OwnerCleanupException failedOwnerCleanup(CompletableFuture<?> attempt)
            throws Exception {
        try {
            attempt.get(30, TimeUnit.SECONDS);
            throw new AssertionError("Expected unresolved original owner cleanup");
        } catch (java.util.concurrent.ExecutionException failure) {
            Throwable cause = failure;
            while (!(cause instanceof PulsarNativeM4ControlV1.OwnerCleanupException) && cause.getCause() != null) {
                cause = cause.getCause();
            }
            assertThat(cause).isInstanceOf(PulsarNativeM4ControlV1.OwnerCleanupException.class);
            return (PulsarNativeM4ControlV1.OwnerCleanupException) cause;
        }
    }

    private void verifyNativeCellAllocator(
            PulsarNativeBindingV1 binding,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            Namespace objectNamespace,
            String bucket,
            String name,
            String root,
            PulsarTopicIncarnationIdentity incarnation,
            ExecutorService objectExecutor,
            ExecutorService fileExecutor,
            AtomicInteger reads,
            SelectorResponseFault responseFault,
            boolean loseDrainResponse)
            throws Exception {
        var hold = hold();
        var held = new AtomicReference<SdkHold>(hold);
        try (var metadata = metadata();
                var sourceClient = client(reads, held, null, null, true);
                var otherClient = OxiaClientBuilder.create(System.getProperty("nereus.m5.nativeOffloadOxia"))
                        .namespace("default")
                        .requestTimeout(Duration.ofSeconds(10))
                        .asyncClient()
                        .get(30, TimeUnit.SECONDS)) {
            var store = new S3PulsarOffloadObjectStoreV1(sourceClient, bucket, objectExecutor, false);
            var offloader = offloader(store, fileExecutor);
            var factory = factory(metadata, bookKeeper.borrowClient());
            ManagedLedgerImpl ledger = null;
            PulsarNativeSourcePreparationV1 preparation = null;
            try {
                ledger = (ManagedLedgerImpl) factory.open(name, config(offloader));
                var budget = readBudget(binding);
                var sibling = PulsarNativeBindingV1.read(otherClient, binding.metadataNamespace(), root, incarnation)
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS);
                var share = new PulsarNativeReadCellBudgetV1.Usage(3, 4, 1L << 30, 4L << 30);
                var competing = sibling.readCellBudget(share, Map.of(sibling.bindingIdentity(), share), readLimits());
                assertThat(competing.ownerKey()).isEqualTo(budget.ownerKey());
                assertThatThrownBy(() -> competing.admit().get(30, TimeUnit.SECONDS))
                        .hasRootCauseMessage("Native Cell allocator has no confirmed durable drain");
                var changed = sibling.readCellBudget(
                        new PulsarNativeReadCellBudgetV1.Usage(4, 4, 1L << 30, 4L << 30),
                        Map.of(sibling.bindingIdentity(), share),
                        readLimits());
                assertThatThrownBy(() -> changed.admit().get(30, TimeUnit.SECONDS))
                        .hasRootCauseMessage("Native Cell fixed profile differs");
                preparation = binding.prepareReadSources(ledger, bookKeeper, offloader, objectNamespace, budget, true)
                        .get(30, TimeUnit.SECONDS);
                assertThat(hold.started().await(30, TimeUnit.SECONDS)).isTrue();
                var active = otherClient.get(budget.ownerKey()).get(30, TimeUnit.SECONDS);
                var occupied = budget.usage();
                var closed = budget.closeCell();
                assertThat(closed.complete(null)).isTrue();
                var actualCellClose = budget.closeCell();
                assertThat(actualCellClose).isNotDone();
                assertThat(budget.usage()).isEqualTo(occupied);
                assertThat(otherClient
                                .get(budget.ownerKey())
                                .get(30, TimeUnit.SECONDS)
                                .value())
                        .containsExactly(active.value());
                assertThatThrownBy(() -> competing.admit().get(30, TimeUnit.SECONDS))
                        .hasRootCauseMessage("Native Cell allocator has no confirmed durable drain");
                var sourceClose = preparation.close();
                assertThatThrownBy(() -> sourceClose.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
                if (loseDrainResponse) {
                    responseFault.arm(budget.ownerKey(), budget::isDrainedOwnerValue);
                }
                hold.released().countDown();
                sourceClose.get(30, TimeUnit.SECONDS);
                if (loseDrainResponse) {
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                    while (responseFault.putLosses.get() == 0) {
                        assertThat(System.nanoTime()).isLessThan(deadline);
                        TimeUnit.MILLISECONDS.sleep(10);
                    }
                    assertThat(actualCellClose).isNotDone();
                    assertThat(otherClient
                                    .get(budget.ownerKey())
                                    .get(30, TimeUnit.SECONDS)
                                    .value())
                            .isNotEqualTo(active.value());
                    responseFault.clear();
                }
                budget.closeCell().get(30, TimeUnit.SECONDS);
                assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
                competing.admit().get(30, TimeUnit.SECONDS);
                assertThatThrownBy(() -> readBudget(binding))
                        .hasRootCauseMessage("Native Cell allocator has no confirmed durable drain");
                competing.closeCell().get(30, TimeUnit.SECONDS);
                System.out.println("NATIVE_CELL_ALLOCATOR actualIndependentSdk=true fixedProfile=true"
                        + " heldRootKeepsActive=true observerCannotDrain=true actualSourceDrain=true"
                        + " durableCloseUnknown=" + loseDrainResponse
                        + " newAllocatorAfterDrain=true wholeM5Authority=false");
            } finally {
                responseFault.clear();
                hold.released().countDown();
                try {
                    if (preparation != null) {
                        preparation.close().get(30, TimeUnit.SECONDS);
                    }
                    if (ledger != null) {
                        closeNativeLedger(ledger).get(30, TimeUnit.SECONDS);
                    }
                } finally {
                    factory.shutdown();
                    offloader.close();
                    store.close().toCompletableFuture().get(30, TimeUnit.SECONDS);
                }
            }
        }
    }

    private void verifyNativeOwnedVerification(
            PulsarNativeBindingV1 binding,
            PulsarNativeBindingReadViewV1 diagnostic,
            ManagedLedgerImpl ledger,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            Namespace objectNamespace,
            S3Client independent,
            String bucket,
            long offloaded,
            long plain,
            AtomicReference<SdkHold> heldGet,
            AtomicInteger reads,
            int mode)
            throws Exception {
        var budget = readBudget(binding);
        var view = preparedView(binding, ledger, bookKeeper, offloader, objectNamespace, budget, false);
        var capsule = Path.of(System.getProperty("nereus.m5.nativeM4AdmissionFile"));
        var sha = Sha256Digest.copyOf(
                java.util.HexFormat.of().parseHex(System.getProperty("nereus.m5.nativeM4AdmissionSha256")));
        var admission = PulsarNativeM4DrainAdmissionV1.load(capsule, sha);
        var ownerLoop = Executors.newSingleThreadExecutor();
        var controlLoop = Executors.newSingleThreadExecutor();
        var hold = hold();
        PulsarNativeM4ReadOwnerV1 owner = null;
        String replacement = null;
        String verifiedDataKey = null;
        try {
            var identity = ledger.captureOffloadReadSource(offloaded)
                    .get(30, TimeUnit.SECONDS)
                    .captureOffloadedReadSourceIdentity()
                    .get(30, TimeUnit.SECONDS);
            verifiedDataKey = identity.data().key();
            replacement = replaceWithIdenticalVersion(
                    independent,
                    bucket,
                    identity.data().key(),
                    identity.data().version().substring(8));
            owner = binding.m4Control()
                    .createReadOwner(view, admission, bookKeeper, offloader, controlLoop, ownerLoop, 2, budget)
                    .get(30, TimeUnit.SECONDS);
            int beforeBad = reads.get();
            assertThatThrownBy(
                            () -> view.verifyOwnedObjectSource(plain).result().get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("Native verification requires a captured Object source");
            assertThat(reads.get()).isEqualTo(beforeBad);
            heldGet.set(hold);
            var operation = view.verifyOwnedObjectSource(offloaded);
            var result = operation.result();
            assertThat(hold.started().await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(hold.key().get()).isEqualTo(identity.data().key());
            assertThat(hold.version().get()).isEqualTo(identity.data().version().substring(8));
            var occupied = budget.usage();
            assertThat(owner.hazardState()).isEqualTo(ScanOutcome.PINNED);
            if (mode == 17) {
                assertThat(result.cancel(false)).isTrue();
            } else if (mode == 18) {
                assertThat(result.complete(null)).isTrue();
            }
            assertThat(operation.drained().complete(null)).isTrue();
            var actualDrain = operation.drained();
            assertThat(actualDrain).isNotDone();
            if (mode == 16) {
                hold.released().countDown();
                result.get(30, TimeUnit.SECONDS);
                actualDrain.get(30, TimeUnit.SECONDS);
            } else {
                var fence = owner.fence();
                assertThatThrownBy(() -> fence.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
                assertThat(budget.usage()).isEqualTo(occupied);
                assertThat(owner.hazardState()).isEqualTo(ScanOutcome.PINNED);
                hold.released().countDown();
                actualDrain.get(30, TimeUnit.SECONDS);
                fence.get(30, TimeUnit.SECONDS);
            }
            owner.fence().get(30, TimeUnit.SECONDS);
            assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
            int afterFence = reads.get();
            assertThatThrownBy(() ->
                            view.verifyOwnedObjectSource(offloaded).result().get(30, TimeUnit.SECONDS))
                    .isInstanceOf(java.util.concurrent.ExecutionException.class);
            assertThat(reads.get()).isEqualTo(afterFence);
            System.out.println("NATIVE_M4_OPENED_VERIFICATION cachedExactVersion=true newHeadVersionIgnored=true"
                    + " heldSdkLease=true observer=" + mode + " privateDrain=true physicalDeleteAuthority=false");
        } finally {
            hold.released().countDown();
            if (owner != null) {
                owner.fence().get(30, TimeUnit.SECONDS);
            }
            if (replacement != null) {
                independent.deleteObject(DeleteObjectRequest.builder()
                        .bucket(bucket)
                        .key(verifiedDataKey)
                        .versionId(replacement)
                        .build());
            }
            ownerLoop.shutdown();
            controlLoop.shutdown();
            assertThat(ownerLoop.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            assertThat(controlLoop.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    private void verifyNativeSourcePreparation(
            PulsarNativeBindingV1 binding,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            Namespace objectNamespace,
            String bucket,
            String name,
            long offloaded,
            ExecutorService objectExecutor,
            ExecutorService fileExecutor,
            AtomicInteger reads,
            boolean holdRoot)
            throws Exception {
        var hold = hold();
        var held = new AtomicReference<SdkHold>(holdRoot ? hold : null);
        var ownerLoop = Executors.newSingleThreadExecutor();
        var controlLoop = Executors.newSingleThreadExecutor();
        try (var metadata = metadata();
                var actual = client(reads, held, null, null, true)) {
            var store = new S3PulsarOffloadObjectStoreV1(actual, bucket, objectExecutor, false);
            var offloader = offloader(store, fileExecutor);
            var factory = factory(metadata, bookKeeper.borrowClient());
            ManagedLedgerImpl ledger = null;
            PulsarNativeSourcePreparationV1 preparation = null;
            try {
                ledger = (ManagedLedgerImpl) factory.open(name, config(offloader));
                var budget = readBudget(binding);
                preparation = binding.prepareReadSources(ledger, bookKeeper, offloader, objectNamespace, budget, true)
                        .get(30, TimeUnit.SECONDS);
                if (holdRoot) {
                    assertThat(hold.started().await(30, TimeUnit.SECONDS)).isTrue();
                    var occupied = budget.usage();
                    assertThat(occupied.owners()).isEqualTo(1);
                    assertThat(occupied.readSlots()).isZero();
                    assertThat(occupied.encodedBytes()).isPositive();
                    assertThat(occupied.decodedBytes()).isPositive();
                    assertThat(preparation.view().cancel(false)).isTrue();
                    var closedObserver = preparation.close();
                    assertThat(closedObserver.complete(null)).isTrue();
                    var actualClose = preparation.close();
                    assertThatThrownBy(() -> actualClose.get(1, TimeUnit.SECONDS))
                            .isInstanceOf(TimeoutException.class);
                    assertThat(budget.usage()).isEqualTo(occupied);
                    hold.released().countDown();
                    actualClose.get(30, TimeUnit.SECONDS);
                } else {
                    var view = preparation.view().get(30, TimeUnit.SECONDS);
                    int beforeGrant = reads.get();
                    assertThatThrownBy(() -> view.readOwnedOperation(offloaded, 0, 0, OWNED_READ_LIMITS, () -> true)
                                    .result()
                                    .get(30, TimeUnit.SECONDS))
                            .hasRootCauseMessage("Native source preparation has no read grant");
                    assertThat(reads.get()).isEqualTo(beforeGrant);
                    var capsule = Path.of(System.getProperty("nereus.m5.nativeM4AdmissionFile"));
                    var sha = Sha256Digest.copyOf(
                            java.util.HexFormat.of().parseHex(System.getProperty("nereus.m5.nativeM4AdmissionSha256")));
                    var admission = PulsarNativeM4DrainAdmissionV1.load(capsule, sha);
                    var owner = binding.m4Control()
                            .createReadOwner(view, admission, bookKeeper, offloader, controlLoop, ownerLoop, 2, budget)
                            .get(30, TimeUnit.SECONDS);
                    assertThat(budget.usage().owners()).isEqualTo(2);
                    var delivered = readNativeEntry(ledger, offloaded).get(30, TimeUnit.SECONDS);
                    try {
                        assertThat(delivered.getData()).containsExactly(payload(0));
                    } finally {
                        delivered.release();
                    }
                    owner.fence().get(30, TimeUnit.SECONDS);
                    preparation.close().get(30, TimeUnit.SECONDS);
                }
                assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
                System.out.println("NATIVE_SOURCE_PREPARATION actualRootOpen=" + holdRoot
                        + " budgetBeforeSource=true observerCannotRelease=true preGrantReadRejected=" + !holdRoot
                        + " actualFenceReturnsCapacity=true allReaderOrPhysicalDeleteAuthority=false");
            } finally {
                hold.released().countDown();
                try {
                    if (preparation != null) {
                        preparation.close().get(30, TimeUnit.SECONDS);
                    }
                    if (ledger != null) {
                        closeNativeLedger(ledger).get(30, TimeUnit.SECONDS);
                    }
                } finally {
                    factory.shutdown();
                    offloader.close();
                    store.close().toCompletableFuture().get(30, TimeUnit.SECONDS);
                }
            }
        } finally {
            ownerLoop.shutdown();
            controlLoop.shutdown();
            assertThat(ownerLoop.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            assertThat(controlLoop.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    private void verifyNativeCellShares(
            PulsarNativeBindingV1 binding,
            PulsarNativeBindingReadViewV1 diagnostic,
            ManagedLedgerImpl ledger,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            Namespace objectNamespace,
            ManagedLedgerFactoryImpl factory,
            PulsarTopicAuthorityCoordinator publisher,
            AsyncOxiaClient client,
            String root,
            long offloaded,
            long tail,
            AtomicReference<SdkHold> heldGet,
            AtomicInteger reads)
            throws Exception {
        var incarnation = new PulsarTopicIncarnationIdentity(
                PulsarPersistenceName.fromString("tenant/ns/persistent/native-cell-share-healthy"),
                PulsarTopicName.fromString("persistent://tenant/ns/native-cell-share-healthy"),
                new PulsarBindingGeneration(1));
        var cell = binding.authority().aggregate().binding().cellIdentity();
        var id = DeterministicTopicIdsV1.deriveBindingId(cell, incarnation);
        var aggregate = new TopicBindingAggregateV1(
                TopicBindingAggregateV1.SCHEMA_VERSION,
                new TopicBindingV1(ProtocolKindV1.PULSAR, id, cell, incarnation),
                new InitialStorageEpochV1(
                        DeterministicTopicIdsV1.deriveStorageEpochId(id, 0),
                        0,
                        StorageProfileV1.BOOKKEEPER_WAL_ASYNC_OBJECT,
                        ProfileOriginV1.TOPIC_EXPLICIT,
                        new PolicyCatalogDigest(Sha256Digest.hash(
                                CanonicalBytes.copyOf("m5-native-real".getBytes(StandardCharsets.UTF_8)))),
                        FrameEncodingPolicyValueV1.none()));
        var bytes = Nta1CodecV1.encode(aggregate);
        publisher
                .activate(new AggregatePublicationCandidate(aggregate, bytes, Sha256Digest.hash(bytes)))
                .toCompletableFuture()
                .get(30, TimeUnit.SECONDS);
        var healthyBinding = PulsarNativeBindingV1.read(client, binding.metadataNamespace(), root, incarnation)
                .toCompletableFuture()
                .get(30, TimeUnit.SECONDS);
        var config = config(offloader).setMaxEntriesPerLedger(32);
        config.setProperties(Map.of("user-property", "preserved"));
        healthyBinding.configureCreate(config);
        var healthy = (ManagedLedgerImpl)
                factory.open(incarnation.persistenceName().value().value(), config);
        var ownerLoop = Executors.newSingleThreadExecutor();
        var controlLoop = Executors.newSingleThreadExecutor();
        SdkHold hold = hold();
        Entry alias = null;
        PulsarNativeM4ReadOwnerV1 owner = null;
        PulsarNativeM4ReadOwnerV1 healthyOwner = null;
        try {
            var position = healthy.addEntry(payload(99));
            for (int ordinal = 0; ordinal < 16; ordinal++) {
                healthy.addEntry(payload(100 + ordinal));
            }
            var healthyDiagnostic = healthyBinding
                    .captureReadView(healthy, bookKeeper, offloader, objectNamespace)
                    .get(30, TimeUnit.SECONDS);
            var share = new PulsarNativeReadCellBudgetV1.Usage(2, 2, 1L << 30, 2L << 30);
            var budget = binding.readCellBudget(
                            new PulsarNativeReadCellBudgetV1.Usage(4, 4, 2L << 30, 4L << 30),
                            Map.of(binding.bindingIdentity(), share, healthyBinding.bindingIdentity(), share),
                            readLimits())
                    .admit()
                    .get(30, TimeUnit.SECONDS);
            var capsule = Path.of(System.getProperty("nereus.m5.nativeM4AdmissionFile"));
            var digest = Sha256Digest.copyOf(
                    java.util.HexFormat.of().parseHex(System.getProperty("nereus.m5.nativeM4AdmissionSha256")));
            var admission = PulsarNativeM4DrainAdmissionV1.load(capsule, digest);
            var tiny = new PulsarNativeReadCellBudgetV1.Usage(1, 2, 1, 1);
            var insufficient = binding.readCellBudget(tiny, Map.of(binding.bindingIdentity(), tiny), readLimits());
            assertThatThrownBy(() -> binding.prepareReadSources(
                                    ledger, bookKeeper, offloader, objectNamespace, insufficient, false)
                            .get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("Native Binding or Cell owner capacity exhausted");
            assertThat(insufficient.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
            assertThat(binding.m4Control().readFacts(diagnostic).get(30, TimeUnit.SECONDS))
                    .isEmpty();
            var view = preparedView(binding, ledger, bookKeeper, offloader, objectNamespace, budget, false);
            var healthyView =
                    preparedView(healthyBinding, healthy, bookKeeper, offloader, objectNamespace, budget, false);
            var control = binding.m4Control();
            owner = control.createReadOwner(view, admission, bookKeeper, offloader, controlLoop, ownerLoop, 2, budget)
                    .get(30, TimeUnit.SECONDS);
            int beforeMemoryRejection = reads.get();
            var oversizedCopy = new CapturedReadLimits(1, LIMITS.maxEntryBytes(), LIMITS.maxDecodedBlockBytes() + 1);
            assertThatThrownBy(() -> view.readOwnedOperation(offloaded, 0, 0, oversizedCopy, () -> true)
                            .result()
                            .get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("Native owned copy exceeds its reserved memory profile");
            assertThat(reads.get()).isEqualTo(beforeMemoryRejection);
            var occupiedBytes = budget.bindingUsage().get(binding.bindingIdentity());
            var entry = readNativeEntry(ledger, tail).get(30, TimeUnit.SECONDS);
            alias = EntryImpl.create(entry);
            entry.release();
            heldGet.set(hold);
            var pending = view.readOwnedOperation(offloaded, 0, 0, OWNED_READ_LIMITS, () -> true);
            assertThat(hold.started().await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(pending.result().cancel(false)).isTrue();
            var fence = owner.fence();
            assertThat(fence.cancel(false)).isTrue();
            var actualFenceObserver = owner.fence();
            assertThatThrownBy(() -> actualFenceObserver.get(1, TimeUnit.SECONDS))
                    .isInstanceOf(TimeoutException.class);
            assertNativeCapacity(budget, 3, 2);
            assertThat(budget.bindingUsage().get(binding.bindingIdentity())).isEqualTo(occupiedBytes);
            int beforeRejected = reads.get();
            assertThatThrownBy(() -> control.createReadOwner(
                                    view, admission, bookKeeper, offloader, controlLoop, ownerLoop, 2, budget)
                            .get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("Native Binding or Cell owner capacity exhausted");
            assertThat(reads.get()).isEqualTo(beforeRejected);
            healthyOwner = healthyBinding
                    .m4Control()
                    .createReadOwner(healthyView, admission, bookKeeper, offloader, controlLoop, ownerLoop, 2, budget)
                    .get(30, TimeUnit.SECONDS);
            var excessRange = new CapturedReadLimits(17, 1, LIMITS.maxDecodedBlockBytes());
            assertThatThrownBy(() -> healthyView
                            .readOwnedOperation(position.getLedgerId(), 0, 16, excessRange, () -> true)
                            .result()
                            .get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("Native M4 range exceeds its reserved memory profile");
            var delivered = readNativeEntry(healthy, position.getLedgerId()).get(30, TimeUnit.SECONDS);
            try {
                assertThat(delivered.getData()).containsExactly(payload(99));
            } finally {
                delivered.release();
            }
            assertNativeCapacity(budget, 4, 4);
            healthyOwner.fence().get(30, TimeUnit.SECONDS);
            assertNativeCapacity(budget, 2, 2);
            hold.released().countDown();
            pending.drained().get(30, TimeUnit.SECONDS);
            assertThat(owner.fence()).isNotDone();
            assertNativeCapacity(budget, 2, 2);
            assertThat(budget.bindingUsage().get(binding.bindingIdentity())).isEqualTo(occupiedBytes);
            alias.release();
            alias = null;
            owner.fence().get(30, TimeUnit.SECONDS);
            assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
            System.out.println("NATIVE_M4_CELL_SHARES actualTwoBindings=true heldGetAndAlias=true"
                    + " timeoutCancellationRetain=true healthyBindingReads=true confirmedDrainReturnsShares=true"
                    + " byteBudgetOrWholeM5Authority=false");
        } finally {
            hold.released().countDown();
            if (alias != null) {
                alias.release();
            }
            if (owner != null) {
                owner.fence().get(30, TimeUnit.SECONDS);
            }
            if (healthyOwner != null) {
                healthyOwner.fence().get(30, TimeUnit.SECONDS);
            }
            closeNativeLedger(healthy).get(30, TimeUnit.SECONDS);
            ownerLoop.shutdown();
            controlLoop.shutdown();
            assertThat(ownerLoop.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            assertThat(controlLoop.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static PulsarNativeBindingReadViewV1 preparedView(
            PulsarNativeBindingV1 binding,
            ManagedLedgerImpl ledger,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            Namespace objects,
            PulsarNativeReadCellBudgetV1 budget,
            boolean preferredOnly)
            throws Exception {
        return binding.prepareReadSources(ledger, bookKeeper, offloader, objects, budget, preferredOnly)
                .get(30, TimeUnit.SECONDS)
                .view()
                .get(30, TimeUnit.SECONDS);
    }

    private static PulsarNativeReadCellBudgetV1.ReadLimits readLimits() {
        return new PulsarNativeReadCellBudgetV1.ReadLimits(16, LIMITS.maxEntryBytes(), LIMITS.maxDecodedBlockBytes());
    }

    private static void assertNativeCapacity(PulsarNativeReadCellBudgetV1 budget, long owners, long slots) {
        var observed = budget.usage();
        assertThat(observed.owners()).isEqualTo(owners);
        assertThat(observed.readSlots()).isEqualTo(slots);
        assertThat(observed.encodedBytes()).isPositive();
        assertThat(observed.decodedBytes()).isPositive();
    }

    private static PulsarNativeReadCellBudgetV1 readBudget(PulsarNativeBindingV1 binding) throws Exception {
        return readBudget(binding, 3);
    }

    private static PulsarNativeReadCellBudgetV1 readBudget(PulsarNativeBindingV1 binding, int owners) throws Exception {
        var share = new PulsarNativeReadCellBudgetV1.Usage(owners, 4, 1L << 30, 4L << 30);
        return binding.readCellBudget(share, Map.of(binding.bindingIdentity(), share), readLimits())
                .admit()
                .get(30, TimeUnit.SECONDS);
    }

    private static BindingReadSelector readNativeSelector(AsyncOxiaClient client, String key) throws Exception {
        var stored =
                CanonicalBytes.copyOf(client.get(key).get(30, TimeUnit.SECONDS).value());
        return M5BindingAuthorityCodecV1.isAuthorityValue(stored)
                ? M5BindingAuthorityCodecV1.decodeAuthority(stored).selectorProjection()
                : M4ReadControlCodecV1.decodeSelector(stored);
    }

    private void verifyNativeM4Retirement(
            PulsarNativeBindingV1 binding,
            PulsarNativeBindingReadViewV1 diagnostic,
            ManagedLedgerImpl ledger,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            Namespace objectNamespace,
            String bucket,
            String name,
            long offloaded,
            long tail,
            ExecutorService objectExecutor,
            ExecutorService fileExecutor,
            AtomicInteger reads,
            AtomicReference<SdkHold> heldGet,
            SelectorResponseFault selectorFault,
            AsyncOxiaClient independent,
            int mode)
            throws Exception {
        var capsule = Path.of(System.getProperty("nereus.m5.nativeM4AdmissionFile"));
        var expected = Sha256Digest.copyOf(
                java.util.HexFormat.of().parseHex(System.getProperty("nereus.m5.nativeM4AdmissionSha256")));
        var admission = PulsarNativeM4DrainAdmissionV1.load(capsule, expected);
        var control = binding.m4Control();
        var cellBudget = readBudget(binding);
        var original = preparedView(binding, ledger, bookKeeper, offloader, objectNamespace, cellBudget, false);
        var ownerLoop = Executors.newSingleThreadExecutor();
        var controlLoop = Executors.newSingleThreadExecutor();
        SdkHold hold = hold();
        Entry alias = null;
        try (var coldMetadata = metadata();
                var coldClient = client(reads, null, null, null)) {
            var owner = control.createReadOwner(
                            original, admission, bookKeeper, offloader, controlLoop, ownerLoop, 2, cellBudget)
                    .get(30, TimeUnit.SECONDS);
            var originalMap = control.readSelectedSourceMap().get(30, TimeUnit.SECONDS);
            assertThat(originalMap.selectedViewSha256()).isEqualTo(original.sha256());
            assertThat(originalMap.actualViewBytes()).isEqualTo(original.canonicalBytes());
            var entry = readNativeEntry(ledger, tail).get(30, TimeUnit.SECONDS);
            alias = EntryImpl.create(entry);
            entry.release();
            heldGet.set(hold);
            var oldRead = original.readOwnedOperation(offloaded, 0, 0, OWNED_READ_LIMITS, () -> true);
            assertThat(hold.started().await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(oldRead.result().cancel(false)).isTrue();
            var oldFence = owner.fence();
            var oldClose = closeNativeLedger(ledger);
            assertThat(oldFence).isNotDone();
            assertThat(oldClose).isNotDone();
            var coldStore = new S3PulsarOffloadObjectStoreV1(coldClient, bucket, objectExecutor, false);
            var coldOffloader = offloader(coldStore, fileExecutor);
            var coldFactory = factory(coldMetadata, bookKeeper.borrowClient());
            ManagedLedgerImpl cold = null;
            try {
                cold = (ManagedLedgerImpl) coldFactory.open(name, config(coldOffloader));
                var preferred =
                        preparedView(binding, cold, bookKeeper, coldOffloader, objectNamespace, cellBudget, true);
                if (mode == 8) {
                    selectorFault.arm(control.selectorKey());
                } else if (mode == 9 || mode == 24) {
                    selectorFault.arm(control.sourceMapKey(preferred.sha256()));
                }
                var retirement = control.retireFallback(owner, preferred, bookKeeper, coldOffloader, controlLoop);
                if (mode == 8 || mode == 9 || mode == 24) {
                    assertThatThrownBy(() -> retirement.get(30, TimeUnit.SECONDS))
                            .hasRootCauseMessage("actual selector observation unavailable");
                    assertThat(selectorFault.putLosses.get()).isEqualTo(1);
                    assertThat(selectorFault.getLosses.get()).isGreaterThan(0);
                } else {
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                    while (true) {
                        var observed = readNativeSelector(independent, control.selectorKey());
                        if (observed.mode() == SelectorMode.PREFERRED_ONLY) {
                            break;
                        }
                        assertThat(System.nanoTime()).isLessThan(deadline);
                        TimeUnit.MILLISECONDS.sleep(25);
                    }
                    assertThat(retirement).isNotDone();
                    assertThat(retirement.cancel(false)).isTrue();
                }
                var durable = readNativeSelector(independent, control.selectorKey());
                if (mode == 9 || mode == 24) {
                    assertThat(durable).isEqualTo(owner.selector());
                } else {
                    assertThat(durable.mode()).isEqualTo(SelectorMode.PREFERRED_ONLY);
                    assertThat(durable.readAdmissionEpoch())
                            .isEqualTo(owner.selector().readAdmissionEpoch() + 1);
                }
                var publishedMap = PulsarNativeM4SourceMapV1.decode(
                        CanonicalBytes.copyOf(independent
                                .get(control.sourceMapKey(preferred.sha256()))
                                .get(30, TimeUnit.SECONDS)
                                .value()),
                        preferred.sha256());
                assertThat(publishedMap.actualViewBytes()).isEqualTo(preferred.canonicalBytes());
                var source = owner.fallbackSources().get(0);
                assertThat(M4ReadControlCodecV1.decodeProtection(CanonicalBytes.copyOf(independent
                                        .get(control.protectionKey(source))
                                        .get(30, TimeUnit.SECONDS)
                                        .value()))
                                .state())
                        .isEqualTo(ProtectionState.PROTECTED);
                assertThat(independent
                                .get(control.terminalKey(owner.selector().readAdmissionEpoch()))
                                .get(30, TimeUnit.SECONDS))
                        .isNull();
                assertThat(independent
                                .get(control.proofKey(owner.selector().readAdmissionEpoch()))
                                .get(30, TimeUnit.SECONDS))
                        .isNull();
                assertThat(owner.hazardState()).isEqualTo(ScanOutcome.PINNED);
                hold.released().countDown();
                oldRead.drained().get(30, TimeUnit.SECONDS);
                assertThat(oldFence).isNotDone();
                var hazardDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                var observedHazard = owner.hazardState();
                while (observedHazard == ScanOutcome.INCONCLUSIVE) {
                    assertThat(System.nanoTime()).isLessThan(hazardDeadline);
                    TimeUnit.MILLISECONDS.sleep(1);
                    observedHazard = owner.hazardState();
                }
                assertThat(observedHazard).isEqualTo(ScanOutcome.PINNED);
                alias.release();
                alias = null;
                oldFence.get(30, TimeUnit.SECONDS);
                oldClose.get(30, TimeUnit.SECONDS);
                selectorFault.clear();
                if (mode == 24) {
                    int beforeCleanup = reads.get();
                    selectorFault.arm(cellBudget.ownerKey(), cellBudget::hasDurableReservationValue);
                    var cleanup = failedOwnerCleanup(
                            control.retireFallback(owner, preferred, bookKeeper, coldOffloader, controlLoop));
                    assertThat(selectorFault.putLosses.get()).isEqualTo(2);
                    assertThat(cellBudget.usage().owners()).isEqualTo(2);
                    assertThat(reads.get()).isEqualTo(beforeCleanup);
                    assertThat(M4ReadControlCodecV1.decodeProtection(CanonicalBytes.copyOf(independent
                                            .get(control.protectionKey(source))
                                            .get(30, TimeUnit.SECONDS)
                                            .value()))
                                    .state())
                            .isEqualTo(ProtectionState.RELEASED);
                    assertThat(failedOwnerCleanup(
                                    control.retireFallback(owner, preferred, bookKeeper, coldOffloader, controlLoop)))
                            .isSameAs(cleanup);
                    assertThatThrownBy(() -> cleanup.reconcileCleanup().get(30, TimeUnit.SECONDS))
                            .hasRootCauseMessage("actual selector observation unavailable");
                    selectorFault.clear();
                    cleanup.reconcileCleanup().get(30, TimeUnit.SECONDS);
                    assertThat(cellBudget.usage().owners()).isEqualTo(1);
                    assertThat(cellBudget.durableUsage().get(30, TimeUnit.SECONDS))
                            .isEqualTo(cellBudget.usage());
                    assertThat(reads.get()).isEqualTo(beforeCleanup);
                    preferred.closePreparation().get(30, TimeUnit.SECONDS);
                    cellBudget.closeCell().get(30, TimeUnit.SECONDS);
                    System.out.println("NATIVE_SUCCESSOR_OWNER_CLEANUP releasedPredecessor=true reserveUnknown=true"
                            + " noNewActorOrRead=true originalCleanupRetained=true exactRetry=true"
                            + " wholeM5Authority=false");
                    return;
                }
                var released = control.retireFallback(owner, preferred, bookKeeper, coldOffloader, controlLoop)
                        .get(30, TimeUnit.SECONDS);
                if (mode != 9) {
                    assertThat(released.selector()).isEqualTo(durable);
                } else {
                    assertThat(released.selector().mode()).isEqualTo(SelectorMode.PREFERRED_ONLY);
                }
                assertThat(M4ReadControlCodecV1.decodeTerminal(CanonicalBytes.copyOf(independent
                                .get(control.terminalKey(released.terminal().readAdmissionEpoch()))
                                .get(30, TimeUnit.SECONDS)
                                .value())))
                        .isEqualTo(released.terminal());
                assertThat(M4ReadControlCodecV1.decodeProof(CanonicalBytes.copyOf(independent
                                .get(control.proofKey(released.proof().readAdmissionEpoch()))
                                .get(30, TimeUnit.SECONDS)
                                .value())))
                        .isEqualTo(released.proof());
                assertThat(M4ReadControlCodecV1.decodeProtection(CanonicalBytes.copyOf(independent
                                        .get(control.protectionKey(source))
                                        .get(30, TimeUnit.SECONDS)
                                        .value()))
                                .state())
                        .isEqualTo(ProtectionState.RELEASED);
                assertThat(control.retireFallback(owner, preferred, bookKeeper, coldOffloader, controlLoop)
                                .get(30, TimeUnit.SECONDS))
                        .isSameAs(released);
                var reloaded = binding.m4Control().readSelectedSourceMap().get(30, TimeUnit.SECONDS);
                assertThat(reloaded.actualViewBytes()).isEqualTo(preferred.canonicalBytes());
                var corrupt = reloaded.encode().toByteArray();
                corrupt[corrupt.length - 1] ^= 1;
                assertThatThrownBy(() ->
                                PulsarNativeM4SourceMapV1.decode(CanonicalBytes.copyOf(corrupt), preferred.sha256()))
                        .hasMessageContaining("selected view digest");
                var nextRead = preferred.readOwnedOperation(offloaded, 0, 0, OWNED_READ_LIMITS, () -> true);
                assertThat(nextRead.result()
                                .get(30, TimeUnit.SECONDS)
                                .entries()
                                .get(0)
                                .payload())
                        .containsExactly(payload(0));
                nextRead.drained().get(30, TimeUnit.SECONDS);
                if (mode == 11 || mode == 12) {
                    verifyNativeColdOwnerRecovery(
                            binding,
                            released.successorOwner(),
                            cold,
                            admission,
                            bookKeeper,
                            objectNamespace,
                            bucket,
                            name,
                            offloaded,
                            objectExecutor,
                            fileExecutor,
                            ownerLoop,
                            controlLoop,
                            reads,
                            selectorFault,
                            independent,
                            mode == 12);
                } else {
                    released.successorOwner().fence().get(30, TimeUnit.SECONDS);
                }
                if (mode == 10) {
                    closeNativeLedger(cold).get(30, TimeUnit.SECONDS);
                    var namespace = bookKeeper
                            .readBinding()
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow();
                    var objects = coldOffloader
                            .observePhysicalNamespace(objectNamespace, namespace.metadataNamespace())
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS);
                    var published = reloaded.publishedObjects(binding, namespace, objects)
                            .get(offloaded);
                    verifyPublishedColdVersions(
                            binding,
                            control,
                            bookKeeper,
                            objectNamespace,
                            bucket,
                            name,
                            published,
                            objectExecutor,
                            fileExecutor,
                            reads);
                }
                System.out.println("NATIVE_M4_FALLBACK_RETIRED actualFusedSelector=true heldGetAndAlias=true"
                        + " exactTerminalAndProof=true protectionReleased=true successorRead=true unknown="
                        + (mode == 8)
                        + " sourceMapUnknown=" + (mode == 9)
                        + " actualSourceMapReload=true physicalDeleteAuthority=false");
            } finally {
                hold.released().countDown();
                if (alias != null) {
                    alias.release();
                    alias = null;
                }
                selectorFault.clear();
                try {
                    if (cold != null) {
                        closeNativeLedger(cold).get(30, TimeUnit.SECONDS);
                    }
                } finally {
                    coldFactory.shutdown();
                    coldOffloader.close();
                    coldStore.close().toCompletableFuture().get(30, TimeUnit.SECONDS);
                }
            }
        } finally {
            hold.released().countDown();
            if (alias != null) {
                alias.release();
            }
            selectorFault.clear();
            ownerLoop.shutdown();
            controlLoop.shutdown();
            assertThat(ownerLoop.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            assertThat(controlLoop.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    private void verifyNativeColdOwnerRecovery(
            PulsarNativeBindingV1 binding,
            PulsarNativeM4ReadOwnerV1 predecessor,
            ManagedLedgerImpl previousLedger,
            PulsarNativeM4DrainAdmissionV1 admission,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            Namespace objectNamespace,
            String bucket,
            String name,
            long offloaded,
            ExecutorService objectExecutor,
            ExecutorService fileExecutor,
            Executor ownerLoop,
            Executor controlLoop,
            AtomicInteger reads,
            SelectorResponseFault selectorFault,
            AsyncOxiaClient independent,
            boolean loseGrantResponse)
            throws Exception {
        var entry = readNativeEntry(previousLedger, offloaded).get(30, TimeUnit.SECONDS);
        Entry alias = EntryImpl.create(entry);
        entry.release();
        var drainObserver = predecessor.drainForRecovery(controlLoop);
        assertThat(drainObserver).isNotDone();
        assertThat(drainObserver.cancel(false)).isTrue();
        var previousClose = closeNativeLedger(previousLedger);
        var coldControl = binding.m4Control();
        try (var metadata = metadata();
                var sourceClient = client(reads, null, null, null)) {
            var store = new S3PulsarOffloadObjectStoreV1(sourceClient, bucket, objectExecutor, false);
            var offloader = offloader(store, fileExecutor);
            var factory = factory(metadata, bookKeeper.borrowClient());
            ManagedLedgerImpl cold = null;
            PulsarNativeM4ReadOwnerV1 recovered = null;
            try {
                cold = (ManagedLedgerImpl) factory.open(name, config(offloader));
                var candidate = cold;
                var sources = coldControl
                        .preparePublishedReadSources(
                                cold, bookKeeper, offloader, objectNamespace, predecessor.cellBudget())
                        .get(30, TimeUnit.SECONDS);
                sources.view().get(30, TimeUnit.SECONDS);
                assertThatThrownBy(() -> coldControl
                                .prepareColdReadOwner(sources, admission, bookKeeper, offloader, controlLoop)
                                .get(30, TimeUnit.SECONDS))
                        .hasRootCauseMessage("Native current owner has no confirmed durable read drain");
                assertThat(independent
                                .get(coldControl.ownerDrainKey(predecessor.selector()))
                                .get(30, TimeUnit.SECONDS))
                        .isNull();
                assertThat(readNativeSelector(independent, coldControl.selectorKey()))
                        .isEqualTo(predecessor.selector());
                assertThat(previousClose).isNotDone();
                assertThat(predecessor.hazardState()).isEqualTo(ScanOutcome.PINNED);
                alias.release();
                alias = null;
                predecessor.drainForRecovery(controlLoop).get(30, TimeUnit.SECONDS);
                previousClose.get(30, TimeUnit.SECONDS);
                assertThat(independent
                                .get(coldControl.ownerDrainKey(predecessor.selector()))
                                .get(30, TimeUnit.SECONDS))
                        .isNotNull();
                var recovery = coldControl
                        .prepareColdReadOwner(sources, admission, bookKeeper, offloader, controlLoop)
                        .get(30, TimeUnit.SECONDS);
                var competing = coldControl
                        .prepareColdReadOwner(sources, admission, bookKeeper, offloader, controlLoop)
                        .get(30, TimeUnit.SECONDS);
                if (loseGrantResponse) {
                    selectorFault.arm(coldControl.selectorKey());
                    assertThatThrownBy(() -> recovery.recover(controlLoop, ownerLoop, 2, predecessor.cellBudget())
                                    .get(30, TimeUnit.SECONDS))
                            .hasRootCauseMessage("actual selector observation unavailable");
                    assertThat(selectorFault.putLosses.get()).isEqualTo(1);
                    assertThat(selectorFault.getLosses.get()).isGreaterThan(0);
                    assertThat(readNativeSelector(independent, coldControl.selectorKey())
                                    .ownerEpoch())
                            .isEqualTo(predecessor.selector().ownerEpoch() + 1);
                    assertThat(predecessor.authority().admitting()).isFalse();
                    assertNativeCapacity(predecessor.cellBudget(), 2, 2);
                    selectorFault.clear();
                }
                recovered = recovery.recover(controlLoop, ownerLoop, 2, predecessor.cellBudget())
                        .get(30, TimeUnit.SECONDS);
                assertThat(recovered.authority().ownerEpoch())
                        .isEqualTo(predecessor.selector().ownerEpoch() + 1);
                assertThat(recovered.authority().readAdmissionEpoch())
                        .isEqualTo(predecessor.selector().readAdmissionEpoch() + 1);
                assertThat(recovered.authority().sourceGeneration())
                        .isEqualTo(predecessor.selector().sourceGeneration() + 1);
                assertThat(recovery.recover(controlLoop, ownerLoop, 2, predecessor.cellBudget())
                                .get(30, TimeUnit.SECONDS))
                        .isSameAs(recovered);
                assertThatThrownBy(() -> competing
                                .recover(controlLoop, ownerLoop, 2, predecessor.cellBudget())
                                .get(30, TimeUnit.SECONDS))
                        .hasRootCauseMessage("Native recovery publication is unknown or belongs to another attempt");
                assertNativeCapacity(predecessor.cellBudget(), 2, 2);
                assertThatThrownBy(() -> coldControl
                                .prepareColdReadOwner(sources, admission, bookKeeper, offloader, controlLoop)
                                .get(30, TimeUnit.SECONDS))
                        .hasRootCauseMessage("Native current owner has no confirmed durable read drain");
                var ordinary = readNativeEntry(cold, offloaded).get(30, TimeUnit.SECONDS);
                try {
                    assertThat(ordinary.getData()).containsExactly(payload(0));
                } finally {
                    ordinary.release();
                }
                assertThat(coldControl
                                .readSelectedSourceMap()
                                .get(30, TimeUnit.SECONDS)
                                .selectedViewSha256())
                        .isEqualTo(recovered.authority().selectedViewSha256());
                System.out.println("NATIVE_M4_COLD_OWNER actualDurableReadDrain=true aliasBlocksGrant=true"
                        + " observerCancellationIgnored=true newController=true uniqueGrant=true ordinaryRead=true"
                        + " exactGrantRetry=" + loseGrantResponse + " lostActorOrPhysicalDeleteAuthority=false");
            } finally {
                selectorFault.clear();
                if (alias != null) {
                    alias.release();
                    alias = null;
                }
                try {
                    if (recovered != null) {
                        recovered.fence().get(30, TimeUnit.SECONDS);
                        assertThat(predecessor.cellBudget().usage())
                                .isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
                    }
                    if (cold != null) {
                        closeNativeLedger(cold).get(30, TimeUnit.SECONDS);
                    }
                } finally {
                    factory.shutdown();
                    offloader.close();
                    store.close().toCompletableFuture().get(30, TimeUnit.SECONDS);
                }
            }
        } finally {
            if (alias != null) {
                alias.release();
            }
        }
    }

    private void verifyPublishedColdVersions(
            PulsarNativeBindingV1 binding,
            PulsarNativeM4ControlV1 control,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            Namespace objectNamespace,
            String bucket,
            String name,
            PulsarNativeM4SourceMapV1.PublishedObject published,
            ExecutorService objectExecutor,
            ExecutorService fileExecutor,
            AtomicInteger reads)
            throws Exception {
        try (var actual = client(reads, null, null, null)) {
            var identity = published.identity();
            String rootReplacement = replaceWithIdenticalVersion(
                    actual,
                    bucket,
                    identity.root().key(),
                    identity.root().version().substring(8));
            String dataReplacement = replaceWithIdenticalVersion(
                    actual,
                    bucket,
                    identity.data().key(),
                    identity.data().version().substring(8));
            try {
                assertThat(rootReplacement)
                        .isNotEqualTo(identity.root().version().substring(8));
                assertThat(dataReplacement)
                        .isNotEqualTo(identity.data().version().substring(8));
                for (int mode = 0; mode < 2; mode++) {
                    try (var metadata = metadata();
                            var sourceClient = client(reads, null, null, null)) {
                        var store = new S3PulsarOffloadObjectStoreV1(sourceClient, bucket, objectExecutor, false);
                        var offloader = offloader(store, fileExecutor);
                        var factory = factory(metadata, bookKeeper.borrowClient());
                        ManagedLedgerImpl ledger = null;
                        try {
                            ledger = (ManagedLedgerImpl) factory.open(name, config(offloader));
                            if (mode == 0) {
                                var captured = control.capturePublishedPreferredView(
                                                ledger, bookKeeper, offloader, objectNamespace)
                                        .get(30, TimeUnit.SECONDS);
                                var direct = ledger.captureOffloadObjectReadSource(identity.ledgerId(), identity)
                                        .get(30, TimeUnit.SECONDS);
                                assertThat(direct.captureOffloadedReadSourceIdentity()
                                                .get(30, TimeUnit.SECONDS))
                                        .isEqualTo(identity);
                                var operation = captured.readOwnedOperation(
                                        identity.ledgerId(), 0, 0, OWNED_READ_LIMITS, () -> true);
                                assertThat(operation
                                                .result()
                                                .get(30, TimeUnit.SECONDS)
                                                .entries()
                                                .get(0)
                                                .payload())
                                        .containsExactly(payload(0));
                                operation.drained().get(30, TimeUnit.SECONDS);
                                var ordinary = readNativeEntry(ledger, identity.ledgerId())
                                        .get(30, TimeUnit.SECONDS);
                                try {
                                    assertThat(ordinary.getData()).containsExactly(payload(0));
                                } finally {
                                    ordinary.release();
                                }
                            } else {
                                // Controlled missing-published-version fixture; the same-byte current root remains.
                                actual.deleteObject(DeleteObjectRequest.builder()
                                        .bucket(bucket)
                                        .key(identity.root().key())
                                        .versionId(identity.root().version().substring(8))
                                        .build());
                                assertThat(actual.headObject(HeadObjectRequest.builder()
                                                        .bucket(bucket)
                                                        .key(identity.root().key())
                                                        .build())
                                                .versionId())
                                        .isEqualTo(rootReplacement);
                                var missing = control.capturePublishedPreferredView(
                                        ledger, bookKeeper, offloader, objectNamespace);
                                assertThatThrownBy(() -> missing.get(30, TimeUnit.SECONDS))
                                        .isInstanceOf(java.util.concurrent.ExecutionException.class);
                            }
                        } finally {
                            try {
                                if (ledger != null) {
                                    closeNativeLedger(ledger).get(30, TimeUnit.SECONDS);
                                }
                            } finally {
                                factory.shutdown();
                                offloader.close();
                                store.close().toCompletableFuture().get(30, TimeUnit.SECONDS);
                            }
                        }
                    }
                }
                System.out.println("NATIVE_M4_PUBLISHED_COLD actualMapDecoded=true exactPublishedVersions=true"
                        + " newerSameByteVersionsIgnored=true missingPublishedRootRejected=true ordinaryColdRead=true"
                        + " ownerActivationOrPhysicalDeleteAuthority=false");
            } finally {
                actual.deleteObject(DeleteObjectRequest.builder()
                        .bucket(bucket)
                        .key(identity.root().key())
                        .versionId(rootReplacement)
                        .build());
                actual.deleteObject(DeleteObjectRequest.builder()
                        .bucket(bucket)
                        .key(identity.data().key())
                        .versionId(dataReplacement)
                        .build());
            }
        }
    }

    private void verifyPreferredOnlySource(
            PulsarNativeBindingV1 binding,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            Namespace objectNamespace,
            String bucket,
            String name,
            long offloaded,
            long plain,
            long tail,
            ExecutorService objectExecutor,
            ExecutorService fileExecutor,
            AtomicInteger reads,
            AtomicReference<SdkHold> heldGet,
            int mode)
            throws Exception {
        try (var coldMetadata = metadata();
                var coldClient = client(reads, heldGet, null, null)) {
            var coldStore = new S3PulsarOffloadObjectStoreV1(coldClient, bucket, objectExecutor, false);
            var coldOffloader = offloader(coldStore, fileExecutor);
            var coldFactory = factory(coldMetadata, bookKeeper.borrowClient());
            ManagedLedgerImpl cold = null;
            try {
                cold = (ManagedLedgerImpl) coldFactory.open(name, config(coldOffloader));
                var preferred = binding.capturePreferredOnlyReadView(cold, bookKeeper, coldOffloader, objectNamespace)
                        .get(30, TimeUnit.SECONDS);
                assertThat(preferred.members())
                        .doesNotContain(new PhysicalResourceIdV2.BookKeeperLedger(bookKeeper.namespace(), offloaded));
                assertThat(preferred.fallbackSourceIdentities()).isEmpty();
                assertThat(PulsarNativeM4SourceMapV1.capture(preferred).floorChain(binding))
                        .containsExactlyElementsOf(preferred.materializationChain());
                for (long id : new long[] {offloaded, plain, tail}) {
                    var operation = preferred.readOwnedOperation(id, 0, 0, OWNED_READ_LIMITS, () -> true);
                    int ordinal = id == offloaded ? 0 : id == plain ? 3 : 6;
                    assertThat(operation
                                    .result()
                                    .get(30, TimeUnit.SECONDS)
                                    .entries()
                                    .get(0)
                                    .payload())
                            .containsExactly(payload(ordinal));
                    operation.drained().get(30, TimeUnit.SECONDS);
                }
                if (mode == 6) {
                    verifyNativeM4Owner(
                            binding,
                            preferred,
                            cold,
                            bookKeeper,
                            coldOffloader,
                            offloaded,
                            plain,
                            tail,
                            heldGet,
                            reads,
                            mode,
                            new AtomicBoolean());
                } else {
                    var ordinary = readNativeEntry(cold, offloaded).get(30, TimeUnit.SECONDS);
                    try {
                        assertThat(ordinary.getData()).containsExactly(payload(0));
                    } finally {
                        ordinary.release();
                    }
                    assertThat(binding.m4Control().readFacts(preferred).get(30, TimeUnit.SECONDS))
                            .isEmpty();
                    System.out.println("NATIVE_PULSAR_PREFERRED_ONLY_SOURCE bookKeeperMetadataMissing=true"
                            + " offloadedBookKeeperNotSelected=true actualObjectOnly=true coldRead=true"
                            + " physicalDeleteAuthority=false");
                }
            } finally {
                try {
                    if (cold != null) {
                        closeNativeLedger(cold).get(30, TimeUnit.SECONDS);
                    }
                } finally {
                    coldFactory.shutdown();
                    coldOffloader.close();
                    coldStore.close().toCompletableFuture().get(30, TimeUnit.SECONDS);
                }
            }
        }
    }

    private void verifyNativeM4Owner(
            PulsarNativeBindingV1 binding,
            PulsarNativeBindingReadViewV1 diagnostic,
            ManagedLedgerImpl ledger,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            long offloaded,
            long plain,
            long tail,
            AtomicReference<SdkHold> heldGet,
            AtomicInteger reads,
            int mode,
            AtomicBoolean callbackFailureIssued)
            throws Exception {
        Path capsule = Path.of(System.getProperty("nereus.m5.nativeM4AdmissionFile"));
        var expected = Sha256Digest.copyOf(
                java.util.HexFormat.of().parseHex(System.getProperty("nereus.m5.nativeM4AdmissionSha256")));
        assertThatThrownBy(() -> PulsarNativeM4DrainAdmissionV1.load(
                        capsule, Sha256Digest.hash(CanonicalBytes.copyOf(new byte[1]))))
                .hasMessageContaining("explicit admission");
        var admission = PulsarNativeM4DrainAdmissionV1.load(capsule, expected);
        var control = binding.m4Control();
        var cellBudget = readBudget(binding);
        var view = preparedView(
                binding,
                ledger,
                bookKeeper,
                offloader,
                diagnostic.objectNamespace(),
                cellBudget,
                diagnostic.fallbackSourceIdentities().isEmpty());
        var ownerLoop = Executors.newSingleThreadExecutor();
        var controlLoop = Executors.newSingleThreadExecutor();
        SdkHold hold = hold();
        Entry alias = null;
        try {
            int beforeUnpreparedGrant = reads.get();
            assertThatThrownBy(() -> control.createReadOwner(
                                    diagnostic, admission, bookKeeper, offloader, controlLoop, ownerLoop, 2, cellBudget)
                            .get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("Native M4 owner requires its actual source preparation");
            assertThat(reads.get()).isEqualTo(beforeUnpreparedGrant);
            assertThat(control.readFacts(view).get(30, TimeUnit.SECONDS)).isEmpty();
            // An explicit capsule still has to match the actual SDK configuration, before any capability is created.
            var changed = new Properties();
            try (var input = Files.newInputStream(capsule)) {
                changed.load(input);
            }
            changed.setProperty(
                    "configuration",
                    Sha256Digest.hash(CanonicalBytes.copyOf(new byte[1])).toHex());
            Path foreign = temporaryDirectory.resolve("foreign-native-m4.properties");
            try (var output = Files.newOutputStream(foreign)) {
                changed.store(output, "negative configuration control");
            }
            var wrongConfiguration = PulsarNativeM4DrainAdmissionV1.load(
                    foreign, Sha256Digest.hash(CanonicalBytes.copyOf(Files.readAllBytes(foreign))));
            assertThatThrownBy(() -> control.createReadOwner(
                                    view,
                                    wrongConfiguration,
                                    bookKeeper,
                                    offloader,
                                    controlLoop,
                                    ownerLoop,
                                    2,
                                    cellBudget)
                            .join())
                    .hasRootCauseMessage("Native M4 actual backend configuration differs from conformance");
            assertThat(control.readFacts(view).get(30, TimeUnit.SECONDS)).isEmpty();
            var owner = control.createReadOwner(
                            view, admission, bookKeeper, offloader, controlLoop, ownerLoop, 2, cellBudget)
                    .get(30, TimeUnit.SECONDS);
            var facts = control.readFacts(view).get(30, TimeUnit.SECONDS).orElseThrow();
            assertThat(facts.capability().conformanceReceiptSha256()).isEqualTo(expected);
            assertThat(owner.authority().publicationCell().protocolStateReference())
                    .isSameAs(view);
            assertThat(owner.authority().ownerEpoch())
                    .isEqualTo(facts.selector().ownerEpoch());
            assertThat(owner.authority().sourceGeneration())
                    .isEqualTo(facts.selector().sourceGeneration());
            int beforeDuplicate = reads.get();
            assertThatThrownBy(() -> control.createReadOwner(
                                    view, admission, bookKeeper, offloader, controlLoop, ownerLoop, 2, cellBudget)
                            .join())
                    .hasRootCauseMessage("Native M4 selector already has an owner or unresolved grant");
            assertThat(reads.get()).isEqualTo(beforeDuplicate);
            for (long id : new long[] {offloaded, plain, tail}) {
                var operation = view.readOwnedOperation(id, 0, 0, OWNED_READ_LIMITS, () -> true);
                int ordinal = id == offloaded ? 0 : id == plain ? 3 : 6;
                assertThat(operation
                                .result()
                                .get(30, TimeUnit.SECONDS)
                                .entries()
                                .get(0)
                                .payload())
                        .containsExactly(payload(ordinal));
                operation.drained().get(30, TimeUnit.SECONDS);
                ownerLoop.submit(() -> {}).get(30, TimeUnit.SECONDS);
                assertThat(owner.hazardState()).isEqualTo(ScanOutcome.CLEAN);
            }
            if (mode == 4 || mode == 6) {
                hold.failure()
                        .set(SdkClientException.builder()
                                .message("actual M4 primary response lost")
                                .build());
                heldGet.set(hold);
                var ordinary = readNativeEntry(ledger, offloaded);
                assertThat(hold.started().await(30, TimeUnit.SECONDS)).isTrue();
                assertThat(owner.hazardState()).isEqualTo(ScanOutcome.PINNED);
                if (mode == 4) {
                    ledger.setProperty("m4-scope-change", "confirmed");
                    assertThat(view.isCurrent()).isFalse();
                } else {
                    assertThat(facts.selector().mode()).isEqualTo(SelectorMode.PREFERRED_ONLY);
                    assertThat(view.fallbackSourceIdentities()).isEmpty();
                }
                hold.released().countDown();
                // The legacy ordinary composite would return a successful BK fallback under the new metadata cut.
                try {
                    assertThatThrownBy(() -> ordinary.get(30, TimeUnit.SECONDS))
                            .hasRootCauseMessage("actual M4 primary response lost");
                } finally {
                    if (ordinary.isDone() && !ordinary.isCompletedExceptionally()) {
                        ordinary.join().release();
                    }
                }
                owner.fence().get(30, TimeUnit.SECONDS);
                assertThat(owner.hazardState()).isEqualTo(ScanOutcome.CLEAN);
                System.out.println(
                        mode == 4
                                ? "NATIVE_M4_ORDINARY_SCOPE actualPrimaryLoss=true actualMetadataAdvance=true"
                                        + " secondaryUnderChangedCutRejected=true privateCleanupConfirmed=true"
                                : "NATIVE_M4_PREFERRED_ONLY actualPrimaryLoss=true actualSelectorPO=true"
                                        + " existingBookKeeperFallbackRejected=true privateCleanupConfirmed=true");
                return;
            }
            if (mode == 2) {
                var delivered = new CompletableFuture<Void>();
                ledger.asyncReadEntry(
                        PositionFactory.create(tail, 0),
                        new ReadEntryCallback() {
                            @Override
                            public void readEntryComplete(Entry entry, Object ctx) {
                                entry.release();
                                callbackFailureIssued.set(true);
                                delivered.complete(null);
                                throw new IllegalStateException("actual Native M4 callback failed");
                            }

                            @Override
                            public void readEntryFailed(ManagedLedgerException failure, Object ctx) {
                                delivered.completeExceptionally(failure);
                            }
                        },
                        null);
                delivered.get(30, TimeUnit.SECONDS);
                assertThatThrownBy(() -> owner.fence().get(30, TimeUnit.SECONDS))
                        .hasRootCauseMessage("actual Native M4 callback failed");
                assertThat(owner.authority().admitting()).isFalse();
                assertThat(owner.hazardState()).isEqualTo(ScanOutcome.PINNED);
                assertNativeCapacity(cellBudget, 2, 2);
                assertThatThrownBy(() -> closeNativeLedger(ledger).get(30, TimeUnit.SECONDS))
                        .hasRootCauseMessage("actual Native M4 callback failed");
                System.out.println("NATIVE_M4_OWNER_UNKNOWN actualCallbackFailure=true localAdmissionClosed=true"
                        + " exactHazardRetained=true remoteTerminalOrReleased=false");
                return;
            }
            var entry = (EntryImpl) readNativeEntry(ledger, tail).get(30, TimeUnit.SECONDS);
            alias = EntryImpl.create(entry);
            entry.release();
            assertThat(owner.hazardState()).isEqualTo(ScanOutcome.PINNED);
            heldGet.set(hold);
            var pending = view.readOwnedOperation(offloaded, 0, 0, OWNED_READ_LIMITS, () -> true);
            assertThat(hold.started().await(30, TimeUnit.SECONDS)).isTrue();
            var result = pending.result();
            assertThat(result.cancel(false)).isTrue();
            assertThat(pending.drained().complete(null)).isTrue();
            var privateDrainObserver = pending.drained();
            assertThat(privateDrainObserver).isNotDone();
            var fenced = owner.fence();
            var repeated = owner.fence();
            assertThat(fenced.cancel(false)).isTrue();
            assertThatThrownBy(() -> repeated.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
            assertThat(owner.hazardState()).isEqualTo(ScanOutcome.PINNED);
            hold.released().countDown();
            privateDrainObserver.get(30, TimeUnit.SECONDS);
            ownerLoop.submit(() -> {}).get(30, TimeUnit.SECONDS);
            assertThat(repeated).isNotDone();
            assertThat(owner.hazardState()).isEqualTo(ScanOutcome.PINNED);
            alias.release();
            alias = null;
            repeated.get(30, TimeUnit.SECONDS);
            assertThat(owner.hazardState()).isEqualTo(ScanOutcome.CLEAN);
            int afterFence = reads.get();
            assertThatThrownBy(() -> view.readOwnedAsync(offloaded, 0, 0, OWNED_READ_LIMITS, () -> true)
                            .join())
                    .hasRootCauseInstanceOf(ManagedLedgerException.ManagedLedgerFencedException.class)
                    .hasRootCauseMessage("Native owner read admission is closed");
            assertThat(reads.get()).isEqualTo(afterFence);
            closeNativeLedger(ledger).get(30, TimeUnit.SECONDS);
            System.out.println(
                    "NATIVE_M4_OWNER_ACTUAL typedNativePosition=true actualSelector=true currentSourceCapsule=true"
                            + " heldGetHazard=true borrowedAliasHazard=true observerMutationCannotDrain=true"
                            + " duplicateActorRejected=true newReadAfterFenceRejected=true"
                            + " remoteTerminalOrReleased=false");
        } finally {
            hold.released().countDown();
            if (alias != null) {
                alias.release();
            }
            controlLoop.shutdown();
            ownerLoop.shutdown();
            assertThat(controlLoop.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            assertThat(ownerLoop.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void nativeOffloadBoundsFallbackReadsAndColdSubscriptionKeepsBacklog() throws Exception {
        AtomicInteger actualObjectReads = new AtomicInteger();
        var heldGet = new AtomicReference<SdkHold>();
        var heldPut = new AtomicReference<SdkHold>();
        var versionProbe = new AtomicReference<VersionProbe>();
        var objectExecutor = Executors.newFixedThreadPool(2);
        var fileExecutor = Executors.newFixedThreadPool(2);
        try (S3Client independent = client(null);
                S3Client actual = client(actualObjectReads, heldGet, heldPut, versionProbe)) {
            independent.createBucket(
                    CreateBucketRequest.builder().bucket(BUCKET).build());
            independent.putBucketVersioning(PutBucketVersioningRequest.builder()
                    .bucket(BUCKET)
                    .versioningConfiguration(value -> value.status(BucketVersioningStatus.ENABLED))
                    .build());
            var store = new S3PulsarOffloadObjectStoreV1(actual, BUCKET, objectExecutor, false);
            var writer = offloader(store, fileExecutor);
            var offloader = offloader(store, fileExecutor);
            try {
                Offloaded source = offloadAndCloseDuringActualPut(writer, independent, heldPut, fileExecutor);
                int readsBeforeCold = actualObjectReads.get();
                readSubscriptionCold(offloader, source);
                assertThat(actualObjectReads.get()).isGreaterThan(readsBeforeCold);
                closeNativeOwnerWithCachedBufferAliases(offloader, source, actualObjectReads, false);
                closeNativeOwnerWithCachedBufferAliases(offloader, source, actualObjectReads, true);
                closeNativeOwnerDuringOrdinaryCallback(offloader, source, false);
                closeNativeOwnerDuringOrdinaryCallback(offloader, source, true);
                readAndCloseDuringActualGet(offloader, source, heldGet);
                closeNativeOwnerDuringActualGet(offloader, source, heldGet, actualObjectReads);
                closeNativeOwnerDuringCancelledOpen(offloader, source, heldGet, actualObjectReads);
                closeNativeOwnerDuringCancelledRange(offloader, source, heldGet, actualObjectReads);
                verifyCapturedNativeSource(offloader, source, actualObjectReads);
                verifyNativeImmutableObjectVersions(offloader, source, independent, versionProbe);
                closeNativeOwnerDuringIdentityCapture(offloader, source, heldGet, actualObjectReads);
                closeFencedNativeOwnerDuringActualGet(offloader, source, heldGet, actualObjectReads);
                verifyCapturedFallbackKeepsMetadataVersion(offloader, source, heldGet);
                closeNativeOwnerDuringCancelledCapture(offloader, source, heldGet);
                closeNativeOwnerDuringCursorMetadata(offloader, source, false, heldGet);
                closeNativeOwnerDuringCursorMetadata(offloader, source, true, heldGet);
                verifyOwnedCapturedRange(offloader, source, heldGet, actualObjectReads, "delivered");
                verifyOwnedCapturedRange(offloader, source, heldGet, actualObjectReads, "cancelled");
                verifyOwnedCapturedRange(offloader, source, heldGet, actualObjectReads, "completed");
                verifyCancelledOwnedRangePreventsFallback(offloader, source, heldGet, store);
                assertThatThrownBy(() -> offloader
                                .deleteOffloaded(
                                        source.ledgerId(), source.attempt(), offloader.getOffloadDriverMetadata())
                                .join())
                        .hasRootCauseMessage("Pulsar offload deletion requires M5 intent, release"
                                + " and native reference-free authority");
                var keys = PulsarOffloadKeysV1.derive("cells/m5-native", source.ledgerId(), source.attempt());
                assertThat(independent
                                .headObject(HeadObjectRequest.builder()
                                        .bucket(BUCKET)
                                        .key(keys.rootKey())
                                        .build())
                                .versionId())
                        .isEqualTo(source.rootVersion());
                assertThat(independent
                                .headObject(HeadObjectRequest.builder()
                                        .bucket(BUCKET)
                                        .key(keys.dataKey())
                                        .build())
                                .versionId())
                        .isEqualTo(source.dataVersion());
                try (var files = java.nio.file.Files.list(temporaryDirectory)) {
                    assertThat(files.count()).isZero();
                }
            } finally {
                writer.close();
                offloader.close();
                store.close().toCompletableFuture().get(30, TimeUnit.SECONDS);
            }
        } finally {
            fileExecutor.shutdown();
            objectExecutor.shutdown();
            assertThat(fileExecutor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            assertThat(objectExecutor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static void closeNativeOwnerDuringCursorMetadata(
            NereusPulsarLedgerOffloaderV1 offloader,
            Offloaded source,
            boolean failAcknowledgement,
            AtomicReference<SdkHold> heldGet)
            throws Exception {
        try (var metadata = metadata();
                var independent = metadata();
                var bk = bookKeeper()) {
            String cursorPath = "/managed-ledgers/" + LEDGER + "/" + SUBSCRIPTION;
            var holdNext = new AtomicBoolean();
            var written = new CountDownLatch(1);
            var acknowledgement = new CompletableFuture<Void>();
            var heldMetadata = new FaultInjectionMetadataStore(metadata) {
                @Override
                public CompletableFuture<Stat> put(
                        String path, byte[] value, Optional<Long> expectedVersion, Set<Option> options) {
                    var actual = super.put(path, value, expectedVersion, options);
                    if (path.equals(cursorPath) && holdNext.compareAndSet(true, false)) {
                        return actual.thenCompose(stat -> {
                            written.countDown();
                            return acknowledgement.thenApply(ignored -> stat);
                        });
                    }
                    return actual;
                }
            };
            var factory = factory(heldMetadata, bk);
            var ledger = (ManagedLedgerImpl) factory.open(LEDGER, config(offloader));
            var get = hold();
            try {
                var captured =
                        ledger.captureOffloadReadSource(source.ledgerId()).get(30, TimeUnit.SECONDS);
                heldGet.set(get);
                var reading = captured.readOwnedAsync(0, 0, OWNED_READ_LIMITS);
                assertThat(get.started().await(30, TimeUnit.SECONDS)).isTrue();
                var cursor = ledger.openCursor(SUBSCRIPTION);
                var previous =
                        independent.get(cursorPath).get(30, TimeUnit.SECONDS).orElseThrow();
                assertThat(cursor.getNumberOfEntriesInBacklog(false)).isEqualTo(SEALED_ENTRIES + 1);
                holdNext.set(true);
                var first = closeNativeCursor(cursor);
                assertThat(written.await(30, TimeUnit.SECONDS)).isTrue();
                var committed =
                        independent.get(cursorPath).get(30, TimeUnit.SECONDS).orElseThrow();
                assertThat(committed.getStat().getVersion())
                        .isGreaterThan(previous.getStat().getVersion());
                assertThat(committed.getValue()).isNotEmpty();
                var repeated = closeNativeCursor(cursor);
                assertThatThrownBy(() -> repeated.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
                assertThat(closeNativeCursor(cursor).cancel(false)).isTrue();
                var afterCancelled = closeNativeCursor(cursor);
                assertThat(afterCancelled).isNotDone();
                var nativeClose = closeNativeLedger(ledger);
                assertThatThrownBy(() -> nativeClose.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
                if (failAcknowledgement) {
                    acknowledgement.completeExceptionally(
                            new MetadataStoreException("actual cursor metadata acknowledgement lost"));
                    assertThatThrownBy(() -> first.get(30, TimeUnit.SECONDS))
                            .hasRootCauseMessage("actual cursor metadata acknowledgement lost");
                    assertThatThrownBy(() -> repeated.get(30, TimeUnit.SECONDS))
                            .hasRootCauseMessage("actual cursor metadata acknowledgement lost");
                    assertThatThrownBy(() -> afterCancelled.get(30, TimeUnit.SECONDS))
                            .hasRootCauseMessage("actual cursor metadata acknowledgement lost");
                    assertThatThrownBy(() -> nativeClose.get(1, TimeUnit.SECONDS))
                            .isInstanceOf(TimeoutException.class);
                    get.released().countDown();
                    assertThat(reading.get(30, TimeUnit.SECONDS)
                                    .entries()
                                    .get(0)
                                    .payload())
                            .containsExactly(payload(0));
                    assertThatThrownBy(() -> nativeClose.get(30, TimeUnit.SECONDS))
                            .hasRootCauseMessage("actual cursor metadata acknowledgement lost");
                    assertThatThrownBy(() -> closeNativeCursor(cursor).join())
                            .hasRootCauseMessage("actual cursor metadata acknowledgement lost");
                    assertThatThrownBy(() -> closeNativeLedger(ledger).join())
                            .hasRootCauseMessage("actual cursor metadata acknowledgement lost");
                } else {
                    acknowledgement.complete(null);
                    first.get(30, TimeUnit.SECONDS);
                    repeated.get(30, TimeUnit.SECONDS);
                    afterCancelled.get(30, TimeUnit.SECONDS);
                    assertThatThrownBy(() -> nativeClose.get(1, TimeUnit.SECONDS))
                            .isInstanceOf(TimeoutException.class);
                    get.released().countDown();
                    assertThat(reading.get(30, TimeUnit.SECONDS)
                                    .entries()
                                    .get(0)
                                    .payload())
                            .containsExactly(payload(0));
                    nativeClose.get(30, TimeUnit.SECONDS);
                    assertThat(closeNativeCursor(cursor)).isCompleted();
                    assertThat(closeNativeLedger(ledger)).isCompleted();
                }
                System.out.println("NATIVE_PULSAR_CURSOR_METADATA_DRAIN ledger=" + source.ledgerId() + " attempt="
                        + source.attempt() + " failedAcknowledgement=" + failAcknowledgement + " committedVersion="
                        + committed.getStat().getVersion());
            } finally {
                acknowledgement.complete(null);
                get.released().countDown();
                factory.shutdown();
            }
        }
    }

    private static CompletableFuture<Void> closeNativeCursor(ManagedCursor cursor) {
        var future = new Futures.CloseFuture();
        cursor.asyncClose(future, null);
        return future;
    }

    private static void closeNativeOwnerDuringActualGet(
            NereusPulsarLedgerOffloaderV1 offloader,
            Offloaded source,
            AtomicReference<SdkHold> heldGet,
            AtomicInteger reads)
            throws Exception {
        try (var metadata = metadata();
                var bk = bookKeeper()) {
            var factory = factory(metadata, bk);
            var ledger = (ManagedLedgerImpl) factory.open(LEDGER, config(offloader));
            var hold = hold();
            try {
                ledger.reopenReadHandle(source.ledgerId()).get(30, TimeUnit.SECONDS);
                heldGet.set(hold);
                var read = readNativeEntry(ledger, source.ledgerId());
                assertThat(hold.started().await(30, TimeUnit.SECONDS)).isTrue();
                var close = closeNativeLedger(ledger);
                var repeatedClose = closeNativeLedger(ledger);
                assertThat(repeatedClose).isNotDone();
                assertThat(close).isNotDone();
                assertThat(repeatedClose.cancel(false)).isTrue();
                repeatedClose = closeNativeLedger(ledger);
                assertThat(repeatedClose).isNotDone();
                int before = reads.get();
                assertThatThrownBy(
                                () -> readNativeEntry(ledger, source.ledgerId()).join())
                        .hasRootCauseMessage("Managed ledger is closed");
                assertThat(reads.get()).isEqualTo(before);
                hold.released().countDown();
                var entry = read.get(30, TimeUnit.SECONDS);
                try {
                    assertThat(entry.getData()).containsExactly(payload(0));
                } finally {
                    entry.release();
                }
                close.get(30, TimeUnit.SECONDS);
                repeatedClose.get(30, TimeUnit.SECONDS);
                assertThat(closeNativeLedger(ledger)).isCompleted();
                System.out.println("NATIVE_PULSAR_OWNER_HANDLE_DRAIN ledger=" + source.ledgerId() + " attempt="
                        + source.attempt());
            } finally {
                hold.released().countDown();
                try {
                    ledger.close();
                } finally {
                    factory.shutdown();
                }
            }
        }
    }

    private static void closeNativeOwnerDuringCancelledOpen(
            NereusPulsarLedgerOffloaderV1 offloader,
            Offloaded source,
            AtomicReference<SdkHold> heldGet,
            AtomicInteger reads)
            throws Exception {
        try (var metadata = metadata();
                var bk = bookKeeper()) {
            var factory = factory(metadata, bk);
            var ledger = (ManagedLedgerImpl) factory.open(LEDGER, config(offloader));
            var hold = hold();
            try {
                heldGet.set(hold);
                var observer = ledger.reopenReadHandle(source.ledgerId());
                assertThat(hold.started().await(30, TimeUnit.SECONDS)).isTrue();
                assertThat(observer.cancel(false)).isTrue();
                var close = closeNativeLedger(ledger);
                var repeatedClose = closeNativeLedger(ledger);
                assertThat(repeatedClose).isNotDone();
                assertThat(close).isNotDone();
                int before = reads.get();
                assertThatThrownBy(
                                () -> ledger.reopenReadHandle(source.ledgerId()).join())
                        .hasRootCauseMessage("Managed ledger is closed");
                assertThat(reads.get()).isEqualTo(before);
                hold.released().countDown();
                close.get(30, TimeUnit.SECONDS);
                repeatedClose.get(30, TimeUnit.SECONDS);
                assertThat(observer).isCancelled();
                assertThat(closeNativeLedger(ledger)).isCompleted();
                System.out.println("NATIVE_PULSAR_OWNER_CANCELLED_OPEN_DRAIN ledger=" + source.ledgerId() + " attempt="
                        + source.attempt());
            } finally {
                hold.released().countDown();
                try {
                    ledger.close();
                } finally {
                    factory.shutdown();
                }
            }
        }
    }

    private static void closeNativeOwnerDuringCancelledRange(
            NereusPulsarLedgerOffloaderV1 offloader,
            Offloaded source,
            AtomicReference<SdkHold> heldGet,
            AtomicInteger reads)
            throws Exception {
        try (var metadata = metadata();
                var bk = bookKeeper()) {
            var factory = factory(metadata, bk);
            var ledger = (ManagedLedgerImpl) factory.open(LEDGER, config(offloader));
            var hold = hold();
            try {
                var handle = ledger.reopenReadHandle(source.ledgerId()).get(30, TimeUnit.SECONDS);
                heldGet.set(hold);
                var read = handle.readAsync(0, 1);
                assertThat(hold.started().await(30, TimeUnit.SECONDS)).isTrue();
                assertThat(read.cancel(false)).isTrue();
                var close = closeNativeLedger(ledger);
                var repeated = closeNativeLedger(ledger);
                assertThatThrownBy(() -> close.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
                assertThatThrownBy(() -> repeated.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
                int before = reads.get();
                assertThatThrownBy(
                                () -> readNativeEntry(ledger, source.ledgerId()).join())
                        .hasRootCauseMessage("Managed ledger is closed");
                assertThat(reads.get()).isEqualTo(before);
                hold.released().countDown();
                close.get(30, TimeUnit.SECONDS);
                repeated.get(30, TimeUnit.SECONDS);
                assertThat(read).isCancelled();
                assertThat(closeNativeLedger(ledger)).isCompleted();
                System.out.println("NATIVE_PULSAR_CANCELLED_RANGE_DRAIN ledger=" + source.ledgerId() + " attempt="
                        + source.attempt());
            } finally {
                hold.released().countDown();
                factory.shutdown();
            }
        }
    }

    private static void verifyCapturedNativeSource(
            NereusPulsarLedgerOffloaderV1 offloader, Offloaded source, AtomicInteger reads) throws Exception {
        try (var metadata = metadata();
                var independent = metadata();
                var bk = bookKeeper()) {
            var factory = factory(metadata, bk);
            var ledger = (ManagedLedgerImpl) factory.open(LEDGER, config(offloader));
            try {
                var captured =
                        ledger.captureOffloadReadSource(source.ledgerId()).get(30, TimeUnit.SECONDS);
                var observed = independent
                        .get("/managed-ledgers/" + LEDGER)
                        .get(30, TimeUnit.SECONDS)
                        .orElseThrow();
                assertThat(captured.sourceState().metadataVersion())
                        .isEqualTo(observed.getStat().getVersion());
                assertThat(captured.ledgerId()).isEqualTo(source.ledgerId());
                assertThat(captured.sourceState().attemptUuid()).isEqualTo(source.attempt());
                assertThat(captured.lastAddConfirmed()).isEqualTo(SEALED_ENTRIES - 1);
                assertThat(captured.length()).isEqualTo((long) SEALED_ENTRIES * ENTRY_BYTES);
                assertThat(captured.readPriority()).isEqualTo(OffloadedReadPriority.TIERED_STORAGE_FIRST);
                assertThat(captured.driverMetadata()).containsEntry("ManagedLedgerName", LEDGER);
                assertThatThrownBy(() -> captured.driverMetadata().put("ManagedLedgerName", "another-owner"))
                        .isInstanceOf(UnsupportedOperationException.class);
                assertThat(captured.isCurrent()).isTrue();
                try (var entries = captured.readAsync(0, 1).get(30, TimeUnit.SECONDS)) {
                    assertThat(entries.getEntry(0).getEntryBytes()).containsExactly(payload(0));
                    assertThat(entries.getEntry(1).getEntryBytes()).containsExactly(payload(1));
                }
                ledger.setProperty("m5-captured-source-version", "changed");
                int before = reads.get();
                assertThat(captured.isCurrent()).isFalse();
                assertThatThrownBy(() -> captured.readAsync(0, 0).join())
                        .hasRootCauseMessage("captured native source is stale");
                assertThatThrownBy(() ->
                                captured.captureOffloadedReadSourceIdentity().join())
                        .hasRootCauseMessage("captured native source is stale");
                assertThat(reads.get()).isEqualTo(before);
                var refreshed =
                        ledger.captureOffloadReadSource(source.ledgerId()).get(30, TimeUnit.SECONDS);
                assertThat(refreshed.sourceState().metadataVersion())
                        .isGreaterThan(captured.sourceState().metadataVersion());
                assertThat(refreshed.sourceState().attemptUuid()).isEqualTo(source.attempt());
                try (var entries = refreshed.readAsync(0, 0).get(30, TimeUnit.SECONDS)) {
                    assertThat(entries.getEntry(0).getEntryBytes()).containsExactly(payload(0));
                }
                var replacementHandle =
                        ledger.reopenReadHandle(source.ledgerId()).get(30, TimeUnit.SECONDS);
                before = reads.get();
                assertThat(refreshed.isCurrent()).isFalse();
                assertThatThrownBy(() -> refreshed.readAsync(0, 0).join())
                        .hasRootCauseMessage("captured native source is stale");
                assertThatThrownBy(() ->
                                refreshed.captureOffloadedReadSourceIdentity().join())
                        .hasRootCauseMessage("captured native source is stale");
                assertThat(reads.get()).isEqualTo(before);
                var replacement =
                        ledger.captureOffloadReadSource(source.ledgerId()).get(30, TimeUnit.SECONDS);
                replacementHandle.closeAsync().get(30, TimeUnit.SECONDS);
                before = reads.get();
                assertThat(replacement.isCurrent()).isFalse();
                assertThatThrownBy(() -> replacement.readAsync(0, 0).join())
                        .hasRootCauseMessage("captured native source is stale");
                assertThatThrownBy(() ->
                                replacement.captureOffloadedReadSourceIdentity().join())
                        .hasRootCauseMessage("captured native source is stale");
                assertThat(reads.get()).isEqualTo(before);
                ledger.reopenReadHandle(source.ledgerId()).get(30, TimeUnit.SECONDS);
                var finalCapture =
                        ledger.captureOffloadReadSource(source.ledgerId()).get(30, TimeUnit.SECONDS);
                closeNativeLedger(ledger).get(30, TimeUnit.SECONDS);
                before = reads.get();
                assertThat(finalCapture.isCurrent()).isFalse();
                assertThatThrownBy(() -> finalCapture.readAsync(0, 0).join())
                        .hasRootCauseMessage("Managed ledger is closed");
                assertThatThrownBy(() -> finalCapture
                                .captureOffloadedReadSourceIdentity()
                                .join())
                        .hasRootCauseMessage("Managed ledger is closed");
                assertThat(reads.get()).isEqualTo(before);
                System.out.println("NATIVE_PULSAR_CAPTURED_SOURCE ledger=" + source.ledgerId() + " attempt="
                        + source.attempt() + " metadataVersion="
                        + captured.sourceState().metadataVersion()
                        + " refreshedMetadataVersion=" + refreshed.sourceState().metadataVersion());
            } finally {
                factory.shutdown();
            }
        }
    }

    private static void verifyNativeImmutableObjectVersions(
            NereusPulsarLedgerOffloaderV1 offloader,
            Offloaded source,
            S3Client independent,
            AtomicReference<VersionProbe> versionProbe)
            throws Exception {
        var keys = PulsarOffloadKeysV1.derive("cells/m5-native", source.ledgerId(), source.attempt());
        var replacedRoot = new AtomicReference<String>();
        String replacedData = null;
        try (var metadata = metadata();
                var bk = bookKeeper()) {
            var factory = factory(metadata, bk);
            var ledger = (ManagedLedgerImpl) factory.open(LEDGER, config(offloader));
            try {
                var root = new VersionProbe(
                        keys.rootKey(),
                        () -> replacedRoot.set(
                                replaceWithIdenticalVersion(independent, keys.rootKey(), source.rootVersion())));
                versionProbe.set(root);
                var captured =
                        ledger.captureOffloadReadSource(source.ledgerId()).get(30, TimeUnit.SECONDS);
                var identity = captured.captureOffloadedReadSourceIdentity().get(30, TimeUnit.SECONDS);
                assertThat(identity.ledgerId()).isEqualTo(source.ledgerId());
                assertThat(identity.attemptUuid()).isEqualTo(source.attempt());
                assertThat(identity.root().key()).isEqualTo(keys.rootKey());
                assertThat(identity.data().key()).isEqualTo(keys.dataKey());
                assertThat(identity.root().version()).isEqualTo("version:" + source.rootVersion());
                assertThat(identity.data().version()).isEqualTo("version:" + source.dataVersion());
                assertThat(root.headReplaced).isTrue();
                assertThat(replacedRoot.get()).isNotBlank().isNotEqualTo(source.rootVersion());
                var data = new VersionProbe(keys.dataKey(), null);
                versionProbe.set(data);
                replacedData = replaceWithIdenticalVersion(independent, keys.dataKey(), source.dataVersion());
                assertThat(replacedData).isNotBlank().isNotEqualTo(source.dataVersion());
                assertThat(captured.captureOffloadedReadSourceIdentity().get(30, TimeUnit.SECONDS))
                        .isEqualTo(identity);
                var owned = captured.readOwnedAsync(0, 0, OWNED_READ_LIMITS).get(30, TimeUnit.SECONDS);
                assertThat(owned.entries().get(0).payload()).containsExactly(payload(0));
                assertThat(Arrays.asList(root.returnedVersion.get(), data.returnedVersion.get()))
                        .containsExactly(source.rootVersion(), source.dataVersion());
                System.out.println("NATIVE_PULSAR_IMMUTABLE_GET ledger=" + source.ledgerId()
                        + " rootVersion=" + root.returnedVersion.get() + " dataVersion=" + data.returnedVersion.get()
                        + " replacedRootVersion=" + replacedRoot.get() + " replacedDataVersion=" + replacedData);
            } finally {
                versionProbe.set(null);
                try {
                    closeNativeLedger(ledger).get(30, TimeUnit.SECONDS);
                } finally {
                    factory.shutdown();
                }
            }
        } finally {
            versionProbe.set(null);
            if (replacedRoot.get() != null) {
                independent.deleteObject(DeleteObjectRequest.builder()
                        .bucket(BUCKET)
                        .key(keys.rootKey())
                        .versionId(replacedRoot.get())
                        .build());
            }
            if (replacedData != null) {
                independent.deleteObject(DeleteObjectRequest.builder()
                        .bucket(BUCKET)
                        .key(keys.dataKey())
                        .versionId(replacedData)
                        .build());
            }
        }
        assertThat(independent
                        .headObject(HeadObjectRequest.builder()
                                .bucket(BUCKET)
                                .key(keys.rootKey())
                                .build())
                        .versionId())
                .isEqualTo(source.rootVersion());
        assertThat(independent
                        .headObject(HeadObjectRequest.builder()
                                .bucket(BUCKET)
                                .key(keys.dataKey())
                                .build())
                        .versionId())
                .isEqualTo(source.dataVersion());
    }

    private static void closeNativeOwnerDuringIdentityCapture(
            NereusPulsarLedgerOffloaderV1 offloader,
            Offloaded source,
            AtomicReference<SdkHold> heldGet,
            AtomicInteger reads)
            throws Exception {
        var policies = (OffloadPoliciesImpl) offloader.getOffloadPolicies();
        var previousPriority = policies.getManagedLedgerOffloadedReadPriority();
        policies.setManagedLedgerOffloadedReadPriority(OffloadedReadPriority.BOOKKEEPER_FIRST);
        var get = hold();
        try (var metadata = metadata();
                var bk = bookKeeper()) {
            var factory = factory(metadata, bk);
            var ledger = (ManagedLedgerImpl) factory.open(LEDGER, config(offloader));
            try {
                var captured =
                        ledger.captureOffloadReadSource(source.ledgerId()).get(30, TimeUnit.SECONDS);
                assertThat(captured.readPriority()).isEqualTo(OffloadedReadPriority.BOOKKEEPER_FIRST);
                heldGet.set(get);
                var identity = captured.captureOffloadedReadSourceIdentity();
                assertThat(get.started().await(30, TimeUnit.SECONDS)).isTrue();
                assertThat(identity.cancel(false)).isTrue();
                var close = closeNativeLedger(ledger);
                var repeated = closeNativeLedger(ledger);
                assertThatThrownBy(() -> close.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
                assertThatThrownBy(() -> repeated.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
                int before = reads.get();
                assertThatThrownBy(() ->
                                captured.captureOffloadedReadSourceIdentity().join())
                        .hasRootCauseMessage("Managed ledger is closed");
                assertThat(reads.get()).isEqualTo(before);
                get.released().countDown();
                close.get(30, TimeUnit.SECONDS);
                repeated.get(30, TimeUnit.SECONDS);
                assertThat(identity).isCancelled();
                System.out.println("NATIVE_PULSAR_IDENTITY_CAPTURE_DRAIN ledger=" + source.ledgerId() + " attempt="
                        + source.attempt() + " cachedObjectOpen=true observerCancelled=true");
            } finally {
                get.released().countDown();
                heldGet.compareAndSet(get, null);
                factory.shutdown();
            }
        } finally {
            get.released().countDown();
            policies.setManagedLedgerOffloadedReadPriority(previousPriority);
        }
    }

    /** Actual same-byte versions isolate physical-version substitution from payload corruption. */
    private static String replaceWithIdenticalVersion(S3Client independent, String key, String version) {
        return replaceWithIdenticalVersion(independent, BUCKET, key, version);
    }

    private static String replaceWithIdenticalVersion(S3Client independent, String bucket, String key, String version) {
        var head = independent.headObject(HeadObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .versionId(version)
                .build());
        return independent
                .copyObject(CopyObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .copySource(bucket + "/" + key + "?versionId=" + version)
                        .metadataDirective(MetadataDirective.REPLACE)
                        .metadata(head.metadata())
                        .build())
                .versionId();
    }

    private static void closeFencedNativeOwnerDuringActualGet(
            NereusPulsarLedgerOffloaderV1 offloader,
            Offloaded source,
            AtomicReference<SdkHold> heldGet,
            AtomicInteger reads)
            throws Exception {
        try (var metadata = metadata();
                var independent = metadata();
                var bk = bookKeeper()) {
            var factory = factory(metadata, bk);
            var ledger = (ManagedLedgerImpl) factory.open(LEDGER, config(offloader));
            var hold = hold();
            try {
                var captured =
                        ledger.captureOffloadReadSource(source.ledgerId()).get(30, TimeUnit.SECONDS);
                heldGet.set(hold);
                var operation = captured.readOwnedOperation(0, 0, OWNED_READ_LIMITS, () -> true);
                var reading = operation.result();
                assertThat(hold.started().await(30, TimeUnit.SECONDS)).isTrue();
                String path = "/managed-ledgers/" + LEDGER;
                var before = independent.get(path).get(30, TimeUnit.SECONDS).orElseThrow();
                var conflict = independent
                        .put(
                                path,
                                before.getValue(),
                                Optional.of(before.getStat().getVersion()))
                        .get(30, TimeUnit.SECONDS);
                assertThat(conflict.getVersion()).isGreaterThan(before.getStat().getVersion());
                assertThatThrownBy(() -> ledger.setProperty("m5-fenced-close", "conflict"))
                        .isInstanceOf(ManagedLedgerException.BadVersionException.class);
                assertThat(captured.isCurrent()).isFalse();
                var close = closeNativeLedger(ledger);
                assertThatThrownBy(() -> close.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
                assertThat(closeNativeLedger(ledger).cancel(false)).isTrue();
                var repeated = closeNativeLedger(ledger);
                assertThatThrownBy(() -> repeated.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
                int beforeRejected = reads.get();
                assertThatThrownBy(() ->
                                captured.readOwnedAsync(0, 0, OWNED_READ_LIMITS).join())
                        .hasCauseInstanceOf(ManagedLedgerException.ManagedLedgerFencedException.class);
                assertThat(reads.get()).isEqualTo(beforeRejected);
                hold.released().countDown();
                assertThat(reading.get(30, TimeUnit.SECONDS).entries().get(0).payload())
                        .containsExactly(payload(0));
                assertThatThrownBy(() -> close.get(30, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(ManagedLedgerException.ManagedLedgerFencedException.class);
                assertThatThrownBy(() -> repeated.get(30, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(ManagedLedgerException.ManagedLedgerFencedException.class);
                assertThatThrownBy(() -> closeNativeLedger(ledger).join())
                        .hasCauseInstanceOf(ManagedLedgerException.ManagedLedgerFencedException.class);
                System.out.println("NATIVE_PULSAR_FENCED_OWNER_DRAIN ledger=" + source.ledgerId()
                        + " metadataConflictVersion=" + conflict.getVersion() + " fenceFailureRetained=true");
            } finally {
                hold.released().countDown();
                factory.shutdown();
            }
        }
    }

    private static void verifyCapturedFallbackKeepsMetadataVersion(
            NereusPulsarLedgerOffloaderV1 offloader, Offloaded source, AtomicReference<SdkHold> heldGet)
            throws Exception {
        var reads = new NativeReadStats();
        CapturedReadBatch retained;
        try (var metadata = metadata();
                var independent = metadata();
                var bk = BookKeeper.forConfig(new ClientConfiguration()
                                .setUseV2WireProtocol(false)
                                .setBatchReadEnabled(false)
                                .setMetadataServiceUri(
                                        "zk://" + System.getProperty("nereus.m5.nativeOffloadZk") + "/ledgers"))
                        .statsLogger(reads)
                        .build()) {
            var factory = factory(metadata, bk);
            var ledger = (ManagedLedgerImpl) factory.open(LEDGER, config(offloader));
            var hold = hold();
            var unchanged = hold();
            var owned = hold();
            try {
                var captured =
                        ledger.captureOffloadReadSource(source.ledgerId()).get(30, TimeUnit.SECONDS);
                int before = reads.completions.get();
                hold.failure().set(SdkClientException.create("held native GET response was lost"));
                heldGet.set(hold);
                var reading = captured.readAsync(0, 1);
                assertThat(hold.started().await(30, TimeUnit.SECONDS)).isTrue();
                ledger.setProperty("m5-captured-fallback-version", "changed");
                var observed = independent
                        .get("/managed-ledgers/" + LEDGER)
                        .get(30, TimeUnit.SECONDS)
                        .orElseThrow();
                assertThat(observed.getStat().getVersion())
                        .isGreaterThan(captured.sourceState().metadataVersion());
                hold.released().countDown();
                try {
                    assertThatThrownBy(() -> reading.get(30, TimeUnit.SECONDS))
                            .hasRootCauseMessage("held native GET response was lost");
                } finally {
                    if (reading.isDone() && !reading.isCompletedExceptionally()) {
                        reading.getNow(null).close();
                    }
                }
                assertThat(reads.completions.get()).isEqualTo(before);
                assertThat(captured.isCurrent()).isFalse();

                var refreshed =
                        ledger.captureOffloadReadSource(source.ledgerId()).get(30, TimeUnit.SECONDS);
                unchanged.failure().set(SdkClientException.create("unchanged native GET response was lost"));
                heldGet.set(unchanged);
                var fallback = refreshed.readAsync(0, 1);
                assertThat(unchanged.started().await(30, TimeUnit.SECONDS)).isTrue();
                unchanged.released().countDown();
                try (var entries = fallback.get(30, TimeUnit.SECONDS)) {
                    assertThat(entries.getEntry(0).getEntryBytes()).containsExactly(payload(0));
                    assertThat(entries.getEntry(1).getEntryBytes()).containsExactly(payload(1));
                }
                assertThat(reads.completions.get()).isGreaterThan(before);
                int beforeOwned = reads.completions.get();
                owned.failure().set(SdkClientException.create("owned native GET response was lost"));
                heldGet.set(owned);
                var ownedFallback = refreshed.readOwnedAsync(0, 0, OWNED_READ_LIMITS);
                assertThat(owned.started().await(30, TimeUnit.SECONDS)).isTrue();
                owned.released().countDown();
                retained = ownedFallback.get(30, TimeUnit.SECONDS);
                assertThat(retained.sourceState()).isEqualTo(refreshed.sourceState());
                assertThat(retained.bytes()).isEqualTo(ENTRY_BYTES);
                assertThat(reads.completions.get()).isGreaterThan(beforeOwned);
                closeNativeLedger(ledger).get(30, TimeUnit.SECONDS);
                System.out.println("NATIVE_PULSAR_CAPTURED_FALLBACK ledger=" + source.ledgerId()
                        + " originalVersion=" + captured.sourceState().metadataVersion()
                        + " currentVersion=" + refreshed.sourceState().metadataVersion()
                        + " unchangedFallbackReadCompletions=" + (reads.completions.get() - before));
            } finally {
                hold.released().countDown();
                unchanged.released().countDown();
                owned.released().countDown();
                factory.shutdown();
            }
        }
        assertThat(retained.ledgerId()).isEqualTo(source.ledgerId());
        assertThat(retained.entries().get(0).payload()).containsExactly(payload(0));
        System.out.println("NATIVE_PULSAR_OWNED_CAPTURE_BK ledger=" + source.ledgerId()
                + " sourceAndClientsClosed=true bytes=" + retained.bytes());
    }

    private static void closeNativeOwnerDuringCancelledCapture(
            NereusPulsarLedgerOffloaderV1 offloader, Offloaded source, AtomicReference<SdkHold> heldGet)
            throws Exception {
        try (var metadata = metadata();
                var bk = bookKeeper()) {
            var factory = factory(metadata, bk);
            var ledger = (ManagedLedgerImpl) factory.open(LEDGER, config(offloader));
            var hold = hold();
            try {
                heldGet.set(hold);
                var capture = ledger.captureOffloadReadSource(source.ledgerId());
                assertThat(hold.started().await(30, TimeUnit.SECONDS)).isTrue();
                assertThat(capture.cancel(false)).isTrue();
                var close = closeNativeLedger(ledger);
                assertThatThrownBy(() -> close.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
                hold.released().countDown();
                close.get(30, TimeUnit.SECONDS);
                assertThat(capture).isCancelled();
                assertThat(closeNativeLedger(ledger)).isCompleted();
                System.out.println("NATIVE_PULSAR_CANCELLED_CAPTURE_DRAIN ledger=" + source.ledgerId() + " attempt="
                        + source.attempt());
            } finally {
                hold.released().countDown();
                factory.shutdown();
            }
        }
    }

    private static void verifyOwnedCapturedRange(
            NereusPulsarLedgerOffloaderV1 offloader,
            Offloaded source,
            AtomicReference<SdkHold> heldGet,
            AtomicInteger reads,
            String observation)
            throws Exception {
        CapturedReadBatch retained = null;
        try (var metadata = metadata();
                var bk = bookKeeper()) {
            var factory = factory(metadata, bk);
            var ledger = (ManagedLedgerImpl) factory.open(LEDGER, config(offloader));
            var hold = hold();
            try {
                var captured =
                        ledger.captureOffloadReadSource(source.ledgerId()).get(30, TimeUnit.SECONDS);
                int before = reads.get();
                assertThatThrownBy(() ->
                                captured.readOwnedAsync(0, 1, OWNED_READ_LIMITS).join())
                        .hasRootCauseMessage("captured range exceeds its admitted bounds");
                assertThat(reads.get()).isEqualTo(before);
                heldGet.set(hold);
                var operation = captured.readOwnedOperation(0, 0, OWNED_READ_LIMITS, () -> true);
                var reading = operation.result();
                assertThat(hold.started().await(30, TimeUnit.SECONDS)).isTrue();
                if (observation.equals("cancelled")) {
                    assertThat(reading.cancel(false)).isTrue();
                } else if (observation.equals("completed")) {
                    assertThat(reading.completeExceptionally(new IllegalStateException("external observation ended")))
                            .isTrue();
                }
                var mutatedDrain = operation.drained();
                if (observation.equals("cancelled")) {
                    assertThat(mutatedDrain.cancel(false)).isTrue();
                } else {
                    assertThat(mutatedDrain.complete(null)).isTrue();
                }
                var actualDrain = operation.drained();
                assertThat(actualDrain).isNotDone();
                var close = closeNativeLedger(ledger);
                assertThatThrownBy(() -> close.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
                hold.released().countDown();
                actualDrain.get(30, TimeUnit.SECONDS);
                if (observation.equals("delivered")) {
                    retained = reading.get(30, TimeUnit.SECONDS);
                    assertThat(retained.ledgerId()).isEqualTo(source.ledgerId());
                    assertThat(retained.sourceState()).isEqualTo(captured.sourceState());
                    assertThat(retained.bytes()).isEqualTo(ENTRY_BYTES);
                    assertThat(retained.entries())
                            .extracting(value -> value.entryId())
                            .containsExactly(0L);
                } else if (observation.equals("cancelled")) {
                    assertThat(reading).isCancelled();
                } else {
                    assertThatThrownBy(reading::join).hasRootCauseMessage("external observation ended");
                }
                close.get(30, TimeUnit.SECONDS);
                before = reads.get();
                assertThatThrownBy(() ->
                                captured.readOwnedAsync(0, 0, OWNED_READ_LIMITS).join())
                        .hasRootCauseMessage("Managed ledger is closed");
                assertThat(reads.get()).isEqualTo(before);
            } finally {
                hold.released().countDown();
                factory.shutdown();
            }
        }
        if (retained != null) {
            byte[] detached = retained.entries().get(0).payload();
            assertThat(detached).containsExactly(payload(0));
            detached[0] ^= 1;
            assertThat(retained.entries().get(0).payload()).containsExactly(payload(0));
            CapturedReadBatch batch = retained;
            assertThatThrownBy(() -> batch.entries().clear()).isInstanceOf(UnsupportedOperationException.class);
        }
        System.out.println("NATIVE_PULSAR_OWNED_CAPTURE_DRAIN ledger=" + source.ledgerId() + " observation="
                + observation + " sourceAndClientsClosed=true");
    }

    private static void verifyCancelledOwnedRangePreventsFallback(
            NereusPulsarLedgerOffloaderV1 offloader,
            Offloaded source,
            AtomicReference<SdkHold> heldGet,
            S3PulsarOffloadObjectStoreV1 store)
            throws Exception {
        var reads = new NativeReadStats();
        try (var metadata = metadata();
                var bk = BookKeeper.forConfig(new ClientConfiguration()
                                .setUseV2WireProtocol(false)
                                .setBatchReadEnabled(false)
                                .setMetadataServiceUri(
                                        "zk://" + System.getProperty("nereus.m5.nativeOffloadZk") + "/ledgers"))
                        .statsLogger(reads)
                        .build()) {
            var factory = factory(metadata, bk);
            var ledger = (ManagedLedgerImpl) factory.open(LEDGER, config(offloader));
            var hold = hold();
            try {
                var captured =
                        ledger.captureOffloadReadSource(source.ledgerId()).get(30, TimeUnit.SECONDS);
                int before = reads.completions.get();
                hold.failure().set(SdkClientException.create("cancelled owned native GET response was lost"));
                heldGet.set(hold);
                var reading = captured.readOwnedAsync(0, 0, OWNED_READ_LIMITS);
                assertThat(hold.started().await(30, TimeUnit.SECONDS)).isTrue();
                assertThat(reading.cancel(false)).isTrue();
                var providerClose = store.close().toCompletableFuture();
                assertThatThrownBy(() -> providerClose.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
                hold.released().countDown();
                providerClose.get(30, TimeUnit.SECONDS);
                closeNativeLedger(ledger).get(30, TimeUnit.SECONDS);
                assertThat(reads.completions.get()).isEqualTo(before);
                assertThat(reading).isCancelled();
                System.out.println("NATIVE_PULSAR_OWNED_CAPTURE_CANCELLED_FALLBACK ledger=" + source.ledgerId()
                        + " nativeBookKeeperReadCompletions=0");
            } finally {
                hold.released().countDown();
                factory.shutdown();
            }
        }
    }

    private static void closeNativeOwnerWithCachedBufferAliases(
            NereusPulsarLedgerOffloaderV1 offloader, Offloaded source, AtomicInteger actualReads, boolean fenceOnly)
            throws Exception {
        try (var metadata = metadata();
                var bk = bookKeeper()) {
            var factoryConfig = new ManagedLedgerFactoryConfig();
            factoryConfig.setMaxCacheSize(32L * PulsarOffloadLimitCandidateV1.MIB);
            var factory = new ManagedLedgerFactoryImpl(metadata, bk, factoryConfig);
            var ledger = (ManagedLedgerImpl) factory.open(LEDGER, config(offloader));
            Entry borrowed = null;
            Entry duplicate = null;
            io.netty.buffer.ByteBuf payloadAlias = null;
            try {
                var cursor = ledger.openCursor(SUBSCRIPTION);
                var warm = cursor.readEntries(1);
                assertThat(warm).hasSize(1);
                warm.forEach(Entry::release);
                var captured =
                        ledger.captureOffloadReadSource(source.ledgerId()).get(30, TimeUnit.SECONDS);
                int beforeCached = actualReads.get();
                var admission = new CompletableFuture<Void>();
                var cleanup = new AtomicReference<java.util.concurrent.CompletionStage<Void>>();
                if (fenceOnly) {
                    ledger.captureNativeReadView()
                            .get(30, TimeUnit.SECONDS)
                            .installReadAdmission((ledgerId, firstEntry, lastEntry, actualCleanup) -> {
                                assertThat(ledgerId).isEqualTo(source.ledgerId());
                                assertThat(firstEntry).isZero();
                                assertThat(lastEntry).isZero();
                                cleanup.set(actualCleanup);
                                return admission;
                            });
                }
                var cached = readNativeEntry(ledger, source.ledgerId());
                if (fenceOnly) {
                    assertThat(cached).isNotDone();
                    assertThat(actualReads.get()).isEqualTo(beforeCached);
                    assertThat(cleanup.get().toCompletableFuture()).isNotDone();
                    admission.complete(null);
                }
                borrowed = cached.get(30, TimeUnit.SECONDS);
                assertThat(actualReads.get()).isEqualTo(beforeCached);
                duplicate = EntryImpl.create(borrowed);
                payloadAlias = borrowed.getDataBuffer().retainedDuplicate();
                borrowed.release();
                borrowed = null;
                java.util.function.Supplier<CompletableFuture<Void>> stop =
                        fenceOnly ? captured::fenceOwnerReadAdmission : () -> closeNativeLedger(ledger);
                var close = stop.get();
                var repeated = stop.get();
                assertThat(stop.get().cancel(false)).isTrue();
                if (fenceOnly) {
                    assertThat(stop.get().complete(null)).isTrue();
                    assertThatThrownBy(() ->
                                    readNativeEntry(ledger, source.ledgerId()).join())
                            .hasRootCauseMessage("Native owner read admission is closed");
                    assertThatThrownBy(() -> cursor.readEntries(1))
                            .isInstanceOf(ManagedLedgerException.ManagedLedgerFencedException.class);
                    assertThatThrownBy(() -> ledger.captureOffloadReadSource(source.ledgerId())
                                    .join())
                            .hasRootCauseMessage("Native owner read admission is closed");
                    assertThatThrownBy(() -> captured.captureOffloadedReadSourceIdentity()
                                    .join())
                            .hasRootCauseMessage("Native owner read admission is closed");
                    assertThat(actualReads.get()).isEqualTo(beforeCached);
                }
                assertThatThrownBy(() -> close.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
                assertThat(repeated).isNotDone();
                assertThat(duplicate.getData()).containsExactly(payload(0));
                duplicate.release();
                duplicate = null;
                assertThat(close).isNotDone();
                if (fenceOnly) {
                    assertThat(cleanup.get().toCompletableFuture()).isNotDone();
                }
                assertThat(ByteBufUtil.getBytes(payloadAlias)).containsExactly(payload(0));
                payloadAlias.release();
                payloadAlias = null;
                close.get(30, TimeUnit.SECONDS);
                if (fenceOnly) {
                    cleanup.get().toCompletableFuture().get(30, TimeUnit.SECONDS);
                }
                repeated.get(30, TimeUnit.SECONDS);
                System.out.println("NATIVE_CACHED_ENTRY_ALIASES ledger=" + source.ledgerId()
                        + " cacheHitNoGet=true waited=true observerCancellationIsolated=true readFence=" + fenceOnly);
            } finally {
                if (borrowed != null) {
                    borrowed.release();
                }
                if (duplicate != null) {
                    duplicate.release();
                }
                if (payloadAlias != null) {
                    payloadAlias.release();
                }
                try {
                    closeNativeLedger(ledger).get(30, TimeUnit.SECONDS);
                } finally {
                    factory.shutdown();
                }
            }
        }
    }

    private static void closeNativeOwnerDuringOrdinaryCallback(
            NereusPulsarLedgerOffloaderV1 offloader, Offloaded source, boolean failCallback) throws Exception {
        try (var metadata = metadata();
                var bk = bookKeeper()) {
            var factory = factory(metadata, bk);
            var ledger = (ManagedLedgerImpl) factory.open(LEDGER, config(offloader));
            var entered = new CountDownLatch(1);
            var returned = new CountDownLatch(1);
            var delivery = new CompletableFuture<Void>();
            try {
                ledger.asyncReadEntry(
                        PositionFactory.create(source.ledgerId(), 0),
                        new ReadEntryCallback() {
                            @Override
                            public void readEntryComplete(Entry entry, Object ctx) {
                                try {
                                    assertThat(entry.getData()).containsExactly(payload(0));
                                } finally {
                                    entry.release();
                                }
                                entered.countDown();
                                try {
                                    if (!returned.await(30, TimeUnit.SECONDS)) {
                                        throw new IllegalStateException("ordinary callback response hold expired");
                                    }
                                    delivery.complete(null);
                                } catch (InterruptedException error) {
                                    Thread.currentThread().interrupt();
                                    delivery.completeExceptionally(error);
                                    throw new IllegalStateException(error);
                                }
                                if (failCallback) {
                                    throw new IllegalStateException("actual native ordinary callback failed");
                                }
                            }

                            @Override
                            public void readEntryFailed(ManagedLedgerException failure, Object ctx) {
                                delivery.completeExceptionally(failure);
                            }
                        },
                        null);
                assertThat(entered.await(30, TimeUnit.SECONDS)).isTrue();
                var close = closeNativeLedger(ledger);
                var repeated = closeNativeLedger(ledger);
                assertThatThrownBy(() -> close.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
                assertThat(repeated).isNotDone();
                returned.countDown();
                delivery.get(30, TimeUnit.SECONDS);
                if (failCallback) {
                    assertThatThrownBy(() -> close.get(30, TimeUnit.SECONDS))
                            .hasRootCauseMessage("actual native ordinary callback failed");
                    assertThatThrownBy(() -> repeated.get(30, TimeUnit.SECONDS))
                            .hasRootCauseMessage("actual native ordinary callback failed");
                } else {
                    close.get(30, TimeUnit.SECONDS);
                    repeated.get(30, TimeUnit.SECONDS);
                }
                System.out.println("NATIVE_ORDINARY_CALLBACK_DRAIN ledger=" + source.ledgerId()
                        + " waited=true failedCallback=" + failCallback);
            } finally {
                returned.countDown();
                factory.shutdown();
            }
        }
    }

    private static Map<String, Object> observeMaterializedReadTickets(
            PulsarNativeBindingV1 binding,
            PulsarNativeBindingReadViewV1 view,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2 objects,
            SelectorResponseFault responses,
            boolean held)
            throws Exception {
        var exact = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(responses);
        var p1Key = new OxiaV2AuthorityKeys(binding.authorityRoot())
                .aggregateKey(binding.authority().aggregate().binding().incarnationIdentity());
        var c = com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                        exact.read(p1Key)
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS)
                                .orElseThrow()
                                .canonicalStoredBytes())
                .referenceMutationTickets();
        var snapshot = new java.util.TreeMap<String, Object>();
        snapshot.put(p1Key, c);
        var sdkRecovery = c.stream()
                .filter(ticket -> ticket.referenceKind()
                        == com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                .RECOVERY_CHECKPOINT_OR_SNAPSHOT)
                .toList();
        assertThat(sdkRecovery).hasSize(1);
        var readTickets = c.stream()
                .filter(ticket -> ticket.referenceKind()
                        == com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                .READ_GENERATION_PIN_OR_OPEN_HANDLE)
                .toList();
        assertThat(c).hasSize(held ? 2 : 1);
        assertThat(readTickets).hasSize(held ? 1 : 0);
        var resources = new java.util.TreeSet<PhysicalResourceIdV2>(view.members());
        resources.addAll(view.materializedMap()
                .fallbackPhysicalReferences(binding, view.bookKeeperNamespaceBinding(), view.objectBinding()));
        view.materializedMap()
                .readableOutputs()
                .forEach(value -> resources.add(value.resource().orElseThrow()));
        var bk = binding.materializationQuota(bookKeeper).get(30, TimeUnit.SECONDS);
        var sdkSpec = ((com.nereusstream.storage.bookkeeper.M5PulsarNativeMetadataDriverV1)
                        bookKeeper.borrowClient().getMetadataClientDriver())
                .createSpec();
        var operations = new java.util.HashSet<Sha256Digest>();
        var roots = new java.util.HashSet<Sha256Digest>();
        for (var resource : resources) {
            var route = resource instanceof PhysicalResourceIdV2.ObjectVersion ? objects : bk;
            var d = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                            route.read(resource.authorityKey())
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS)
                                    .orElseThrow()
                                    .canonicalStoredBytes())
                    .activeWriterTickets();
            snapshot.put(resource.authorityKey(), d);
            var sdkRoot = resource instanceof PhysicalResourceIdV2.BookKeeperLedger
                    ? Sha256Digest.hash(CanonicalBytes.copyOf(java.nio.ByteBuffer.allocate(100)
                            .putInt(0x4e505348)
                            .put(sdkSpec.sha256().bytes().toByteArray())
                            .put(resource.sha256().bytes().toByteArray())
                            .put(Sha256Digest.hash(sdkSpec.allocatorOwner())
                                    .bytes()
                                    .toByteArray())
                            .array()))
                    : null;
            var readD = d.stream()
                    .filter(ticket -> !ticket.externalFactsRootSha256().equals(sdkRoot))
                    .toList();
            if (held) {
                assertThat(readD).singleElement().satisfies(ticket -> {
                    assertThat(ticket.writerClass())
                            .isEqualTo(
                                    com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                            .ProofBoundWriterClassV1.OWNER_WORKER_LEASE_HANDLE_PIN_V1);
                    operations.add(ticket.operationIdSha256());
                    roots.add(ticket.externalFactsRootSha256());
                });
            } else {
                assertThat(readD).isEmpty();
            }
        }
        if (held) {
            assertThat(operations).hasSize(1);
            assertThat(roots).hasSize(1);
        }
        return snapshot;
    }

    private static void verifyOrdinaryMaterializedReads(
            PulsarNativeBindingV1 binding,
            PulsarNativeM4ControlV1 control,
            ManagedLedgerImpl original,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            PulsarNativeReadCellBudgetV1 budget,
            java.util.concurrent.Executor worker,
            Map<org.apache.bookkeeper.mledger.Position, byte[]> expected,
            com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationPlan plan,
            SelectorResponseFault responses)
            throws Exception {
        closeNativeLedger(original).get(30, TimeUnit.SECONDS);
        verifyMappedParentOriginalSdk(binding, bookKeeper, offloader, budget, responses, true);
        var controlCharge = new PulsarNativeReadCellBudgetV1.Usage(1, 0, 0, 24L * 1024 * 1024);
        assertThat(budget.usage()).isEqualTo(controlCharge);
        var capsule = Path.of(System.getProperty("nereus.m5.nativeM4AdmissionFile"));
        var admission = PulsarNativeM4DrainAdmissionV1.load(
                capsule,
                Sha256Digest.copyOf(
                        java.util.HexFormat.of().parseHex(System.getProperty("nereus.m5.nativeM4AdmissionSha256"))));
        var event = Executors.newSingleThreadExecutor();
        try {
            for (int restart = 0; restart < 2; restart++) {
                try (var nativeMetadata = metadata()) {
                    var nativeFactory = guardedMappedFactory(
                            binding, nativeMetadata, bookKeeper, offloader, budget, worker, responses);
                    var factoryCharge = budget.usage();
                    ManagedLedgerImpl actual = null;
                    PulsarNativeM4ReadOwnerV1 reader = null;
                    PulsarNativeSourcePreparationV1 prepared = null;
                    try {
                        actual = nativeFactory.openLedger(
                                config(offloader).setMaxUnackedRangesToPersistInMetadataStore(0));
                        var objects = control.readSelectedMaterialization(worker)
                                .get(30, TimeUnit.SECONDS)
                                .providerProfile()
                                .physicalBinding()
                                .physicalNamespace();
                        prepared = control.prepareMaterializedReadSources(
                                        actual, nativeFactory.namespace(), offloader, objects, budget, worker)
                                .get(60, TimeUnit.SECONDS);
                        var recovery = control.prepareMaterializedReadOwner(
                                        prepared, admission, nativeFactory.namespace(), offloader, worker)
                                .get(60, TimeUnit.SECONDS);
                        reader = recovery.recover(worker, event, 2, budget).get(60, TimeUnit.SECONDS);
                        for (var item : expected.entrySet()) {
                            var entry = readOrdinaryEntry(actual, item.getKey()).get(30, TimeUnit.SECONDS);
                            assertThat(entry.getPosition()).isEqualTo(item.getKey());
                            assertThat(entry.getData()).isEqualTo(item.getValue());
                            entry.release();
                        }
                        var cursor = actual.openCursor("mapped-ordinary-" + restart, InitialPosition.Earliest);
                        var pending = new java.util.LinkedHashMap<>(expected);
                        while (!pending.isEmpty()) {
                            var entries = cursor.readEntries(
                                    Math.min(pending.size(), budget.readLimits().maximumEntries()));
                            assertThat(entries).isNotEmpty();
                            for (var entry : entries) {
                                try {
                                    assertThat(entry.getData()).isEqualTo(pending.remove(entry.getPosition()));
                                } finally {
                                    entry.release();
                                }
                            }
                        }
                        cursor.close();
                        var readFootprint = prepared.view().get(30, TimeUnit.SECONDS);
                        var readObjects = binding.materializationQuota(readFootprint
                                        .materializedConnectionForControl()
                                        .physical())
                                .get(30, TimeUnit.SECONDS);
                        var retained = readOrdinaryEntry(
                                        actual, expected.keySet().iterator().next())
                                .get(30, TimeUnit.SECONDS);
                        boolean loseReadCleanup = restart == 1;
                        boolean losePhysicalCleanup = restart == 0;
                        var readPhysicalTarget = readFootprint
                                .materializedMap()
                                .readableOutputs()
                                .getFirst()
                                .resource()
                                .orElseThrow();
                        var readP1Key = new OxiaV2AuthorityKeys(binding.authorityRoot())
                                .aggregateKey(binding.authority()
                                        .aggregate()
                                        .binding()
                                        .incarnationIdentity());
                        if (loseReadCleanup) {
                            responses.holdAppliedPut(readP1Key, bytes -> {
                                try {
                                    return com
                                            .nereusstream
                                            .storage
                                            .object
                                            .retention
                                            .M5PulsarAggregateAuthorityCodecV1
                                            .decodeAuthority(CanonicalBytes.copyOf(bytes))
                                            .referenceMutationTickets()
                                            .stream()
                                            .noneMatch(ticket -> ticket.referenceKind()
                                                    == com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                                            .ReferenceKindV1.READ_GENERATION_PIN_OR_OPEN_HANDLE);
                                } catch (RuntimeException anotherRecord) {
                                    return false;
                                }
                            });
                        } else if (losePhysicalCleanup) {
                            responses.holdAppliedPut(null, bytes -> {
                                try {
                                    var value = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1
                                            .decodeAuthority(CanonicalBytes.copyOf(bytes));
                                    return value.target().resourceId().equals(readPhysicalTarget)
                                            && value.activeWriterTickets().isEmpty();
                                } catch (RuntimeException anotherRecord) {
                                    return false;
                                }
                            });
                        }
                        CompletableFuture<Void> closing;
                        Map<String, Object> heldTickets;
                        try {
                            heldTickets = observeMaterializedReadTickets(
                                    binding, readFootprint, nativeFactory.namespace(), readObjects, responses, true);
                            closing = reader.drainForRecovery(worker);
                            assertThat(reader.drainForRecovery(worker).cancel(false))
                                    .isTrue();
                            assertThatThrownBy(() -> closing.get(1, TimeUnit.SECONDS))
                                    .isInstanceOf(TimeoutException.class);
                            assertThat(budget.usage().owners()).isGreaterThan(controlCharge.owners());
                            assertThat(observeMaterializedReadTickets(
                                            binding,
                                            readFootprint,
                                            nativeFactory.namespace(),
                                            readObjects,
                                            responses,
                                            true))
                                    .isEqualTo(heldTickets);
                        } finally {
                            retained.release();
                        }
                        if (loseReadCleanup || losePhysicalCleanup) {
                            try (var independentReadMetadata = OxiaClientBuilder.create(
                                            System.getProperty("nereus.m5.nativeOffloadOxia"))
                                    .namespace("default")
                                    .asyncClient()
                                    .get(30, TimeUnit.SECONDS)) {
                                assertThat(responses.heldApplied.await(30, TimeUnit.SECONDS))
                                        .isTrue();
                                var independentRead =
                                        new com.nereusstream.metadata.oxia.v2.retention
                                                .Oxia09ExactMetadataTransactionStoreV1(independentReadMetadata);
                                var originalCleanupKey = losePhysicalCleanup ? responses.heldActualKey : readP1Key;
                                if (losePhysicalCleanup) {
                                    assertThat(com.nereusstream.storage.object.retention
                                                    .M5PulsarAggregateAuthorityCodecV1.decodeAuthority(independentRead
                                                            .read(readP1Key)
                                                            .toCompletableFuture()
                                                            .get(30, TimeUnit.SECONDS)
                                                            .orElseThrow()
                                                            .canonicalStoredBytes())
                                                    .referenceMutationTickets())
                                            .isEqualTo(heldTickets.get(readP1Key));
                                }
                                var originalCleanupCandidate = independentRead
                                        .read(originalCleanupKey)
                                        .toCompletableFuture()
                                        .get(30, TimeUnit.SECONDS)
                                        .orElseThrow();
                                responses.loseHeldPutReply();
                                int originalClearAttempts = responses.watchedPutAttempts.get();
                                if (losePhysicalCleanup) {
                                    assertThatThrownBy(() -> closing.get(30, TimeUnit.SECONDS))
                                            .hasRootCauseMessage("original read-hold D cleanup remains unknown");
                                } else {
                                    assertThatThrownBy(() -> closing.get(30, TimeUnit.SECONDS))
                                            .hasRootCauseInstanceOf(TimeoutException.class)
                                            .hasRootCauseMessage("actual selector observation unavailable");
                                }
                                var unresolvedRead = budget.usage();
                                assertThat(unresolvedRead.owners()).isEqualTo(factoryCharge.owners() + 1);
                                responses.clear();
                                reader.fence().get(30, TimeUnit.SECONDS);
                                assertThat(responses.watchedPutAttempts.get()).isEqualTo(originalClearAttempts);
                                assertThat(independentRead
                                                .read(originalCleanupKey)
                                                .toCompletableFuture()
                                                .get(30, TimeUnit.SECONDS)
                                                .orElseThrow())
                                        .isEqualTo(originalCleanupCandidate);
                                assertThat(budget.usage()).isEqualTo(factoryCharge);
                                reader.drainForRecovery(worker).get(30, TimeUnit.SECONDS);
                                System.out.println((losePhysicalCleanup
                                                ? "NATIVE_READ_LIFETIME_D_CLEANUP_UNKNOWN"
                                                : "NATIVE_READ_LIFETIME_C_CLEANUP_UNKNOWN")
                                        + " originalCellOwnerRetained=true"
                                        + " metadataOnlyOriginalCleanup=true candidateVersionPreserved=true"
                                        + " originalMutationRedispatch=false wholeM5Authority=false");
                            } finally {
                                responses.clear();
                            }
                        } else {
                            closing.get(30, TimeUnit.SECONDS);
                        }
                        observeMaterializedReadTickets(
                                binding, readFootprint, nativeFactory.namespace(), readObjects, responses, false);
                        assertThat(budget.usage()).isEqualTo(factoryCharge);
                        System.out.println("NATIVE_READ_LIFETIME_ORIGINAL_C_D entryAliasHeld=true"
                                + " originalTicketsStable=true observerCancellationIsolated=true"
                                + " originalDrainClearsTickets=true wholeM5Authority=false");
                        if (restart == 1) {
                            closeNativeLedger(actual).get(30, TimeUnit.SECONDS);
                            nativeFactory.close();
                            assertThat(budget.usage()).isEqualTo(controlCharge);
                            verifyMaterializedFallbackRelease(
                                    binding,
                                    control,
                                    actual,
                                    reader,
                                    bookKeeper,
                                    offloader,
                                    budget,
                                    worker,
                                    expected,
                                    admission,
                                    responses);
                        }
                    } finally {
                        if (reader != null) {
                            reader.fence().get(30, TimeUnit.SECONDS);
                        }
                        if (prepared != null) {
                            prepared.close().get(30, TimeUnit.SECONDS);
                        }
                        if (actual != null) {
                            closeNativeLedger(actual).get(30, TimeUnit.SECONDS);
                        }
                        nativeFactory.close();
                    }
                }
            }
        } finally {
            event.shutdown();
            assertThat(event.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static void verifyMaterializedFallbackRelease(
            PulsarNativeBindingV1 binding,
            PulsarNativeM4ControlV1 control,
            ManagedLedgerImpl drainedLedger,
            PulsarNativeM4ReadOwnerV1 drainedOwner,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            PulsarNativeReadCellBudgetV1 budget,
            java.util.concurrent.Executor worker,
            Map<org.apache.bookkeeper.mledger.Position, byte[]> expected,
            PulsarNativeM4DrainAdmissionV1 admission,
            SelectorResponseFault responses)
            throws Exception {
        var originalProtections = drainedOwner.fallbackSources();
        assertThat(originalProtections).isNotEmpty();
        closeNativeLedger(drainedLedger).get(30, TimeUnit.SECONDS);
        try (var nativeMetadata = metadata();
                var proofMetadata = OxiaClientBuilder.create(System.getProperty("nereus.m5.nativeOffloadOxia"))
                        .namespace("default")
                        .asyncClient()
                        .get(30, TimeUnit.SECONDS)) {
            var nativeFactory =
                    guardedMappedFactory(binding, nativeMetadata, bookKeeper, offloader, budget, worker, responses);
            var factoryCharge = budget.usage();
            ManagedLedgerImpl actual = null;
            PulsarNativeSourcePreparationV1 prepared = null;
            PulsarNativeM4ReadOwnerV1 successor = null;
            try {
                actual = nativeFactory.openLedger(config(offloader).setMaxUnackedRangesToPersistInMetadataStore(0));
                var objects = control.readSelectedMaterialization(worker)
                        .get(30, TimeUnit.SECONDS)
                        .providerProfile()
                        .physicalBinding()
                        .physicalNamespace();
                prepared = control.prepareMaterializedReadSources(
                                actual, nativeFactory.namespace(), offloader, objects, budget, worker)
                        .get(60, TimeUnit.SECONDS);
                var preferred = prepared.view().get(60, TimeUnit.SECONDS);
                var exact = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(
                        proofMetadata);
                var firstProtectionKey = control.protectionKey(originalProtections.get(0));
                var physicalReferences = new java.util.TreeSet<PhysicalResourceIdV2>(preferred
                        .materializedMap()
                        .fallbackPhysicalReferences(
                                binding, preferred.bookKeeperNamespaceBinding(), preferred.objectBinding()));
                preferred
                        .materializedMap()
                        .readableOutputs()
                        .forEach(
                                value -> physicalReferences.add(value.resource().orElseThrow()));
                var bkQuota = binding.materializationQuota(bookKeeper).get(30, TimeUnit.SECONDS);
                var objectQuota = binding.materializationQuota(
                                preferred.materializedConnectionForControl().physical())
                        .get(30, TimeUnit.SECONDS);
                responses.holdAppliedPut(firstProtectionKey, bytes -> {
                    try {
                        return M4ReadControlCodecV1.decodeProtection(CanonicalBytes.copyOf(bytes))
                                        .state()
                                == ProtectionState.RELEASED;
                    } catch (RuntimeException anotherRecord) {
                        return false;
                    }
                });
                var retiring =
                        control.retireFallback(drainedOwner, preferred, nativeFactory.namespace(), offloader, worker);
                try {
                    assertThat(responses.heldApplied.await(30, TimeUnit.SECONDS))
                            .isTrue();
                    assertThat(control.retireFallback(
                                            drainedOwner, preferred, nativeFactory.namespace(), offloader, worker)
                                    .cancel(false))
                            .isTrue();
                    assertThat(retiring).isNotDone();
                    var originalRelease = exact.read(firstProtectionKey)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow();
                    var originalCharge = budget.usage();
                    String p1Key = new OxiaV2AuthorityKeys(binding.authorityRoot())
                            .aggregateKey(
                                    binding.authority().aggregate().binding().incarnationIdentity());
                    var cTickets =
                            com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                            exact.read(p1Key)
                                                    .toCompletableFuture()
                                                    .get(30, TimeUnit.SECONDS)
                                                    .orElseThrow()
                                                    .canonicalStoredBytes())
                                    .referenceMutationTickets();
                    assertThat(cTickets).hasSize(2);
                    assertThat(cTickets.stream()
                                    .map(ticket -> ticket.referenceKind())
                                    .toList())
                            .containsExactlyInAnyOrder(
                                    com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                            .SOURCE_PROTECTION,
                                    com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                            .RECOVERY_CHECKPOINT_OR_SNAPSHOT);
                    var operations = new java.util.HashSet<Sha256Digest>();
                    var roots = new java.util.HashSet<Sha256Digest>();
                    for (var resource : physicalReferences) {
                        var route = resource instanceof PhysicalResourceIdV2.ObjectVersion ? objectQuota : bkQuota;
                        var value = route.read(resource.authorityKey())
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS)
                                .orElseThrow();
                        var tickets = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                                        value.canonicalStoredBytes())
                                .activeWriterTickets();
                        assertThat(tickets).singleElement().satisfies(ticket -> {
                            assertThat(ticket.writerClass())
                                    .isEqualTo(
                                            com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                                    .ProofBoundWriterClassV1.M4_SOURCE_PROTECTION_RELEASE_V1);
                            operations.add(ticket.operationIdSha256());
                            roots.add(ticket.externalFactsRootSha256());
                        });
                    }
                    assertThat(operations).hasSize(1);
                    assertThat(roots).hasSize(1);
                    responses.loseHeldPutReply();
                    int originalSdkAttempts = responses.watchedPutAttempts.get();
                    assertThatThrownBy(() -> retiring.get(60, TimeUnit.SECONDS))
                            .hasRootCauseInstanceOf(TimeoutException.class);
                    assertThat(budget.usage()).isEqualTo(originalCharge);
                    assertThat(
                                    com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1
                                            .decodeAuthority(exact.read(p1Key)
                                                    .toCompletableFuture()
                                                    .get(30, TimeUnit.SECONDS)
                                                    .orElseThrow()
                                                    .canonicalStoredBytes())
                                            .referenceMutationTickets())
                            .containsExactlyElementsOf(cTickets);
                    responses.clear();
                    var retired = control.retireFallback(
                                    drainedOwner, preferred, nativeFactory.namespace(), offloader, worker)
                            .get(60, TimeUnit.SECONDS);
                    assertThat(responses.watchedPutAttempts.get()).isEqualTo(originalSdkAttempts);
                    assertThat(exact.read(firstProtectionKey)
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS)
                                    .orElseThrow())
                            .isEqualTo(originalRelease);
                    for (var resource : physicalReferences) {
                        var route = resource instanceof PhysicalResourceIdV2.ObjectVersion ? objectQuota : bkQuota;
                        var value = route.read(resource.authorityKey())
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS)
                                .orElseThrow();
                        assertThat(com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                                                value.canonicalStoredBytes())
                                        .activeWriterTickets())
                                .allSatisfy(ticket -> assertThat(ticket.writerClass())
                                        .isEqualTo(
                                                com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                                        .ProofBoundWriterClassV1.OWNER_WORKER_LEASE_HANDLE_PIN_V1));
                    }
                    System.out.println("NATIVE_M4_RELEASED_ORIGINAL_C_D sdkReplyLost=true originalMixedTickets=true"
                            + " originalCandidateVersionPreserved=true sdkRedispatch=false"
                            + " observerCancellationIsolated=true"
                            + " wholeM5Authority=false");
                    successor = retired.successorOwner();
                    assertThat(retired.selector().mode()).isEqualTo(SelectorMode.PREFERRED_ONLY);
                    assertThat(retired.batch().sources()).containsExactlyElementsOf(originalProtections);
                    assertThat(retired.terminal().readAdmissionEpoch())
                            .isEqualTo(drainedOwner.selector().readAdmissionEpoch());
                } finally {
                    responses.clear();
                }
                var retiredBatch = control.coordinator().readSelector().orElseThrow().activeBatches().stream()
                        .filter(value -> value.sources().equals(originalProtections))
                        .findFirst()
                        .orElseThrow();
                for (var protection : originalProtections) {
                    var stored = M4ReadControlCodecV1.decodeProtection(exact.read(control.protectionKey(protection))
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow()
                            .canonicalStoredBytes());
                    assertThat(stored.identity()).isEqualTo(protection);
                    assertThat(stored.state()).isEqualTo(ProtectionState.RELEASED);
                    assertThat(stored.releasedByBatchSha256()).contains(retiredBatch.batchIdSha256());
                }
                for (var item : expected.entrySet()) {
                    var entry = readOrdinaryEntry(actual, item.getKey()).get(30, TimeUnit.SECONDS);
                    assertThat(entry.getData()).isEqualTo(item.getValue());
                    entry.release();
                }
                verifyActualCursorMetadata(successor, budget, worker, actual, nativeFactory.namespace(), control);
                verifyRepeatedMappedMaterialization(
                        binding,
                        control,
                        actual,
                        successor,
                        nativeFactory.namespace(),
                        offloader,
                        budget,
                        worker,
                        expected,
                        admission,
                        responses,
                        nativeFactory,
                        bookKeeper,
                        factoryCharge);
            } finally {
                if (successor != null) {
                    successor.fence().get(30, TimeUnit.SECONDS);
                }
                if (prepared != null) {
                    prepared.close().get(30, TimeUnit.SECONDS);
                }
                if (actual != null) {
                    closeNativeLedger(actual).get(30, TimeUnit.SECONDS);
                }
                nativeFactory.close();
            }
        }
    }

    private static List<com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.ReferenceMutationTicketV1>
            observeSdkRecoveryReferences(PulsarNativeBindingV1 binding, SelectorResponseFault responses)
                    throws Exception {
        var key = new OxiaV2AuthorityKeys(binding.authorityRoot())
                .aggregateKey(binding.authority().aggregate().binding().incarnationIdentity());
        var exact = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(responses);
        return com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                        exact.read(key)
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS)
                                .orElseThrow()
                                .canonicalStoredBytes())
                .referenceMutationTickets()
                .stream()
                .filter(ticket -> ticket.referenceKind()
                        == com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                .RECOVERY_CHECKPOINT_OR_SNAPSHOT)
                .toList();
    }

    private static final class NativeDataReplyLoss implements AutoCloseable {
        private final io.netty.channel.Channel channel;
        private final AtomicBoolean dropAdd = new AtomicBoolean(true);
        private final AtomicBoolean dropForce = new AtomicBoolean(true);
        private final AtomicBoolean dropLac = new AtomicBoolean();
        private final CountDownLatch lacApplied = new CountDownLatch(1);
        private final AtomicInteger lacRequests = new AtomicInteger();
        private final CountDownLatch forceApplied = new CountDownLatch(1);
        private final AtomicInteger forceRequests = new AtomicInteger();
        private final AtomicBoolean dropFences = new AtomicBoolean();
        private final AtomicBoolean dropOriginalFence = new AtomicBoolean();
        private final CountDownLatch originalFenceApplied = new CountDownLatch(1);
        private final AtomicBoolean holdFence = new AtomicBoolean();
        private final CountDownLatch fenceHeld = new CountDownLatch(1);
        private volatile io.netty.channel.ChannelHandlerContext heldContext;
        private volatile Object heldWire;
        private final CountDownLatch addApplied = new CountDownLatch(1);
        private final CountDownLatch fenceApplied = new CountDownLatch(1);
        private final AtomicInteger addRequests = new AtomicInteger();

        private NativeDataReplyLoss(io.netty.channel.Channel channel, long ledgerId, long entryId) throws Exception {
            this.channel = channel;
            channel.eventLoop()
                    .submit(() -> channel.pipeline()
                            .addBefore(
                                    "bookieProtoDecoder",
                                    "m5-real-reply-loss",
                                    new io.netty.channel.ChannelDuplexHandler() {
                                        @Override
                                        public void write(
                                                io.netty.channel.ChannelHandlerContext ctx,
                                                Object msg,
                                                io.netty.channel.ChannelPromise promise)
                                                throws Exception {
                                            if (msg instanceof org.apache.bookkeeper.proto.Request request
                                                    && request.hasAddRequest()
                                                    && request.getAddRequest().getLedgerId() == ledgerId
                                                    && request.getAddRequest().getEntryId() == entryId) {
                                                addRequests.incrementAndGet();
                                            }
                                            if (msg instanceof org.apache.bookkeeper.proto.Request request
                                                    && request.hasForceLedgerRequest()
                                                    && request.getForceLedgerRequest()
                                                                    .getLedgerId()
                                                            == ledgerId) {
                                                forceRequests.incrementAndGet();
                                            }
                                            if (msg instanceof org.apache.bookkeeper.proto.Request request
                                                    && request.hasWriteLacRequest()
                                                    && request.getWriteLacRequest()
                                                                    .getLedgerId()
                                                            == ledgerId) {
                                                lacRequests.incrementAndGet();
                                            }
                                            ctx.write(msg, promise);
                                        }

                                        @Override
                                        public void channelRead(io.netty.channel.ChannelHandlerContext ctx, Object msg)
                                                throws Exception {
                                            if (msg instanceof io.netty.buffer.ByteBuf wire) {
                                                var response = new org.apache.bookkeeper.proto.Response();
                                                response.parseFrom(wire.duplicate(), wire.readableBytes());
                                                if (response.hasAddResponse()
                                                        && response.getAddResponse()
                                                                        .getLedgerId()
                                                                == ledgerId
                                                        && response.getAddResponse()
                                                                        .getEntryId()
                                                                == entryId
                                                        && response.getAddResponse()
                                                                        .getStatus()
                                                                == org.apache.bookkeeper.proto.StatusCode.EOK
                                                        && dropAdd.compareAndSet(true, false)) {
                                                    io.netty.util.ReferenceCountUtil.release(msg);
                                                    addApplied.countDown();
                                                    return;
                                                }
                                                if (response.hasForceLedgerResponse()
                                                        && response.getForceLedgerResponse()
                                                                        .getLedgerId()
                                                                == ledgerId
                                                        && response.getForceLedgerResponse()
                                                                        .getStatus()
                                                                == org.apache.bookkeeper.proto.StatusCode.EOK
                                                        && dropForce.compareAndSet(true, false)) {
                                                    io.netty.util.ReferenceCountUtil.release(msg);
                                                    forceApplied.countDown();
                                                    return;
                                                }
                                                if (response.hasWriteLacResponse()
                                                        && response.getWriteLacResponse()
                                                                        .getLedgerId()
                                                                == ledgerId
                                                        && response.getWriteLacResponse()
                                                                        .getStatus()
                                                                == org.apache.bookkeeper.proto.StatusCode.EOK
                                                        && dropLac.compareAndSet(true, false)) {
                                                    io.netty.util.ReferenceCountUtil.release(msg);
                                                    lacApplied.countDown();
                                                    return;
                                                }
                                                if (dropOriginalFence.get()
                                                        && response.hasReadResponse()
                                                        && response.getReadResponse()
                                                                        .getLedgerId()
                                                                == ledgerId
                                                        && response.getReadResponse()
                                                                        .getEntryId()
                                                                == org.apache.bookkeeper.proto.BookieProtocol
                                                                        .LAST_ADD_CONFIRMED
                                                        && response.getReadResponse()
                                                                        .getStatus()
                                                                == org.apache.bookkeeper.proto.StatusCode.EOK
                                                        && dropOriginalFence.compareAndSet(true, false)) {
                                                    io.netty.util.ReferenceCountUtil.release(msg);
                                                    originalFenceApplied.countDown();
                                                    return;
                                                }
                                                if (holdFence.get()
                                                        && response.hasReadResponse()
                                                        && response.getReadResponse()
                                                                        .getLedgerId()
                                                                == ledgerId
                                                        && response.getReadResponse()
                                                                        .getEntryId()
                                                                == org.apache.bookkeeper.proto.BookieProtocol
                                                                        .LAST_ADD_CONFIRMED
                                                        && response.getReadResponse()
                                                                        .getStatus()
                                                                == org.apache.bookkeeper.proto.StatusCode.EOK
                                                        && holdFence.compareAndSet(true, false)) {
                                                    heldContext = ctx;
                                                    heldWire = msg;
                                                    fenceHeld.countDown();
                                                    return;
                                                }
                                                if (dropFences.get()
                                                        && response.hasReadResponse()
                                                        && response.getReadResponse()
                                                                        .getLedgerId()
                                                                == ledgerId
                                                        && response.getReadResponse()
                                                                        .getEntryId()
                                                                == org.apache.bookkeeper.proto.BookieProtocol
                                                                        .LAST_ADD_CONFIRMED
                                                        && response.getReadResponse()
                                                                        .getStatus()
                                                                == org.apache.bookkeeper.proto.StatusCode.EOK) {
                                                    io.netty.util.ReferenceCountUtil.release(msg);
                                                    fenceApplied.countDown();
                                                    return;
                                                }
                                            }
                                            ctx.fireChannelRead(msg);
                                        }
                                    }))
                    .get(30, TimeUnit.SECONDS);
        }

        private static NativeDataReplyLoss install(
                org.apache.bookkeeper.client.BookKeeper sdk,
                org.apache.bookkeeper.net.BookieId address,
                long ledgerId,
                long entryId)
                throws Exception {
            var dataPort = sdk.getClientCtx().getBookieClient();
            var delegateField = dataPort.getClass().getDeclaredField("delegate");
            assertThat(delegateField.trySetAccessible()).isTrue();
            var original = (org.apache.bookkeeper.proto.BookieClientImpl) delegateField.get(dataPort);
            var connected = new CompletableFuture<org.apache.bookkeeper.proto.PerChannelBookieClient>();
            original.lookupClient(address)
                    .obtain(
                            (rc, client) -> {
                                if (rc == org.apache.bookkeeper.client.BKException.Code.OK) {
                                    connected.complete(client);
                                } else {
                                    connected.completeExceptionally(
                                            org.apache.bookkeeper.client.BKException.create(rc));
                                }
                            },
                            ledgerId);
            var channelField = org.apache.bookkeeper.proto.PerChannelBookieClient.class.getDeclaredField("channel");
            assertThat(channelField.trySetAccessible()).isTrue();
            return new NativeDataReplyLoss(
                    (io.netty.channel.Channel) channelField.get(connected.get(30, TimeUnit.SECONDS)),
                    ledgerId,
                    entryId);
        }

        private void releaseFence() throws Exception {
            channel.eventLoop()
                    .submit(() -> {
                        var original = heldWire;
                        if (original != null) {
                            heldWire = null;
                            heldContext.fireChannelRead(original);
                        }
                    })
                    .get(30, TimeUnit.SECONDS);
        }

        @Override
        public void close() throws Exception {
            channel.eventLoop()
                    .submit(() -> {
                        if (heldWire != null) {
                            io.netty.util.ReferenceCountUtil.release(heldWire);
                            heldWire = null;
                        }
                        if (channel.pipeline().get("m5-real-reply-loss") != null) {
                            channel.pipeline().remove("m5-real-reply-loss");
                        }
                    })
                    .get(30, TimeUnit.SECONDS);
        }
    }

    private static void verifyNativeBookKeeperLifecycle(
            PulsarNativeBindingV1 binding,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            NereusPulsarLedgerOffloaderV1 offloader,
            PhysicalResourceIdV2.Namespace objects,
            SelectorResponseFault responses,
            MetadataStoreExtended metadata,
            boolean expiry)
            throws Exception {
        var worker = Executors.newSingleThreadExecutor();
        var event = Executors.newSingleThreadExecutor();
        var staging = Files.createTempDirectory("nereus-native-bk-lifecycle-");
        var budget = readBudget(binding, 6);
        var exact = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(responses);
        var authority = new com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1(exact);
        var registry = nativeExpectedWriterRegistry(
                new CapabilityBinding(1, bootstrap.capability().configurationDigest()));
        String key = new OxiaV2AuthorityKeys(binding.authorityRoot())
                .aggregateKey(binding.authority().aggregate().binding().incarnationIdentity());
        var gcPolicy = Optional.of(
                new com.nereusstream.storage.bookkeeper.M5PulsarNativeCreateSpecV1.BookKeeperGcGraceV1(0, 1, 25));
        var factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setMaxCacheSize(0);
        var nativeConfig = config(offloader).setMaxEntriesPerLedger(3).setMaxUnackedRangesToPersistInMetadataStore(0);
        String uri = "zk://" + System.getProperty("nereus.m5.nativeOffloadZk") + "/ledgers";
        PulsarNativeBookKeeperCreateV1 sourceCreation = null;
        ManagedLedgerFactoryImpl sourceFactory = null;
        PulsarNativeManagedLedgerFactoryV2 factory = null;
        PulsarNativeSourcePreparationV1 prepared = null;
        PulsarNativeMaterializationSourceV2 source = null;
        PulsarNativeM4ReadOwnerV1 owner = null;
        PulsarNativeBindingReadViewV1 unregisteredView = null;
        var initialControl = binding.m4Control();
        PulsarNativeM4ControlV1 activeControl = null;
        Throwable primary = null;
        try {
            bootstrap
                    .nativeDeleteCellBudget()
                    .initialize(new com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteCellBudgetV2.Limits(1, 1))
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS);
            assertThat(com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                    exact.read(key)
                                            .toCompletableFuture()
                                            .get(30, TimeUnit.SECONDS)
                                            .orElseThrow()
                                            .canonicalStoredBytes())
                            .firstNativeFactoryScope())
                    .isEmpty();
            // This is the existing original-source qualification phase, before the first M5 metadata writer.
            // BK creation/data still uses the actual original v3 guarded scope and permanent birth quota.
            sourceCreation = binding.openFactoryBookKeeper(
                            uri, bootstrap, budget, NATIVE_BK_CANONICAL_CAPACITY, worker, gcPolicy)
                    .get(60, TimeUnit.SECONDS);
            var originalScope = sourceCreation.spec();
            assertThat(originalScope.bookKeeperGcGrace()).isEqualTo(gcPolicy);
            sourceFactory = factory(metadata, sourceCreation.namespace().borrowClient());
            var sourceMetadata = (org.apache.bookkeeper.mledger.impl.MetaStoreImpl) sourceFactory.getMetaStore();
            binding.claimFirstNativeFactory(sourceCreation, sourceMetadata)
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS);
            var retention = binding.prepareNativeRetention(sourceCreation, sourceMetadata, nativeConfig)
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS);
            binding.configureCreate(retention.config());
            var ledger = (ManagedLedgerImpl) sourceFactory.open(binding.managedLedgerName(), retention.config());
            retention.installed(ledger).toCompletableFuture().get(30, TimeUnit.SECONDS);
            var cursor = ledger.openCursor(SUBSCRIPTION, InitialPosition.Earliest);
            var positions = new java.util.ArrayList<org.apache.bookkeeper.mledger.Position>();
            for (int index = 0; index < 7; index++) {
                positions.add(ledger.addEntry(payload(index)));
            }
            long oldLedger = positions.get(0).getLedgerId();
            var target = new PhysicalResourceIdV2.BookKeeperLedger(
                    originalScope.namespaceBinding().physicalNamespace(), oldLedger);
            ledger.offloadPrefix(positions.get(3));
            var offloaded = ledger.getLedgersInfo().get(oldLedger).getOffloadContext();
            assertThat(offloaded.isComplete()).isTrue();
            assertThat(offloaded.getBookkeeperDeleteState())
                    .isEqualTo(org.apache.bookkeeper.mledger.proto.BookKeeperDeleteState.BK_DELETE_NONE);
            assertThat(cursor.getNumberOfEntriesInBacklog(false)).isEqualTo(7);
            sourceFactory.shutdown();
            sourceFactory = null;
            sourceCreation.close();
            sourceCreation = null;
            assertThat(com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                    exact.read(key)
                                            .toCompletableFuture()
                                            .get(30, TimeUnit.SECONDS)
                                            .orElseThrow()
                                            .canonicalStoredBytes())
                            .firstNativeFactoryScope())
                    .contains(originalScope.sha256());
            factory = PulsarNativeManagedLedgerFactoryV2.open(
                            binding,
                            metadata,
                            uri,
                            bootstrap,
                            budget,
                            NATIVE_BK_CANONICAL_CAPACITY,
                            factoryConfig,
                            registry,
                            authority,
                            key,
                            offloader,
                            objects,
                            8_388_608,
                            worker,
                            gcPolicy)
                    .get(60, TimeUnit.SECONDS);
            ledger = factory.openLedger(nativeConfig);
            assertThat(ledger.openCursor(SUBSCRIPTION, InitialPosition.Earliest).getNumberOfEntriesInBacklog(false))
                    .isEqualTo(7);
            assertThat(((com.nereusstream.storage.bookkeeper.M5PulsarNativeMetadataDriverV1)
                                    factory.namespace().borrowClient().getMetadataClientDriver())
                            .createSpec()
                            .sha256())
                    .isNotEqualTo(originalScope.sha256());
            System.out.println("NATIVE_PULSAR_BK_SOURCE_PREPARATION originalV3Birth=true originalQuota=true"
                    + " offloadCompleteBeforeM5MetadataWriter=true originalCreatorDrained=true"
                    + " originalMissingParentClaim=true oldScopeNotRetagged=true");
            var capsule = PulsarNativeM4DrainAdmissionV1.load(
                    Path.of(System.getProperty(
                            "nereus.m5.nativeBkGcAdmissionFile",
                            System.getProperty("nereus.m5.nativeM4AdmissionFile"))),
                    Sha256Digest.copyOf(java.util.HexFormat.of()
                            .parseHex(System.getProperty(
                                    "nereus.m5.nativeBkGcAdmissionSha256",
                                    System.getProperty("nereus.m5.nativeM4AdmissionSha256")))));
            unregisteredView = preparedView(binding, ledger, factory.namespace(), offloader, objects, budget, false);
            var view = unregisteredView;
            initialControl
                    .installControlWriter(registry, authority, key, budget)
                    .get(30, TimeUnit.SECONDS);
            owner = initialControl
                    .createReadOwner(view, capsule, factory.namespace(), offloader, worker, event, 2, budget)
                    .get(30, TimeUnit.SECONDS);
            unregisteredView = null; // The actual owner now owns the original preparation and its private drain.
            // Retire the actual Native fallback before freezing the full NMS1 source cut. Its source identities
            // remain bound to the original immutable map and must not be substituted with later slot identities.
            owner.drainForLogicalTrim(worker).get(30, TimeUnit.SECONDS);
            factory.closeAsync().get(60, TimeUnit.SECONDS);
            factory = null;
            factory = PulsarNativeManagedLedgerFactoryV2.open(
                            binding,
                            metadata,
                            uri,
                            bootstrap,
                            budget,
                            NATIVE_BK_CANONICAL_CAPACITY,
                            factoryConfig,
                            registry,
                            authority,
                            key,
                            offloader,
                            objects,
                            8_388_608,
                            worker,
                            gcPolicy)
                    .get(60, TimeUnit.SECONDS);
            ledger = factory.openLedger(nativeConfig);
            unregisteredView = preparedView(binding, ledger, factory.namespace(), offloader, objects, budget, true);
            var released = initialControl
                    .retireFallback(owner, unregisteredView, factory.namespace(), offloader, worker)
                    .get(60, TimeUnit.SECONDS);
            owner = released.successorOwner();
            view = unregisteredView;
            unregisteredView = null;
            assertThat(released.selector().mode()).isEqualTo(SelectorMode.PREFERRED_ONLY);
            var ranges = view.materializationChain().stream()
                    .filter(row -> !row.active() && row.entryEndExclusive() > 0)
                    .map(row -> new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.EntryRange(
                            row.ledgerId(), 0, row.entryEndExclusive()))
                    .toList();
            source = PulsarNativeMaterializationSourceV2.capture(owner, factory.namespace(), offloader, ranges, worker)
                    .get(60, TimeUnit.SECONDS);
            var objectConnection =
                    source.qualifyProvider(64L * 1024 * 1024, worker).get(60, TimeUnit.SECONDS);
            var task = source.prepareTask(
                            true,
                            List.of(
                                    com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind
                                            .OFFSET_OR_POSITION,
                                    com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind
                                            .CHECKSUM_COVERAGE),
                            new com.nereusstream.storage.object.materialization.M5MaterializationAdmissionV1.Caps(
                                    4, 1L << 30, 1L << 30, 1024, 1024, 128, 128),
                            worker)
                    .get(60, TimeUnit.SECONDS);
            task.writeAndVerify(staging, 64L * 1024 * 1024, worker).get(60, TimeUnit.SECONDS);
            task.publish(worker).get(60, TimeUnit.SECONDS);
            source.closeAsync().get(60, TimeUnit.SECONDS);
            source = null;
            factory.closeAsync().get(60, TimeUnit.SECONDS);
            factory = null;
            initialControl.closeControlWriter().get(30, TimeUnit.SECONDS);
            var control = binding.m4Control();
            activeControl = control;
            control.installControlWriter(registry, authority, key, budget).get(30, TimeUnit.SECONDS);
            factory =
                    guardedMappedFactory(binding, metadata, bootstrap, offloader, budget, worker, responses, gcPolicy);
            ledger = factory.openLedger(nativeConfig);
            prepared = control.prepareMaterializedReadSources(
                            ledger, factory.namespace(), offloader, objects, budget, worker)
                    .get(60, TimeUnit.SECONDS);
            var preferred = prepared.view().get(60, TimeUnit.SECONDS);
            assertThat(preferred
                            .materializedMap()
                            .fallbackPhysicalReferences(
                                    binding, preferred.bookKeeperNamespaceBinding(), preferred.objectBinding()))
                    .doesNotContain(target);
            var currentOwner = control.prepareMaterializedReadOwner(
                            prepared, capsule, factory.namespace(), offloader, worker)
                    .get(60, TimeUnit.SECONDS);
            owner = currentOwner.recover(worker, event, 2, budget).get(60, TimeUnit.SECONDS);
            assertThat(owner.selector().mode()).isEqualTo(SelectorMode.PREFERRED_WITH_FALLBACK);
            assertThat(owner.fallbackSources()).isNotEmpty();
            try (var nativeMetadata =
                    bootstrap.borrowClient().getLedgerManagerFactory().newLedgerManager()) {
                assertThat(nativeMetadata
                                .readLedgerMetadata(oldLedger)
                                .get(30, TimeUnit.SECONDS)
                                .getValue()
                                .isClosed())
                        .isTrue();
            }
            released.prepareBookKeeperDeleteIntent(target, owner, worker).get(60, TimeUnit.SECONDS);
            assertThat(ledger.getLedgersInfo()
                            .get(oldLedger)
                            .getOffloadContext()
                            .getBookkeeperDeleteState())
                    .isEqualTo(org.apache.bookkeeper.mledger.proto.BookKeeperDeleteState.BK_DELETE_INTENT);
            owner = null;
            prepared.close().get(30, TimeUnit.SECONDS);
            prepared = null;
            factory.closeAsync().get(60, TimeUnit.SECONDS);
            factory = null;
            control.closeControlWriter().get(30, TimeUnit.SECONDS);
            if (expiry) {
                nativeConfig.setRetentionTime(0, TimeUnit.MILLISECONDS).setRetentionSizeInMB(0);
                assertThat(binding.updateNativeRetention(nativeConfig)
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS))
                        .isEqualTo(
                                com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1
                                        .Outcome.APPLIED_EXACT);
                factory = guardedMappedFactory(
                        binding, metadata, bootstrap, offloader, budget, worker, responses, gcPolicy);
                var active = factory.openLedger(nativeConfig);
                var subscribed = active.openCursor(SUBSCRIPTION, InitialPosition.Earliest);
                subscribed.markDelete(positions.get(2));
                // Persist the SDK's equivalent next-ledger boundary before the later exact cold capture.
                active.maybeUpdateCursorBeforeTrimmingConsumedLedger().get(30, TimeUnit.SECONDS);
                assertThat(subscribed.getMarkDeletedPosition())
                        .isEqualTo(PositionFactory.create(positions.get(3).getLedgerId(), -1));
                assertThat(subscribed.getNumberOfEntriesInBacklog(false)).isEqualTo(4);
                factory.closeAsync().get(60, TimeUnit.SECONDS);
                factory = null;
            }
            try (var observed = M5BookKeeperNamespaceAuthorityV2.observePulsarScope(
                            uri, bootstrap.capability(), originalScope);
                    var faults = new com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteFaultFixtureV2(
                            uri, bootstrap.capability(), originalScope)) {
                var bkLayout = com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Layout.withPhysicalMembers(
                        originalScope.namespaceBinding().authorityRoot(), target.namespace());
                String deleteKey = bkLayout.nativeKey(target.authorityKey());
                var quotaBefore = observeActualQuota("bk-gc-before", bkLayout, target);
                if (expiry) {
                    commitNativeBookKeeperTrim(
                            binding,
                            control,
                            observed,
                            offloader,
                            objectConnection.physical(),
                            metadata,
                            budget,
                            registry,
                            responses,
                            nativeConfig,
                            target,
                            oldLedger,
                            worker);
                }
                var trim = binding.logicalTrimCoordinator();
                var attempt = Sha256Digest.hash(
                        CanonicalUtf8.fromString("native-bk-gc-" + expiry).bytes());
                var scan = trim.beginSourceScan(
                                exact.read(key)
                                        .toCompletableFuture()
                                        .get(30, TimeUnit.SECONDS)
                                        .orElseThrow(),
                                attempt)
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS)
                        .exactAuthority()
                        .orElseThrow();
                try (var cursors = PulsarNativeCursorFloorScanV3.capture(
                                        binding, metadata, observed, scan, budget, nativeConfig, worker)
                                .get(30, TimeUnit.SECONDS);
                        var tasks = control.captureMaterializationTaskRoots(
                                        scan, budget, worker, bootstrap, objectConnection.physical())
                                .get(30, TimeUnit.SECONDS);
                        var gc = PulsarNativeGcScopeV1.openBookKeeper(
                                        binding,
                                        target,
                                        budget,
                                        cursors,
                                        scan,
                                        observed,
                                        worker,
                                        Optional.of(faults.pulsarDeleteAuthority(target)))
                                .get(30, TimeUnit.SECONDS)) {
                    if (expiry) {
                        var frontier = com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1
                                .decodeAuthority(scan.canonicalStoredBytes())
                                .logicalTrim()
                                .orElseThrow()
                                .frontier()
                                .orElseThrow();
                        assertThat(frontier.pulsarContext().orElseThrow().coverage())
                                .isEqualTo(cursors.coverage());
                        assertThat(frontier.storageFence().metadataVersion()).isNotEqualTo(scan.metadataVersion());
                    }
                    gc.mark(control, tasks, registry, worker).get(30, TimeUnit.SECONDS);
                    Thread.sleep(30); // The actual explicitly admitted monotonic grace is 25 ms.
                    assertThat(gc.requireElapsed().get(30, TimeUnit.SECONDS)).isNotNull();
                    var reason = expiry
                            ? com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.ReclamationReason
                                    .LOGICAL_EXPIRY
                            : com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.ReclamationReason
                                    .REPLACED_REPRESENTATION;
                    faults.dropNextDeleteCallback();
                    assertThatThrownBy(
                                    () -> gc.reclaimBookKeeperAfterElapsed(control, tasks, registry, released, reason)
                                            .get(60, TimeUnit.SECONDS))
                            .hasRootCauseMessage("Original Native BK delete remains unknown or retained");
                    var retained = observed.nativeDeleteCellBudget()
                            .snapshot()
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS);
                    assertThat(retained.reservations()).hasSize(1).allSatisfy(hold -> assertThat(hold.terminalUnknown())
                            .isTrue());
                    var intent = exact.read(deleteKey)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow();
                    assertThat(com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                                            intent.canonicalStoredBytes())
                                    .state())
                            .isEqualTo(
                                    com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                            .TargetDeleteAuthorityStateV1.DELETE_INTENT_V1);
                    assertThatThrownBy(
                                    () -> gc.reclaimBookKeeperAfterElapsed(control, tasks, registry, released, reason)
                                            .get(60, TimeUnit.SECONDS))
                            .hasRootCauseMessage("Original Native BK delete remains unknown or retained");
                    assertThat(exact.read(deleteKey).toCompletableFuture().get(30, TimeUnit.SECONDS))
                            .contains(intent);
                    faults.deliverDroppedDelete().toCompletableFuture().get(30, TimeUnit.SECONDS);
                    var compact = gc.reclaimBookKeeperAfterElapsed(control, tasks, registry, released, reason)
                            .get(60, TimeUnit.SECONDS);
                    assertThat(com.nereusstream.storage.object.gc.M5TargetDeleteStoredValueV2.decode(compact)
                                    .compactDone())
                            .isPresent();
                    var quotaAfter = observeActualQuota("bk-gc-after", bkLayout, target);
                    assertThat(quotaAfter.head().capacityBytes())
                            .isEqualTo(quotaBefore.head().capacityBytes());
                    assertThat(quotaAfter.head().reservedResources())
                            .isEqualTo(quotaBefore.head().reservedResources() - 1);
                    assertThat(quotaAfter.head().settledResources())
                            .isEqualTo(quotaBefore.head().settledResources() + 1);
                    assertThat(observed.nativeDeleteCellBudget()
                                    .snapshot()
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS)
                                    .reservations())
                            .isEmpty();
                    try (var nativeMetadata =
                            bootstrap.borrowClient().getLedgerManagerFactory().newLedgerManager()) {
                        assertThatThrownBy(() -> nativeMetadata
                                        .readLedgerMetadata(oldLedger)
                                        .get(30, TimeUnit.SECONDS))
                                .hasCauseInstanceOf(
                                        org.apache.bookkeeper.client.BKException
                                                .BKNoSuchLedgerExistsOnMetadataServerException.class);
                    }
                } finally {
                    trim.abortSourceScan(scan, attempt).toCompletableFuture().get(30, TimeUnit.SECONDS);
                }
            }
            var finalControl = binding.m4Control();
            activeControl = finalControl;
            finalControl.installControlWriter(registry, authority, key, budget).get(30, TimeUnit.SECONDS);
            factory =
                    guardedMappedFactory(binding, metadata, bootstrap, offloader, budget, worker, responses, gcPolicy);
            var remaining = factory.openLedger(nativeConfig);
            prepared = finalControl
                    .prepareMaterializedReadSources(remaining, factory.namespace(), offloader, objects, budget, worker)
                    .get(60, TimeUnit.SECONDS);
            var recovered = finalControl
                    .prepareMaterializedReadOwner(prepared, capsule, factory.namespace(), offloader, worker)
                    .get(60, TimeUnit.SECONDS);
            owner = recovered.recover(worker, event, 2, budget).get(60, TimeUnit.SECONDS);
            var subscription = remaining.openCursor(SUBSCRIPTION, InitialPosition.Earliest);
            int ordinal = expiry ? 3 : 0;
            while (ordinal < 7) {
                var entries = subscription.readEntries(7 - ordinal);
                assertThat(entries).isNotEmpty();
                try {
                    for (var entry : entries) {
                        assertThat(entry.getData()).isEqualTo(payload(ordinal++));
                    }
                } finally {
                    entries.forEach(Entry::release);
                }
            }
            var doneView = remaining.captureNativeReadView().get(30, TimeUnit.SECONDS);
            var done = doneView.captureBookKeeperDeletion(oldLedger);
            owner.drainForRecovery(worker).get(30, TimeUnit.SECONDS);
            owner = null;
            doneView.fenceOwnerReadAdmission().get(30, TimeUnit.SECONDS);
            done.completeDone().get(30, TimeUnit.SECONDS);
            assertThat(remaining
                            .getLedgersInfo()
                            .get(oldLedger)
                            .getOffloadContext()
                            .getBookkeeperDeleteState())
                    .isEqualTo(org.apache.bookkeeper.mledger.proto.BookKeeperDeleteState.BK_DELETE_DONE);
            prepared.close().get(30, TimeUnit.SECONDS);
            prepared = null;
            factory.closeAsync().get(60, TimeUnit.SECONDS);
            factory = null;
            finalControl.closeControlWriter().get(30, TimeUnit.SECONDS);
            if (expiry) {
                // Opening the actual Native factory and recording DONE advanced the real parent version.
                // A fresh scan may not reuse the prior directory/frontier after that actual change.
                try (var observed = M5BookKeeperNamespaceAuthorityV2.observePulsarScope(
                        uri, bootstrap.capability(), originalScope)) {
                    assertThatThrownBy(() -> commitNativeBookKeeperTrim(
                                    binding,
                                    control,
                                    observed,
                                    offloader,
                                    objectConnection.physical(),
                                    metadata,
                                    budget,
                                    registry,
                                    responses,
                                    nativeConfig,
                                    new PhysicalResourceIdV2.BookKeeperLedger(
                                            target.namespace(), positions.get(3).getLedgerId()),
                                    oldLedger,
                                    worker))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("typed trim predecessor changes Binding/policy/chain/frontier");
                }
            }
            System.out.println(
                    "NATIVE_PULSAR_BK_LIFECYCLE reason=" + (expiry ? "LOGICAL_EXPIRY" : "REPLACED_REPRESENTATION")
                            + " actualOffload=true exactM4Released=true actualNativeIntent=true"
                            + " actualBoundBkDelete=true originalUnknownReconciled=true compactDone=true"
                            + " nativeDone=true activeBacklogReadable=true wholeM5Authority=false");
        } catch (Exception | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            try {
                responses.clear();
                if (source != null) {
                    source.closeAsync().get(60, TimeUnit.SECONDS);
                }
                if (owner != null) {
                    owner.fence().get(30, TimeUnit.SECONDS);
                }
                if (unregisteredView != null) {
                    unregisteredView.closeUnregisteredPreparation().get(30, TimeUnit.SECONDS);
                }
                if (prepared != null) {
                    prepared.close().get(30, TimeUnit.SECONDS);
                }
                if (factory != null) {
                    factory.closeAsync().get(60, TimeUnit.SECONDS);
                }
                if (sourceFactory != null) {
                    sourceFactory.shutdown();
                }
                if (sourceCreation != null) {
                    sourceCreation.close();
                }
                initialControl.closeControlWriter().get(30, TimeUnit.SECONDS);
                if (activeControl != null) {
                    activeControl.closeControlWriter().get(30, TimeUnit.SECONDS);
                }
                budget.closeCell().get(30, TimeUnit.SECONDS);
            } catch (Exception | Error cleanup) {
                if (primary == null) {
                    primary = cleanup;
                    throw cleanup;
                }
                primary.addSuppressed(cleanup);
            } finally {
                worker.shutdown();
                event.shutdown();
                boolean workerStopped = worker.awaitTermination(30, TimeUnit.SECONDS);
                boolean eventStopped = event.awaitTermination(30, TimeUnit.SECONDS);
                if (!workerStopped || !eventStopped) {
                    var cleanup = new AssertionError("Native BK fixture executors did not drain");
                    if (primary == null) {
                        throw cleanup;
                    }
                    primary.addSuppressed(cleanup);
                }
                try (var paths = Files.walk(staging)) {
                    for (var path :
                            paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                        Files.delete(path);
                    }
                }
            }
        }
    }

    private static void commitNativeBookKeeperTrim(
            PulsarNativeBindingV1 binding,
            PulsarNativeM4ControlV1 control,
            M5BookKeeperNamespaceAuthorityV2 observed,
            NereusPulsarLedgerOffloaderV1 offloader,
            com.nereusstream.storage.api.lifecycle.NativePhysicalNamespaceAuthorityV2 objectBackend,
            MetadataStoreExtended metadata,
            PulsarNativeReadCellBudgetV1 budget,
            com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1 registry,
            SelectorResponseFault responses,
            ManagedLedgerConfig config,
            PhysicalResourceIdV2.BookKeeperLedger target,
            long expiredLedgerId,
            java.util.concurrent.Executor worker)
            throws Exception {
        var exact = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(responses);
        String key = new OxiaV2AuthorityKeys(binding.authorityRoot())
                .aggregateKey(binding.authority().aggregate().binding().incarnationIdentity());
        var trim = binding.logicalTrimCoordinator();
        var attempt = Sha256Digest.hash(
                CanonicalUtf8.fromString("native-bk-actual-expiry-trim").bytes());
        var scan = trim.beginSourceScan(
                        exact.read(key)
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS)
                                .orElseThrow(),
                        attempt)
                .toCompletableFuture()
                .get(30, TimeUnit.SECONDS)
                .exactAuthority()
                .orElseThrow();
        boolean sealed = false;
        try {
            com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorSnapshotV1 snapshot;
            try (var cursors = PulsarNativeCursorFloorScanV3.capture(
                                    binding, metadata, observed, scan, budget, config, worker)
                            .get(30, TimeUnit.SECONDS);
                    var tasks = control.captureMaterializationTaskRoots(scan, budget, worker, observed, objectBackend)
                            .get(30, TimeUnit.SECONDS)) {
                var route = binding.materializationQuota(observed).get(30, TimeUnit.SECONDS);
                var floors = control.captureRetentionFloors(
                                scan, cursors, tasks, registry, route.members(), target, budget, worker)
                        .get(30, TimeUnit.SECONDS);
                var references = control.capturePhysicalReferences(
                                scan, cursors, tasks, registry, route.members(), target, budget, worker)
                        .get(30, TimeUnit.SECONDS);
                var floorAdapters = new java.util.EnumMap<
                        com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1,
                        com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.FloorAdapterV1>(
                        com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1.class);
                floors.forEach(value ->
                        floorAdapters.put(value.floorClass(), ignored -> CompletableFuture.completedFuture(value)));
                var taskFloor = tasks.closedTaskFloor(registry, target, cursors);
                floorAdapters.put(taskFloor.floorClass(), ignored -> CompletableFuture.completedFuture(taskFloor));
                var referenceAdapters = new java.util.EnumMap<
                        com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1,
                        com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.ReferenceAdapterV1>(
                        com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1.class);
                references.forEach(value -> referenceAdapters.put(
                        value.referenceKind(), ignored -> CompletableFuture.completedFuture(value)));
                var source =
                        com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                scan.canonicalStoredBytes());
                var configured = source.nativeRetention().orElseThrow();
                var policy = Sha256Digest.hash(CanonicalBytes.copyOf(
                        java.nio.ByteBuffer.allocate(36 + configured.encode().length())
                                .putInt(0x4e525031)
                                .put(com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1
                                        .nativeOnlyPolicyRoot(source)
                                        .bytes()
                                        .toByteArray())
                                .put(configured.encode().toByteArray())
                                .array()));
                var selected = control.readSelectedMaterialization(worker).get(30, TimeUnit.SECONDS);
                var materializationIdentity =
                        selected.readMap().plan().sourceCut().identity();
                var floorIdentity =
                        new com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IdentityEnvelope(
                                materializationIdentity.protocolCellSha256(),
                                materializationIdentity.providerScopeSha256(),
                                materializationIdentity.binding(),
                                materializationIdentity.ownerEpoch(),
                                materializationIdentity.workerEpoch(),
                                materializationIdentity.storageFence(),
                                source.capability());
                var request =
                        new com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1
                                .FloorSnapshotRequestV1(
                                floorIdentity,
                                PositionDomain.PULSAR_ENTRY,
                                Math.addExact(source.authorityGeneration(), 1),
                                -1,
                                policy,
                                new com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1(
                                        selected.selectorFact().key(),
                                        selected.selectorFact().metadataVersion(),
                                        selected.selectorFact().canonicalStoredSha256()),
                                new com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1(
                                        key, scan.metadataVersion(), scan.canonicalStoredSha256()),
                                Optional.of(
                                        new com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                                .PulsarFloorContextV2(
                                                cursors.coverage(),
                                                source.logicalTrim()
                                                        .flatMap(value -> value.frontier())
                                                        .map(value -> value.pulsarContext()
                                                                .orElseThrow()
                                                                .newFrontier())
                                                        .orElse(new com.nereusstream.storage.object.materialization
                                                                .M5PulsarCoverageV2.Position(
                                                                cursors.coverage()
                                                                        .chain()
                                                                        .get(0)
                                                                        .ledgerId(),
                                                                0)))));
                snapshot = new com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1(
                                exact, floorAdapters, referenceAdapters)
                        .buildFloorSnapshot(request)
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS);
                assertThat(snapshot.minimumPulsarFloor())
                        .isEqualTo(cursors.coverage()
                                .firstAtOrAfter(
                                        new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                                                expiredLedgerId, 3))
                                .orElseThrow());
                var fixed = trim.sealSourceScan(scan, snapshot, attempt)
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS)
                        .exactAuthority()
                        .orElseThrow();
                sealed = true;
                assertThat(trim.commitStoredSourceScan(fixed, attempt)
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS)
                                .exactAuthority())
                        .isPresent();
            }
        } finally {
            if (!sealed) {
                trim.abortSourceScan(scan, attempt).toCompletableFuture().get(30, TimeUnit.SECONDS);
            }
        }
    }

    private static void verifyNativeOnlyFirstAdmission(
            PulsarNativeBindingV1 binding,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            NereusPulsarLedgerOffloaderV1 offloader,
            PhysicalResourceIdV2.Namespace objects,
            SelectorResponseFault responses,
            MetadataStoreExtended metadata,
            boolean taskClosure)
            throws Exception {
        var worker = Executors.newSingleThreadExecutor();
        var budget = readBudget(binding, 6);
        var exact = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(responses);
        var authority = new com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1(exact);
        var capability = new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding(
                1, bootstrap.capability().configurationDigest());
        var registry = nativeExpectedWriterRegistry(capability);
        String key = new OxiaV2AuthorityKeys(binding.authorityRoot())
                .aggregateKey(binding.authority().aggregate().binding().incarnationIdentity());
        var nativeConfig = config(offloader).setMaxEntriesPerLedger(3);
        var factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setMaxCacheSize(0);
        PulsarNativeManagedLedgerFactoryV2 original = null;
        PulsarNativeManagedLedgerFactoryV2 reopened = null;
        try (var policyClient = OxiaClientBuilder.create(System.getProperty("nereus.m5.nativeOffloadOxia"))
                .namespace("default")
                .requestTimeout(Duration.ofSeconds(10))
                .asyncClient()
                .get(30, TimeUnit.SECONDS)) {
            original = PulsarNativeManagedLedgerFactoryV2.open(
                            binding,
                            metadata,
                            "zk://" + System.getProperty("nereus.m5.nativeOffloadZk") + "/ledgers",
                            bootstrap,
                            budget,
                            NATIVE_BK_CANONICAL_CAPACITY,
                            factoryConfig,
                            registry,
                            authority,
                            key,
                            offloader,
                            objects,
                            8_388_608,
                            worker)
                    .get(60, TimeUnit.SECONDS);
            var ledger = original.openLedger(nativeConfig);
            var installedPolicy =
                    com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                    exact.read(key)
                                            .toCompletableFuture()
                                            .get(30, TimeUnit.SECONDS)
                                            .orElseThrow()
                                            .canonicalStoredBytes())
                            .nativeRetention()
                            .orElseThrow();
            assertThat(installedPolicy.generation()).isEqualTo(1);
            assertThat(installedPolicy.installedByFactoryScope()).isPresent();
            assertThat(installedPolicy.sameValues(
                            nativeConfig.getRetentionTimeMillis(), nativeConfig.getRetentionSizeInMB()))
                    .isTrue();
            assertThat(ledger.getConfig()).isNotSameAs(nativeConfig);
            assertThatThrownBy(() -> ledger.getConfig().setRetentionTime(0, TimeUnit.MILLISECONDS))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(binding.updateNativeRetention(config(offloader).setRetentionTime(2, TimeUnit.HOURS))
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS))
                    .isEqualTo(
                            com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1.Outcome
                                    .RETAIN);
            var cursor = ledger.openCursor(SUBSCRIPTION, InitialPosition.Earliest);
            for (int entry = 0; entry < 7; entry++) {
                ledger.addEntry(payload(entry));
            }
            assertThat(cursor.getNumberOfEntriesInBacklog(false)).isEqualTo(7);
            original.closeAsync().get(60, TimeUnit.SECONDS);
            var coldResponses = new SelectorResponseFault(policyClient);
            var cold = PulsarNativeBindingV1.read(
                            coldResponses,
                            binding.metadataNamespace(),
                            binding.authorityRoot(),
                            (PulsarTopicIncarnationIdentity)
                                    binding.authority().aggregate().binding().incarnationIdentity())
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS);
            try (var coldMetadata = metadata()) {
                reopened = PulsarNativeManagedLedgerFactoryV2.open(
                                cold,
                                coldMetadata,
                                "zk://" + System.getProperty("nereus.m5.nativeOffloadZk") + "/ledgers",
                                bootstrap,
                                budget,
                                NATIVE_BK_CANONICAL_CAPACITY,
                                factoryConfig,
                                registry,
                                authority,
                                key,
                                offloader,
                                objects,
                                8_388_608,
                                worker)
                        .get(60, TimeUnit.SECONDS);
                var coldLedger = reopened.openLedger(config(offloader).setMaxEntriesPerLedger(3));
                var coldCursor = coldLedger.openCursor(SUBSCRIPTION, InitialPosition.Earliest);
                int ordinal = 0;
                while (ordinal < 7) {
                    var entries = coldCursor.readEntries(7 - ordinal);
                    assertThat(entries).isNotEmpty();
                    try {
                        for (Entry entry : entries) {
                            assertThat(entry.getData()).isEqualTo(payload(ordinal++));
                        }
                    } finally {
                        entries.forEach(Entry::release);
                    }
                }
                assertThat(ordinal).isEqualTo(7);
                if (taskClosure) {
                    Throwable primary = null;
                    try {
                        verifyNativeOnlyTaskClosure(
                                cold,
                                reopened,
                                coldLedger,
                                bootstrap,
                                offloader,
                                objects,
                                coldResponses,
                                budget,
                                registry,
                                authority,
                                key,
                                worker,
                                coldMetadata);
                    } catch (Exception | Error failure) {
                        primary = failure;
                        throw failure;
                    } finally {
                        // Factory owns native cursor writes, so it must close before its metadata/policy clients.
                        var originalFactory = reopened;
                        reopened = null;
                        try {
                            originalFactory.closeAsync().get(60, TimeUnit.SECONDS);
                        } catch (Exception | Error cleanup) {
                            if (primary == null) {
                                throw cleanup;
                            }
                            primary.addSuppressed(cleanup);
                        }
                    }
                    return;
                }
                reopened.closeAsync().get(60, TimeUnit.SECONDS);
            }
            var open = exact.read(key)
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            var value = com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                    open.canonicalStoredBytes());
            assertThat(value.retentionPolicy())
                    .contains(
                            com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityRecordsV1
                                    .PulsarRetentionPolicyV1.NATIVE_PULSAR_ONLY_V1);
            var guard = com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1.forPulsarAggregate(
                    registry, authority, key, value.originalAggregateSha256());
            for (var kind : List.of(
                    com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                            .KAFKA_GROUP_RETENTION,
                    com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                            .KAFKA_PRODUCER_RECOVERY,
                    com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                            .KAFKA_TRANSACTION_OR_ABORTED_RECOVERY,
                    com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                            .KAFKA_REPLICA_OR_LEADER_EPOCH_RECOVERY)) {
                var request =
                        new com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1
                                .GuardedMutationRequestV1(
                                registry.writerFor(kind),
                                com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceTargetKindV1
                                        .PULSAR_AGGREGATE,
                                value.originalAggregateSha256(),
                                Sha256Digest.hash(CanonicalUtf8.fromString("forbidden-native-only-" + kind)
                                        .bytes()),
                                Sha256Digest.hash(CanonicalUtf8.fromString("actual-p1-kafka-writer-" + kind)
                                        .bytes()));
                var rejected = guard.execute(request, () -> {
                            throw new AssertionError(
                                    "Native-only forbidden protocol writer reached external SDK dispatch");
                        })
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS);
                assertThat(rejected.externalMutationDispatched()).isFalse();
            }
            open = exact.read(key)
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            var coordinator = cold.logicalTrimCoordinator();
            var attempt = Sha256Digest.hash(
                    CanonicalUtf8.fromString("native-only-policy-floors").bytes());
            var scan = coordinator
                    .beginSourceScan(open, attempt)
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .exactAuthority()
                    .orElseThrow();
            try {
                var rows = cold.captureNativeOnlyKafkaFloors(registry, scan)
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS);
                assertThat(rows.rows()).hasSize(3).allSatisfy(row -> {
                    assertThat(row.constraining()).isFalse();
                    assertThat(row.safeFloor()).isEqualTo(-1);
                    assertThat(row.pulsarSafeFloor()).isEmpty();
                    assertThat(row.authority().metadataVersion()).isEqualTo(scan.metadataVersion());
                    assertThat(row.authority().valueSha256()).isEqualTo(scan.canonicalStoredSha256());
                });
                assertThatThrownBy(() -> cold.captureAccessProjectionFloor(registry, scan)
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS))
                        .hasRootCauseMessage("Native Binding has no first-admission Access Projection exclusion");
                assertThat(rows.policyRoot())
                        .isEqualTo(
                                com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1
                                        .nativeOnlyPolicyRoot(value));
                try (var cursors = PulsarNativeCursorFloorScanV3.capture(
                                cold, metadata, bootstrap, scan, budget, nativeConfig, worker)
                        .get(30, TimeUnit.SECONDS)) {
                    assertThat(cursors.floor().constraining()).isTrue();
                    assertThat(cursors.floor().pulsarSafeFloor()).isPresent();
                    assertThat(cursors.cursorCount()).isEqualTo(1);
                    assertThat(cursors.bindingRetentionFloor().constraining()).isTrue();
                    assertThat(cursors.bindingRetentionFloor().authority().metadataVersion())
                            .isEqualTo(scan.metadataVersion());
                    assertThat(cursors.bindingRetentionFloor().pulsarSafeFloor().orElseThrow())
                            .isEqualTo(new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                                    cursors.coverage().chain().get(0).ledgerId(), 0));
                    var references = cold.captureNativeOnlyKafkaAggregateReferences(registry, scan, cursors.coverage())
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS);
                    assertThat(references).hasSize(4).allSatisfy(result -> {
                        assertThat(result.observations()).singleElement().satisfies(row -> {
                            assertThat(row.targetIdentitySha256()).isEqualTo(value.originalAggregateSha256());
                            assertThat(row.authority().metadataVersion()).isEqualTo(scan.metadataVersion());
                            assertThat(row.disposition())
                                    .isEqualTo(
                                            com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                                    .ReferenceDispositionV1.ABSENT);
                            assertThat(row.enumerationComplete()).isTrue();
                        });
                    });
                    var directory = cursors.coverage();
                    var foreign = com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.of(
                            new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity(
                                    binding.bindingIdentity().bindingId(),
                                    Sha256Digest.hash(CanonicalUtf8.fromString("foreign-native-only-incarnation")
                                            .bytes()),
                                    binding.bindingIdentity().storageEpochSha256()),
                            directory.chainAuthorityRootSha256(),
                            directory.chain(),
                            directory.ranges());
                    assertThatThrownBy(() -> cold.captureNativeOnlyKafkaAggregateReferences(registry, scan, foreign)
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS))
                            .hasRootCauseInstanceOf(IllegalArgumentException.class);
                }
                var before = budget.usage();
                assertThat(exact.compareAndSet(Optional.of(scan), key, scan.canonicalStoredBytes())
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS))
                        .isEqualTo(
                                com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome
                                        .APPLIED_EXACT);
                assertThatThrownBy(() -> cold.captureNativeOnlyKafkaFloors(registry, scan)
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS))
                        .hasRootCauseInstanceOf(
                                com.nereusstream.storage.object.retention.M5ReferenceFreshnessVerifierV1
                                        .StaleAuthorityException.class);
                assertThat(budget.usage()).isEqualTo(before);
            } finally {
                coordinator
                        .abortSourceScan(
                                exact.read(key)
                                        .toCompletableFuture()
                                        .get(30, TimeUnit.SECONDS)
                                        .orElseThrow(),
                                attempt)
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS);
            }
            var updatedConfig = config(offloader).setMaxEntriesPerLedger(3).setRetentionTime(2, TimeUnit.HOURS);
            assertThat(cold.updateNativeRetention(updatedConfig)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS))
                    .isEqualTo(
                            com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1.Outcome
                                    .APPLIED_EXACT);
            var pendingPolicy =
                    com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                    exact.read(key)
                                            .toCompletableFuture()
                                            .get(30, TimeUnit.SECONDS)
                                            .orElseThrow()
                                            .canonicalStoredBytes())
                            .nativeRetention()
                            .orElseThrow();
            assertThat(pendingPolicy.generation()).isEqualTo(2);
            assertThat(pendingPolicy.installedByFactoryScope()).isEmpty();
            try (var updatedMetadata = metadata()) {
                reopened = PulsarNativeManagedLedgerFactoryV2.open(
                                cold,
                                updatedMetadata,
                                "zk://" + System.getProperty("nereus.m5.nativeOffloadZk") + "/ledgers",
                                bootstrap,
                                budget,
                                NATIVE_BK_CANONICAL_CAPACITY,
                                factoryConfig,
                                registry,
                                authority,
                                key,
                                offloader,
                                objects,
                                8_388_608,
                                worker)
                        .get(60, TimeUnit.SECONDS);
                var updatedFactory = reopened;
                assertThatThrownBy(() -> updatedFactory.openLedger(nativeConfig))
                        .isInstanceOf(java.util.concurrent.ExecutionException.class)
                        .hasRootCauseInstanceOf(IllegalArgumentException.class);
                var updatedLedger = reopened.openLedger(updatedConfig);
                assertThat(updatedLedger.getConfig().getRetentionTimeMillis()).isEqualTo(TimeUnit.HOURS.toMillis(2));
                assertThat(com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                        exact.read(key)
                                                .toCompletableFuture()
                                                .get(30, TimeUnit.SECONDS)
                                                .orElseThrow()
                                                .canonicalStoredBytes())
                                .nativeRetention()
                                .orElseThrow()
                                .installedByFactoryScope())
                        .isPresent();
                reopened.closeAsync().get(60, TimeUnit.SECONDS);
            }
            System.out.println(
                    "NATIVE_RETENTION_POLICY firstInstall=true frozenSdkConfig=true activeUpdateRejected=true"
                            + " drainedGenerationUpdate=true pendingUntilSdkInstall=true"
                            + " oldConfigRejected=true wholeM5Authority=false");
        } finally {
            if (reopened != null) {
                reopened.closeAsync().get(60, TimeUnit.SECONDS);
            }
            if (original != null) {
                original.closeAsync().get(60, TimeUnit.SECONDS);
            }
            worker.shutdown();
            assertThat(worker.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static void verifyNativeOnlyTaskClosure(
            PulsarNativeBindingV1 binding,
            PulsarNativeManagedLedgerFactoryV2 factory,
            ManagedLedgerImpl ledger,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            NereusPulsarLedgerOffloaderV1 offloader,
            PhysicalResourceIdV2.Namespace objects,
            SelectorResponseFault responses,
            PulsarNativeReadCellBudgetV1 budget,
            com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1 registry,
            com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1 authority,
            String key,
            java.util.concurrent.Executor worker,
            MetadataStoreExtended actualMetadata)
            throws Exception {
        var openPolicy = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(
                        responses)
                .read(key)
                .toCompletableFuture()
                .get(30, TimeUnit.SECONDS)
                .orElseThrow();
        var policy = com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                openPolicy.canonicalStoredBytes());
        assertThat(policy.retentionPolicy().orElseThrow().excludesAccessProjection())
                .isTrue();
        var referenceGuard = com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1.forPulsarAggregate(
                registry, authority, key, policy.originalAggregateSha256());
        var projectionKind = com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                .PROJECTION_MIGRATION_OR_EXPORT;
        var forbiddenProjection =
                new com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1.GuardedMutationRequestV1(
                        registry.writerFor(projectionKind),
                        com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceTargetKindV1
                                .PULSAR_AGGREGATE,
                        policy.originalAggregateSha256(),
                        Sha256Digest.hash(CanonicalUtf8.fromString("projection").bytes()),
                        Sha256Digest.hash(
                                CanonicalUtf8.fromString("projection-export").bytes()));
        assertThat(referenceGuard
                        .execute(forbiddenProjection, () -> {
                            throw new AssertionError("excluded Access Projection writer reached native dispatch");
                        })
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS)
                        .externalMutationDispatched())
                .isFalse();
        var control = binding.m4Control();
        var event = Executors.newSingleThreadExecutor();
        var staging = Files.createTempDirectory("nereus-native-only-task-closure-");
        PulsarNativeMaterializationSourceV2 source = null;
        PulsarNativeM4ReadOwnerV1 owner = null;
        var orphan = new AtomicReference<PhysicalResourceIdV2>();
        var exact = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(responses);
        try {
            var capsule = PulsarNativeM4DrainAdmissionV1.load(
                    Path.of(System.getProperty("nereus.m5.nativeM4AdmissionFile")),
                    Sha256Digest.copyOf(java.util.HexFormat.of()
                            .parseHex(System.getProperty("nereus.m5.nativeM4AdmissionSha256"))));
            var view = preparedView(binding, ledger, factory.namespace(), offloader, objects, budget, true);
            control.installControlWriter(registry, authority, key, budget).get(30, TimeUnit.SECONDS);
            owner = control.createReadOwner(view, capsule, factory.namespace(), offloader, worker, event, 2, budget)
                    .get(30, TimeUnit.SECONDS);
            var ranges = view.materializationChain().stream()
                    .filter(row -> !row.active() && row.entryEndExclusive() > 0)
                    .map(row -> new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.EntryRange(
                            row.ledgerId(), 0, row.entryEndExclusive()))
                    .toList();
            var caps = new com.nereusstream.storage.object.materialization.M5MaterializationAdmissionV1.Caps(
                    4, 1024L * 1024 * 1024, 1024L * 1024 * 1024, 1024, 1024, 128, 128);
            var indexes = List.of(
                    com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind
                            .OFFSET_OR_POSITION,
                    com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind
                            .CHECKSUM_COVERAGE);
            // The same original owner consumes distinct complete-plan epochs for no-dispatch and orphan terminals.
            for (int terminal = 0; terminal < 2; terminal++) {
                source = PulsarNativeMaterializationSourceV2.capture(
                                owner, factory.namespace(), offloader, ranges, worker)
                        .get(60, TimeUnit.SECONDS);
                var orphanConnection =
                        source.qualifyProvider(64L * 1024 * 1024, worker).get(60, TimeUnit.SECONDS);
                var unfinished = source.prepareTask(true, indexes, caps, worker).get(60, TimeUnit.SECONDS);
                if (terminal == 1) {
                    var orphanStaging = Files.createDirectory(staging.resolve("quarantined"));
                    var unfinishedPlan = unfinished.plan();
                    int lostBefore = responses.putLosses.get();
                    responses.armCandidate(bytes ->
                            bytes.length >= 8 && java.nio.ByteBuffer.wrap(bytes).getInt() == 0x4d35504d);
                    try {
                        assertThatThrownBy(() -> unfinished
                                        .writeAndVerify(orphanStaging, 64L * 1024 * 1024, worker)
                                        .get(60, TimeUnit.SECONDS))
                                .hasRootCauseMessage("actual selector observation unavailable");
                        assertThat(responses.putLosses.get()).isEqualTo(lostBefore + 1);
                        var retained = budget.usage();
                        var originalSource = source;
                        assertThatThrownBy(() -> originalSource.closeAsync().get(30, TimeUnit.SECONDS))
                                .hasRootCauseMessage("actual selector observation unavailable");
                        assertThat(budget.usage()).isEqualTo(retained);
                        assertThat(budget.durableUsage().get(30, TimeUnit.SECONDS))
                                .isEqualTo(retained);
                    } finally {
                        responses.clear();
                    }
                    source.closeAsync().get(60, TimeUnit.SECONDS);
                    var route = binding.materializationQuota(orphanConnection.physical())
                            .get(30, TimeUnit.SECONDS);
                    var physical = route.members()
                            .captureNamespace(1024, 16L * 1024 * 1024)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS);
                    var assigned = physical.births().stream()
                            .filter(birth -> birth.worker().taskIdSha256().equals(unfinishedPlan.taskIdSha256()))
                            .filter(birth -> birth.resource().isPresent())
                            .toList();
                    assertThat(assigned).hasSize(1);
                    orphan.set(assigned.get(0).resource().orElseThrow());
                    assertThat(route.members()
                                    .readFact(orphan.get())
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS)
                                    .inventory()
                                    .members())
                            .anySatisfy(member -> assertThat(member.binding()).isEqualTo(view.bindingIdentity()));
                } else {
                    source.closeAsync().get(60, TimeUnit.SECONDS);
                }
                source = null;
            }
            source = PulsarNativeMaterializationSourceV2.capture(owner, factory.namespace(), offloader, ranges, worker)
                    .get(60, TimeUnit.SECONDS);
            var connection = source.qualifyProvider(64L * 1024 * 1024, worker).get(60, TimeUnit.SECONDS);
            var task = source.prepareTask(true, indexes, caps, worker).get(60, TimeUnit.SECONDS);
            task.writeAndVerify(Files.createDirectory(staging.resolve("published")), 64L * 1024 * 1024, worker)
                    .get(60, TimeUnit.SECONDS);
            task.publish(worker).get(60, TimeUnit.SECONDS);
            var target = task.readMap().readableOutputs().get(0).resource().orElseThrow();
            var originalPlan = task.plan();
            var coverage = originalPlan.sourceCut().coverage().pulsarCoverage().orElseThrow();
            source.closeAsync().get(60, TimeUnit.SECONDS);
            source = null;
            var auditId = Sha256Digest.hash(
                    CanonicalUtf8.fromString("native-audit-obligation").bytes());
            var audit = binding.prepareAuditRegistration(
                            registry, connection.physical(), orphan.get(), coverage, auditId)
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS);
            int auditLosses = responses.putLosses.get();
            responses.armCandidate(bytes -> {
                var canonical = CanonicalBytes.copyOf(bytes);
                return com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.isAuthorityValue(
                                canonical)
                        && com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                        canonical)
                                .auditDomain()
                                .stream()
                                .flatMap(domain -> domain.holds().stream())
                                .anyMatch(audit.hold()::equals);
            });
            try {
                assertThat(audit.register().get(30, TimeUnit.SECONDS))
                        .isEqualTo(
                                com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1
                                        .Outcome.RESPONSE_UNKNOWN);
                assertThat(responses.putLosses.get()).isEqualTo(auditLosses + 1);
            } finally {
                responses.clear();
            }
            assertThat(audit.reconcile().get(30, TimeUnit.SECONDS))
                    .isEqualTo(
                            com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1.Outcome
                                    .APPLIED_EXACT);
            assertThat(audit.register().get(30, TimeUnit.SECONDS))
                    .isEqualTo(
                            com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1.Outcome
                                    .APPLIED_EXACT);
            control.closeControlWriter().get(30, TimeUnit.SECONDS);
            factory.closeAsync().get(60, TimeUnit.SECONDS);
            // Publication drained the predecessor. Qualify the new selected view with its own actual SDK actor.
            var selectedBinding = PulsarNativeBindingV1.read(
                            responses,
                            binding.metadataNamespace(),
                            binding.authorityRoot(),
                            (PulsarTopicIncarnationIdentity)
                                    binding.authority().aggregate().binding().incarnationIdentity())
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS);
            var selectedControl = selectedBinding.m4Control();
            selectedControl
                    .installControlWriter(registry, authority, key, budget)
                    .get(30, TimeUnit.SECONDS);
            var selectedFactory = guardedMappedFactory(
                    selectedBinding, actualMetadata, bootstrap, offloader, budget, worker, responses);
            PulsarNativeSourcePreparationV1 selectedSources = null;
            PulsarNativeM4ReadOwnerV1 selectedOwner = null;
            try {
                var selectedLedger =
                        selectedFactory.openLedger(config(offloader).setMaxUnackedRangesToPersistInMetadataStore(0));
                var selectedObjects = selectedControl
                        .readSelectedMaterialization(worker)
                        .get(30, TimeUnit.SECONDS)
                        .providerProfile()
                        .physicalBinding()
                        .physicalNamespace();
                selectedSources = selectedControl
                        .prepareMaterializedReadSources(
                                selectedLedger, selectedFactory.namespace(), offloader, selectedObjects, budget, worker)
                        .get(60, TimeUnit.SECONDS);
                var recovery = selectedControl
                        .prepareMaterializedReadOwner(
                                selectedSources, capsule, selectedFactory.namespace(), offloader, worker)
                        .get(60, TimeUnit.SECONDS);
                selectedOwner = recovery.recover(worker, event, 2, budget).get(60, TimeUnit.SECONDS);
                var first = originalPlan
                        .sourceCut()
                        .coverage()
                        .pulsarCoverage()
                        .orElseThrow()
                        .ranges()
                        .get(0);
                var entry = readOrdinaryEntry(
                                selectedLedger,
                                org.apache.bookkeeper.mledger.PositionFactory.create(
                                        first.ledgerId(), first.startEntryInclusive()))
                        .get(30, TimeUnit.SECONDS);
                try {
                    assertThat(entry.getData()).isEqualTo(payload(0));
                } finally {
                    entry.release();
                }
                selectedOwner.drainForLogicalTrim(worker).get(30, TimeUnit.SECONDS);
            } finally {
                if (selectedOwner != null) {
                    selectedOwner.fence().get(30, TimeUnit.SECONDS);
                }
                if (selectedSources != null) {
                    selectedSources.close().get(30, TimeUnit.SECONDS);
                }
                selectedFactory.closeAsync().get(60, TimeUnit.SECONDS);
                selectedControl.closeControlWriter().get(30, TimeUnit.SECONDS);
            }
            budget.closeCell().get(30, TimeUnit.SECONDS);
            var observed = readBudget(binding, 6);
            var trim = binding.logicalTrimCoordinator();
            var attempt = Sha256Digest.hash(
                    CanonicalUtf8.fromString("native-only-task-closure").bytes());
            var scan = trim.beginSourceScan(
                            exact.read(key)
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS)
                                    .orElseThrow(),
                            attempt)
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .exactAuthority()
                    .orElseThrow();
            try {
                var auditRoute =
                        binding.materializationQuota(connection.physical()).get(30, TimeUnit.SECONDS);
                var auditReferences = binding.captureAuditReferences(
                                registry, scan, auditRoute.members(), orphan.get(), coverage)
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS);
                assertThat(auditReferences.observations()).hasSize(2).allSatisfy(row -> assertThat(row.disposition())
                        .isEqualTo(
                                com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceDispositionV1
                                        .PRESENT));
                assertThat(authority
                                .clearAuditHold(key, scan, registry, audit.hold())
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS))
                        .isEqualTo(
                                com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1
                                        .Outcome.RETAIN);
                try (var roots = control.captureMaterializationTaskRoots(
                                scan, observed, worker, bootstrap, connection.physical())
                        .get(30, TimeUnit.SECONDS)) {
                    var states = roots.plans().stream()
                            .map(plan ->
                                    com.nereusstream.storage.object.materialization.M5MaterializationCodecV1.decodeTask(
                                                    roots.facts()
                                                            .get(control.materializationTaskKey(plan.taskIdSha256()))
                                                            .canonicalStoredBytes())
                                            .state())
                            .collect(java.util.stream.Collectors.toSet());
                    assertThat(states)
                            .containsExactlyInAnyOrder(
                                    com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.TaskState
                                            .CANCELLED_STALE,
                                    com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.TaskState
                                            .QUARANTINED,
                                    com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.TaskState
                                            .PUBLISHED);
                    var result = roots.closedTaskReferences(registry, target, coverage);
                    assertThat(result.summary().complete()).isTrue();
                    assertThat(result.observations()).isNotEmpty().allSatisfy(row -> {
                        assertThat(row.disposition())
                                .isEqualTo(
                                        com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                                .ReferenceDispositionV1.ABSENT);
                        assertThat(row.enumerationComplete()).isTrue();
                        assertThat(row.targetIdentitySha256()).isEqualTo(target.sha256());
                        assertThat(row.authority().key()).doesNotContain("/v2/physical-delete-m5-v2/");
                    });
                }
                var held = observed.reserveCursorObservation(binding, PulsarNativeCursorMetadataV2.MAX_BYTES);
                held.ready().get(30, TimeUnit.SECONDS);
                try (var roots = control.captureMaterializationTaskRoots(
                                scan, observed, worker, bootstrap, connection.physical())
                        .get(30, TimeUnit.SECONDS)) {
                    assertThatThrownBy(() -> roots.closedTaskReferences(registry, target, coverage))
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("actors, reservations");
                } finally {
                    held.releaseBeforeActor().get(30, TimeUnit.SECONDS);
                }
                var taskFact = exact.read(control.materializationTaskKey(originalPlan.taskIdSha256()))
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS)
                        .orElseThrow();
                var terminal = com.nereusstream.storage.object.materialization.M5MaterializationCodecV1.decodeTask(
                        taskFact.canonicalStoredBytes());
                var validationKey = control.materializationValidationKey(
                        terminal.validationRootSha256().orElseThrow());
                var validation = exact.read(validationKey)
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS)
                        .orElseThrow();
                assertThat(responses.delete(validationKey).get(30, TimeUnit.SECONDS))
                        .isTrue();
                try (var roots = control.captureMaterializationTaskRoots(
                                scan, observed, worker, bootstrap, connection.physical())
                        .get(30, TimeUnit.SECONDS)) {
                    assertThatThrownBy(() -> roots.closedTaskReferences(registry, target, coverage))
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("root is missing: validations");
                } finally {
                    assertThat(exact.compareAndSet(Optional.empty(), validationKey, validation.canonicalStoredBytes())
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS))
                            .isEqualTo(
                                    com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1
                                            .MutationOutcome.APPLIED_EXACT);
                }
                try (var roots = control.captureMaterializationTaskRoots(
                                scan, observed, worker, bootstrap, connection.physical())
                        .get(30, TimeUnit.SECONDS)) {
                    assertThat(roots.closedTaskReferences(registry, target, coverage)
                                    .observations())
                            .anySatisfy(row -> {
                                assertThat(row.authority().key()).isEqualTo(validationKey);
                                assertThat(row.authority().metadataVersion())
                                        .isNotEqualTo(validation.metadataVersion());
                            });
                }
                var beforeCursor = observed.usage();
                try (var cursors = PulsarNativeCursorFloorScanV3.capture(
                                binding, actualMetadata, bootstrap, scan, observed, ledger.getConfig(), worker)
                        .get(30, TimeUnit.SECONDS)) {
                    try (var allTasks = control.captureMaterializationTaskRoots(
                                    scan, observed, worker, bootstrap, connection.physical())
                            .get(30, TimeUnit.SECONDS)) {
                        var allReferences = control.capturePhysicalReferences(
                                        scan,
                                        cursors,
                                        allTasks,
                                        registry,
                                        binding.materializationQuota(connection.physical())
                                                .get(30, TimeUnit.SECONDS)
                                                .members(),
                                        orphan.get(),
                                        observed,
                                        worker)
                                .get(30, TimeUnit.SECONDS);
                        assertThat(allReferences).hasSize(15);
                        assertThat(allReferences)
                                .filteredOn(result -> result.referenceKind()
                                        == com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                                .ReferenceKindV1.AUDIT_GRACE)
                                .singleElement()
                                .satisfies(result -> assertThat(result.observations())
                                        .allSatisfy(row -> assertThat(row.disposition())
                                                .isEqualTo(
                                                        com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                                                .ReferenceDispositionV1.PRESENT)));
                        responses.holdOneGet(observed.ownerKey());
                        try {
                            var refreshing =
                                    allTasks.refreshReferenceReservations().toCompletableFuture();
                            assertThat(responses.heldReadEntered.await(30, TimeUnit.SECONDS))
                                    .isTrue();
                            var retained = observed.usage();
                            var closingRefresh = allTasks.closeAsync();
                            assertThat(refreshing.cancel(false)).isTrue();
                            assertThat(closingRefresh).isNotDone();
                            assertThat(observed.usage()).isEqualTo(retained);
                            assertThat(observed.durableUsage().get(30, TimeUnit.SECONDS))
                                    .isEqualTo(retained);
                            responses.heldReadRelease.complete(null);
                            closingRefresh.get(30, TimeUnit.SECONDS);
                        } finally {
                            responses.clear();
                        }
                    }
                    var auditFloor = binding.captureAuditFloor(registry, scan, cursors.coverage())
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS);
                    assertThat(auditFloor.observations()).singleElement().satisfies(row -> {
                        assertThat(row.constraining()).isTrue();
                        assertThat(row.pulsarSafeFloor()).isPresent();
                        assertThat(row.authority().metadataVersion()).isEqualTo(scan.metadataVersion());
                    });
                    var projectionFloor = binding.captureAccessProjectionFloor(registry, scan)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS);
                    assertThat(projectionFloor.observations()).singleElement().satisfies(row -> {
                        assertThat(row.constraining()).isFalse();
                        assertThat(row.enumerationComplete()).isTrue();
                        assertThat(row.authority().metadataVersion()).isEqualTo(scan.metadataVersion());
                    });
                    var physicalInventory = binding.materializationQuota(connection.physical())
                            .get(30, TimeUnit.SECONDS)
                            .members();
                    var excluded = binding.captureExcludedPhysicalReferences(
                                    registry, scan, physicalInventory, orphan.get(), cursors.coverage())
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS);
                    assertThat(excluded).hasSize(5).anySatisfy(result -> assertThat(result.referenceKind())
                            .isEqualTo(projectionKind));
                    for (var result : excluded) {
                        assertThat(result.observations()).hasSize(2).allSatisfy(row -> {
                            assertThat(row.disposition())
                                    .isEqualTo(
                                            com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                                    .ReferenceDispositionV1.ABSENT);
                            assertThat(row.targetIdentitySha256())
                                    .isEqualTo(orphan.get().sha256());
                            assertThat(row.coverage().pulsarCoverage()).contains(cursors.coverage());
                        });
                    }
                    for (var kind : List.of(
                            com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                    .PULSAR_SUBSCRIPTION_OR_REPLICATION_CURSOR,
                            com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                    .RECOVERY_CHECKPOINT_OR_SNAPSHOT)) {
                        var present = cursors.scanReferences(registry, kind, target, worker)
                                .get(30, TimeUnit.SECONDS);
                        assertThat(present.observations()).allSatisfy(row -> assertThat(row.disposition())
                                .isEqualTo(
                                        com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                                .ReferenceDispositionV1.PRESENT));
                        var absent = cursors.scanReferences(registry, kind, orphan.get(), worker)
                                .get(30, TimeUnit.SECONDS);
                        assertThat(absent.observations()).allSatisfy(row -> {
                            assertThat(row.disposition())
                                    .isEqualTo(
                                            com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                                    .ReferenceDispositionV1.ABSENT);
                            assertThat(row.coverage().pulsarCoverage()).contains(cursors.coverage());
                            assertThat(row.enumerationComplete()).isTrue();
                        });
                    }
                    try (var roots = control.captureMaterializationTaskRoots(
                                    scan, observed, worker, bootstrap, connection.physical())
                            .get(30, TimeUnit.SECONDS)) {
                        assertThatThrownBy(() -> roots.closedTaskReferences(registry, orphan.get(), coverage))
                                .isInstanceOf(IllegalStateException.class)
                                .hasMessageContaining("actors, reservations");
                        var composed = roots.closedTaskReferences(registry, orphan.get(), cursors);
                        assertThat(composed.observations())
                                .allSatisfy(row -> assertThat(row.coverage().pulsarCoverage())
                                        .contains(cursors.coverage()));
                    }
                    var generation = control.captureGenerationReferences(
                                    scan, cursors, registry, target, observed, worker)
                            .get(30, TimeUnit.SECONDS);
                    assertThat(generation).hasSize(4).anySatisfy(result -> {
                        assertThat(result.referenceKind())
                                .isEqualTo(
                                        com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                                .MANIFEST_SELECTED);
                        assertThat(result.observations()).allSatisfy(row -> assertThat(row.disposition())
                                .isEqualTo(
                                        com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                                .ReferenceDispositionV1.PRESENT));
                    });
                    var unselected = control.captureGenerationReferences(
                                    scan, cursors, registry, orphan.get(), observed, worker)
                            .get(30, TimeUnit.SECONDS);
                    assertThat(unselected).hasSize(4).allSatisfy(result -> {
                        assertThat(result.summary().complete()).isTrue();
                        assertThat(result.observations()).allSatisfy(row -> {
                            assertThat(row.disposition())
                                    .isEqualTo(
                                            com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                                    .ReferenceDispositionV1.ABSENT);
                            assertThat(row.coverage().pulsarCoverage()).contains(cursors.coverage());
                        });
                    });
                    var actualBk = new PhysicalResourceIdV2.BookKeeperLedger(
                            bootstrap
                                    .readBinding()
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS)
                                    .orElseThrow()
                                    .physicalNamespace(),
                            originalPlan
                                    .sourceCut()
                                    .coverage()
                                    .pulsarCoverage()
                                    .orElseThrow()
                                    .ranges()
                                    .get(0)
                                    .ledgerId());
                    var fallback = control.captureGenerationReferences(
                                    scan, cursors, registry, actualBk, observed, worker)
                            .get(30, TimeUnit.SECONDS);
                    for (var kind : List.of(
                            com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                    .MANIFEST_FALLBACK,
                            com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                    .SOURCE_PROTECTION)) {
                        assertThat(fallback).anySatisfy(result -> {
                            assertThat(result.referenceKind()).isEqualTo(kind);
                            assertThat(result.observations()).allSatisfy(row -> assertThat(row.disposition())
                                    .isEqualTo(
                                            com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                                    .ReferenceDispositionV1.PRESENT));
                        });
                    }
                    var unresolved = observed.reserveCursorObservation(binding, PulsarNativeCursorMetadataV2.MAX_BYTES);
                    unresolved.ready().get(30, TimeUnit.SECONDS);
                    try {
                        var retained = observed.usage();
                        assertThatThrownBy(() -> control.captureGenerationReferences(
                                                scan, cursors, registry, orphan.get(), observed, worker)
                                        .get(30, TimeUnit.SECONDS))
                                .hasRootCauseMessage("Native generation references have unresolved Cell reservations");
                        assertThat(observed.usage()).isEqualTo(retained);
                    } finally {
                        unresolved.releaseBeforeActor().get(30, TimeUnit.SECONDS);
                    }
                    responses.holdOneGet(scan.key());
                    try {
                        var pending = cursors.scanReferences(
                                registry,
                                com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                        .RECOVERY_CHECKPOINT_OR_SNAPSHOT,
                                orphan.get(),
                                worker);
                        assertThat(responses.heldReadEntered.await(30, TimeUnit.SECONDS))
                                .isTrue();
                        var charge = observed.usage();
                        var closing = cursors.closeAsync();
                        assertThat(pending.cancel(false)).isTrue();
                        assertThat(closing).isNotDone();
                        assertThat(observed.usage()).isEqualTo(charge);
                        responses.heldReadRelease.complete(null);
                        closing.get(30, TimeUnit.SECONDS);
                        assertThat(observed.usage()).isEqualTo(beforeCursor);
                    } finally {
                        responses.heldReadRelease.complete(null);
                        responses.clear();
                    }
                }
            } finally {
                trim.abortSourceScan(scan, attempt).toCompletableFuture().get(30, TimeUnit.SECONDS);
                observed.closeCell().get(30, TimeUnit.SECONDS);
            }
            var auditOpen = exact.read(key)
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            assertThat(authority
                            .clearAuditHold(key, auditOpen, registry, audit.hold())
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS))
                    .isEqualTo(
                            com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1.Outcome
                                    .APPLIED_EXACT);
            var replacementAudit = binding.prepareAuditRegistration(
                            registry, connection.physical(), orphan.get(), coverage, auditId)
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS);
            assertThat(replacementAudit.register().get(30, TimeUnit.SECONDS))
                    .isEqualTo(
                            com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1.Outcome
                                    .APPLIED_EXACT);
            auditOpen = exact.read(key)
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            assertThat(authority
                            .clearAuditHold(key, auditOpen, registry, audit.hold())
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS))
                    .isEqualTo(
                            com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1.Outcome
                                    .CONFLICT);
            assertThat(authority
                            .clearAuditHold(key, auditOpen, registry, replacementAudit.hold())
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS))
                    .isEqualTo(
                            com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1.Outcome
                                    .APPLIED_EXACT);
            assertThat(audit.reconcile().get(30, TimeUnit.SECONDS))
                    .isEqualTo(
                            com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1.Outcome
                                    .APPLIED_EXACT);
            var emptyAttempt = Sha256Digest.hash(
                    CanonicalUtf8.fromString("native-audit-empty-scan").bytes());
            var emptyScan = trim.beginSourceScan(
                            exact.read(key)
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS)
                                    .orElseThrow(),
                            emptyAttempt)
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .exactAuthority()
                    .orElseThrow();
            try {
                var completeBudget = readBudget(binding, 6);
                try {
                    try (var completeCursors = PulsarNativeCursorFloorScanV3.capture(
                                            binding,
                                            actualMetadata,
                                            bootstrap,
                                            emptyScan,
                                            completeBudget,
                                            config(offloader),
                                            worker)
                                    .get(30, TimeUnit.SECONDS);
                            var completeTasks = control.captureMaterializationTaskRoots(
                                            emptyScan, completeBudget, worker, bootstrap, connection.physical())
                                    .get(30, TimeUnit.SECONDS)) {
                        var complete = control.capturePhysicalReferences(
                                        emptyScan,
                                        completeCursors,
                                        completeTasks,
                                        registry,
                                        binding.materializationQuota(connection.physical())
                                                .get(30, TimeUnit.SECONDS)
                                                .members(),
                                        orphan.get(),
                                        completeBudget,
                                        worker)
                                .get(30, TimeUnit.SECONDS);
                        assertThat(complete).hasSize(15);
                        assertThat(complete)
                                .extracting(result -> result.referenceKind())
                                .doesNotHaveDuplicates();
                        assertThat(complete).allSatisfy(result -> {
                            assertThat(result.summary().complete()).isTrue();
                            assertThat(result.observations()).isNotEmpty().allSatisfy(row -> {
                                assertThat(row.disposition())
                                        .isEqualTo(
                                                com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                                        .ReferenceDispositionV1.ABSENT);
                                assertThat(row.targetIdentitySha256())
                                        .isEqualTo(orphan.get().sha256());
                                assertThat(row.coverage().pulsarCoverage()).contains(completeCursors.coverage());
                            });
                        });
                        var floors = control.captureRetentionFloors(
                                        emptyScan,
                                        completeCursors,
                                        completeTasks,
                                        registry,
                                        binding.materializationQuota(connection.physical())
                                                .get(30, TimeUnit.SECONDS)
                                                .members(),
                                        orphan.get(),
                                        completeBudget,
                                        worker)
                                .get(30, TimeUnit.SECONDS);
                        assertThat(floors).hasSize(10);
                        assertThat(floors).extracting(row -> row.floorClass()).doesNotHaveDuplicates();
                        assertThat(floors.stream().flatMap(row -> row.observations().stream()))
                                .allSatisfy(row ->
                                        assertThat(row.enumerationComplete()).isTrue());
                        System.out.println("NATIVE_COMPLETE_FLOOR_INVENTORY actualKinds=10 taskOriginalDrain=true"
                                + " exclusiveMemberScope=true actualP1Policy=true wholeM5Authority=false");
                        try (var gc = PulsarNativeGcScopeV1.open(
                                        binding,
                                        (PhysicalResourceIdV2.ObjectVersion) orphan.get(),
                                        completeBudget,
                                        completeCursors,
                                        emptyScan,
                                        offloader,
                                        64L * 1024 * 1024,
                                        worker)
                                .get(30, TimeUnit.SECONDS)) {
                            assertThat(gc.context().coordinatorOwner().key()).endsWith("/fact-v1");
                            assertThat(gc.context().observationEpoch()).isEqualTo(1);
                            responses.holdAppliedPut(
                                    connection.binding().authorityRoot() + "/"
                                            + orphan.get().authorityKey(),
                                    bytes -> {
                                        var canonical = CanonicalBytes.copyOf(bytes);
                                        return com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1
                                                .decodeAuthority(canonical)
                                                .epochGrace()
                                                .isPresent();
                                    });
                            try {
                                var markObserver = gc.mark(control, completeTasks, registry, worker);
                                var applied = responses.heldApplied.await(30, TimeUnit.SECONDS);
                                if (!applied) {
                                    markObserver.get(30, TimeUnit.SECONDS);
                                }
                                assertThat(applied)
                                        .as("original GC mark: %s", markObserver)
                                        .isTrue();
                                assertThatThrownBy(() -> gc.requireElapsed().get(30, TimeUnit.SECONDS))
                                        .hasRootCauseMessage("epoch grace has no confirmed local clock");
                                var retained = completeBudget.usage();
                                assertThat(markObserver.cancel(false)).isTrue();
                                assertThat(completeBudget.usage()).isEqualTo(retained);
                                responses.loseHeldPutReply();
                                assertThatThrownBy(() -> gc.mark(control, completeTasks, registry, worker)
                                                .get(30, TimeUnit.SECONDS))
                                        .isInstanceOf(java.util.concurrent.ExecutionException.class);
                            } finally {
                                responses.clear();
                            }
                            gc.mark(control, completeTasks, registry, worker).get(30, TimeUnit.SECONDS);
                            Thread.sleep(25);
                            var elapsed = gc.requireElapsed().get(30, TimeUnit.SECONDS);
                            assertThat(elapsed.mark().durationMillis()).isEqualTo(25);
                            assertThat(elapsed.mark().authority()).isEqualTo(gc.context());
                            elapsed.requireCurrent().toCompletableFuture().get(30, TimeUnit.SECONDS);
                            responses.holdOneGet(completeBudget.ownerKey());
                            try {
                                var pending = elapsed.requireCurrent().toCompletableFuture();
                                assertThat(responses.heldReadEntered.await(30, TimeUnit.SECONDS))
                                        .isTrue();
                                var retained = completeBudget.usage();
                                var cursorClose = completeCursors.closeAsync();
                                var gcClose = gc.closeAsync();
                                assertThat(pending.cancel(false)).isTrue();
                                assertThat(cursorClose).isNotDone();
                                assertThat(gcClose).isNotDone();
                                assertThat(completeBudget.usage()).isEqualTo(retained);
                                responses.heldReadRelease.complete(null);
                                gcClose.get(30, TimeUnit.SECONDS);
                                cursorClose.get(30, TimeUnit.SECONDS);
                            } finally {
                                responses.clear();
                            }
                            System.out.println("NATIVE_GC_EPOCH actualCellOwner=true actualAdmittedPolicy=true"
                                    + " originalMarkUnknownReconciled=true actualMonotonicGrace=true"
                                    + " closeWaitsOriginalRead=true physicalDeleteAuthority=false");
                        }
                        System.out.println("NATIVE_C_REFERENCE_INVENTORY actualKinds=15 actualBusinessAuditAbsent=true"
                                + " epochGraceQualified=false physicalDeleteAuthority=false");
                    }
                } finally {
                    completeBudget.closeCell().get(30, TimeUnit.SECONDS);
                }
                var auditRoute =
                        binding.materializationQuota(connection.physical()).get(30, TimeUnit.SECONDS);
                assertThat(binding.captureAuditReferences(
                                        registry, emptyScan, auditRoute.members(), orphan.get(), coverage)
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS)
                                .observations())
                        .hasSize(2)
                        .allSatisfy(row -> assertThat(row.disposition())
                                .isEqualTo(
                                        com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                                .ReferenceDispositionV1.ABSENT));
                assertThat(binding.captureAuditFloor(registry, emptyScan, coverage)
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS)
                                .observations())
                        .singleElement()
                        .satisfies(row -> assertThat(row.constraining()).isFalse());
                verifyNativeUnpublishedGc(
                        binding,
                        control,
                        bootstrap,
                        connection,
                        offloader,
                        registry,
                        emptyScan,
                        (PhysicalResourceIdV2.ObjectVersion) orphan.get(),
                        actualMetadata,
                        worker);
            } finally {
                trim.abortSourceScan(emptyScan, emptyAttempt)
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS);
            }
            verifyNativeQuotaReadmission(
                    binding,
                    bootstrap,
                    offloader,
                    actualMetadata,
                    responses,
                    registry,
                    authority,
                    key,
                    worker,
                    event,
                    staging,
                    caps,
                    indexes);
            System.out.println("NATIVE_ONLY_TASK_CLOSURE actualFirstAdmission=true actualPublishedGraph=true"
                    + " actualCellDrain=true"
                    + " heldReservationVeto=true recoveryRootHandoff=false"
                    + " physicalDeleteAuthority=false wholeM5Authority=false");
        } finally {
            if (source != null) {
                source.closeAsync().get(60, TimeUnit.SECONDS);
            }
            if (owner != null) {
                owner.fence().get(30, TimeUnit.SECONDS);
            }
            control.closeControlWriter().get(30, TimeUnit.SECONDS);
            event.shutdown();
            assertThat(event.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            try (var paths = Files.walk(staging)) {
                for (var owned :
                        paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                    Files.delete(owned);
                }
            }
        }
    }

    private static void verifyNativeQuotaReadmission(
            PulsarNativeBindingV1 original,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            NereusPulsarLedgerOffloaderV1 offloader,
            MetadataStoreExtended metadata,
            SelectorResponseFault responses,
            com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1 registry,
            com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1 authority,
            String key,
            java.util.concurrent.Executor worker,
            java.util.concurrent.ExecutorService event,
            Path staging,
            com.nereusstream.storage.object.materialization.M5MaterializationAdmissionV1.Caps caps,
            List<com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind> indexes)
            throws Exception {
        var binding = PulsarNativeBindingV1.read(
                        responses,
                        original.metadataNamespace(),
                        original.authorityRoot(),
                        (PulsarTopicIncarnationIdentity)
                                original.authority().aggregate().binding().incarnationIdentity())
                .toCompletableFuture()
                .get(30, TimeUnit.SECONDS);
        var control = binding.m4Control();
        var budget = readBudget(binding, 6);
        PulsarNativeManagedLedgerFactoryV2 factory = null;
        PulsarNativeSourcePreparationV1 sources = null;
        PulsarNativeM4ReadOwnerV1 owner = null;
        PulsarNativeMaterializationSourceV2 source = null;
        try {
            control.installControlWriter(registry, authority, key, budget).get(30, TimeUnit.SECONDS);
            factory = guardedMappedFactory(binding, metadata, bootstrap, offloader, budget, worker, responses);
            var ledger = factory.openLedger(config(offloader).setMaxUnackedRangesToPersistInMetadataStore(0));
            var selected = control.readSelectedMaterialization(worker).get(30, TimeUnit.SECONDS);
            var objects = selected.providerProfile().physicalBinding().physicalNamespace();
            sources = control.prepareMaterializedReadSources(
                            ledger, factory.namespace(), offloader, objects, budget, worker)
                    .get(60, TimeUnit.SECONDS);
            var capsule = PulsarNativeM4DrainAdmissionV1.load(
                    Path.of(System.getProperty("nereus.m5.nativeM4AdmissionFile")),
                    Sha256Digest.copyOf(java.util.HexFormat.of()
                            .parseHex(System.getProperty("nereus.m5.nativeM4AdmissionSha256"))));
            var recovery = control.prepareMaterializedReadOwner(
                            sources, capsule, factory.namespace(), offloader, worker)
                    .get(60, TimeUnit.SECONDS);
            owner = recovery.recover(worker, event, 2, budget).get(60, TimeUnit.SECONDS);
            var ranges = selected.readMap()
                    .plan()
                    .sourceCut()
                    .coverage()
                    .pulsarCoverage()
                    .orElseThrow()
                    .ranges();
            source = PulsarNativeMaterializationSourceV2.capture(owner, factory.namespace(), offloader, ranges, worker)
                    .get(60, TimeUnit.SECONDS);
            var connection = source.qualifyProvider(64L * 1024 * 1024, worker).get(60, TimeUnit.SECONDS);
            var route = binding.materializationQuota(connection.physical()).get(30, TimeUnit.SECONDS);
            var before = route.quota().snapshot().toCompletableFuture().get(30, TimeUnit.SECONDS);
            var layout = com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Layout.withPhysicalMembers(
                    connection.binding().authorityRoot(), objects);
            assertThat(observeActualQuota("readmission-before", layout, null)).isEqualTo(before);
            var task = source.prepareTask(true, indexes, caps, worker).get(60, TimeUnit.SECONDS);
            task.writeAndVerify(
                            Files.createDirectory(staging.resolve("after-gc-readmission")), 64L * 1024 * 1024, worker)
                    .get(60, TimeUnit.SECONDS);
            var actual = route.members()
                    .captureNamespace(1024, 16L * 1024 * 1024)
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS);
            var assigned = actual.births().stream()
                    .filter(birth ->
                            birth.worker().taskIdSha256().equals(task.plan().taskIdSha256()))
                    .filter(birth -> birth.resource().isPresent())
                    .toList();
            assertThat(assigned).isNotEmpty();
            for (var birth : assigned) {
                route.readResourceFact(birth.resource().orElseThrow())
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS);
            }
            var after = route.quota().snapshot().toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertThat(observeActualQuota("readmission-after", layout, null)).isEqualTo(after);
            assertThat(after.head().capacityBytes()).isEqualTo(before.head().capacityBytes());
            assertThat(after.head().reservedResources())
                    .isGreaterThan(before.head().reservedResources());
            assertThat(after.availableCanonicalBytes()).isLessThan(before.availableCanonicalBytes());
            source.closeAsync().get(60, TimeUnit.SECONDS);
            source = null;
            owner.drainForLogicalTrim(worker).get(30, TimeUnit.SECONDS);
            System.out.println("NATIVE_GC_QUOTA_READMISSION sameBackend=true actualMappedSourceColdOwner=true"
                    + " newCompletePlan=true actualSdkOutputs=true originalCapacityUnchanged=true"
                    + " permanentHistoryRetained=true wholeM5Authority=false");
        } finally {
            if (source != null) {
                source.closeAsync().get(60, TimeUnit.SECONDS);
            }
            if (owner != null) {
                owner.fence().get(30, TimeUnit.SECONDS);
            }
            if (sources != null) {
                sources.close().get(30, TimeUnit.SECONDS);
            }
            if (factory != null) {
                factory.closeAsync().get(60, TimeUnit.SECONDS);
            }
            control.closeControlWriter().get(30, TimeUnit.SECONDS);
            budget.closeCell().get(30, TimeUnit.SECONDS);
        }
    }

    private static void verifyNativeUnpublishedGc(
            PulsarNativeBindingV1 binding,
            PulsarNativeM4ControlV1 control,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            S3PulsarOffloadObjectStoreV1.MaterializationConnection connection,
            NereusPulsarLedgerOffloaderV1 offloader,
            com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1 registry,
            com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue scan,
            PhysicalResourceIdV2.ObjectVersion target,
            MetadataStoreExtended metadata,
            java.util.concurrent.Executor worker)
            throws Exception {
        var route = binding.materializationQuota(connection.physical()).get(30, TimeUnit.SECONDS);
        var before = route.quota().snapshot().toCompletableFuture().get(30, TimeUnit.SECONDS);
        var layout = com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Layout.withPhysicalMembers(
                connection.binding().authorityRoot(), target.namespace());
        assertThat(observeActualQuota("gc-before", layout, target)).isEqualTo(before);
        try (var independent = client(null)) {
            assertThat(independent
                            .headObject(value -> value.bucket(target.namespace()
                                            .containerIdentity()
                                            .value())
                                    .key(target.objectKey().value())
                                    .versionId(target.immutableIdentity().value()))
                            .versionId())
                    .isEqualTo(target.immutableIdentity().value());
        }
        Sha256Digest originalReadAttempt = null;
        Sha256Digest originalDeleteAttempt = null;
        Sha256Digest originalExternalIdentity = null;
        VersionedValue compact = null;
        com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1.EpochGraceReady stale = null;
        for (int phase = 0; phase < 3; phase++) {
            try (var client = OxiaClientBuilder.create(System.getProperty("nereus.m5.nativeOffloadOxia"))
                    .namespace("default")
                    .requestTimeout(Duration.ofSeconds(10))
                    .asyncClient()
                    .get(30, TimeUnit.SECONDS)) {
                var delivery = new SelectorResponseFault(client);
                var coldBinding = PulsarNativeBindingV1.read(
                                delivery,
                                binding.metadataNamespace(),
                                binding.authorityRoot(),
                                (PulsarTopicIncarnationIdentity) binding.authority()
                                        .aggregate()
                                        .binding()
                                        .incarnationIdentity())
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS);
                var coldControl = coldBinding.m4Control();
                var budget = readBudget(coldBinding, 6);
                try {
                    try (var cursors = PulsarNativeCursorFloorScanV3.capture(
                                            coldBinding, metadata, bootstrap, scan, budget, config(offloader), worker)
                                    .get(30, TimeUnit.SECONDS);
                            var tasks = coldControl
                                    .captureMaterializationTaskRoots(
                                            scan, budget, worker, bootstrap, connection.physical())
                                    .get(30, TimeUnit.SECONDS);
                            var gc = PulsarNativeGcScopeV1.open(
                                            coldBinding,
                                            target,
                                            budget,
                                            cursors,
                                            scan,
                                            offloader,
                                            64L * 1024 * 1024,
                                            worker)
                                    .get(30, TimeUnit.SECONDS)) {
                        assertThat(gc.context().observationEpoch()).isEqualTo(phase + 2);
                        gc.mark(coldControl, tasks, registry, worker).get(30, TimeUnit.SECONDS);
                        if (stale != null) {
                            var old = stale;
                            assertThatThrownBy(() -> old.requireCurrent()
                                            .toCompletableFuture()
                                            .get(30, TimeUnit.SECONDS))
                                    .isInstanceOf(IllegalStateException.class)
                                    .hasMessage("Native GC scope is not open");
                        }
                        Thread.sleep(25);
                        var elapsed = gc.requireElapsed().get(30, TimeUnit.SECONDS);
                        if (phase > 0) {
                            var renewedState = phase == 1
                                    ? com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                            .TargetDeleteAuthorityStateV1.READ_FENCED_V1
                                    : com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                            .TargetDeleteAuthorityStateV1.DELETE_INTENT_V1;
                            delivery.armCandidate(bytes -> {
                                try {
                                    var candidate = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1
                                            .decodeAuthority(CanonicalBytes.copyOf(bytes));
                                    return candidate.target().resourceId().equals(target)
                                            && candidate.state() == renewedState
                                            && candidate.recoveryVeto().isEmpty()
                                            && candidate.epochGrace().equals(java.util.Optional.of(elapsed.mark()));
                                } catch (RuntimeException differentRecord) {
                                    return false;
                                }
                            });
                            int refreshLosses = delivery.putLosses.get();
                            try {
                                var failure = org.assertj.core.api.Assertions.catchThrowable(
                                        () -> gc.reclaimUnpublishedAfterElapsed(coldControl, tasks, registry)
                                                .get(60, TimeUnit.SECONDS));
                                assertThat(failure).isInstanceOf(java.util.concurrent.ExecutionException.class);
                                if (delivery.putLosses.get() != refreshLosses + 1) {
                                    throw new AssertionError(
                                            "cold phase " + phase + " failed before its one refresh response loss",
                                            failure);
                                }
                            } finally {
                                delivery.clear();
                            }
                            var renewed =
                                    com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                                            route.read(target.authorityKey())
                                                    .toCompletableFuture()
                                                    .get(30, TimeUnit.SECONDS)
                                                    .orElseThrow()
                                                    .canonicalStoredBytes());
                            assertThat(renewed.state()).isEqualTo(renewedState);
                            assertThat(renewed.recoveryVeto()).isEmpty();
                            assertThat(renewed.epochGrace()).contains(elapsed.mark());
                            assertThat(renewed.readFence().orElseThrow().attemptIdSha256())
                                    .isEqualTo(originalReadAttempt);
                            if (phase == 2) {
                                assertThat(renewed.deleteIntent().orElseThrow().deleteAttemptIdSha256())
                                        .isEqualTo(originalDeleteAttempt);
                                assertThat(renewed.externalIdentity()
                                                .orElseThrow()
                                                .externalIdentitySha256())
                                        .isEqualTo(originalExternalIdentity);
                            }
                            assertThat(route.quota()
                                            .requireExistingReservation(target)
                                            .toCompletableFuture()
                                            .get(30, TimeUnit.SECONDS)
                                            .settled())
                                    .isFalse();
                            try (var independent = client(null)) {
                                assertThat(independent
                                                .headObject(value -> value.bucket(target.namespace()
                                                                .containerIdentity()
                                                                .value())
                                                        .key(target.objectKey().value())
                                                        .versionId(target.immutableIdentity()
                                                                .value()))
                                                .versionId())
                                        .isEqualTo(target.immutableIdentity().value());
                            }
                        }
                        if (phase < 2) {
                            var wanted = phase == 0
                                    ? com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                            .TargetDeleteAuthorityStateV1.READ_FENCED_V1
                                    : com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                            .TargetDeleteAuthorityStateV1.DELETE_INTENT_V1;
                            delivery.armCandidate(bytes -> {
                                try {
                                    var value = com.nereusstream.storage.object.gc.M5TargetDeleteStoredValueV2.decode(
                                            VersionedValue.of(
                                                    target.authorityKey(),
                                                    CanonicalBytes.copyOf(bytes),
                                                    scan.metadataVersion()));
                                    return value.resource().equals(target) && value.state() == wanted;
                                } catch (RuntimeException differentRecord) {
                                    return false;
                                }
                            });
                            int losses = delivery.putLosses.get();
                            try {
                                var failure = org.assertj.core.api.Assertions.catchThrowable(
                                        () -> gc.reclaimUnpublishedAfterElapsed(coldControl, tasks, registry)
                                                .get(60, TimeUnit.SECONDS));
                                assertThat(failure).isInstanceOf(java.util.concurrent.ExecutionException.class);
                                if (delivery.putLosses.get() != losses + 1) {
                                    throw new AssertionError(
                                            "cold phase " + phase + " failed before its one response loss", failure);
                                }
                            } finally {
                                delivery.clear();
                            }
                            var stored =
                                    com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                                            route.read(target.authorityKey())
                                                    .toCompletableFuture()
                                                    .get(30, TimeUnit.SECONDS)
                                                    .orElseThrow()
                                                    .canonicalStoredBytes());
                            assertThat(stored.state()).isEqualTo(wanted);
                            if (originalReadAttempt == null) {
                                originalReadAttempt =
                                        stored.readFence().orElseThrow().attemptIdSha256();
                            }
                            assertThat(stored.readFence().orElseThrow().attemptIdSha256())
                                    .isEqualTo(originalReadAttempt);
                            if (phase == 1) {
                                originalDeleteAttempt =
                                        stored.deleteIntent().orElseThrow().deleteAttemptIdSha256();
                                originalExternalIdentity =
                                        stored.externalIdentity().orElseThrow().externalIdentitySha256();
                            }
                            assertThat(route.quota()
                                            .requireExistingReservation(target)
                                            .toCompletableFuture()
                                            .get(30, TimeUnit.SECONDS)
                                            .settled())
                                    .isFalse();
                            try (var independent = client(null)) {
                                assertThat(independent
                                                .headObject(value -> value.bucket(target.namespace()
                                                                .containerIdentity()
                                                                .value())
                                                        .key(target.objectKey().value())
                                                        .versionId(target.immutableIdentity()
                                                                .value()))
                                                .versionId())
                                        .isEqualTo(target.immutableIdentity().value());
                            }
                            stale = elapsed;
                            continue;
                        }
                        compact = gc.reclaimUnpublishedAfterElapsed(coldControl, tasks, registry)
                                .get(60, TimeUnit.SECONDS);
                        var stored = com.nereusstream.storage.object.gc.M5TargetDeleteStoredValueV2.decode(compact);
                        assertThat(stored.compactDone()).isPresent();
                        assertThat(stored.resource()).isEqualTo(target);
                        assertThat(stored.compactDone().orElseThrow().done().deleteAttemptIdSha256())
                                .isEqualTo(originalDeleteAttempt);
                        assertThat(stored.compactDone().orElseThrow().externalIdentitySha256())
                                .isEqualTo(originalExternalIdentity);
                        assertThat(gc.reclaimUnpublishedAfterElapsed(coldControl, tasks, registry)
                                        .get(30, TimeUnit.SECONDS))
                                .isEqualTo(compact);
                    }
                } finally {
                    budget.closeCell().get(30, TimeUnit.SECONDS);
                }
            }
        }
        assertThat(compact).isNotNull();
        try (var independent = client(null)) {
            assertThatThrownBy(() -> independent.headObject(value -> value.bucket(
                                    target.namespace().containerIdentity().value())
                            .key(target.objectKey().value())
                            .versionId(target.immutableIdentity().value())))
                    .isInstanceOf(S3Exception.class)
                    .satisfies(failure ->
                            assertThat(((S3Exception) failure).statusCode()).isEqualTo(404));
        }
        var after = route.quota().snapshot().toCompletableFuture().get(30, TimeUnit.SECONDS);
        assertThat(observeActualQuota("gc-after", layout, target)).isEqualTo(after);
        assertThat(after.head().capacityBytes()).isEqualTo(before.head().capacityBytes());
        assertThat(after.head().reservedResources()).isEqualTo(before.head().reservedResources() - 1);
        assertThat(after.head().settledResources()).isEqualTo(before.head().settledResources() + 1);
        assertThat(after.head().compactDoneValueBytes())
                .isEqualTo(before.head().compactDoneValueBytes()
                        + compact.canonicalStoredBytes().length());
        assertThat(after.head().births()).isEqualTo(before.head().births());
        assertThat(after.chargedCanonicalBytes())
                .isEqualTo(before.chargedCanonicalBytes()
                        - com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MAX_VALUE_BYTES
                        + compact.canonicalStoredBytes().length());
        assertThat(after.availableCanonicalBytes()).isGreaterThan(before.availableCanonicalBytes());
        assertThat(route.quota().settle(target).toCompletableFuture().get(30, TimeUnit.SECONDS))
                .isEqualTo(com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2.Result.SETTLED);
        System.out.println("NATIVE_UNPUBLISHED_GC actualTenFloors=true actual15References=true"
                + " drainedNewOwnerFullGrace=true exactVersionDeleted=true compactDone=true quotaSettled=true"
                + " readFenceUnknownColdResume=true intentUnknownColdResume=true originalAttemptsAndIdentity=true"
                + " staleEpochRejected=true originalReservationsRetained=true freshOxiaClients=true"
                + " replayNoRedispatch=true wholeM5Authority=false");
    }

    private static void verifyUnfenceableSdkReplyLoss(
            PulsarNativeBindingV1 binding,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            NereusPulsarLedgerOffloaderV1 offloader,
            PhysicalResourceIdV2.Namespace objectNamespace,
            SelectorResponseFault responses,
            int operationMode,
            com.nereusstream.storage.object.materialization.M5PulsarCoverageV2 trimCoverage)
            throws Exception {
        boolean explicitLac = operationMode == 69;
        var worker = Executors.newSingleThreadExecutor();
        var budget = readBudget(binding, operationMode >= 72 ? 6 : 3);
        var exact = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(responses);
        var authority = new com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1(exact);
        String key = new OxiaV2AuthorityKeys(binding.authorityRoot())
                .aggregateKey(binding.authority().aggregate().binding().incarnationIdentity());
        var capability = new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding(
                1, bootstrap.capability().configurationDigest());
        var predecessor = authority
                .read(key)
                .toCompletableFuture()
                .get(30, TimeUnit.SECONDS)
                .orElseThrow();
        assertThat(authority
                        .migrateLegacy(
                                new com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1
                                        .MigrationRequest(key, predecessor, capability))
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS))
                .isEqualTo(
                        com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1.Outcome
                                .APPLIED_EXACT);
        var registry = nativeExpectedWriterRegistry(capability);
        var current = authority
                .read(key)
                .toCompletableFuture()
                .get(30, TimeUnit.SECONDS)
                .orElseThrow();
        assertThat(authority
                        .enrollWriters(
                                new com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1
                                        .EnrollmentRequest(key, current, registry.enrollment()))
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS))
                .isEqualTo(
                        com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1.Outcome
                                .APPLIED_EXACT);
        com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorSnapshotV1 trimFixture = null;
        if (operationMode >= 71 && operationMode <= 73) {
            var stored = exact.read(key)
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            var fact = new com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1(
                    key, stored.metadataVersion(), stored.canonicalStoredSha256());
            var floor = new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                    trimCoverage.chain().get(1).ledgerId(), 1);
            var prior = new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                    trimCoverage.chain().get(0).ledgerId(), 0);
            var rows = java.util.Arrays.stream(
                            com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1.values())
                    .map(kind ->
                            new com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                    .RetentionFloorObservationV1(
                                    kind, fact, PositionDomain.PULSAR_ENTRY, -1, true, true, Optional.of(floor)))
                    .toList();
            var snapshot = com.nereusstream.storage.object.retention.M5RetentionCodecV1.finalizeSnapshot(
                    new com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorSnapshotV1(
                            new com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1
                                    .IdentityEnvelope(
                                    Sha256Digest.hash(CanonicalBytes.copyOf(new byte[] {71})),
                                    bootstrap.capability().configurationDigest(),
                                    binding.bindingIdentity(),
                                    1,
                                    1,
                                    1,
                                    capability),
                            PositionDomain.PULSAR_ENTRY,
                            1,
                            -1,
                            Sha256Digest.hash(CanonicalBytes.copyOf(new byte[] {72})),
                            fact,
                            fact,
                            1,
                            4096,
                            rows,
                            Sha256Digest.hash(CanonicalBytes.copyOf(new byte[] {73})),
                            Optional.of(
                                    new com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                            .PulsarFloorContextV2(trimCoverage, prior))));
            trimFixture = snapshot;
            var coordinator = binding.logicalTrimCoordinator();
            var attempt = Sha256Digest.hash(CanonicalBytes.copyOf(new byte[] {74}));
            var fenced = coordinator
                    .fence(stored, snapshot, attempt)
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .exactAuthority()
                    .orElseThrow();
            assertThat(coordinator
                            .commit(fenced, snapshot, attempt)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .exactAuthority())
                    .isPresent();
        }
        PulsarNativeManagedLedgerFactoryV2 factory = null;
        NativeDataReplyLoss replies = null;
        try (var nativeMetadata = metadata()) {
            var failResetFallback = new AtomicBoolean();
            var fallbackFailures = new AtomicInteger();
            String resetPath = "/managed-ledgers/" + binding.managedLedgerName() + "/reset-unknown";
            var resetMetadata = new FaultInjectionMetadataStore(nativeMetadata) {
                @Override
                public CompletableFuture<org.apache.pulsar.metadata.api.Stat> put(
                        String path,
                        byte[] value,
                        Optional<Long> expectedVersion,
                        Set<org.apache.pulsar.metadata.api.Option> options) {
                    if (path.equals(resetPath) && failResetFallback.compareAndSet(true, false)) {
                        fallbackFailures.incrementAndGet();
                        return CompletableFuture.failedFuture(
                                new java.io.IOException("injected reset fallback failure"));
                    }
                    return super.put(path, value, expectedVersion, options);
                }
            };
            var factoryConfig = new ManagedLedgerFactoryConfig();
            factoryConfig.setMaxCacheSize(0);
            factory = PulsarNativeManagedLedgerFactoryV2.open(
                            binding,
                            operationMode == 70 ? resetMetadata : nativeMetadata,
                            "zk://" + System.getProperty("nereus.m5.nativeOffloadZk") + "/ledgers",
                            bootstrap,
                            budget,
                            NATIVE_BK_CANONICAL_CAPACITY,
                            factoryConfig,
                            registry,
                            authority,
                            key,
                            offloader,
                            objectNamespace,
                            8_388_608,
                            worker)
                    .get(60, TimeUnit.SECONDS);
            var original = factory;
            var namespace = original.namespace();
            var ledger = original.openLedger(config(offloader));
            var position = ledger.addEntry(payload(99));
            long readyDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (namespace.sdkDataWriteSnapshot().unresolvedWrites() != 0) {
                assertThat(System.nanoTime()).isLessThan(readyDeadline);
                TimeUnit.MILLISECONDS.sleep(10);
            }
            if (operationMode >= 76 && operationMode <= 78) {
                verifyEnrolledNativeColdOwner(
                        binding,
                        bootstrap,
                        offloader,
                        objectNamespace,
                        original,
                        ledger,
                        budget,
                        responses,
                        registry,
                        authority,
                        key,
                        worker,
                        operationMode == 77,
                        operationMode == 78);
                return;
            }
            if (operationMode == 74 || operationMode == 75) {
                try {
                    verifyActualCursorFloor(
                            binding,
                            bootstrap,
                            original,
                            ledger,
                            nativeMetadata,
                            exact,
                            key,
                            budget,
                            worker,
                            responses,
                            operationMode == 75);
                } finally {
                    original.closeAsync().get(30, TimeUnit.SECONDS);
                }
                return;
            }
            if (operationMode == 72 || operationMode == 73) {
                try {
                    verifyPostTrimReadOwner(
                            binding,
                            bootstrap,
                            offloader,
                            objectNamespace,
                            original,
                            ledger,
                            budget,
                            responses,
                            registry,
                            authority,
                            key,
                            trimFixture,
                            worker,
                            operationMode == 73);
                } finally {
                    original.closeAsync().get(30, TimeUnit.SECONDS);
                }
                return;
            }
            if (operationMode == 71) {
                try {
                    verifyPostTrimCursorReset(binding, bootstrap, ledger, nativeMetadata, trimCoverage);
                } finally {
                    // Cursor drain must finish while the original metadata store is still available.
                    original.closeAsync().get(30, TimeUnit.SECONDS);
                }
                return;
            }
            if (operationMode == 70) {
                verifyOriginalCursorResetUnknown(
                        binding,
                        bootstrap,
                        original,
                        ledger,
                        position,
                        budget,
                        responses,
                        nativeMetadata,
                        failResetFallback,
                        fallbackFailures);
                return;
            }
            var metadata = namespace
                    .borrowClient()
                    .getClientCtx()
                    .getLedgerManager()
                    .readLedgerMetadata(position.getLedgerId())
                    .get(30, TimeUnit.SECONDS)
                    .getValue();
            var bookie = metadata.getEnsembleAt(position.getEntryId()).get(0);
            var masterKey =
                    org.apache.bookkeeper.proto.checksum.DigestManager.generateMasterKey(metadata.getPassword());
            var physical = new PhysicalResourceIdV2.BookKeeperLedger(namespace.namespace(), position.getLedgerId());
            var quota = binding.materializationQuota(bootstrap).get(30, TimeUnit.SECONDS);
            var reference = observeSdkRecoveryReferences(binding, responses);
            var scope = ((com.nereusstream.storage.bookkeeper.M5PulsarNativeMetadataDriverV1)
                            namespace.borrowClient().getMetadataClientDriver())
                    .createSpec()
                    .sha256();
            assertThat(reference).singleElement().satisfies(ticket -> assertThat(ticket.externalAuthorityRootSha256())
                    .isEqualTo(scope));
            var physicalTickets = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                            quota.read(physical.authorityKey())
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS)
                                    .orElseThrow()
                                    .canonicalStoredBytes())
                    .activeWriterTickets();
            assertThat(physicalTickets).singleElement().satisfies(ticket -> assertThat(ticket.writerClass())
                    .isEqualTo(
                            com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1
                                    .OWNER_WORKER_LEASE_HANDLE_PIN_V1));
            var charged = budget.usage();
            assertThat(charged.owners()).isEqualTo(2);
            replies = NativeDataReplyLoss.install(namespace.borrowClient(), bookie, position.getLedgerId(), 0);
            replies.dropAdd.set(!explicitLac);
            replies.dropLac.set(explicitLac);
            var result = new CompletableFuture<Integer>();
            org.apache.bookkeeper.util.ByteBufList packet;
            if (explicitLac) {
                var digest = org.apache.bookkeeper.proto.checksum.DigestManager.instantiate(
                        position.getLedgerId(),
                        metadata.getPassword(),
                        org.apache.bookkeeper.client.BookKeeper.DigestType.toProtoDigestType(
                                org.apache.bookkeeper.client.BookKeeper.DigestType.fromApiDigestType(
                                        metadata.getDigestType())),
                        namespace.borrowClient().getClientCtx().getByteBufAllocator(),
                        false);
                packet = digest.computeDigestAndPackageForSendingLac(position.getEntryId());
            } else {
                packet = readOriginalSdkPacket(bootstrap, bookie, position.getLedgerId());
            }
            try {
                var port = namespace.borrowClient().getClientCtx().getBookieClient();
                if (explicitLac) {
                    port.writeLac(
                            bookie,
                            position.getLedgerId(),
                            masterKey,
                            position.getEntryId(),
                            packet,
                            (rc, actualLedger, actualBookie, ctx) -> result.complete(rc),
                            null);
                } else {
                    port.addEntry(
                            bookie,
                            position.getLedgerId(),
                            masterKey,
                            0,
                            packet,
                            (rc, actualLedger, actualEntry, actualBookie, ctx) -> result.complete(rc),
                            null,
                            org.apache.bookkeeper.proto.BookieProtocol.FLAG_RECOVERY_ADD,
                            false,
                            org.apache.bookkeeper.client.api.WriteFlag.NONE);
                }
                assertThat((explicitLac ? replies.lacApplied : replies.addApplied).await(30, TimeUnit.SECONDS))
                        .isTrue();
                assertThat(result.get(30, TimeUnit.SECONDS))
                        .isEqualTo(org.apache.bookkeeper.client.BKException.Code.TimeoutException);
            } finally {
                packet.release();
            }
            var pending = namespace.sdkDataWriteSnapshot();
            assertThat(pending.unresolvedWrites()).isEqualTo(1);
            assertThat(pending.retainedBytes()).isPositive();
            assertThatThrownBy(() -> original.closeAsync().get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("original SDK data write remains unknown");
            assertThat(bootstrap
                            .borrowClient()
                            .getClientCtx()
                            .getLedgerManager()
                            .readLedgerMetadata(position.getLedgerId())
                            .get(30, TimeUnit.SECONDS)
                            .getValue()
                            .isClosed())
                    .isTrue();
            // Actual nonempty EOK waits the bookie's journal fence; it still cannot exclude LAC/RECOVERY_ADD.
            var fenced = new CompletableFuture<Integer>();
            bootstrap
                    .borrowClient()
                    .getClientCtx()
                    .getBookieClient()
                    .readEntry(
                            bookie,
                            position.getLedgerId(),
                            org.apache.bookkeeper.proto.BookieProtocol.LAST_ADD_CONFIRMED,
                            (rc, actualLedger, actualEntry, buffer, ctx) -> {
                                if (rc == org.apache.bookkeeper.client.BKException.Code.OK) {
                                    assertThat(buffer).isNotNull();
                                    assertThat(buffer.readableBytes()).isGreaterThanOrEqualTo(ENTRY_BYTES);
                                }
                                fenced.complete(rc);
                            },
                            null,
                            org.apache.bookkeeper.proto.BookieProtocol.FLAG_DO_FENCING,
                            masterKey,
                            false);
            assertThat(fenced.get(30, TimeUnit.SECONDS)).isEqualTo(org.apache.bookkeeper.client.BKException.Code.OK);
            assertThatThrownBy(() -> original.closeAsync().get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("original SDK data write remains unknown");
            assertThat(namespace.sdkDataWriteSnapshot().unresolvedWrites()).isEqualTo(1);
            assertThat(namespace.sdkDataWriteSnapshot().retainedBytes()).isEqualTo(pending.retainedBytes());
            assertThat(namespace.sdkDataWriteSnapshot().fencedWrites()).isZero();
            assertThat(namespace.borrowClient().getClientCtx().isClientClosed()).isFalse();
            assertThat(budget.usage()).isEqualTo(charged);
            assertThat(budget.durableUsage().get(30, TimeUnit.SECONDS)).isEqualTo(charged);
            assertThat(observeSdkRecoveryReferences(binding, responses)).isEqualTo(reference);
            assertThat(com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                                    quota.read(physical.authorityKey())
                                            .toCompletableFuture()
                                            .get(30, TimeUnit.SECONDS)
                                            .orElseThrow()
                                            .canonicalStoredBytes())
                            .activeWriterTickets())
                    .isEqualTo(physicalTickets);
            var closing = budget.closeCell();
            assertThat(budget.closeCell().cancel(false)).isTrue();
            assertThatThrownBy(() -> closing.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
            assertThatThrownBy(() -> readBudget(binding))
                    .hasRootCauseMessage("Native Cell allocator has no confirmed durable drain");
            assertThat(explicitLac ? replies.lacRequests.get() : replies.addRequests.get())
                    .isEqualTo(1);
            replies.close();
            replies = null;
            // Closing the actual transport cannot manufacture an original-operation terminal proof.
            namespace.close();
            assertThat(namespace.borrowClient().getClientCtx().isClientClosed()).isTrue();
            assertThat(namespace.sdkDataWriteSnapshot().unresolvedWrites()).isEqualTo(1);
            assertThat(namespace.sdkDataWriteSnapshot().retainedBytes()).isEqualTo(pending.retainedBytes());
            assertThat(budget.usage()).isEqualTo(charged);
            assertThat(budget.durableUsage().get(30, TimeUnit.SECONDS)).isEqualTo(charged);
            assertThat(observeSdkRecoveryReferences(binding, responses)).isEqualTo(reference);
            assertThat(com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                                    quota.read(physical.authorityKey())
                                            .toCompletableFuture()
                                            .get(30, TimeUnit.SECONDS)
                                            .orElseThrow()
                                            .canonicalStoredBytes())
                            .activeWriterTickets())
                    .isEqualTo(physicalTickets);
            assertThat(closing).isNotDone();
            assertThatThrownBy(() -> readBudget(binding))
                    .hasRootCauseMessage("Native Cell allocator has no confirmed durable drain");
            System.out.println("NATIVE_UNFENCEABLE_SDK_UNKNOWN explicitLac=" + explicitLac
                    + " actualAppliedReplyLost=true originalSdkTimeout=true independentDurableFence=true"
                    + " originalDescriptorCAndDRetained=true originalLocalAndDurableActorsRetained=true"
                    + " sdkAliveUntilFixtureShutdown=true transportStopDoesNotRefund=true"
                    + " coldOwnerRejected=true originalPacketRedispatch=false"
                    + " unknownTerminationQualified=false wholeM5Authority=false");
        } finally {
            if (replies != null) {
                replies.close();
            }
            if (factory != null
                    && !factory.namespace().borrowClient().getClientCtx().isClientClosed()) {
                // Test transport shutdown bounds test-owned threads; it does not settle the retained M5 actors.
                factory.closeAsync().handle((unused, failure) -> null).get(30, TimeUnit.SECONDS);
                factory.namespace().close();
            }
            worker.shutdown();
            assertThat(worker.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static void verifyEnrolledNativeColdOwner(
            PulsarNativeBindingV1 binding,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            NereusPulsarLedgerOffloaderV1 offloader,
            Namespace objects,
            PulsarNativeManagedLedgerFactoryV2 originalFactory,
            ManagedLedgerImpl originalLedger,
            PulsarNativeReadCellBudgetV1 budget,
            SelectorResponseFault responses,
            com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1 registry,
            com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1 authority,
            String authorityKey,
            java.util.concurrent.Executor worker,
            boolean loseReply,
            boolean objectOnlyFloor)
            throws Exception {
        var exact = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(responses);
        var oldControl = binding.m4Control();
        oldControl
                .installControlWriter(registry, authority, authorityKey, budget)
                .get(30, TimeUnit.SECONDS);
        var capsule = Path.of(System.getProperty("nereus.m5.nativeM4AdmissionFile"));
        var admission = PulsarNativeM4DrainAdmissionV1.load(
                capsule,
                Sha256Digest.copyOf(
                        java.util.HexFormat.of().parseHex(System.getProperty("nereus.m5.nativeM4AdmissionSha256"))));
        var event = Executors.newSingleThreadExecutor();
        PulsarNativeM4ReadOwnerV1 oldOwner = null;
        PulsarNativeM4ReadOwnerV1 recovered = null;
        PulsarNativeManagedLedgerFactoryV2 coldFactory = null;
        PulsarNativeSourcePreparationV1 sources = null;
        PulsarNativeM4ControlV1 coldControl = null;
        Entry retained = null;
        try (var coldMetadata = metadata()) {
            var originalView = preparedView(
                    binding, originalLedger, originalFactory.namespace(), offloader, objects, budget, true);
            oldOwner = oldControl
                    .createReadOwner(
                            originalView, admission, originalFactory.namespace(), offloader, worker, event, 2, budget)
                    .get(30, TimeUnit.SECONDS);
            var predecessor = oldOwner.selector();
            assertThat(predecessor.mode()).isEqualTo(SelectorMode.PREFERRED_ONLY);
            long dataLedger = originalView.ledgerIds().get(0);
            retained = readOrdinaryEntry(originalLedger, PositionFactory.create(dataLedger, 0))
                    .get(30, TimeUnit.SECONDS);
            var drain = oldOwner.drainForLogicalTrim(worker);
            assertThat(drain).isNotDone();
            retained.release();
            retained = null;
            drain.get(30, TimeUnit.SECONDS);
            originalFactory.closeAsync().get(30, TimeUnit.SECONDS);
            oldControl.closeControlWriter().get(30, TimeUnit.SECONDS);
            assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
            if (objectOnlyFloor) {
                // Controlled absence fixture after actual Object reads; this grants no production delete authority.
                var pinned = oldControl
                        .readSelectedSourceMap()
                        .get(30, TimeUnit.SECONDS)
                        .publishedObjects(
                                binding,
                                bootstrap
                                        .readBinding()
                                        .toCompletableFuture()
                                        .get(30, TimeUnit.SECONDS)
                                        .orElseThrow(),
                                originalView.objectBinding())
                        .get(dataLedger);
                try (var independent = client(null)) {
                    var replacement = independent.putObject(
                            PutObjectRequest.builder()
                                    .bucket(objects.containerIdentity().value())
                                    .key(pinned.identity().root().key())
                                    .build(),
                            software.amazon.awssdk.core.sync.RequestBody.fromBytes(new byte[] {9, 7, 3}));
                    assertThat("version:" + replacement.versionId())
                            .isNotEqualTo(pinned.identity().root().version());
                }
                bootstrap.borrowClient().deleteLedger(dataLedger);
                assertThatThrownBy(() -> bootstrap
                                .borrowClient()
                                .getLedgerManager()
                                .readLedgerMetadata(dataLedger)
                                .get(30, TimeUnit.SECONDS))
                        .hasRootCauseInstanceOf(
                                org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsOnMetadataServerException
                                        .class);
            }
            var factoryConfig = new ManagedLedgerFactoryConfig();
            factoryConfig.setMaxCacheSize(0);
            coldFactory = PulsarNativeManagedLedgerFactoryV2.open(
                            binding,
                            coldMetadata,
                            "zk://" + System.getProperty("nereus.m5.nativeOffloadZk") + "/ledgers",
                            bootstrap,
                            budget,
                            NATIVE_BK_CANONICAL_CAPACITY,
                            factoryConfig,
                            registry,
                            authority,
                            authorityKey,
                            offloader,
                            objects,
                            8_388_608,
                            worker)
                    .get(60, TimeUnit.SECONDS);
            var coldLedger = coldFactory.openLedger(config(offloader));
            if (objectOnlyFloor) {
                coldLedger.openCursor("actual-object-only-floor", InitialPosition.Earliest);
            }
            coldControl = binding.m4Control();
            coldControl
                    .installControlWriter(registry, authority, authorityKey, budget)
                    .get(30, TimeUnit.SECONDS);
            sources = coldControl
                    .preparePublishedReadSources(coldLedger, coldFactory.namespace(), offloader, objects, budget)
                    .get(60, TimeUnit.SECONDS);
            sources.view().get(30, TimeUnit.SECONDS);
            var missingControl = binding.m4Control();
            var missing = missingControl
                    .prepareColdReadOwner(sources, admission, coldFactory.namespace(), offloader, worker)
                    .get(30, TimeUnit.SECONDS);
            var baseline = budget.usage();
            assertThatThrownBy(() -> missing.recover(worker, event, 2, budget).get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("Enrolled Native read owner requires its original control writer");
            assertThat(budget.usage()).isEqualTo(baseline);
            assertThat(readNativeSelector(responses, coldControl.selectorKey())).isEqualTo(predecessor);
            var recovery = coldControl
                    .prepareColdReadOwner(sources, admission, coldFactory.namespace(), offloader, worker)
                    .get(30, TimeUnit.SECONDS);
            var competing = coldControl
                    .prepareColdReadOwner(sources, admission, coldFactory.namespace(), offloader, worker)
                    .get(30, TimeUnit.SECONDS);
            if (loseReply) {
                responses.arm(coldControl.selectorKey());
                assertThatThrownBy(
                                () -> recovery.recover(worker, event, 2, budget).get(30, TimeUnit.SECONDS))
                        .isInstanceOf(java.util.concurrent.ExecutionException.class);
                assertThat(responses.putLosses.get()).isEqualTo(1);
                responses.clear();
                assertThat(readNativeSelector(responses, coldControl.selectorKey())
                                .ownerEpoch())
                        .isEqualTo(predecessor.ownerEpoch() + 1);
                var unresolved =
                        com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                exact.read(authorityKey)
                                        .toCompletableFuture()
                                        .get(30, TimeUnit.SECONDS)
                                        .orElseThrow()
                                        .canonicalStoredBytes());
                assertThat(unresolved.referenceMutationTickets())
                        .anySatisfy(ticket -> assertThat(ticket.referenceKind())
                                .isEqualTo(
                                        com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                                .MANIFEST_SELECTED));
                assertThat(budget.usage().owners()).isEqualTo(baseline.owners() + 1);
            }
            recovered = recovery.recover(worker, event, 2, budget).get(30, TimeUnit.SECONDS);
            var active = com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                    exact.read(authorityKey)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow()
                            .canonicalStoredBytes());
            assertThat(active.referenceMutationTickets()).anySatisfy(ticket -> assertThat(ticket.referenceKind())
                    .isEqualTo(
                            com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                    .READ_GENERATION_PIN_OR_OPEN_HANDLE));
            assertThat(recovered.selector().ownerEpoch()).isEqualTo(predecessor.ownerEpoch() + 1);
            var recoveringUsage = budget.usage();
            assertThat(recovery.recover(worker, event, 2, budget).get(30, TimeUnit.SECONDS))
                    .isSameAs(recovered);
            assertThatThrownBy(() -> competing.recover(worker, event, 2, budget).get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("Native recovery publication is unknown or belongs to another attempt");
            assertThat(budget.usage()).isEqualTo(recoveringUsage);
            retained = readOrdinaryEntry(coldLedger, PositionFactory.create(dataLedger, 0))
                    .get(30, TimeUnit.SECONDS);
            assertThat(retained.getData()).containsExactly(payload(0));
            var closing = objectOnlyFloor ? recovered.drainForLogicalTrim(worker) : recovered.fence();
            assertThat(closing).isNotDone();
            var held = com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                    exact.read(authorityKey)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow()
                            .canonicalStoredBytes());
            assertThat(held.referenceMutationTickets()).anySatisfy(ticket -> assertThat(ticket.referenceKind())
                    .isEqualTo(
                            com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                    .READ_GENERATION_PIN_OR_OPEN_HANDLE));
            retained.release();
            retained = null;
            closing.get(30, TimeUnit.SECONDS);
            coldFactory.closeAsync().get(30, TimeUnit.SECONDS);
            coldControl.closeControlWriter().get(30, TimeUnit.SECONDS);
            var settled = com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                    exact.read(authorityKey)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow()
                            .canonicalStoredBytes());
            assertThat(settled.referenceMutationTickets()).isEmpty();
            assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
            if (objectOnlyFloor) {
                verifyObjectOnlyNativeFloors(
                        binding,
                        coldControl,
                        bootstrap,
                        coldMetadata,
                        dataLedger,
                        budget,
                        config(offloader),
                        responses,
                        worker,
                        exact,
                        authorityKey);
            }
            System.out.println("NATIVE_ENROLLED_COLD_READ actualGuardedFactories=true durableNpod=true "
                    + "missingWriterRejected=true actualP1ReadTicket=true entryDrainKeepsTicket=true "
                    + "exactTicketCleanup=true grantReplyUnknown=" + loseReply + " wholeM5Authority=false");
        } finally {
            responses.clear();
            if (retained != null) {
                retained.release();
            }
            if (recovered != null) {
                recovered.fence().get(30, TimeUnit.SECONDS);
            }
            if (oldOwner != null) {
                oldOwner.fence().get(30, TimeUnit.SECONDS);
            }
            if (sources != null) {
                sources.close().get(30, TimeUnit.SECONDS);
            }
            if (coldFactory != null) {
                coldFactory.closeAsync().get(30, TimeUnit.SECONDS);
            }
            if (coldControl != null) {
                coldControl.closeControlWriter().get(30, TimeUnit.SECONDS);
            }
            oldControl.closeControlWriter().get(30, TimeUnit.SECONDS);
            event.shutdown();
            assertThat(event.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static void verifyObjectOnlyNativeFloors(
            PulsarNativeBindingV1 binding,
            PulsarNativeM4ControlV1 control,
            M5BookKeeperNamespaceAuthorityV2 observer,
            MetadataStoreExtended metadata,
            long objectLedger,
            PulsarNativeReadCellBudgetV1 budget,
            ManagedLedgerConfig config,
            SelectorResponseFault responses,
            java.util.concurrent.Executor worker,
            com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1 exact,
            String authorityKey)
            throws Exception {
        var coordinator = binding.logicalTrimCoordinator();
        var attempt = Sha256Digest.hash(
                CanonicalUtf8.fromString("actual-object-only-native-floors").bytes());
        var scan = coordinator
                .beginSourceScan(
                        exact.read(authorityKey)
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS)
                                .orElseThrow(),
                        attempt)
                .toCompletableFuture()
                .get(30, TimeUnit.SECONDS)
                .exactAuthority()
                .orElseThrow();
        var baseline = budget.usage();
        try {
            var selected = control.coordinator().readSelector().orElseThrow();
            var storedMap = control.readSelectedSourceMap().get(30, TimeUnit.SECONDS);
            var chain = storedMap.floorChain(binding);
            try (var source = PulsarNativeCursorFloorScanV3.capture(
                            binding, metadata, observer, scan, budget, config, worker)
                    .get(30, TimeUnit.SECONDS)) {
                assertThat(source.cursorCount()).isPositive();
                assertThat(source.coverage().chain()).anySatisfy(row -> {
                    assertThat(row.ledgerId()).isEqualTo(objectLedger);
                    assertThat(row)
                            .isEqualTo(chain.stream()
                                    .filter(actual -> actual.ledgerId() == objectLedger)
                                    .findFirst()
                                    .orElseThrow());
                });
                assertThat(source.floor().pulsarSafeFloor())
                        .contains(new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                                objectLedger, 0));
                var generation = control.captureGenerationFloor(scan, source.coverage(), budget, worker)
                        .get(30, TimeUnit.SECONDS);
                assertThat(generation.observations())
                        .anySatisfy(row -> assertThat(row.authority().key())
                                .isEqualTo(control.sourceMapKey(selected.selectedViewSha256())));
            }
            assertThat(budget.usage()).isEqualTo(baseline);
            responses.holdOneGet(control.sourceMapKey(selected.selectedViewSha256()));
            var changing =
                    PulsarNativeCursorFloorScanV3.capture(binding, metadata, observer, scan, budget, config, worker);
            assertThat(responses.heldReadEntered.await(30, TimeUnit.SECONDS)).isTrue();
            try {
                var before = exact.read(control.selectorKey())
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS)
                        .orElseThrow();
                exact.compareAndSet(Optional.of(before), before.key(), before.canonicalStoredBytes())
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS);
                var after = exact.read(before.key())
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS)
                        .orElseThrow();
                assertThat(after.canonicalStoredBytes()).isEqualTo(before.canonicalStoredBytes());
                assertThat(after.metadataVersion()).isNotEqualTo(before.metadataVersion());
            } finally {
                responses.heldReadRelease.complete(null);
            }
            assertThatThrownBy(() -> changing.get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(IllegalStateException.class);
            assertThat(budget.usage()).isEqualTo(baseline);
            responses.clear();
            responses.selector = control.sourceMapKey(selected.selectedViewSha256());
            responses.blockedReads.set(true);
            assertThatThrownBy(() -> PulsarNativeCursorFloorScanV3.capture(
                                    binding, metadata, observer, scan, budget, config, worker)
                            .get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(TimeoutException.class);
            assertThat(budget.usage()).isEqualTo(baseline);
            System.out.println("NATIVE_OBJECT_ONLY_FLOORS actualSelectedNpsm=true originalBkMissing=true "
                    + "actualCursorPosition=true actualGenerationFloor=true sameByteSelectorRejected=true "
                    + "sourceMapUnknownVeto=true physicalDeleteAuthority=false wholeM5Authority=false");
        } finally {
            responses.clear();
            coordinator.abortSourceScan(scan, attempt).toCompletableFuture().get(30, TimeUnit.SECONDS);
        }
    }

    private static void verifyPostTrimReadOwner(
            PulsarNativeBindingV1 binding,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            PhysicalResourceIdV2.Namespace objects,
            PulsarNativeManagedLedgerFactoryV2 factory,
            ManagedLedgerImpl ledger,
            PulsarNativeReadCellBudgetV1 budget,
            SelectorResponseFault metadata,
            com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1 registry,
            com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1 authority,
            String authorityKey,
            com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorSnapshotV1 snapshot,
            java.util.concurrent.Executor executor,
            boolean missingWriter)
            throws Exception {
        var control = binding.m4Control();
        bookKeeper = factory.namespace();
        if (!missingWriter) {
            control.installControlWriter(registry, authority, authorityKey, budget)
                    .get(30, TimeUnit.SECONDS);
        }
        var view = preparedView(binding, ledger, bookKeeper, offloader, objects, budget, false);
        var floorChain = view.materializationChain();
        var floorDirectory = com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.of(
                binding.bindingIdentity(),
                view.sha256(),
                floorChain,
                floorChain.stream()
                        .filter(row -> !row.active() && row.entryEndExclusive() > 0)
                        .map(row -> new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.EntryRange(
                                row.ledgerId(), 0, row.entryEndExclusive()))
                        .toList());
        var capsule = Path.of(System.getProperty("nereus.m5.nativeM4AdmissionFile"));
        var capsuleSha = Sha256Digest.copyOf(
                java.util.HexFormat.of().parseHex(System.getProperty("nereus.m5.nativeM4AdmissionSha256")));
        var admission = PulsarNativeM4DrainAdmissionV1.load(capsule, capsuleSha);
        var event = Executors.newSingleThreadExecutor();
        PulsarNativeM4ReadOwnerV1 owner = null;
        Entry retained = null;
        try {
            if (missingWriter) {
                var originalSdk = bookKeeper;
                assertThatThrownBy(() -> control.createReadOwner(
                                        view, admission, originalSdk, offloader, executor, event, 2, budget)
                                .get(30, TimeUnit.SECONDS))
                        .hasRootCauseMessage("Enrolled Native read owner requires its original control writer");
                var exact =
                        new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(metadata);
                assertThat(exact.read(control.selectorKey())
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS))
                        .isEmpty();
                System.out.println("NATIVE_READ_MISSING_CONTROL_WRITER actualEnrolledP1=true "
                        + "committedTrim=true durableM4GrantAbsent=true wholeM5Authority=false");
                return;
            }
            owner = control.createReadOwner(view, admission, bookKeeper, offloader, executor, event, 2, budget)
                    .get(30, TimeUnit.SECONDS);
            var exact = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(metadata);
            var pinned = exact.read(authorityKey)
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            assertThat(com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                    pinned.canonicalStoredBytes())
                            .referenceMutationTickets())
                    .anyMatch(ticket -> ticket.referenceKind()
                            == com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                    .READ_GENERATION_PIN_OR_OPEN_HANDLE);
            assertThat(binding.logicalTrimCoordinator()
                            .fence(pinned, snapshot, Sha256Digest.hash(CanonicalBytes.copyOf(new byte[] {75})))
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .outcome())
                    .isEqualTo(
                            com.nereusstream.storage.object.retention.M5PulsarLogicalTrimCoordinatorV2.Outcome.RETAIN);
            long first = snapshot.pulsarContext()
                    .orElseThrow()
                    .coverage()
                    .chain()
                    .get(0)
                    .ledgerId();
            long floor = snapshot.minimumPulsarFloor().ledgerId();
            assertThatThrownBy(() -> readOrdinaryEntry(ledger, PositionFactory.create(first, 0))
                            .get(30, TimeUnit.SECONDS))
                    .hasMessageContaining("precedes committed logical trim");
            assertThatThrownBy(() -> readOrdinaryEntry(ledger, PositionFactory.create(floor, 0))
                            .get(30, TimeUnit.SECONDS))
                    .hasMessageContaining("precedes committed logical trim");
            retained =
                    readOrdinaryEntry(ledger, PositionFactory.create(floor, 1)).get(30, TimeUnit.SECONDS);
            assertThat(retained.getData()).containsExactly(payload(4));
            var originalSelector = exact.read(control.selectorKey())
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            var originalProtections = new java.util.LinkedHashMap<
                    String,
                    Optional<com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue>>();
            for (var protection : owner.fallbackSources()) {
                String protectionKey = control.protectionKey(protection);
                originalProtections.put(
                        protectionKey,
                        exact.read(protectionKey).toCompletableFuture().get(30, TimeUnit.SECONDS));
            }
            metadata.holdOneGet(control.ownerDrainKey(owner.selector()));
            var draining = owner.drainForLogicalTrim(executor);
            assertThatThrownBy(() -> draining.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
            var duringDrain = exact.read(authorityKey)
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            assertThat(com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                    duringDrain.canonicalStoredBytes())
                            .referenceMutationTickets())
                    .anyMatch(ticket -> ticket.referenceKind()
                            == com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                    .READ_GENERATION_PIN_OR_OPEN_HANDLE);
            retained.release();
            retained = null;
            assertThat(metadata.heldReadEntered.await(30, TimeUnit.SECONDS)).isTrue();
            var proofCharge = budget.usage();
            assertThat(draining).isNotDone();
            metadata.heldReadRelease.complete(null);
            draining.get(30, TimeUnit.SECONDS);
            assertThat(proofCharge.owners()).isEqualTo(budget.usage().owners() + 1);
            assertThat(proofCharge.decodedBytes()).isGreaterThan(budget.usage().decodedBytes());
            metadata.clear();
            assertThat(exact.read(control.selectorKey()).toCompletableFuture().get(30, TimeUnit.SECONDS))
                    .contains(originalSelector);
            for (var protection : originalProtections.entrySet()) {
                assertThat(exact.read(protection.getKey()).toCompletableFuture().get(30, TimeUnit.SECONDS))
                        .isEqualTo(protection.getValue());
            }
            assertThat(exact.read(control.ownerDrainKey(owner.selector()))
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS))
                    .isPresent();
            var originalOwner = owner;
            assertThatThrownBy(() -> originalOwner.drainForRecovery(executor).get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("fallback-bearing Native owner requires fused retirement");
            var drained = exact.read(authorityKey)
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            assertThat(com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                    drained.canonicalStoredBytes())
                            .referenceMutationTickets())
                    .noneMatch(ticket -> ticket.referenceKind()
                            == com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                    .READ_GENERATION_PIN_OR_OPEN_HANDLE);
            factory.closeAsync().get(30, TimeUnit.SECONDS);
            var trim = binding.logicalTrimCoordinator();
            var missingWriterAttempt = Sha256Digest.hash(
                    CanonicalUtf8.fromString("missing-legacy-control-writer").bytes());
            var scan = trim.beginSourceScan(
                            exact.read(authorityKey)
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS)
                                    .orElseThrow(),
                            missingWriterAttempt)
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .exactAuthority()
                    .orElseThrow();
            try {
                verifyDirectNativeGenerationFloor(binding, control, scan, floorDirectory, budget, executor, metadata);
                var legacy = binding.m4Control();
                var selector = owner.selector();
                var protectionId =
                        new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1
                                .SourceProtectionIdentity(
                                selector.selectedViewSha256(),
                                selector.sourceGeneration() + 17,
                                selector.readAdmissionEpoch(),
                                selector.sourceGeneration(),
                                selector.capability());
                var protection =
                        new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtection(
                                binding.bindingIdentity(),
                                protectionId,
                                ProtectionState.PROTECTED,
                                Optional.empty(),
                                Optional.empty());
                try {
                    legacy.coordinator().createProtection(protection);
                } catch (IllegalStateException rejected) {
                    assertThat(rejected)
                            .hasMessageContaining("Native enrolled control mutation requires its original P1 writer");
                }
                assertThat(exact.read(legacy.protectionKey(protectionId))
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS))
                        .isEmpty();
                assertThat(exact.read(authorityKey).toCompletableFuture().get(30, TimeUnit.SECONDS))
                        .contains(scan);
            } finally {
                trim.abortSourceScan(scan, missingWriterAttempt)
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS);
            }
            System.out.println("NATIVE_READ_TRIM declaredFloorFixture=true actualReadOwnerC=true "
                    + "belowTrimOrdinaryReadRejected=true atFrontierReadSucceeded=true "
                    + "originalEntryDrainRetainsC=true exactDrainClearsC=true wholeM5Authority=false");
        } finally {
            metadata.heldReadRelease.complete(null);
            metadata.clear();
            if (retained != null) {
                retained.release();
            }
            if (owner != null) {
                owner.fence().get(30, TimeUnit.SECONDS);
            }
            control.closeControlWriter().get(30, TimeUnit.SECONDS);
            event.shutdown();
            assertThat(event.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static void verifyDirectNativeGenerationFloor(
            PulsarNativeBindingV1 binding,
            PulsarNativeM4ControlV1 control,
            com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue scan,
            com.nereusstream.storage.object.materialization.M5PulsarCoverageV2 directory,
            PulsarNativeReadCellBudgetV1 budget,
            java.util.concurrent.Executor executor,
            SelectorResponseFault responses)
            throws Exception {
        var baseline = budget.usage();
        var exact = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(responses);
        var selected = control.coordinator().readSelector().orElseThrow();
        assertThat(exact.read(control.materializationManifestKey(selected.selectedViewSha256()))
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS))
                .isEmpty();
        var last = directory.chain().stream()
                .filter(row -> row.entryEndExclusive() > 0)
                .reduce((left, right) -> right)
                .orElseThrow();
        var expected = new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                last.ledgerId(), last.entryEndExclusive());
        var floor = control.captureGenerationFloor(scan, directory, budget, executor)
                .get(30, TimeUnit.SECONDS);
        assertThat(floor.observations())
                .anySatisfy(row -> assertThat(row.authority().key())
                        .isEqualTo(control.sourceMapKey(selected.selectedViewSha256())));
        for (var row : floor.observations()) {
            assertThat(row.pulsarSafeFloor()).contains(expected);
            var actual = exact.read(row.authority().key())
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            assertThat(actual.metadataVersion()).isEqualTo(row.authority().metadataVersion());
            assertThat(actual.canonicalStoredSha256()).isEqualTo(row.authority().valueSha256());
        }
        assertThat(budget.usage()).isEqualTo(baseline);
        responses.selector = control.ownerDrainKey(selected);
        responses.blockedReads.set(true);
        try {
            assertThatThrownBy(() -> control.captureGenerationFloor(scan, directory, budget, executor)
                            .get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(TimeoutException.class);
            assertThat(budget.usage()).isEqualTo(baseline);
        } finally {
            responses.clear();
        }
        responses.holdOneGet(control.sourceMapKey(selected.selectedViewSha256()));
        var changing = control.captureGenerationFloor(scan, directory, budget, executor);
        assertThat(responses.heldReadEntered.await(30, TimeUnit.SECONDS)).isTrue();
        try {
            var original = exact.read(control.selectorKey())
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            exact.compareAndSet(Optional.of(original), original.key(), original.canonicalStoredBytes())
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS);
            var changed = exact.read(original.key())
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            assertThat(changed.canonicalStoredBytes()).isEqualTo(original.canonicalStoredBytes());
            assertThat(changed.metadataVersion()).isNotEqualTo(original.metadataVersion());
        } finally {
            responses.heldReadRelease.complete(null);
        }
        assertThatThrownBy(() -> changing.get(30, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(IllegalStateException.class);
        assertThat(budget.usage()).isEqualTo(baseline);
        responses.clear();
        System.out.println("NATIVE_DIRECT_GENERATION_FLOOR actualSelectedNpsm=true materializationManifestAbsent=true "
                + "actualNativeVersions=true ownerDrainUnknownVeto=true sameByteSelectorRejected=true "
                + "allFloorAuthority=false wholeM5Authority=false");
    }

    private static void verifyMaterializationRetentionRoots(
            PulsarNativeBindingV1 binding,
            PulsarNativeM4ControlV1 control,
            PulsarNativeReadCellBudgetV1 budget,
            java.util.concurrent.Executor executor,
            SelectorResponseFault fault,
            com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue scan,
            com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationPlan expectedPlan,
            com.nereusstream.storage.api.lifecycle.NativePhysicalNamespaceAuthorityV2... physicalBackends)
            throws Exception {
        var exact = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(fault);
        var baseline = budget.usage();
        com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue memberFact = null;
        try (var roots = control.captureMaterializationTaskRoots(scan, budget, executor, physicalBackends)
                .get(30, TimeUnit.SECONDS)) {
            var historicalRegistry = nativeExpectedWriterRegistry(
                    roots.workerAuthority().orElseThrow().capability());
            var physicalTarget = roots.physicalNamespaces().stream()
                    .flatMap(namespace -> namespace.resources().stream())
                    .flatMap(resource -> resource.storedAuthority().stream())
                    .map(value -> value.resource())
                    .findFirst()
                    .orElseThrow();
            assertThatThrownBy(() -> roots.closedTaskReferences(
                            historicalRegistry,
                            physicalTarget,
                            expectedPlan.sourceCut().coverage().pulsarCoverage().orElseThrow()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("original enrolled Binding/source scan");
            assertThat(roots.otherCellReservations()).isEmpty();
            assertThat(roots.facts()).containsKey(budget.ownerKey());
            assertThat(roots.physicalNamespaces()).hasSize(physicalBackends.length);
            for (var namespace : roots.physicalNamespaces()) {
                assertThat(namespace.resources()).isNotEmpty();
                assertThat(roots.facts()).containsAllEntriesOf(namespace.facts());
                assertThat(namespace.resources()).anySatisfy(resource -> {
                    assertThat(resource.storedAuthority()).isPresent();
                    assertThat(resource.reservation()).isPresent();
                    assertThat(resource.members()).isPresent();
                    assertThat(resource.members().orElseThrow().inventory().members())
                            .anySatisfy(member -> assertThat(member.binding())
                                    .isEqualTo(
                                            expectedPlan.sourceCut().identity().binding()));
                });
                memberFact = namespace.facts().values().stream()
                        .filter(value -> value.key().contains("/v2/physical-members-m5-v2/"))
                        .findFirst()
                        .orElseThrow();
            }
            assertThat(roots.plans()).isNotEmpty().contains(expectedPlan);
            for (var source : expectedPlan.sourceCut().sources()) {
                var references = roots.presentTaskReferences(source.sourceIdentitySha256());
                assertThat(references)
                        .anySatisfy(row -> assertThat(row.authority().key())
                                .isEqualTo(control.materializationTaskKey(expectedPlan.taskIdSha256())));
                assertThat(references).allSatisfy(row -> {
                    assertThat(row.kind())
                            .isEqualTo(
                                    com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                            .MATERIALIZATION_OR_COMPACTION_TASK);
                    assertThat(row.disposition())
                            .isEqualTo(
                                    com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                            .ReferenceDispositionV1.PRESENT);
                    assertThat(row.enumerationComplete()).isFalse();
                    assertThat(roots.facts().get(row.authority().key()).metadataVersion())
                            .isEqualTo(row.authority().metadataVersion());
                });
            }
            assertThat(roots.physicalNamespaces().stream()
                            .flatMap(namespace -> namespace.births().stream())
                            .filter(birth -> birth.worker().taskIdSha256().equals(expectedPlan.taskIdSha256()))
                            .flatMap(birth -> birth.resource().stream())
                            .toList())
                    .isNotEmpty()
                    .allSatisfy(resource -> {
                        var references = roots.presentTaskReferences(resource.sha256());
                        assertThat(references)
                                .anySatisfy(
                                        row -> assertThat(row.authority().key()).contains("/v3/object-birth-credits/"));
                        assertThat(references).allSatisfy(row -> assertThat(row.disposition())
                                .isEqualTo(
                                        com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                                .ReferenceDispositionV1.PRESENT));
                    });
            assertThatThrownBy(() -> roots.presentTaskReferences(
                            Sha256Digest.hash(CanonicalUtf8.fromString("unresolved-native-task-target")
                                    .bytes())))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("absence requires closed lifecycle/root handoff");
            assertThat(roots.nativeInputs()).isNotEmpty();
            assertThat(roots.facts()).containsKey(control.materializationTaskKey(expectedPlan.taskIdSha256()));
            assertThat(roots.facts()).containsEntry(scan.key(), scan);
            assertThat(roots.workerAuthority()).isPresent();
            assertThat(roots.workerAuthority().orElseThrow().binding())
                    .isEqualTo(expectedPlan.sourceCut().identity().binding());
            assertThat(roots.cellAdmission()).isPresent();
            assertThat(roots.cellAdmission().orElseThrow().reservations())
                    .noneMatch(reservation -> reservation.taskIdSha256().equals(expectedPlan.taskIdSha256()));
            assertThat(roots.facts()).containsKey(control.selectorKey());
            assertThat(roots.facts())
                    .containsKey(control.materializationAdmissionKey(
                            expectedPlan.sourceCut().identity().protocolCellSha256()));
            assertThat(budget.usage().owners()).isGreaterThan(baseline.owners());
        }
        assertThat(budget.usage()).isEqualTo(baseline);
        if (memberFact != null) {
            var originalMember = memberFact;
            fault.holdOneGet(originalMember.key());
            var changingMembers = control.captureMaterializationTaskRoots(scan, budget, executor, physicalBackends);
            assertThat(fault.heldReadEntered.await(30, TimeUnit.SECONDS)).isTrue();
            try {
                assertThat(exact.compareAndSet(
                                        Optional.of(originalMember),
                                        originalMember.key(),
                                        originalMember.canonicalStoredBytes())
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS))
                        .isEqualTo(
                                com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome
                                        .APPLIED_EXACT);
            } finally {
                fault.heldReadRelease.complete(null);
            }
            assertThatThrownBy(() -> changingMembers.get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(IllegalStateException.class);
            assertThat(budget.usage()).isEqualTo(baseline);
            fault.clear();
            fault.selector = originalMember.key();
            fault.blockedReads.set(true);
            try {
                assertThatThrownBy(
                                () -> control.captureMaterializationTaskRoots(scan, budget, executor, physicalBackends)
                                        .get(30, TimeUnit.SECONDS))
                        .hasRootCauseInstanceOf(TimeoutException.class);
                assertThat(budget.usage()).isEqualTo(baseline);
            } finally {
                fault.clear();
            }
        }
        String taskKey = control.materializationTaskKey(expectedPlan.taskIdSha256());
        var other = budget.reserveCursorObservation(binding, PulsarNativeCursorMetadataV2.MAX_BYTES);
        other.ready().get(30, TimeUnit.SECONDS);
        var otherUsage = budget.usage();
        try (var roots = control.captureMaterializationTaskRoots(scan, budget, executor, physicalBackends)
                .get(30, TimeUnit.SECONDS)) {
            assertThat(roots.otherCellReservations()).containsOnlyKeys(other.operationId());
            assertThat(roots.otherCellReservations().get(other.operationId())).isEqualTo(otherUsage);
        } finally {
            other.releaseBeforeActor().get(30, TimeUnit.SECONDS);
        }
        assertThat(budget.usage()).isEqualTo(baseline);
        fault.holdOneGet(taskKey);
        var changingCell = control.captureMaterializationTaskRoots(scan, budget, executor, physicalBackends);
        assertThat(fault.heldReadEntered.await(30, TimeUnit.SECONDS)).isTrue();
        try {
            var cellHead = exact.read(budget.ownerKey())
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            assertThat(exact.compareAndSet(Optional.of(cellHead), cellHead.key(), cellHead.canonicalStoredBytes())
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS))
                    .isEqualTo(
                            com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome
                                    .APPLIED_EXACT);
        } finally {
            fault.heldReadRelease.complete(null);
        }
        assertThatThrownBy(() -> changingCell.get(30, TimeUnit.SECONDS))
                .hasRootCauseMessage("Native Cell reservations changed during root capture");
        assertThat(budget.usage()).isEqualTo(baseline);
        fault.clear();
        fault.holdOneGet(taskKey);
        var unknownCell = control.captureMaterializationTaskRoots(scan, budget, executor, physicalBackends);
        assertThat(fault.heldReadEntered.await(30, TimeUnit.SECONDS)).isTrue();
        var retainedUsage = budget.usage();
        fault.selector = budget.ownerKey();
        fault.blockedReads.set(true);
        fault.heldReadRelease.complete(null);
        try {
            assertThatThrownBy(() -> unknownCell.get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(TimeoutException.class);
            assertThat(budget.usage()).isEqualTo(retainedUsage);
            assertThat(retainedUsage.owners()).isGreaterThan(baseline.owners());
        } finally {
            fault.clear();
        }
        budget.reconcileUndeliveredSdkCleanup().get(30, TimeUnit.SECONDS);
        assertThat(budget.usage()).isEqualTo(baseline);
        var originalTask = exact.read(taskKey)
                .toCompletableFuture()
                .get(30, TimeUnit.SECONDS)
                .orElseThrow();
        fault.holdOneGet(taskKey);
        var changingRoots = control.captureMaterializationTaskRoots(scan, budget, executor, physicalBackends);
        assertThat(fault.heldReadEntered.await(30, TimeUnit.SECONDS)).isTrue();
        try {
            assertThat(exact.compareAndSet(Optional.of(originalTask), taskKey, originalTask.canonicalStoredBytes())
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS))
                    .isEqualTo(
                            com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome
                                    .APPLIED_EXACT);
        } finally {
            fault.heldReadRelease.complete(null);
        }
        assertThatThrownBy(() -> changingRoots.get(30, TimeUnit.SECONDS))
                .hasRootCauseMessage("Native materialization root inventory changed during capture");
        assertThat(budget.usage()).isEqualTo(baseline);
        fault.clear();
        try (var roots = control.captureMaterializationTaskRoots(scan, budget, executor, physicalBackends)
                .get(30, TimeUnit.SECONDS)) {
            assertThat(roots.facts().get(taskKey).metadataVersion()).isNotEqualTo(originalTask.metadataVersion());
        }
        String admissionKey = control.materializationAdmissionKey(
                expectedPlan.sourceCut().identity().protocolCellSha256());
        var originalAdmission = exact.read(admissionKey)
                .toCompletableFuture()
                .get(30, TimeUnit.SECONDS)
                .orElseThrow();
        fault.holdOneGet(admissionKey);
        var changingAdmission = control.captureMaterializationTaskRoots(scan, budget, executor, physicalBackends);
        assertThat(fault.heldReadEntered.await(30, TimeUnit.SECONDS)).isTrue();
        try {
            assertThat(exact.compareAndSet(
                                    Optional.of(originalAdmission),
                                    admissionKey,
                                    originalAdmission.canonicalStoredBytes())
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS))
                    .isEqualTo(
                            com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome
                                    .APPLIED_EXACT);
        } finally {
            fault.heldReadRelease.complete(null);
        }
        assertThatThrownBy(() -> changingAdmission.get(30, TimeUnit.SECONDS))
                .hasRootCauseMessage("Native materialization worker/admission version changed during capture");
        assertThat(budget.usage()).isEqualTo(baseline);
        fault.clear();
        var currentAdmission = exact.read(admissionKey)
                .toCompletableFuture()
                .get(30, TimeUnit.SECONDS)
                .orElseThrow();
        assertThat(currentAdmission.canonicalStoredBytes()).isEqualTo(originalAdmission.canonicalStoredBytes());
        assertThat(currentAdmission.metadataVersion()).isNotEqualTo(originalAdmission.metadataVersion());
        fault.selector = control.selectorKey();
        fault.blockedReads.set(true);
        try {
            assertThatThrownBy(() -> control.captureMaterializationTaskRoots(scan, budget, executor, physicalBackends)
                            .get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(TimeoutException.class);
            assertThat(budget.usage()).isEqualTo(baseline);
        } finally {
            fault.clear();
        }
        fault.selector = taskKey;
        fault.blockedReads.set(true);
        try {
            assertThatThrownBy(() -> control.captureMaterializationTaskRoots(scan, budget, executor, physicalBackends)
                            .get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(TimeoutException.class);
            assertThat(budget.usage()).isEqualTo(baseline);
        } finally {
            fault.clear();
        }
    }

    private static void verifyActualGenerationFloor(
            PulsarNativeBindingV1 binding,
            PulsarNativeM4ControlV1 control,
            PulsarNativeReadCellBudgetV1 budget,
            java.util.concurrent.Executor executor,
            SelectorResponseFault fault,
            M5BookKeeperNamespaceAuthorityV2 observer,
            ManagedLedgerConfig config,
            com.nereusstream.storage.api.lifecycle.NativePhysicalNamespaceAuthorityV2 objectBackend)
            throws Exception {
        var exact = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(fault);
        String key = new OxiaV2AuthorityKeys(binding.authorityRoot())
                .aggregateKey(binding.authority().aggregate().binding().incarnationIdentity());
        var selected = control.readSelectedMaterialization(executor).get(30, TimeUnit.SECONDS);
        var directory = selected.readMap()
                .plan()
                .sourceCut()
                .coverage()
                .pulsarCoverage()
                .orElseThrow();
        var coordinator = binding.logicalTrimCoordinator();
        var attempt = Sha256Digest.hash(
                CanonicalUtf8.fromString("actual-generation-floor").bytes());
        var scan = coordinator
                .beginSourceScan(
                        exact.read(key)
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS)
                                .orElseThrow(),
                        attempt)
                .toCompletableFuture()
                .get(30, TimeUnit.SECONDS)
                .exactAuthority()
                .orElseThrow();
        var baseline = budget.usage();
        try {
            verifyMaterializationRetentionRoots(
                    binding,
                    control,
                    budget,
                    executor,
                    fault,
                    scan,
                    selected.readMap().plan(),
                    observer,
                    objectBackend);
            verifyMappedCursorFloor(binding, control, observer, scan, budget, config, executor, fault);
            var floor = control.captureGenerationFloor(scan, directory, budget, executor)
                    .get(30, TimeUnit.SECONDS);
            assertThat(floor.floorClass())
                    .isEqualTo(
                            com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1
                                    .GENERATION_READ);
            assertThat(floor.observations()).hasSizeGreaterThan(5);
            String selectorKey = control.selectorKey();
            String protections = selectorKey.substring(0, selectorKey.lastIndexOf('/')) + "/protections/";
            var actualProtections = fault.list(protections, protections + "/").get(30, TimeUnit.SECONDS);
            assertThat(actualProtections).isNotEmpty();
            assertThat(floor.observations().stream()
                            .map(row -> row.authority().key())
                            .toList())
                    .containsAll(actualProtections);
            assertThat(floor.observations())
                    .anySatisfy(row -> assertThat(row.authority().key()).isEqualTo(scan.key()));
            var lastLedger = directory.chain().stream()
                    .filter(value -> value.entryEndExclusive() > 0)
                    .reduce((left, right) -> right)
                    .orElseThrow();
            var safe = new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                    lastLedger.ledgerId(), lastLedger.entryEndExclusive());
            for (var row : floor.observations()) {
                assertThat(row.pulsarSafeFloor()).contains(safe);
                assertThat(row.constraining()).isTrue();
                var actual = exact.read(row.authority().key())
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS)
                        .orElseThrow();
                assertThat(actual.metadataVersion()).isEqualTo(row.authority().metadataVersion());
                assertThat(actual.canonicalStoredSha256())
                        .isEqualTo(row.authority().valueSha256());
            }
            assertThat(budget.usage()).isEqualTo(baseline);
            fault.selector = control.ownerDrainKey(selected.selector());
            fault.blockedReads.set(true);
            try {
                assertThatThrownBy(() -> control.captureGenerationFloor(scan, directory, budget, executor)
                                .get(30, TimeUnit.SECONDS))
                        .hasRootCauseMessage("actual selector observation unavailable");
                assertThat(budget.usage()).isEqualTo(baseline);
            } finally {
                fault.clear();
            }
            String manifestKey = control.materializationManifestKey(selected.materializedManifestSha256());
            fault.holdOneGet(manifestKey);
            var changing = control.captureGenerationFloor(scan, directory, budget, executor);
            assertThat(fault.heldReadEntered.await(30, TimeUnit.SECONDS)).isTrue();
            try {
                var current = exact.read(control.selectorKey())
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS)
                        .orElseThrow();
                exact.compareAndSet(Optional.of(current), current.key(), current.canonicalStoredBytes())
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS);
            } finally {
                fault.heldReadRelease.complete(null);
            }
            assertThatThrownBy(() -> changing.get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("Native selected materialization native version changed during capture");
            assertThat(budget.usage()).isEqualTo(baseline);
            fault.clear();
            fault.holdOneGet(manifestKey);
            var cancelled = control.captureGenerationFloor(scan, directory, budget, executor);
            assertThat(fault.heldReadEntered.await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(cancelled.cancel(false)).isTrue();
            assertThat(budget.usage().owners()).isEqualTo(baseline.owners() + 1);
            var closing = budget.closeCell();
            assertThat(closing).isNotDone();
            fault.heldReadRelease.complete(null);
            closing.get(30, TimeUnit.SECONDS);
            assertThat(budget.usage()).isEqualTo(baseline);
            System.out.println("NATIVE_GENERATION_FLOOR actualSelectedManifest=true actualNativeVersions=true "
                    + "ownerDrainReadUnknownVeto=true sameByteSelectorChangeRejected=true "
                    + "observerCancellationRetainsCell=true "
                    + "allFloorAuthority=false wholeM5Authority=false");
        } finally {
            fault.clear();
            coordinator.abortSourceScan(scan, attempt).toCompletableFuture().get(30, TimeUnit.SECONDS);
        }
    }

    private static void verifyMappedColdFactoryAfterOriginalRemoval(
            PulsarNativeBindingV1 binding,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            NereusPulsarLedgerOffloaderV1 offloader,
            java.util.concurrent.Executor worker,
            SelectorResponseFault responses,
            Map<org.apache.bookkeeper.mledger.Position, byte[]> expected,
            boolean partial)
            throws Exception {
        var budget = readBudget(binding, 6);
        var control = binding.m4Control();
        var event = Executors.newSingleThreadExecutor();
        PulsarNativeManagedLedgerFactoryV2 factory = null;
        PulsarNativeSourcePreparationV1 sources = null;
        PulsarNativeM4ReadOwnerV1 owner = null;
        try (var metadata = metadata()) {
            if (partial) {
                var selected = control.readSelectedMaterialization(worker).get(30, TimeUnit.SECONDS);
                var directory = selected.readMap().predecessor().floorDirectory(binding);
                var retainedObject =
                        directory.objectOnly().entrySet().stream().findFirst().orElseThrow();
                assertThat(selected.readMap()
                                .fullyReplaces(
                                        retainedObject.getKey(),
                                        retainedObject.getValue().lastEntry()))
                        .isFalse();
                try (var independent = client(null)) {
                    var replacement = independent.putObject(
                            PutObjectRequest.builder()
                                    .bucket(directory
                                            .objects()
                                            .physicalNamespace()
                                            .containerIdentity()
                                            .value())
                                    .key(retainedObject
                                            .getValue()
                                            .identity()
                                            .root()
                                            .key())
                                    .build(),
                            software.amazon.awssdk.core.sync.RequestBody.fromBytes(new byte[] {9, 7, 3}));
                    assertThat("version:" + replacement.versionId())
                            .isNotEqualTo(
                                    retainedObject.getValue().identity().root().version());
                }
                bootstrap.borrowClient().deleteLedger(retainedObject.getKey());
                assertThatThrownBy(() -> bootstrap
                                .borrowClient()
                                .getLedgerManager()
                                .readLedgerMetadata(retainedObject.getKey())
                                .get(30, TimeUnit.SECONDS))
                        .hasRootCauseInstanceOf(
                                org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsOnMetadataServerException
                                        .class);
            }
            // Fully rewritten sources and retained Object sources must use their actual selected immutable versions.
            factory = guardedMappedFactory(binding, metadata, bootstrap, offloader, budget, worker, responses);
            var ledger = factory.openLedger(config(offloader));
            if (partial) {
                ledger.openCursor("actual-partial-map-floor", InitialPosition.Earliest);
            }
            var capability = new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding(
                    1, bootstrap.capability().configurationDigest());
            var registry = nativeExpectedWriterRegistry(capability);
            var exact =
                    new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(responses);
            var authority =
                    new com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1(exact);
            String key = new OxiaV2AuthorityKeys(binding.authorityRoot())
                    .aggregateKey(binding.authority().aggregate().binding().incarnationIdentity());
            control.installControlWriter(registry, authority, key, budget).get(30, TimeUnit.SECONDS);
            var selected = control.readSelectedMaterialization(worker).get(30, TimeUnit.SECONDS);
            sources = control.prepareMaterializedReadSources(
                            ledger,
                            factory.namespace(),
                            offloader,
                            selected.providerProfile().physicalBinding().physicalNamespace(),
                            budget,
                            worker)
                    .get(60, TimeUnit.SECONDS);
            var capsule = Path.of(System.getProperty("nereus.m5.nativeM4AdmissionFile"));
            var admission = PulsarNativeM4DrainAdmissionV1.load(
                    capsule,
                    Sha256Digest.copyOf(java.util.HexFormat.of()
                            .parseHex(System.getProperty("nereus.m5.nativeM4AdmissionSha256"))));
            var recovery = control.prepareMaterializedReadOwner(
                            sources, admission, factory.namespace(), offloader, worker)
                    .get(60, TimeUnit.SECONDS);
            owner = recovery.recover(worker, event, 2, budget).get(60, TimeUnit.SECONDS);
            for (var item : expected.entrySet()) {
                var entry = readOrdinaryEntry(ledger, item.getKey()).get(30, TimeUnit.SECONDS);
                try {
                    assertThat(entry.getData()).isEqualTo(item.getValue());
                } finally {
                    entry.release();
                }
            }
            owner.drainForLogicalTrim(worker).get(30, TimeUnit.SECONDS);
            factory.closeAsync().get(30, TimeUnit.SECONDS);
            control.closeControlWriter().get(30, TimeUnit.SECONDS);
            assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
            assertThat(com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                    exact.read(key)
                                            .toCompletableFuture()
                                            .get(30, TimeUnit.SECONDS)
                                            .orElseThrow()
                                            .canonicalStoredBytes())
                            .referenceMutationTickets())
                    .isEmpty();
            if (partial) {
                verifyPartialMappedFloors(
                        binding,
                        control,
                        bootstrap,
                        metadata,
                        budget,
                        config(offloader),
                        responses,
                        worker,
                        exact,
                        key,
                        false);
                var chain = selected.readMap()
                        .plan()
                        .sourceCut()
                        .coverage()
                        .pulsarCoverage()
                        .orElseThrow()
                        .chain();
                var retained = chain.get(1);
                assertThat(retained.active()).isFalse();
                assertThat(selected.readMap().fullyReplaces(retained.ledgerId(), retained.entryEndExclusive() - 1))
                        .isFalse();
                var beforeMissing = control.readSelectedMaterialization(worker).get(30, TimeUnit.SECONDS);
                bootstrap.borrowClient().deleteLedger(retained.ledgerId());
                verifyPartialMappedFloors(
                        binding,
                        control,
                        bootstrap,
                        metadata,
                        budget,
                        config(offloader),
                        responses,
                        worker,
                        exact,
                        key,
                        true);
                assertThatThrownBy(() -> guardedMappedFactory(
                                binding, metadata, bootstrap, offloader, budget, worker, responses))
                        .hasRootCauseInstanceOf(
                                org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsOnMetadataServerException
                                        .class);
                assertThat(control.readSelectedMaterialization(worker)
                                .get(30, TimeUnit.SECONDS)
                                .selector())
                        .isEqualTo(beforeMissing.selector());
                assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0));
                assertThat(com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                        exact.read(key)
                                                .toCompletableFuture()
                                                .get(30, TimeUnit.SECONDS)
                                                .orElseThrow()
                                                .canonicalStoredBytes())
                                .referenceMutationTickets())
                        .isEmpty();
                System.out.println("NATIVE_PARTIAL_MAPPED_COLD_FACTORY actualPublishedPartialCut=true "
                        + "ordinaryRewrittenAndRetainedReads=true retainedMissingBkRejects=true "
                        + "selectorPreserved=true allOriginalCAndCellCleared=true wholeM5Authority=false");
                return;
            }
            System.out.println("NATIVE_MAPPED_COLD_FACTORY actualSelectedM5Graph=true originalMappedBkMissing=true "
                    + "guardedFactory=true ordinaryReadsPreserved=true allOriginalCAndCellCleared=true "
                    + "physicalDeleteAuthority=false wholeM5Authority=false");
        } finally {
            responses.clear();
            if (owner != null) {
                owner.fence().get(30, TimeUnit.SECONDS);
            }
            if (sources != null) {
                sources.close().get(30, TimeUnit.SECONDS);
            }
            if (factory != null) {
                factory.closeAsync().get(30, TimeUnit.SECONDS);
            }
            control.closeControlWriter().get(30, TimeUnit.SECONDS);
            budget.closeCell().get(30, TimeUnit.SECONDS);
            event.shutdown();
            assertThat(event.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static void verifyPartialMappedFloors(
            PulsarNativeBindingV1 binding,
            PulsarNativeM4ControlV1 control,
            M5BookKeeperNamespaceAuthorityV2 observer,
            MetadataStoreExtended metadata,
            PulsarNativeReadCellBudgetV1 budget,
            ManagedLedgerConfig config,
            SelectorResponseFault responses,
            java.util.concurrent.Executor worker,
            com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1 exact,
            String authorityKey,
            boolean missingRequired)
            throws Exception {
        var coordinator = binding.logicalTrimCoordinator();
        var attempt = Sha256Digest.hash(CanonicalUtf8.fromString("actual-partial-map-floors-" + missingRequired)
                .bytes());
        var scan = coordinator
                .beginSourceScan(
                        exact.read(authorityKey)
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS)
                                .orElseThrow(),
                        attempt)
                .toCompletableFuture()
                .get(30, TimeUnit.SECONDS)
                .exactAuthority()
                .orElseThrow();
        var baseline = budget.usage();
        try {
            if (missingRequired) {
                assertThatThrownBy(() -> PulsarNativeCursorFloorScanV3.capture(
                                        binding, metadata, observer, scan, budget, config, worker)
                                .get(30, TimeUnit.SECONDS))
                        .hasRootCauseInstanceOf(
                                org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsOnMetadataServerException
                                        .class);
            } else {
                var selected = control.readSelectedMaterialization(worker).get(30, TimeUnit.SECONDS);
                var original = selected.readMap().originalNativeDirectory(binding);
                long objectLedger =
                        original.objectOnly().keySet().stream().findFirst().orElseThrow();
                try (var source = PulsarNativeCursorFloorScanV3.capture(
                                binding, metadata, observer, scan, budget, config, worker)
                        .get(30, TimeUnit.SECONDS)) {
                    assertThat(source.cursorCount()).isPositive();
                    assertThat(source.floor().pulsarSafeFloor())
                            .contains(new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                                    objectLedger, 0));
                    var generation = control.captureGenerationFloor(scan, source.coverage(), budget, worker)
                            .get(30, TimeUnit.SECONDS);
                    assertThat(generation.observations())
                            .anySatisfy(row -> assertThat(row.authority().key())
                                    .isEqualTo(control.materializationManifestKey(
                                            selected.selector().selectedViewSha256())));
                    for (var row : generation.observations()) {
                        var actual = exact.read(row.authority().key())
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS)
                                .orElseThrow();
                        assertThat(actual.metadataVersion())
                                .isEqualTo(row.authority().metadataVersion());
                        assertThat(actual.canonicalStoredSha256())
                                .isEqualTo(row.authority().valueSha256());
                    }
                }
            }
            assertThat(budget.usage()).isEqualTo(baseline);
            System.out.println("NATIVE_PARTIAL_MAPPED_FLOORS actualSelectedM5Graph=true "
                    + "actualCursorAndGenerationFloors=" + !missingRequired
                    + " missingRequiredBkVeto=" + missingRequired
                    + " originalOffloadedBkMissing=true allFloorAuthority=false wholeM5Authority=false");
        } finally {
            responses.clear();
            coordinator.abortSourceScan(scan, attempt).toCompletableFuture().get(30, TimeUnit.SECONDS);
        }
    }

    private static void verifyMappedCursorFloor(
            PulsarNativeBindingV1 binding,
            PulsarNativeM4ControlV1 control,
            M5BookKeeperNamespaceAuthorityV2 observer,
            com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue scan,
            PulsarNativeReadCellBudgetV1 budget,
            ManagedLedgerConfig config,
            java.util.concurrent.Executor executor,
            SelectorResponseFault fault)
            throws Exception {
        var selected = control.readSelectedMaterialization(executor).get(30, TimeUnit.SECONDS);
        var baseline = budget.usage();
        try (var metadata = metadata()) {
            String parent = "/managed-ledgers/" + binding.managedLedgerName();
            var stored = metadata.get(parent).get(30, TimeUnit.SECONDS).orElseThrow();
            var info = new org.apache.bookkeeper.mledger.impl.MetaStoreImpl(null, null)
                    .parseManagedLedgerInfo(stored.getValue(), 8_388_608);
            var directory = selected.readMap()
                    .plan()
                    .sourceCut()
                    .coverage()
                    .pulsarCoverage()
                    .orElseThrow();
            var mapped = directory.chain().stream()
                    .filter(row -> !row.active()
                            && row.entryEndExclusive() > 0
                            && selected.readMap().fullyReplaces(row.ledgerId(), row.entryEndExclusive() - 1)
                            && info.getLedgerInfosList().stream()
                                    .anyMatch(actual -> actual.getLedgerId() == row.ledgerId()))
                    .findFirst()
                    .orElseThrow();
            // Controlled physical absence after actual mapped reads, not a production delete-authority receipt.
            observer.borrowClient().deleteLedger(mapped.ledgerId());
            assertThatThrownBy(() -> observer.borrowClient()
                            .getLedgerManager()
                            .readLedgerMetadata(mapped.ledgerId())
                            .get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(
                            org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsOnMetadataServerException
                                    .class);
            try (var source = PulsarNativeCursorFloorScanV3.capture(
                            binding, metadata, observer, scan, budget, config, executor)
                    .get(30, TimeUnit.SECONDS)) {
                assertThat(source.cursorCount()).isPositive();
                assertThat(source.coverage().chain().stream()
                                .map(row -> row.ledgerId())
                                .toList())
                        .containsExactlyElementsOf(info.getLedgerInfosList().stream()
                                .map(row -> row.getLedgerId())
                                .toList());
                assertThat(source.coverage().chain()).anySatisfy(row -> {
                    assertThat(row.ledgerId()).isEqualTo(mapped.ledgerId());
                    assertThat(row.entryEndExclusive()).isEqualTo(mapped.entryEndExclusive());
                    assertThat(row.sourceIdentitySha256())
                            .isEqualTo(PulsarNativeMaterializationReadMapV2.mappedLedgerRoot(
                                    selected.selector().selectedViewSha256(), mapped.ledgerId()));
                });
                assertThat(source.floor().pulsarSafeFloor()).isPresent();
            }
            assertThat(budget.usage()).isEqualTo(baseline);
            fault.holdOneGet(control.materializationManifestKey(selected.materializedManifestSha256()));
            var changing =
                    PulsarNativeCursorFloorScanV3.capture(binding, metadata, observer, scan, budget, config, executor);
            assertThat(fault.heldReadEntered.await(30, TimeUnit.SECONDS)).isTrue();
            try {
                var exact =
                        new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(fault);
                var current = exact.read(control.selectorKey())
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS)
                        .orElseThrow();
                exact.compareAndSet(Optional.of(current), current.key(), current.canonicalStoredBytes())
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS);
                var changed = exact.read(current.key())
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS)
                        .orElseThrow();
                assertThat(changed.canonicalStoredBytes()).isEqualTo(current.canonicalStoredBytes());
                assertThat(changed.metadataVersion()).isNotEqualTo(current.metadataVersion());
            } finally {
                fault.heldReadRelease.complete(null);
            }
            assertThatThrownBy(() -> changing.get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(IllegalStateException.class);
            assertThat(budget.usage()).isEqualTo(baseline);
            fault.clear();
            System.out.println("NATIVE_MAPPED_CURSOR_FLOOR actualSelectedGraph=true missingOriginalBk=true "
                    + "sameByteSelectorChangeRejected=true physicalDeleteAuthority=false wholeM5Authority=false");
        } finally {
            fault.clear();
        }
    }

    private static void verifyActualCursorFloor(
            PulsarNativeBindingV1 binding,
            M5BookKeeperNamespaceAuthorityV2 observer,
            PulsarNativeManagedLedgerFactoryV2 factory,
            ManagedLedgerImpl ledger,
            MetadataStoreExtended metadata,
            com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1 exact,
            String key,
            PulsarNativeReadCellBudgetV1 budget,
            java.util.concurrent.Executor worker,
            SelectorResponseFault responses,
            boolean loseReplies)
            throws Exception {
        var config = ledger.getConfig();
        var cursor = (org.apache.bookkeeper.mledger.impl.ManagedCursorImpl)
                ledger.openCursor(SUBSCRIPTION, InitialPosition.Earliest);
        var actualChain = ledger.getLedgersInfo().values().stream().toList();
        long floorLedger = actualChain.get(1).getLedgerId();
        cursor.markDelete(org.apache.bookkeeper.mledger.PositionFactory.create(floorLedger, 0));
        cursor.markDelete(org.apache.bookkeeper.mledger.PositionFactory.create(floorLedger, 1));
        // A real noncontiguous delete keeps the final authoritative checkpoint in BK on cursor close.
        config.setMaxUnackedRangesToPersistInMetadataStore(0);
        cursor.delete(ledger.getLastConfirmedEntry());
        long checkpointLedger = cursor.getCursorLedger();
        assertThat(checkpointLedger).isNotNegative();
        var coordinator = binding.logicalTrimCoordinator();
        var attempt = Sha256Digest.hash(
                CanonicalUtf8.fromString("actual-native-cursor-floor").bytes());
        var active =
                exact.read(key).toCompletableFuture().get(30, TimeUnit.SECONDS).orElseThrow();
        assertThat(coordinator
                        .beginSourceScan(active, attempt)
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS)
                        .outcome())
                .isEqualTo(
                        com.nereusstream.storage.object.retention.M5SingleAuthorityLogicalTrimCoordinatorV2.Outcome
                                .RETAIN);
        // Original SDK closure, not a quiet-write observation, releases the lifetime recovery ticket.
        factory.closeAsync().get(30, TimeUnit.SECONDS);
        var open =
                exact.read(key).toCompletableFuture().get(30, TimeUnit.SECONDS).orElseThrow();
        var began =
                coordinator.beginSourceScan(open, attempt).toCompletableFuture().get(30, TimeUnit.SECONDS);
        assertThat(began.outcome())
                .isEqualTo(
                        com.nereusstream.storage.object.retention.M5SingleAuthorityLogicalTrimCoordinatorV2.Outcome
                                .APPLIED_EXACT);
        var scan = began.exactAuthority().orElseThrow();
        var baselineUsage = budget.usage();
        boolean sealed = false;
        try {
            var fenced = com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                    scan.canonicalStoredBytes());
            assertThat(fenced.logicalTrimSourceScan()).isPresent();
            String path = "/managed-ledgers/" + binding.managedLedgerName() + "/" + SUBSCRIPTION;
            var before = metadata.get(path).get(30, TimeUnit.SECONDS).orElseThrow();
            var actualCursor = new org.apache.bookkeeper.mledger.impl.MetaStoreImpl(null, null)
                    .parseManagedCursorInfo(before.getValue(), 8_388_608);
            assertThat(actualCursor.getCursorsLedgerId()).isEqualTo(checkpointLedger);
            long nextCheckpoint = cursor.getCursorLedgerLastEntry() + 1;
            try (var source = PulsarNativeCursorFloorScanV3.capture(
                            binding, metadata, observer, scan, budget, config, worker)
                    .get(30, TimeUnit.SECONDS)) {
                var observed = source.floor();
                assertThat(source.cursorCount()).isEqualTo(1);
                assertThat(source.witness().toByteArray().length).isPositive();
                var coverage = source.coverage();
                var actualParent = metadata.get("/managed-ledgers/" + binding.managedLedgerName())
                        .get(30, TimeUnit.SECONDS)
                        .orElseThrow();
                var persistedDirectory = new org.apache.bookkeeper.mledger.impl.MetaStoreImpl(null, null)
                        .parseManagedLedgerInfo(actualParent.getValue(), 8_388_608);
                // Native mark-delete may have legitimately retired an already consumed head ledger.
                assertThat(coverage.chain().stream()
                                .map(value -> value.ledgerId())
                                .toList())
                        .containsExactlyElementsOf(persistedDirectory.getLedgerInfosList().stream()
                                .map(value -> value.getLedgerId())
                                .toList());
                try (var independent = observer.borrowClient()
                        .openLedgerNoRecovery(checkpointLedger, BookKeeper.DigestType.CRC32C, new byte[0])) {
                    long lac = independent.getLastAddConfirmed();
                    assertThat(lac).isNotNegative();
                    var entry = independent.readEntries(lac, lac).nextElement();
                    var position = new org.apache.bookkeeper.mledger.proto.PositionInfo();
                    position.parseFrom(entry.getEntry());
                    assertThat(observed.pulsarSafeFloor())
                            .contains(new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                                    position.getLedgerId(), position.getEntryId() + 1));
                    assertThatThrownBy(() -> independent.readUnconfirmedEntries(nextCheckpoint, nextCheckpoint))
                            .isInstanceOf(org.apache.bookkeeper.client.BKException.BKNoSuchEntryException.class);
                }
                var after = metadata.get(path).get(30, TimeUnit.SECONDS).orElseThrow();
                assertThat(after.getValue()).isEqualTo(before.getValue());
                assertThat(after.getStat().getVersion())
                        .isEqualTo(before.getStat().getVersion());
                var fact = observed.authority();
                var rows = java.util.Arrays.stream(
                                com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1.values())
                        .map(kind -> kind
                                        == com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1
                                                .PULSAR_CURSOR_SUBSCRIPTION
                                ? observed
                                : new com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                        .RetentionFloorObservationV1(
                                        kind,
                                        fact,
                                        PositionDomain.PULSAR_ENTRY,
                                        -1,
                                        true,
                                        true,
                                        observed.pulsarSafeFloor()))
                        .toList();
                var prior = new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                        coverage.chain().get(0).ledgerId(), 0);
                var snapshot = com.nereusstream.storage.object.retention.M5RetentionCodecV1.finalizeSnapshot(
                        new com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorSnapshotV1(
                                new com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1
                                        .IdentityEnvelope(
                                        attempt,
                                        observer.capability().configurationDigest(),
                                        binding.bindingIdentity(),
                                        1,
                                        1,
                                        1,
                                        fenced.capability()),
                                PositionDomain.PULSAR_ENTRY,
                                1,
                                -1,
                                attempt,
                                fact,
                                fact,
                                1,
                                4096,
                                rows,
                                attempt,
                                Optional.of(
                                        new com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                                .PulsarFloorContextV2(coverage, prior))));
                int priorLosses = responses.putLosses.get();
                if (loseReplies) {
                    responses.arm(coordinator.floorSnapshotKey(snapshot.snapshotRootSha256()));
                    assertThat(coordinator
                                    .sealSourceScan(scan, snapshot, attempt)
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS)
                                    .outcome())
                            .isEqualTo(
                                    com.nereusstream.storage.object.retention.M5SingleAuthorityLogicalTrimCoordinatorV2
                                            .Outcome.RESPONSE_UNKNOWN);
                    assertThat(exact.read(key).toCompletableFuture().get(30, TimeUnit.SECONDS))
                            .contains(scan);
                    responses.clear();
                    responses.arm(
                            key,
                            bytes ->
                                    com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1
                                            .decodeAuthority(CanonicalBytes.copyOf(bytes))
                                            .logicalTrim()
                                            .flatMap(
                                                    com.nereusstream.storage.object.retention.M5LogicalTrimRecordsV2
                                                                    .LogicalTrimStateV2::fence)
                                            .isPresent());
                }
                var fixed = coordinator
                        .sealSourceScan(scan, snapshot, attempt)
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS);
                assertThat(fixed.outcome())
                        .isEqualTo(
                                loseReplies
                                        ? com.nereusstream.storage.object.retention
                                                .M5SingleAuthorityLogicalTrimCoordinatorV2.Outcome.RESPONSE_UNKNOWN
                                        : com.nereusstream.storage.object.retention
                                                .M5SingleAuthorityLogicalTrimCoordinatorV2.Outcome.APPLIED_EXACT);
                sealed = true;
                // End the original Native observation before the new metadata client reconstructs the fixed fence/SF.
                source.closeAsync().get(30, TimeUnit.SECONDS);
                assertThat(budget.usage()).isEqualTo(baselineUsage);
                try (var restarted = OxiaClientBuilder.create(System.getProperty("nereus.m5.nativeOffloadOxia"))
                        .namespace("default")
                        .clientIdentifier("native-cursor-trim-cold-continuation")
                        .asyncClient()
                        .get(30, TimeUnit.SECONDS)) {
                    var coldStore =
                            new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(
                                    restarted);
                    var cold = new com.nereusstream.storage.object.retention.M5PulsarLogicalTrimCoordinatorV2(
                            coldStore, key);
                    var actualFixed = coldStore
                            .read(key)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow();
                    var persistedSnapshot = coldStore
                            .read(cold.floorSnapshotKey(snapshot.snapshotRootSha256()))
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow();
                    assertThat(persistedSnapshot.canonicalStoredBytes())
                            .isEqualTo(com.nereusstream.storage.object.retention.M5RetentionCodecV1.encodeSnapshot(
                                    snapshot));
                    assertThat(cold.commitStoredSourceScan(actualFixed, attempt)
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS)
                                    .outcome())
                            .isEqualTo(
                                    com.nereusstream.storage.object.retention.M5SingleAuthorityLogicalTrimCoordinatorV2
                                            .Outcome.APPLIED_EXACT);
                    assertThat(coldStore
                                    .read(cold.floorSnapshotKey(snapshot.snapshotRootSha256()))
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS))
                            .contains(persistedSnapshot);
                } finally {
                    responses.clear();
                }
                if (loseReplies) {
                    assertThat(responses.putLosses.get() - priorLosses).isEqualTo(2);
                }
                assertThat(coordinator
                                .readFrontier()
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS)
                                .orElseThrow()
                                .pulsarContext()
                                .orElseThrow()
                                .newFrontier())
                        .isEqualTo(observed.pulsarSafeFloor().orElseThrow());
                System.out.println("NATIVE_ACTUAL_CURSOR_FLOOR actualCheckpoint=true freezeBeforeScan=true "
                        + "activeFactoryRetained=true originalFactoryDrained=true "
                        + "sourceScanSealed=true inlineFrontier=true durableSnapshot=true "
                        + "coldMetadataContinuation=true "
                        + "otherNineFloorClassesDeclared=true fullNativeFloorAuthority=false wholeM5Authority=false");
            }
            assertThat(budget.usage()).isEqualTo(baselineUsage);
        } finally {
            if (!sealed) {
                coordinator.abortSourceScan(scan, attempt).toCompletableFuture().get(30, TimeUnit.SECONDS);
            }
        }
    }

    private static void verifyPostTrimCursorReset(
            PulsarNativeBindingV1 binding,
            M5BookKeeperNamespaceAuthorityV2 observer,
            ManagedLedgerImpl ledger,
            MetadataStoreExtended metadata,
            com.nereusstream.storage.object.materialization.M5PulsarCoverageV2 coverage)
            throws Exception {
        var cursor = (org.apache.bookkeeper.mledger.impl.ManagedCursorImpl)
                ledger.openCursor("trim-reset", InitialPosition.Latest);
        cursor.markDelete(ledger.addEntry(payload(100)));
        String path = "/managed-ledgers/" + binding.managedLedgerName() + "/trim-reset";
        var before = metadata.get(path).get(30, TimeUnit.SECONDS).orElseThrow();
        long cursorLedger = cursor.getCursorLedger();
        assertThat(cursorLedger).isNotNegative();
        long nextEntry = cursor.getCursorLedgerLastEntry() + 1;
        long dataLedger = coverage.chain().get(1).ledgerId();
        var rejected = resetCursor(cursor, org.apache.bookkeeper.mledger.PositionFactory.create(dataLedger, 0));
        assertThatThrownBy(() -> rejected.get(30, TimeUnit.SECONDS))
                .hasMessageContaining("precedes committed logical trim");
        assertThat(cursor.getCursorLedger()).isEqualTo(cursorLedger);
        assertThat(cursor.getCursorLedgerLastEntry()).isEqualTo(nextEntry - 1);
        var unchanged = metadata.get(path).get(30, TimeUnit.SECONDS).orElseThrow();
        assertThat(unchanged.getValue()).isEqualTo(before.getValue());
        assertThat(unchanged.getStat().getVersion()).isEqualTo(before.getStat().getVersion());
        try (var independent =
                observer.borrowClient().openLedgerNoRecovery(cursorLedger, BookKeeper.DigestType.CRC32C, new byte[0])) {
            assertThatThrownBy(() -> independent.readUnconfirmedEntries(nextEntry, nextEntry))
                    .isInstanceOf(org.apache.bookkeeper.client.BKException.BKNoSuchEntryException.class);
        }
        var retained = org.apache.bookkeeper.mledger.PositionFactory.create(dataLedger, 1);
        resetCursor(cursor, retained).get(30, TimeUnit.SECONDS);
        assertThat(cursor.getReadPosition()).isEqualTo(retained);
        var appendedTail = ledger.getLastConfirmedEntry();
        assertThat(coverage.chain().stream().noneMatch(value -> value.ledgerId() == appendedTail.getLedgerId()))
                .isTrue();
        resetCursor(cursor, appendedTail).get(30, TimeUnit.SECONDS);
        assertThat(cursor.getReadPosition()).isEqualTo(appendedTail);
        System.out.println("NATIVE_POST_TRIM_RESET declaredFloorFixture=true actualInlineFrontier=true "
                + "belowTrimRejectedBeforeOriginalSdk=true cursorMetadataUnchanged=true "
                + "independentNextCheckpointAbsent=true atFrontierResetSucceeded=true "
                + "appendedTailResetSucceeded=true wholeM5Authority=false");
    }

    private static CompletableFuture<Void> resetCursor(
            org.apache.bookkeeper.mledger.impl.ManagedCursorImpl cursor,
            org.apache.bookkeeper.mledger.Position position) {
        var result = new CompletableFuture<Void>();
        cursor.asyncResetCursor(position, true, new org.apache.bookkeeper.mledger.AsyncCallbacks.ResetCursorCallback() {
            public void resetComplete(Object ctx) {
                result.complete(null);
            }

            public void resetFailed(ManagedLedgerException failure, Object ctx) {
                result.completeExceptionally(failure);
            }
        });
        return result;
    }

    private static void verifyOriginalCursorResetUnknown(
            PulsarNativeBindingV1 binding,
            M5BookKeeperNamespaceAuthorityV2 observer,
            PulsarNativeManagedLedgerFactoryV2 factory,
            ManagedLedgerImpl ledger,
            org.apache.bookkeeper.mledger.Position position,
            PulsarNativeReadCellBudgetV1 budget,
            SelectorResponseFault responses,
            MetadataStoreExtended metadata,
            AtomicBoolean failFallback,
            AtomicInteger fallbackFailures)
            throws Exception {
        var namespace = factory.namespace();
        var cursor = (org.apache.bookkeeper.mledger.impl.ManagedCursorImpl)
                ledger.openCursor("reset-unknown", InitialPosition.Earliest);
        cursor.markDelete(position);
        long cursorLedger = cursor.getCursorLedger();
        long entry = cursor.getCursorLedgerLastEntry() + 1;
        assertThat(cursorLedger).isNotNegative();
        var header = namespace
                .borrowClient()
                .getClientCtx()
                .getLedgerManager()
                .readLedgerMetadata(cursorLedger)
                .get(30, TimeUnit.SECONDS)
                .getValue();
        String cursorPath = "/managed-ledgers/" + binding.managedLedgerName() + "/reset-unknown";
        var before = metadata.get(cursorPath).get(30, TimeUnit.SECONDS).orElseThrow();
        String aggregateKey = new OxiaV2AuthorityKeys(binding.authorityRoot())
                .aggregateKey(binding.authority().aggregate().binding().incarnationIdentity());
        var exact = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(responses);
        var quota = binding.materializationQuota(observer).get(30, TimeUnit.SECONDS);
        var replies = new java.util.ArrayList<NativeDataReplyLoss>();
        try {
            for (var bookie : header.getEnsembleAt(entry)) {
                replies.add(NativeDataReplyLoss.install(namespace.borrowClient(), bookie, cursorLedger, entry));
            }
            failFallback.set(true);
            var reset = new CompletableFuture<Void>();
            cursor.asyncResetCursor(
                    position, false, new org.apache.bookkeeper.mledger.AsyncCallbacks.ResetCursorCallback() {
                        public void resetComplete(Object ctx) {
                            reset.complete(null);
                        }

                        public void resetFailed(ManagedLedgerException failure, Object ctx) {
                            reset.completeExceptionally(failure);
                        }
                    });
            for (var reply : replies) {
                assertThat(reply.addApplied.await(30, TimeUnit.SECONDS)).isTrue();
            }
            assertThatThrownBy(() -> reset.get(60, TimeUnit.SECONDS))
                    .isInstanceOf(java.util.concurrent.ExecutionException.class)
                    .hasMessageContaining("ticket is unresolved");
            assertThat(fallbackFailures.get()).isEqualTo(1);
            var unchanged = metadata.get(cursorPath).get(30, TimeUnit.SECONDS).orElseThrow();
            assertThat(unchanged.getValue()).isEqualTo(before.getValue());
            assertThat(unchanged.getStat().getVersion())
                    .isEqualTo(before.getStat().getVersion());
            for (var bookie : header.getEnsembleAt(entry)) {
                var packet = readOriginalSdkPacket(observer, bookie, cursorLedger, entry);
                try {
                    var digest = org.apache.bookkeeper.proto.checksum.DigestManager.instantiate(
                            cursorLedger,
                            header.getPassword(),
                            org.apache.bookkeeper.client.BookKeeper.DigestType.toProtoDigestType(
                                    org.apache.bookkeeper.client.BookKeeper.DigestType.fromApiDigestType(
                                            header.getDigestType())),
                            io.netty.buffer.UnpooledByteBufAllocator.DEFAULT,
                            false);
                    var body = digest.verifyDigestAndReturnData(entry, packet.getBuffer(0));
                    var stored = new org.apache.bookkeeper.mledger.proto.PositionInfo();
                    stored.parseFrom(body, body.readableBytes());
                    assertThat(org.apache.bookkeeper.mledger.PositionFactory.create(
                                            stored.getLedgerId(), stored.getEntryId())
                                    .compareTo(position))
                            .isNegative();
                } finally {
                    packet.release();
                }
            }
            var authority = com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                    exact.read(aggregateKey)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow()
                            .canonicalStoredBytes());
            var retainedC = authority.referenceMutationTickets().stream()
                    .filter(ticket -> ticket.referenceKind()
                            == com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                    .PULSAR_SUBSCRIPTION_OR_REPLICATION_CURSOR)
                    .toList();
            assertThat(retainedC).hasSize(2);
            var retainedD = new java.util.HashMap<
                    PhysicalResourceIdV2,
                    List<
                            com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                    .ProofBoundWriterTicketV1>>();
            for (long id : List.of(cursorLedger, position.getLedgerId())) {
                var target = new PhysicalResourceIdV2.BookKeeperLedger(namespace.namespace(), id);
                var tickets = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                                quota.read(target.authorityKey())
                                        .toCompletableFuture()
                                        .get(30, TimeUnit.SECONDS)
                                        .orElseThrow()
                                        .canonicalStoredBytes())
                        .activeWriterTickets()
                        .stream()
                        .filter(ticket -> retainedC.stream().anyMatch(c -> c.externalAuthorityRootSha256()
                                .equals(ticket.externalFactsRootSha256())))
                        .toList();
                assertThat(tickets).isNotEmpty();
                retainedD.put(target, tickets);
            }
            var charged = budget.usage();
            assertThat(budget.durableUsage().get(30, TimeUnit.SECONDS)).isEqualTo(charged);
            assertThatThrownBy(() -> factory.closeAsync().get(30, TimeUnit.SECONDS))
                    .isInstanceOf(java.util.concurrent.ExecutionException.class);
            for (var reply : replies) {
                assertThat(reply.addRequests.get()).isEqualTo(1);
                reply.close();
            }
            replies.clear();
            namespace.close();
            var after = com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                    exact.read(aggregateKey)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow()
                            .canonicalStoredBytes());
            assertThat(after.referenceMutationTickets()).containsAll(retainedC);
            for (var row : retainedD.entrySet()) {
                assertThat(com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                                        quota.read(row.getKey().authorityKey())
                                                .toCompletableFuture()
                                                .get(30, TimeUnit.SECONDS)
                                                .orElseThrow()
                                                .canonicalStoredBytes())
                                .activeWriterTickets())
                        .containsAll(row.getValue());
            }
            assertThat(budget.usage()).isEqualTo(charged);
            assertThat(budget.durableUsage().get(30, TimeUnit.SECONDS)).isEqualTo(charged);
            assertThatThrownBy(() -> readBudget(binding))
                    .hasRootCauseMessage("Native Cell allocator has no confirmed durable drain");
            System.out.println("NATIVE_CURSOR_RESET_UNKNOWN actualBkReplyLoss=true actualEarlierPositionStored=true"
                    + " fallbackFailed=true unchangedCursorMetadata=true originalCAndDRetained=true"
                    + " transportCloseDoesNotRefund=true redispatch=false"
                    + " unknownTerminationQualified=false wholeM5Authority=false");
        } finally {
            for (var reply : replies) {
                reply.close();
            }
        }
    }

    private static void verifyFactoryOwnsNativeLedgerDrain(
            PulsarNativeBindingV1 binding,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            NereusPulsarLedgerOffloaderV1 offloader,
            PulsarNativeReadCellBudgetV1 budget,
            java.util.concurrent.Executor worker,
            SelectorResponseFault responses,
            boolean loseMetadataHandleCleanup,
            boolean loseDataReply)
            throws Exception {
        var baseline = budget.usage();
        try (var nativeMetadata = metadata()) {
            var ownedFactory =
                    guardedMappedFactory(binding, nativeMetadata, bootstrap, offloader, budget, worker, responses);
            NativeDataReplyLoss dataReplies = null;
            try {
                var sdkScope = ((com.nereusstream.storage.bookkeeper.M5PulsarNativeMetadataDriverV1)
                                ownedFactory.namespace().borrowClient().getMetadataClientDriver())
                        .createSpec()
                        .sha256();
                var recoveryReferences = observeSdkRecoveryReferences(binding, responses);
                assertThat(recoveryReferences).singleElement().satisfies(ticket -> assertThat(
                                ticket.externalAuthorityRootSha256())
                        .isEqualTo(sdkScope));
                var sdkP1Key = new OxiaV2AuthorityKeys(binding.authorityRoot())
                        .aggregateKey(binding.authority().aggregate().binding().incarnationIdentity());
                var ledger = ownedFactory.openLedger(config(offloader));
                var position = ledger.addEntry(payload(99));
                var physicalTail = new PhysicalResourceIdV2.BookKeeperLedger(
                        ownedFactory.namespace().namespace(), position.getLedgerId());
                var tailQuota = binding.materializationQuota(bootstrap).get(30, TimeUnit.SECONDS);
                var tailBefore = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                                tailQuota
                                        .read(physicalTail.authorityKey())
                                        .toCompletableFuture()
                                        .get(30, TimeUnit.SECONDS)
                                        .orElseThrow()
                                        .canonicalStoredBytes())
                        .activeWriterTickets();
                assertThat(tailBefore).singleElement().satisfies(ticket -> assertThat(ticket.writerClass())
                        .isEqualTo(
                                com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                        .ProofBoundWriterClassV1.OWNER_WORKER_LEASE_HANDLE_PIN_V1));
                assertThat(bootstrap.namespace())
                        .isEqualTo(ownedFactory.namespace().namespace());
                if (loseDataReply) {
                    var header = ownedFactory
                            .namespace()
                            .borrowClient()
                            .getClientCtx()
                            .getLedgerManager()
                            .readLedgerMetadata(position.getLedgerId())
                            .get(30, TimeUnit.SECONDS)
                            .getValue();
                    var targetBookie =
                            header.getEnsembleAt(position.getEntryId()).get(0);
                    dataReplies = NativeDataReplyLoss.install(
                            ownedFactory.namespace().borrowClient(),
                            targetBookie,
                            position.getLedgerId(),
                            position.getEntryId() + 1);
                    var added = ledger.addEntry(payload(100));
                    assertThat(added.getLedgerId()).isEqualTo(position.getLedgerId());
                    assertThat(added.getEntryId()).isEqualTo(position.getEntryId() + 1);
                    assertThat(dataReplies.addApplied.await(30, TimeUnit.SECONDS))
                            .isTrue();
                    var stored = new CompletableFuture<byte[]>();
                    bootstrap
                            .borrowClient()
                            .getClientCtx()
                            .getBookieClient()
                            .readEntry(
                                    targetBookie,
                                    added.getLedgerId(),
                                    added.getEntryId(),
                                    (rc, actualLedger, actualEntry, buffer, ctx) -> {
                                        try {
                                            if (rc == org.apache.bookkeeper.client.BKException.Code.OK) {
                                                stored.complete(ByteBufUtil.getBytes(
                                                        buffer, buffer.writerIndex() - ENTRY_BYTES, ENTRY_BYTES));
                                            } else {
                                                stored.completeExceptionally(
                                                        org.apache.bookkeeper.client.BKException.create(rc));
                                            }
                                        } catch (Exception failure) {
                                            stored.completeExceptionally(failure);
                                        }
                                    },
                                    null,
                                    org.apache.bookkeeper.proto.BookieProtocol.FLAG_NONE);
                    assertThat(stored.get(30, TimeUnit.SECONDS)).isEqualTo(payload(100));
                    var forceResult = new CompletableFuture<Integer>();
                    ownedFactory
                            .namespace()
                            .borrowClient()
                            .getClientCtx()
                            .getBookieClient()
                            .forceLedger(
                                    targetBookie,
                                    position.getLedgerId(),
                                    (rc, actualLedger, actualBookie, ctx) -> forceResult.complete(rc),
                                    null);
                    assertThat(dataReplies.forceApplied.await(30, TimeUnit.SECONDS))
                            .isTrue();
                    assertThat(forceResult.get(30, TimeUnit.SECONDS))
                            .isEqualTo(org.apache.bookkeeper.client.BKException.Code.TimeoutException);
                    dataReplies.dropOriginalFence.set(true);
                    var originalFenceResult = new CompletableFuture<Integer>();
                    ownedFactory
                            .namespace()
                            .borrowClient()
                            .getClientCtx()
                            .getBookieClient()
                            .readEntry(
                                    targetBookie,
                                    position.getLedgerId(),
                                    org.apache.bookkeeper.proto.BookieProtocol.LAST_ADD_CONFIRMED,
                                    (rc, actualLedger, actualEntry, buffer, ctx) -> {
                                        originalFenceResult.complete(rc);
                                    },
                                    null,
                                    org.apache.bookkeeper.proto.BookieProtocol.FLAG_DO_FENCING,
                                    org.apache.bookkeeper.proto.checksum.DigestManager.generateMasterKey(
                                            header.getPassword()),
                                    false);
                    assertThat(dataReplies.originalFenceApplied.await(30, TimeUnit.SECONDS))
                            .isTrue();
                    assertThat(originalFenceResult.get(30, TimeUnit.SECONDS))
                            .isEqualTo(org.apache.bookkeeper.client.BKException.Code.TimeoutException);
                    verifyExplicitLacRemainsMutableAfterFence(
                            ownedFactory.namespace(), bootstrap, targetBookie, position.getLedgerId(), header);
                    verifyRecoveryAddAcceptedAfterFence(
                            ownedFactory.namespace(), bootstrap, targetBookie, position.getLedgerId(), header);
                }
                responses.holdAppliedPut(
                        loseMetadataHandleCleanup ? null : sdkP1Key, bytes -> {
                            try {
                                if (loseMetadataHandleCleanup) {
                                    var authority = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1
                                            .decodeAuthority(CanonicalBytes.copyOf(bytes));
                                    return authority.target().resourceId().equals(physicalTail)
                                            && authority.activeWriterTickets().isEmpty();
                                }
                                return com
                                        .nereusstream
                                        .storage
                                        .object
                                        .retention
                                        .M5PulsarAggregateAuthorityCodecV1
                                        .decodeAuthority(CanonicalBytes.copyOf(bytes))
                                        .referenceMutationTickets()
                                        .stream()
                                        .noneMatch(ticket -> ticket.referenceKind()
                                                == com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                                        .ReferenceKindV1.RECOVERY_CHECKPOINT_OR_SNAPSHOT);
                            } catch (RuntimeException anotherRecord) {
                                return false;
                            }
                        });
                assertThat(ownedFactory.namespace().sdkDataWriteSnapshot().dispatchedAdds())
                        .isGreaterThanOrEqualTo(3);
                var entry = readOrdinaryEntry(ledger, position).get(30, TimeUnit.SECONDS);
                CompletableFuture<Void> closing;
                try {
                    assertThat(entry.getData()).isEqualTo(payload(99));
                    var held = budget.usage();
                    closing = ownedFactory.closeAsync();
                    var closeObserver = closing;
                    assertThat(ownedFactory.closeAsync().cancel(false)).isTrue();
                    assertThatThrownBy(() -> closeObserver.get(1, TimeUnit.SECONDS))
                            .isInstanceOf(TimeoutException.class);
                    assertThat(budget.usage()).isEqualTo(held);
                    assertThat(observeSdkRecoveryReferences(binding, responses)).isEqualTo(recoveryReferences);
                    assertThat(com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                                            tailQuota
                                                    .read(physicalTail.authorityKey())
                                                    .toCompletableFuture()
                                                    .get(30, TimeUnit.SECONDS)
                                                    .orElseThrow()
                                                    .canonicalStoredBytes())
                                    .activeWriterTickets())
                            .isEqualTo(tailBefore);
                } finally {
                    entry.release();
                }
                if (loseDataReply) {
                    var closeObserver = closing;
                    assertThatThrownBy(() -> closeObserver.get(30, TimeUnit.SECONDS))
                            .hasRootCauseInstanceOf(IllegalStateException.class)
                            .hasRootCauseMessage("original SDK data write remains unknown");
                    assertThat(ownedFactory
                                    .namespace()
                                    .borrowClient()
                                    .getClientCtx()
                                    .isClientClosed())
                            .isFalse();
                    assertThat(budget.usage().owners()).isEqualTo(baseline.owners() + 2);
                    assertThat(observeSdkRecoveryReferences(binding, responses)).isEqualTo(recoveryReferences);
                    assertThat(ownedFactory.namespace().sdkDataWriteSnapshot().unresolvedWrites())
                            .isEqualTo(3);
                    dataReplies.dropFences.set(true);
                    var unavailableFence = ownedFactory.closeAsync();
                    assertThat(dataReplies.fenceApplied.await(30, TimeUnit.SECONDS))
                            .isTrue();
                    assertThatThrownBy(() -> unavailableFence.get(30, TimeUnit.SECONDS))
                            .hasRootCauseInstanceOf(IllegalStateException.class)
                            .hasRootCauseMessage("original SDK append fence remains unknown");
                    assertThat(budget.usage().owners()).isEqualTo(baseline.owners() + 2);
                    assertThat(observeSdkRecoveryReferences(binding, responses)).isEqualTo(recoveryReferences);
                    assertThat(ownedFactory.namespace().sdkDataWriteSnapshot().fencedWrites())
                            .isZero();
                    dataReplies.dropFences.set(false);
                    dataReplies.holdFence.set(true);
                    var changedHeaderFence = ownedFactory.closeAsync();
                    assertThat(dataReplies.fenceHeld.await(30, TimeUnit.SECONDS))
                            .isTrue();
                    var metadataPort = ownedFactory
                            .namespace()
                            .borrowClient()
                            .getClientCtx()
                            .getLedgerManager();
                    var originalHeader = metadataPort
                            .readLedgerMetadata(position.getLedgerId())
                            .get(30, TimeUnit.SECONDS);
                    var sameValue = metadataPort
                            .writeLedgerMetadata(
                                    position.getLedgerId(), originalHeader.getValue(), originalHeader.getVersion())
                            .get(30, TimeUnit.SECONDS);
                    assertThat(((org.apache.bookkeeper.versioning.LongVersion) sameValue.getVersion()).getLongVersion())
                            .isGreaterThan(((org.apache.bookkeeper.versioning.LongVersion) originalHeader.getVersion())
                                    .getLongVersion());
                    dataReplies.releaseFence();
                    assertThatThrownBy(() -> changedHeaderFence.get(30, TimeUnit.SECONDS))
                            .hasRootCauseInstanceOf(IllegalStateException.class)
                            .hasRootCauseMessage("original SDK append header changed during fencing");
                    assertThat(budget.usage().owners()).isEqualTo(baseline.owners() + 2);
                    assertThat(observeSdkRecoveryReferences(binding, responses)).isEqualTo(recoveryReferences);
                    assertThat(ownedFactory.namespace().sdkDataWriteSnapshot().fencedWrites())
                            .isZero();
                    assertThat(ownedFactory.namespace().sdkDataWriteSnapshot().unresolvedWrites())
                            .isEqualTo(3);
                    closing = ownedFactory.closeAsync();
                }
                try (var proofMetadata = OxiaClientBuilder.create(System.getProperty("nereus.m5.nativeOffloadOxia"))
                        .namespace("default")
                        .asyncClient()
                        .get(30, TimeUnit.SECONDS)) {
                    assertThat(responses.heldApplied.await(30, TimeUnit.SECONDS))
                            .isTrue();
                    var exact = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(
                            proofMetadata);
                    var originalKey = responses.heldActualKey;
                    var original = exact.read(originalKey)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow();
                    responses.loseHeldPutReply();
                    int originalAttempts = responses.watchedPutAttempts.get();
                    var closeObserver = closing;
                    if (loseMetadataHandleCleanup) {
                        assertThatThrownBy(() -> closeObserver.get(30, TimeUnit.SECONDS))
                                .hasRootCauseInstanceOf(IllegalStateException.class)
                                .hasRootCauseMessage("original SDK metadata handle cleanup remains unknown");
                    } else {
                        assertThatThrownBy(() -> closeObserver.get(30, TimeUnit.SECONDS))
                                .hasRootCauseInstanceOf(TimeoutException.class)
                                .hasRootCauseMessage("actual selector observation unavailable");
                    }
                    var unknown = budget.usage();
                    assertThat(unknown.owners()).isEqualTo(baseline.owners() + 2);
                    if (loseMetadataHandleCleanup) {
                        assertThat(observeSdkRecoveryReferences(binding, responses))
                                .isEqualTo(recoveryReferences);
                    }
                    responses.clear();
                    ownedFactory.closeAsync().get(30, TimeUnit.SECONDS);
                    assertThat(responses.watchedPutAttempts.get()).isEqualTo(originalAttempts);
                    assertThat(exact.read(originalKey)
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS)
                                    .orElseThrow())
                            .isEqualTo(original);
                    System.out.println((loseMetadataHandleCleanup
                                    ? "NATIVE_SDK_METADATA_HANDLE_UNKNOWN"
                                    : "NATIVE_SDK_RECOVERY_REFERENCE_UNKNOWN")
                            + " originalSdkAndMetadataActorsRetained=true"
                            + " originalCandidateVersionPreserved=true originalMutationRedispatch=false"
                            + " sdkAlreadyClosedMetadataOnlyCleanup=true wholeM5Authority=false");
                } finally {
                    responses.clear();
                }
                var originalData = ownedFactory.namespace().sdkDataWriteSnapshot();
                assertThat(originalData.admissionClosed()).isTrue();
                assertThat(originalData.unresolvedWrites()).isZero();
                assertThat(originalData.retainedBytes()).isZero();
                assertThat(originalData.fencedWrites()).isEqualTo(loseDataReply ? 2 : 0);
                assertThat(originalData.harmlessForces()).isEqualTo(loseDataReply ? 1 : 0);
                assertThat(originalData.completedWrites() + originalData.fencedWrites() + originalData.harmlessForces())
                        .isEqualTo(originalData.dispatchedAdds()
                                + originalData.dispatchedLacs()
                                + originalData.dispatchedFences()
                                + originalData.dispatchedForces());
                assertThat(budget.usage()).isEqualTo(baseline);
                assertThat(observeSdkRecoveryReferences(binding, responses)).isEmpty();
                assertThat(com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(tailQuota
                                        .read(physicalTail.authorityKey())
                                        .toCompletableFuture()
                                        .get(30, TimeUnit.SECONDS)
                                        .orElseThrow()
                                        .canonicalStoredBytes())
                                .activeWriterTickets())
                        .isEmpty();
                System.out.println("NATIVE_FACTORY_OWNED_LEDGER_DRAIN actualNativeEntryHeld=true"
                        + " originalSdkAndMetadataActorsHeld=true observerCancellationIsolated=true"
                        + " ledgerDrainBeforeFactoryShutdown=true originalSdkRecoveryReferenceHeld=true"
                        + " originalSdkMetadataClosedBeforeReferenceClear=true originalMetadataHandleDObserved=true"
                        + " originalSdkDataRpcRepliesObserved=true originalSdkDataAdmissionFenced=true"
                        + " wholeM5Authority=false");
                if (loseDataReply) {
                    assertThat(dataReplies.addRequests.get()).isEqualTo(1);
                    assertThat(dataReplies.forceRequests.get()).isEqualTo(1);
                    System.out.println("NATIVE_SDK_DATA_RPC_UNKNOWN actualAppliedAppendReplyLost=true"
                            + " originalActorCAndDRetained=true lostFenceReplyRetained=true"
                            + " headerVersionChangeRetained=true forceReplyLostWithoutReplay=true"
                            + " originalFenceReplyLost=true independentDurableFenceForBothPackets=true"
                            + " originalFailedAppendNotReplayed=true realDurableFenceBeforeRefund=true"
                            + " wholeM5Authority=false");
                }
            } finally {
                responses.clear();
                if (dataReplies != null) {
                    dataReplies.close();
                }
                ownedFactory.close();
            }
        }
    }

    /** A durable native fence cannot terminate an unknown explicit-LAC write on the locked BK4.18 backend. */
    private static void verifyExplicitLacRemainsMutableAfterFence(
            M5BookKeeperNamespaceAuthorityV2 original,
            M5BookKeeperNamespaceAuthorityV2 observer,
            org.apache.bookkeeper.net.BookieId bookie,
            long ledgerId,
            org.apache.bookkeeper.client.api.LedgerMetadata metadata)
            throws Exception {
        var client = original.borrowClient();
        var digest = org.apache.bookkeeper.proto.checksum.DigestManager.instantiate(
                ledgerId,
                metadata.getPassword(),
                org.apache.bookkeeper.client.BookKeeper.DigestType.toProtoDigestType(
                        org.apache.bookkeeper.client.BookKeeper.DigestType.fromApiDigestType(metadata.getDigestType())),
                client.getClientCtx().getByteBufAllocator(),
                false);
        var masterKey = org.apache.bookkeeper.proto.checksum.DigestManager.generateMasterKey(metadata.getPassword());
        var before = original.sdkDataWriteSnapshot();
        // Both values describe actual entries. The older packet can arrive after the newer one and after fencing.
        for (long lac : new long[] {1, 0}) {
            var written = new CompletableFuture<Integer>();
            var packet = digest.computeDigestAndPackageForSendingLac(lac);
            try {
                client.getClientCtx()
                        .getBookieClient()
                        .writeLac(
                                bookie,
                                ledgerId,
                                masterKey,
                                lac,
                                packet,
                                (rc, actualLedger, actualBookie, ctx) -> written.complete(rc),
                                null);
                assertThat(written.get(30, TimeUnit.SECONDS))
                        .isEqualTo(org.apache.bookkeeper.client.BKException.Code.OK);
            } finally {
                packet.release();
            }
            var observed = new CompletableFuture<Long>();
            observer.borrowClient()
                    .getClientCtx()
                    .getBookieClient()
                    .readLac(
                            bookie,
                            ledgerId,
                            (rc, actualLedger, explicitLac, lastEntry, ctx) -> {
                                try {
                                    if (rc != org.apache.bookkeeper.client.BKException.Code.OK
                                            || actualLedger != ledgerId) {
                                        observed.completeExceptionally(
                                                org.apache.bookkeeper.client.BKException.create(rc));
                                    } else {
                                        observed.complete(digest.verifyDigestAndReturnLac(explicitLac));
                                    }
                                } catch (Exception failure) {
                                    observed.completeExceptionally(failure);
                                } finally {
                                    io.netty.util.ReferenceCountUtil.release(explicitLac);
                                    io.netty.util.ReferenceCountUtil.release(lastEntry);
                                }
                            },
                            null);
            assertThat(observed.get(30, TimeUnit.SECONDS)).isEqualTo(lac);
        }
        var after = original.sdkDataWriteSnapshot();
        assertThat(after.dispatchedLacs() - before.dispatchedLacs()).isEqualTo(2);
        assertThat(after.completedWrites() - before.completedWrites()).isEqualTo(2);
        assertThat(after.unresolvedWrites()).isEqualTo(before.unresolvedWrites());
        assertThat(after.retainedBytes()).isEqualTo(before.retainedBytes());
        System.out.println("NATIVE_SDK_LAC_FENCE_LIMIT actualFencedLedgerAcceptsLac=true"
                + " laterOlderLacOverwritesExplicitBuffer=true originalSdkCallbacksObserved=true"
                + " fenceDoesNotProveLacUnknownTermination=true wholeM5Authority=false");
    }

    private static void verifyRecoveryAddAcceptedAfterFence(
            M5BookKeeperNamespaceAuthorityV2 original,
            M5BookKeeperNamespaceAuthorityV2 observer,
            org.apache.bookkeeper.net.BookieId bookie,
            long ledgerId,
            org.apache.bookkeeper.client.api.LedgerMetadata metadata)
            throws Exception {
        var body = readOriginalSdkPacket(observer, bookie, ledgerId);
        var wire = body.getBuffer(0);
        try {
            assertThat(ByteBufUtil.getBytes(wire, wire.writerIndex() - ENTRY_BYTES, ENTRY_BYTES))
                    .isEqualTo(payload(99));
            var before = original.sdkDataWriteSnapshot();
            var written = new CompletableFuture<Integer>();
            original.borrowClient()
                    .getClientCtx()
                    .getBookieClient()
                    .addEntry(
                            bookie,
                            ledgerId,
                            org.apache.bookkeeper.proto.checksum.DigestManager.generateMasterKey(
                                    metadata.getPassword()),
                            0,
                            body,
                            (rc, actualLedger, actualEntry, actualBookie, ctx) -> written.complete(rc),
                            null,
                            org.apache.bookkeeper.proto.BookieProtocol.FLAG_RECOVERY_ADD,
                            false,
                            org.apache.bookkeeper.client.api.WriteFlag.NONE);
            assertThat(written.get(30, TimeUnit.SECONDS)).isEqualTo(org.apache.bookkeeper.client.BKException.Code.OK);
            var after = original.sdkDataWriteSnapshot();
            assertThat(after.dispatchedAdds() - before.dispatchedAdds()).isEqualTo(1);
            assertThat(after.completedWrites() - before.completedWrites()).isEqualTo(1);
            assertThat(after.unresolvedWrites()).isEqualTo(before.unresolvedWrites());
            assertThat(after.retainedBytes()).isEqualTo(before.retainedBytes());
        } finally {
            body.release();
        }
        System.out.println("NATIVE_SDK_RECOVERY_ADD_FENCE_LIMIT actualFencedLedgerAcceptsRecoveryAdd=true"
                + " originalStoredPacketAndSdkSuccessObserved=true fenceDoesNotProveRecoveryAddTermination=true"
                + " wholeM5Authority=false");
    }

    private static org.apache.bookkeeper.util.ByteBufList readOriginalSdkPacket(
            M5BookKeeperNamespaceAuthorityV2 observer, org.apache.bookkeeper.net.BookieId bookie, long ledgerId)
            throws Exception {
        return readOriginalSdkPacket(observer, bookie, ledgerId, 0);
    }

    private static org.apache.bookkeeper.util.ByteBufList readOriginalSdkPacket(
            M5BookKeeperNamespaceAuthorityV2 observer,
            org.apache.bookkeeper.net.BookieId bookie,
            long ledgerId,
            long entryId)
            throws Exception {
        var packet = new CompletableFuture<io.netty.buffer.ByteBuf>();
        observer.borrowClient()
                .getClientCtx()
                .getBookieClient()
                .readEntry(
                        bookie,
                        ledgerId,
                        entryId,
                        (rc, actualLedger, actualEntry, buffer, ctx) -> {
                            if (rc == org.apache.bookkeeper.client.BKException.Code.OK) {
                                packet.complete(buffer.retainedDuplicate());
                            } else {
                                packet.completeExceptionally(org.apache.bookkeeper.client.BKException.create(rc));
                            }
                        },
                        null,
                        org.apache.bookkeeper.proto.BookieProtocol.FLAG_NONE);
        var wire = packet.get(30, TimeUnit.SECONDS);
        return org.apache.bookkeeper.util.ByteBufList.get(wire);
    }

    private static void verifyRepeatedMappedMaterialization(
            PulsarNativeBindingV1 binding,
            PulsarNativeM4ControlV1 control,
            ManagedLedgerImpl original,
            PulsarNativeM4ReadOwnerV1 owner,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            PulsarNativeReadCellBudgetV1 budget,
            java.util.concurrent.Executor worker,
            Map<org.apache.bookkeeper.mledger.Position, byte[]> expected,
            PulsarNativeM4DrainAdmissionV1 admission,
            SelectorResponseFault responses,
            PulsarNativeManagedLedgerFactoryV2 originalFactory,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            PulsarNativeReadCellBudgetV1.Usage factoryCharge)
            throws Exception {
        var previous = control.readSelectedMaterialization(worker).get(30, TimeUnit.SECONDS);
        var priorRanges = previous.readMap()
                .plan()
                .sourceCut()
                .coverage()
                .pulsarCoverage()
                .orElseThrow()
                .ranges();
        long objectLedger = priorRanges.get(0).ledgerId();
        long mappedLedger = priorRanges.get(1).ledgerId();
        var ranges = List.of(
                new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.EntryRange(
                        objectLedger, 0, priorRanges.get(0).endEntryExclusive()),
                new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.EntryRange(mappedLedger, 0, 1));
        var before = budget.usage();
        var staging = Files.createTempDirectory("nereus-native-repeated-materialization-");
        try (var source = PulsarNativeMaterializationSourceV2.capture(owner, bookKeeper, offloader, ranges, worker)
                .get(60, TimeUnit.SECONDS)) {
            assertThat(source.sources().stream().map(value -> value.kind()))
                    .containsExactly(
                            com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind
                                    .PULSAR_NPD1_DATA,
                            com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind
                                    .PULSAR_NMS1_DATA_V2,
                            com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind
                                    .PULSAR_NMS1_DATA_V2);
            for (var extent : source.sources()) {
                try (var entries = source.openPulsarEntries(extent)) {
                    for (var entry = entries.next(); entry.isPresent(); entry = entries.next()) {
                        var value = entry.orElseThrow();
                        assertThat(value.payload().toByteArray())
                                .isEqualTo(expected.get(PositionFactory.create(
                                        value.position().ledgerId(),
                                        value.position().entryId())));
                    }
                }
            }
            source.qualifyProvider(64L * 1024 * 1024, worker).get(60, TimeUnit.SECONDS);
            var indexes = List.of(
                    com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind
                            .OFFSET_OR_POSITION,
                    com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind
                            .CHECKSUM_COVERAGE);
            var caps = new com.nereusstream.storage.object.materialization.M5MaterializationAdmissionV1.Caps(
                    4, 1024L * 1024 * 1024, 1024L * 1024 * 1024, 1024, 1024, 128, 128);
            var task = source.prepareTask(true, indexes, caps, worker).get(60, TimeUnit.SECONDS);
            assertThat(task.plan().sourceCut().identity().workerEpoch())
                    .isGreaterThan(
                            previous.readMap().plan().sourceCut().identity().workerEpoch());
            task.writeAndVerify(staging, 64L * 1024 * 1024, worker).get(60, TimeUnit.SECONDS);
            PulsarNativeMaterializationTaskV2.Publication published;
            try (var observer = OxiaClientBuilder.create(System.getProperty("nereus.m5.nativeOffloadOxia"))
                            .namespace("default")
                            .asyncClient()
                            .get(30, TimeUnit.SECONDS);
                    var objects = client(null)) {
                var exact =
                        new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(observer);
                var selectorBefore = exact.read(control.selectorKey())
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS)
                        .orElseThrow();
                var physical = previous.providerProfile().physicalBinding();
                var versions = objects.listObjectVersions(request -> request.bucket(
                                physical.physicalNamespace().containerIdentity().value())
                        .prefix(previous.providerProfile().exclusiveNamespacePrefix()));
                assertThat(versions.isTruncated()).isFalse();
                var versionsBefore = versions.versions().stream()
                        .map(value -> value.key() + ":" + value.versionId())
                        .sorted()
                        .toList();
                var map = task.readMap();
                responses.armCandidate(bytes -> {
                    try {
                        var value = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                                CanonicalBytes.copyOf(bytes));
                        return map.readableOutputs().stream().anyMatch(credit -> credit.resource()
                                        .orElseThrow()
                                        .equals(value.target().resourceId()))
                                && value.activeWriterTickets().stream()
                                        .anyMatch(ticket -> ticket.writerClass()
                                                == com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                                        .ProofBoundWriterClassV1
                                                        .MANIFEST_SELECTOR_GENERATION_REPRESENTATION_V1);
                    } catch (RuntimeException notTarget) {
                        return false;
                    }
                });
                int losses = responses.putLosses.get();
                try {
                    assertThatThrownBy(() -> task.publish(worker).get(60, TimeUnit.SECONDS))
                            .hasRootCauseMessage("Native original physical publication ticket remains unresolved");
                    assertThat(responses.putLosses.get()).isEqualTo(losses + 1);
                    assertThat(exact.read(control.selectorKey())
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS)
                                    .orElseThrow())
                            .isEqualTo(selectorBefore);
                } finally {
                    responses.clear();
                }
                published = task.publish(worker).get(60, TimeUnit.SECONDS);
                var versionsAfter = objects.listObjectVersions(request -> request.bucket(
                                physical.physicalNamespace().containerIdentity().value())
                        .prefix(previous.providerProfile().exclusiveNamespacePrefix()));
                assertThat(versionsAfter.isTruncated()).isFalse();
                assertThat(versionsAfter.versions().stream()
                                .map(value -> value.key() + ":" + value.versionId())
                                .sorted()
                                .toList())
                        .isEqualTo(versionsBefore);
            }
            assertThat(published
                            .readMap()
                            .locate(new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                                    mappedLedger, 1))
                            .rewrittenPayload())
                    .isEqualTo(previous.readMap()
                            .locate(new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                                    mappedLedger, 1))
                            .rewrittenPayload());
            assertThat(published
                            .readMap()
                            .locate(new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                                    mappedLedger, 0))
                            .rewrittenPayload())
                    .isNotEqualTo(previous.readMap()
                            .locate(new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                                    mappedLedger, 0))
                            .rewrittenPayload());
        } finally {
            try (var files = Files.walk(staging)) {
                for (var path :
                        files.sorted(java.util.Comparator.reverseOrder()).toList()) {
                    Files.delete(path);
                }
            }
        }
        assertThat(budget.usage()).isEqualTo(factoryCharge);
        closeNativeLedger(original).get(30, TimeUnit.SECONDS);
        originalFactory.close();
        assertThat(budget.usage()).isEqualTo(new PulsarNativeReadCellBudgetV1.Usage(1, 0, 0, 24L * 1024 * 1024));
        var event = Executors.newSingleThreadExecutor();
        try (var nativeMetadata = metadata()) {
            var factory =
                    guardedMappedFactory(binding, nativeMetadata, bootstrap, offloader, budget, worker, responses);
            ManagedLedgerImpl actual = null;
            PulsarNativeSourcePreparationV1 prepared = null;
            PulsarNativeM4ReadOwnerV1 recovered = null;
            try {
                actual = factory.openLedger(config(offloader).setMaxUnackedRangesToPersistInMetadataStore(0));
                prepared = control.prepareMaterializedReadSources(
                                actual,
                                factory.namespace(),
                                offloader,
                                previous.providerProfile().physicalBinding().physicalNamespace(),
                                budget,
                                worker)
                        .get(60, TimeUnit.SECONDS);
                recovered = control.prepareMaterializedReadOwner(
                                prepared, admission, factory.namespace(), offloader, worker)
                        .get(60, TimeUnit.SECONDS)
                        .recover(worker, event, 2, budget)
                        .get(60, TimeUnit.SECONDS);
                verifyActualCursorMetadata(recovered, budget, worker, actual, factory.namespace(), control);
                for (var item : expected.entrySet()) {
                    var entry = readOrdinaryEntry(actual, item.getKey()).get(30, TimeUnit.SECONDS);
                    try {
                        assertThat(entry.getData()).isEqualTo(item.getValue());
                    } finally {
                        entry.release();
                    }
                }
                recovered.drainForRecovery(worker).get(30, TimeUnit.SECONDS);
            } finally {
                if (recovered != null) {
                    recovered.fence().get(30, TimeUnit.SECONDS);
                }
                if (prepared != null) {
                    prepared.close().get(30, TimeUnit.SECONDS);
                }
                if (actual != null) {
                    closeNativeLedger(actual).get(30, TimeUnit.SECONDS);
                }
                factory.close();
            }
        } finally {
            event.shutdown();
            assertThat(event.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(before.owners()).isPositive();
        verifyFactoryOwnsNativeLedgerDrain(binding, bootstrap, offloader, budget, worker, responses, false, false);
        verifyFactoryOwnsNativeLedgerDrain(binding, bootstrap, offloader, budget, worker, responses, true, false);
        verifyFactoryOwnsNativeLedgerDrain(binding, bootstrap, offloader, budget, worker, responses, false, true);
        verifyMappedParentOriginalSdk(binding, bootstrap, offloader, budget, responses);
        System.out.println("NATIVE_PULSAR_REPEATED_MATERIALIZATION actualNpdAndSelectedNms1=true "
                + "originalVersions=true nextWorkerAndOutputs=true oldOutsideCutPayloadPreserved=true "
                + "coldFullBytes=true realOwnerDrain=true wholeM5Authority=false");
    }

    private static void verifyMappedParentOriginalSdk(
            PulsarNativeBindingV1 binding,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            NereusPulsarLedgerOffloaderV1 offloader,
            PulsarNativeReadCellBudgetV1 budget,
            SelectorResponseFault responses)
            throws Exception {
        verifyMappedParentOriginalSdk(binding, bootstrap, offloader, budget, responses, false);
    }

    private static void verifyMappedParentOriginalSdk(
            PulsarNativeBindingV1 binding,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            NereusPulsarLedgerOffloaderV1 offloader,
            PulsarNativeReadCellBudgetV1 budget,
            SelectorResponseFault responses,
            boolean checkpointAdmissionOnly)
            throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        PulsarNativeBookKeeperCreateV1 creation = null;
        ManagedLedgerFactoryImpl factory = null;
        PulsarNativeMetadataWriterV2 writer = null;
        try (var metadata = metadata();
                var sdk = client(null)) {
            try {
                var selected = binding.m4Control()
                        .readSelectedMaterialization(executor)
                        .get(30, TimeUnit.SECONDS);
                creation = binding.openGuardedBookKeeper(
                                "zk://" + System.getProperty("nereus.m5.nativeOffloadZk") + "/ledgers",
                                bootstrap,
                                budget,
                                NATIVE_BK_CANONICAL_CAPACITY,
                                executor)
                        .get(30, TimeUnit.SECONDS);
                var puts = new AtomicInteger();
                var gate = new AtomicReference<CompletableFuture<Void>>();
                var stored = new AtomicReference<CompletableFuture<org.apache.pulsar.metadata.api.Stat>>();
                String parent = "/managed-ledgers/" + binding.managedLedgerName();
                var counted = new FaultInjectionMetadataStore(metadata) {
                    @Override
                    public CompletableFuture<org.apache.pulsar.metadata.api.Stat> put(
                            String path,
                            byte[] value,
                            Optional<Long> version,
                            Set<org.apache.pulsar.metadata.api.Option> options) {
                        puts.incrementAndGet();
                        return super.put(path, value, version, options).thenCompose(stat -> {
                            var held = gate.get();
                            if (held == null || !parent.equals(path)) {
                                return CompletableFuture.completedFuture(stat);
                            }
                            stored.get().complete(stat);
                            return held.thenApply(done -> stat);
                        });
                    }
                };
                factory = factory(counted, creation.namespace().borrowClient());
                var nativeStore = (org.apache.bookkeeper.mledger.impl.MetaStoreImpl) factory.getMetaStore();
                var exact = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(
                        responses);
                String key = new OxiaV2AuthorityKeys(binding.authorityRoot())
                        .aggregateKey(binding.authority().aggregate().binding().incarnationIdentity());
                var coordinator =
                        new com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1(exact);
                var capability =
                        new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding(
                                1, creation.spec().configuration());
                var registry = nativeExpectedWriterRegistry(capability);
                var original = exact.read(key)
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS)
                        .orElseThrow();
                assertThat(coordinator
                                .enrollWriters(new com.nereusstream.storage.object.retention
                                        .M5PulsarAggregateRetirementCoordinatorV1.EnrollmentRequest(
                                        key, original, registry.enrollment()))
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS))
                        .isEqualTo(
                                com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1
                                        .Outcome.EXISTING_EXACT);
                writer = PulsarNativeMetadataWriterV2.install(
                                binding, budget, nativeStore, registry, coordinator, key, creation)
                        .get(30, TimeUnit.SECONDS);
                var physical = selected.providerProfile().physicalBinding().physicalNamespace();
                writer.admitObjectPhysicalScope(
                                offloader, physical, selected.providerProfile().maximumObjectBytes())
                        .get(30, TimeUnit.SECONDS);
                writer.admitSealedPlainSources().get(30, TimeUnit.SECONDS);
                int admittedCheckpoints = 0;
                for (var cursorName : metadata.getChildrenFromStore(parent).get(30, TimeUnit.SECONDS)) {
                    var storedCursor = nativeStore
                            .readStoredMutationValue(binding.managedLedgerName(), parent + "/" + cursorName, 8_388_608)
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow();
                    var cursorInfo = nativeStore.parseManagedCursorInfo(storedCursor.storedBytes(), 8_388_608);
                    if (!cursorInfo.hasCursorsLedgerId() || cursorInfo.getCursorsLedgerId() < 0) {
                        continue;
                    }
                    var resource = new PhysicalResourceIdV2.BookKeeperLedger(
                            creation.namespace().namespace(), cursorInfo.getCursorsLedgerId());
                    var header = creation.namespace()
                            .borrowClient()
                            .getLedgerManager()
                            .readLedgerMetadata(resource.ledgerId())
                            .get(30, TimeUnit.SECONDS)
                            .getValue();
                    assertThat(header.isClosed()).isTrue();
                    assertThat(header.getCustomMetadata().get("pulsar/cursor"))
                            .isEqualTo(cursorName.getBytes(StandardCharsets.UTF_8));
                    var physicalFact = creation.route()
                            .read(resource.authorityKey())
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow();
                    var authority = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                            physicalFact.canonicalStoredBytes());
                    assertThat(authority.target().resourceId()).isEqualTo(resource);
                    assertThat(authority.state())
                            .isEqualTo(
                                    com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                            .TargetDeleteAuthorityStateV1.OPEN_V1);
                    assertThat(authority.activeWriterTickets()).isEmpty();
                    var member = creation.route()
                            .members()
                            .readFact(resource)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS);
                    assertThat(member.inventory().members())
                            .contains(
                                    new com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2
                                            .Member(
                                            com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1
                                                    .PositionDomain.PULSAR_ENTRY,
                                            creation.spec().providerScope(),
                                            binding.bindingIdentity()));
                    admittedCheckpoints++;
                }
                if (checkpointAdmissionOnly) {
                    assertThat(admittedCheckpoints).isPositive();
                    var guardedConfig = config(offloader)
                            .setEnsembleSize(creation.spec().ensemble())
                            .setWriteQuorumSize(creation.spec().writeQuorum())
                            .setAckQuorumSize(creation.spec().ackQuorum())
                            .setMetadataEnsembleSize(creation.spec().ensemble())
                            .setMetadataWriteQuorumSize(creation.spec().writeQuorum())
                            .setMetadataAckQuorumSize(creation.spec().ackQuorum());
                    binding.configureCreate(guardedConfig);
                    ManagedLedgerImpl sdkRecovered = null;
                    try {
                        sdkRecovered = (ManagedLedgerImpl) factory.open(binding.managedLedgerName(), guardedConfig);
                        var newTail = sdkRecovered.getLedgersInfo().lastKey();
                        var tailHeader = creation.namespace()
                                .borrowClient()
                                .getLedgerManager()
                                .readLedgerMetadata(newTail)
                                .get(30, TimeUnit.SECONDS)
                                .getValue();
                        assertThat(creation.spec().bindingFor(tailHeader, true)).isPresent();
                        var cursor = sdkRecovered.openCursor("guarded-factory-recovery", InitialPosition.Earliest);
                        cursor.markDelete(PositionFactory.create(
                                selected.readMap()
                                        .plan()
                                        .sourceCut()
                                        .coverage()
                                        .pulsarCoverage()
                                        .orElseThrow()
                                        .chain()
                                        .get(0)
                                        .ledgerId(),
                                0));
                        var checkpoint =
                                ((org.apache.bookkeeper.mledger.impl.ManagedCursorImpl) cursor).getCursorLedger();
                        assertThat(checkpoint).isNotNegative();
                        var checkpointHeader = creation.namespace()
                                .borrowClient()
                                .getLedgerManager()
                                .readLedgerMetadata(checkpoint)
                                .get(30, TimeUnit.SECONDS)
                                .getValue();
                        assertThat(creation.spec().bindingFor(checkpointHeader, true))
                                .isPresent();
                        cursor.close();
                    } finally {
                        if (sdkRecovered != null) {
                            closeNativeLedger(sdkRecovered).get(30, TimeUnit.SECONDS);
                        }
                    }
                    return;
                }
                var actual = metadata.get(parent).get(30, TimeUnit.SECONDS).orElseThrow();
                var info = nativeStore.parseManagedLedgerInfo(actual.getValue(), 8_388_608);
                var expected = new java.util.TreeSet<PhysicalResourceIdV2>();
                for (var row : info.getLedgerInfosList()) {
                    var resolved = selected.readMap()
                            .physicalLedgerReferences(
                                    binding,
                                    creation.spec().namespaceBinding(),
                                    selected.providerProfile().physicalBinding(),
                                    row.getLedgerId(),
                                    row.getEntries());
                    if (resolved.isPresent()) {
                        expected.addAll(resolved.orElseThrow());
                    } else {
                        assertThat(row.hasOffloadContext()
                                        && row.getOffloadContext().isComplete())
                                .isFalse();
                        expected.add(new PhysicalResourceIdV2.BookKeeperLedger(
                                creation.namespace().namespace(), row.getLedgerId()));
                    }
                }
                var objects = expected.stream()
                        .filter(PhysicalResourceIdV2.ObjectVersion.class::isInstance)
                        .toList();
                assertThat(objects).isNotEmpty();
                var backend = S3M5PhysicalNamespaceAuthorityV2.connect(
                        sdk, physical.containerIdentity().value(), physical);
                var objectRoute = binding.materializationQuota(backend).get(30, TimeUnit.SECONDS);
                verifyNativeMixedReferenceTickets(
                        binding,
                        budget,
                        writer,
                        creation,
                        objectRoute,
                        objects,
                        nativeStore,
                        counted,
                        responses,
                        puts,
                        gate,
                        stored,
                        Optional.of(List.copyOf(expected)),
                        true);
                var rejectedSource =
                        metadata.get(parent).get(30, TimeUnit.SECONDS).orElseThrow();
                int beforeRejected = puts.get();
                var beforeCharge = budget.usage();
                var heldObject = objects.get(0);
                responses.holdAppliedPut(null, bytes -> {
                    try {
                        var value = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                                CanonicalBytes.copyOf(bytes));
                        return value.target().resourceId().equals(heldObject)
                                && !value.activeWriterTickets().isEmpty();
                    } catch (RuntimeException notTarget) {
                        return false;
                    }
                });
                try {
                    var stale = updateOriginalNativeParent(nativeStore, binding.managedLedgerName(), rejectedSource);
                    assertThat(responses.heldApplied.await(30, TimeUnit.SECONDS))
                            .isTrue();
                    assertThat(puts.get()).isEqualTo(beforeRejected);
                    assertThat(budget.usage().owners()).isEqualTo(beforeCharge.owners() + 1);
                    var selectorKey = binding.m4Control().selectorKey();
                    var originalSelector = exact.read(selectorKey)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow();
                    assertThat(exact.compareAndSet(
                                            Optional.of(originalSelector),
                                            selectorKey,
                                            originalSelector.canonicalStoredBytes())
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS))
                            .isEqualTo(
                                    com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1
                                            .MutationOutcome.APPLIED_EXACT);
                    responses.heldRelease.complete(null);
                    assertThatThrownBy(() -> stale.get(30, TimeUnit.SECONDS))
                            .hasRootCauseMessage("actual Native source selection changed before dispatch");
                    assertThat(puts.get()).isEqualTo(beforeRejected);
                } finally {
                    responses.clear();
                }
                writer.reconcileCleanup().get(30, TimeUnit.SECONDS);
                assertThat(budget.usage()).isEqualTo(beforeCharge);
                var stillStored = metadata.get(parent).get(30, TimeUnit.SECONDS).orElseThrow();
                assertThat(stillStored.getValue()).isEqualTo(rejectedSource.getValue());
                assertThat(stillStored.getStat().getVersion())
                        .isEqualTo(rejectedSource.getStat().getVersion());
                for (var target : expected) {
                    var route = target instanceof PhysicalResourceIdV2.ObjectVersion ? objectRoute : creation.route();
                    var value = route.read(target.authorityKey())
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow();
                    assertThat(com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                                            value.canonicalStoredBytes())
                                    .activeWriterTickets())
                            .isEmpty();
                }
                assertThat(metadata.get(parent)
                                .get(30, TimeUnit.SECONDS)
                                .orElseThrow()
                                .getValue())
                        .isEqualTo(actual.getValue());
                verifyNativeControlWriter(
                        binding, budget, registry, coordinator, key, exact, selected.readMap(), responses, executor);
            } finally {
                if (factory != null) {
                    factory.shutdown();
                }
                if (writer != null) {
                    writer.closeAsync().get(30, TimeUnit.SECONDS);
                }
                if (creation != null) {
                    creation.close();
                }
            }
        } finally {
            executor.shutdown();
            assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static void verifyNativeControlWriter(
            PulsarNativeBindingV1 binding,
            PulsarNativeReadCellBudgetV1 budget,
            com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1 registry,
            com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1 authority,
            String authorityKey,
            com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1 exact,
            PulsarNativeMaterializationReadMapV2 map,
            SelectorResponseFault responses,
            java.util.concurrent.Executor executor)
            throws Exception {
        var before = budget.usage();
        var control = binding.m4Control();
        control.installControlWriter(registry, authority, authorityKey, budget).get(30, TimeUnit.SECONDS);
        var charged = budget.usage();
        assertThat(charged.owners()).isEqualTo(before.owners() + 1);
        try {
            var coordinator = control.coordinator();
            var selector = coordinator.readSelector().orElseThrow();
            var source = map.plan().sourceCut().sources().get(0).sourceIdentitySha256();
            var keys =
                    new com.nereusstream.storage.object.read.control.M4ReadControlKeysV1(0, binding.bindingIdentity());
            String root = control.selectorKey()
                    .substring(
                            0, control.selectorKey().length() - keys.selector().length());
            long generation = Math.addExact(selector.sourceGeneration(), 1);
            var protection = new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtection(
                    binding.bindingIdentity(),
                    new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity(
                            source, generation, selector.readAdmissionEpoch(), generation, selector.capability()),
                    com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProtectionState.PROTECTED,
                    Optional.empty(),
                    Optional.empty());
            var protectionKey = root + keys.protection(source, generation);
            responses.holdAppliedPut(authorityKey, bytes -> {
                try {
                    return !com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                    CanonicalBytes.copyOf(bytes))
                            .referenceMutationTickets()
                            .isEmpty();
                } catch (RuntimeException notAuthority) {
                    return false;
                }
            });
            var original = CompletableFuture.supplyAsync(() -> coordinator.createProtection(protection), executor);
            try {
                assertThat(responses.heldApplied.await(30, TimeUnit.SECONDS)).isTrue();
                assertThat(exact.read(protectionKey).toCompletableFuture().get(30, TimeUnit.SECONDS))
                        .isEmpty();
                var held = authority
                        .read(authorityKey)
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS)
                        .orElseThrow();
                assertThat(com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                        held.canonicalStoredBytes())
                                .referenceMutationTickets())
                        .hasSize(1);
                var cleanupObserver = control.reconcileControlWriterCleanup();
                assertThat(cleanupObserver).isNotDone();
                assertThat(cleanupObserver.complete(null)).isTrue();
                assertThat(budget.usage()).isEqualTo(charged);
                responses.heldRelease.complete(null);
                assertThat(original.get(30, TimeUnit.SECONDS))
                        .isEqualTo(
                                com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1.Outcome
                                        .APPLIED);
            } finally {
                responses.clear();
            }
            control.reconcileControlWriterCleanup().get(30, TimeUnit.SECONDS);
            var first = exact.read(protectionKey)
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            assertThat(com.nereusstream.storage.object.read.control.M4ReadControlCodecV1.decodeProtection(
                            first.canonicalStoredBytes()))
                    .isEqualTo(protection);
            var uncertain = new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtection(
                    binding.bindingIdentity(),
                    new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity(
                            source,
                            generation + 2,
                            selector.readAdmissionEpoch(),
                            generation + 2,
                            selector.capability()),
                    protection.state(),
                    Optional.empty(),
                    Optional.empty());
            var uncertainKey = root + keys.protection(source, generation + 2);
            responses.arm(uncertainKey);
            int originalPutCount = responses.watchedPutAttempts.get();
            try {
                assertThatThrownBy(() -> coordinator.createProtection(uncertain))
                        .hasRootCauseInstanceOf(TimeoutException.class);
                assertThat(responses.watchedPutAttempts.get()).isEqualTo(originalPutCount + 1);
                assertThat(budget.usage()).isEqualTo(charged);
            } finally {
                responses.clear();
            }
            var uncertainStored = exact.read(uncertainKey)
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            assertThat(com.nereusstream.storage.object.read.control.M4ReadControlCodecV1.decodeProtection(
                            uncertainStored.canonicalStoredBytes()))
                    .isEqualTo(uncertain);
            assertThat(com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                    authority
                                            .read(authorityKey)
                                            .toCompletableFuture()
                                            .get(30, TimeUnit.SECONDS)
                                            .orElseThrow()
                                            .canonicalStoredBytes())
                            .referenceMutationTickets())
                    .hasSize(1);
            control.reconcileControlWriterCleanup().get(30, TimeUnit.SECONDS);
            assertThat(exact.read(uncertainKey).toCompletableFuture().get(30, TimeUnit.SECONDS))
                    .contains(uncertainStored);
            assertThat(responses.watchedPutAttempts.get()).isEqualTo(originalPutCount + 1);
            var secondIdentity =
                    new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity(
                            source,
                            generation + 1,
                            selector.readAdmissionEpoch(),
                            generation + 1,
                            selector.capability());
            var second = new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtection(
                    binding.bindingIdentity(), secondIdentity, protection.state(), Optional.empty(), Optional.empty());
            var secondKey = root + keys.protection(source, generation + 1);
            // Cell close waits for accepted actors; the original actor retains its admitted owner identity.
            var cellClose = budget.closeCell();
            assertThat(cellClose).isNotDone();
            responses.arm(authorityKey, bytes -> {
                try {
                    return com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                    CanonicalBytes.copyOf(bytes))
                            .referenceMutationTickets()
                            .isEmpty();
                } catch (RuntimeException notAuthority) {
                    return false;
                }
            });
            int losses = responses.putLosses.get();
            try {
                assertThat(coordinator.createProtection(second))
                        .isEqualTo(
                                com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1.Outcome
                                        .EXISTING_EXACT);
                assertThat(responses.putLosses.get()).isEqualTo(losses + 1);
                assertThat(budget.usage()).isEqualTo(charged);
                assertThat(cellClose).isNotDone();
            } finally {
                responses.clear();
            }
            var stored = exact.read(secondKey)
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            control.reconcileControlWriterCleanup().complete(null);
            control.reconcileControlWriterCleanup().get(30, TimeUnit.SECONDS);
            assertThat(exact.read(secondKey)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .orElseThrow())
                    .isEqualTo(stored);
            assertThat(com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                    authority
                                            .read(authorityKey)
                                            .toCompletableFuture()
                                            .get(30, TimeUnit.SECONDS)
                                            .orElseThrow()
                                            .canonicalStoredBytes())
                            .referenceMutationTickets())
                    .isEmpty();
            assertThat(budget.usage()).isEqualTo(charged);
        } finally {
            responses.clear();
            control.closeControlWriter().get(30, TimeUnit.SECONDS);
        }
        assertThat(budget.usage()).isEqualTo(before);
        System.out.println("NATIVE_CONTROL_WRITER originalCBeforeSdk=true actualProtectionSource=true"
                + " cleanupUnknownRetainsActor=true originalMetadataOnlyRetry=true"
                + " noSdkRedispatch=true wholeM5Authority=false");
    }

    private static CompletableFuture<Entry> readOrdinaryEntry(
            ManagedLedgerImpl ledger, org.apache.bookkeeper.mledger.Position position) {
        var result = new CompletableFuture<Entry>();
        ledger.asyncReadEntry(
                position,
                new ReadEntryCallback() {
                    @Override
                    public void readEntryComplete(Entry entry, Object context) {
                        if (!result.complete(entry)) {
                            entry.release();
                        }
                    }

                    @Override
                    public void readEntryFailed(ManagedLedgerException failure, Object context) {
                        result.completeExceptionally(failure);
                    }
                },
                null);
        return result;
    }

    private static CompletableFuture<Void> closeNativeLedger(ManagedLedgerImpl ledger) {
        var result = new Futures.CloseFuture();
        ledger.asyncClose(result, null);
        return result;
    }

    private static CompletableFuture<Entry> readNativeEntry(ManagedLedgerImpl ledger, long ledgerId) {
        var result = new CompletableFuture<Entry>();
        ledger.asyncReadEntry(
                PositionFactory.create(ledgerId, 0),
                new ReadEntryCallback() {
                    @Override
                    public void readEntryComplete(Entry entry, Object context) {
                        if (!result.complete(entry)) {
                            entry.release();
                        }
                    }

                    @Override
                    public void readEntryFailed(ManagedLedgerException failure, Object context) {
                        result.completeExceptionally(failure);
                    }
                },
                null);
        return result;
    }

    private NereusPulsarLedgerOffloaderV1 offloader(S3PulsarOffloadObjectStoreV1 store, Executor fileExecutor) {
        return offloader(
                store,
                fileExecutor,
                RetentionClass.valueOf(
                        System.getProperty("nereus.m5.nativeOffloadRetentionClass", RetentionClass.RETAIN_BK.name())));
    }

    private NereusPulsarLedgerOffloaderV1 offloader(
            S3PulsarOffloadObjectStoreV1 store, Executor fileExecutor, RetentionClass retention) {
        var policies = new OffloadPoliciesImpl();
        policies.setManagedLedgerOffloadedReadPriority(OffloadedReadPriority.TIERED_STORAGE_FIRST);
        return new NereusPulsarLedgerOffloaderV1(
                store,
                new RuntimePolicy(
                        M6_BROKER_PROVIDER_SCOPE == null ? "m5-native-real" : "m6-native-verified-delete",
                        "cells/m5-native",
                        retention,
                        BlockClass.LATENCY_1_MIB,
                        M6_BROKER_PROVIDER_SCOPE == null
                                ? CompressionPolicy.FIXED_NONE
                                : CompressionPolicy.ZSTD_IF_SMALLER,
                        LIMITS),
                keyManager(),
                (ledgerId, attempt, failure) -> {
                    throw new AssertionError("actual native read integrity failure", failure);
                },
                policies,
                fileExecutor,
                temporaryDirectory);
    }

    private static Offloaded offloadAndCloseDuringActualPut(
            NereusPulsarLedgerOffloaderV1 writer,
            S3Client independent,
            AtomicReference<SdkHold> heldPut,
            Executor fileExecutor)
            throws Exception {
        var hold = hold();
        heldPut.set(hold);
        var nativeOwner = new AtomicReference<ManagedLedgerImpl>();
        var publication = CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return writeAndOffload(writer, independent, nativeOwner);
                    } catch (Exception failure) {
                        throw new CompletionException(failure);
                    }
                },
                fileExecutor);
        try {
            assertThat(hold.started().await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(independent
                            .headObject(HeadObjectRequest.builder()
                                    .bucket(BUCKET)
                                    .key(hold.key().get())
                                    .build())
                            .versionId())
                    .isEqualTo(hold.version().get());
            var close = writer.closeAsync();
            assertThat(close).isNotDone();
            assertThat(writer.closeAsync().cancel(false)).isTrue();
            var repeatedClose = writer.closeAsync();
            assertThat(repeatedClose).isNotDone();
            var nativeClose = closeNativeLedger(nativeOwner.get());
            var repeatedNativeClose = closeNativeLedger(nativeOwner.get());
            assertThatThrownBy(() -> nativeClose.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
            assertThat(repeatedNativeClose.cancel(false)).isTrue();
            repeatedNativeClose = closeNativeLedger(nativeOwner.get());
            assertThat(repeatedNativeClose).isNotDone();
            hold.released().countDown();
            Offloaded result = publication.get(60, TimeUnit.SECONDS);
            close.get(30, TimeUnit.SECONDS);
            repeatedClose.get(30, TimeUnit.SECONDS);
            nativeClose.get(30, TimeUnit.SECONDS);
            repeatedNativeClose.get(30, TimeUnit.SECONDS);
            assertThat(closeNativeLedger(nativeOwner.get())).isCompleted();
            assertThat(writer.closeAsync()).isCompleted();
            System.out.println("NATIVE_PULSAR_OFFLOADER_DRAIN ledger=" + result.ledgerId() + " attempt="
                    + result.attempt() + " heldPutVersion=" + hold.version().get());
            return result;
        } finally {
            hold.released().countDown();
            publication.handle((result, failure) -> null).get(60, TimeUnit.SECONDS);
            writer.closeAsync().get(30, TimeUnit.SECONDS);
        }
    }

    private static void readAndCloseDuringActualGet(
            NereusPulsarLedgerOffloaderV1 offloader, Offloaded source, AtomicReference<SdkHold> heldGet)
            throws Exception {
        var handle = offloader
                .readOffloaded(source.ledgerId(), source.attempt(), offloader.getOffloadDriverMetadata())
                .get(30, TimeUnit.SECONDS);
        var hold = hold();
        heldGet.set(hold);
        var read = handle.readAsync(0, 1);
        var deliveryStarted = new CountDownLatch(1);
        var deliveryReleased = new CountDownLatch(1);
        var delivery = read.thenAccept(entries -> {
            try (entries) {
                int ordinal = 0;
                for (var entry : entries) {
                    assertThat(entry.getEntryId()).isEqualTo(ordinal);
                    assertThat(entry.getEntryBytes()).containsExactly(payload(ordinal++));
                }
                assertThat(ordinal).isEqualTo(2);
                deliveryStarted.countDown();
                try {
                    if (!deliveryReleased.await(30, TimeUnit.SECONDS)) {
                        throw new AssertionError("actual Native read delivery was not released");
                    }
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new CompletionException(failure);
                }
            }
        });
        try {
            assertThat(hold.started().await(30, TimeUnit.SECONDS)).isTrue();
            var close = handle.closeAsync();
            assertThat(close).isNotDone();
            assertThat(handle.closeAsync().cancel(false)).isTrue();
            var repeatedClose = handle.closeAsync();
            assertThat(repeatedClose).isNotDone();
            assertThatThrownBy(() -> handle.readAsync(0, 0).join())
                    .hasRootCauseMessage("Nereus Object ReadHandle is closed");
            hold.released().countDown();
            assertThat(deliveryStarted.await(30, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> close.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
            assertThat(repeatedClose).isNotDone();
            deliveryReleased.countDown();
            delivery.get(30, TimeUnit.SECONDS);
            close.get(30, TimeUnit.SECONDS);
            repeatedClose.get(30, TimeUnit.SECONDS);
            assertThat(handle.closeAsync()).isCompleted();
            System.out.println("NATIVE_PULSAR_READ_DRAIN ledger=" + source.ledgerId() + " attempt=" + source.attempt()
                    + " entries=2 nativeDeliveryDrained=true");
        } finally {
            hold.released().countDown();
            deliveryReleased.countDown();
            delivery.handle((result, failure) -> null).get(30, TimeUnit.SECONDS);
            handle.closeAsync().get(30, TimeUnit.SECONDS);
        }
    }

    private static Offloaded writeAndOffload(
            NereusPulsarLedgerOffloaderV1 offloader,
            S3Client independent,
            AtomicReference<ManagedLedgerImpl> nativeOwner)
            throws Exception {
        try (var metadata = metadata();
                var bk = bookKeeper()) {
            var factory = factory(metadata, bk);
            ManagedLedgerImpl ledger = null;
            try {
                ledger = (ManagedLedgerImpl) factory.open(LEDGER, config(offloader));
                nativeOwner.set(ledger);
                var cursor = ledger.openCursor(SUBSCRIPTION, InitialPosition.Earliest);
                long firstLedger = -1;
                for (int ordinal = 0; ordinal <= SEALED_ENTRIES; ordinal++) {
                    var position = ledger.addEntry(payload(ordinal));
                    if (ordinal == 0) {
                        firstLedger = position.getLedgerId();
                    }
                }
                assertThat(cursor.getNumberOfEntriesInBacklog(false)).isEqualTo(SEALED_ENTRIES + 1L);
                var info = ledger.getLedgersInfo().get(firstLedger);
                assertThat(info.getEntries()).isEqualTo(SEALED_ENTRIES);
                assertThat(info.getSize()).isEqualTo((long) SEALED_ENTRIES * ENTRY_BYTES);
                ledger.offloadPrefix(ledger.getLastConfirmedEntry());
                info = ledger.getLedgersInfo().get(firstLedger);
                assertThat(info.getOffloadContext().isComplete()).isTrue();
                assertThat(info.getOffloadContext().isBookkeeperDeleted()).isFalse();
                assertThat(OffloadUtils.getOffloadDriverMetadata(info))
                        .containsAllEntriesOf(offloader.getOffloadDriverMetadata());
                var attempt = new UUID(
                        info.getOffloadContext().getUidMsb(),
                        info.getOffloadContext().getUidLsb());
                var keys = PulsarOffloadKeysV1.derive("cells/m5-native", firstLedger, attempt);
                var rootHead = independent.headObject(HeadObjectRequest.builder()
                        .bucket(BUCKET)
                        .key(keys.rootKey())
                        .build());
                assertThat(rootHead.versionId()).isNotBlank().isNotEqualTo("null");
                byte[] rootBytes;
                try (var root = independent.getObject(GetObjectRequest.builder()
                        .bucket(BUCKET)
                        .key(keys.rootKey())
                        .versionId(rootHead.versionId())
                        .build())) {
                    rootBytes = root.readAllBytes();
                }
                var root = Npo1CodecV1.parseCanonical(rootBytes, LIMITS);
                assertThat(Npo1CodecV1.rootSha256(rootBytes))
                        .isEqualTo(rootHead.metadata().get("nereus-sha256"));
                assertThat(root.attempt().attemptUuid()).isEqualTo(attempt);
                var dataHead = independent.headObject(HeadObjectRequest.builder()
                        .bucket(BUCKET)
                        .key(keys.dataKey())
                        .build());
                assertThat(dataHead.versionId()).isNotBlank().isNotEqualTo("null");
                assertThat(root.dataExtent().immutableVersion()).isEqualTo("version:" + dataHead.versionId());
                assertThat(root.sealedLedger().ensembleSize()).isEqualTo(3);
                assertThat(root.sealedLedger().writeQuorum()).isEqualTo(3);
                assertThat(root.sealedLedger().entryCount()).isEqualTo(SEALED_ENTRIES);
                System.out.println("NATIVE_PULSAR_OFFLOAD ledger=" + firstLedger + " attempt=" + attempt
                        + " entries=" + SEALED_ENTRIES + " bytes=" + info.getSize()
                        + " rootVersion=" + rootHead.versionId() + " dataVersion=" + dataHead.versionId());
                return new Offloaded(firstLedger, attempt, rootHead.versionId(), dataHead.versionId());
            } finally {
                try {
                    if (ledger != null) {
                        ledger.close();
                    }
                } finally {
                    factory.shutdown();
                }
            }
        }
    }

    private static void readSubscriptionCold(NereusPulsarLedgerOffloaderV1 offloader, Offloaded source)
            throws Exception {
        try (var metadata = metadata();
                var bk = bookKeeper()) {
            var factory = factory(metadata, bk);
            ManagedLedgerImpl ledger = null;
            try {
                ledger = (ManagedLedgerImpl) factory.open(LEDGER, config(offloader));
                var info = ledger.getLedgersInfo().get(source.ledgerId());
                assertThat(info.getOffloadContext().isComplete()).isTrue();
                assertThat(new UUID(
                                info.getOffloadContext().getUidMsb(),
                                info.getOffloadContext().getUidLsb()))
                        .isEqualTo(source.attempt());
                var cursor = ledger.openCursor(SUBSCRIPTION, InitialPosition.Earliest);
                int ordinal = 0;
                while (ordinal <= SEALED_ENTRIES) {
                    var entries = cursor.readEntries(SEALED_ENTRIES + 1 - ordinal);
                    assertThat(entries).isNotEmpty();
                    try {
                        for (Entry entry : entries) {
                            assertThat(entry.getData()).isEqualTo(payload(ordinal++));
                        }
                    } finally {
                        entries.forEach(Entry::release);
                    }
                }
                assertThat(ordinal).isEqualTo(SEALED_ENTRIES + 1);
                assertThat(cursor.getNumberOfEntriesInBacklog(false)).isEqualTo(SEALED_ENTRIES + 1L);
                try (var original = bk.newOpenLedgerOp()
                        .withLedgerId(source.ledgerId())
                        .withRecovery(false)
                        .withDigestType(DigestType.CRC32C)
                        .withPassword(new byte[0])
                        .execute()
                        .get(30, TimeUnit.SECONDS)) {
                    assertThat(original.isClosed()).isTrue();
                    assertThat(original.getLastAddConfirmed()).isEqualTo(SEALED_ENTRIES - 1L);
                }
            } finally {
                try {
                    if (ledger != null) {
                        ledger.close();
                    }
                } finally {
                    factory.shutdown();
                }
            }
        }
    }

    private static ManagedLedgerConfig config(NereusPulsarLedgerOffloaderV1 offloader) {
        // Match the actual fixed 3/3/2 BK creation capability for fresh drain conformance and factory lifetimes.
        var config = new ManagedLedgerConfig()
                .setEnsembleSize(3)
                .setWriteQuorumSize(3)
                .setAckQuorumSize(2)
                .setDigestType(DigestType.CRC32C)
                .setPassword("")
                .setMaxEntriesPerLedger(SEALED_ENTRIES)
                .setRetentionTime(1, TimeUnit.HOURS)
                .setLedgerOffloader(offloader);
        if (M6_BROKER_PROVIDER_SCOPE != null) {
            config.setReadEntryTimeoutSeconds(Integer.getInteger("nereus.m6.brokerReadTimeoutSeconds", 0));
        }
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        return config;
    }

    private static PulsarNativeManagedLedgerFactoryV2 guardedMappedFactory(
            PulsarNativeBindingV1 binding,
            MetadataStoreExtended metadata,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            NereusPulsarLedgerOffloaderV1 offloader,
            PulsarNativeReadCellBudgetV1 budget,
            java.util.concurrent.Executor worker,
            SelectorResponseFault responses)
            throws Exception {
        return guardedMappedFactory(
                binding, metadata, bootstrap, offloader, budget, worker, responses, Optional.empty());
    }

    private static PulsarNativeManagedLedgerFactoryV2 guardedMappedFactory(
            PulsarNativeBindingV1 binding,
            MetadataStoreExtended metadata,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            NereusPulsarLedgerOffloaderV1 offloader,
            PulsarNativeReadCellBudgetV1 budget,
            java.util.concurrent.Executor worker,
            SelectorResponseFault responses,
            Optional<com.nereusstream.storage.bookkeeper.M5PulsarNativeCreateSpecV1.BookKeeperGcGraceV1> grace)
            throws Exception {
        var selected = binding.m4Control().readSelectedMaterialization(worker).get(30, TimeUnit.SECONDS);
        var exact = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(responses);
        var aggregate = new com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1(exact);
        String key = new OxiaV2AuthorityKeys(binding.authorityRoot())
                .aggregateKey(binding.authority().aggregate().binding().incarnationIdentity());
        var registry = nativeExpectedWriterRegistry(
                new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding(
                        1, bootstrap.capability().configurationDigest()));
        var config = new ManagedLedgerFactoryConfig();
        config.setMaxCacheSize(0);
        return PulsarNativeManagedLedgerFactoryV2.open(
                        binding,
                        metadata,
                        "zk://" + System.getProperty("nereus.m5.nativeOffloadZk") + "/ledgers",
                        bootstrap,
                        budget,
                        NATIVE_BK_CANONICAL_CAPACITY,
                        config,
                        registry,
                        aggregate,
                        key,
                        offloader,
                        selected.providerProfile().physicalBinding().physicalNamespace(),
                        selected.providerProfile().maximumObjectBytes(),
                        worker,
                        grace)
                .get(60, TimeUnit.SECONDS);
    }

    private static ManagedLedgerFactoryImpl factory(MetadataStoreExtended metadata, BookKeeper bk) throws Exception {
        var config = new ManagedLedgerFactoryConfig();
        config.setMaxCacheSize(0);
        return new ManagedLedgerFactoryImpl(metadata, bk, config);
    }

    private static MetadataStoreExtended metadata() throws Exception {
        return MetadataStoreExtended.create(
                "zk:" + System.getProperty("nereus.m5.nativeOffloadZk"),
                MetadataStoreConfig.builder()
                        .metadataStoreName("m5-native-pulsar")
                        .build());
    }

    private static BookKeeperCapabilitySnapshotV1 bookKeeperCapability() {
        int frameLimit = 5_242_880;
        return new BookKeeperCapabilitySnapshotV1(
                new CellProviderScopeId(
                        M6_BROKER_PROVIDER_SCOPE == null
                                ? Sha256Digest.hash(
                                        CanonicalBytes.copyOf("m5-native-pulsar-bk".getBytes(StandardCharsets.UTF_8)))
                                : Sha256Digest.copyOf(java.util.HexFormat.of().parseHex(M6_BROKER_PROVIDER_SCOPE))),
                "cd06340851d6d657b7c7546df01df365c18980de",
                Sha256Digest.copyOf(java.util.HexFormat.of()
                        .parseHex("8e64f2b7436bb814705f611eb0ac48d64d90de7a50d295905c459d89bc3f9d8f")),
                "cd06340851d6d657b7c7546df01df365c18980de",
                Sha256Digest.copyOf(java.util.HexFormat.of()
                        .parseHex("c0a128931c402d6bf6a6f973ba2f305b9be261659e30754ab95a29510a33bc0d")),
                BookKeeperProtocolModeV1.V3,
                frameLimit,
                frameLimit,
                BookKeeperV3Crc32cAddPayloadLimitV1.maximumAddPayloadBytes(frameLimit, frameLimit),
                true,
                3,
                3,
                2,
                BookKeeperDigestTypeV1.CRC32C,
                true,
                true,
                new BookKeeperTimeoutClassV1(10_000, 5_000, 5_000, 30_000),
                "bk-k0-no-auth:v1",
                Sha256Digest.copyOf(java.util.HexFormat.of()
                        .parseHex("eaf41c4b42b767b8ea6e86023a784425b8073f174dbade92b4249c8f3d301dbd")));
    }

    private static BookKeeper bookKeeper() throws Exception {
        return new BookKeeper(new ClientConfiguration()
                .setUseV2WireProtocol(false)
                .setBatchReadEnabled(false)
                .setMetadataServiceUri("zk://" + System.getProperty("nereus.m5.nativeOffloadZk") + "/ledgers"));
    }

    private static byte[] payload(int ordinal) {
        var metadata = new MessageMetadata()
                .setProducerName("m5-native-producer")
                .setSequenceId(ordinal)
                .setPublishTime(1_791_177_600_000L + ordinal);
        // CRC32C magic/checksum plus metadata-size prefix are ten bytes in the native Pulsar frame.
        byte[] value = new byte[ENTRY_BYTES - 10 - metadata.getSerializedSize()];
        Arrays.fill(value, (byte) ordinal);
        var body = Unpooled.wrappedBuffer(value);
        try {
            var frame = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata, body);
            try {
                assertThat(frame.readableBytes()).isEqualTo(ENTRY_BYTES);
                return ByteBufUtil.getBytes(frame);
            } finally {
                frame.release();
            }
        } finally {
            body.release();
        }
    }

    private static S3Client client(AtomicInteger reads) {
        return client(reads, null, null, null);
    }

    private static S3Client client(
            AtomicInteger reads,
            AtomicReference<SdkHold> heldGet,
            AtomicReference<SdkHold> heldPut,
            AtomicReference<VersionProbe> versionProbe) {
        return client(reads, heldGet, heldPut, versionProbe, false);
    }

    private static S3Client client(
            AtomicInteger reads,
            AtomicReference<SdkHold> heldGet,
            AtomicReference<SdkHold> heldPut,
            AtomicReference<VersionProbe> versionProbe,
            boolean rangeOnlyHold) {
        var builder = S3Client.builder()
                .endpointOverride(URI.create(System.getProperty("nereus.m5.nativeOffloadEndpoint")))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("nereus-m5-drain", "nereus-m5-drain-secret")));
        if (reads != null) {
            builder.overrideConfiguration(config -> config.addExecutionInterceptor(new ExecutionInterceptor() {
                @Override
                public void afterExecution(Context.AfterExecution context, ExecutionAttributes attributes) {
                    VersionProbe probe = versionProbe == null ? null : versionProbe.get();
                    if (probe != null) {
                        if (context.request() instanceof HeadObjectRequest head
                                && head.key().equals(probe.key)
                                && probe.afterHead != null
                                && probe.headReplaced.compareAndSet(false, true)) {
                            probe.afterHead.run();
                        } else if (context.request() instanceof GetObjectRequest get
                                && get.key().equals(probe.key)) {
                            probe.returnedVersion.set(((GetObjectResponse) context.response()).versionId());
                        }
                    }
                    SdkHold hold = null;
                    if (context.request() instanceof GetObjectRequest get) {
                        reads.incrementAndGet();
                        if (heldGet != null && (!rangeOnlyHold || get.range() != null)) {
                            var candidate = heldGet.get();
                            if (candidate != null
                                    && candidate
                                            .expectedKey()
                                            .filter(key -> !key.equals(get.key()))
                                            .isEmpty()
                                    && heldGet.compareAndSet(candidate, null)) {
                                hold = candidate;
                            }
                        }
                        if (hold != null) {
                            hold.key().set(get.key());
                            hold.version().set(((GetObjectResponse) context.response()).versionId());
                        }
                    } else if (context.request() instanceof PutObjectRequest put) {
                        hold = heldPut == null ? null : heldPut.getAndSet(null);
                        if (hold != null) {
                            hold.key().set(put.key());
                            hold.version().set(((PutObjectResponse) context.response()).versionId());
                        }
                    }
                    if (hold != null) {
                        hold.started().countDown();
                        try {
                            if (!hold.released().await(30, TimeUnit.SECONDS)) {
                                throw new IllegalStateException("held native SDK response was not released");
                            }
                        } catch (InterruptedException failure) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException("held native SDK response was interrupted", failure);
                        }
                        if (hold.failure().get() != null) {
                            throw hold.failure().get();
                        }
                    }
                }
            }));
        }
        return builder.build();
    }

    private static AttemptKeyManager keyManager() {
        return new AttemptKeyManager() {
            @Override
            public PreparedAttemptKey create(long ledgerId, UUID attempt, Map<String, String> metadata) {
                return new PreparedAttemptKey(KEY, ENVELOPE);
            }

            @Override
            public SecretKey unwrap(
                    long ledgerId, UUID attempt, AttemptKeyEnvelope envelope, Map<String, String> metadata) {
                assertThat(envelope).isEqualTo(ENVELOPE);
                try {
                    var cipher = Cipher.getInstance("AESWrap");
                    cipher.init(Cipher.UNWRAP_MODE, WRAPPING_KEY);
                    return (SecretKey) cipher.unwrap(envelope.wrappedKey(), "AES", Cipher.SECRET_KEY);
                } catch (GeneralSecurityException failure) {
                    throw new IllegalStateException("cannot unwrap native test key", failure);
                }
            }
        };
    }

    private static AttemptKeyEnvelope wrappedKey() {
        try {
            var cipher = Cipher.getInstance("AESWrap");
            cipher.init(Cipher.WRAP_MODE, WRAPPING_KEY);
            return new AttemptKeyEnvelope(
                    1, "test-kms", "cells/m5-native/kms", "version-1", "aes-kw", cipher.wrap(KEY));
        } catch (GeneralSecurityException failure) {
            throw new ExceptionInInitializerError(failure);
        }
    }

    private static final class VersionProbe {
        private final String key;
        private final Runnable afterHead;
        private final AtomicBoolean headReplaced = new AtomicBoolean();
        private final AtomicReference<String> returnedVersion = new AtomicReference<>();

        private VersionProbe(String key, Runnable afterHead) {
            this.key = key;
            this.afterHead = afterHead;
        }
    }

    private record Offloaded(long ledgerId, UUID attempt, String rootVersion, String dataVersion) {}

    private static final class NativeReadStats extends NullStatsLogger {
        private final AtomicInteger completions = new AtomicInteger();

        @Override
        public OpStatsLogger getOpStatsLogger(String name) {
            var delegate = super.getOpStatsLogger(name);
            if (!name.equals(BookKeeperClientStats.READ_OP)) {
                return delegate;
            }
            return new OpStatsLogger() {
                @Override
                public void registerFailedEvent(long latency, TimeUnit unit) {
                    completions.incrementAndGet();
                }

                @Override
                public void registerSuccessfulEvent(long latency, TimeUnit unit) {
                    completions.incrementAndGet();
                }

                @Override
                public void registerSuccessfulValue(long value) {
                    delegate.registerSuccessfulValue(value);
                }

                @Override
                public void registerFailedValue(long value) {
                    delegate.registerFailedValue(value);
                }

                @Override
                public OpStatsData toOpStatsData() {
                    return delegate.toOpStatsData();
                }

                @Override
                public void clear() {
                    delegate.clear();
                }
            };
        }
    }

    private static SdkHold hold() {
        return hold(null);
    }

    private static SdkHold hold(String expectedKey) {
        return new SdkHold(
                new CountDownLatch(1),
                new CountDownLatch(1),
                new AtomicReference<>(),
                new AtomicReference<>(),
                new AtomicReference<>(),
                Optional.ofNullable(expectedKey));
    }

    private static final class QualificationGetHold {
        private final AtomicBoolean armed = new AtomicBoolean();
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CompletableFuture<Void> release = new CompletableFuture<>();
    }

    private record SdkHold(
            CountDownLatch started,
            CountDownLatch released,
            AtomicReference<String> key,
            AtomicReference<String> version,
            AtomicReference<RuntimeException> failure,
            Optional<String> expectedKey) {}
}
