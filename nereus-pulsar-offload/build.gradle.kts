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

dependencies {
    api(project(":nereus-domain"))
    api(project(":nereus-storage-object"))
    api(project(":nereus-metadata-spi"))
    api(project(":nereus-metadata-oxia"))
    api(project(":nereus-storage-object-s3"))
    api(project(":nereus-storage-bookkeeper"))
    implementation(libs.zstd.jni)
    api(platform(libs.aws.sdk.v2.bom))
    api(libs.aws.sdk.v2.s3)
    compileOnlyApi(libs.pulsar.managed.ledger)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(libs.pulsar.managed.ledger)
    testRuntimeOnly(libs.junit.platform.launcher)
}

// This current P1/NV consumer needs the current domain API. The BK Storage API retains its historical N1 edge.
configurations.configureEach {
    resolutionStrategy.preferProjectModules()
}

val p6ProviderTest by sourceSets.creating {
    java.srcDir("src/p6ProviderTest/java")
    compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
    runtimeClasspath += output + compileClasspath
}

configurations[p6ProviderTest.implementationConfigurationName]
    .extendsFrom(configurations.testImplementation.get())
configurations[p6ProviderTest.runtimeOnlyConfigurationName]
    .extendsFrom(configurations.testRuntimeOnly.get())

dependencies {
    add(p6ProviderTest.implementationConfigurationName, platform(libs.aws.sdk.v2.bom))
    add(p6ProviderTest.implementationConfigurationName,
        project(path = ":nereus-storage-bookkeeper", configuration = "m5NativeDeleteFaultFixtures"))
    add(p6ProviderTest.implementationConfigurationName, libs.aws.sdk.v2.s3)
    add(p6ProviderTest.implementationConfigurationName, libs.testcontainers.junit.jupiter)
    add(p6ProviderTest.implementationConfigurationName, libs.testcontainers.localstack)
}

tasks.register<Test>("p6ProviderTest") {
    group = "verification"
    description = "Run the explicit M2-P6 S3-compatible provider evidence suite."
    testClassesDirs = p6ProviderTest.output.classesDirs
    classpath = p6ProviderTest.runtimeClasspath
    useJUnitPlatform()
    include("**/S3PulsarOffloadObjectStoreV1Test.class")
    maxParallelForks = 1
    outputs.upToDateWhen { false }
}

tasks.register<Test>("v2M5S3ProviderDrainRealTest") {
    group = "verification"
    description = "Verify Pulsar S3 provider shutdown against actual held native I/O."
    testClassesDirs = p6ProviderTest.output.classesDirs
    classpath = p6ProviderTest.runtimeClasspath
    useJUnitPlatform()
    include("**/M5S3PulsarObjectStoreDrainTest.class")
    maxParallelForks = 1
    outputs.upToDateWhen { false }
    doFirst {
        systemProperty("nereus.m5.s3DrainEndpoint", providers.gradleProperty("v2M5S3DrainEndpoint").get())
    }
}

tasks.register<Test>("v2M5NativeOffloadRealTest") {
    group = "verification"
    description = "Verify native ManagedLedger offload, byte admission and cold subscription reads."
    testClassesDirs = p6ProviderTest.output.classesDirs
    classpath = p6ProviderTest.runtimeClasspath
    useJUnitPlatform()
    include("**/M5NativePulsarOffloadRealTest.class")
    filter {
        if (providers.gradleProperty("v2M5NativeM4RuntimeOutput").isPresent) {
            includeTestsMatching("*.nativeBindingForM4Conformance")
            includeTestsMatching("*.nativePreferredOnlySourceSurvivesMissingBookKeeperMetadata")
        } else {
            includeTestsMatching("*.nativeBindingCapturesExactP1OwnerAndObjectNamespace")
        }
        includeTestsMatching("*.nativeOffloadBoundsFallbackReadsAndColdSubscriptionKeepsBacklog")
        includeTestsMatching("*.nativeTimedOutDataCreationRetainsActualSdkOwnership")
        includeTestsMatching("*.nativeTimedOutCursorCreationRetainsActualSdkOwnership")
    }
    maxParallelForks = 1
    maxHeapSize = "768m"
    outputs.upToDateWhen { false }
    doFirst {
        systemProperty("nereus.m5.nativeOffloadEndpoint", providers.gradleProperty("v2M5NativeOffloadEndpoint").get())
        systemProperty("nereus.m5.nativeOffloadZk", providers.gradleProperty("v2M5NativeOffloadZk").get())
        systemProperty("nereus.m5.nativeOffloadOxia", providers.gradleProperty("v2M5NativeOffloadOxia").get())
        providers.gradleProperty("v2M5NativeM4RuntimeOutput").orNull?.let {
            systemProperty("nereus.m5.nativeM4RuntimeOutput", it)
        }
        providers.gradleProperty("v2M5NativeOffloadRetentionClass").orNull?.let {
            systemProperty("nereus.m5.nativeOffloadRetentionClass", it)
        }
    }
}

tasks.register<Test>("v2M5NativeBkDeleteSdkDrainRealTest") {
    group = "verification"
    description = "Verify the existing Native SDK drain recipe with explicit DELETE_AFTER_VERIFIED configuration."
    testClassesDirs = p6ProviderTest.output.classesDirs
    classpath = p6ProviderTest.runtimeClasspath
    useJUnitPlatform()
    include("**/M5NativePulsarOffloadRealTest.class")
    filter {
        includeTestsMatching("*.nativeBindingForM4Conformance")
        includeTestsMatching("*.nativePreferredOnlySourceSurvivesMissingBookKeeperMetadata")
        includeTestsMatching("*.nativeOffloadBoundsFallbackReadsAndColdSubscriptionKeepsBacklog")
        includeTestsMatching("*.nativeTimedOutDataCreationRetainsActualSdkOwnership")
        includeTestsMatching("*.nativeTimedOutCursorCreationRetainsActualSdkOwnership")
    }
    maxParallelForks = 1
    maxHeapSize = "768m"
    outputs.upToDateWhen { false }
    doFirst {
        systemProperty("nereus.m5.nativeOffloadEndpoint", providers.gradleProperty("v2M5NativeOffloadEndpoint").get())
        systemProperty("nereus.m5.nativeOffloadZk", providers.gradleProperty("v2M5NativeOffloadZk").get())
        systemProperty("nereus.m5.nativeOffloadOxia", providers.gradleProperty("v2M5NativeOffloadOxia").get())
        systemProperty("nereus.m5.nativeM4RuntimeOutput", providers.gradleProperty("v2M5NativeM4RuntimeOutput").get())
        systemProperty("nereus.m5.nativeOffloadRetentionClass", "DELETE_AFTER_VERIFIED")
    }
}

tasks.register<Test>("v2M5NativeM4OwnerRealTest") {
    group = "verification"
    description = "Verify actual Native M4 selector/hazard admission from explicit current SDK drain conformance."
    testClassesDirs = p6ProviderTest.output.classesDirs
    classpath = p6ProviderTest.runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("*.nativeM4OwnerHoldsActualCleanupAndBorrowedAliases")
        includeTestsMatching("*.nativeM4CleanupFailureRetainsHazard")
        includeTestsMatching("*.nativeM4OrdinaryReadCannotFallbackAfterCutChanges")
        includeTestsMatching("*.nativeM4PreferredOnlyOwnerRejectsBookKeeperFallback")
        includeTestsMatching("*.nativeM4FallbackRetiresAfterActualGetAndAliasDrain")
        includeTestsMatching("*.nativeM4FallbackUnknownRetainsProtectionAndRetriesExact")
        includeTestsMatching("*.nativeM4SourceMapUnknownCannotPublishSelector")
        includeTestsMatching("*.nativeM4ColdSourceUsesPublishedObjectVersions")
        includeTestsMatching("*.nativeM4ColdOwnerRequiresActualDurableDrain")
        includeTestsMatching("*.nativeM4ColdOwnerUnknownRetainsUniqueAttempt")
        includeTestsMatching("*.nativeM4CellSharesRetainUnknownReaderAndHealthyBinding")
        includeTestsMatching("*.nativeSourcePreparationRetainsHeldRootOpen")
        includeTestsMatching("*.nativePreparedSourceRequiresGrantAndDrainsWithOwner")
        includeTestsMatching("*.nativeM4VerificationUsesCachedVersionsAndPrivateDrain")
        includeTestsMatching("*.nativeM4CancelledVerificationCannotClearSdkLease")
        includeTestsMatching("*.nativeM4CompletedVerificationObserverCannotClearSdkLease")
        includeTestsMatching("*.nativeCellAllocatorRejectsIndependentOwnerUntilActualDrain")
        includeTestsMatching("*.nativeCellAllocatorUnknownDrainRetainsOriginalOwner")
        includeTestsMatching("*.nativeCellReservationUnknownDoesNotDispatchSource")
        includeTestsMatching("*.nativeCellReservationReleaseUnknownRetainsLocalCharge")
        includeTestsMatching("*.nativeInitialOwnerUnknownRetainsExactCleanup")
        includeTestsMatching("*.nativeSuccessorOwnerUnknownRetainsExactCleanup")
        includeTestsMatching("*.nativeGuardedPulsarCreatesDurableBirthsAndMembers")
        includeTestsMatching("*.nativeGuardedPulsarBirthUnknownCannotCreatePhysicalLedger")
        includeTestsMatching("*.nativeMetadataCleanupUnknownRetainsOriginalSdkCapacity")
        includeTestsMatching("*.nativeMetadataTicketUnknownRetainsOriginalNoDispatchCleanup")
        includeTestsMatching("*.nativeMetadataWriteUnknownUsesActualVersionFence")
        includeTestsMatching("*.nativeMetadataWriteUnknownKeepsOriginalProofAcrossCleanupLoss")
        includeTestsMatching("*.nativeMetadataWriteUnknownRejectsChangedCreationIdentity")
        includeTestsMatching("*.nativeMetadataCreateUnknownReconcilesOriginalCreatedIdentity")
        includeTestsMatching("*.nativeMetadataCreateFencedAbsenceExcludesLateCreation")
        includeTestsMatching("*.nativeMetadataCreateUnknownRejectsForeignCreator")
        includeTestsMatching("*.nativeMetadataCreatePreDispatchReadFailureReconcilesOriginalTicket")
        includeTestsMatching("*.nativeMaterializationSourceUsesActualChainAndEveryEntry")
        includeTestsMatching("*.nativeMaterializationSourceClosesFromOriginalDeliveryCallback")
        includeTestsMatching("*.nativeMaterializationQualificationGetRetainsWorkspaceThroughClose")
        includeTestsMatching("*.nativeMaterializationConnectionRetainsOriginalSdkUntilActualGetDrain")
        includeTestsMatching("*.nativeMaterializationInputPersistsExactP1ViewAndRecoveryReferences")
        includeTestsMatching("*.nativeMaterializationInputUnknownRetainsOriginalCellChargeUntilExactRecovery")
        includeTestsMatching("*.nativeMaterializationProviderScopeUsesOriginalSdkAndFrozenCellPrefix")
        includeTestsMatching("*.nativeMaterializationProviderScopeUnknownRetainsOriginalSourceAndCellCharge")
        includeTestsMatching("*.nativeMaterializationTaskBindsOriginalPlanScopeOwnerAndSharedCellCharge")
        includeTestsMatching("*.nativeMaterializationTaskPlanUnknownRetainsOriginalSourceAndCellCharge")
        includeTestsMatching("*.nativeMaterializationTaskCapacityRejectsBeforePlanAndOutput")
        includeTestsMatching("*.nativeMaterializationTaskCurrentReadRetainsOriginalSourceUntilDrain")
        includeTestsMatching("*.nativeMaterializationTaskUnknownRetainsExclusiveSourceThenAdvancesOriginalWorker")
        includeTestsMatching("*.nativeMaterializationPendingInventoryUnknownRetainsOriginalKeysAndCharge")
        includeTestsMatching("*.nativeMaterializationBirthCreditUnknownRetainsOriginalCapacityUntilNoDispatchDrain")
        includeTestsMatching("*.nativeMaterializationBirthCapacityRejectsBeforeAnyProviderOutput")
        includeTestsMatching("*.nativeMaterializationWritesRealVersionsAndIndependentlyVerifiesEveryEntry")
        includeTestsMatching("*.nativeMaterializationWriteIntentUnknownCannotDispatchProviderOutput")
        includeTestsMatching("*.nativeMaterializationCancelledOutputObserverRetainsActualPutUntilDrain")
        includeTestsMatching("*.nativeMaterializationActualPutReplyLossUsesOriginalImmutableVersion")
        includeTestsMatching("*.nativeMaterializationPartialCutRetainsOutsideEntryAndActiveTailRoutes")
        includeTestsMatching("*.nativePartialMaterializationColdFactoryRetainsOriginalSourceDependencies")
        includeTestsMatching("*.nativeMaterializationMappedGetObserverCancelRetainsSdkHazardAndCellUntilDrain")
        includeTestsMatching("*.nativeMaterializationPublishesActualManifestAfterOriginalOwnerDrain")
        includeTestsMatching("*.nativeMaterializationAppliedPublicationUnknownRetainsOriginalCandidateUntilHandoff")
        includeTestsMatching("*.nativeMaterializationStopBeforeSelectorDrainsOriginalPhysicalTickets")
        includeTestsMatching("*.nativeRecoveryAddReplyLossRetainsOriginalSdkReferencesAfterFence")
        includeTestsMatching("*.nativeExplicitLacReplyLossRetainsOriginalSdkReferencesAfterFence")
        includeTestsMatching("*.nativeCursorResetReplyLossRetainsOriginalCAndDReferences")
        includeTestsMatching("*.nativeCursorResetRejectsCommittedTrimBeforeOriginalSdkDispatch")
        includeTestsMatching("*.nativeReadOwnerPinsCommittedTrimThroughOriginalEntryDrain")
        includeTestsMatching("*.nativeCursorFloorScansActualCheckpointInsideSingleAuthorityFence")
        includeTestsMatching("*.nativeOnlyFirstAdmissionPersistsPolicyAndFencesKafkaFloors")
        includeTestsMatching("*.nativeOnlyTaskTerminalReferencesRequireOriginalCellDrain")
        includeTestsMatching("*.nativeBookKeeperReplacementRequiresExactReleaseAndReconcilesUnknown")
        includeTestsMatching("*.nativeBookKeeperExpiryRequiresTypedTrimAndRetainsActiveBacklog")
        includeTestsMatching("*.nativeCursorTrimColdCommitRecoversDurableSnapshotAfterAppliedReplyLoss")
        includeTestsMatching("*.nativeEnrolledReadOwnerRejectsMissingControlWriterBeforeGrant")
        includeTestsMatching("*.nativeEnrolledColdOwnerKeepsActualP1ReadTicketUntilEntryDrain")
        includeTestsMatching("*.nativeEnrolledColdOwnerUnknownKeepsOriginalGrantAndCell")
        includeTestsMatching("*.nativeObjectOnlyCursorAndGenerationFloorsSurviveMissingOriginalBookKeeper")
        includeTestsMatching("*.nativeCancelledInitialOwnerRetainsOriginalGrantCleanup")
        includeTestsMatching("*.nativeCompletedInitialOwnerRetainsOriginalGrantCleanup")
        includeTestsMatching("*.nativeInitialGrantUnknownWithoutActorRetainsQuarantine")
        includeTestsMatching("*.nativeInitialClaimRejectsConcurrentInvocation")
        includeTestsMatching("*.nativeInitialClaimUnknownAndAbortUnknownKeepOriginalCharge")
        includeTestsMatching("*.nativeInitialClaimMissingHeadCannotReinitialize")
        includeTestsMatching("*.nativeActorlessGrantReconcilesActualDrainAndColdOwner")
    }
    maxParallelForks = 1
    maxHeapSize = "768m"
    outputs.upToDateWhen { false }
    doFirst {
        systemProperty("nereus.m5.nativeOffloadEndpoint", providers.gradleProperty("v2M5NativeOffloadEndpoint").get())
        systemProperty("nereus.m5.nativeOffloadZk", providers.gradleProperty("v2M5NativeOffloadZk").get())
        systemProperty("nereus.m5.nativeOffloadOxia", providers.gradleProperty("v2M5NativeOffloadOxia").get())
        systemProperty("nereus.m5.nativeM4AdmissionFile", providers.gradleProperty("v2M5NativeM4AdmissionFile").get())
        systemProperty("nereus.m5.nativeM4AdmissionSha256", providers.gradleProperty("v2M5NativeM4AdmissionSha256").get())
        providers.gradleProperty("v2M5NativeBkGcAdmissionFile").orNull?.let {
            systemProperty("nereus.m5.nativeBkGcAdmissionFile", it)
        }
        providers.gradleProperty("v2M5NativeBkGcAdmissionSha256").orNull?.let {
            systemProperty("nereus.m5.nativeBkGcAdmissionSha256", it)
        }
    }
}

tasks.register<Test>("p6EvidenceTest") {
    group = "verification"
    description = "Generate the M2-P6 S3-compatible candidate matrix evidence receipt."
    testClassesDirs = p6ProviderTest.output.classesDirs
    classpath = p6ProviderTest.runtimeClasspath
    useJUnitPlatform()
    include("**/PulsarP6CandidateEvidenceTest.class")
    maxParallelForks = 1
    maxHeapSize = "2048m"
    outputs.upToDateWhen { false }
    systemProperty(
        "nereus.p6.evidenceOutput",
        providers.gradleProperty("v2M2PulsarP6EvidenceOutput")
            .getOrElse(layout.buildDirectory.file("p6-evidence/pulsar-p6-candidate.json").get().asFile.absolutePath),
    )
    systemProperty(
        "nereus.p6.testedSourceCommit",
        providers.gradleProperty("v2M2PulsarP6TestedSourceCommit").getOrElse("UNSET"),
    )
    systemProperty(
        "nereus.p6.pulsarSourceCommit",
        providers.gradleProperty("v2M2PulsarP6PulsarSourceCommit").getOrElse("UNSET"),
    )
}

tasks.register<Test>("p6RealProviderTest") {
    group = "verification"
    description = "Run M2-P6 against the fixed external MinIO S3-compatible provider."
    testClassesDirs = p6ProviderTest.output.classesDirs
    classpath = p6ProviderTest.runtimeClasspath
    useJUnitPlatform()
    include("**/P6MinioProviderEvidenceTest.class")
    maxParallelForks = 1
    maxHeapSize = "1024m"
    outputs.upToDateWhen { false }
    systemProperty(
        "nereus.p6.realProviderOutput",
        providers.gradleProperty("v2M2PulsarP6RealProviderOutput")
            .getOrElse(layout.buildDirectory.file("p6-evidence/minio-provider.json").get().asFile.absolutePath),
    )
    systemProperty(
        "nereus.p6.testedSourceCommit",
        providers.gradleProperty("v2M2PulsarP6TestedSourceCommit").getOrElse("UNSET"),
    )
    systemProperty(
        "nereus.p6.minioImageReference",
        providers.gradleProperty("v2M2PulsarP6MinioImageReference").getOrElse("UNSET"),
    )
    systemProperty(
        "nereus.p6.minioImageDigest",
        providers.gradleProperty("v2M2PulsarP6MinioImageDigest").getOrElse("UNSET"),
    )
}

tasks.named<Test>("test") {
    maxHeapSize = "1024m"
}

val v2M3PulsarNativeReceiptOutput = providers.gradleProperty("v2M3PulsarNativeReceiptOutput")
val v2M3PulsarTestedSourceCommit = providers.gradleProperty("v2M3PulsarTestedSourceCommit")
val v2M3PulsarSourceRepository = providers.gradleProperty("v2M3PulsarSourceRepository")
val v2M3PulsarSourceCommit = providers.gradleProperty("v2M3PulsarSourceCommit")
val v2M3PulsarTestStartedAtUtc = providers.gradleProperty("v2M3PulsarTestStartedAtUtc")
val v2M3PulsarTestFinishedAtUtc = providers.gradleProperty("v2M3PulsarTestFinishedAtUtc")

tasks.register<JavaExec>("v2M3PulsarNativeReceiptEmit") {
    group = "verification"
    description = "Emit the source/JUnit-bound M3 Pulsar Object-WAL raw native receipt to an explicit untracked path."
    dependsOn(tasks.named("test"), tasks.named("classes"))
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalNativeResultV1")
    outputs.upToDateWhen { false }
    doFirst {
        val output = v2M3PulsarNativeReceiptOutput.orNull
            ?: throw GradleException("-Pv2M3PulsarNativeReceiptOutput is required")
        val outputPath = file(output).toPath().toAbsolutePath().normalize()
        val repositoryPath = rootProject.projectDir.toPath().toAbsolutePath().normalize()
        val buildPath = rootProject.layout.buildDirectory.get().asFile.toPath().toAbsolutePath().normalize()
        val moduleBuildPath = layout.buildDirectory.get().asFile.toPath().toAbsolutePath().normalize()
        if (outputPath.startsWith(repositoryPath)
            && !outputPath.startsWith(buildPath)
            && !outputPath.startsWith(moduleBuildPath)
        ) {
            throw GradleException("M3 raw receipt output must remain outside tracked repository paths")
        }
        args(
            "generate",
            rootProject.projectDir.absolutePath,
            v2M3PulsarTestedSourceCommit.orNull
                ?: throw GradleException("-Pv2M3PulsarTestedSourceCommit is required"),
            v2M3PulsarSourceRepository.orNull
                ?: throw GradleException("-Pv2M3PulsarSourceRepository is required"),
            v2M3PulsarSourceCommit.orNull
                ?: throw GradleException("-Pv2M3PulsarSourceCommit is required"),
            v2M3PulsarTestStartedAtUtc.orNull
                ?: throw GradleException("-Pv2M3PulsarTestStartedAtUtc is required"),
            v2M3PulsarTestFinishedAtUtc.orNull
                ?: throw GradleException("-Pv2M3PulsarTestFinishedAtUtc is required"),
            outputPath.toString(),
        )
    }
}

val v2M3PulsarNativeReceiptInput = providers.gradleProperty("v2M3PulsarNativeReceiptInput")

tasks.register<JavaExec>("v2M3PulsarNativeReceiptCheck") {
    group = "verification"
    description = "Strictly parse and self-hash-check one explicit M3 Pulsar Object-WAL raw native receipt."
    dependsOn(tasks.named("classes"))
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalNativeResultV1")
    outputs.upToDateWhen { false }
    doFirst {
        args(
            "parse",
            v2M3PulsarNativeReceiptInput.orNull
                ?: throw GradleException("-Pv2M3PulsarNativeReceiptInput is required"),
        )
    }
}

tasks.register<Test>("v2M4CurrentSourcePulsarTest") {
    group = "verification"
    description = "Run fresh M4 Pulsar P4 captured-source and locator-retirement regressions."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    maxParallelForks = 1
    useJUnitPlatform()
    filter {
        includeTestsMatching(
            "com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1Test." +
                "m4CapturedActiveSourceSurvivesManifestSwitchAndNewReadUsesManifest",
        )
        includeTestsMatching(
            "com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1Test." +
                "m4OuterHazardAloneClosesCaptureToP4InnerPinRetirementRace",
        )
        includeTestsMatching(
            "com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1Test." +
                "m4PulsarCurrentSourceLatencyAndAllocationAreBounded",
        )
    }
    outputs.upToDateWhen { false }
}

tasks.withType<Jar>().configureEach {
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
}
