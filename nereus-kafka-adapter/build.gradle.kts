/* Licensed under the Apache License, Version 2.0 */

dependencies {
    api(project(":nereus-api"))
    implementation(project(":nereus-core"))
    implementation(project(":nereus-metadata-oxia"))
    implementation(project(":nereus-object-store"))
    implementation(project(":nereus-materialization"))
    implementation(project(":nereus-bookkeeper"))
    compileOnly(libs.kafka.clients)

    testImplementation(testFixtures(project(":nereus-metadata-oxia")))
    testImplementation(testFixtures(project(":nereus-object-store")))
    testImplementation(libs.kafka.clients)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.platform.launcher)
}

val f9ProviderIntegrationTest by sourceSets.creating {
    compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
    runtimeClasspath += output + compileClasspath
}

configurations[f9ProviderIntegrationTest.implementationConfigurationName].extendsFrom(
    configurations.testImplementation.get(),
)
configurations[f9ProviderIntegrationTest.runtimeOnlyConfigurationName].extendsFrom(
    configurations.testRuntimeOnly.get(),
)

dependencies {
    add(f9ProviderIntegrationTest.implementationConfigurationName, project())
    add(f9ProviderIntegrationTest.implementationConfigurationName, testFixtures(project(":nereus-object-store")))
    add(f9ProviderIntegrationTest.implementationConfigurationName, platform(libs.aws.sdk.v2.bom))
    add(f9ProviderIntegrationTest.implementationConfigurationName, libs.aws.sdk.v2.s3)
    add(f9ProviderIntegrationTest.implementationConfigurationName, libs.oxia.testcontainers)
    add(f9ProviderIntegrationTest.implementationConfigurationName, libs.pulsar.metadata)
    add(f9ProviderIntegrationTest.implementationConfigurationName, libs.testcontainers.junit.jupiter)
    add(f9ProviderIntegrationTest.implementationConfigurationName, libs.testcontainers.localstack)
    add(f9ProviderIntegrationTest.implementationConfigurationName, libs.junit.jupiter)
    add(f9ProviderIntegrationTest.implementationConfigurationName, libs.assertj)
    add(f9ProviderIntegrationTest.runtimeOnlyConfigurationName, libs.junit.platform.launcher)
}

tasks.register<Test>("f9M2Test") {
    group = "verification"
    description = "Run F9-M2 deterministic Kafka binding, scanner, checkpoint, and recovery contracts."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
}

tasks.register<Test>("f9M2IntegrationTest") {
    group = "verification"
    description = "Run F9-M2 adapter restart/failure-cut integration contracts."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    shouldRunAfter(tasks.test, tasks.named("f9M2Test"))
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.kafka.*IntegrationTest")
    }
}

tasks.register<Test>("f9M3CodecTest") {
    group = "verification"
    description = "Run F9-M3 byte-exact codec, partition IO, and bounded async runtime contracts."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.kafka.codec.*")
        includeTestsMatching("com.nereusstream.kafka.partition.DefaultKafkaPartitionStorageTest")
        includeTestsMatching("com.nereusstream.kafka.runtime.KafkaBoundedAppendExecutorTest")
        includeTestsMatching("com.nereusstream.kafka.runtime.KafkaAppendFailureClassifierTest")
        includeTestsMatching("com.nereusstream.kafka.runtime.KafkaStorageAdmissionTest")
        includeTestsMatching("com.nereusstream.kafka.runtime.KafkaRuntimeResourcesTest")
        includeTestsMatching("com.nereusstream.kafka.runtime.DefaultNereusKafkaRuntimeTest")
        includeTestsMatching("com.nereusstream.kafka.runtime.NereusKafkaRuntimeFactoryTest")
        includeTestsMatching("com.nereusstream.kafka.runtime.NereusKafkaObjectWalRuntimeConfigurationTest")
        includeTestsMatching("com.nereusstream.kafka.runtime.NereusKafkaObjectWalRuntimeFactoryTest")
        includeTestsMatching("com.nereusstream.kafka.partition.KafkaFetchOperationTest")
        includeTestsMatching("com.nereusstream.kafka.fetch.KafkaFetchWaveOperationTest")
        includeTestsMatching("com.nereusstream.kafka.partition.KafkaPartitionLeaderManagerTest")
        includeTestsMatching("com.nereusstream.kafka.partition.KafkaStorageProfilePolicyTest")
        includeTestsMatching("com.nereusstream.kafka.partition.DefaultKafkaPartitionStorageManagerTest")
        includeTestsMatching("com.nereusstream.kafka.partition.DefaultKafkaPartitionOpenerTest")
        includeTestsMatching("com.nereusstream.kafka.partition.KafkaListOffsetsResolverTest")
    }
}

tasks.register<Test>("f9M3ProviderIntegrationTest") {
    group = "verification"
    description = "Run the F9-M3 Object-WAL leader open/Produce/Fetch gate against real Oxia."
    testClassesDirs = f9ProviderIntegrationTest.output.classesDirs
    classpath = f9ProviderIntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.test, tasks.named("f9M3CodecTest"))
    useJUnitPlatform()
    filter {
        includeTestsMatching(
            "com.nereusstream.kafka.runtime.NereusKafkaObjectWalRuntimeIntegrationTest." +
                "activatesThenRoundTripsStableKafkaBatchThroughRealOxiaProviderGraph",
        )
    }
}

tasks.register<Test>("f9MultiBrokerTakeoverProviderIntegrationTest") {
    group = "verification"
    description =
        "Run the F9 live two-broker higher-leader-epoch takeover gate against real Oxia and Object-WAL."
    testClassesDirs = f9ProviderIntegrationTest.output.classesDirs
    classpath = f9ProviderIntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.named("f9M3ProviderIntegrationTest"))
    useJUnitPlatform()
    filter {
        includeTestsMatching(
            "com.nereusstream.kafka.runtime.NereusKafkaObjectWalRuntimeIntegrationTest." +
                "higherLeaderEpochTakesOverLiveBrokerAndRecoversCommittedKafkaBatch",
        )
    }
}

tasks.register<Test>("f9BookKeeperWalOnlyProviderIntegrationTest") {
    group = "verification"
    description = "Run the F9 BookKeeper-WAL-only leader open/Produce/Fetch gate against real Oxia and BookKeeper."
    jvmArgs("--add-opens=java.base/java.io=ALL-UNNAMED")
    testClassesDirs = f9ProviderIntegrationTest.output.classesDirs
    classpath = f9ProviderIntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.test, tasks.named("f9M3ProviderIntegrationTest"))
    useJUnitPlatform()
    filter {
        includeTestsMatching(
            "com.nereusstream.kafka.runtime.NereusKafkaObjectWalRuntimeIntegrationTest." +
                "activatesThenRoundTripsKafkaBatchThroughRealBookKeeperWalOnlyGraph",
        )
    }
}

tasks.register<Test>("f9BookKeeperLedgerDeletionProviderIntegrationTest") {
    group = "verification"
    description =
        "Run the F9 Kafka proof activation and physical BookKeeper ledger-deletion gate."
    jvmArgs("--add-opens=java.base/java.io=ALL-UNNAMED")
    testClassesDirs = f9ProviderIntegrationTest.output.classesDirs
    classpath = f9ProviderIntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.named("f9BookKeeperWalOnlyProviderIntegrationTest"))
    useJUnitPlatform()
    filter {
        includeTestsMatching(
            "com.nereusstream.kafka.runtime.NereusKafkaObjectWalRuntimeIntegrationTest." +
                "activatesKafkaProofThenPhysicallyDeletesSealedBookKeeperLedger",
        )
    }
}

tasks.register<Test>("f9M6KafkaProcessIntegrationTest") {
    group = "verification"
    description = "Run the F9 provider-backed Nereus Kafka cold-restart Produce/Fetch/ListOffsets gate."
    dependsOn(rootProject.tasks.named("phase9M6KafkaProcessRuntime"))
    testClassesDirs = f9ProviderIntegrationTest.output.classesDirs
    classpath = f9ProviderIntegrationTest.runtimeClasspath
    systemProperty(
        "nereus.kafka.fork.checkout",
        providers.gradleProperty("kafkaForkCheckout")
            .orElse(providers.environmentVariable("NEREUS_KAFKA_FORK_CHECKOUT"))
            .orElse(rootProject.layout.projectDirectory.dir("../../nereusstream/kafka").asFile.absolutePath)
            .get(),
    )
    systemProperty(
        "nereus.kafka.process.evidence.dir",
        layout.buildDirectory.dir("f9-kafka-process-evidence").get().asFile.absolutePath,
    )
    maxParallelForks = 1
    useJUnitPlatform()
    filter {
        includeTestsMatching(
            "com.nereusstream.kafka.runtime.NereusKafkaNativeProcessIntegrationTest." +
                "productProcessRecoversUserGroupAndTransactionStateAcrossGracefulAndForcedRestarts",
        )
    }
}

tasks.register<Test>("f9MultiBrokerTakeoverProcessIntegrationTest") {
    group = "verification"
    description =
        "Run two release Kafka processes through a live RF1 shared-storage leader reassignment."
    dependsOn(rootProject.tasks.named("phase9M6KafkaProcessRuntime"))
    shouldRunAfter(tasks.named("f9M6KafkaProcessIntegrationTest"))
    testClassesDirs = f9ProviderIntegrationTest.output.classesDirs
    classpath = f9ProviderIntegrationTest.runtimeClasspath
    systemProperty(
        "nereus.kafka.fork.checkout",
        providers.gradleProperty("kafkaForkCheckout")
            .orElse(providers.environmentVariable("NEREUS_KAFKA_FORK_CHECKOUT"))
            .orElse(rootProject.layout.projectDirectory.dir("../../nereusstream/kafka").asFile.absolutePath)
            .get(),
    )
    systemProperty(
        "nereus.kafka.process.evidence.dir",
        layout.buildDirectory.dir("f9-kafka-multi-broker-process-evidence").get().asFile.absolutePath,
    )
    maxParallelForks = 1
    useJUnitPlatform()
    filter {
        includeTestsMatching(
            "com.nereusstream.kafka.runtime.NereusKafkaNativeProcessIntegrationTest." +
                "twoReleaseProcessesAtomicallyReassignLiveSharedStorageLeader",
        )
    }
}

tasks.register<Test>("f9BookKeeperWalOnlyProcessIntegrationTest") {
    group = "verification"
    description = "Run the native Kafka BookKeeper-WAL-only Produce/Fetch cold-restart process gate."
    jvmArgs("--add-opens=java.base/java.io=ALL-UNNAMED")
    dependsOn(rootProject.tasks.named("phase9M6KafkaProcessRuntime"))
    shouldRunAfter(tasks.named("f9M6KafkaProcessIntegrationTest"))
    testClassesDirs = f9ProviderIntegrationTest.output.classesDirs
    classpath = f9ProviderIntegrationTest.runtimeClasspath
    systemProperty(
        "nereus.kafka.fork.checkout",
        providers.gradleProperty("kafkaForkCheckout")
            .orElse(providers.environmentVariable("NEREUS_KAFKA_FORK_CHECKOUT"))
            .orElse(rootProject.layout.projectDirectory.dir("../../nereusstream/kafka").asFile.absolutePath)
            .get(),
    )
    systemProperty(
        "nereus.kafka.process.evidence.dir",
        layout.buildDirectory.dir("f9-kafka-bookkeeper-process-evidence").get().asFile.absolutePath,
    )
    maxParallelForks = 1
    useJUnitPlatform()
    filter {
        includeTestsMatching(
            "com.nereusstream.kafka.runtime.NereusKafkaNativeProcessIntegrationTest." +
                "bookKeeperWalOnlyProcessRecoversAcrossFreshJvmRestart",
        )
    }
}

tasks.register<Test>("f9BookKeeperWalAsyncObjectProcessIntegrationTest") {
    group = "verification"
    description =
        "Run the native Kafka BookKeeper async-object physical-deletion and fresh-JVM NCP2 recovery process gate."
    jvmArgs("--add-opens=java.base/java.io=ALL-UNNAMED")
    dependsOn(rootProject.tasks.named("phase9M6KafkaProcessRuntime"))
    mustRunAfter(tasks.named("f9BookKeeperWalOnlyProcessIntegrationTest"))
    testClassesDirs = f9ProviderIntegrationTest.output.classesDirs
    classpath = f9ProviderIntegrationTest.runtimeClasspath
    systemProperty(
        "nereus.kafka.fork.checkout",
        providers.gradleProperty("kafkaForkCheckout")
            .orElse(providers.environmentVariable("NEREUS_KAFKA_FORK_CHECKOUT"))
            .orElse(rootProject.layout.projectDirectory.dir("../../nereusstream/kafka").asFile.absolutePath)
            .get(),
    )
    systemProperty(
        "nereus.kafka.process.evidence.dir",
        layout.buildDirectory.dir("f9-kafka-bookkeeper-async-process-evidence").get().asFile.absolutePath,
    )
    maxParallelForks = 1
    useJUnitPlatform()
    filter {
        includeTestsMatching(
            "com.nereusstream.kafka.runtime.NereusKafkaNativeProcessIntegrationTest." +
                "bookKeeperWalAsyncObjectProcessMaterializesAndRecoversAcrossFreshJvmRestart",
        )
    }
}

tasks.register<Test>("f9BookKeeperWalSyncObjectProcessIntegrationTest") {
    group = "verification"
    description = "Run the native Kafka BookKeeper sync-object NCP2 cold-restart process gate."
    jvmArgs("--add-opens=java.base/java.io=ALL-UNNAMED")
    dependsOn(rootProject.tasks.named("phase9M6KafkaProcessRuntime"))
    mustRunAfter(tasks.named("f9BookKeeperWalAsyncObjectProcessIntegrationTest"))
    testClassesDirs = f9ProviderIntegrationTest.output.classesDirs
    classpath = f9ProviderIntegrationTest.runtimeClasspath
    systemProperty(
        "nereus.kafka.fork.checkout",
        providers.gradleProperty("kafkaForkCheckout")
            .orElse(providers.environmentVariable("NEREUS_KAFKA_FORK_CHECKOUT"))
            .orElse(rootProject.layout.projectDirectory.dir("../../nereusstream/kafka").asFile.absolutePath)
            .get(),
    )
    systemProperty(
        "nereus.kafka.process.evidence.dir",
        layout.buildDirectory.dir("f9-kafka-bookkeeper-sync-process-evidence").get().asFile.absolutePath,
    )
    maxParallelForks = 1
    useJUnitPlatform()
    filter {
        includeTestsMatching(
            "com.nereusstream.kafka.runtime.NereusKafkaNativeProcessIntegrationTest." +
                "bookKeeperWalSyncObjectProcessMaterializesBeforeAppendAndRecoversAcrossFreshJvmRestart",
        )
    }
}

tasks.register<Test>("f9ObjectWalAsyncObjectProcessIntegrationTest") {
    group = "verification"
    description = "Run the native Kafka async Object-WAL Produce/Fetch cold-restart process gate."
    dependsOn(rootProject.tasks.named("phase9M6KafkaProcessRuntime"))
    shouldRunAfter(tasks.named("f9M6KafkaProcessIntegrationTest"))
    testClassesDirs = f9ProviderIntegrationTest.output.classesDirs
    classpath = f9ProviderIntegrationTest.runtimeClasspath
    systemProperty(
        "nereus.kafka.fork.checkout",
        providers.gradleProperty("kafkaForkCheckout")
            .orElse(providers.environmentVariable("NEREUS_KAFKA_FORK_CHECKOUT"))
            .orElse(rootProject.layout.projectDirectory.dir("../../nereusstream/kafka").asFile.absolutePath)
            .get(),
    )
    systemProperty(
        "nereus.kafka.process.evidence.dir",
        layout.buildDirectory.dir("f9-kafka-object-async-process-evidence").get().asFile.absolutePath,
    )
    maxParallelForks = 1
    useJUnitPlatform()
    filter {
        includeTestsMatching(
            "com.nereusstream.kafka.runtime.NereusKafkaNativeProcessIntegrationTest." +
                "objectWalAsyncObjectProcessRecoversAcrossFreshJvmRestart",
        )
    }
}

tasks.register<Test>("f9ProducerStatePropertyTest") {
    group = "verification"
    description = "Run the partial F9-M4 complete seven-section canonical checkpoint contracts."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.kafka.checkpoint.KafkaProducerTransactionStateCodecV1Test")
        includeTestsMatching("com.nereusstream.kafka.checkpoint.KafkaProducerStatePropertyTest")
        includeTestsMatching("com.nereusstream.kafka.checkpoint.KafkaLeaderEpochStateCodecV1Test")
        includeTestsMatching("com.nereusstream.kafka.checkpoint.KafkaDerivedIndexStateCodecV1Test")
        includeTestsMatching("com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentStateCodecV1Test")
        includeTestsMatching("com.nereusstream.kafka.checkpoint.KafkaCanonicalCheckpointStateCodecV1Test")
        includeTestsMatching("com.nereusstream.kafka.checkpoint.KafkaCanonicalCheckpointPublicationFactoryTest")
    }
}

tasks.register<Test>("f9RetentionTest") {
    group = "verification"
    description = "Run the partial F9-M5 retention, DeleteRecords, and checkpoint-before-trim contracts."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.kafka.retention.KafkaRetentionPlannerTest")
        includeTestsMatching("com.nereusstream.kafka.retention.KafkaTrimBarrierTest")
        includeTestsMatching("com.nereusstream.kafka.retention.KafkaRetentionCheckpointGateTest")
        includeTestsMatching("com.nereusstream.kafka.retention.KafkaRetentionCoordinatorTest")
        includeTestsMatching("com.nereusstream.kafka.retention.KafkaRetentionDurableTrimListenerTest")
        includeTestsMatching("com.nereusstream.kafka.retention.KafkaDeleteRecordsCoordinatorTest")
        includeTestsMatching("com.nereusstream.kafka.retention.DefaultKafkaPartitionMaintenanceTest")
        includeTestsMatching("com.nereusstream.kafka.retention.KafkaPartitionMaintenanceRuntimeTest")
        includeTestsMatching("com.nereusstream.kafka.checkpoint.KafkaCheckpointPublicationRecoveryIntegrationTest")
        includeTestsMatching("com.nereusstream.kafka.checkpoint.DurableKafkaCheckpointFailureQuarantineTest")
        includeTestsMatching("com.nereusstream.kafka.runtime.NereusKafkaMaintenanceConfigurationTest")
    }
}

tasks.register<Test>("f9CheckpointQuarantineTest") {
    group = "verification"
    description = "Run durable F9 NKC1 quarantine, audit, fallback-order, and restart-skip contracts."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.kafka.checkpoint.DurableKafkaCheckpointFailureQuarantineTest")
        includeTestsMatching("com.nereusstream.kafka.checkpoint.KafkaCheckpointPublicationRecoveryIntegrationTest")
        includeTestsMatching("com.nereusstream.kafka.retention.KafkaRetentionCheckpointGateTest")
        includeTestsMatching("com.nereusstream.kafka.runtime.NereusKafkaObjectWalRuntimeFactoryTest")
    }
}

tasks.register<Test>("f9CompactionPropertyTest") {
    group = "verification"
    description = "Run the partial F9-M5 Kafka compaction planner, strategy, and codec contracts."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.kafka.compaction.KafkaCompactionBatchSourceTest")
        includeTestsMatching("com.nereusstream.kafka.compaction.KafkaCompactionGenerationSetTest")
        includeTestsMatching("com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollectorTest")
        includeTestsMatching("com.nereusstream.kafka.compaction.KafkaCompactionPartitionPassTest")
        includeTestsMatching("com.nereusstream.kafka.compaction.KafkaCompactionPlanCodecV1Test")
        includeTestsMatching("com.nereusstream.kafka.compaction.KafkaCompactionPlanCoordinatorTest")
        includeTestsMatching("com.nereusstream.kafka.compaction.KafkaCompactionPlanOrphanScannerTest")
        includeTestsMatching("com.nereusstream.kafka.compaction.KafkaCompactionPlannerTest")
        includeTestsMatching("com.nereusstream.kafka.compaction.KafkaCompactionProductionRuntimeFactoryTest")
        includeTestsMatching("com.nereusstream.kafka.compaction.KafkaCompactionPublicationCoordinatorTest")
        includeTestsMatching("com.nereusstream.kafka.compaction.KafkaCompactionRowMapperTest")
        includeTestsMatching("com.nereusstream.kafka.compaction.KafkaCompactionRowSpoolTest")
        includeTestsMatching("com.nereusstream.kafka.compaction.KafkaCompactionRuntimeTest")
        includeTestsMatching("com.nereusstream.kafka.compaction.KafkaCompactionSchedulerTest")
        includeTestsMatching("com.nereusstream.kafka.compaction.KafkaCompactionSourceResolverTest")
        includeTestsMatching("com.nereusstream.kafka.compaction.KafkaCompactionStreamingExecutorTest")
        includeTestsMatching("com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1Test")
        includeTestsMatching("com.nereusstream.kafka.compaction.KafkaCompactionTerminalRetirerTest")
        includeTestsMatching("com.nereusstream.kafka.compaction.KafkaTopicCompactionCodecV1Test")
        includeTestsMatching("com.nereusstream.kafka.compaction.KafkaCompactionTwoPassExecutorTest")
        includeTestsMatching("com.nereusstream.kafka.compaction.KafkaCompactionWinnerIndexTest")
        includeTestsMatching("com.nereusstream.kafka.compaction.KafkaActivatedGenerationSetResolverTest")
        includeTestsMatching("com.nereusstream.kafka.compaction.KafkaCompactedFetchPlannerTest")
        includeTestsMatching("com.nereusstream.kafka.compaction.KafkaCompactedFetchIntegrationTest")
        includeTestsMatching("com.nereusstream.kafka.compaction.KafkaCompactedNoResurrectionIntegrationTest")
        includeTestsMatching("com.nereusstream.kafka.runtime.NereusKafkaCompactionRuntimeConfigurationTest")
    }
}

tasks.register<Test>("f9ActivationTest") {
    group = "verification"
    description = "Run F9 broker capability publication and ACTIVE/readiness admission contracts."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.kafka.activation.*")
    }
}
