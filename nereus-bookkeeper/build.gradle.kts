plugins {
    `java-library`
}

dependencies {
    api(project(":nereus-api"))
    api(project(":nereus-core"))
    implementation(project(":nereus-materialization"))
    implementation(project(":nereus-metadata-oxia"))
    implementation(libs.bookkeeper.server)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(testFixtures(project(":nereus-metadata-oxia")))
    testRuntimeOnly(libs.junit.platform.launcher)
}

fun registerFocusedTest(name: String, descriptionText: String, vararg classes: String) {
    tasks.register<Test>(name) {
        group = "verification"
        description = descriptionText
        testClassesDirs = sourceSets.test.get().output.classesDirs
        classpath = sourceSets.test.get().runtimeClasspath
        useJUnitPlatform()
        filter {
            classes.forEach(::includeTestsMatching)
        }
    }
}

registerFocusedTest(
    "bkM2AllocatorTest",
    "Run BK-M2 reserved-id allocation, provider identity, and writer monotonicity tests.",
    "com.nereusstream.bookkeeper.BookKeeperLedgerAllocatorTest",
    "com.nereusstream.bookkeeper.BookKeeperWriterStatePropertyTest",
)

registerFocusedTest(
    "bkM2AppendReadTest",
    "Run BK-M2 append, exact read, buffer ownership, and L0 composition tests.",
    "com.nereusstream.bookkeeper.BookKeeperClientApiContractTest",
    "com.nereusstream.bookkeeper.BookKeeperAppenderDeadlineTest",
    "com.nereusstream.bookkeeper.BookKeeperAppenderResourceTest",
    "com.nereusstream.bookkeeper.BookKeeperPrimaryWalAppenderTest",
    "com.nereusstream.bookkeeper.BookKeeperAppendRecoveryCoordinatorTest",
    "com.nereusstream.bookkeeper.BookKeeperPrimaryWalReaderTest",
    "com.nereusstream.bookkeeper.BookKeeperStreamStorageIntegrationTest",
)

registerFocusedTest(
    "bkM2RecoveryFencingTest",
    "Run BK-M2 uncertain-create, stale-session, ownership-transfer, and recovery-inventory tests.",
    "com.nereusstream.bookkeeper.BookKeeperLedgerAllocatorTest",
    "com.nereusstream.bookkeeper.BookKeeperLedgerRecoveryTest",
    "com.nereusstream.bookkeeper.BookKeeperAppenderDeadlineTest",
    "com.nereusstream.bookkeeper.BookKeeperAppenderResourceTest",
    "com.nereusstream.bookkeeper.BookKeeperPrimaryWalAppenderTest",
    "com.nereusstream.bookkeeper.BookKeeperAppendRecoveryCoordinatorTest",
    "com.nereusstream.bookkeeper.BookKeeperWalRetentionGateTest",
)

registerFocusedTest(
    "bkM6ScaleTest",
    "Run BK-M6 root, protection, reader, hazard-slot, and stack-bounded scale boundaries.",
    "com.nereusstream.bookkeeper.BookKeeperPrimaryWalScaleTest",
    "com.nereusstream.bookkeeper.BookKeeperLedgerAllocatorTest",
)

registerFocusedTest(
    "bkM6ChaosTest",
    "Run BK-M6 allocation, seal, append/head, and deletion response-loss recovery matrix.",
    "com.nereusstream.bookkeeper.BookKeeperLedgerAllocatorTest",
    "com.nereusstream.bookkeeper.BookKeeperLedgerRecoveryTest",
    "com.nereusstream.bookkeeper.BookKeeperAppendRecoveryCoordinatorTest",
    "com.nereusstream.bookkeeper.BookKeeperWalRetentionGateTest",
)

registerFocusedTest(
    "rangedBookKeeperIntegrationTest",
    "Run the F9-M1 BookKeeper ranged codec, boundary, overflow, and L0 composition contracts.",
    "com.nereusstream.bookkeeper.BookKeeperRangedEntryCodecV1Test",
    "com.nereusstream.bookkeeper.BookKeeperPrimaryWalReaderTest",
    "com.nereusstream.bookkeeper.BookKeeperStreamStorageIntegrationTest",
)
