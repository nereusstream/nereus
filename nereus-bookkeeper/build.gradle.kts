plugins {
    `java-library`
}

dependencies {
    api(project(":nereus-api"))
    api(project(":nereus-core"))
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
    "com.nereusstream.bookkeeper.BookKeeperPrimaryWalAppenderTest",
    "com.nereusstream.bookkeeper.BookKeeperPrimaryWalReaderTest",
    "com.nereusstream.bookkeeper.BookKeeperStreamStorageIntegrationTest",
)

registerFocusedTest(
    "bkM2RecoveryFencingTest",
    "Run BK-M2 uncertain-create, stale-session, ownership-transfer, and recovery-inventory tests.",
    "com.nereusstream.bookkeeper.BookKeeperLedgerAllocatorTest",
    "com.nereusstream.bookkeeper.BookKeeperPrimaryWalAppenderTest",
    "com.nereusstream.bookkeeper.BookKeeperWalRetentionGateTest",
)
