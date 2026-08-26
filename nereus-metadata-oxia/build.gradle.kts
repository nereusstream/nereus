import org.gradle.api.tasks.JavaExec
import org.gradle.jvm.tasks.Jar

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

sourceSets {
    main {
        java.setSrcDirs(listOf("src/main/java"))
        java.include("com/nereusstream/metadata/oxia/v2/**")
    }
    test {
        java.setSrcDirs(listOf("src/test/java"))
        java.include("com/nereusstream/metadata/oxia/v2/**")
    }
}

val oxiaIntegrationTest by sourceSets.creating {
    java.setSrcDirs(listOf("src/oxiaIntegrationTest/java"))
    java.include("com/nereusstream/metadata/oxia/v2/**")
    compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
    runtimeClasspath += output + compileClasspath
}

val realAllocatorTest by sourceSets.creating {
    java.setSrcDirs(listOf("src/realAllocatorTest/java"))
    java.include("com/nereusstream/metadata/oxia/v2/allocator/evidence/**")
    java.include("org/apache/bookkeeper/client/M3PayloadReleasingPulsarMockBookKeeper.java")
    compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
    runtimeClasspath += output + compileClasspath
}

configurations[oxiaIntegrationTest.implementationConfigurationName].extendsFrom(
    configurations.testImplementation.get(),
)
configurations[oxiaIntegrationTest.runtimeOnlyConfigurationName].extendsFrom(
    configurations.testRuntimeOnly.get(),
)
configurations[realAllocatorTest.implementationConfigurationName].extendsFrom(
    configurations.testImplementation.get(),
)
configurations[realAllocatorTest.runtimeOnlyConfigurationName].extendsFrom(
    configurations.testRuntimeOnly.get(),
)

dependencies {
    api(project(":nereus-metadata-spi"))
    api(project(":nereus-storage-object"))
    implementation(platform(libs.grpc.bom))
    // Oxia's public client API exposes OpenTelemetry API types. Publish its BOM on the API edge so
    // an external M3 consumer never resolves the source-locked oxia-client-api with an empty version.
    api(platform(libs.opentelemetry.bom))
    implementation(platform(libs.opentelemetry.bom.alpha))
    api(libs.oxia.client)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.platform.launcher)

    add(oxiaIntegrationTest.implementationConfigurationName, project())
    add(oxiaIntegrationTest.implementationConfigurationName, libs.oxia.testcontainers)
    add(oxiaIntegrationTest.implementationConfigurationName, libs.testcontainers.junit.jupiter)
    add(oxiaIntegrationTest.implementationConfigurationName, libs.junit.jupiter)
    add(oxiaIntegrationTest.implementationConfigurationName, libs.assertj)
    add(oxiaIntegrationTest.runtimeOnlyConfigurationName, libs.junit.platform.launcher)

    add(realAllocatorTest.implementationConfigurationName, project())
    add(realAllocatorTest.implementationConfigurationName, libs.pulsar.managed.ledger)
    add(realAllocatorTest.implementationConfigurationName, "org.apache.pulsar:testmocks:${libs.versions.pulsar.get()}")
    add(realAllocatorTest.implementationConfigurationName, libs.junit.jupiter)
    add(realAllocatorTest.implementationConfigurationName, libs.assertj)
    add(realAllocatorTest.runtimeOnlyConfigurationName, libs.junit.platform.launcher)
}

val p1ArtifactJar by tasks.registering(Jar::class) {
    group = "build"
    description = "Build the source-qualified P1-only metadata capability artifact."
    archiveBaseName.set("nereus-metadata-oxia-p1")
    from(sourceSets.main.get().output)
    include("com/nereusstream/metadata/oxia/v2/**")
    // P1 is an immutable historical capability slice. Later M3 production packages live in the
    // same source set but must never leak into a rebuilt P1-only artifact.
    exclude("com/nereusstream/metadata/oxia/v2/allocator/**")
    exclude("com/nereusstream/metadata/oxia/v2/objectwal/**")
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
    dependsOn(tasks.named("classes"))
}

val p1ArtifactSourcesJar by tasks.registering(Jar::class) {
    group = "build"
    description = "Build sources for the source-qualified P1-only metadata capability artifact."
    archiveBaseName.set("nereus-metadata-oxia-p1")
    archiveClassifier.set("sources")
    from(sourceSets.main.get().allSource)
    include("com/nereusstream/metadata/oxia/v2/**")
    exclude("com/nereusstream/metadata/oxia/v2/allocator/**")
    exclude("com/nereusstream/metadata/oxia/v2/objectwal/**")
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
}

val realAllocatorEvidenceArtifactJar by tasks.registering(Jar::class) {
    group = "build"
    description = "Build the reproducible thin M3 real allocator evidence runner artifact."
    archiveBaseName.set("nereus-v2-m3-real-allocator-evidence")
    from(realAllocatorTest.output)
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
    dependsOn(tasks.named(realAllocatorTest.classesTaskName))
}

val realAllocatorEvidenceRuntimeClasspath = files(realAllocatorEvidenceArtifactJar.flatMap { it.archiveFile }) +
    realAllocatorTest.runtimeClasspath.filter { it.isFile } +
    files(tasks.named("jar"))

val realAllocatorEvidenceClasspathFile =
    layout.buildDirectory.file("m3-allocator-evidence/runtime-classpath.txt")

tasks.register("writeRealAllocatorEvidenceRuntimeClasspath") {
    group = "build"
    description = "Write the exact ordered artifact-only classpath used by the formal allocator evidence JVM."
    dependsOn(realAllocatorEvidenceArtifactJar, tasks.named("jar"))
    inputs.files(realAllocatorEvidenceRuntimeClasspath)
    outputs.file(realAllocatorEvidenceClasspathFile)
    doLast {
        val entries = realAllocatorEvidenceRuntimeClasspath.files.toList()
        check(entries.isNotEmpty()) { "allocator formal runtime classpath is empty" }
        realAllocatorEvidenceClasspathFile.get().asFile.apply {
            parentFile.mkdirs()
            writeText(entries.joinToString(separator = "\n", postfix = "\n") { it.absolutePath })
        }
        entries.forEach { entry ->
            check(entry.isFile) {
                "allocator formal runtime classpath contains a directory or missing artifact: ${entry.absolutePath}"
            }
        }
    }
}

tasks.register<Test>("p1MetadataTest") {
    group = "verification"
    description = "Run the deterministic P1 V2 metadata capability unit gate."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.*")
        // R1 shares the V2 package but owns a separate receipt and exact-source gate.
        excludeTestsMatching("com.nereusstream.metadata.oxia.v2.codec.Nvr1RegistryAuthorityCodecTest")
        excludeTestsMatching("com.nereusstream.metadata.oxia.v2.capability.R1RegistryAuthorityTest")
    }
}

tasks.register<Test>("p1OxiaIntegrationTest") {
    group = "verification"
    description = "Run the P1 selector, aggregate, restart, and notification gate against source-locked real Oxia."
    testClassesDirs = oxiaIntegrationTest.output.classesDirs
    classpath = oxiaIntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.test)
    maxParallelForks = 1
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.PulsarP1OxiaIntegrationTest")
    }
}

tasks.register<Test>("r1MetadataTest") {
    group = "verification"
    description = "Run the deterministic R1 Registry domain bridge, interlock, evidence, and mutation gate."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.codec.Nvr1RegistryAuthorityCodecTest")
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.capability.R1RegistryAuthorityTest")
    }
}

tasks.register<Test>("r1OxiaIntegrationTest") {
    group = "verification"
    description = "Run R1 Registry create/CAS/restart conformance against source-locked real Oxia."
    testClassesDirs = oxiaIntegrationTest.output.classesDirs
    classpath = oxiaIntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.test)
    maxParallelForks = 1
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.R1RegistryOxiaIntegrationTest")
    }
}

val realAllocatorEvidenceTest = tasks.register<Test>("realAllocatorEvidenceTest") {
    group = "verification"
    description = "Run fail-closed M3 allocator evidence against real Oxia and exact-source native Pulsar code."
    dependsOn(realAllocatorEvidenceArtifactJar)
    // The formal runner is discovered and loaded from the exact thin JAR whose digest enters
    // NAEA1. Production domain/SPI/Oxia classes remain separate runtime JARs.
    // Gradle scans the compiled directory only to discover test class names; it is deliberately
    // absent from the worker classpath, so the JVM loads those classes from the exact thin JAR.
    testClassesDirs = realAllocatorTest.output.classesDirs
    classpath = realAllocatorEvidenceRuntimeClasspath
    shouldRunAfter(tasks.test)
    maxParallelForks = 1
    maxHeapSize = "6144m"
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3RealAllocatorEvidenceTest")
    }
    outputs.upToDateWhen { false }
    systemProperty(
        "nereus.m3.allocator.outputDirectory",
        providers.gradleProperty("v2M3AllocatorOutputDirectory")
            .getOrElse(layout.buildDirectory.dir("m3-allocator-evidence/formal").get().asFile.absolutePath),
    )
    listOf(
        "nereusSourceCommit",
        "pulsarSourceCommit",
        "oxiaClientSourceCommit",
        "oxiaClientJarSha256",
        "oxiaServerSourceCommit",
        "testedEvidenceArtifactSha256",
        "runtimeDomainArtifactSha256",
        "runtimeMetadataSpiArtifactSha256",
        "runtimeMetadataOxiaArtifactSha256",
        "oxiaServiceAddress",
        "sourceLocksSha256",
        "executorManifestSha256",
    ).forEach { key ->
        systemProperty(
            "nereus.m3.allocator.$key",
            providers.gradleProperty("v2M3Allocator${key.replaceFirstChar(Char::uppercaseChar)}").getOrElse("UNSET"),
        )
    }
}

val realAllocatorDomainJar = project(":nereus-domain").tasks.named<Jar>("jar")
val realAllocatorMetadataSpiJar = project(":nereus-metadata-spi").tasks.named<Jar>("jar")
val realAllocatorMetadataOxiaJar = tasks.named<Jar>("jar")
val realAllocatorEvidenceJUnitXml = layout.buildDirectory.file(
    "test-results/realAllocatorEvidenceTest/" +
        "TEST-com.nereusstream.metadata.oxia.v2.allocator.evidence.M3RealAllocatorEvidenceTest.xml",
)

val sealRealAllocatorEvidence = tasks.register<JavaExec>("sealRealAllocatorEvidence") {
    group = "verification"
    description = "Seal the exact formal JUnit XML as test.naea and derive NARS1 only through the production parser."
    dependsOn(
        realAllocatorEvidenceTest,
        realAllocatorEvidenceArtifactJar,
        realAllocatorDomainJar,
        realAllocatorMetadataSpiJar,
        realAllocatorMetadataOxiaJar,
    )
    classpath = realAllocatorEvidenceRuntimeClasspath
    mainClass.set("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3AllocatorEvidenceSealMain")
    outputs.upToDateWhen { false }
    doFirst {
        val outputDirectory = providers.gradleProperty("v2M3AllocatorOutputDirectory")
            .orNull
            ?.let { file(it).absolutePath }
            ?: error("v2M3AllocatorOutputDirectory is required for formal allocator evidence sealing")
        val oxiaClientJar = providers.gradleProperty("v2M3AllocatorOxiaClientJarPath")
            .orNull
            ?.let { file(it).absolutePath }
            ?: error("v2M3AllocatorOxiaClientJarPath is required for formal allocator evidence sealing")
        val executorManifest = providers.gradleProperty("v2M3AllocatorExecutorManifestPath")
            .orNull
            ?.let { file(it).absolutePath }
            ?: error("v2M3AllocatorExecutorManifestPath is required for formal allocator evidence sealing")
        setArgs(
            listOf(
                outputDirectory,
                realAllocatorEvidenceJUnitXml.get().asFile.absolutePath,
                oxiaClientJar,
                realAllocatorEvidenceArtifactJar.get().archiveFile.get().asFile.absolutePath,
                realAllocatorDomainJar.get().archiveFile.get().asFile.absolutePath,
                realAllocatorMetadataSpiJar.get().archiveFile.get().asFile.absolutePath,
                realAllocatorMetadataOxiaJar.get().archiveFile.get().asFile.absolutePath,
                rootProject.file("docs/v2/source-locks.json").absolutePath,
                executorManifest,
            ),
        )
    }
}

val realAllocatorEvidenceFormalCheck = tasks.register("realAllocatorEvidenceFormalCheck") {
    group = "verification"
    description = "Execute, seal, recompute, and require one eligible exact-source ADR-0094 allocator selection."
}

tasks.register<JavaExec>("verifyRealAllocatorEvidence") {
    group = "verification"
    description = "Read-only reparse NARS1, five exact NAEA1 files, JUnit XML, and all source artifacts."
    dependsOn(
        realAllocatorEvidenceArtifactJar,
        realAllocatorDomainJar,
        realAllocatorMetadataSpiJar,
        realAllocatorMetadataOxiaJar,
    )
    classpath = realAllocatorEvidenceRuntimeClasspath
    mainClass.set("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3AllocatorEvidenceVerifyMain")
    outputs.upToDateWhen { false }
    doFirst {
        fun requiredPath(property: String): String = providers.gradleProperty(property)
            .orNull
            ?.let { file(it).absolutePath }
            ?: error("$property is required for raw allocator evidence verification")
        setArgs(
            listOf(
                requiredPath("v2M3AllocatorOutputDirectory"),
                requiredPath("v2M3AllocatorOxiaClientJarPath"),
                realAllocatorEvidenceArtifactJar.get().archiveFile.get().asFile.absolutePath,
                realAllocatorDomainJar.get().archiveFile.get().asFile.absolutePath,
                realAllocatorMetadataSpiJar.get().archiveFile.get().asFile.absolutePath,
                realAllocatorMetadataOxiaJar.get().archiveFile.get().asFile.absolutePath,
                rootProject.file("docs/v2/source-locks.json").absolutePath,
                requiredPath("v2M3AllocatorExecutorManifestPath"),
                requiredPath("v2M3AllocatorVerificationOutput"),
            ),
        )
    }
}

val realAllocatorRawVerificationTest = tasks.register<Test>("realAllocatorRawVerificationTest") {
    group = "verification"
    description = "Recompute sealed NARS1/NAEA1/JUnit/source artifacts through the production parser."
    dependsOn(sealRealAllocatorEvidence)
    testClassesDirs = realAllocatorTest.output.classesDirs
    classpath = realAllocatorEvidenceRuntimeClasspath
    maxParallelForks = 1
    useJUnitPlatform()
    filter {
        includeTestsMatching(
            "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3AllocatorRawEvidenceVerificationTest",
        )
    }
    outputs.upToDateWhen { false }
    doFirst {
        fun requiredPath(property: String): String = providers.gradleProperty(property)
            .orNull
            ?.let { file(it).absolutePath }
            ?: error("$property is required for raw allocator evidence verification")
        systemProperty("nereus.m3.allocator.outputDirectory", requiredPath("v2M3AllocatorOutputDirectory"))
        systemProperty("nereus.m3.allocator.oxiaClientJarPath", requiredPath("v2M3AllocatorOxiaClientJarPath"))
        systemProperty(
            "nereus.m3.allocator.testedEvidenceArtifactPath",
            realAllocatorEvidenceArtifactJar.get().archiveFile.get().asFile.absolutePath,
        )
        systemProperty(
            "nereus.m3.allocator.runtimeDomainArtifactPath",
            realAllocatorDomainJar.get().archiveFile.get().asFile.absolutePath,
        )
        systemProperty(
            "nereus.m3.allocator.runtimeMetadataSpiArtifactPath",
            realAllocatorMetadataSpiJar.get().archiveFile.get().asFile.absolutePath,
        )
        systemProperty(
            "nereus.m3.allocator.runtimeMetadataOxiaArtifactPath",
            realAllocatorMetadataOxiaJar.get().archiveFile.get().asFile.absolutePath,
        )
        systemProperty("nereus.m3.allocator.sourceLocksPath", rootProject.file("docs/v2/source-locks.json").absolutePath)
        systemProperty(
            "nereus.m3.allocator.executorManifestPath",
            requiredPath("v2M3AllocatorExecutorManifestPath"),
        )
        systemProperty(
            "nereus.m3.allocator.verificationPayload",
            file(requiredPath("v2M3AllocatorOutputDirectory"))
                .resolve("raw-verification-payload.json")
                .absolutePath,
        )
    }
}

val realAllocatorRawVerificationJUnitXml = layout.buildDirectory.file(
    "test-results/realAllocatorRawVerificationTest/" +
        "TEST-com.nereusstream.metadata.oxia.v2.allocator.evidence.M3AllocatorRawEvidenceVerificationTest.xml",
)

val sealRealAllocatorRawVerification = tasks.register<JavaExec>("sealRealAllocatorRawVerification") {
    group = "verification"
    description = "Seal the raw allocator recomputation with exact verifier JUnit bytes, SHA, and testcase."
    dependsOn(realAllocatorRawVerificationTest)
    classpath = realAllocatorEvidenceRuntimeClasspath
    mainClass.set("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3AllocatorVerificationSealMain")
    outputs.upToDateWhen { false }
    doFirst {
        val outputDirectory = providers.gradleProperty("v2M3AllocatorOutputDirectory")
            .orNull
            ?.let { file(it) }
            ?: error("v2M3AllocatorOutputDirectory is required for sealed allocator verification")
        setArgs(
            listOf(
                outputDirectory.resolve("raw-verification-payload.json").absolutePath,
                realAllocatorRawVerificationJUnitXml.get().asFile.absolutePath,
                outputDirectory.resolve("raw-verification.json").absolutePath,
            ),
        )
    }
}

realAllocatorEvidenceFormalCheck.configure {
    dependsOn(sealRealAllocatorRawVerification)
}

tasks.register<Test>("realAllocatorContractTest") {
    group = "verification"
    description = "Validate the frozen ADR-0094 allocator workload before any real-service execution."
    dependsOn(realAllocatorEvidenceArtifactJar)
    testClassesDirs = realAllocatorTest.output.classesDirs
    classpath = realAllocatorEvidenceRuntimeClasspath
    maxParallelForks = 1
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3AllocatorWorkloadPlanTest")
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3AllocatorEvidenceWiringTest")
    }
}

tasks.register<Test>("realAllocatorNativePathTest") {
    group = "verification"
    description = "Exercise exact pinned Pulsar ENTRY/BYTE/AGE production rollover decisions without formal scale."
    dependsOn(realAllocatorEvidenceArtifactJar)
    testClassesDirs = realAllocatorTest.output.classesDirs
    classpath = realAllocatorEvidenceRuntimeClasspath
    maxParallelForks = 1
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3NativePulsarPathTest")
    }
}

tasks.register<Test>("realAllocatorFaultBatchDiagnosticTest") {
    group = "verification"
    description = "Run the diagnostic-only 10k STRICT fault batch against real Oxia without emitting evidence."
    dependsOn(realAllocatorEvidenceArtifactJar)
    testClassesDirs = realAllocatorTest.output.classesDirs
    classpath = realAllocatorEvidenceRuntimeClasspath
    maxParallelForks = 1
    useJUnitPlatform()
    filter {
        includeTestsMatching(
            "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3RealAllocatorFaultBatchDiagnosticTest",
        )
    }
    outputs.upToDateWhen { false }
    systemProperty(
        "nereus.m3.allocator.oxiaServiceAddress",
        providers.gradleProperty("v2M3AllocatorOxiaServiceAddress").getOrElse("UNSET"),
    )
    systemProperty(
        "nereus.m3.allocator.nereusSourceCommit",
        providers.gradleProperty("v2M3AllocatorNereusSourceCommit").getOrElse("UNSET"),
    )
}

tasks.register<Test>("realAllocatorRangeCellProofDiagnosticTest") {
    group = "verification"
    description =
        "Run diagnostic-only 10k RANGE construction and exact-Cell renewal overlap against real Oxia."
    dependsOn(realAllocatorEvidenceArtifactJar)
    testClassesDirs = realAllocatorTest.output.classesDirs
    classpath = realAllocatorEvidenceRuntimeClasspath
    maxParallelForks = 1
    useJUnitPlatform()
    filter {
        includeTestsMatching(
            "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3RealAllocatorRangeCellProofDiagnosticTest",
        )
    }
    outputs.upToDateWhen { false }
    systemProperty(
        "nereus.m3.allocator.oxiaServiceAddress",
        providers.gradleProperty("v2M3AllocatorOxiaServiceAddress").getOrElse("UNSET"),
    )
    systemProperty(
        "nereus.m3.allocator.nereusSourceCommit",
        providers.gradleProperty("v2M3AllocatorNereusSourceCommit").getOrElse("UNSET"),
    )
}

tasks.register<Test>("realAllocatorRange100kConstructionDiagnosticTest") {
    group = "verification"
    description = "Run diagnostic-only RANGE-1024 construction through 100k Heads against real Oxia."
    dependsOn(realAllocatorEvidenceArtifactJar)
    testClassesDirs = realAllocatorTest.output.classesDirs
    classpath = realAllocatorEvidenceRuntimeClasspath
    maxParallelForks = 1
    useJUnitPlatform()
    filter {
        includeTestsMatching(
            "com.nereusstream.metadata.oxia.v2.allocator.evidence." +
                "M3RealAllocatorRange100kConstructionDiagnosticTest",
        )
    }
    outputs.upToDateWhen { false }
    systemProperty(
        "nereus.m3.allocator.oxiaServiceAddress",
        providers.gradleProperty("v2M3AllocatorOxiaServiceAddress").getOrElse("UNSET"),
    )
    systemProperty(
        "nereus.m3.allocator.nereusSourceCommit",
        providers.gradleProperty("v2M3AllocatorNereusSourceCommit").getOrElse("UNSET"),
    )
}

tasks.register<Test>("realAllocatorRangeFaultBatchDiagnosticTest") {
    group = "verification"
    description = "Run the diagnostic-only 10k RANGE-16 nine-cut batch against real Oxia without emitting evidence."
    dependsOn(realAllocatorEvidenceArtifactJar)
    testClassesDirs = realAllocatorTest.output.classesDirs
    classpath = realAllocatorEvidenceRuntimeClasspath
    maxParallelForks = 1
    useJUnitPlatform()
    filter {
        includeTestsMatching(
            "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3RealAllocatorRangeFaultBatchDiagnosticTest",
        )
    }
    outputs.upToDateWhen { false }
    systemProperty(
        "nereus.m3.allocator.oxiaServiceAddress",
        providers.gradleProperty("v2M3AllocatorOxiaServiceAddress").getOrElse("UNSET"),
    )
    systemProperty(
        "nereus.m3.allocator.nereusSourceCommit",
        providers.gradleProperty("v2M3AllocatorNereusSourceCommit").getOrElse("UNSET"),
    )
}

tasks.register<Test>("m3ObjectWalMetadataTest") {
    group = "verification"
    description = "Run the deterministic M3 Object-WAL Oxia control-metadata adapter gate."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.objectwal.*")
    }
}

tasks.register<Test>("m3ObjectWalOxiaIntegrationTest") {
    group = "verification"
    description = "Run the M3 Object-WAL exact-byte control-metadata gate against source-locked real Oxia."
    testClassesDirs = oxiaIntegrationTest.output.classesDirs
    classpath = oxiaIntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.test)
    maxParallelForks = 1
    useJUnitPlatform()
    systemProperty(
        "nereus.m3.objectwal.oxia.serviceAddress",
        providers.gradleProperty("v2M3ObjectWalOxiaServiceAddress").getOrElse("UNSET"),
    )
    filter {
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.objectwal.*")
    }
}
