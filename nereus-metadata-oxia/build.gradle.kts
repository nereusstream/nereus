import org.gradle.api.tasks.JavaExec
import org.gradle.jvm.tasks.Jar
import java.nio.file.Files
import java.nio.file.LinkOption
import java.security.MessageDigest
import java.time.Duration

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
    doFirst {
        error(
            "legacy full allocator execution is disabled; use only the separately authorized " +
                "realAllocatorV2BoundedAdaptiveFormalCampaign entry",
        )
    }
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
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V2BoundedActorLaneRunnerTest")
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AsyncActorLaneRunnerTest")
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V4AsyncActorLaneRunnerTest")
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3FormalCampaignPlanTest")
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AllocatorProtocolMainTest")
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V4AllocatorProtocolMainTest")
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AdaptiveCampaignExecutorTest")
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V4AdaptiveCampaignExecutorTest")
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V2AllocatorProtocolMainTest")
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V2AdaptiveCampaignExecutorTest")
    }
}

val realAllocatorV2BoundedAdaptiveFormalCampaign = tasks.register<Test>(
    "realAllocatorV2BoundedAdaptiveFormalCampaign",
) {
    group = "verification"
    description =
        "Explicitly authorized ADR-0104 bounded-adaptive campaign; never runs from build, check, or v2M3Check."
    notCompatibleWithConfigurationCache("formal preflight inspects live Git and exact external worktrees")
    dependsOn(realAllocatorEvidenceArtifactJar)
    testClassesDirs = realAllocatorTest.output.classesDirs
    classpath = realAllocatorEvidenceRuntimeClasspath
    maxParallelForks = 1
    maxHeapSize = "6144m"
    timeout.set(Duration.ofSeconds(48_000))
    useJUnitPlatform()
    filter {
        includeTestsMatching(
            "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V2BoundedAdaptiveFormalCampaignTest",
        )
    }
    outputs.upToDateWhen { false }
    doFirst {
        fun required(property: String): String = providers.gradleProperty(property)
            .orNull
            ?.takeIf { it.isNotBlank() }
            ?: error("$property is required for the bounded-adaptive formal campaign")
        fun command(vararg command: String, directory: File = rootProject.projectDir): String {
            val process = ProcessBuilder(*command)
                .directory(directory)
                .redirectErrorStream(true)
                .start()
            val output = process.inputStream.bufferedReader().use { it.readText() }.trim()
            check(process.waitFor() == 0) {
                "allocator formal preflight command failed: ${command.joinToString(" ")}\n$output"
            }
            return output
        }
        fun git(directory: File, vararg arguments: String): String =
            command("git", "-C", directory.absolutePath, *arguments)
        fun sha256(path: File): String {
            check(path.isFile) { "allocator formal hash input is absent: ${path.absolutePath}" }
            val digest = MessageDigest.getInstance("SHA-256")
            path.inputStream().use { input ->
                val buffer = ByteArray(64 * 1024)
                while (true) {
                    val read = input.read(buffer)
                    if (read < 0) break
                    digest.update(buffer, 0, read)
                }
            }
            return digest.digest().joinToString("") { "%02x".format(it) }
        }
        fun requireCheckout(pathProperty: String, commitProperty: String): File {
            val checkout = file(required(pathProperty)).canonicalFile
            val expectedCommit = required(commitProperty)
            check(git(checkout, "rev-parse", "HEAD") == expectedCommit) {
                "$pathProperty HEAD differs from $commitProperty"
            }
            check(git(checkout, "status", "--porcelain", "--untracked-files=all").isEmpty()) {
                "$pathProperty worktree is not clean"
            }
            return checkout
        }

        val authorization = required("v2M3AllocatorV2FormalAuthorizationSha")
        val head = git(rootProject.projectDir, "rev-parse", "HEAD")
        check(head == authorization) { "allocator formal HEAD differs from the explicit authorization SHA" }
        check(git(rootProject.projectDir, "branch", "--show-current") == "main") {
            "allocator formal source is not on main"
        }
        check(git(rootProject.projectDir, "rev-parse", "refs/remotes/origin/main") == authorization) {
            "allocator formal origin/main differs from the explicit authorization SHA"
        }
        check(git(rootProject.projectDir, "status", "--porcelain", "--untracked-files=all").isEmpty()) {
            "allocator formal Nereus worktree is not clean"
        }

        val sourceLocks = rootProject.file("docs/v2/source-locks.json")
        val expectedSourceLocks = required("v2M3AllocatorV2SourceLocksSha256")
        check(sha256(sourceLocks) == expectedSourceLocks) {
            "allocator formal source-lock SHA differs from the explicit authorization tuple"
        }
        val expectedDependencyLock = required("v2M3AllocatorV2DependencyLockSha256")
        val planOutput = command("python3", rootProject.file("scripts/v2-m3-allocator-plan.py").absolutePath)
        check(planOutput.contains("\"dependencyLockSha256\": \"$expectedDependencyLock\"")) {
            "allocator formal dependency-lock SHA differs from the pure plan projection"
        }
        val expectedPlan = required("v2M3AllocatorV2ZeroDecisionPlanSha256")
        check(planOutput.contains("\"zeroDecisionPlanSha256\": \"$expectedPlan\"")) {
            "allocator formal frozen plan SHA differs from plan-only"
        }
        check(planOutput.contains("\"nereusCommit\": \"$authorization\"")) {
            "allocator formal plan-only source tuple differs from the authorized commit"
        }

        val pulsar = requireCheckout(
            "v2M3AllocatorV2PulsarCheckout",
            "v2M3AllocatorV2PulsarCommit",
        )
        val oxiaServer = requireCheckout(
            "v2M3AllocatorV2OxiaServerCheckout",
            "v2M3AllocatorV2OxiaServerCommit",
        )
        val oxiaClient = requireCheckout(
            "v2M3AllocatorV2OxiaClientCheckout",
            "v2M3AllocatorV2OxiaClientCommit",
        )
        check(planOutput.contains("\"pulsarCommit\": \"${git(pulsar, "rev-parse", "HEAD")}\"")) {
            "allocator formal Pulsar source differs from the source-lock tuple"
        }
        check(planOutput.contains("\"oxiaServerCommit\": \"${git(oxiaServer, "rev-parse", "HEAD")}\"")) {
            "allocator formal Oxia-server source differs from the source-lock tuple"
        }
        check(planOutput.contains("\"oxiaClientCommit\": \"${git(oxiaClient, "rev-parse", "HEAD")}\"")) {
            "allocator formal Oxia-client source differs from the source-lock tuple"
        }

        val oxiaClientJar = file(required("v2M3AllocatorV2OxiaClientJarPath")).canonicalFile
        val expectedOxiaClientJar = required("v2M3AllocatorV2OxiaClientJarSha256")
        check(sha256(oxiaClientJar) == expectedOxiaClientJar) {
            "allocator formal Oxia-client JAR differs from the explicit source tuple"
        }
        check(planOutput.contains("\"oxiaClientJarSha256\": \"$expectedOxiaClientJar\"")) {
            "allocator formal Oxia-client JAR differs from the source-lock tuple"
        }
        val expectedOxiaImage = required("v2M3AllocatorV2OxiaImageDigest")
        check(planOutput.contains("\"oxiaServerImageDigest\": \"$expectedOxiaImage\"")) {
            "allocator formal Oxia image differs from the source-lock tuple"
        }
        val oxiaContainer = required("v2M3AllocatorV2OxiaContainerName")
        check(command("docker", "inspect", "--format", "{{.State.Running}}", oxiaContainer) == "true") {
            "allocator formal Oxia container is not running"
        }
        check(command("docker", "inspect", "--format", "{{.Image}}", oxiaContainer) == expectedOxiaImage) {
            "allocator formal Oxia container image differs from the source-lock tuple"
        }
        check(
            command(
                "docker",
                "inspect",
                "--format",
                "{{index .Config.Labels \"com.nereusstream.evidence\"}}",
                oxiaContainer,
            ) == "v2-m3-bounded-adaptive-formal",
        ) {
            "allocator formal Oxia container is not owned by the bounded-adaptive launcher"
        }
        check(
            command(
                "docker",
                "image",
                "inspect",
                expectedOxiaImage,
                "--format",
                "{{index .Config.Labels \"org.opencontainers.image.revision\"}}",
            ) == git(oxiaServer, "rev-parse", "HEAD"),
        ) {
            "allocator formal Oxia image revision differs from the locked Oxia-server source"
        }
        val oxiaServiceAddress = required("v2M3AllocatorV2OxiaServiceAddress")
        val oxiaBoundPort = command("docker", "port", oxiaContainer, "6648/tcp")
            .lineSequence()
            .single()
            .substringAfterLast(':')
        check(oxiaServiceAddress == "127.0.0.1:$oxiaBoundPort") {
            "allocator formal Oxia service address differs from the task-owned container"
        }
        val evidenceArtifact = realAllocatorEvidenceArtifactJar.get().archiveFile.get().asFile
        val executorSha = sha256(evidenceArtifact)
        check(executorSha == required("v2M3AllocatorV2ExecutorSha256")) {
            "allocator formal executor artifact differs from the explicit authorization tuple"
        }

        listOf(
            "v2M3AllocatorV2InputAttachment",
            "v2M3AllocatorV2ResumeAttachment",
            "v2M3AllocatorV1EvidenceDirectory",
            "v2M3AllocatorDiagnosticDirectory",
        ).forEach { forbidden ->
            check(!providers.gradleProperty(forbidden).isPresent) {
                "$forbidden is forbidden for the V2 formal campaign"
            }
        }
        val outputDirectory = file(required("v2M3AllocatorV2FormalOutputDirectory")).toPath()
            .toAbsolutePath()
            .normalize()
        val unsafe = outputDirectory.toString().lowercase()
        check(listOf("full-matrix", "diagnostic", "nare1", "naea1", "nars1").none(unsafe::contains)) {
            "allocator formal output path aliases an old V1 or diagnostic directory"
        }
        if (Files.exists(outputDirectory, LinkOption.NOFOLLOW_LINKS)) {
            check(Files.isDirectory(outputDirectory, LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(outputDirectory)) {
                "allocator formal output is not a real directory"
            }
            Files.list(outputDirectory).use { entries ->
                check(entries.findAny().isEmpty) { "allocator formal output directory is not empty" }
            }
        } else {
            Files.createDirectories(outputDirectory)
        }

        systemProperty("nereus.m3.allocator.v2.formal.authorizedCommit", authorization)
        systemProperty("nereus.m3.allocator.v2.formal.zeroDecisionPlanSha256", expectedPlan)
        systemProperty("nereus.m3.allocator.v2.formal.outputDirectory", outputDirectory.toString())
        systemProperty(
            "nereus.m3.allocator.v2.formal.oxiaServiceAddress",
            oxiaServiceAddress,
        )
        systemProperty(
            "nereus.m3.allocator.v2.formal.oxiaImageDigest",
            expectedOxiaImage,
        )
        systemProperty("nereus.m3.allocator.v2.formal.dependencyLockSha256", expectedDependencyLock)
        systemProperty("nereus.m3.allocator.v2.formal.executorSha256", executorSha)
    }
}

val realAllocatorV3BoundedAdaptiveFormalCampaign = tasks.register<Test>(
    "realAllocatorV3BoundedAdaptiveFormalCampaign",
) {
    group = "verification"
    description =
        "Separately authorized ADR-0108 bounded-adaptive V3 campaign; never runs from build, check, or v2M3Check."
    notCompatibleWithConfigurationCache("formal preflight inspects live Git and the task-owned Oxia container")
    dependsOn(realAllocatorEvidenceArtifactJar)
    testClassesDirs = realAllocatorTest.output.classesDirs
    classpath = realAllocatorEvidenceRuntimeClasspath
    maxParallelForks = 1
    maxHeapSize = "6144m"
    timeout.set(Duration.ofSeconds(48_000))
    useJUnitPlatform()
    filter {
        includeTestsMatching(
            "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3BoundedAdaptiveFormalCampaignTest",
        )
    }
    outputs.upToDateWhen { false }
    doFirst {
        fun required(property: String): String = providers.gradleProperty(property)
            .orNull
            ?.takeIf { it.isNotBlank() }
            ?: error("$property is required for the V3 bounded-adaptive formal campaign")
        fun command(vararg command: String): String {
            val process = ProcessBuilder(*command)
                .directory(rootProject.projectDir)
                .redirectErrorStream(true)
                .start()
            val output = process.inputStream.bufferedReader().use { it.readText() }.trim()
            check(process.waitFor() == 0) {
                "allocator V3 formal preflight command failed: ${command.joinToString(" ")}\n$output"
            }
            return output
        }
        fun git(vararg arguments: String): String =
            command("git", "-C", rootProject.projectDir.absolutePath, *arguments)
        fun sha256(path: File): String {
            check(path.isFile) { "allocator V3 formal hash input is absent: ${path.absolutePath}" }
            val digest = MessageDigest.getInstance("SHA-256")
            path.inputStream().use { input ->
                val buffer = ByteArray(64 * 1024)
                while (true) {
                    val read = input.read(buffer)
                    if (read < 0) break
                    digest.update(buffer, 0, read)
                }
            }
            return digest.digest().joinToString("") { "%02x".format(it) }
        }

        val authorization = required("v2M3AllocatorV3FormalAuthorizationSha")
        check(git("rev-parse", "HEAD") == authorization) { "allocator V3 formal HEAD differs" }
        check(git("branch", "--show-current") == "main") { "allocator V3 formal source is not main" }
        check(git("rev-parse", "refs/remotes/origin/main") == authorization) {
            "allocator V3 formal origin/main differs"
        }
        check(git("status", "--porcelain", "--untracked-files=all").isEmpty()) {
            "allocator V3 formal Nereus worktree is not clean"
        }
        val planOutput = command("python3", rootProject.file("scripts/v2-m3-allocator-plan-v3.py").absolutePath)
        val expectedPlan = required("v2M3AllocatorV3ZeroDecisionPlanSha256")
        check(planOutput.contains("\"feasibilityStatus\": \"PLAN_FEASIBLE\"")) {
            "allocator V3 formal feasibility gate did not pass"
        }
        check(planOutput.contains("\"zeroDecisionPlanSha256\": \"$expectedPlan\"")) {
            "allocator V3 formal plan digest differs"
        }
        check(planOutput.contains("\"nereusCommit\": \"$authorization\"")) {
            "allocator V3 formal plan source tuple differs"
        }
        val expectedDependencyLock = required("v2M3AllocatorV3DependencyLockSha256")
        check(planOutput.contains("\"dependencyLockSha256\": \"$expectedDependencyLock\"")) {
            "allocator V3 formal dependency-lock digest differs"
        }
        val expectedOxiaImage = required("v2M3AllocatorV3OxiaImageDigest")
        val oxiaContainer = required("v2M3AllocatorV3OxiaContainerName")
        check(command("docker", "inspect", "--format", "{{.State.Running}}", oxiaContainer) == "true") {
            "allocator V3 formal Oxia container is not running"
        }
        check(command("docker", "inspect", "--format", "{{.Image}}", oxiaContainer) == expectedOxiaImage) {
            "allocator V3 formal Oxia image differs"
        }
        check(
            command(
                "docker",
                "inspect",
                "--format",
                "{{index .Config.Labels \"com.nereusstream.evidence\"}}",
                oxiaContainer,
            ) == "v3-m3-bounded-adaptive-formal",
        ) { "allocator V3 formal Oxia container ownership differs" }
        val oxiaServiceAddress = required("v2M3AllocatorV3OxiaServiceAddress")
        val oxiaBoundPort = command("docker", "port", oxiaContainer, "6648/tcp")
            .lineSequence()
            .single()
            .substringAfterLast(':')
        check(oxiaServiceAddress == "127.0.0.1:$oxiaBoundPort") {
            "allocator V3 formal Oxia service address differs"
        }
        val evidenceArtifact = realAllocatorEvidenceArtifactJar.get().archiveFile.get().asFile
        val executorSha = sha256(evidenceArtifact)
        check(executorSha == required("v2M3AllocatorV3ExecutorSha256")) {
            "allocator V3 formal executor artifact differs"
        }
        val outputDirectory = file(required("v2M3AllocatorV3FormalOutputDirectory")).toPath()
            .toAbsolutePath()
            .normalize()
        check(Files.isDirectory(outputDirectory, LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(outputDirectory)) {
            "allocator V3 formal output is absent or nonregular"
        }
        Files.list(outputDirectory).use { entries ->
            check(entries.findAny().isEmpty) { "allocator V3 formal output is not empty"
            }
        }

        systemProperty("nereus.m3.allocator.v3.formal.authorizedCommit", authorization)
        systemProperty("nereus.m3.allocator.v3.formal.zeroDecisionPlanSha256", expectedPlan)
        systemProperty("nereus.m3.allocator.v3.formal.outputDirectory", outputDirectory.toString())
        systemProperty("nereus.m3.allocator.v3.formal.oxiaServiceAddress", oxiaServiceAddress)
        systemProperty("nereus.m3.allocator.v3.formal.oxiaImageDigest", expectedOxiaImage)
        systemProperty("nereus.m3.allocator.v3.formal.dependencyLockSha256", expectedDependencyLock)
        systemProperty("nereus.m3.allocator.v3.formal.executorSha256", executorSha)
    }
}

val realAllocatorV4BoundedAdaptiveFormalCampaign = tasks.register<Test>(
    "realAllocatorV4BoundedAdaptiveFormalCampaign",
) {
    group = "verification"
    description =
        "Separately authorized ADR-0125 bounded-adaptive V4 campaign; never runs from build, check, or v2M3Check."
    notCompatibleWithConfigurationCache("formal preflight inspects live Git and the task-owned Oxia container")
    dependsOn(realAllocatorEvidenceArtifactJar)
    testClassesDirs = realAllocatorTest.output.classesDirs
    classpath = realAllocatorEvidenceRuntimeClasspath
    maxParallelForks = 1
    maxHeapSize = "6144m"
    timeout.set(Duration.ofSeconds(48_000))
    useJUnitPlatform()
    filter {
        includeTestsMatching(
            "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V4BoundedAdaptiveFormalCampaignTest",
        )
    }
    outputs.upToDateWhen { false }
    doFirst {
        fun required(property: String): String = providers.gradleProperty(property)
            .orNull
            ?.takeIf { it.isNotBlank() }
            ?: error("$property is required for the V4 bounded-adaptive formal campaign")
        fun command(vararg command: String): String {
            val process = ProcessBuilder(*command)
                .directory(rootProject.projectDir)
                .redirectErrorStream(true)
                .start()
            val output = process.inputStream.bufferedReader().use { it.readText() }.trim()
            check(process.waitFor() == 0) {
                "allocator V4 formal preflight command failed: ${command.joinToString(" ")}\n$output"
            }
            return output
        }
        fun git(directory: File, vararg arguments: String): String =
            command("git", "-C", directory.absolutePath, *arguments)
        fun sha256(path: File): String {
            check(path.isFile && !Files.isSymbolicLink(path.toPath())) {
                "allocator V4 formal hash input is absent or a link: ${path.absolutePath}"
            }
            val digest = MessageDigest.getInstance("SHA-256")
            path.inputStream().use { input ->
                val buffer = ByteArray(64 * 1024)
                while (true) {
                    val read = input.read(buffer)
                    if (read < 0) break
                    digest.update(buffer, 0, read)
                }
            }
            return digest.digest().joinToString("") { "%02x".format(it) }
        }

        val authorization = required("v2M3AllocatorV4FormalAuthorizationSha")
        check(git(rootProject.projectDir, "rev-parse", "HEAD") == authorization) {
            "allocator V4 formal HEAD differs"
        }
        check(git(rootProject.projectDir, "branch", "--show-current") == "main") {
            "allocator V4 formal source is not main"
        }
        check(git(rootProject.projectDir, "rev-parse", "refs/remotes/origin/main") == authorization) {
            "allocator V4 formal origin/main differs"
        }
        check(git(rootProject.projectDir, "status", "--porcelain", "--untracked-files=all").isEmpty()) {
            "allocator V4 formal Nereus worktree is not clean"
        }

        val planOutput = command("python3", rootProject.file("scripts/v2-m3-allocator-plan-v4.py").absolutePath)
        val expectedPlan = required("v2M3AllocatorV4ZeroDecisionPlanSha256")
        val expectedProfile = required("v2M3AllocatorV4NativeExecutionProfileSha256")
        val expectedSourceLocks = required("v2M3AllocatorV4SourceLocksSha256")
        val expectedDependencyLock = required("v2M3AllocatorV4DependencyLockSha256")
        check(planOutput.contains("\"feasibilityStatus\": \"PLAN_FEASIBLE\"")) {
            "allocator V4 formal feasibility gate did not pass"
        }
        check(planOutput.contains("\"zeroDecisionPlanSha256\": \"$expectedPlan\"")) {
            "allocator V4 formal plan digest differs"
        }
        check(planOutput.contains("\"nativeExecutionProfileSha256\": \"$expectedProfile\"")) {
            "allocator V4 formal execution profile differs"
        }
        check(planOutput.contains("\"nereusCommit\": \"$authorization\"")) {
            "allocator V4 formal plan source tuple differs"
        }
        check(planOutput.contains("\"sourceLocksSha256\": \"$expectedSourceLocks\"")) {
            "allocator V4 formal source-lock digest differs"
        }
        check(planOutput.contains("\"dependencyLockSha256\": \"$expectedDependencyLock\"")) {
            "allocator V4 formal dependency-lock digest differs"
        }
        check(sha256(rootProject.file("docs/v2/source-locks.json")) == expectedSourceLocks) {
            "allocator V4 formal source-lock bytes differ"
        }

        val pulsarCheckout = file(required("v2M3AllocatorV4PulsarCheckout"))
        val oxiaServerCheckout = file(required("v2M3AllocatorV4OxiaServerCheckout"))
        val oxiaClientCheckout = file(required("v2M3AllocatorV4OxiaClientCheckout"))
        val pulsarCommit = required("v2M3AllocatorV4PulsarCommit")
        val oxiaServerCommit = required("v2M3AllocatorV4OxiaServerCommit")
        val oxiaClientCommit = required("v2M3AllocatorV4OxiaClientCommit")
        listOf(
            Triple("Pulsar", pulsarCheckout, pulsarCommit),
            Triple("Oxia-server", oxiaServerCheckout, oxiaServerCommit),
            Triple("Oxia-client", oxiaClientCheckout, oxiaClientCommit),
        ).forEach { (label, checkout, commit) ->
            check(git(checkout, "rev-parse", "HEAD") == commit) {
                "allocator V4 formal $label checkout commit differs"
            }
            check(git(checkout, "status", "--porcelain", "--untracked-files=all").isEmpty()) {
                "allocator V4 formal $label checkout is not clean"
            }
        }
        check(planOutput.contains("\"pulsarCommit\": \"$pulsarCommit\"")) {
            "allocator V4 formal Pulsar source tuple differs"
        }
        check(planOutput.contains("\"oxiaServerCommit\": \"$oxiaServerCommit\"")) {
            "allocator V4 formal Oxia-server source tuple differs"
        }
        check(planOutput.contains("\"oxiaClientCommit\": \"$oxiaClientCommit\"")) {
            "allocator V4 formal Oxia-client source tuple differs"
        }
        val oxiaClientJar = file(required("v2M3AllocatorV4OxiaClientJarPath"))
        val oxiaClientJarSha = required("v2M3AllocatorV4OxiaClientJarSha256")
        check(sha256(oxiaClientJar) == oxiaClientJarSha) {
            "allocator V4 formal Oxia-client JAR differs"
        }
        check(planOutput.contains("\"oxiaClientJarSha256\": \"$oxiaClientJarSha\"")) {
            "allocator V4 formal Oxia-client JAR source tuple differs"
        }

        val expectedOxiaImage = required("v2M3AllocatorV4OxiaImageDigest")
        check(planOutput.contains("\"oxiaServerImageDigest\": \"$expectedOxiaImage\"")) {
            "allocator V4 formal Oxia image source tuple differs"
        }
        val oxiaContainer = required("v2M3AllocatorV4OxiaContainerName")
        check(command("docker", "inspect", "--format", "{{.State.Running}}", oxiaContainer) == "true") {
            "allocator V4 formal Oxia container is not running"
        }
        check(command("docker", "inspect", "--format", "{{.Image}}", oxiaContainer) == expectedOxiaImage) {
            "allocator V4 formal Oxia image differs"
        }
        check(
            command(
                "docker",
                "inspect",
                "--format",
                "{{index .Config.Labels \"com.nereusstream.evidence\"}}",
                oxiaContainer,
            ) == "v4-m3-bounded-adaptive-formal",
        ) { "allocator V4 formal Oxia container ownership differs" }
        check(
            command(
                "docker",
                "inspect",
                "--format",
                "{{index .Config.Labels \"org.opencontainers.image.revision\"}}",
                oxiaContainer,
            ) == oxiaServerCommit,
        ) { "allocator V4 formal Oxia container revision differs" }
        val oxiaServiceAddress = required("v2M3AllocatorV4OxiaServiceAddress")
        val oxiaBoundPort = command("docker", "port", oxiaContainer, "6648/tcp")
            .lineSequence()
            .single()
            .substringAfterLast(':')
        check(oxiaServiceAddress == "127.0.0.1:$oxiaBoundPort") {
            "allocator V4 formal Oxia service address differs"
        }

        val evidenceArtifact = realAllocatorEvidenceArtifactJar.get().archiveFile.get().asFile
        val executorSha = sha256(evidenceArtifact)
        check(executorSha == required("v2M3AllocatorV4ExecutorSha256")) {
            "allocator V4 formal executor artifact differs"
        }
        val diagnosticReceipt = file(required("v2M3AllocatorV4DiagnosticPath"))
        check(sha256(diagnosticReceipt) == required("v2M3AllocatorV4DiagnosticSha256")) {
            "allocator V4 formal NADV4 receipt differs"
        }
        val diagnosticJUnit = file(required("v2M3AllocatorV4DiagnosticJUnitDirectory")).toPath()
            .toAbsolutePath()
            .normalize()
        check(Files.isDirectory(diagnosticJUnit, LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(diagnosticJUnit)) {
            "allocator V4 formal diagnostic JUnit directory is absent or a link"
        }
        Files.list(diagnosticJUnit).use { files ->
            check(files.filter { it.fileName.toString().matches(Regex("TEST-[A-Za-z0-9_.]+\\.xml")) }.count() == 8L) {
                "allocator V4 formal diagnostic JUnit file inventory differs"
            }
        }

        val outputDirectory = file(required("v2M3AllocatorV4FormalOutputDirectory")).toPath()
            .toAbsolutePath()
            .normalize()
        val unsafe = outputDirectory.toString().lowercase()
        check(listOf("full-matrix", "diagnostic", "nare1", "naea1", "nars1").none(unsafe::contains)) {
            "allocator V4 formal output aliases an old V1 or diagnostic directory"
        }
        check(Files.isDirectory(outputDirectory, LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(outputDirectory)) {
            "allocator V4 formal output is absent or nonregular"
        }
        Files.list(outputDirectory).use { entries ->
            check(entries.findAny().isEmpty) { "allocator V4 formal output is not empty" }
        }

        systemProperty("nereus.m3.allocator.v4.formal.authorizedCommit", authorization)
        systemProperty("nereus.m3.allocator.v4.formal.zeroDecisionPlanSha256", expectedPlan)
        systemProperty("nereus.m3.allocator.v4.formal.outputDirectory", outputDirectory.toString())
        systemProperty("nereus.m3.allocator.v4.formal.oxiaServiceAddress", oxiaServiceAddress)
        systemProperty("nereus.m3.allocator.v4.formal.oxiaImageDigest", expectedOxiaImage)
        systemProperty("nereus.m3.allocator.v4.formal.dependencyLockSha256", expectedDependencyLock)
        systemProperty("nereus.m3.allocator.v4.formal.executorSha256", executorSha)
    }
}

val realAllocatorV2ShortDiagnosticTest = tasks.register<Test>("realAllocatorV2ShortDiagnosticTest") {
    group = "verification"
    description =
        "Run the four-test non-promotable V2 STRICT/installed-RANGE/renewal/conflict diagnostic against real Oxia."
    dependsOn(realAllocatorEvidenceArtifactJar)
    testClassesDirs = realAllocatorTest.output.classesDirs
    classpath = realAllocatorEvidenceRuntimeClasspath
    maxParallelForks = 1
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V2RealOxiaDiagnosticTest")
    }
    outputs.upToDateWhen { false }
    doFirst {
        fun required(property: String): String = providers.gradleProperty(property)
            .orNull
            ?: error("$property is required for the V2 allocator short diagnostic")
        systemProperty("nereus.m3.allocator.v2.oxiaServiceAddress", required("v2M3AllocatorOxiaServiceAddress"))
        systemProperty("nereus.m3.allocator.v2.nereusCommit", required("v2M3AllocatorV2NereusCommit"))
        systemProperty("nereus.m3.allocator.v2.diagnosticRunId", required("v2M3AllocatorV2DiagnosticRunId"))
    }
}

val realAllocatorV3DiagnosticTest = tasks.register<Test>("realAllocatorV3DiagnosticTest") {
    group = "verification"
    description =
        "Run diagnostic-only V3 runner, real-Oxia operation, allocator workflow, and native-path measurements."
    dependsOn(realAllocatorEvidenceArtifactJar)
    testClassesDirs = realAllocatorTest.output.classesDirs
    classpath = realAllocatorEvidenceRuntimeClasspath
    maxParallelForks = 1
    forkEvery = 1
    maxHeapSize = "6144m"
    timeout.set(Duration.ofMinutes(60))
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AsyncActorLaneRunnerTest")
        includeTestsMatching(
            "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3RealOxiaOperationDiagnosticTest",
        )
        includeTestsMatching(
            "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AllocatorWorkflowDiagnosticTest",
        )
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3NativePathDiagnosticTest")
        includeTestsMatching(
            "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3NativeBaselineCanaryTest",
        )
        includeTestsMatching(
            "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3RealAllocatorStrictIntervalDiagnosticTest",
        )
    }
    outputs.upToDateWhen { false }
    doFirst {
        fun required(property: String): String = providers.gradleProperty(property)
            .orNull
            ?: error("$property is required for the V3 allocator diagnostic")
        val output = file(required("v2M3AllocatorV3DiagnosticOutput")).toPath().toAbsolutePath().normalize()
        require(!Files.exists(output, LinkOption.NOFOLLOW_LINKS)) {
            "V3 allocator diagnostic output already exists: $output"
        }
        val parent = output.parent
        require(parent != null && Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(parent)) {
            "V3 allocator diagnostic output parent is absent or nonregular: $parent"
        }
        Files.createDirectory(output)
        systemProperty("nereus.m3.allocator.v3.oxiaServiceAddress", required("v2M3AllocatorOxiaServiceAddress"))
        systemProperty("nereus.m3.allocator.v3.nereusCommit", required("v2M3AllocatorV3NereusCommit"))
        systemProperty("nereus.m3.allocator.v3.diagnosticRunId", required("v2M3AllocatorV3DiagnosticRunId"))
        systemProperty("nereus.m3.allocator.v3.diagnosticOutput", output.toString())
    }
}

val realAllocatorV3NativeCanaryTest = tasks.register<Test>("realAllocatorV3NativeCanaryTest") {
    group = "verification"
    description = "Run the exact-schedule diagnostic-only ADR-0109 Native baseline conformance canary."
    dependsOn(realAllocatorEvidenceArtifactJar)
    testClassesDirs = realAllocatorTest.output.classesDirs
    classpath = realAllocatorEvidenceRuntimeClasspath
    maxParallelForks = 1
    maxHeapSize = "6144m"
    timeout.set(Duration.ofMinutes(45))
    useJUnitPlatform()
    filter {
        includeTestsMatching(
            "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3NativeBaselineCanaryTest",
        )
    }
    outputs.upToDateWhen { false }
    doFirst {
        fun required(property: String): String = providers.gradleProperty(property)
            .orNull
            ?: error("$property is required for the V3 Native canary")
        val output = file(required("v2M3AllocatorV3NativeCanaryOutput")).toPath().toAbsolutePath().normalize()
        require(!Files.exists(output, LinkOption.NOFOLLOW_LINKS)) {
            "V3 allocator Native canary output already exists: $output"
        }
        val parent = output.parent
        require(parent != null && Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(parent)) {
            "V3 allocator Native canary output parent is absent or nonregular: $parent"
        }
        Files.createDirectory(output)
        systemProperty("nereus.m3.allocator.v3.nereusCommit", required("v2M3AllocatorV3NereusCommit"))
        systemProperty("nereus.m3.allocator.v3.diagnosticRunId", required("v2M3AllocatorV3NativeCanaryRunId"))
        systemProperty("nereus.m3.allocator.v3.diagnosticOutput", output.toString())
    }
}

val realAllocatorV3DiagnosticJUnitDirectory = layout.buildDirectory.dir(
    "test-results/realAllocatorV3DiagnosticTest",
)

val realAllocatorV4DiagnosticTest = tasks.register<Test>("realAllocatorV4DiagnosticTest") {
    group = "verification"
    description = "Run the complete diagnostic-only ADR-0125 V4 current-source inventory."
    dependsOn(realAllocatorEvidenceArtifactJar)
    testClassesDirs = realAllocatorTest.output.classesDirs
    classpath = realAllocatorEvidenceRuntimeClasspath
    maxParallelForks = 1
    forkEvery = 1
    maxHeapSize = "6144m"
    timeout.set(Duration.ofMinutes(75))
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AsyncActorLaneRunnerTest")
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V4AsyncActorLaneRunnerTest")
        includeTestsMatching(
            "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3RealOxiaOperationDiagnosticTest",
        )
        includeTestsMatching(
            "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AllocatorWorkflowDiagnosticTest",
        )
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3NativePathDiagnosticTest")
        includeTestsMatching(
            "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3NativeBaselineCanaryTest",
        )
        includeTestsMatching(
            "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V4TerminalAdmissionDrainDiagnosticTest",
        )
        includeTestsMatching(
            "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3RealAllocatorStrictIntervalDiagnosticTest",
        )
    }
    outputs.upToDateWhen { false }
    doFirst {
        fun required(property: String): String = providers.gradleProperty(property)
            .orNull
            ?: error("$property is required for the V4 allocator diagnostic")
        val output = file(required("v2M3AllocatorV4DiagnosticOutput")).toPath().toAbsolutePath().normalize()
        require(!Files.exists(output, LinkOption.NOFOLLOW_LINKS)) {
            "V4 allocator diagnostic output already exists: $output"
        }
        val parent = output.parent
        require(parent != null && Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(parent)) {
            "V4 allocator diagnostic output parent is absent or nonregular: $parent"
        }
        Files.createDirectory(output)
        systemProperty("nereus.m3.allocator.protocol", "V4")
        systemProperty("nereus.m3.allocator.v3.oxiaServiceAddress", required("v2M3AllocatorOxiaServiceAddress"))
        systemProperty("nereus.m3.allocator.v3.nereusCommit", required("v2M3AllocatorV4NereusCommit"))
        systemProperty("nereus.m3.allocator.v3.diagnosticRunId", required("v2M3AllocatorV4DiagnosticRunId"))
        systemProperty("nereus.m3.allocator.v3.diagnosticOutput", output.toString())
    }
}

val realAllocatorV4DiagnosticJUnitDirectory = layout.buildDirectory.dir(
    "test-results/realAllocatorV4DiagnosticTest",
)

tasks.register<Test>("realAllocatorV3CandidateCutoffDiagnosticTest") {
    group = "verification"
    description = "Run the diagnostic-only exact RANGE cutoff attribution sequence; never emits formal evidence."
    dependsOn(realAllocatorEvidenceArtifactJar)
    testClassesDirs = realAllocatorTest.output.classesDirs
    classpath = realAllocatorEvidenceRuntimeClasspath
    maxParallelForks = 1
    maxHeapSize = "6144m"
    timeout.set(Duration.ofMinutes(15))
    useJUnitPlatform()
    filter {
        includeTestsMatching(
            "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3CandidateCutoffDiagnosticTest",
        )
    }
    outputs.upToDateWhen { false }
    doFirst {
        fun required(property: String): String = providers.gradleProperty(property)
            .orNull
            ?: error("$property is required for the V3 candidate cutoff diagnostic")
        val output = file(required("v2M3AllocatorV3CandidateCutoffDiagnosticOutput"))
            .toPath()
            .toAbsolutePath()
            .normalize()
        require(!Files.exists(output, LinkOption.NOFOLLOW_LINKS)) {
            "V3 allocator candidate cutoff diagnostic output already exists: $output"
        }
        val parent = output.parent
        require(parent != null && Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(parent)) {
            "V3 allocator candidate cutoff diagnostic output parent is absent or nonregular: $parent"
        }
        Files.createDirectory(output)
        systemProperty("nereus.m3.allocator.v3.oxiaServiceAddress", required("v2M3AllocatorOxiaServiceAddress"))
        systemProperty("nereus.m3.allocator.v3.nereusCommit", required("v2M3AllocatorV3NereusCommit"))
        systemProperty("nereus.m3.allocator.v3.diagnosticRunId", required("v2M3AllocatorV3CandidateCutoffRunId"))
        systemProperty("nereus.m3.allocator.v3.diagnosticOutput", output.toString())
    }
}

tasks.register<JavaExec>("sealRealAllocatorV3Diagnostic") {
    group = "verification"
    description = "Seal the complete current-source V3 diagnostic JUnit inventory as non-promotable NADV3."
    dependsOn(realAllocatorV3DiagnosticTest, realAllocatorEvidenceArtifactJar)
    classpath = realAllocatorEvidenceRuntimeClasspath
    mainClass.set("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AllocatorProtocolMain")
    outputs.upToDateWhen { false }
    doFirst {
        fun required(property: String): String = providers.gradleProperty(property)
            .orNull
            ?: error("$property is required for V3 allocator diagnostic sealing")
        setArgs(
            listOf(
                "seal-diagnostic",
                realAllocatorV3DiagnosticJUnitDirectory.get().asFile.absolutePath,
                file(required("v2M3AllocatorV3DiagnosticReceiptOutput")).absolutePath,
                required("v2M3AllocatorV3NereusCommit"),
                required("v2M3AllocatorV3OxiaImageDigest"),
                required("v2M3AllocatorV3DependencyLockDigest"),
                required("v2M3AllocatorV3ExecutorDigest"),
                required("v2M3AllocatorV3WorkloadDigest"),
            ),
        )
    }
}

tasks.register<JavaExec>("validateRealAllocatorV3Diagnostic") {
    group = "verification"
    description = "Parse-canonically revalidate the complete current-source V3 NADV3 and JUnit inventory."
    dependsOn("sealRealAllocatorV3Diagnostic", realAllocatorEvidenceArtifactJar)
    classpath = realAllocatorEvidenceRuntimeClasspath
    mainClass.set("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AllocatorProtocolMain")
    outputs.upToDateWhen { false }
    doFirst {
        fun required(property: String): String = providers.gradleProperty(property)
            .orNull
            ?: error("$property is required for V3 allocator diagnostic validation")
        setArgs(
            listOf(
                "validate-diagnostic",
                file(required("v2M3AllocatorV3DiagnosticReceiptOutput")).absolutePath,
                realAllocatorV3DiagnosticJUnitDirectory.get().asFile.absolutePath,
                required("v2M3AllocatorV3NereusCommit"),
                required("v2M3AllocatorV3OxiaImageDigest"),
                required("v2M3AllocatorV3DependencyLockDigest"),
                required("v2M3AllocatorV3ExecutorDigest"),
                required("v2M3AllocatorV3WorkloadDigest"),
            ),
        )
    }
}

tasks.register<JavaExec>("sealRealAllocatorV4Diagnostic") {
    group = "verification"
    description = "Seal the complete current-source V4 diagnostic JUnit inventory as non-promotable NADV4."
    dependsOn(realAllocatorV4DiagnosticTest, realAllocatorEvidenceArtifactJar)
    classpath = realAllocatorEvidenceRuntimeClasspath
    mainClass.set("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V4AllocatorProtocolMain")
    outputs.upToDateWhen { false }
    doFirst {
        fun required(property: String): String = providers.gradleProperty(property)
            .orNull
            ?: error("$property is required for V4 allocator diagnostic sealing")
        setArgs(
            listOf(
                "seal-diagnostic",
                realAllocatorV4DiagnosticJUnitDirectory.get().asFile.absolutePath,
                file(required("v2M3AllocatorV4DiagnosticReceiptOutput")).absolutePath,
                required("v2M3AllocatorV4NereusCommit"),
                required("v2M3AllocatorV4OxiaImageDigest"),
                required("v2M3AllocatorV4DependencyLockDigest"),
                required("v2M3AllocatorV4ExecutorDigest"),
                required("v2M3AllocatorV4WorkloadDigest"),
            ),
        )
    }
}

tasks.register<JavaExec>("validateRealAllocatorV4Diagnostic") {
    group = "verification"
    description = "Parse-canonically revalidate the complete current-source V4 NADV4 and JUnit inventory."
    dependsOn("sealRealAllocatorV4Diagnostic", realAllocatorEvidenceArtifactJar)
    classpath = realAllocatorEvidenceRuntimeClasspath
    mainClass.set("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V4AllocatorProtocolMain")
    outputs.upToDateWhen { false }
    doFirst {
        fun required(property: String): String = providers.gradleProperty(property)
            .orNull
            ?: error("$property is required for V4 allocator diagnostic validation")
        setArgs(
            listOf(
                "validate-diagnostic",
                file(required("v2M3AllocatorV4DiagnosticReceiptOutput")).absolutePath,
                realAllocatorV4DiagnosticJUnitDirectory.get().asFile.absolutePath,
                required("v2M3AllocatorV4NereusCommit"),
                required("v2M3AllocatorV4OxiaImageDigest"),
                required("v2M3AllocatorV4DependencyLockDigest"),
                required("v2M3AllocatorV4ExecutorDigest"),
                required("v2M3AllocatorV4WorkloadDigest"),
            ),
        )
    }
}

tasks.register<JavaExec>("validateExistingRealAllocatorV4Diagnostic") {
    group = "verification"
    description = "Offline parse-canonical validation of an existing exact-source NADV4 and JUnit inventory."
    dependsOn(realAllocatorEvidenceArtifactJar)
    classpath = realAllocatorEvidenceRuntimeClasspath
    mainClass.set("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V4AllocatorProtocolMain")
    outputs.upToDateWhen { false }
    doFirst {
        fun required(property: String): String = providers.gradleProperty(property)
            .orNull
            ?: error("$property is required for existing V4 allocator diagnostic validation")
        setArgs(
            listOf(
                "validate-diagnostic",
                file(required("v2M3AllocatorV4DiagnosticPath")).absolutePath,
                file(required("v2M3AllocatorV4DiagnosticJUnitDirectory")).absolutePath,
                required("v2M3AllocatorV4NereusCommit"),
                required("v2M3AllocatorV4OxiaImageDigest"),
                required("v2M3AllocatorV4DependencyLockDigest"),
                required("v2M3AllocatorV4ExecutorDigest"),
                required("v2M3AllocatorV4WorkloadDigest"),
            ),
        )
    }
}

val realAllocatorV2DiagnosticJUnitXml = layout.buildDirectory.file(
    "test-results/realAllocatorV2ShortDiagnosticTest/" +
        "TEST-com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V2RealOxiaDiagnosticTest.xml",
)

tasks.register<JavaExec>("sealRealAllocatorV2Diagnostic") {
    group = "verification"
    description = "Seal the exact four-test real-Oxia diagnostic JUnit as non-promotable NADV2."
    dependsOn(realAllocatorV2ShortDiagnosticTest, realAllocatorEvidenceArtifactJar)
    classpath = realAllocatorEvidenceRuntimeClasspath
    mainClass.set("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V2AllocatorProtocolMain")
    outputs.upToDateWhen { false }
    doFirst {
        fun required(property: String): String = providers.gradleProperty(property)
            .orNull
            ?: error("$property is required for V2 allocator diagnostic sealing")
        setArgs(
            listOf(
                "seal-diagnostic",
                realAllocatorV2DiagnosticJUnitXml.get().asFile.absolutePath,
                file(required("v2M3AllocatorV2DiagnosticOutput")).absolutePath,
                required("v2M3AllocatorV2NereusCommit"),
                required("v2M3AllocatorV2OxiaImageDigest"),
                required("v2M3AllocatorV2DependencyLockDigest"),
                required("v2M3AllocatorV2ExecutorDigest"),
                required("v2M3AllocatorV2WorkloadDigest"),
            ),
        )
    }
}

tasks.register<JavaExec>("validateRealAllocatorV2Checkpoint") {
    group = "verification"
    description = "Offline strict NACP2 checkpoint/resume validation; accesses no Oxia service."
    dependsOn(realAllocatorEvidenceArtifactJar)
    classpath = realAllocatorEvidenceRuntimeClasspath
    mainClass.set("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V2AllocatorProtocolMain")
    outputs.upToDateWhen { false }
    doFirst {
        fun required(property: String): String = providers.gradleProperty(property)
            .orNull
            ?: error("$property is required for V2 allocator checkpoint validation")
        setArgs(
            listOf(
                "validate-checkpoint",
                file(required("v2M3AllocatorV2CheckpointPath")).absolutePath,
                required("v2M3AllocatorV2NereusCommit"),
                required("v2M3AllocatorV2OxiaImageDigest"),
                required("v2M3AllocatorV2DependencyLockDigest"),
                required("v2M3AllocatorV2ExecutorDigest"),
                required("v2M3AllocatorV2WorkloadDigest"),
            ),
        )
    }
}

tasks.register<JavaExec>("sealRealAllocatorV2Evaluation") {
    group = "verification"
    description = "Seal one complete validator-reproved NACP2 as NAEV2; NONE/BOTH remain valid and non-promotable."
    dependsOn(realAllocatorEvidenceArtifactJar)
    classpath = realAllocatorEvidenceRuntimeClasspath
    mainClass.set("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V2AllocatorProtocolMain")
    outputs.upToDateWhen { false }
    doFirst {
        fun required(property: String): String = providers.gradleProperty(property)
            .orNull
            ?: error("$property is required for V2 allocator evaluation sealing")
        setArgs(
            listOf(
                "seal-evaluation",
                file(required("v2M3AllocatorV2CheckpointPath")).absolutePath,
                file(required("v2M3AllocatorV2EvaluationOutput")).absolutePath,
                required("v2M3AllocatorV2NereusCommit"),
                required("v2M3AllocatorV2OxiaImageDigest"),
                required("v2M3AllocatorV2DependencyLockDigest"),
                required("v2M3AllocatorV2ExecutorDigest"),
                required("v2M3AllocatorV2WorkloadDigest"),
            ),
        )
    }
}

tasks.register<JavaExec>("realAllocatorV2PromotionCheck") {
    group = "verification"
    description = "Verify exact NAEV2/NACP2/NADV2/JUnit/attachment freshness and emit one promotion decision."
    dependsOn(realAllocatorEvidenceArtifactJar)
    classpath = realAllocatorEvidenceRuntimeClasspath
    mainClass.set("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V2AllocatorProtocolMain")
    outputs.upToDateWhen { false }
    doFirst {
        fun required(property: String): String = providers.gradleProperty(property)
            .orNull
            ?: error("$property is required for the V2 allocator promotion gate")
        setArgs(
            listOf(
                "promotion-check",
                file(required("v2M3AllocatorV2EvaluationPath")).absolutePath,
                file(required("v2M3AllocatorV2CheckpointPath")).absolutePath,
                file(required("v2M3AllocatorV2DiagnosticPath")).absolutePath,
                file(required("v2M3AllocatorV2DiagnosticJUnitPath")).absolutePath,
                file(required("v2M3AllocatorV2FormalJUnitPath")).absolutePath,
                file(required("v2M3AllocatorV2AttachmentDirectory")).absolutePath,
                file(required("v2M3AllocatorV2PromotionOutput")).absolutePath,
                required("v2M3AllocatorV2NereusCommit"),
                required("v2M3AllocatorV2OxiaImageDigest"),
                required("v2M3AllocatorV2DependencyLockDigest"),
                required("v2M3AllocatorV2ExecutorDigest"),
                required("v2M3AllocatorV2WorkloadDigest"),
            ),
        )
    }
}

tasks.register("realAllocatorV2PreCampaignCheck") {
    group = "verification"
    description = "Run every offline V2 allocator prerequisite; this task never starts a formal campaign."
    dependsOn(
        project(":nereus-domain").tasks.named("test"),
        project(":nereus-metadata-spi").tasks.named("test"),
        tasks.named("realAllocatorContractTest"),
        tasks.named("checkstyleRealAllocatorTest"),
    )
}

tasks.register<JavaExec>("validateRealAllocatorV4Checkpoint") {
    group = "verification"
    description = "Offline strict NACP4 checkpoint/resume validation; accesses no Oxia service."
    dependsOn(realAllocatorEvidenceArtifactJar)
    classpath = realAllocatorEvidenceRuntimeClasspath
    mainClass.set("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V4AllocatorProtocolMain")
    outputs.upToDateWhen { false }
    doFirst {
        fun required(property: String): String = providers.gradleProperty(property)
            .orNull
            ?: error("$property is required for V4 allocator checkpoint validation")
        setArgs(
            listOf(
                "validate-checkpoint",
                file(required("v2M3AllocatorV4CheckpointPath")).absolutePath,
                required("v2M3AllocatorV4NereusCommit"),
                required("v2M3AllocatorV4OxiaImageDigest"),
                required("v2M3AllocatorV4DependencyLockDigest"),
                required("v2M3AllocatorV4ExecutorDigest"),
                required("v2M3AllocatorV4WorkloadDigest"),
            ),
        )
    }
}

tasks.register<JavaExec>("sealRealAllocatorV4Evaluation") {
    group = "verification"
    description = "Seal one complete validator-reproved NACP4 as canonical NAEV4."
    dependsOn(realAllocatorEvidenceArtifactJar)
    classpath = realAllocatorEvidenceRuntimeClasspath
    mainClass.set("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V4AllocatorProtocolMain")
    outputs.upToDateWhen { false }
    doFirst {
        fun required(property: String): String = providers.gradleProperty(property)
            .orNull
            ?: error("$property is required for V4 allocator evaluation sealing")
        setArgs(
            listOf(
                "seal-evaluation",
                file(required("v2M3AllocatorV4CheckpointPath")).absolutePath,
                file(required("v2M3AllocatorV4EvaluationOutput")).absolutePath,
                required("v2M3AllocatorV4NereusCommit"),
                required("v2M3AllocatorV4OxiaImageDigest"),
                required("v2M3AllocatorV4DependencyLockDigest"),
                required("v2M3AllocatorV4ExecutorDigest"),
                required("v2M3AllocatorV4WorkloadDigest"),
            ),
        )
    }
}

fun JavaExec.configureAllocatorV4PromotionCommand(command: String, outputProperty: String) {
    group = "verification"
    dependsOn(realAllocatorEvidenceArtifactJar)
    classpath = realAllocatorEvidenceRuntimeClasspath
    mainClass.set("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V4AllocatorProtocolMain")
    outputs.upToDateWhen { false }
    doFirst {
        fun required(property: String): String = providers.gradleProperty(property)
            .orNull
            ?: error("$property is required for the V4 allocator promotion/selection command")
        setArgs(
            listOf(
                command,
                file(required("v2M3AllocatorV4EvaluationPath")).absolutePath,
                file(required("v2M3AllocatorV4CheckpointPath")).absolutePath,
                file(required("v2M3AllocatorV4DiagnosticPath")).absolutePath,
                file(required("v2M3AllocatorV4DiagnosticJUnitPath")).absolutePath,
                file(required("v2M3AllocatorV4FormalJUnitPath")).absolutePath,
                file(required("v2M3AllocatorV4AttachmentDirectory")).absolutePath,
                file(required(outputProperty)).absolutePath,
                required("v2M3AllocatorV4NereusCommit"),
                required("v2M3AllocatorV4OxiaImageDigest"),
                required("v2M3AllocatorV4DependencyLockDigest"),
                required("v2M3AllocatorV4ExecutorDigest"),
                required("v2M3AllocatorV4WorkloadDigest"),
            ),
        )
    }
}

tasks.register<JavaExec>("realAllocatorV4PromotionCheck") {
    description = "Verify exact NAEV4/NACP4/NADV4/JUnit/attachment freshness and emit one promotion decision."
    configureAllocatorV4PromotionCommand("promotion-check", "v2M3AllocatorV4PromotionOutput")
}

tasks.register<JavaExec>("sealRealAllocatorV4Selection") {
    description = "Seal canonical NARS4 only after a unique V4 promotion decision."
    configureAllocatorV4PromotionCommand("seal-selection", "v2M3AllocatorV4SelectionOutput")
}

tasks.register("realAllocatorV4PreCampaignCheck") {
    group = "verification"
    description =
        "Run every offline ADR-0125 V4 feasibility, protocol, terminal-drain, and V3-compatibility prerequisite."
    dependsOn(
        project(":nereus-domain").tasks.named("test"),
        project(":nereus-metadata-spi").tasks.named("test"),
        tasks.named("realAllocatorContractTest"),
        tasks.named("checkstyleRealAllocatorTest"),
    )
}

tasks.register<JavaExec>("validateRealAllocatorV3Checkpoint") {
    group = "verification"
    description = "Offline strict NACP3 checkpoint/resume validation; accesses no Oxia service."
    dependsOn(realAllocatorEvidenceArtifactJar)
    classpath = realAllocatorEvidenceRuntimeClasspath
    mainClass.set("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AllocatorProtocolMain")
    outputs.upToDateWhen { false }
    doFirst {
        fun required(property: String): String = providers.gradleProperty(property)
            .orNull
            ?: error("$property is required for V3 allocator checkpoint validation")
        setArgs(
            listOf(
                "validate-checkpoint",
                file(required("v2M3AllocatorV3CheckpointPath")).absolutePath,
                required("v2M3AllocatorV3NereusCommit"),
                required("v2M3AllocatorV3OxiaImageDigest"),
                required("v2M3AllocatorV3DependencyLockDigest"),
                required("v2M3AllocatorV3ExecutorDigest"),
                required("v2M3AllocatorV3WorkloadDigest"),
            ),
        )
    }
}

tasks.register<JavaExec>("sealRealAllocatorV3Evaluation") {
    group = "verification"
    description =
        "Seal one complete validator-reproved NACP3 as NAEV3; baseline-unavailable/NONE/BOTH stay non-promotable."
    dependsOn(realAllocatorEvidenceArtifactJar)
    classpath = realAllocatorEvidenceRuntimeClasspath
    mainClass.set("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AllocatorProtocolMain")
    outputs.upToDateWhen { false }
    doFirst {
        fun required(property: String): String = providers.gradleProperty(property)
            .orNull
            ?: error("$property is required for V3 allocator evaluation sealing")
        setArgs(
            listOf(
                "seal-evaluation",
                file(required("v2M3AllocatorV3CheckpointPath")).absolutePath,
                file(required("v2M3AllocatorV3EvaluationOutput")).absolutePath,
                required("v2M3AllocatorV3NereusCommit"),
                required("v2M3AllocatorV3OxiaImageDigest"),
                required("v2M3AllocatorV3DependencyLockDigest"),
                required("v2M3AllocatorV3ExecutorDigest"),
                required("v2M3AllocatorV3WorkloadDigest"),
            ),
        )
    }
}

fun JavaExec.configureAllocatorV3PromotionCommand(command: String, outputProperty: String) {
    group = "verification"
    dependsOn(realAllocatorEvidenceArtifactJar)
    classpath = realAllocatorEvidenceRuntimeClasspath
    mainClass.set("com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AllocatorProtocolMain")
    outputs.upToDateWhen { false }
    doFirst {
        fun required(property: String): String = providers.gradleProperty(property)
            .orNull
            ?: error("$property is required for the V3 allocator promotion/selection command")
        setArgs(
            listOf(
                command,
                file(required("v2M3AllocatorV3EvaluationPath")).absolutePath,
                file(required("v2M3AllocatorV3CheckpointPath")).absolutePath,
                file(required("v2M3AllocatorV3DiagnosticPath")).absolutePath,
                file(required("v2M3AllocatorV3DiagnosticJUnitPath")).absolutePath,
                file(required("v2M3AllocatorV3FormalJUnitPath")).absolutePath,
                file(required("v2M3AllocatorV3AttachmentDirectory")).absolutePath,
                file(required(outputProperty)).absolutePath,
                required("v2M3AllocatorV3NereusCommit"),
                required("v2M3AllocatorV3OxiaImageDigest"),
                required("v2M3AllocatorV3DependencyLockDigest"),
                required("v2M3AllocatorV3ExecutorDigest"),
                required("v2M3AllocatorV3WorkloadDigest"),
            ),
        )
    }
}

tasks.register<JavaExec>("realAllocatorV3PromotionCheck") {
    description = "Verify exact NAEV3/NACP3/NADV3/JUnit/attachment freshness and emit one promotion decision."
    configureAllocatorV3PromotionCommand("promotion-check", "v2M3AllocatorV3PromotionOutput")
}

tasks.register<JavaExec>("sealRealAllocatorV3Selection") {
    description = "Seal canonical NARS3 only after a unique V3 promotion decision."
    configureAllocatorV3PromotionCommand("seal-selection", "v2M3AllocatorV3SelectionOutput")
}

tasks.register("realAllocatorV3PreCampaignCheck") {
    group = "verification"
    description =
        "Run every offline ADR-0108 V3 feasibility, protocol, async-runner, and compatibility prerequisite."
    dependsOn(
        project(":nereus-domain").tasks.named("test"),
        project(":nereus-metadata-spi").tasks.named("test"),
        tasks.named("realAllocatorContractTest"),
        tasks.named("checkstyleRealAllocatorTest"),
    )
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
    description =
        "Run the diagnostic-only V3 async Cell handoff plus 10k RANGE-16 nine-cut batch against real Oxia."
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

tasks.register<Test>("realAllocatorStrictIntervalDiagnosticTest") {
    group = "verification"
    description = "Replay consecutive diagnostic-only formal 10k/1ms STRICT intervals against real Oxia."
    dependsOn(realAllocatorEvidenceArtifactJar)
    testClassesDirs = realAllocatorTest.output.classesDirs
    classpath = realAllocatorEvidenceRuntimeClasspath
    maxParallelForks = 1
    useJUnitPlatform()
    filter {
        includeTestsMatching(
            "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3RealAllocatorStrictIntervalDiagnosticTest",
        )
    }
    outputs.upToDateWhen { false }
    doFirst {
        fun required(property: String): String = providers.gradleProperty(property)
            .orNull
            ?: error("$property is required for the V3 STRICT interval diagnostic")
        val output = file(required("v2M3AllocatorV3StrictDiagnosticOutput")).toPath().toAbsolutePath().normalize()
        require(!Files.exists(output, LinkOption.NOFOLLOW_LINKS)) {
            "V3 STRICT interval diagnostic output already exists: $output"
        }
        val parent = output.parent
        require(parent != null && Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(parent)) {
            "V3 STRICT interval diagnostic output parent is absent or nonregular: $parent"
        }
        Files.createDirectory(output)
        systemProperty("nereus.m3.allocator.v3.oxiaServiceAddress", required("v2M3AllocatorOxiaServiceAddress"))
        systemProperty("nereus.m3.allocator.v3.nereusCommit", required("v2M3AllocatorV3NereusCommit"))
        systemProperty("nereus.m3.allocator.v3.diagnosticOutput", output.toString())
    }
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
