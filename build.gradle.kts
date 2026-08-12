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

import com.diffplug.gradle.spotless.SpotlessExtension
import org.gradle.api.DefaultTask
import org.gradle.api.artifacts.VersionCatalogsExtension
import org.gradle.api.plugins.quality.Checkstyle
import org.gradle.api.plugins.quality.CheckstyleExtension
import org.gradle.api.provider.Property
import org.gradle.api.services.BuildService
import org.gradle.api.services.BuildServiceParameters
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.TaskAction

abstract class DockerIntegrationGateService : BuildService<BuildServiceParameters.None>

abstract class PulsarCheckoutGateService : BuildService<BuildServiceParameters.None>

abstract class KafkaCheckoutGateService : BuildService<BuildServiceParameters.None>

abstract class DevelopmentCoordinateVerificationTask : DefaultTask() {
    @get:Input
    abstract val actualVersion: Property<String>

    @get:Input
    abstract val expectedVersion: Property<String>

    @TaskAction
    fun verifyCoordinate() {
        check(actualVersion.get() == expectedVersion.get()) {
            "Refusing to publish Kafka F9 development artifacts as ${actualVersion.get()}; " +
                    "expected ${expectedVersion.get()}. Add the calling gate to " +
                    "kafkaDevelopmentGateRequested."
        }
    }
}

abstract class ReleaseVersionVerificationTask : DefaultTask() {
    @get:Input
    abstract val actualVersion: Property<String>

    @get:Input
    abstract val expectedVersion: Property<String>

    @TaskAction
    fun verifyReleaseVersion() {
        val actual = actualVersion.get()
        val expected = expectedVersion.get()
        check(Regex("[0-9]+\\.[0-9]+\\.[0-9]+").matches(expected)) {
            "Release version must use stable X.Y.Z form, got $expected"
        }
        check(actual == expected) {
            "Release candidate version mismatch: expected $expected, got $actual"
        }
    }
}

abstract class V2FoundationDependencyVerificationTask : DefaultTask() {
    @get:org.gradle.api.tasks.Classpath
    abstract val domainCompileClasspath: org.gradle.api.file.ConfigurableFileCollection

    @get:org.gradle.api.tasks.Classpath
    abstract val metadataSpiCompileClasspath: org.gradle.api.file.ConfigurableFileCollection

    @get:org.gradle.api.tasks.Classpath
    abstract val allowedDomainArtifacts: org.gradle.api.file.ConfigurableFileCollection

    @TaskAction
    fun verifyDependencies() {
        val domainFiles = domainCompileClasspath.files.mapTo(sortedSetOf()) { it.canonicalFile }
        check(domainFiles.isEmpty()) {
            "nereus-domain production classpath must be empty, found $domainFiles"
        }

        val metadataSpiFiles = metadataSpiCompileClasspath.files.mapTo(sortedSetOf()) { it.canonicalFile }
        val allowedFiles = allowedDomainArtifacts.files.mapTo(sortedSetOf()) { it.canonicalFile }
        check(metadataSpiFiles.size == 1 && allowedFiles.containsAll(metadataSpiFiles)) {
            "nereus-metadata-spi production classpath must contain only :nereus-domain, found $metadataSpiFiles"
        }
    }
}

abstract class V2FoundationArtifactVerificationTask : DefaultTask() {
    @get:org.gradle.api.tasks.InputFile
    @get:org.gradle.api.tasks.PathSensitive(org.gradle.api.tasks.PathSensitivity.NONE)
    abstract val domainJar: org.gradle.api.file.RegularFileProperty

    @get:org.gradle.api.tasks.InputFile
    @get:org.gradle.api.tasks.PathSensitive(org.gradle.api.tasks.PathSensitivity.NONE)
    abstract val domainSourcesJar: org.gradle.api.file.RegularFileProperty

    @get:org.gradle.api.tasks.InputFile
    @get:org.gradle.api.tasks.PathSensitive(org.gradle.api.tasks.PathSensitivity.NONE)
    abstract val domainPom: org.gradle.api.file.RegularFileProperty

    @get:org.gradle.api.tasks.InputFile
    @get:org.gradle.api.tasks.PathSensitive(org.gradle.api.tasks.PathSensitivity.NONE)
    abstract val metadataSpiJar: org.gradle.api.file.RegularFileProperty

    @get:org.gradle.api.tasks.InputFile
    @get:org.gradle.api.tasks.PathSensitive(org.gradle.api.tasks.PathSensitivity.NONE)
    abstract val metadataSpiSourcesJar: org.gradle.api.file.RegularFileProperty

    @get:org.gradle.api.tasks.InputFile
    @get:org.gradle.api.tasks.PathSensitive(org.gradle.api.tasks.PathSensitivity.NONE)
    abstract val metadataSpiPom: org.gradle.api.file.RegularFileProperty

    @get:org.gradle.api.tasks.OutputFile
    abstract val hashReport: org.gradle.api.file.RegularFileProperty

    @TaskAction
    fun verifyArtifacts() {
        val artifacts = linkedMapOf(
            "nereus-domain.jar" to domainJar.get().asFile,
            "nereus-domain.sources.jar" to domainSourcesJar.get().asFile,
            "nereus-domain.pom" to domainPom.get().asFile,
            "nereus-metadata-spi.jar" to metadataSpiJar.get().asFile,
            "nereus-metadata-spi.sources.jar" to metadataSpiSourcesJar.get().asFile,
            "nereus-metadata-spi.pom" to metadataSpiPom.get().asFile,
        )
        artifacts.forEach { (label, file) ->
            check(file.isFile && file.length() > 0) { "$label is missing or empty: $file" }
        }

        val domainPomText = domainPom.get().asFile.readText()
        check("<dependency>" !in domainPomText) {
            "nereus-domain POM must publish no production dependencies"
        }
        val metadataSpiPomText = metadataSpiPom.get().asFile.readText()
        check(Regex("<dependency>").findAll(metadataSpiPomText).count() == 1) {
            "nereus-metadata-spi POM must publish exactly one dependency"
        }
        check("<artifactId>nereus-domain</artifactId>" in metadataSpiPomText) {
            "nereus-metadata-spi POM does not depend on nereus-domain"
        }

        fun sha256(file: File): String {
            val digest = java.security.MessageDigest.getInstance("SHA-256")
            file.inputStream().use { input ->
                val buffer = ByteArray(8192)
                while (true) {
                    val read = input.read(buffer)
                    if (read < 0) {
                        break
                    }
                    digest.update(buffer, 0, read)
                }
            }
            return java.util.HexFormat.of().formatHex(digest.digest())
        }

        val report = hashReport.get().asFile
        report.parentFile.mkdirs()
        report.writeText(artifacts.entries.joinToString(separator = "\n", postfix = "\n") { (label, file) ->
            "${sha256(file)}  $label"
        })
    }
}

abstract class V2OxiaDependencyVerificationTask : DefaultTask() {
    @get:org.gradle.api.tasks.Classpath
    abstract val runtimeClasspath: org.gradle.api.file.ConfigurableFileCollection

    @get:org.gradle.api.tasks.InputFiles
    @get:org.gradle.api.tasks.PathSensitive(org.gradle.api.tasks.PathSensitivity.NONE)
    abstract val lockedClientArtifacts: org.gradle.api.file.ConfigurableFileCollection

    @TaskAction
    fun verifyDependencies() {
        val selected = runtimeClasspath.files
            .filter { it.name == "oxia-client-0.9.4.jar" || it.name == "oxia-client-api-0.9.4.jar" }
            .mapTo(sortedSetOf()) { it.canonicalFile }
        val locked = lockedClientArtifacts.files.mapTo(sortedSetOf()) { it.canonicalFile }
        check(selected == locked && selected.size == 2) {
            "nereus-metadata-oxia must resolve exactly the locked O1 client/client-api pair; " +
                "selected=$selected locked=$locked"
        }
        val forbidden = runtimeClasspath.files.filter {
            it.name.startsWith("oxia-client-") && it.name.endsWith(".jar") && it.canonicalFile !in locked
        }
        check(forbidden.isEmpty()) {
            "nereus-metadata-oxia contains an unqualified or duplicate Oxia client artifact: $forbidden"
        }
    }
}

plugins {
    `base`
    `maven-publish`
    alias(libs.plugins.spotless) apply false
}

group = providers.gradleProperty("nereusGroup").get()
val configuredNereusVersion = providers.gradleProperty("nereusVersion").get()
val supportedNereusVersion =
    Regex(
        "([0-9]+\\.[0-9]+\\.[0-9]+)" +
            "(?:-SNAPSHOT|-f2-dev|-f9-dev|-n1\\.[0-9a-f]{40}|-p1\\.[0-9a-f]{40})?",
    )
val configuredVersionMatch = requireNotNull(supportedNereusVersion.matchEntire(configuredNereusVersion)) {
    "nereusVersion must be X.Y.Z, X.Y.Z-SNAPSHOT, X.Y.Z-f2-dev, X.Y.Z-f9-dev, " +
        "or source-qualified X.Y.Z-n1.<40-lowercase-hex>/X.Y.Z-p1.<40-lowercase-hex>"
}
if ("-p1." in configuredNereusVersion) {
    val allowedP1ArtifactTasks = setOf("clean", "p1ArtifactJar", "p1ArtifactSourcesJar")
    check(gradle.startParameter.taskNames.isNotEmpty()
            && gradle.startParameter.taskNames.all { it.substringAfterLast(':') in allowedP1ArtifactTasks }) {
        "The source-qualified P1 coordinate is restricted to the filtered metadata capability artifact tasks"
    }
}
val releaseLineVersion = configuredVersionMatch.groupValues[1]
val phase2DevelopmentVersion = "$releaseLineVersion-f2-dev"
val phase9DevelopmentVersion = "$releaseLineVersion-f9-dev"
val pulsarDevelopmentGateRequested = gradle.startParameter.taskNames.any { requested ->
    requested.substringAfterLast(':').startsWith("phase2")
            || requested.substringAfterLast(':').startsWith("phase3")
            // M1 consumes the final-gated F3 source composite. The Pulsar fork remains on the
            // frozen F2 development coordinate until the F4 broker rollout milestone changes both repos.
            || requested.substringAfterLast(':').startsWith("phase4")
            || requested.substringAfterLast(':').startsWith("bookKeeperPrimaryWal")
            || requested.substringAfterLast(':') == "publishPhase2DevelopmentArtifacts"
}
val kafkaDevelopmentGateRequested = gradle.startParameter.taskNames.any { requested ->
    val task = requested.substringAfterLast(':')
    task.startsWith("phase9M3")
            || task.startsWith("phase9M6Kafka")
            || task == "phase9M5KafkaCompactionOracleCheck"
            || task == "phase9M5KafkaRetentionOracleCheck"
            || task == "phase9ChaosCheck"
            || task == "phase9CompatibilityCheck"
            || task == "phase9PerformanceCheck"
            || task == "phase9M4FinalCheck"
            || task == "phase9M5FinalCheck"
            || task == "phase9M6FinalCheck"
            || task == "phase9M7Check"
            || task == "phase9M7FinalCheck"
            || task == "phase9PrepareFinalEvidence"
            || task == "phase9FinalEvidenceReport"
            || task == "phase9FinalCheck"
            || task == "phase9KafkaForkCompatibilityCheck"
            || task == "f9EvidenceAggregatorTest"
            || task == "f9M6KafkaProcessIntegrationTest"
            || task == "f9CheckpointTrimRecoveryProcessIntegrationTest"
            || task == "f9DeleteRecordsBoundaryProcessIntegrationTest"
            || task == "f9TrimResponseLossProcessIntegrationTest"
            || task == "f9TrimProfileMatrixProcessIntegrationTest"
            || task == "f9MultiBrokerTakeoverProviderIntegrationTest"
            || task == "f9MultiBrokerTakeoverProcessIntegrationTest"
            || task == "f9CoordinatorMigrationProcessIntegrationTest"
            || task == "f9OngoingTransactionMigrationProcessIntegrationTest"
            || task == "f9TransactionResolutionCutProcessIntegrationTest"
            || task == "f9TransactionResolutionProfileMatrixProcessIntegrationTest"
            || task == "f9MandatoryInternalTopicNtc2ProcessIntegrationTest"
            || task == "f9MandatoryInternalTopicNtc2ProfileMatrixProcessIntegrationTest"
            || task == "f9MultiControllerFailoverProcessIntegrationTest"
            || task == "f9ActivationCutFailoverProcessIntegrationTest"
            || task == "f9ActivationProofCutFailoverProcessIntegrationTest"
            || task == "f9ActivationTransportRecoveryProcessIntegrationTest"
            || task == "f9InFlightTakeoverProcessIntegrationTest"
            || task == "f9BookKeeperProfileTakeoverProcessIntegrationTest"
            || task == "f9BookKeeperInFlightTakeoverProcessIntegrationTest"
            || task == "f9BookKeeperWalOnlyProcessIntegrationTest"
            || task == "f9BookKeeperWalAsyncObjectProcessIntegrationTest"
            || task == "f9BookKeeperWalSyncObjectProcessIntegrationTest"
            || task == "f9ObjectWalAsyncObjectProcessIntegrationTest"
            || task == "f9LeaderChurnChaosProcessIntegrationTest"
            || task == "f9ClientCompatibilityProcessIntegrationTest"
            || task == "f9PerformanceProfileProcessIntegrationTest"
            || task == "publishPhase9DevelopmentArtifacts"
}
check(!(pulsarDevelopmentGateRequested && kafkaDevelopmentGateRequested)) {
    "Pulsar F2 and Kafka F9 development artifact gates require separate Gradle invocations"
}
version = gradle.startParameter.projectProperties["nereusVersion"]
    ?: if (pulsarDevelopmentGateRequested) {
        phase2DevelopmentVersion
    } else if (kafkaDevelopmentGateRequested) {
        phase9DevelopmentVersion
    } else {
        configuredNereusVersion
    }
if (pulsarDevelopmentGateRequested) {
    check(version.toString() == phase2DevelopmentVersion) {
        "Phase 2 development gates require version $phase2DevelopmentVersion, got $version"
    }
}
if (kafkaDevelopmentGateRequested) {
    check(version.toString() == phase9DevelopmentVersion) {
        "Kafka F9 development gates require version $phase9DevelopmentVersion, got $version"
    }
}

tasks.register<ReleaseVersionVerificationTask>("verifyReleaseVersion") {
    group = "verification"
    description = "Verify that the configured project version exactly matches a stable release version."
    actualVersion.set(version.toString())
    expectedVersion.set(providers.gradleProperty("releaseVersion"))
}

val javaLanguageVersion = providers.gradleProperty("javaVersion").map(String::toInt).getOrElse(21)
val checkstyleToolVersion = libs.versions.checkstyle.get()
val palantirJavaFormatVersion = extensions.getByType<VersionCatalogsExtension>()
    .named("libs")
    .findVersion("palantir-java-format")
    .orElseThrow()
    .requiredVersion
val dockerIntegrationGate = gradle.sharedServices.registerIfAbsent(
    "nereusDockerIntegrationGate",
    DockerIntegrationGateService::class,
) {
    maxParallelUsages.set(1)
}
val pulsarCheckoutGate = gradle.sharedServices.registerIfAbsent(
    "nereusPulsarCheckoutGate",
    PulsarCheckoutGateService::class,
) {
    maxParallelUsages.set(1)
}
val kafkaCheckoutGate = gradle.sharedServices.registerIfAbsent(
    "nereusKafkaCheckoutGate",
    KafkaCheckoutGateService::class,
) {
    maxParallelUsages.set(1)
}
val dockerBackedSubprojectTasks = mapOf(
    ":nereus-core" to setOf("phase1IntegrationTest"),
    ":nereus-managed-ledger" to setOf("cursorS3IntegrationTest", "cursorM2IntegrationTest"),
    ":nereus-materialization" to setOf("f4M2IntegrationTest", "f4M3IntegrationTest"),
    ":nereus-metadata-oxia" to setOf(
        "oxiaCapabilitySpike",
        "oxiaIntegrationTest",
        "p1OxiaIntegrationTest",
        "r1OxiaIntegrationTest",
        "f4OxiaIntegrationTest",
        "f9ActivationOxiaIntegrationTest",
        "f9OxiaIntegrationTest",
        "f9BindingScaleOxiaIntegrationTest",
    ),
    ":nereus-object-store" to setOf(
        "s3IntegrationTest",
        "rangedFormatS3IntegrationTest",
        "kafkaCheckpointS3IntegrationTest",
    ),
    ":nereus-kafka-adapter" to setOf(
        "f9M3ProviderIntegrationTest",
        "f9MultiBrokerTakeoverProviderIntegrationTest",
        "f9BookKeeperWalOnlyProviderIntegrationTest",
        "f9BookKeeperLedgerDeletionProviderIntegrationTest",
        "f9M6KafkaProcessIntegrationTest",
        "f9CheckpointTrimRecoveryProcessIntegrationTest",
        "f9DeleteRecordsBoundaryProcessIntegrationTest",
        "f9TrimResponseLossProcessIntegrationTest",
        "f9TrimProfileMatrixProcessIntegrationTest",
        "f9MultiBrokerTakeoverProcessIntegrationTest",
        "f9CoordinatorMigrationProcessIntegrationTest",
        "f9OngoingTransactionMigrationProcessIntegrationTest",
        "f9TransactionResolutionCutProcessIntegrationTest",
        "f9TransactionResolutionProfileMatrixProcessIntegrationTest",
        "f9MandatoryInternalTopicNtc2ProcessIntegrationTest",
        "f9MandatoryInternalTopicNtc2ProfileMatrixProcessIntegrationTest",
        "f9MultiControllerFailoverProcessIntegrationTest",
        "f9ActivationCutFailoverProcessIntegrationTest",
        "f9ActivationProofCutFailoverProcessIntegrationTest",
        "f9ActivationTransportRecoveryProcessIntegrationTest",
        "f9InFlightTakeoverProcessIntegrationTest",
        "f9BookKeeperProfileTakeoverProcessIntegrationTest",
        "f9BookKeeperInFlightTakeoverProcessIntegrationTest",
        "f9BookKeeperWalOnlyProcessIntegrationTest",
        "f9BookKeeperWalAsyncObjectProcessIntegrationTest",
        "f9BookKeeperWalSyncObjectProcessIntegrationTest",
        "f9ObjectWalAsyncObjectProcessIntegrationTest",
        "f9LeaderChurnChaosProcessIntegrationTest",
    ),
    ":nereus-pulsar-adapter" to setOf(
        "f4M4IntegrationTest",
        "bkM2IntegrationTest",
        "bkM3IntegrationTest",
        "bkM4IntegrationTest",
    ),
)
val dockerBackedPulsarExecTasks = setOf(
    "phase2PulsarFinalCheck",
    "phase3M5PulsarFinalCheck",
    "phase3M6PulsarFinalCheck",
    "phase4M4PhysicalGcMultiBrokerPulsarCheck",
    "phase4M5AsyncRetentionMultiBrokerPulsarCheck",
    "phase4M6TwoBrokerWorkerContentionPulsarCheck",
    "bookKeeperPrimaryWalM2PulsarCheck",
    "checkBookKeeperPrimaryWalM5AdminRoutingContractSurface",
    "bookKeeperPrimaryWalM5AdminRoutingCheck",
    "bookKeeperPrimaryWalM5TwoBrokerCheck",
    "bookKeeperPrimaryWalM5Check",
)
val pulsarCheckoutExecTasks = setOf(
    "v2M1P1FocusedSourceCheck",
    "phase2PulsarCheck",
    "phase2PulsarFinalCheck",
    "phase3M4PulsarCheck",
    "phase3M5PulsarFinalCheck",
    "phase3M6PulsarFinalCheck",
    "phase4M4PhysicalGcConfigPulsarCheck",
    "phase4M4PhysicalDeletionActivationPulsarCheck",
    "phase4M4ReadinessRolloverPulsarCheck",
    "phase4M4PhysicalGcMultiBrokerPulsarCheck",
    "phase4M5GenerationCapabilityPulsarCheck",
    "phase4M5RegistrationBackfillPulsarCheck",
    "phase4M5ActivationGuardPulsarCheck",
    "phase4M5PublicationActivationPulsarCheck",
    "phase4M5RetentionRuntimePulsarCheck",
    "phase4M5RetentionPolicyAdminPulsarCheck",
    "phase4M5AsyncRetentionMultiBrokerPulsarCheck",
    "phase4M6TwoBrokerWorkerContentionPulsarCheck",
    "bookKeeperPrimaryWalM2PulsarCheck",
    "bookKeeperPrimaryWalM5CapabilityCheck",
    "bookKeeperPrimaryWalM5BorrowedClientCheck",
    "bookKeeperPrimaryWalM5DeletionActivationCheck",
    "bookKeeperPrimaryWalM5AdminRoutingCheck",
    "bookKeeperPrimaryWalM5TwoBrokerCheck",
    "bookKeeperPrimaryWalM5Check",
)

// A nested Pulsar Gradle invocation can saturate the same local CPU, ports, and Docker
// forwarding path used while a Nereus Testcontainers fixture is establishing its first
// Oxia/BookKeeper connection. Keep every checkout invocation behind the real-service gate,
// even when the particular Pulsar test does not itself start a container.
tasks.matching {
    it.name in dockerBackedPulsarExecTasks || it.name in pulsarCheckoutExecTasks
}.configureEach {
    usesService(dockerIntegrationGate)
}

tasks.matching { it.name in pulsarCheckoutExecTasks }.configureEach {
    usesService(pulsarCheckoutGate)
}

subprojects {
    group = rootProject.group
    version = rootProject.version
    dockerBackedSubprojectTasks[path]?.let { taskNames ->
        tasks.matching { it.name in taskNames }.configureEach {
            usesService(dockerIntegrationGate)
        }
    }
}

configure(subprojects.filter { it.name != "nereus-bom" }) {
    apply(plugin = "checkstyle")
    apply(plugin = "java-library")
    apply(plugin = "maven-publish")
    apply(plugin = "com.diffplug.spotless")

    extensions.configure<CheckstyleExtension>("checkstyle") {
        toolVersion = checkstyleToolVersion
        configFile = rootProject.file("buildtools/src/main/resources/nereus/checkstyle.xml")
        configProperties["checkstyle.suppressions.file"] =
            rootProject.file("buildtools/src/main/resources/nereus/suppressions.xml").absolutePath
        maxErrors = 0
        maxWarnings = 0
    }

    tasks.withType<Checkstyle>().configureEach {
        maxHeapSize.set("1g")
        exclude("**/generated/**")
        exclude("**/generated-sources/**")
        exclude("**/generated-test-sources/**")
    }

    extensions.configure<SpotlessExtension>("spotless") {
        java {
            target("src/**/*.java")
            palantirJavaFormat(palantirJavaFormatVersion)
            importOrder("\\#|")
            removeUnusedImports()
            trimTrailingWhitespace()
            endWithNewline()
        }
    }

    extensions.configure<JavaPluginExtension>("java") {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(javaLanguageVersion))
        }
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
        withSourcesJar()
    }

    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
        options.release.set(if (name == "compileJava") 17 else javaLanguageVersion)
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }

    extensions.configure<PublishingExtension>("publishing") {
        repositories {
            maven {
                name = "development"
                url = rootProject.layout.buildDirectory.dir("development-repository").get().asFile.toURI()
            }
        }
        publications {
            create<MavenPublication>("mavenJava") {
                from(components["java"])
                pom {
                    name.set(project.name)
                    description.set("Nereus module ${project.name}")
                    url.set("https://nereusstream.com")
                    licenses {
                        license {
                            name.set("Apache License, Version 2.0")
                            url.set("https://www.apache.org/licenses/LICENSE-2.0")
                        }
                    }
                    scm {
                        url.set("https://github.com/nereusstream/nereus")
                        connection.set("scm:git:https://github.com/nereusstream/nereus.git")
                    }
                }
            }
        }
    }
}

tasks.register("quickCheck") {
    group = "verification"
    description = "Fast scaffold check for Nereus."
    dependsOn("checkPhase0")
}

tasks.register<Exec>("checkPhase0") {
    group = "verification"
    description = "Verify the Phase 0 repository scaffold."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase0.sh")
}

val phase1L0Modules = listOf(
    ":nereus-api",
    ":nereus-core",
    ":nereus-metadata-oxia",
    ":nereus-object-store",
)

tasks.register<Exec>("checkPhase1L0Dependencies") {
    group = "verification"
    description = "Verify Phase 1 L0 modules stay protocol-neutral."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase1-l0-dependencies.sh")
}

tasks.register<Exec>("checkPhase1Namespace") {
    group = "verification"
    description = "Verify Java packages and Maven coordinates use the owned nereusstream.com namespace."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase1-namespace.sh")
}

tasks.register("phase1Check") {
    group = "verification"
    description = "Verify the Phase 1 Core StreamStorage scaffold."
    dependsOn("checkPhase0")
    dependsOn("checkPhase1L0Dependencies")
    dependsOn("checkPhase1Namespace")
    dependsOn(phase1L0Modules.map { "$it:test" })
    dependsOn(":nereus-metadata-oxia:compileOxiaCapabilitySpikeJava")
}

tasks.register("phase1FinalCheck") {
    group = "verification"
    description = "Run every ordinary and Docker-backed Phase 1 release gate."
    dependsOn("phase1Check")
    dependsOn(":nereus-metadata-oxia:oxiaCapabilitySpike")
    dependsOn(":nereus-metadata-oxia:oxiaIntegrationTest")
    dependsOn(":nereus-core:phase1IntegrationTest")
}

tasks.register("phase15Check") {
    group = "verification"
    description = "Verify the Phase 1.5 generic target, recovery, lifecycle, and compatibility foundation."
    dependsOn("phase1Check")
    dependsOn(phase1L0Modules.map { "$it:test" })
}

tasks.register("phase15FinalCheck") {
    group = "verification"
    description = "Run the production Oxia/Object WAL Phase 1.5 mixed-version and lifecycle gates."
    dependsOn("phase15Check")
    dependsOn("phase1FinalCheck")
}

val pulsarCheckoutPath = providers.gradleProperty("pulsarCheckout")
    .orElse(providers.environmentVariable("NEREUS_PULSAR_CHECKOUT"))
    .orElse(layout.projectDirectory.dir("../../nereusstream/pulsar").asFile.absolutePath)
val pulsarExpectedHead = providers.gradleProperty("pulsarExpectedHead")
    .orElse("2f9c1eb93be96e2036fbdc8c5e39545f21fa6200")

tasks.register<Exec>("checkPulsarSourceLock") {
    group = "verification"
    description = "Verify the exact clean Pulsar fork checkout used by Phase 2."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-pulsar-source-lock.sh",
        pulsarCheckoutPath.get(),
        pulsarExpectedHead.get(),
    )
}

tasks.register<Exec>("checkPhase2StorageIsolation") {
    group = "verification"
    description = "Verify virtual-ledger routing cannot enter BookKeeper APIs and production has no local object path."
    dependsOn("checkPulsarSourceLock")
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase2-storage-isolation.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register("phase2M1Check") {
    group = "verification"
    description = "Verify the F2-M1 projection, Position, entry codec, and L0 request foundation."
    dependsOn("phase15Check")
    dependsOn("checkPulsarSourceLock")
    dependsOn(":nereus-metadata-oxia:test")
    dependsOn(":nereus-managed-ledger:test")
}

tasks.register("phase2M2Check") {
    group = "verification"
    description = "Verify the F2-M2 projection metadata model, codec, CAS, repair, and shared-runtime contracts."
    dependsOn("phase2M1Check")
    dependsOn(":nereus-metadata-oxia:test")
    dependsOn(":nereus-metadata-oxia:compileOxiaIntegrationTestJava")
}

tasks.register("phase2M2FinalCheck") {
    group = "verification"
    description = "Run the ordinary and Docker-backed real Oxia F2-M2 projection metadata gates."
    dependsOn("phase2M2Check")
    dependsOn(":nereus-metadata-oxia:oxiaIntegrationTest")
}

val phase2PublishedModules = listOf(
    ":nereus-api",
    ":nereus-core",
    ":nereus-metadata-oxia",
    ":nereus-object-store",
    ":nereus-managed-ledger",
    ":nereus-materialization",
    ":nereus-bookkeeper",
    ":nereus-pulsar-adapter",
)

tasks.register("publishPhase2DevelopmentArtifacts") {
    group = "verification"
    description = "Publish the exact Nereus F2 development coordinate for the Pulsar fork gate."
    dependsOn(phase2PublishedModules.map { "$it:publishAllPublicationsToDevelopmentRepository" })
}

val phase2DevelopmentRepository = layout.buildDirectory.dir("development-repository")
val pulsarGradleWrapper = file(pulsarCheckoutPath.get()).resolve("gradlew").absolutePath

tasks.register<Exec>("phase2PulsarCheck") {
    group = "verification"
    description = "Run ordinary Pulsar fork Nereus tests, stock persistence regressions, and checkstyle."
    dependsOn("checkPulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    // phase3FinalCheck runs with org.gradle.parallel=true and --rerun-tasks. Keep the first
    // nested Pulsar build behind every local task that consumes the included Pulsar outputs.
    mustRunAfter("phase3M3Check", "phase3M2FinalCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.*",
        "--tests", "org.apache.pulsar.broker.service.persistent.PersistentTopicNereusAdmissionTest",
        "--tests", "org.apache.pulsar.broker.admin.TopicPoliciesTest.testPersistencePolicyRejectsMissingTopic",
        "--tests", "org.apache.pulsar.broker.admin.TopicPoliciesTest.testGetPersistenceApplied",
        "--tests", "org.apache.pulsar.broker.admin.TopicPoliciesTest.testSetPersistence",
        "--tests", "org.apache.pulsar.broker.admin.TopicPoliciesTest.testRemovePersistence",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky,broker-isolated",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase2Check") {
    group = "verification"
    description = "Run every ordinary Nereus F2 product and Pulsar fork gate."
    dependsOn("phase2M2Check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-pulsar-adapter:check")
    dependsOn("checkPhase2StorageIsolation")
    dependsOn("phase2PulsarCheck")
}

tasks.register<Exec>("phase2PulsarFinalCheck") {
    group = "verification"
    description = "Run the real two-broker Oxia/LocalStack/BookKeeper Nereus recovery gate."
    dependsOn("checkPulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase2PulsarCheck", "phase3M6Check")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusMultiBrokerIntegrationTest",
        "--rerun-tasks",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase2FinalCheck") {
    group = "verification"
    description = "Run every ordinary and Docker-backed Nereus F2 release gate."
    dependsOn("phase2Check")
    dependsOn("phase15FinalCheck")
    dependsOn("phase2M2FinalCheck")
    dependsOn(":nereus-object-store:s3IntegrationTest")
    dependsOn("phase2PulsarFinalCheck")
}

tasks.register("phase3M1Check") {
    group = "verification"
    description = "Verify the F3-M1 cursor metadata, activation marker, ack domain, and snapshot foundation."
    dependsOn("phase2M2Check")
    dependsOn("checkPulsarSourceLock")
    dependsOn(":nereus-metadata-oxia:test")
    dependsOn(":nereus-metadata-oxia:compileOxiaIntegrationTestJava")
    dependsOn(":nereus-managed-ledger:test")
    dependsOn(":nereus-object-store:test")
}

tasks.register("phase3M1FinalCheck") {
    group = "verification"
    description = "Run ordinary and Docker-backed real Oxia/ObjectStore F3-M1 foundation gates."
    dependsOn("phase3M1Check")
    dependsOn(":nereus-metadata-oxia:oxiaIntegrationTest")
    dependsOn(":nereus-managed-ledger:cursorS3IntegrationTest")
    dependsOn(":nereus-object-store:s3IntegrationTest")
}

tasks.register("phase3M2Check") {
    group = "verification"
    description = "Verify the F3-M2 cursor storage, retention, recovery, and failure-injection state machines."
    dependsOn("phase3M1Check")
    dependsOn(":nereus-managed-ledger:test")
    dependsOn(":nereus-managed-ledger:compileCursorM2IntegrationTestJava")
}

tasks.register("phase3M2FinalCheck") {
    group = "verification"
    description = "Run ordinary and Docker-backed real Oxia/ObjectStore F3-M2 recovery gates."
    dependsOn("phase3M2Check")
    dependsOn("phase3M1FinalCheck")
    dependsOn(":nereus-managed-ledger:cursorM2IntegrationTest")
}

tasks.register("phase3M3Check") {
    group = "verification"
    description = "Verify the F3-M3 durable cursor facade, lifecycle, reads, callbacks, and locked Pulsar API compile."
    dependsOn("phase3M2Check")
    dependsOn("checkPulsarSourceLock")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("phase3M4PulsarCheck") {
    group = "verification"
    description = "Run the exact F3-M4 Pulsar broker capability, admission, recovery, ack, and admin suites."
    dependsOn("checkPulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    dependsOn("phase3M3Check")
    mustRunAfter("phase2PulsarCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker-common:spotlessJavaCheck",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusTopicFeatureValidatorTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusAcknowledgeValidatorTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusCursorProtocolCapabilityTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusBrokerStorageConfigurationCursorTest",
        "--tests", "org.apache.pulsar.broker.service.persistent.NereusPersistentTopicCursorRecoveryTest",
        "--tests", "org.apache.pulsar.broker.service.persistent.NereusPersistentSubscriptionAckTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusConsumerAckOrderingTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusAdminOperationTest",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky,broker-isolated",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase3M4Check") {
    group = "verification"
    description = "Verify the complete F3-M4 Pulsar broker integration against the exact local fork."
    dependsOn("phase3M3Check")
    dependsOn("phase3M4PulsarCheck")
}

tasks.register("phase3M5Check") {
    group = "verification"
    description = "Verify F3-M5 deterministic recovery cuts and the exact 10,000-cursor scale boundary."
    dependsOn("phase3M4Check")
    dependsOn(":nereus-managed-ledger:test")
}

tasks.register<Exec>("checkPhase3PulsarAdminRoutes") {
    group = "verification"
    description = "Audit every loaded, unloaded, and namespace Nereus admin route in the locked Pulsar fork."
    dependsOn("checkPulsarSourceLock")
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase3-pulsar-admin-routes.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("checkPhase3ContractSurface") {
    group = "verification"
    description = "Audit the code-level Phase 3 production/test inventory and completion invariants."
    dependsOn("checkPulsarSourceLock")
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase3-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("v2DocumentationCheck") {
    group = "verification"
    description = "Verify the V2 authority, profiles, source locks, scenarios, tradeoffs, receipts, and links."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-documentation.sh")
}

tasks.register("v2M0Check") {
    group = "verification"
    description = "Verify the documentation-only V2 M0 baseline without claiming runtime implementation."
    dependsOn("v2DocumentationCheck")
}

val v2DomainCompileClasspath = project(":nereus-domain").configurations.named("compileClasspath")
val v2MetadataSpiCompileClasspath = project(":nereus-metadata-spi").configurations.named("compileClasspath")
val v2DomainMainOutput = project(":nereus-domain")
    .extensions
    .getByType<org.gradle.api.tasks.SourceSetContainer>()
    .named("main")
    .get()
    .output
val v2DomainMainSourceSet = project(":nereus-domain")
    .extensions
    .getByType<org.gradle.api.tasks.SourceSetContainer>()
    .named("main")
    .get()
val v2DomainTestSourceSet = project(":nereus-domain")
    .extensions
    .getByType<org.gradle.api.tasks.SourceSetContainer>()
    .named("test")
    .get()
val v2DomainJar = project(":nereus-domain").tasks.named<Jar>("jar").flatMap { it.archiveFile }
val v2DomainSourcesJar = project(":nereus-domain").tasks.named<Jar>("sourcesJar").flatMap { it.archiveFile }
val v2MetadataSpiJar = project(":nereus-metadata-spi").tasks.named<Jar>("jar").flatMap { it.archiveFile }
val v2MetadataSpiSourcesJar = project(":nereus-metadata-spi").tasks.named<Jar>("sourcesJar").flatMap { it.archiveFile }

tasks.register<V2FoundationDependencyVerificationTask>("v2M1FoundationDependencyCheck") {
    group = "verification"
    description = "Verify the M1.1a-A domain/SPI production dependency boundary."
    dependsOn(":nereus-domain:compileJava", ":nereus-metadata-spi:compileJava")
    domainCompileClasspath.from(v2DomainCompileClasspath)
    metadataSpiCompileClasspath.from(v2MetadataSpiCompileClasspath)
    allowedDomainArtifacts.from(v2DomainMainOutput)
    allowedDomainArtifacts.from(v2DomainJar)
}

tasks.register<Exec>("v2M1FoundationApiCheck") {
    group = "verification"
    description = "Verify the M1.1a-A import, SPI capability, and no-final-gate boundary after later domain slices."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m1-foundation.sh")
}

tasks.register<V2FoundationArtifactVerificationTask>("v2M1FoundationArtifactCheck") {
    group = "verification"
    description = "Build and SHA-256 qualify the M1.1a-A JAR, source JAR, and Maven POM artifacts."
    dependsOn(":nereus-domain:jar")
    dependsOn(":nereus-domain:sourcesJar")
    dependsOn(":nereus-domain:generatePomFileForMavenJavaPublication")
    dependsOn(":nereus-metadata-spi:jar")
    dependsOn(":nereus-metadata-spi:sourcesJar")
    dependsOn(":nereus-metadata-spi:generatePomFileForMavenJavaPublication")
    domainJar.set(v2DomainJar)
    domainSourcesJar.set(v2DomainSourcesJar)
    domainPom.set(project(":nereus-domain").layout.buildDirectory.file("publications/mavenJava/pom-default.xml"))
    metadataSpiJar.set(v2MetadataSpiJar)
    metadataSpiSourcesJar.set(v2MetadataSpiSourcesJar)
    metadataSpiPom.set(
        project(":nereus-metadata-spi").layout.buildDirectory.file("publications/mavenJava/pom-default.xml"),
    )
    hashReport.set(layout.buildDirectory.file("v2-m1-foundation/artifacts.sha256"))
}

tasks.register("v2M1FoundationCheck") {
    group = "verification"
    description = "Verify only the partial M1.1a-A domain and metadata SPI foundation; this is not M1 PASS."
    dependsOn(":nereus-domain:check")
    dependsOn(":nereus-metadata-spi:check")
    dependsOn("v2M1FoundationDependencyCheck")
    dependsOn("v2M1FoundationApiCheck")
    dependsOn("v2M1FoundationArtifactCheck")
    dependsOn("v2DocumentationCheck")
}

tasks.register<Exec>("v2M1N1ArtifactSourceCheck") {
    group = "verification"
    description = "Verify the immutable, source-qualified N1 domain/SPI bundle and non-promotable receipt."
    dependsOn("v2M1FoundationCheck")
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m1-n1-artifact.sh")
}

tasks.register("v2M1N1ArtifactCheck") {
    group = "verification"
    description = "Verify N1 as an exact K1/P1/R1 input only; no source-tuple promotion or M1 PASS."
    dependsOn("v2M1N1ArtifactSourceCheck")
    dependsOn("v2DocumentationCheck")
}

tasks.register<Exec>("v2M1P1ArtifactSourceCheck") {
    group = "verification"
    description = "Verify the immutable, source-qualified P1 metadata capability bundle and non-promotable receipt."
    dependsOn(":nereus-metadata-oxia:p1MetadataTest")
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m1-p1-artifact.sh")
}

tasks.register("v2M1P1ArtifactCheck") {
    group = "verification"
    description = "Verify the exact P1 adapter input only; no native capability, source-tuple promotion, or M1 PASS."
    dependsOn("v2M1N1ArtifactCheck")
    dependsOn("v2M1P1ArtifactSourceCheck")
    dependsOn("v2DocumentationCheck")
}

val v2OxiaLockedRoot = layout.projectDirectory.dir(
    "gradle/locked-artifacts/oxia-client-java/091a42c2780d92da56e9ec1f02ce1c3d988adc16/m2/" +
        "io/github/oxia-db",
)

tasks.register<V2OxiaDependencyVerificationTask>("v2M1OxiaDependencyCheck") {
    group = "verification"
    description = "Verify metadata-oxia resolves only the immutable O1 client/client-api bundle."
    dependsOn(":nereus-metadata-oxia:compileJava")
    runtimeClasspath.from(project(":nereus-metadata-oxia").configurations.named("runtimeClasspath"))
    lockedClientArtifacts.from(
        v2OxiaLockedRoot.file("oxia-client/0.9.4/oxia-client-0.9.4.jar"),
        v2OxiaLockedRoot.file("oxia-client-api/0.9.4/oxia-client-api-0.9.4.jar"),
    )
}

tasks.register<Exec>("v2M1OxiaScaffoldSourceCheck") {
    group = "verification"
    description = "Verify the local O2 source boundary and non-zero, zero-skip deterministic test report."
    dependsOn(":nereus-metadata-oxia:test")
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m1-oxia-scaffold.sh")
}

tasks.register("v2M1OxiaScaffoldCheck") {
    group = "verification"
    description = "Verify M1.1a-O2 local scaffold only; no P1/R1/runtime activation/scenario/M1 PASS."
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn("v2M1OxiaDependencyCheck")
    dependsOn("v2M1OxiaScaffoldSourceCheck")
    dependsOn("v2M1FoundationCheck")
    dependsOn("v2DocumentationCheck")
}

tasks.register<Exec>("v2M1Nta1ReadinessSourceCheck") {
    group = "verification"
    description = "Verify Q1 evidence boundaries and non-zero, zero-skip NTA1 readiness tests."
    dependsOn(":nereus-domain:test")
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m1-nta1-readiness.sh")
}

tasks.register("v2M1Nta1ReadinessCheck") {
    group = "verification"
    description = "Verify immutable M1.1b-Q1 readiness evidence; no Docker/runtime/scenario/M1 PASS."
    dependsOn(":nereus-domain:check")
    dependsOn("v2M1Nta1ReadinessSourceCheck")
    dependsOn("v2M1FoundationCheck")
    dependsOn("v2DocumentationCheck")
}

tasks.register<Exec>("v2M1RegistryCapacitySourceCheck") {
    group = "verification"
    description = "Verify deterministic R0 Registry capacity evidence, focused counts, and production absence."
    dependsOn(":nereus-domain:test")
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m1-registry-capacity.sh")
}

tasks.register<org.gradle.api.tasks.testing.Test>("v2M1ReceiptCapsFocusedTest") {
    group = "verification"
    description = "Run only the deterministic M1-2 receipt/parser cap boundary suite."
    dependsOn(":nereus-domain:testClasses")
    testClassesDirs = v2DomainTestSourceSet.output.classesDirs
    classpath = v2DomainTestSourceSet.runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.domain.receipt.ReceiptV1CapacityEvidenceTest")
    }
    workingDir = project(":nereus-domain").projectDir
    binaryResultsDirectory.set(layout.buildDirectory.dir("test-results/v2-m1-receipt-caps/binary"))
    reports.junitXml.outputLocation.set(layout.buildDirectory.dir("test-results/v2-m1-receipt-caps"))
    reports.html.outputLocation.set(layout.buildDirectory.dir("reports/tests/v2-m1-receipt-caps"))
    outputs.dir(project(":nereus-domain").layout.buildDirectory.dir("reports/v2-m1-receipt-caps"))
}

tasks.register<Exec>("v2M1ReceiptCapsSourceCheck") {
    group = "verification"
    description = "Verify M1-2 receipt cap evidence, focused counts, source binding, and production absence."
    dependsOn("v2M1ReceiptCapsFocusedTest")
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m1-receipt-caps.sh")
}

tasks.register("v2M1ReceiptCapsCheck") {
    group = "verification"
    description = "Verify M1-2 receipt/parser caps readiness only; no N1/N2/N3, scenario promotion, or M1 Final."
    dependsOn("v2M1ReceiptCapsSourceCheck")
    dependsOn("v2DocumentationCheck")
}

tasks.register("v2M1RegistryCapacityCheck") {
    group = "verification"
    description = "Verify M1.1c-R0 readiness only; no R1 authority/real Oxia/allocator/scenario/M1 PASS."
    dependsOn(":nereus-domain:check")
    dependsOn("v2M1RegistryCapacitySourceCheck")
    dependsOn("v2DocumentationCheck")
}

tasks.register<Exec>("v2M1Nta1CodecSourceCheck") {
    group = "verification"
    description = "Verify production NTA1 v1, exact goldens, O2 aggregate wiring, and non-promotion scope."
    dependsOn(":nereus-domain:test", ":nereus-metadata-oxia:test")
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m1-nta1-codec.sh")
}

tasks.register("v2M1Nta1CodecCheck") {
    group = "verification"
    description = "Verify M1.1b exact local codec only; no Docker/K1/P1/R1/runtime/scenario/M1 PASS."
    dependsOn(":nereus-domain:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn("v2M1Nta1CodecSourceCheck")
    dependsOn("v2M1Nta1ReadinessCheck")
    dependsOn("v2M1OxiaScaffoldCheck")
    dependsOn("v2DocumentationCheck")
}

tasks.named("check") {
    dependsOn("v2DocumentationCheck")
    dependsOn("v2M1FoundationDependencyCheck")
    dependsOn("v2M1FoundationApiCheck")
    dependsOn("v2M1N1ArtifactSourceCheck")
}

tasks.register<Exec>("checkPhase3Documentation") {
    group = "verification"
    description = "Verify V1 Phase 3 evidence carries its M6 contract, source lock, gates, and F4 handoff."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase3-documentation.sh")
}

tasks.register<Exec>("phase3M5PulsarFinalCheck") {
    group = "verification"
    description = "Run the real two-broker F3-M5 durable cursor recovery, expiry, and coexistence gate."
    dependsOn("checkPulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase3M4PulsarCheck", "phase3M2FinalCheck", "phase2PulsarFinalCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests",
        "org.apache.pulsar.broker.storage.nereus.NereusCursorMultiBrokerIntegrationTest.preservesDurableCursorTruthAcrossUnloadFailoverRestartExpiryAndBookKeeper",
        "--rerun-tasks",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase3M5FinalCheck") {
    group = "verification"
    description = "Run every ordinary and real-service F3-M5 cursor recovery and retention gate."
    dependsOn("phase3M5Check")
    dependsOn("phase3M2FinalCheck")
    dependsOn("phase3M5PulsarFinalCheck")
}

tasks.register("phase3M6Check") {
    group = "verification"
    description = "Verify F3-M6 compatibility, incarnation, rollout, F4 handoff, and admin-route boundaries."
    dependsOn("phase3M5Check")
    dependsOn("checkPhase2StorageIsolation")
    dependsOn("checkPhase3ContractSurface")
    dependsOn("checkPhase3Documentation")
    dependsOn("checkPhase3PulsarAdminRoutes")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("phase3M6PulsarFinalCheck") {
    group = "verification"
    description = "Run the real two-broker F3-M6 MessageId, property, admin-route, and incarnation gate."
    dependsOn("checkPulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase3M5PulsarFinalCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests",
        "org.apache.pulsar.broker.storage.nereus.NereusCursorMultiBrokerIntegrationTest.preservesMessageIdsPropertiesAndIncarnationAcrossCompatibilityCuts",
        "--rerun-tasks",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase3M6FinalCheck") {
    group = "verification"
    description = "Run every ordinary and real-service F3-M6 compatibility and F4-handoff gate."
    dependsOn("phase3M6Check")
    dependsOn("phase3M5FinalCheck")
    dependsOn("phase3M6PulsarFinalCheck")
}

tasks.register("phase3Check") {
    group = "verification"
    description = "Run every ordinary Phase 3 cursor/subscription product gate."
    dependsOn("phase3M6Check")
}

tasks.register("phase3FinalCheck") {
    group = "verification"
    description = "Run the complete Phase 1, 1.5, 2, and 3 release gate."
    dependsOn("phase3Check")
    dependsOn("phase2FinalCheck")
    dependsOn("phase3M6FinalCheck")
}

tasks.register<Exec>("checkPhase4ContractSurface") {
    group = "verification"
    description = "Audit the implemented F4-M1 production, test, transition, and golden surfaces."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-contract-surface.sh")
}

tasks.register<Exec>("checkPhase4Documentation") {
    group = "verification"
    description = "Verify legacy V1 F4 implementation evidence, source lock, gates, and documentation links."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-documentation.sh")
}

tasks.register<Exec>("bookKeeperPrimaryWalDocumentationCheck") {
    group = "verification"
    description = "Verify V1 F1-BK code-level evidence, source locks, milestone status, and documentation links."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-bookkeeper-primary-wal-documentation.sh")
}

tasks.register<Exec>("checkBookKeeperModuleBoundaries") {
    group = "verification"
    description = "Verify BookKeeper provider code stays outside L0 and ManagedLedger implementation types."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-bookkeeper-module-boundaries.sh")
}

tasks.register("bookKeeperPrimaryWalM1Check") {
    group = "verification"
    description = "Verify the provider-neutral read/append seam and BookKeeper 4.18 adapter foundation."
    dependsOn("bookKeeperPrimaryWalDocumentationCheck")
    dependsOn("checkBookKeeperModuleBoundaries")
    dependsOn(":nereus-api:test")
    dependsOn(":nereus-core:test")
    dependsOn(":nereus-metadata-oxia:test")
    dependsOn(":nereus-object-store:test")
    dependsOn(":nereus-materialization:test")
    dependsOn(":nereus-bookkeeper:test")
}

tasks.register("bookKeeperPrimaryWalM1FinalCheck") {
    group = "verification"
    description = "Run BK-M1 plus every final-gated Phase 1.5 and Phase 4 predecessor."
    dependsOn("bookKeeperPrimaryWalM1Check")
    dependsOn("phase15FinalCheck")
    dependsOn("phase4FinalCheck")
}

tasks.register("bookKeeperPrimaryWalM2MetadataCheck") {
    group = "verification"
    description = "Verify BK-M2 keyspace, durable codecs, exact Oxia/fake stores, and bounded shard scanners."
    dependsOn("bookKeeperPrimaryWalDocumentationCheck")
    dependsOn("checkBookKeeperModuleBoundaries")
    dependsOn(":nereus-metadata-oxia:test")
}

tasks.register("bookKeeperPrimaryWalM2RuntimeCheck") {
    group = "verification"
    description =
        "Verify the BK-M2 allocator, writer, recovery, physical-reference, lease, and exact reader runtime checkpoint."
    dependsOn("bookKeeperPrimaryWalM2RecoveryFencingCheck")
    dependsOn(":nereus-bookkeeper:test")
    dependsOn(":nereus-core:test")
}

tasks.register("bookKeeperPrimaryWalM2AllocatorCheck") {
    group = "verification"
    description = "Verify reserved-id allocation, immutable provider identity, and uncertain-create recovery."
    dependsOn("bookKeeperPrimaryWalM2MetadataCheck")
    dependsOn(":nereus-bookkeeper:bkM2AllocatorTest")
}

tasks.register("bookKeeperPrimaryWalM2AppendReadCheck") {
    group = "verification"
    description = "Verify exact BK append/read targets, buffer ownership, rollover, and L0 generation-zero composition."
    dependsOn("bookKeeperPrimaryWalM2AllocatorCheck")
    dependsOn(":nereus-bookkeeper:bkM2AppendReadTest")
    dependsOn(":nereus-core:test")
}

tasks.register("bookKeeperPrimaryWalM2RecoveryFencingCheck") {
    group = "verification"
    description =
        "Verify stale-session fencing, recovery-open sealing, restart ownership transfer, and inventory repair."
    dependsOn("bookKeeperPrimaryWalM2AppendReadCheck")
    dependsOn(":nereus-bookkeeper:bkM2RecoveryFencingTest")
}

tasks.register("bookKeeperPrimaryWalM2RetentionCheck") {
    group = "verification"
    description = "Verify bounded BK-M2 protection retirement and mark/drain/delete/dual-absence convergence."
    dependsOn("bookKeeperPrimaryWalM2RuntimeCheck")
    dependsOn(":nereus-bookkeeper:test")
    dependsOn(":nereus-metadata-oxia:test")
}

tasks.register<Exec>("bookKeeperPrimaryWalM2PulsarCheck") {
    group = "verification"
    description = "Verify the BK_ONLY ManagedLedger facade and borrowed stock BookKeeper client boundary."
    dependsOn("bookKeeperPrimaryWalM2RetentionCheck")
    dependsOn("checkPulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    dependsOn(":nereus-managed-ledger:test")
    dependsOn(":nereus-pulsar-adapter:test")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusManagedLedgerStorageBookKeeperClientTest",
        "--rerun-tasks",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("bookKeeperPrimaryWalM2RealServiceCheck") {
    group = "verification"
    description =
        "Run BK-M2 real Oxia/BookKeeper append recovery, restart, rollover, and delete-response-loss acceptance."
    dependsOn("bookKeeperPrimaryWalM2RetentionCheck")
    dependsOn(":nereus-pulsar-adapter:bkM2IntegrationTest")
}

tasks.register("bookKeeperPrimaryWalM2StableRecoveryCheck") {
    group = "verification"
    description = "Verify real Oxia BK_ONLY intent/head/gen0 response-loss convergence without another BK write."
    dependsOn("bookKeeperPrimaryWalM2RealServiceCheck")
}

tasks.register("bookKeeperPrimaryWalM2IsolationRetentionCheck") {
    group = "verification"
    description = "Verify real foreign-ledger isolation and the exact BK protection Cartesian retention bound."
    dependsOn("bookKeeperPrimaryWalM2StableRecoveryCheck")
    dependsOn(":nereus-pulsar-adapter:bkM2IntegrationTest")
}

tasks.register("bookKeeperPrimaryWalM2AllocationAuthorityCheck") {
    group = "verification"
    description = "Verify real Oxia mutation-response-loss reload and global BK ledger-id contention authority."
    dependsOn("bookKeeperPrimaryWalM2IsolationRetentionCheck")
    dependsOn(":nereus-pulsar-adapter:bkM2IntegrationTest")
}

tasks.register("bookKeeperPrimaryWalM2Check") {
    group = "verification"
    description = "Run the complete ordinary BK_ONLY metadata, runtime, retention, and Pulsar boundary gate."
    dependsOn("bookKeeperPrimaryWalM2PulsarCheck")
    dependsOn("checkBookKeeperModuleBoundaries")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-bookkeeper:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register("bookKeeperPrimaryWalM2FinalCheck") {
    group = "verification"
    description = "Run BK-M2 ordinary, real Oxia/BookKeeper, pinned Pulsar, and final-gated predecessor acceptance."
    dependsOn("bookKeeperPrimaryWalM2Check")
    dependsOn("bookKeeperPrimaryWalM2AllocationAuthorityCheck")
    dependsOn("bookKeeperPrimaryWalM1FinalCheck")
}

tasks.register("bookKeeperPrimaryWalM3ExactSourceCheck") {
    group = "verification"
    description = "Verify BK task V2 target round-trip and provider-neutral exact-source reads."
    dependsOn("bookKeeperPrimaryWalDocumentationCheck")
    dependsOn(":nereus-metadata-oxia:test")
    dependsOn(":nereus-materialization:test")
}

tasks.register("bookKeeperPrimaryWalM3ProtectionCheck") {
    group = "verification"
    description = "Verify durable BK materialization-source slots and shared F4 protection reconciliation."
    dependsOn("bookKeeperPrimaryWalM3ExactSourceCheck")
    dependsOn(":nereus-bookkeeper:test")
}

tasks.register("bookKeeperPrimaryWalM3AsyncProfileCheck") {
    group = "verification"
    description = "Verify the BK async stable-head plan and shared F4 runtime source-provider composition."
    dependsOn("bookKeeperPrimaryWalM3ProtectionCheck")
    dependsOn(":nereus-pulsar-adapter:test")
}

tasks.register("bookKeeperPrimaryWalM3LagCheck") {
    group = "verification"
    description = "Verify BK async uses the shared authoritative F4 lag calculation."
    dependsOn("bookKeeperPrimaryWalM3AsyncProfileCheck")
    dependsOn(":nereus-materialization:test")
}

tasks.register("bookKeeperPrimaryWalM3SourceRetirementCheck") {
    group = "verification"
    description = "Verify healthy committed Object authority, terminal BK source release, and ledger-retention handoff."
    dependsOn("bookKeeperPrimaryWalM3LagCheck")
    dependsOn(":nereus-materialization:test")
    dependsOn(":nereus-bookkeeper:test")
}

tasks.register("bookKeeperPrimaryWalM3LiveReadCheck") {
    group = "verification"
    description =
        "Verify exact higher-generation retirement uses the normal resolver, durable Object pin, and full-range reader."
    dependsOn("bookKeeperPrimaryWalM3SourceRetirementCheck")
    dependsOn(":nereus-core:test")
    dependsOn(":nereus-materialization:test")
    dependsOn(":nereus-pulsar-adapter:test")
}

tasks.register("bookKeeperPrimaryWalM3SealedLedgerCheck") {
    group = "verification"
    description = "Verify a sealed BK tail triggers the single F4 scanner and ordinary planner admits one gen0 source."
    dependsOn("bookKeeperPrimaryWalM3LiveReadCheck")
    dependsOn(":nereus-materialization:test")
    dependsOn(":nereus-bookkeeper:test")
    dependsOn(":nereus-pulsar-adapter:test")
}

tasks.register("bookKeeperPrimaryWalM3Check") {
    group = "verification"
    description = "Run deterministic BK async Object source, protection, lag, retirement, and sealed-tail gates."
    dependsOn("bookKeeperPrimaryWalM3SealedLedgerCheck")
    dependsOn("checkBookKeeperModuleBoundaries")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-bookkeeper:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register("bookKeeperPrimaryWalM3RealServiceCheck") {
    group = "verification"
    description =
        "Run BK-M3 real Oxia/BookKeeper/Object stable-head, fallback, fresh-runtime publication, read, and retirement proof."
    dependsOn("bookKeeperPrimaryWalM3Check")
    dependsOn(":nereus-pulsar-adapter:bkM3IntegrationTest")
}

tasks.register("bookKeeperPrimaryWalM3PhysicalRetirementCheck") {
    group = "verification"
    description =
        "Verify BK-M3 real source release, mandatory-reference retirement, and whole-ledger physical deletion."
    dependsOn("bookKeeperPrimaryWalM3RealServiceCheck")
}

tasks.register("bookKeeperPrimaryWalM3ResponseLossCheck") {
    group = "verification"
    description = "Verify BK-M3 fresh-runtime task, source, output, and publication response-loss convergence."
    dependsOn("bookKeeperPrimaryWalM3PhysicalRetirementCheck")
    dependsOn(":nereus-pulsar-adapter:bkM3IntegrationTest")
}

tasks.register("bookKeeperPrimaryWalM3LagFailureCheck") {
    group = "verification"
    description = "Verify BK-M3 real lag rejection/recovery and unreadable-Object retirement veto/fallback."
    dependsOn("bookKeeperPrimaryWalM3ResponseLossCheck")
    dependsOn(":nereus-bookkeeper:test")
    dependsOn(":nereus-pulsar-adapter:bkM3IntegrationTest")
}

tasks.register("bookKeeperPrimaryWalM3FinalCheck") {
    group = "verification"
    description =
        "Run BK-M3 ordinary and real Oxia/BookKeeper/Object acceptance over the final-gated BK_ONLY predecessor."
    dependsOn("bookKeeperPrimaryWalM3LagFailureCheck")
    dependsOn("bookKeeperPrimaryWalM2FinalCheck")
}

tasks.register("bookKeeperPrimaryWalM4CompletionPolicyCheck") {
    group = "verification"
    description =
        "Verify BK sync resolves REQUIRED_OBJECT_GENERATION and rejects weaker or uninstalled policies before IO."
    dependsOn("bookKeeperPrimaryWalDocumentationCheck")
    dependsOn(":nereus-core:test")
    dependsOn(":nereus-bookkeeper:test")
}

tasks.register("bookKeeperPrimaryWalM4TaskReuseCheck") {
    group = "verification"
    description =
        "Verify exact single-source task creation uses the shared F4 worker and cannot race the background planner."
    dependsOn("bookKeeperPrimaryWalM4CompletionPolicyCheck")
    dependsOn(":nereus-materialization:test")
    dependsOn(":nereus-pulsar-adapter:bkM4IntegrationTest")
}

tasks.register("bookKeeperPrimaryWalM4KnownCommittedCheck") {
    group = "verification"
    description =
        "Verify post-head Object failure returns KNOWN_COMMITTED and recovery reuses one BK reservation and offset."
    dependsOn("bookKeeperPrimaryWalM4TaskReuseCheck")
    dependsOn(":nereus-bookkeeper:test")
    dependsOn(":nereus-pulsar-adapter:bkM4IntegrationTest")
}

tasks.register("bookKeeperPrimaryWalM4ReadAdmissionCheck") {
    group = "verification"
    description = "Verify producer success waits for exact COMMITTED higher-generation normal-read admission."
    dependsOn("bookKeeperPrimaryWalM4KnownCommittedCheck")
    dependsOn(":nereus-materialization:test")
    dependsOn(":nereus-pulsar-adapter:bkM4IntegrationTest")
}

tasks.register("bookKeeperPrimaryWalM4Check") {
    group = "verification"
    description = "Run BK sync completion policy, deterministic task, recovery, and read-admission gates."
    dependsOn("bookKeeperPrimaryWalM4ReadAdmissionCheck")
    dependsOn("checkBookKeeperModuleBoundaries")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-bookkeeper:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register("bookKeeperPrimaryWalM4FinalCheck") {
    group = "verification"
    description = "Run BK-M4 ordinary and real Oxia/BookKeeper/Object sync acceptance over final-gated BK async."
    dependsOn("bookKeeperPrimaryWalM4Check")
    dependsOn("bookKeeperPrimaryWalM3FinalCheck")
}

tasks.register("bookKeeperPrimaryWalM5ConfigurationCheck") {
    group = "verification"
    description = "Verify typed BK rollout configuration, safe GC defaults, source lock, and current documentation."
    dependsOn("checkPulsarSourceLock")
    dependsOn("bookKeeperPrimaryWalDocumentationCheck")
    dependsOn(":nereus-pulsar-adapter:test")
}

tasks.register<Exec>("bookKeeperPrimaryWalM5CapabilityCheck") {
    group = "verification"
    description = "Verify activation-bound BK reserved properties and stable profile-specific all-broker snapshots."
    dependsOn("bookKeeperPrimaryWalM5ConfigurationCheck")
    dependsOn("publishPhase2DevelopmentArtifacts")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusBookKeeperPrimaryWalCapabilityTest",
        "--rerun-tasks",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("bookKeeperPrimaryWalM5FirstCreateCheck") {
    group = "verification"
    description =
        "Verify BK first-create admission precedes every L0 mutation while existing projection open remains available."
    dependsOn("bookKeeperPrimaryWalM5CapabilityCheck")
    dependsOn(":nereus-managed-ledger:test")
    dependsOn(":nereus-pulsar-adapter:test")
}

tasks.register<Exec>("bookKeeperPrimaryWalM5BorrowedClientCheck") {
    group = "verification"
    description = "Verify the production BK rollout still borrows and never closes the stock broker client."
    dependsOn("bookKeeperPrimaryWalM5FirstCreateCheck")
    dependsOn("publishPhase2DevelopmentArtifacts")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusManagedLedgerStorageBookKeeperClientTest",
        "--rerun-tasks",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("bookKeeperPrimaryWalM5RetentionCheck") {
    group = "verification"
    description = "Verify production all-shard BK reference retirement and activation-guarded ledger GC scheduling."
    dependsOn("bookKeeperPrimaryWalM5BorrowedClientCheck")
    dependsOn("checkBookKeeperModuleBoundaries")
    dependsOn(":nereus-bookkeeper:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("bookKeeperPrimaryWalM5DeletionActivationCheck") {
    group = "verification"
    description = "Verify producer-owned BK deletion proofs, one-CAS activation, and the locked broker handoff surface."
    dependsOn("bookKeeperPrimaryWalM5RetentionCheck")
    dependsOn("checkPulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:compileJava",
        "--rerun-tasks",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
    )
}

tasks.register<Exec>("checkBookKeeperPrimaryWalM5AdminRoutingContractSurface") {
    group = "verification"
    description = "Audit proof-safe BK admin DTOs and one durable-profile route for loaded/unloaded/partitioned topics."
    dependsOn("checkPulsarSourceLock")
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-bookkeeper-primary-wal-m5-admin-routing-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("bookKeeperPrimaryWalM5AdminRoutingCheck") {
    group = "verification"
    description = "Verify authenticated BK rollout administration and exact durable-profile admin routing."
    dependsOn("bookKeeperPrimaryWalM5DeletionActivationCheck")
    dependsOn("checkBookKeeperPrimaryWalM5AdminRoutingContractSurface")
    dependsOn("publishPhase2DevelopmentArtifacts")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.admin.impl.NereusBookKeeperPrimaryWalAdminTest",
        "--tests", "org.apache.pulsar.broker.admin.impl.PersistentTopicsNereusDurableProfileRoutingTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusAdminOperationTest",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky,broker-isolated",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register<Exec>("bookKeeperPrimaryWalM5TwoBrokerCheck") {
    group = "verification"
    description = "Run the retry-disabled real two-broker BK_ONLY ownership, MessageId, seek, and stock-BK gate."
    dependsOn("bookKeeperPrimaryWalM5AdminRoutingCheck")
    dependsOn("checkPulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusBookKeeperMultiBrokerIntegrationTest",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
        "-PtestRetryCount=0",
    )
}

tasks.register<Exec>("bookKeeperPrimaryWalM5Check") {
    group = "verification"
    description = "Run the retry-disabled BK ownership exclusion, authority rollover, and mixed-profile aggregate."
    dependsOn("bookKeeperPrimaryWalM5TwoBrokerCheck")
    dependsOn("checkPulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.loadbalance.extensions.filter.NereusBookKeeperOwnershipFilterTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusBookKeeperCapabilityRolloverTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusMixedPrimaryProfilesMultiBrokerTest",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
        "-PtestRetryCount=0",
    )
}

tasks.register("bookKeeperPrimaryWalM5FinalCheck") {
    group = "verification"
    description = "Final-gate BK-M5 rollout over the retry-disabled aggregate and final-gated BK-M4 chain."
    dependsOn("bookKeeperPrimaryWalM5Check")
    dependsOn("bookKeeperPrimaryWalM4FinalCheck")
}

tasks.register<Exec>("bookKeeperPrimaryWalM6ScenarioEvidenceCheck") {
    group = "verification"
    description = "Trace BK-87 through BK-96 to annotated tests and their executable owning gates."
    dependsOn("checkPulsarSourceLock")
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-bookkeeper-primary-wal-m6-scenario-evidence.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register("bookKeeperPrimaryWalM6ScaleCheck") {
    group = "verification"
    description = "Verify BK root/inventory/hazard bounds and shared F4 task/generation scale boundaries."
    dependsOn("bookKeeperPrimaryWalM6ScenarioEvidenceCheck")
    dependsOn(":nereus-bookkeeper:bkM6ScaleTest")
    dependsOn(":nereus-materialization:bkM6MixedSourceScaleTest")
    dependsOn(":nereus-core:bkM6GenerationScaleTest")
    dependsOn(":nereus-metadata-oxia:test")
}

tasks.register("bookKeeperPrimaryWalM6ChaosCheck") {
    group = "verification"
    description =
        "Verify allocation/write/seal/head/task/publication/delete response-loss recovery across fresh runtimes."
    dependsOn("bookKeeperPrimaryWalM6ScaleCheck")
    dependsOn(":nereus-bookkeeper:bkM6ChaosTest")
    dependsOn("bookKeeperPrimaryWalM2AllocationAuthorityCheck")
    dependsOn("bookKeeperPrimaryWalM3ResponseLossCheck")
}

tasks.register("bookKeeperPrimaryWalM6CompatibilityCheck") {
    group = "verification"
    description = "Run mixed BK/Object rollout with retry-disabled two-broker worker contention and async trim/GC."
    dependsOn("bookKeeperPrimaryWalM6ChaosCheck")
    dependsOn("bookKeeperPrimaryWalM5Check")
    dependsOn("phase4M6TwoBrokerWorkerContentionPulsarCheck")
    dependsOn("phase4M5AsyncRetentionMultiBrokerPulsarCheck")
}

tasks.register("bookKeeperPrimaryWalM6Check") {
    group = "verification"
    description = "Run the complete ordinary BK-M6 scenario, scale, chaos, and compatibility gate."
    dependsOn("bookKeeperPrimaryWalM6CompatibilityCheck")
    dependsOn("bookKeeperPrimaryWalDocumentationCheck")
    dependsOn("checkBookKeeperModuleBoundaries")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPulsarSourceLock")
    dependsOn(":nereus-api:check")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-bookkeeper:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register("bookKeeperPrimaryWalM6FinalCheck") {
    group = "verification"
    description = "Run BK-M6 over every F1-BK milestone and all Phase 1 through Phase 4 final predecessors."
    dependsOn("bookKeeperPrimaryWalM6Check")
    dependsOn("bookKeeperPrimaryWalM1FinalCheck")
    dependsOn("bookKeeperPrimaryWalM2FinalCheck")
    dependsOn("bookKeeperPrimaryWalM3FinalCheck")
    dependsOn("bookKeeperPrimaryWalM4FinalCheck")
    dependsOn("bookKeeperPrimaryWalM5FinalCheck")
    dependsOn("phase15FinalCheck")
    dependsOn("phase2FinalCheck")
    dependsOn("phase3FinalCheck")
    dependsOn("phase4FinalCheck")
    dependsOn("checkPhase4FinalDockerIsolation")
    dependsOn("checkPhase4FinalPulsarCheckoutIsolation")
}

tasks.register("bookKeeperPrimaryWalCheck") {
    group = "verification"
    description = "Run every ordinary BookKeeper primary-WAL delivery gate."
    dependsOn("bookKeeperPrimaryWalM6Check")
}

tasks.register("bookKeeperPrimaryWalFinalCheck") {
    group = "verification"
    description = "Run the complete BookKeeper primary-WAL release gate; this is the only delivery completion claim."
    dependsOn("bookKeeperPrimaryWalCheck")
    dependsOn("bookKeeperPrimaryWalM6FinalCheck")
}

tasks.register<Exec>("checkPhase4ModuleBoundaries") {
    group = "verification"
    description = "Verify the acyclic protocol-neutral F4 module dependency direction."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-module-boundaries.sh")
}

tasks.register<Exec>("checkPhase4PulsarSourceLock") {
    group = "verification"
    description = "Verify the exact clean local Pulsar source consumed by Phase 4."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-pulsar-source-lock.sh",
        pulsarCheckoutPath.get(),
        pulsarExpectedHead.get(),
    )
}

tasks.register<Exec>("checkPhase4GuardedObjectPut") {
    group = "verification"
    description = "Audit authorization immediately before every F4 provider PUT attempt."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-guarded-object-put.sh")
}

tasks.register("phase4M1Check") {
    group = "verification"
    description = "Verify F4-M1 metadata, object lifecycle/IO, durable pins/protections, and module boundaries."
    dependsOn("phase3M6Check")
    dependsOn("checkPhase4ContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn("checkPhase4GuardedObjectPut")
    dependsOn(":nereus-api:check")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-metadata-oxia:compileOxiaIntegrationTestJava")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-object-store:compileS3IntegrationTestJava")
}

tasks.register("phase4M1FinalCheck") {
    group = "verification"
    description = "Run the ordinary and Docker-backed real Oxia/LocalStack F4-M1 gates."
    dependsOn("phase4M1Check")
    dependsOn(":nereus-metadata-oxia:f4OxiaIntegrationTest")
    dependsOn(":nereus-object-store:s3IntegrationTest")
}

tasks.register<Exec>("checkPhase4M2ContractSurface") {
    group = "verification"
    description = "Audit the implemented F4-M2 publication, resolver, fallback, test, and documentation surfaces."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m2-contract-surface.sh")
}

tasks.register("phase4M2Check") {
    group = "verification"
    description = "Verify F4-M2 generation publication, committed reads, bounded retry, and quarantine propagation."
    dependsOn("phase4M1Check")
    dependsOn("checkPhase4M2ContractSurface")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-materialization:compileF4M2IntegrationTestJava")
    dependsOn(":nereus-metadata-oxia:check")
}

tasks.register("phase4M2FinalCheck") {
    group = "verification"
    description = "Run ordinary and Docker-backed real Oxia/LocalStack F4-M2 publication and fallback gates."
    dependsOn("phase4M2Check")
    dependsOn(":nereus-materialization:f4M2IntegrationTest")
}

tasks.register<Exec>("checkPhase4M3ContractSurface") {
    group = "verification"
    description = "Audit the implemented F4-M3 format, planner, worker, recovery, and test surfaces."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m3-contract-surface.sh")
}

tasks.register("phase4M3Check") {
    group = "verification"
    description = "Verify F4-M3 compacted formats, planning, workers, recovery, and bounded lifecycle."
    dependsOn("phase4M2Check")
    dependsOn("checkPhase4M3ContractSurface")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-materialization:compileF4M3IntegrationTestJava")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
}

tasks.register("phase4M3FinalCheck") {
    group = "verification"
    description = "Run ordinary and Docker-backed real Oxia/LocalStack F4-M3 materialization gates."
    dependsOn("phase4M3Check")
    dependsOn("phase4M2FinalCheck")
    dependsOn(":nereus-materialization:f4M3IntegrationTest")
}

tasks.register<Exec>("checkPhase4M4CheckpointContractSurface") {
    group = "verification"
    description = "Audit the in-progress F4-M4 NRC1 object-protocol implementation checkpoint."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-checkpoint-contract-surface.sh")
}

tasks.register("phase4M4CheckpointCheck") {
    group = "verification"
    description = "Verify the in-progress F4-M4 NRC1 streaming codec, strict reader, and metadata verifier."
    dependsOn("phase4M3Check")
    dependsOn("checkPhase4M4CheckpointContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-object-store:check")
}

tasks.register<Exec>("checkPhase4M4ProtectedAppendContractSurface") {
    group = "verification"
    description = "Audit the in-progress F4-M4 protected generation-zero append checkpoint."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-protected-append-contract-surface.sh")
}

tasks.register("phase4M4ProtectedAppendCheck") {
    group = "verification"
    description = "Verify exact intent/root/protection ordering for generation-zero append and recovery."
    dependsOn("phase4M4CheckpointCheck")
    dependsOn("checkPhase4M4ProtectedAppendContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-core:compilePhase1IntegrationTestJava")
    dependsOn(":nereus-materialization:compileF4M2IntegrationTestJava")
    dependsOn(":nereus-materialization:compileF4M3IntegrationTestJava")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-metadata-oxia:compileOxiaIntegrationTestJava")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M4RecoveryRootContractSurface") {
    group = "verification"
    description = "Audit the in-progress F4-M4 recovery-root publication and reconciliation checkpoint."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-recovery-root-contract-surface.sh")
}

tasks.register("phase4M4RecoveryRootCheck") {
    group = "verification"
    description = "Verify anchor-aware NRC1 root publication, response-loss recovery, and protection repair."
    dependsOn("phase4M4ProtectedAppendCheck")
    dependsOn("checkPhase4M4RecoveryRootContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
}

tasks.register<Exec>("checkPhase4M4CheckpointReplayContractSurface") {
    group = "verification"
    description = "Audit the in-progress F4-M4 checkpoint-aware append replay adapter."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-checkpoint-replay-contract-surface.sh")
}

tasks.register("phase4M4CheckpointReplayCheck") {
    group = "verification"
    description = "Verify root-stable pinned append replay across live commits and NRC1 checkpoint entries."
    dependsOn("phase4M4RecoveryRootCheck")
    dependsOn("checkPhase4M4CheckpointReplayContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
}

tasks.register<Exec>("checkPhase4M4CheckpointIndexRepairContractSurface") {
    group = "verification"
    description = "Audit the in-progress F4-M4 checkpoint-derived generation-index repair checkpoint."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-checkpoint-index-repair-contract-surface.sh")
}

tasks.register("phase4M4CheckpointIndexRepairCheck") {
    group = "verification"
    description = "Verify root-stable protected committed-index restoration from NRC1 checkpoint evidence."
    dependsOn("phase4M4CheckpointReplayCheck")
    dependsOn("checkPhase4M4CheckpointIndexRepairContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
}

tasks.register<Exec>("checkPhase4M4RetirementMetadataContractSurface") {
    group = "verification"
    description = "Audit the in-progress F4-M4 exact source/object-audit retirement metadata checkpoint."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-retirement-metadata-contract-surface.sh")
}

tasks.register("phase4M4RetirementMetadataCheck") {
    group = "verification"
    description = "Verify exact read-before-delete metadata retirement without enabling physical deletion."
    dependsOn("phase4M4CheckpointIndexRepairCheck")
    dependsOn("checkPhase4M4RetirementMetadataContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-metadata-oxia:check")
}

tasks.register<Exec>("checkPhase4M4GcPlanContractSurface") {
    group = "verification"
    description = "Audit the in-progress F4-M4 bounded reconstructable GC plan checkpoint."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-gc-plan-contract-surface.sh")
}

tasks.register("phase4M4GcPlanCheck") {
    group = "verification"
    description = "Verify GC configuration, candidate identity, and canonical restart-rebuild plan facts."
    dependsOn("phase4M4RetirementMetadataCheck")
    dependsOn("checkPhase4M4GcPlanContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-materialization:check")
}

tasks.register<Exec>("checkPhase4M4RootFenceContractSurface") {
    group = "verification"
    description = "Audit the in-progress F4-M4 reference-domain and physical-root fence checkpoint."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-root-fence-contract-surface.sh")
}

tasks.register("phase4M4RootFenceCheck") {
    group = "verification"
    description = "Verify reference-domain collection and recoverable MARK/DRAIN/DELETING root fencing."
    dependsOn("phase4M4GcPlanCheck")
    dependsOn("checkPhase4M4RootFenceContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-materialization:check")
}

tasks.register<Exec>("checkPhase4M4ReferenceDomainsContractSurface") {
    group = "verification"
    description = "Audit query-bound F4-M4 generation, append-recovery, and materialization reference domains."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-reference-domains-contract-surface.sh")
}

tasks.register("phase4M4ReferenceDomainsCheck") {
    group = "verification"
    description = "Verify stateless reference revalidation and exact removal binding over real metadata scans."
    dependsOn("phase4M4RootFenceCheck")
    dependsOn("checkPhase4M4ReferenceDomainsContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-materialization:check")
}

tasks.register<Exec>("checkPhase4M4ManagedLedgerDomainsContractSurface") {
    group = "verification"
    description = "Audit exact F2 projection and F3 cursor GC reference domains."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-managed-ledger-domains-contract-surface.sh")
}

tasks.register("phase4M4ManagedLedgerDomainsCheck") {
    group = "verification"
    description = "Verify composed generation markers and restart-safe projection/cursor authorities."
    dependsOn("phase4M4ReferenceDomainsCheck")
    dependsOn("checkPhase4M4ManagedLedgerDomainsContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
}

tasks.register<Exec>("checkPhase4M4RetirementJournalContractSurface") {
    group = "verification"
    description = "Audit manifest-last retirement journal persistence and PREPARE-before-MARK fencing."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-retirement-journal-contract-surface.sh")
}

tasks.register("phase4M4RetirementJournalCheck") {
    group = "verification"
    description = "Verify root-authenticated journal sealing, restart reload, and fail-closed intent admission."
    dependsOn("phase4M4ManagedLedgerDomainsCheck")
    dependsOn("checkPhase4M4RetirementJournalContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-materialization:check")
}

tasks.register("phase4M4DestructiveRecoveryCheck") {
    group = "verification"
    description = "Verify root-authenticated DELETING recovery, exact object deletion, and DELETED convergence."
    dependsOn("phase4M4RetirementJournalCheck")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-materialization:check")
}

tasks.register<Exec>("checkPhase4M4GenerationRetirementContractSurface") {
    group = "verification"
    description = "Audit typed source retirement and view-specific/below-trim higher-generation pre-drain fencing."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-generation-retirement-contract-surface.sh")
}

tasks.register("phase4M4GenerationRetirementCheck") {
    group = "verification"
    description =
        "Verify exact source deletion, view-specific/below-trim eligibility, higher pre-drain, and retirement recovery."
    dependsOn("phase4M4DestructiveRecoveryCheck")
    dependsOn("checkPhase4M4GenerationRetirementContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-api:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-materialization:check")
}

tasks.register<Exec>("checkPhase4M4ActivationMetadataContractSurface") {
    group = "verification"
    description = "Audit the durable generation-protocol activation metadata foundation."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-activation-metadata-contract-surface.sh")
}

tasks.register("phase4M4ActivationMetadataCheck") {
    group = "verification"
    description = "Verify the exact cluster activation record, codec, monotonic CAS, and frozen vectors."
    dependsOn("phase4M4GenerationRetirementCheck")
    dependsOn("checkPhase4M4ActivationMetadataContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-metadata-oxia:check")
}

tasks.register<Exec>("checkPhase4M4GlobalDomainsContractSurface") {
    group = "verification"
    description = "Audit activation-gated ownerless global scope and future catalog sentinel."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-global-domains-contract-surface.sh")
}

tasks.register("phase4M4GlobalDomainsCheck") {
    group = "verification"
    description = "Verify all five ownerless global domains and future-domain fail-closed capability checks."
    dependsOn("phase4M4ActivationMetadataCheck")
    dependsOn("checkPhase4M4GlobalDomainsContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-managed-ledger:check")
}

tasks.register<Exec>("checkPhase4M4TombstoneRetirementContractSurface") {
    group = "verification"
    description = "Audit dual-absence, audit-order, and root-last DELETED tombstone retirement."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-tombstone-retirement-contract-surface.sh")
}

tasks.register("phase4M4TombstoneRetirementCheck") {
    group = "verification"
    description = "Verify restart-safe DELETED-root and Phase 1 object-audit retirement."
    dependsOn("phase4M4GlobalDomainsCheck")
    dependsOn("checkPhase4M4TombstoneRetirementContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-materialization:check")
}

tasks.register<Exec>("checkPhase4M4CursorProtectionContractSurface") {
    group = "verification"
    description = "Audit guarded cursor snapshot publication, permanent protection, and durable read pinning."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-cursor-protection-contract-surface.sh")
}

tasks.register("phase4M4CursorProtectionCheck") {
    group = "verification"
    description = "Verify the F4-protected cursor snapshot write/read frontier and runtime wiring."
    dependsOn("phase4M4TombstoneRetirementCheck")
    dependsOn("checkPhase4M4CursorProtectionContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-managed-ledger:compileCursorS3IntegrationTestJava")
    dependsOn(":nereus-managed-ledger:compileCursorM2IntegrationTestJava")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M4PhysicalRootBackfillContractSurface") {
    group = "verification"
    description = "Audit all-shard physical-root/cursor-root backfill and activation-proof closure."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-physical-root-backfill-contract-surface.sh")
}

tasks.register("phase4M4PhysicalRootBackfillCheck") {
    group = "verification"
    description = "Verify stable live-reference backfill, exact roots/protections, and dual activation proofs."
    dependsOn("phase4M4CursorProtectionCheck")
    dependsOn("checkPhase4M4PhysicalRootBackfillContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
}

tasks.register<Exec>("checkPhase4M4CursorSnapshotGcContractSurface") {
    group = "verification"
    description = "Audit complete cursor-snapshot inventory and post-drain GC revalidation."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-cursor-snapshot-gc-contract-surface.sh")
}

tasks.register("phase4M4CursorSnapshotGcCheck") {
    group = "verification"
    description = "Verify bounded cursor-snapshot candidate discovery and final authority revalidation."
    dependsOn("phase4M4PhysicalRootBackfillCheck")
    dependsOn("checkPhase4M4CursorSnapshotGcContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
}

tasks.register<Exec>("checkPhase4M4CursorGcExecutionContractSurface") {
    group = "verification"
    description = "Audit restart-reconstructable cursor-GC plan execution and safe runtime composition."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-cursor-gc-execution-contract-surface.sh")
}

tasks.register("phase4M4CursorGcExecutionCheck") {
    group = "verification"
    description =
        "Verify cursor snapshot MARK/drain/restart/delete execution with production deletion still default-off."
    dependsOn("phase4M4CursorSnapshotGcCheck")
    dependsOn("checkPhase4M4CursorGcExecutionContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M4ObjectInventoryContractSurface") {
    group = "verification"
    description = "Audit known-prefix orphan inventory, strict key inverses, and exact missing-root registration."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-object-inventory-contract-surface.sh")
}

tasks.register("phase4M4ObjectInventoryCheck") {
    group = "verification"
    description = "Verify old exact-HEAD orphan inventory registration without listing-based deletion authority."
    dependsOn("phase4M4CursorGcExecutionCheck")
    dependsOn("checkPhase4M4ObjectInventoryContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M4RegistrationRetirementContractSurface") {
    group = "verification"
    description = "Audit exact deleted-stream authority, owner drain ordering, and registration-last retirement."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-registration-retirement-contract-surface.sh")
}

tasks.register("phase4M4RegistrationRetirementCheck") {
    group = "verification"
    description = "Verify bounded stream-registration retirement and response-loss convergence without object deletion."
    dependsOn("phase4M4ObjectInventoryCheck")
    dependsOn("checkPhase4M4RegistrationRetirementContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M4LifecycleSchedulingContractSurface") {
    group = "verification"
    description = "Audit metadata-first root/registration/inventory scheduling and lifecycle recovery routing."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-lifecycle-scheduling-contract-surface.sh")
}

tasks.register("phase4M4LifecycleSchedulingCheck") {
    group = "verification"
    description = "Verify non-overlapping periodic physical-GC passes with restart-safe lifecycle routing."
    dependsOn("phase4M4RegistrationRetirementCheck")
    dependsOn("checkPhase4M4LifecycleSchedulingContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M4PhysicalGcConfigContractSurface") {
    group = "verification"
    description = "Audit exact broker physical-GC configuration mapping and provider consumption."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-physical-gc-config-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("phase4M4PhysicalGcConfigPulsarCheck") {
    group = "verification"
    description = "Run locked Pulsar physical-GC configuration formatting, style, and focused tests."
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase4M4LifecycleSchedulingCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker-common:spotlessJavaCheck",
        ":pulsar-broker-common:checkstyleMain",
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusBrokerStorageConfigurationTest",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky,broker-isolated",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase4M4PhysicalGcConfigCheck") {
    group = "verification"
    description = "Verify checkpoint AO exact broker GC mapping while coverage and physical deletion stay closed."
    dependsOn("phase4M4LifecycleSchedulingCheck")
    dependsOn("checkPhase4M4PhysicalGcConfigContractSurface")
    dependsOn("phase4M4PhysicalGcConfigPulsarCheck")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M4ObjectStoreCapabilityContractSurface") {
    group = "verification"
    description = "Audit the configured-scope guarded PUT/HEAD/LIST/exact-DELETE capability proof."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-object-store-capability-contract-surface.sh",
    )
}

tasks.register("phase4M4ObjectStoreCapabilityCheck") {
    group = "verification"
    description = "Verify checkpoint AP object-store destructive-protocol capability without activating deletion."
    dependsOn("phase4M4PhysicalGcConfigCheck")
    dependsOn("checkPhase4M4ObjectStoreCapabilityContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-object-store:check")
}

tasks.register<Exec>("checkPhase4M4PhysicalDeletionActivationContractSurface") {
    group = "verification"
    description = "Audit ordered proof composition, atomic delete activation, and restart scope fencing."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-physical-deletion-activation-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("phase4M4PhysicalDeletionActivationPulsarCheck") {
    group = "verification"
    description = "Run locked Pulsar physical-deletion activation sequencing, formatting, style, and tests."
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    // Both checkpoints invoke the same locked Pulsar checkout with --rerun-tasks.
    // Keep them serialized so one build cannot remove class outputs while the other compiles.
    mustRunAfter("phase4M4ObjectStoreCapabilityCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusManagedLedgerStorageGenerationActivationTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusBrokerStorageConfigurationTest",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky,broker-isolated",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase4M4PhysicalDeletionActivationCheck") {
    group = "verification"
    description = "Verify checkpoint AR product composition, atomic activation, and exact-scope restart recovery."
    dependsOn("phase4M4ObjectStoreCapabilityCheck")
    dependsOn("checkPhase4M4PhysicalDeletionActivationContractSurface")
    dependsOn("phase4M4PhysicalDeletionActivationPulsarCheck")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M4PhysicalDeletionIntegrationContractSurface") {
    group = "verification"
    description = "Audit shared ownerless reference-domain assembly and the real Oxia/LocalStack recovery fixture."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-physical-deletion-integration-contract-surface.sh",
    )
}

tasks.register("phase4M4PhysicalDeletionIntegrationCheck") {
    group = "verification"
    description =
        "Verify checkpoint AS real Oxia/LocalStack activation, scope fencing, and destructive restart recovery."
    dependsOn("phase4M4PhysicalDeletionActivationCheck")
    dependsOn("checkPhase4M4PhysicalDeletionIntegrationContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-pulsar-adapter:f4M4IntegrationTest")
}

tasks.register<Exec>("checkPhase4M4PostDeleteCrashRecoveryContractSurface") {
    group = "verification"
    description = "Audit the real post-DELETE/pre-root-CAS process-death recovery fixture."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-post-delete-crash-recovery-contract-surface.sh",
    )
}

tasks.register("phase4M4PostDeleteCrashRecoveryCheck") {
    group = "verification"
    description = "Verify checkpoint AT durable DELETING recovery after process death following real object DELETE."
    dependsOn("phase4M4PhysicalDeletionIntegrationCheck")
    dependsOn("checkPhase4M4PostDeleteCrashRecoveryContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-pulsar-adapter:f4M4IntegrationTest")
}

tasks.register<Exec>("checkPhase4M4DeletedCasResponseLossContractSurface") {
    group = "verification"
    description = "Audit exact reload after a real DELETED-root CAS applies but its response is lost."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-deleted-cas-response-loss-contract-surface.sh",
    )
}

tasks.register("phase4M4DeletedCasResponseLossCheck") {
    group = "verification"
    description = "Verify checkpoint AU real DELETED-root CAS response-loss convergence without repeated object DELETE."
    dependsOn("phase4M4PostDeleteCrashRecoveryCheck")
    dependsOn("checkPhase4M4DeletedCasResponseLossContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-pulsar-adapter:f4M4IntegrationTest")
}

tasks.register<Exec>("checkPhase4M4TwoWorkerConvergenceContractSurface") {
    group = "verification"
    description = "Audit deterministic two-runtime DELETING-CAS contention and exact-delete convergence."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-two-worker-convergence-contract-surface.sh",
    )
}

tasks.register("phase4M4TwoWorkerConvergenceCheck") {
    group = "verification"
    description =
        "Verify checkpoint AV two independent workers converge one durable delete intent against real services."
    dependsOn("phase4M4DeletedCasResponseLossCheck")
    dependsOn("checkPhase4M4TwoWorkerConvergenceContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-pulsar-adapter:f4M4IntegrationTest")
}

tasks.register<Exec>("checkPhase4M4AllShardRecoveryContractSurface") {
    group = "verification"
    description = "Audit all-shard mixed-lifecycle recovery and opaque object-list continuation semantics."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-all-shard-recovery-contract-surface.sh",
    )
}

tasks.register("phase4M4AllShardRecoveryCheck") {
    group = "verification"
    description = "Verify checkpoint AW all 256 root shards recover from durable metadata with empty object inventory."
    dependsOn("phase4M4TwoWorkerConvergenceCheck")
    dependsOn("checkPhase4M4AllShardRecoveryContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:f4M4IntegrationTest")
}

tasks.register<Exec>("checkPhase4M4RootScaleContractSurface") {
    group = "verification"
    description = "Audit the real-Oxia 1,001-root hot-shard pagination and fresh-process scale fixture."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-root-scale-contract-surface.sh",
    )
}

tasks.register("phase4M4RootScaleCheck") {
    group = "verification"
    description = "Verify checkpoint AX scans 1,001 roots in one shard plus every other root shard after restart."
    dependsOn("phase4M4AllShardRecoveryCheck")
    dependsOn("checkPhase4M4RootScaleContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:f4M4IntegrationTest")
}

tasks.register<Exec>("checkPhase4M4TombstoneScaleContractSurface") {
    group = "verification"
    description = "Audit the 10,000-root dual-window retirement and cancelled-deadline memory bound."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-tombstone-scale-contract-surface.sh",
    )
}

tasks.register("phase4M4TombstoneScaleCheck") {
    group = "verification"
    description = "Verify checkpoint AY retires 10,000 DELETED roots through two bounded absence windows."
    dependsOn("phase4M4RootScaleCheck")
    dependsOn("checkPhase4M4TombstoneScaleContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M4CursorGcScaleContractSurface") {
    group = "verification"
    description = "Audit stack-bounded 10,000-candidate visitation and exact 10,000-root cursor-snapshot GC."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-cursor-gc-scale-contract-surface.sh",
    )
}

tasks.register("phase4M4CursorGcScaleCheck") {
    group = "verification"
    description =
        "Verify checkpoint AZ classifies and deletes live/old/CAS-lost/deleted-cursor snapshots at 10,000 roots."
    dependsOn("phase4M4TombstoneScaleCheck")
    dependsOn("checkPhase4M4CursorGcScaleContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M4SourceProtectionCutContractSurface") {
    group = "verification"
    description = "Audit restart-safe source/protection retirement cuts and applied-delete response loss."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-source-protection-cut-contract-surface.sh",
    )
}

tasks.register("phase4M4SourceProtectionCutCheck") {
    group = "verification"
    description = "Verify checkpoint BA resumes exact DELETING journals after source/protection deletion cuts."
    dependsOn("phase4M4CursorGcScaleCheck")
    dependsOn("checkPhase4M4SourceProtectionCutContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:check")
    dependsOn(":nereus-pulsar-adapter:f4M4IntegrationTest")
}

tasks.register<Exec>("checkPhase4M4LatePutTombstoneContractSurface") {
    group = "verification"
    description = "Audit guarded Object-WAL retries, every tombstone cut, and external-key reappearance recovery."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-late-put-tombstone-contract-surface.sh",
    )
}

tasks.register("phase4M4LatePutTombstoneCheck") {
    group = "verification"
    description = "Verify checkpoint BB rejects stale first/retried PUTs and reclaims external post-root bytes."
    dependsOn("phase4M4SourceProtectionCutCheck")
    dependsOn("checkPhase4GuardedObjectPut")
    dependsOn("checkPhase4M4LatePutTombstoneContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:check")
    dependsOn(":nereus-pulsar-adapter:f4M4IntegrationTest")
}

tasks.register<Exec>("checkPhase4M4ReadinessRolloverContractSurface") {
    group = "verification"
    description = "Audit deletion-active readiness rollover without partial proof publication."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-readiness-rollover-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("phase4M4ReadinessRolloverPulsarCheck") {
    group = "verification"
    description = "Run locked Pulsar readiness-rollover bound formatting, style, and focused tests."
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase4M4LatePutTombstoneCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker-common:spotlessJavaCheck",
        ":pulsar-broker-common:checkstyleMain",
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusGenerationRegistrationBackfillTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusBrokerStorageConfigurationTest",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky,broker-isolated",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase4M4ReadinessRolloverCheck") {
    group = "verification"
    description = "Verify checkpoint BC atomically refreshes deletion authority after broker readiness changes."
    dependsOn("phase4M4LatePutTombstoneCheck")
    dependsOn("checkPhase4M4ReadinessRolloverContractSurface")
    dependsOn("phase4M4ReadinessRolloverPulsarCheck")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register("phase4M4Check") {
    group = "verification"
    description = "Run every ordinary and real-service Nereus F4-M4 product gate."
    dependsOn("phase4M4ReadinessRolloverCheck")
}

tasks.register<Exec>("checkPhase4M4FinalContractSurface") {
    group = "verification"
    description = "Audit stable L0 authority and the real two-broker physical-GC acceptance fixture."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-final-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("phase4M4PhysicalGcMultiBrokerPulsarCheck") {
    group = "verification"
    description = "Run the real two-broker Nereus physical-GC, MessageId, and BookKeeper coexistence gate."
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase4M4Check")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusPhysicalGcMultiBrokerIntegrationTest",
        "--rerun-tasks",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
        "-PtestRetryCount=0",
    )
}

tasks.register("phase4M4FinalCheck") {
    group = "verification"
    description = "Run the complete F4-M4 release gate including real two-broker physical deletion and failover."
    dependsOn("phase4M4Check")
    dependsOn("checkPhase4M4FinalContractSurface")
    dependsOn("phase4M4PhysicalGcMultiBrokerPulsarCheck")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
}

tasks.register<Exec>("checkPhase4M5RegistrationFrontierContractSurface") {
    group = "verification"
    description = "Audit exact managed-ledger registration before every topic-open return."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m5-registration-frontier-contract-surface.sh")
}

tasks.register("phase4M5RegistrationFrontierCheck") {
    group = "verification"
    description = "Verify the F4 registration new-write/open frontier and shared production wiring."
    dependsOn("phase4M4FinalCheck")
    dependsOn("checkPhase4M5RegistrationFrontierContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M5GenerationCapabilityContractSurface") {
    group = "verification"
    description = "Audit the locked Pulsar generation capability, readiness identity, and invalidation surface."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m5-generation-capability-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("phase4M5GenerationCapabilityPulsarCheck") {
    group = "verification"
    description = "Run the exact Pulsar generation capability/readiness formatting, style, and focused tests."
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase4M5RegistrationFrontierCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusGenerationProtocolCapabilityTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusCursorProtocolCapabilityTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusStorageBindingCapabilityTest",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky,broker-isolated",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase4M5GenerationCapabilityCheck") {
    group = "verification"
    description = "Verify checkpoint Y generation capability and deterministic stable broker readiness."
    dependsOn("phase4M5RegistrationFrontierCheck")
    dependsOn("checkPhase4M5GenerationCapabilityContractSurface")
    dependsOn("phase4M5GenerationCapabilityPulsarCheck")
    dependsOn("checkPhase4Documentation")
}

tasks.register<Exec>("checkPhase4M5RegistrationBackfillContractSurface") {
    group = "verification"
    description = "Audit the exact unloaded-topic registration candidate and canonical broker backfill surface."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m5-registration-backfill-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("phase4M5RegistrationBackfillPulsarCheck") {
    group = "verification"
    description = "Run the locked Pulsar registration-backfill formatting, style, and focused tests."
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase4M5GenerationCapabilityCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker-common:spotlessJavaCheck",
        ":pulsar-broker-common:checkstyleMain",
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusGenerationRegistrationBackfillTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusBrokerStorageConfigurationTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusGenerationProtocolCapabilityTest",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky,broker-isolated",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase4M5RegistrationBackfillCheck") {
    group = "verification"
    description = "Verify checkpoint Z canonical cold-topic registration traversal and bounded report."
    dependsOn("phase4M5GenerationCapabilityCheck")
    dependsOn("checkPhase4M5RegistrationBackfillContractSurface")
    dependsOn("phase4M5RegistrationBackfillPulsarCheck")
    dependsOn("checkPhase4Documentation")
    dependsOn(":nereus-managed-ledger:check")
}

tasks.register<Exec>("checkPhase4M5RegistrationProofContractSurface") {
    group = "verification"
    description = "Audit the exact broker-readiness handoff and product-owned durable registration proof CAS."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m5-registration-proof-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register("phase4M5RegistrationProofCheck") {
    group = "verification"
    description = "Verify checkpoint AA durable stream-registration backfill proof completion."
    dependsOn("phase4M5RegistrationBackfillCheck")
    dependsOn("checkPhase4M5RegistrationProofContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M5ActivationGuardContractSurface") {
    group = "verification"
    description = "Audit the product-owned generation activation guard and disabled-by-default broker switch."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m5-activation-guard-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("phase4M5ActivationGuardPulsarCheck") {
    group = "verification"
    description = "Run the locked Pulsar activation-switch formatting, style, and focused configuration test."
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase4M5RegistrationProofCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker-common:spotlessJavaCheck",
        ":pulsar-broker-common:checkstyleMain",
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusBrokerStorageConfigurationTest",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky,broker-isolated",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase4M5ActivationGuardCheck") {
    group = "verification"
    description = "Verify checkpoint AB exact generation activation admission and runtime composition."
    dependsOn("phase4M5RegistrationProofCheck")
    dependsOn("checkPhase4M5ActivationGuardContractSurface")
    dependsOn("phase4M5ActivationGuardPulsarCheck")
    dependsOn("checkPhase4Documentation")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M5PublicationActivationContractSurface") {
    group = "verification"
    description = "Audit proof-gated PREPARED-to-ACTIVE publication activation and broker sequencing."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m5-publication-activation-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("phase4M5PublicationActivationPulsarCheck") {
    group = "verification"
    description = "Run the locked Pulsar proof-to-publication activation formatting, style, and focused tests."
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase4M5ActivationGuardCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusGenerationRegistrationBackfillTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusManagedLedgerStorageGenerationActivationTest",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky,broker-isolated",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase4M5PublicationActivationCheck") {
    group = "verification"
    description = "Verify checkpoint AC publication-only cluster activation after exact registration proof."
    dependsOn("phase4M5ActivationGuardCheck")
    dependsOn("checkPhase4M5PublicationActivationContractSurface")
    dependsOn("phase4M5PublicationActivationPulsarCheck")
    dependsOn("checkPhase4Documentation")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M5AsyncObjectWalContractSurface") {
    group = "verification"
    description = "Audit the opt-in async Object-WAL acknowledgement and protected generation-zero repair surface."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m5-async-object-wal-contract-surface.sh")
}

tasks.register("phase4M5AsyncObjectWalCheck") {
    group = "verification"
    description = "Verify checkpoint AD async Object-WAL acknowledgement and protected read/restart repair."
    dependsOn("phase4M5PublicationActivationCheck")
    dependsOn("checkPhase4M5AsyncObjectWalContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-core:check")
}

tasks.register<Exec>("checkPhase4M5RetentionPlannerContractSurface") {
    group = "verification"
    description = "Audit the exact policy, stable candidate, and F3-delegated logical-retention surface."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m5-retention-planner-contract-surface.sh")
}

tasks.register("phase4M5RetentionPlannerCheck") {
    group = "verification"
    description = "Verify checkpoint AG stable logical-retention planning and ownership-safe F3 trim delegation."
    dependsOn("phase4M5AsyncObjectWalCheck")
    dependsOn("checkPhase4M5RetentionPlannerContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-managed-ledger:check")
}

tasks.register<Exec>("checkPhase4M5RetentionRuntimeContractSurface") {
    group = "verification"
    description = "Audit the bounded retention lane, production ledger wiring, and Pulsar config mapping."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m5-retention-runtime-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("phase4M5RetentionRuntimePulsarCheck") {
    group = "verification"
    description = "Run the locked Pulsar logical-retention configuration formatting, style, and focused test."
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase4M5RetentionPlannerCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker-common:spotlessJavaCheck",
        ":pulsar-broker-common:checkstyleMain",
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusBrokerStorageConfigurationTest",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky,broker-isolated",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase4M5RetentionRuntimeCheck") {
    group = "verification"
    description = "Verify checkpoint AH bounded retention execution and production configuration composition."
    dependsOn("phase4M5RetentionPlannerCheck")
    dependsOn("checkPhase4M5RetentionRuntimeContractSurface")
    dependsOn("phase4M5RetentionRuntimePulsarCheck")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M5RetentionPolicyAdminContractSurface") {
    group = "verification"
    description = "Audit exact Pulsar retention policy projection and generation-gated admin routing."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m5-retention-policy-admin-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("phase4M5RetentionPolicyAdminPulsarCheck") {
    group = "verification"
    description = "Run locked Pulsar exact policy/admin formatting, style, and focused tests."
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase4M5RetentionRuntimeCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusTopicFeatureResolverTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusTopicFeatureValidatorTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusAdminOperationTest",
        "--tests", "org.apache.pulsar.broker.service.persistent.PersistentTopicNereusAdmissionTest",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky,broker-isolated",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase4M5RetentionPolicyAdminCheck") {
    group = "verification"
    description = "Verify checkpoint AI exact topic policy admission and loaded/unloaded logical trim routing."
    dependsOn("phase4M5RetentionRuntimeCheck")
    dependsOn("checkPhase4M5RetentionPolicyAdminContractSurface")
    dependsOn("phase4M5RetentionPolicyAdminPulsarCheck")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M5FinalContractSurface") {
    group = "verification"
    description = "Audit the complete async Object-WAL, logical-retention, ownership-cut, and coexistence surface."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m5-final-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register("phase4M5Check") {
    group = "verification"
    description = "Run the complete ordinary F4-M5 async-profile and logical-retention gate."
    dependsOn("phase4M5RetentionPolicyAdminCheck")
    dependsOn("checkPhase4M5FinalContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("phase4M5AsyncRetentionMultiBrokerPulsarCheck") {
    group = "verification"
    description = "Run the retry-disabled real two-broker async Object-WAL and logical-retention gate."
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase4M5Check")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusAsyncRetentionMultiBrokerIntegrationTest",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
        "-PtestRetryCount=0",
    )
}

tasks.register("phase4M5FinalCheck") {
    group = "verification"
    description = "Run the complete F4-M5 release gate including real two-broker async retention and failover."
    dependsOn("phase4M5Check")
    dependsOn("checkPhase4M5FinalContractSurface")
    dependsOn("phase4M5AsyncRetentionMultiBrokerPulsarCheck")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
}

tasks.register<Exec>("checkPhase4M6RegistryScaleContractSurface") {
    group = "verification"
    description = "Audit exact 16,448-stream all-shard registry pagination and cold-restart evidence."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m6-registry-scale-contract-surface.sh",
    )
}

tasks.register("phase4M6RegistryScaleCheck") {
    group = "verification"
    description = "Verify checkpoint BH scans 257 registrations in each of 64 shards across cold restarts."
    dependsOn("phase4M5FinalCheck")
    dependsOn("checkPhase4M6RegistryScaleContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-materialization:check")
}

tasks.register<Exec>("checkPhase4M6TwoBrokerWorkerContentionContractSurface") {
    group = "verification"
    description = "Audit exact two-broker/two-worker contention, compressed-read, and coexistence evidence."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m6-two-broker-worker-contention-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("phase4M6TwoBrokerWorkerContentionPulsarCheck") {
    group = "verification"
    description = "Run the retry-disabled real two-broker/two-worker materialization contention gate."
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase4M6RegistryScaleCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusMaterializationContentionMultiBrokerIntegrationTest",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
        "-PtestRetryCount=0",
    )
}

tasks.register("phase4M6TwoBrokerWorkerContentionCheck") {
    group = "verification"
    description = "Verify checkpoint BI two-process materialization contention and exact compressed Pulsar reads."
    dependsOn("phase4M6RegistryScaleCheck")
    dependsOn("checkPhase4M6TwoBrokerWorkerContentionContractSurface")
    dependsOn("phase4M6TwoBrokerWorkerContentionPulsarCheck")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-materialization:check")
}

tasks.register<Exec>("checkPhase4M6AbandonedAppendIntentContractSurface") {
    group = "verification"
    description = "Audit protected-head ordering and full-proof abandoned append-intent retirement."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m6-abandoned-append-intent-contract-surface.sh",
    )
}

tasks.register("phase4M6AbandonedAppendIntentCheck") {
    group = "verification"
    description = "Verify checkpoint BJ protected append ordering and abandoned intent GC convergence."
    dependsOn("phase4M6TwoBrokerWorkerContentionCheck")
    dependsOn("checkPhase4M6AbandonedAppendIntentContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M6ScenarioEvidenceMatrix") {
    group = "verification"
    description = "Verify all 52 Phase 4 M6 scenarios map to real annotated tests and declared owning gates."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m6-scenario-evidence-matrix.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("checkPhase4FinalDockerIsolation") {
    group = "verification"
    description = "Verify every Docker-backed release task shares one bounded Gradle build service."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-final-docker-isolation.sh")
}

tasks.register<Exec>("checkPhase4FinalPulsarCheckoutIsolation") {
    group = "verification"
    description = "Verify every nested build of the locked Pulsar checkout shares one exclusive build service."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-final-pulsar-checkout-isolation.sh")
}

tasks.register("phase4M6Check") {
    group = "verification"
    description = "Run the complete ordinary F4-M6 gate and the executable 52-scenario evidence audit."
    dependsOn("phase4M6AbandonedAppendIntentCheck")
    dependsOn("checkPhase4M6ScenarioEvidenceMatrix")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-api:check")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register("phase4M6FinalCheck") {
    group = "verification"
    description = "Run every ordinary, scale, and real-service F4-M6 acceptance gate."
    dependsOn("phase4M6Check")
    dependsOn("phase4M1FinalCheck")
    dependsOn("phase4M2FinalCheck")
    dependsOn("phase4M3FinalCheck")
    dependsOn("phase4M4FinalCheck")
    dependsOn("phase4M5FinalCheck")
    dependsOn("phase3FinalCheck")
    dependsOn("checkPhase4FinalDockerIsolation")
    dependsOn("checkPhase4FinalPulsarCheckoutIsolation")
}

tasks.register("phase4Check") {
    group = "verification"
    description = "Run every ordinary Phase 4 compaction, generation, retention, and GC gate."
    dependsOn("phase4M6Check")
}

tasks.register("phase4FinalCheck") {
    group = "verification"
    description = "Run the complete Phase 1 through Phase 4 release gate; this is the only Phase 4 completion claim."
    dependsOn("phase4Check")
    dependsOn("phase3FinalCheck")
    dependsOn("phase4M1FinalCheck")
    dependsOn("phase4M2FinalCheck")
    dependsOn("phase4M3FinalCheck")
    dependsOn("phase4M4FinalCheck")
    dependsOn("phase4M5FinalCheck")
    dependsOn("phase4M6FinalCheck")
    dependsOn("checkPhase4FinalDockerIsolation")
    dependsOn("checkPhase4FinalPulsarCheckoutIsolation")
}

tasks.register<Exec>("checkPhase9ScenarioManifest") {
    group = "verification"
    description = "Verify the F9 Markdown and JSON scenario contracts have the same 146 stable IDs."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase9-scenario-manifest.sh")
}

val autoMqCheckoutPath = providers.gradleProperty("autoMqCheckout")
    .orElse(providers.environmentVariable("NEREUS_AUTOMQ_CHECKOUT"))
    .orElse(layout.projectDirectory.dir("../automq").asFile.absolutePath)
val phase9PulsarExpectedHead =
    "50fc70fe4620febcf0fd31d97ff7d2be447af3d4"

tasks.register<Exec>("phase9PulsarSourceLockCheck") {
    group = "verification"
    description =
        "Verify the exact clean Pulsar 5.0.0-M1 Nereus fork consumed by the inherited F9 foundation."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-pulsar-source-lock.sh",
        pulsarCheckoutPath.get(),
        phase9PulsarExpectedHead,
    )
}

tasks.register<Exec>("phase9SourceLockCheck") {
    group = "verification"
    description = "Verify the F9 AutoMQ reference and current Nereus ranged-foundation source locks."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase9-source-lock.sh",
        autoMqCheckoutPath.get(),
        "1c648d84819d5c3fef2af585f02149c397584870",
        "3.9.0-SNAPSHOT",
    )
}

val kafkaBaselineCheckoutPath = providers.gradleProperty("kafkaCheckout")
    .orElse(providers.environmentVariable("NEREUS_KAFKA_CHECKOUT"))
    .orElse(layout.projectDirectory.dir("../kafka").asFile.absolutePath)

val kafkaForkCheckoutPath = providers.gradleProperty("kafkaForkCheckout")
    .orElse(providers.environmentVariable("NEREUS_KAFKA_FORK_CHECKOUT"))
    .orElse(layout.projectDirectory.dir("../../nereusstream/kafka").asFile.absolutePath)

tasks.register<Exec>("v2M1K1FocusedSourceCheck") {
    group = "verification"
    description = "Run the exact clean Kafka K1 metadata-authority focused gate; this is not M1 PASS."
    usesService(kafkaCheckoutGate)
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-v2-m1-k1-kafka.sh",
        kafkaForkCheckoutPath.get(),
    )
}

tasks.register("v2M1K1FocusedCheck") {
    group = "verification"
    description = "Verify K1 metadata authority only; no Produce/Fetch, scenario promotion, V1 prune, or M1 PASS."
    dependsOn("v2M1N1ArtifactCheck")
    dependsOn("v2M1K1FocusedSourceCheck")
    dependsOn("v2DocumentationCheck")
}

tasks.register<Exec>("v2M1P1FocusedSourceCheck") {
    group = "verification"
    description = "Run the exact clean Pulsar P1 selector/ownership focused gate; this is not M1 PASS."
    dependsOn(":nereus-metadata-oxia:p1MetadataTest")
    dependsOn(":nereus-metadata-oxia:p1OxiaIntegrationTest")
    dependsOn("publishPhase2DevelopmentArtifacts")
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-v2-m1-p1-pulsar.sh",
        pulsarCheckoutPath.get(),
        phase2DevelopmentRepository.get().asFile.absolutePath,
    )
}

tasks.register("v2M1P1FocusedCheck") {
    group = "verification"
    description = "Verify P1 selector/ownership fence only; no Produce/read runtime, scenario promotion, V1 prune, or M1 PASS."
    dependsOn("v2M1N1ArtifactCheck")
    dependsOn("v2M1P1ArtifactCheck")
    dependsOn("v2M1P1FocusedSourceCheck")
    dependsOn("v2DocumentationCheck")
}

tasks.register<Exec>("v2M1R1FocusedSourceCheck") {
    group = "verification"
    description = "Run the R1 Registry wire, authority, interlock, and source-locked real-Oxia focused gate."
    dependsOn(":nereus-domain:r1RegistryDomainTest")
    dependsOn(":nereus-metadata-spi:check")
    dependsOn(":nereus-metadata-oxia:r1MetadataTest")
    dependsOn(":nereus-metadata-oxia:r1OxiaIntegrationTest")
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m1-r1-registry.sh")
}

tasks.register("v2M1R1FocusedCheck") {
    group = "verification"
    description =
        "Verify focused R1 Registry conformance only; no allocator selection, scenario promotion, V1 prune, or M1 PASS."
    dependsOn("v2M1N1ArtifactCheck")
    dependsOn("v2M1R1FocusedSourceCheck")
    dependsOn("v2DocumentationCheck")
}

tasks.register<Exec>("v2M1G1ValidatorSourceCheck") {
    group = "verification"
    description = "Verify the production receipt/Final validator and evidence-only allocator harness boundary."
    dependsOn(":nereus-domain:g1ReceiptValidationTest")
    dependsOn(":nereus-domain:m1AllocatorHarnessTest")
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m1-g1-validator.sh")
}

tasks.register("v2M1G1ValidatorCheck") {
    group = "verification"
    description = "Verify G1 parser/Final mechanics only; no scenario promotion, V1 prune, N2/N3, or M1 PASS."
    dependsOn("v2M1G1ValidatorSourceCheck")
    dependsOn("v2M1ReceiptCapsCheck")
    dependsOn("v2DocumentationCheck")
}

tasks.register<Exec>("v2M1ActiveGraphCheck") {
    group = "verification"
    description = "Verify the final pure-V2 settings/runtime graph and V1/KoP-runtime absence."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m1-active-graph.sh")
}

tasks.register<Exec>("v2M1FastSourceCheck") {
    group = "verification"
    description = "Aggregate already executed deterministic local M1 tests and the pure-V2 graph boundary."
    dependsOn(":nereus-domain:check")
    dependsOn(":nereus-metadata-spi:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn("v2M1FoundationDependencyCheck")
    dependsOn("v2M1FoundationApiCheck")
    dependsOn("v2M1G1ValidatorSourceCheck")
    dependsOn("v2M1ActiveGraphCheck")
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m1-fast.sh")
}

tasks.register("v2M1Check") {
    group = "verification"
    description = "Run the no-Docker/no-fork M1 fast gate including the final pure-V2/V1-absence graph."
    dependsOn("v2M1FastSourceCheck")
    dependsOn("v2DocumentationCheck")
}

val oxiaClientCheckoutPath = providers.gradleProperty("oxiaClientCheckout")
    .orElse(providers.environmentVariable("NEREUS_OXIA_CLIENT_CHECKOUT"))
    .orElse(layout.projectDirectory.dir("../../nereusstream/oxia-client-java").asFile.absolutePath)
val oxiaServerCheckoutPath = providers.gradleProperty("oxiaServerCheckout")
    .orElse(providers.environmentVariable("NEREUS_OXIA_SERVER_CHECKOUT"))
    .orElse(layout.projectDirectory.dir("../../nereusstream/oxia").asFile.absolutePath)

tasks.register<Exec>("v2M1ExactSourceAggregateCheck") {
    group = "verification"
    description = "Verify the final clean exact K1/P1/Oxia/artifact/image tuple after focused suites execute."
    dependsOn("v2M1K1FocusedCheck")
    dependsOn("v2M1P1FocusedCheck")
    dependsOn("v2M1R1FocusedCheck")
    dependsOn("v2M1G1ValidatorCheck")
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-v2-m1-exact-source.sh",
        kafkaForkCheckoutPath.get(),
        pulsarCheckoutPath.get(),
        oxiaClientCheckoutPath.get(),
        oxiaServerCheckoutPath.get(),
    )
}

tasks.register("v2M1ExactSourceCheck") {
    group = "verification"
    description = "Run the trusted exact-source M1 gate; no Final aggregation or scenario promotion."
    dependsOn("v2M1ExactSourceAggregateCheck")
    dependsOn("v2DocumentationCheck")
}

val v2M1SourceTupleSha = providers.gradleProperty("v2M1SourceTupleSha")
val v2M1FastGateResultPath = providers.gradleProperty("v2M1FastGateResult")
    .orElse(layout.buildDirectory.file("v2-m1/gates/fast.json").map { it.asFile.absolutePath })
val v2M1ExactGateResultPath = providers.gradleProperty("v2M1ExactGateResult")
    .orElse(layout.buildDirectory.file("v2-m1/gates/exact-source.json").map { it.asFile.absolutePath })

tasks.register<JavaExec>("v2M1FastGateResult") {
    group = "verification"
    description = "Write the canonical PASS reference only after v2M1Check succeeds."
    dependsOn("v2M1Check", ":nereus-domain:classes")
    classpath = v2DomainMainSourceSet.runtimeClasspath
    mainClass.set("com.nereusstream.domain.receipt.M1EvidenceCli")
    doFirst {
        setArgs(listOf(
            "write-gate-result",
            "V2_M1_FAST",
            v2M1SourceTupleSha.get(),
            "PASS",
            v2M1FastGateResultPath.get(),
        ))
    }
}

tasks.register<JavaExec>("v2M1ExactSourceGateResult") {
    group = "verification"
    description = "Write the canonical PASS reference only after v2M1ExactSourceCheck succeeds."
    dependsOn("v2M1ExactSourceCheck", ":nereus-domain:classes")
    classpath = v2DomainMainSourceSet.runtimeClasspath
    mainClass.set("com.nereusstream.domain.receipt.M1EvidenceCli")
    doFirst {
        setArgs(listOf(
            "write-gate-result",
            "V2_M1_EXACT_SOURCE",
            v2M1SourceTupleSha.get(),
            "PASS",
            v2M1ExactGateResultPath.get(),
        ))
    }
}

val v2M1FinalIndexPath = providers.gradleProperty("v2M1FinalIndex")
tasks.register<JavaExec>("v2M1FinalCheck") {
    group = "verification"
    description = "Resolve one canonical Final index without rerunning Fast, Exact Source, or any referenced suite."
    dependsOn(":nereus-domain:classes")
    classpath = v2DomainMainSourceSet.runtimeClasspath
    mainClass.set("com.nereusstream.domain.receipt.M1EvidenceCli")
    doFirst {
        setArgs(listOf("validate-final", v2M1FinalIndexPath.get()))
    }
}

tasks.register<Exec>("phase9KafkaBaselineSourceLockCheck") {
    group = "verification"
    description = "Verify the clean local Apache Kafka source baseline used for the F9-M3 fork probe."
    usesService(kafkaCheckoutGate)
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase9-kafka-baseline-source-lock.sh",
        kafkaBaselineCheckoutPath.get(),
        "427b409cf440f745ad6195673d3342f6bd3974d4",
        "4.3.0-SNAPSHOT",
    )
}

tasks.register<Exec>("phase9KafkaForkDevelopmentSourceLockCheck") {
    group = "verification"
    description =
        "Verify the local organization-fork F9 branch, exact bridge/recovery/metadata-lifecycle commits, markers, and source blobs."
    usesService(kafkaCheckoutGate)
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase9-kafka-fork-development-source-lock.sh",
        kafkaForkCheckoutPath.get(),
        "76f62f3b83e882105219b6c7687dbde594a8b8a2",
        "427b409cf440f745ad6195673d3342f6bd3974d4",
        "c300006a7705c240642db6950b5a95fec982bfc5",
        "4.3.0-SNAPSHOT",
    )
}

val phase9PublishedModules = listOf(
    ":nereus-api",
    ":nereus-core",
    ":nereus-metadata-oxia",
    ":nereus-object-store",
    ":nereus-materialization",
    ":nereus-bookkeeper",
    ":nereus-kafka-adapter",
)

tasks.register<DevelopmentCoordinateVerificationTask>("publishPhase9DevelopmentArtifacts") {
    group = "verification"
    description = "Publish the exact Nereus F9 development coordinate for the Kafka fork gate."
    dependsOn(phase9PublishedModules.map { "$it:publishAllPublicationsToDevelopmentRepository" })
    actualVersion.set(version.toString())
    expectedVersion.set(phase9DevelopmentVersion)
}

val phase9DevelopmentRepository = layout.buildDirectory.dir("development-repository")
val kafkaForkGradleWrapper = file(kafkaForkCheckoutPath.get()).resolve("gradlew").absolutePath

tasks.register<Exec>("phase9M6KafkaProcessRuntime") {
    group = "verification"
    description = "Build the exact Nereus-enabled Kafka release distribution used by the provider-backed F9 gate."
    dependsOn("phase9KafkaForkDevelopmentSourceLockCheck")
    dependsOn("publishPhase9DevelopmentArtifacts")
    usesService(kafkaCheckoutGate)
    workingDir = file(kafkaForkCheckoutPath.get())
    commandLine(
        kafkaForkGradleWrapper,
        ":core:releaseTarGz",
        "-PnereusDevelopmentRepository=${phase9DevelopmentRepository.get().asFile.absolutePath}",
        "-PnereusDevelopmentVersion=$phase9DevelopmentVersion",
    )
}

tasks.register<Exec>("phase9M3KafkaForkStockCheck") {
    group = "verification"
    description =
        "Compile and test the stock Kafka ListOffsets, metadata lifecycle, and inert configuration seams with no Nereus artifact inputs."
    dependsOn("phase9KafkaForkDevelopmentSourceLockCheck")
    usesService(kafkaCheckoutGate)
    workingDir = file(kafkaForkCheckoutPath.get())
    commandLine(
        kafkaForkGradleWrapper,
        ":server:checkstyleMain",
        ":server:checkstyleTest",
        ":server:spotbugsMain",
        ":server:test",
        "--tests",
        "org.apache.kafka.server.config.NereusKafkaStorageConfigTest",
        "--tests",
        "org.apache.kafka.server.KRaftClusterTest.testCreateClusterAndRestartBrokerNode",
        ":storage:checkstyleMain",
        ":storage:spotbugsMain",
        ":core:compileScala",
        ":core:checkstyleMain",
        ":core:checkstyleTest",
        ":core:spotbugsMain",
        ":core:test",
        "--tests",
        "kafka.server.KafkaConfigTest",
        "--tests",
        "kafka.server.NereusKafkaConfigValidatorTest",
        "--tests",
        "kafka.server.storage.BrokerStorageRuntimeFactoryTest",
        "--tests",
        "kafka.cluster.PartitionTest.testLeaderEpochAwareOffsetLookup*",
        "--tests",
        "kafka.cluster.PartitionTest.testAuthoritativeAppendPreservesRequiredAcks",
        "--tests",
        "kafka.server.ReplicaManagerTest.testApplyDeltaPreparesOnlyNewLeaderAfterPartitionStatePublication",
        "--tests",
        "kafka.server.ReplicaManagerTest.testStorageAppendExecutorDefersAppendAndCallbacksUntilOwnedWorkCompletes",
        "--tests",
        "kafka.server.ReplicaManagerTest.testStorageFetchExecutorOwnsStockReadWaveAndDefersResponse",
        "--tests",
        "kafka.server.metadata.BrokerMetadataPublisherTest",
    )
}

tasks.register<Exec>("phase9M3KafkaForkBridgeCheck") {
    group = "verification"
    description = "Run the Kafka fork ListOffsets and async metadata-lifecycle tests against isolated F9 artifacts."
    dependsOn("phase9KafkaForkDevelopmentSourceLockCheck")
    dependsOn("publishPhase9DevelopmentArtifacts")
    usesService(kafkaCheckoutGate)
    workingDir = file(kafkaForkCheckoutPath.get())
    commandLine(
        kafkaForkGradleWrapper,
        ":server:checkstyleMain",
        ":server:checkstyleTest",
        ":server:spotbugsMain",
        ":server:test",
        "--tests",
        "org.apache.kafka.server.config.NereusKafkaStorageConfigTest",
        "--tests",
        "org.apache.kafka.server.KRaftClusterTest.testCreateClusterAndRestartBrokerNode",
        ":storage:checkstyleMain",
        ":storage:spotbugsMain",
        ":core:spotlessCheck",
        ":core:checkstyleMain",
        ":core:checkstyleTest",
        ":core:spotbugsMain",
        ":core:test",
        "--tests",
        "kafka.server.KafkaConfigTest",
        "--tests",
        "kafka.server.NereusKafkaConfigValidatorTest",
        "--tests",
        "kafka.server.storage.BrokerStorageRuntimeFactoryTest",
        "--tests",
        "kafka.log.nereus.*Test",
        "--tests",
        "kafka.server.nereus.*Test",
        "--tests",
        "kafka.cluster.PartitionTest.testLeaderEpochAwareOffsetLookup*",
        "--tests",
        "kafka.cluster.PartitionTest.testAuthoritativeAppendPreservesRequiredAcks",
        "--tests",
        "kafka.server.ReplicaManagerTest.testApplyDeltaPreparesOnlyNewLeaderAfterPartitionStatePublication",
        "--tests",
        "kafka.server.ReplicaManagerTest.testStorageAppendExecutorDefersAppendAndCallbacksUntilOwnedWorkCompletes",
        "--tests",
        "kafka.server.ReplicaManagerTest.testStorageFetchExecutorOwnsStockReadWaveAndDefersResponse",
        "--tests",
        "kafka.server.metadata.BrokerMetadataPublisherTest",
        "-PnereusDevelopmentRepository=${phase9DevelopmentRepository.get().asFile.absolutePath}",
        "-PnereusDevelopmentVersion=$phase9DevelopmentVersion",
    )
}

tasks.register<Exec>("phase9M5KafkaRetentionOracleCheck") {
    group = "verification"
    description =
        "Compare Nereus time, size, combined, high-watermark, and compact-only retention boundaries with stock UnifiedLog."
    dependsOn("phase9KafkaForkDevelopmentSourceLockCheck")
    dependsOn("publishPhase9DevelopmentArtifacts")
    usesService(kafkaCheckoutGate)
    workingDir = file(kafkaForkCheckoutPath.get())
    commandLine(
        kafkaForkGradleWrapper,
        ":core:spotlessCheck",
        ":core:checkstyleTest",
        ":core:test",
        "--tests",
        "kafka.log.nereus.KafkaRetentionOracleTest",
        "-PnereusDevelopmentRepository=${phase9DevelopmentRepository.get().asFile.absolutePath}",
        "-PnereusDevelopmentVersion=$phase9DevelopmentVersion",
    )
}

tasks.register<Exec>("phase9M5KafkaCompactionOracleCheck") {
    group = "verification"
    description =
        "Compare Nereus compaction survivors and exact Kafka batch metadata with stock LogCleaner."
    dependsOn("phase9KafkaForkDevelopmentSourceLockCheck")
    dependsOn("publishPhase9DevelopmentArtifacts")
    usesService(kafkaCheckoutGate)
    workingDir = file(kafkaForkCheckoutPath.get())
    commandLine(
        kafkaForkGradleWrapper,
        ":core:spotlessCheck",
        ":core:checkstyleTest",
        ":core:test",
        "--tests",
        "kafka.log.nereus.KafkaCompactionOracleTest",
        "-PnereusDevelopmentRepository=${phase9DevelopmentRepository.get().asFile.absolutePath}",
        "-PnereusDevelopmentVersion=$phase9DevelopmentVersion",
    )
}

tasks.register<Exec>("phase9M6KafkaFeatureServerCommonCheck") {
    group = "verification"
    description = "Verify the opt-in nereus.storage.version definition and stock feature-set isolation."
    dependsOn("phase9KafkaForkDevelopmentSourceLockCheck")
    usesService(kafkaCheckoutGate)
    workingDir = file(kafkaForkCheckoutPath.get())
    commandLine(
        kafkaForkGradleWrapper,
        ":server-common:test",
        "--tests",
        "org.apache.kafka.server.common.FeatureTest",
    )
}

tasks.register<Exec>("phase9M6KafkaFeatureServerCheck") {
    group = "verification"
    description = "Verify broker feature advertisement is enabled only for Nereus storage processes."
    dependsOn("phase9KafkaForkDevelopmentSourceLockCheck")
    usesService(kafkaCheckoutGate)
    workingDir = file(kafkaForkCheckoutPath.get())
    commandLine(
        kafkaForkGradleWrapper,
        ":server:test",
        "--tests",
        "org.apache.kafka.server.BrokerFeaturesTest",
    )
}

tasks.register<Exec>("phase9M6KafkaFeatureMetadataCheck") {
    group = "verification"
    description = "Verify controller feature finalization and RF/minISR/ISR/reassignment/directory enforcement."
    dependsOn("phase9KafkaForkDevelopmentSourceLockCheck")
    usesService(kafkaCheckoutGate)
    workingDir = file(kafkaForkCheckoutPath.get())
    commandLine(
        kafkaForkGradleWrapper,
        ":metadata:test",
        "--tests",
        "org.apache.kafka.controller.FeatureControlManagerTest",
        "--tests",
        "org.apache.kafka.controller.QuorumFeaturesTest",
        "--tests",
        "org.apache.kafka.controller.ConfigurationControlManagerTest",
        "--tests",
        "org.apache.kafka.controller.ReplicationControlManagerTest",
    )
}

tasks.register<Exec>("phase9M6KafkaFeatureCoreCheck") {
    group = "verification"
    description =
        "Verify dedicated-controller admission, explicit storage formatting, and feature-gated activation scheduling."
    dependsOn("phase9KafkaForkDevelopmentSourceLockCheck")
    dependsOn("publishPhase9DevelopmentArtifacts")
    usesService(kafkaCheckoutGate)
    workingDir = file(kafkaForkCheckoutPath.get())
    commandLine(
        kafkaForkGradleWrapper,
        ":core:test",
        "--tests",
        "kafka.server.NereusKafkaConfigValidatorTest",
        "--tests",
        "kafka.tools.StorageToolTest",
        "--tests",
        "kafka.server.nereus.NereusControllerStorageRuntimeTest",
        "-PnereusDevelopmentRepository=${phase9DevelopmentRepository.get().asFile.absolutePath}",
        "-PnereusDevelopmentVersion=$phase9DevelopmentVersion",
    )
}

tasks.register<Exec>("phase9KafkaForkCompatibilityCheck") {
    group = "verification"
    description =
        "Run current-fork ApiVersions, Produce, Fetch, Admin, consumer-group, and transaction compatibility suites."
    dependsOn("phase9KafkaForkDevelopmentSourceLockCheck")
    usesService(kafkaCheckoutGate)
    workingDir = file(kafkaForkCheckoutPath.get())
    commandLine(
        kafkaForkGradleWrapper,
        ":clients:test",
        "--tests",
        "org.apache.kafka.clients.NodeApiVersionsTest",
        "--tests",
        "org.apache.kafka.clients.producer.KafkaProducerTest.testOverwriteAcksAndRetriesForIdempotentProducers",
        "--tests",
        "org.apache.kafka.clients.consumer.KafkaConsumerTest.testPollReturnsRecords",
        "--tests",
        "org.apache.kafka.clients.admin.KafkaAdminClientTest.testCreateTopics",
        "--tests",
        "org.apache.kafka.clients.admin.KafkaAdminClientTest.testListOffsets",
        ":core:test",
        "--tests",
        "kafka.api.TransactionsTest.testBasicTransactions",
        "--tests",
        "kafka.api.GroupCoordinatorIntegrationTest.testGroupCoordinatorPropagatesOffsetsTopicCompressionCodec",
        "--rerun-tasks",
    )
}

tasks.register("phase9M6KafkaFeatureCheck") {
    group = "verification"
    description = "Run the deterministic F9 Kafka durable feature and controller policy gate."
    dependsOn("phase9M6KafkaFeatureServerCommonCheck")
    dependsOn("phase9M6KafkaFeatureServerCheck")
    dependsOn("phase9M6KafkaFeatureMetadataCheck")
    dependsOn("phase9M6KafkaFeatureCoreCheck")
}

tasks.register("phase9M6KafkaProcessCheck") {
    group = "verification"
    description =
        "Run real Oxia + LocalStack + BookKeeper cold-restart, broker/controller takeover, and in-flight fencing acceptance."
    dependsOn(":nereus-kafka-adapter:f9M6KafkaProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9CheckpointTrimRecoveryProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9DeleteRecordsBoundaryProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9TrimResponseLossProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9TrimProfileMatrixProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9MultiBrokerTakeoverProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9CoordinatorMigrationProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9OngoingTransactionMigrationProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9TransactionResolutionCutProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9TransactionResolutionProfileMatrixProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9MandatoryInternalTopicNtc2ProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9MandatoryInternalTopicNtc2ProfileMatrixProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9MultiControllerFailoverProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9ActivationCutFailoverProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9ActivationProofCutFailoverProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9ActivationTransportRecoveryProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9InFlightTakeoverProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9BookKeeperProfileTakeoverProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9BookKeeperInFlightTakeoverProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9BookKeeperWalOnlyProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9BookKeeperWalAsyncObjectProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9BookKeeperWalSyncObjectProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9ObjectWalAsyncObjectProcessIntegrationTest")
}

tasks.register("phase9M6KafkaBookKeeperProcessCheck") {
    group = "verification"
    description =
        "Run focused real BookKeeper three-profile cold-restart, deletion, live-takeover, and in-flight fencing gates."
    dependsOn(":nereus-kafka-adapter:f9TrimProfileMatrixProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9BookKeeperProfileTakeoverProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9BookKeeperInFlightTakeoverProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9BookKeeperWalOnlyProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9BookKeeperWalAsyncObjectProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9BookKeeperWalSyncObjectProcessIntegrationTest")
}

tasks.register("phase9M6KafkaObjectAsyncProcessCheck") {
    group = "verification"
    description = "Run the focused async Object-WAL native Kafka cold-restart process gate."
    dependsOn(":nereus-kafka-adapter:f9ObjectWalAsyncObjectProcessIntegrationTest")
}

tasks.register("phase9M3KafkaForkCheck") {
    group = "verification"
    description = "Run the partial F9-M3 Nereus adapter and local Kafka-fork bridge gates."
    dependsOn("phase9M3ProviderCheck")
    dependsOn("phase9M3KafkaForkStockCheck")
    dependsOn("phase9M3KafkaForkBridgeCheck")
}

tasks.register("phase9M1ApiCheck") {
    group = "verification"
    description = "Run the in-progress F9-M1 public ranged-entry API slice and scenario-manifest gate."
    dependsOn("checkPhase9ScenarioManifest")
    dependsOn(":nereus-api:test")
}

tasks.register("phase9M1Check") {
    group = "verification"
    description = "Run the complete deterministic F9-M1 ranged-entry and NCP2/NTC2 foundation gate."
    dependsOn("phase9M1ApiCheck")
    dependsOn("phase9SourceLockCheck")
    dependsOn(":nereus-core:rangedEntryTest")
    dependsOn(":nereus-object-store:rangedFormatTest")
    dependsOn(":nereus-bookkeeper:rangedBookKeeperIntegrationTest")
    dependsOn(":nereus-materialization:test")
}

tasks.register("phase9M1FinalCheck") {
    group = "verification"
    description = "Run F9-M1 plus inherited protocol-neutral regression and real-object-store gates."
    dependsOn("phase9M1Check")
    dependsOn("phase9PulsarSourceLockCheck")
    dependsOn("phase1FinalCheck")
    dependsOn("phase15FinalCheck")
    dependsOn(":nereus-managed-ledger:test")
    dependsOn(":nereus-materialization:test")
    dependsOn(":nereus-bookkeeper:test")
    dependsOn(":nereus-object-store:rangedFormatS3IntegrationTest")
}

tasks.register("phase9M2Check") {
    group = "verification"
    description = "Run deterministic F9-M2 authority, binding, NKC1 publication, and recovery gates."
    dependsOn("phase9M1Check")
    dependsOn(":nereus-metadata-oxia:f9MetadataTest")
    dependsOn(":nereus-object-store:kafkaCheckpointTest")
    dependsOn(":nereus-kafka-adapter:f9M2Test")
}

tasks.register("phase9M2FinalCheck") {
    group = "verification"
    description = "Run F9-M2 plus inherited and real Oxia/Object-store checkpoint acceptance gates."
    dependsOn("phase9M2Check")
    dependsOn("phase9M1FinalCheck")
    dependsOn(":nereus-metadata-oxia:f9OxiaIntegrationTest")
    dependsOn(":nereus-object-store:kafkaCheckpointS3IntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9M2IntegrationTest")
}

tasks.register("phase9M3CodecCheck") {
    group = "verification"
    description = "Run the partial F9-M3 Kafka baseline and byte-exact adapter codec gate."
    dependsOn("phase9M2Check")
    dependsOn("phase9KafkaBaselineSourceLockCheck")
    dependsOn(":nereus-kafka-adapter:f9M3CodecTest")
}

tasks.register("phase9M3ProviderCheck") {
    group = "verification"
    description =
        "Run the partial F9-M3 provider-backed Object-WAL, live two-broker takeover, and BookKeeper-WAL-only gates."
    dependsOn("phase9M3CodecCheck")
    dependsOn(":nereus-kafka-adapter:f9M3ProviderIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9MultiBrokerTakeoverProviderIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9BookKeeperWalOnlyProviderIntegrationTest")
}

tasks.register("phase9M4ProducerStateCheck") {
    group = "verification"
    description = "Run the partial F9-M4 canonical state checkpoint gate."
    dependsOn("phase9M3ProviderCheck")
    dependsOn(":nereus-kafka-adapter:f9ProducerStatePropertyTest")
}

tasks.register("phase9M5RetentionCheck") {
    group = "verification"
    description = "Run the partial F9-M5 retention, DeleteRecords, and checkpoint-before-trim gate."
    dependsOn("phase9M4ProducerStateCheck")
    dependsOn("phase9M5KafkaRetentionOracleCheck")
    dependsOn(":nereus-kafka-adapter:f9RetentionTest")
    dependsOn(":nereus-kafka-adapter:f9BookKeeperLedgerDeletionProviderIntegrationTest")
}

tasks.register("phase9M5CompactionCoreCheck") {
    group = "verification"
    description = "Run the partial F9-M5 authoritative-source, Kafka compaction, and NTC2 gate."
    dependsOn("phase9M5RetentionCheck")
    dependsOn("phase9M5KafkaCompactionOracleCheck")
    dependsOn(":nereus-materialization:f9ExactSourceSetTest")
    dependsOn(":nereus-kafka-adapter:f9CompactionPropertyTest")
}

tasks.register("phase9M6ActivationMetadataCheck") {
    group = "verification"
    description = "Run partial F9-M6 activation metadata, broker publication, admission, and real-Oxia gates."
    dependsOn("phase9M2Check")
    dependsOn(":nereus-kafka-adapter:f9ActivationTest")
    dependsOn(":nereus-metadata-oxia:f9ActivationOxiaIntegrationTest")
}

tasks.register("phase9M6CheckpointQuarantineCheck") {
    group = "verification"
    description =
        "Verify durable F9 checkpoint quarantine metadata, fallback ordering, restart skip, and runtime composition."
    dependsOn("checkPhase9ScenarioManifest")
    dependsOn(":nereus-metadata-oxia:f9MetadataTest")
    dependsOn(":nereus-metadata-oxia:f9OxiaIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9CheckpointQuarantineTest")
}

tasks.register("phase9ScaleCheck") {
    group = "verification"
    description =
        "Run the F9-M7 binding, partition, IO, ranged-count, and materialization scale boundaries."
    dependsOn("checkPhase9ScenarioManifest")
    dependsOn("phase9SourceLockCheck")
    dependsOn(":nereus-api:f9RangedCountLimitTest")
    dependsOn(":nereus-metadata-oxia:f9BindingScaleOxiaIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9PartitionScaleTest")
    dependsOn(":nereus-kafka-adapter:f9IoConcurrencyStressTest")
    dependsOn(":nereus-kafka-adapter:f9MaterializationScaleTest")
}

tasks.register("phase9ChaosCheck") {
    group = "verification"
    description =
        "Run the implemented F9-M7 leader-churn and Oxia/Object/BookKeeper response-loss matrix."
    dependsOn("checkPhase9ScenarioManifest")
    dependsOn("phase9SourceLockCheck")
    dependsOn(":nereus-kafka-adapter:f9LeaderChurnChaosProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9ActivationTransportRecoveryProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9TrimResponseLossProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9TrimProfileMatrixProcessIntegrationTest")
}

tasks.register("phase9CompatibilityCheck") {
    group = "verification"
    description =
        "Run the F9-M7 multi-version client process contract and current Kafka fork compatibility suites."
    dependsOn("checkPhase9ScenarioManifest")
    dependsOn("phase9SourceLockCheck")
    dependsOn("phase9KafkaForkCompatibilityCheck")
    dependsOn(":nereus-kafka-adapter:f9ClientCompatibilityProcessIntegrationTest")
}

tasks.register("phase9PerformanceCheck") {
    group = "verification"
    description =
        "Run the F9-M7 observation-only five-profile performance and fresh-cache recovery evidence gate."
    dependsOn("checkPhase9ScenarioManifest")
    dependsOn("phase9SourceLockCheck")
    dependsOn(":nereus-kafka-adapter:f9PerformanceProfileProcessIntegrationTest")
}

tasks.register("phase9M3FinalCheck") {
    group = "verification"
    description =
        "Run the complete F9-M3 product, provider, Kafka-fork, and release-process Produce/Fetch gate."
    dependsOn("phase9M2FinalCheck")
    dependsOn("phase9M3KafkaForkCheck")
    dependsOn(":nereus-kafka-adapter:f9M6KafkaProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9BookKeeperWalOnlyProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9BookKeeperWalAsyncObjectProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9BookKeeperWalSyncObjectProcessIntegrationTest")
    dependsOn(":nereus-kafka-adapter:f9ObjectWalAsyncObjectProcessIntegrationTest")
}

tasks.register("phase9M4FinalCheck") {
    group = "verification"
    description =
        "Run the complete F9-M4 checkpoint, producer-state, transaction, coordinator, and profile gate."
    dependsOn("phase9M3FinalCheck")
    dependsOn("phase9M4ProducerStateCheck")
    dependsOn("phase9M6KafkaProcessCheck")
    dependsOn("phase9KafkaForkCompatibilityCheck")
}

tasks.register("phase9M5FinalCheck") {
    group = "verification"
    description =
        "Run the complete F9-M5 retention, compaction, provider response-loss, and restart gate."
    dependsOn("phase9M4FinalCheck")
    dependsOn("phase9M5CompactionCoreCheck")
    dependsOn("phase9ChaosCheck")
}

tasks.register("phase9M6FinalCheck") {
    group = "verification"
    description =
        "Run the complete F9-M6 activation, configuration, quarantine, fork-feature, and process gate."
    dependsOn("phase9M5FinalCheck")
    dependsOn("phase9M6ActivationMetadataCheck")
    dependsOn("phase9M6CheckpointQuarantineCheck")
    dependsOn("phase9M6KafkaFeatureCheck")
    dependsOn("phase9M6KafkaProcessCheck")
}

tasks.register("phase9M7Check") {
    group = "verification"
    description =
        "Run every F9-M7 scale, chaos, compatibility, and observation-only performance slice."
    dependsOn("phase9ScaleCheck")
    dependsOn("phase9ChaosCheck")
    dependsOn("phase9CompatibilityCheck")
    dependsOn("phase9PerformanceCheck")
}

val phase9PreEvidence =
    layout.buildDirectory.file("f9-final-evidence/pre-evidence.json")
val phase9FinalEvidence =
    layout.buildDirectory.file("f9-final-evidence/final-report.json")

val phase9PrepareFinalEvidence =
    tasks.register<Exec>("phase9PrepareFinalEvidence") {
        group = "verification"
        description =
            "Verify every F9 predecessor ran fresh and write the exact source/artifact input for KF-SCL-010."
        dependsOn("phase9M1FinalCheck")
        dependsOn("phase9M2FinalCheck")
        dependsOn("phase9M3FinalCheck")
        dependsOn("phase9M4FinalCheck")
        dependsOn("phase9M5FinalCheck")
        dependsOn("phase9M6FinalCheck")
        dependsOn("phase9M7Check")
        workingDir = layout.projectDirectory.asFile
        commandLine(
            "bash",
            "scripts/prepare-phase9-final-evidence.sh",
            layout.projectDirectory.asFile.absolutePath,
            autoMqCheckoutPath.get(),
            kafkaForkCheckoutPath.get(),
            pulsarCheckoutPath.get(),
            phase9PreEvidence.get().asFile.absolutePath,
            gradle.startParameter.isRerunTasks.toString(),
        )
        inputs.file("scripts/prepare-phase9-final-evidence.sh")
        inputs.file("docs/v1/phase-9-kafka-native-storage/f9-scenarios.json")
        inputs.file("docs/v1/phase-9-kafka-native-storage/08-scenario-evidence-matrix.md")
        outputs.file(phase9PreEvidence)
        outputs.upToDateWhen { false }
    }

gradle.projectsEvaluated {
    project(":nereus-kafka-adapter")
        .tasks
        .named("f9EvidenceAggregatorTest") {
            dependsOn(phase9PrepareFinalEvidence)
        }
}

val phase9FinalEvidenceReport =
    tasks.register<Exec>("phase9FinalEvidenceReport") {
        group = "verification"
        description =
            "Verify 146 unique KF-SCL-010 JUnit mappings and write the deterministic final F9 evidence report."
        dependsOn(":nereus-kafka-adapter:f9EvidenceAggregatorTest")
        workingDir = layout.projectDirectory.asFile
        commandLine(
            "bash",
            "scripts/finalize-phase9-evidence.sh",
            layout.projectDirectory.asFile.absolutePath,
            phase9PreEvidence.get().asFile.absolutePath,
            layout.projectDirectory
                .dir(
                    "nereus-kafka-adapter/build/test-results/"
                            + "f9EvidenceAggregatorTest",
                )
                .asFile
                .absolutePath,
            phase9FinalEvidence.get().asFile.absolutePath,
        )
        inputs.file("scripts/finalize-phase9-evidence.sh")
        inputs.file(phase9PreEvidence)
        inputs.dir(
            layout.projectDirectory.dir(
                "nereus-kafka-adapter/build/test-results/"
                        + "f9EvidenceAggregatorTest",
            ),
        )
        outputs.file(phase9FinalEvidence)
        outputs.upToDateWhen { false }
    }

tasks.register("phase9M7FinalCheck") {
    group = "verification"
    description =
        "Run every F9-M7 slice and the 146-scenario exact-source evidence aggregator."
    dependsOn("phase9M7Check")
    dependsOn(phase9FinalEvidenceReport)
}

tasks.register("phase9FinalCheck") {
    group = "verification"
    description =
        "Run the clean F9-M1 through F9-M7 release aggregate; this is the only F9 completion claim."
    dependsOn("phase9M1FinalCheck")
    dependsOn("phase9M2FinalCheck")
    dependsOn("phase9M3FinalCheck")
    dependsOn("phase9M4FinalCheck")
    dependsOn("phase9M5FinalCheck")
    dependsOn("phase9M6FinalCheck")
    dependsOn("phase9M7FinalCheck")
}
