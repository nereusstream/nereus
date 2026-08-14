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
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.jvm.tasks.Jar

abstract class DockerIntegrationGateService : BuildService<BuildServiceParameters.None>

abstract class PulsarCheckoutGateService : BuildService<BuildServiceParameters.None>

abstract class KafkaCheckoutGateService : BuildService<BuildServiceParameters.None>

abstract class V2ProjectClasspathVerificationTask : DefaultTask() {
    @get:org.gradle.api.tasks.Classpath
    abstract val compileClasspath: org.gradle.api.file.ConfigurableFileCollection

    @get:org.gradle.api.tasks.Classpath
    abstract val allowedArtifacts: org.gradle.api.file.ConfigurableFileCollection

    @get:Input
    abstract val boundaryName: Property<String>

    @TaskAction
    fun verifyDependencies() {
        val actual = compileClasspath.files.mapTo(sortedSetOf()) { it.canonicalFile }
        val allowed = allowedArtifacts.files.mapTo(sortedSetOf()) { it.canonicalFile }
        check(actual == allowed) {
            "${boundaryName.get()} production classpath differs: actual=$actual allowed=$allowed"
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

        check("<dependency>" !in domainPom.get().asFile.readText()) {
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
                    if (read < 0) break
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
require(
    Regex(
        "[0-9]+\\.[0-9]+\\.[0-9]+" +
            "(?:-SNAPSHOT|-n1\\.[0-9a-f]{40}|-p1\\.[0-9a-f]{40}|-m2\\.[0-9a-f]{40})?",
    ).matches(configuredNereusVersion),
) {
    "nereusVersion must be X.Y.Z, X.Y.Z-SNAPSHOT, or source-qualified X.Y.Z-n1/p1/m2.<40-lowercase-hex>"
}
if ("-p1." in configuredNereusVersion) {
    val allowedP1ArtifactTasks = setOf("clean", "p1ArtifactJar", "p1ArtifactSourcesJar")
    check(
        gradle.startParameter.taskNames.isNotEmpty() &&
            gradle.startParameter.taskNames.all { it.substringAfterLast(':') in allowedP1ArtifactTasks },
    ) {
        "The source-qualified P1 coordinate is restricted to the filtered metadata capability artifact tasks"
    }
}
if ("-m2." in configuredNereusVersion) {
    val allowedM2Modules = setOf(
        "nereus-storage-api",
        "nereus-storage-bookkeeper",
        "nereus-kafka-bookkeeper",
    )
    val allowedM2ArtifactTasks = setOf(
        "clean",
        "jar",
        "sourcesJar",
        "generatePomFileForMavenJavaPublication",
        "generateMetadataFileForMavenJavaPublication",
    )
    check(
        gradle.startParameter.taskNames.isNotEmpty() && gradle.startParameter.taskNames.all { requested ->
            val components = requested.split(':').filter(String::isNotEmpty)
            components.size == 2 && components[0] in allowedM2Modules && components[1] in allowedM2ArtifactTasks
        },
    ) {
        "The source-qualified M2 coordinate is restricted to the three Kafka K0 production artifacts"
    }
}
version = configuredNereusVersion

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

subprojects {
    group = rootProject.group
    version = rootProject.version
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
            if (project.name == "nereus-metadata-oxia") {
                target(
                    "src/main/java/com/nereusstream/metadata/oxia/v2/**/*.java",
                    "src/test/java/com/nereusstream/metadata/oxia/v2/**/*.java",
                    "src/oxiaIntegrationTest/java/com/nereusstream/metadata/oxia/v2/**/*.java",
                )
            } else {
                target("src/**/*.java")
            }
            palantirJavaFormat(palantirJavaFormatVersion)
            importOrder("\\#|")
            removeUnusedImports()
            trimTrailingWhitespace()
            endWithNewline()
        }
    }

    extensions.configure<JavaPluginExtension>("java") {
        toolchain.languageVersion.set(JavaLanguageVersion.of(javaLanguageVersion))
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
                    description.set("Nereus V2 module ${project.name}")
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

project(":nereus-metadata-oxia").tasks.matching {
    it.name in setOf("p1OxiaIntegrationTest", "r1OxiaIntegrationTest")
}.configureEach {
    usesService(dockerIntegrationGate)
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

val v2DomainJar = project(":nereus-domain").tasks.named<Jar>("jar").flatMap { it.archiveFile }
val v2DomainSourcesJar = project(":nereus-domain").tasks.named<Jar>("sourcesJar").flatMap { it.archiveFile }
val v2MetadataSpiJar = project(":nereus-metadata-spi").tasks.named<Jar>("jar").flatMap { it.archiveFile }
val v2MetadataSpiSourcesJar = project(":nereus-metadata-spi").tasks.named<Jar>("sourcesJar").flatMap { it.archiveFile }

val v2DomainDependencyBoundary = project(":nereus-domain").tasks.register<
    V2ProjectClasspathVerificationTask
>("v2DependencyBoundary") {
    group = "verification"
    description = "Verify that the V2 domain production classpath is empty."
    dependsOn("compileJava")
    compileClasspath.from(project.configurations.named("compileClasspath"))
    boundaryName.set("nereus-domain")
}

val v2MetadataSpiDependencyBoundary = project(":nereus-metadata-spi").tasks.register<
    V2ProjectClasspathVerificationTask
>("v2DependencyBoundary") {
    group = "verification"
    description = "Verify that the V2 metadata SPI production classpath contains only domain."
    dependsOn("compileJava")
    compileClasspath.from(project.configurations.named("compileClasspath"))
    allowedArtifacts.from(rootProject.file("nereus-domain/build/classes/java/main"))
    boundaryName.set("nereus-metadata-spi")
}

tasks.register("v2M1FoundationDependencyCheck") {
    group = "verification"
    description = "Aggregate the project-local M1.1a-A domain/SPI dependency boundaries."
    dependsOn(v2DomainDependencyBoundary, v2MetadataSpiDependencyBoundary)
}

tasks.register<Exec>("v2M1FoundationApiCheck") {
    group = "verification"
    description = "Verify the M1.1a-A import, SPI capability, and no-final-gate boundary."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m1-foundation.sh")
}

tasks.register<V2FoundationArtifactVerificationTask>("v2M1FoundationArtifactCheck") {
    group = "verification"
    description = "Build and SHA-256 qualify the M1.1a-A JAR, source JAR, and Maven POM artifacts."
    dependsOn(":nereus-domain:jar", ":nereus-domain:sourcesJar")
    dependsOn(":nereus-domain:generatePomFileForMavenJavaPublication")
    dependsOn(":nereus-metadata-spi:jar", ":nereus-metadata-spi:sourcesJar")
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
    dependsOn(":nereus-domain:check", ":nereus-metadata-spi:check")
    dependsOn("v2M1FoundationDependencyCheck", "v2M1FoundationApiCheck", "v2M1FoundationArtifactCheck")
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
    dependsOn("v2M1N1ArtifactSourceCheck", "v2DocumentationCheck")
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
    dependsOn("v2M1N1ArtifactCheck", "v2M1P1ArtifactSourceCheck", "v2DocumentationCheck")
}

val v2OxiaDependencyVerification = project(":nereus-metadata-oxia").tasks.register<
    V2OxiaDependencyVerificationTask
>("v2OxiaDependencyVerification") {
    group = "verification"
    description = "Verify metadata-oxia resolves only the immutable O1 client/client-api bundle."
    dependsOn("compileJava")
    runtimeClasspath.from(project.configurations.named("runtimeClasspath"))
    lockedClientArtifacts.from(
        rootProject.file(
            "gradle/locked-artifacts/oxia-client-java/091a42c2780d92da56e9ec1f02ce1c3d988adc16/" +
                "m2/io/github/oxia-db/oxia-client/0.9.4/oxia-client-0.9.4.jar",
        ),
        rootProject.file(
            "gradle/locked-artifacts/oxia-client-java/091a42c2780d92da56e9ec1f02ce1c3d988adc16/" +
                "m2/io/github/oxia-db/oxia-client-api/0.9.4/oxia-client-api-0.9.4.jar",
        ),
    )
}

tasks.register("v2M1OxiaDependencyCheck") {
    group = "verification"
    description = "Aggregate the project-local locked O1 dependency verification."
    dependsOn(v2OxiaDependencyVerification)
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
    dependsOn("v2M1OxiaDependencyCheck", "v2M1OxiaScaffoldSourceCheck", "v2M1FoundationCheck")
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
    dependsOn(":nereus-domain:check", "v2M1Nta1ReadinessSourceCheck", "v2M1FoundationCheck")
    dependsOn("v2DocumentationCheck")
}

tasks.register<Exec>("v2M1RegistryCapacitySourceCheck") {
    group = "verification"
    description = "Verify deterministic R0 Registry capacity evidence, focused counts, and production absence."
    dependsOn(":nereus-domain:test")
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m1-registry-capacity.sh")
}

tasks.register("v2M1RegistryCapacityCheck") {
    group = "verification"
    description = "Verify M1.1c-R0 readiness only; no R1 authority/real Oxia/allocator/scenario/M1 PASS."
    dependsOn(":nereus-domain:check", "v2M1RegistryCapacitySourceCheck", "v2DocumentationCheck")
}

val v2ReceiptCapsFocusedTest = project(":nereus-domain").tasks.register<Test>(
    "v2ReceiptCapsFocusedTest",
) {
    group = "verification"
    description = "Run only the deterministic M1-2 receipt/parser cap boundary suite."
    dependsOn("testClasses")
    testClassesDirs = project.extensions.getByType<SourceSetContainer>().named("test").get().output.classesDirs
    classpath = project.extensions.getByType<SourceSetContainer>().named("test").get().runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.domain.receipt.ReceiptV1CapacityEvidenceTest")
    }
    workingDir = project.projectDir
    binaryResultsDirectory.set(rootProject.layout.buildDirectory.dir("test-results/v2-m1-receipt-caps/binary"))
    reports.junitXml.outputLocation.set(rootProject.layout.buildDirectory.dir("test-results/v2-m1-receipt-caps"))
    reports.html.outputLocation.set(rootProject.layout.buildDirectory.dir("reports/tests/v2-m1-receipt-caps"))
    outputs.dir(project.layout.buildDirectory.dir("reports/v2-m1-receipt-caps"))
}

tasks.register("v2M1ReceiptCapsFocusedTest") {
    group = "verification"
    description = "Aggregate the project-local M1-2 receipt/parser cap boundary suite."
    dependsOn(v2ReceiptCapsFocusedTest)
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
    dependsOn("v2M1ReceiptCapsSourceCheck", "v2DocumentationCheck")
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
    dependsOn(":nereus-domain:check", ":nereus-metadata-oxia:check")
    dependsOn("v2M1Nta1CodecSourceCheck", "v2M1Nta1ReadinessCheck", "v2M1OxiaScaffoldCheck")
    dependsOn("v2DocumentationCheck")
}

val kafkaForkCheckoutPath = providers.gradleProperty("kafkaForkCheckout")
    .orElse(providers.environmentVariable("NEREUS_KAFKA_FORK_CHECKOUT"))
    .orElse(layout.projectDirectory.dir("../../nereusstream/kafka").asFile.absolutePath)
val pulsarCheckoutPath = providers.gradleProperty("pulsarCheckout")
    .orElse(providers.environmentVariable("NEREUS_PULSAR_CHECKOUT"))
    .orElse(layout.projectDirectory.dir("../../nereusstream/pulsar").asFile.absolutePath)
tasks.register<Exec>("v2M1K1FocusedSourceCheck") {
    group = "verification"
    description = "Run the exact clean Kafka K1 metadata-authority focused gate; this is not M1 PASS."
    usesService(kafkaCheckoutGate)
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m1-k1-kafka.sh", kafkaForkCheckoutPath.get())
}

tasks.register("v2M1K1FocusedCheck") {
    group = "verification"
    description = "Verify K1 metadata authority only; no Produce/Fetch, scenario promotion, V1 prune, or M1 PASS."
    dependsOn("v2M1N1ArtifactCheck", "v2M1K1FocusedSourceCheck", "v2DocumentationCheck")
}

tasks.register<Exec>("v2M1P1FocusedSourceCheck") {
    group = "verification"
    description = "Run the exact clean Pulsar P1 selector/ownership focused gate; this is not M1 PASS."
    dependsOn(":nereus-metadata-oxia:p1MetadataTest", ":nereus-metadata-oxia:p1OxiaIntegrationTest")
    usesService(dockerIntegrationGate)
    usesService(pulsarCheckoutGate)
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-v2-m1-p1-pulsar.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register("v2M1P1FocusedCheck") {
    group = "verification"
    description = "Verify P1 selector/ownership fence only; no Produce/read runtime, scenario promotion, V1 prune, or M1 PASS."
    dependsOn("v2M1N1ArtifactCheck", "v2M1P1ArtifactCheck", "v2M1P1FocusedSourceCheck")
    dependsOn("v2DocumentationCheck")
}

tasks.register<Exec>("v2M1R1FocusedSourceCheck") {
    group = "verification"
    description = "Run the R1 Registry wire, authority, interlock, and source-locked real-Oxia focused gate."
    dependsOn(":nereus-domain:r1RegistryDomainTest", ":nereus-metadata-spi:check")
    dependsOn(":nereus-metadata-oxia:r1MetadataTest", ":nereus-metadata-oxia:r1OxiaIntegrationTest")
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m1-r1-registry.sh")
}

tasks.register("v2M1R1FocusedCheck") {
    group = "verification"
    description = "Verify focused R1 Registry conformance only; no allocator selection, scenario promotion, V1 prune, or M1 PASS."
    dependsOn("v2M1N1ArtifactCheck", "v2M1R1FocusedSourceCheck", "v2DocumentationCheck")
}

tasks.register<Exec>("v2M1G1ValidatorSourceCheck") {
    group = "verification"
    description = "Verify the production receipt/Final validator and evidence-only allocator harness boundary."
    dependsOn(":nereus-domain:g1ReceiptValidationTest", ":nereus-domain:m1AllocatorHarnessTest")
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m1-g1-validator.sh")
}

tasks.register("v2M1G1ValidatorCheck") {
    group = "verification"
    description = "Verify G1 parser/Final mechanics only; no scenario promotion, V1 prune, N2/N3, or M1 PASS."
    dependsOn(
        "v2M1G1ValidatorSourceCheck",
        "v2M1ReceiptCapsCheck",
        "v2M1EvidenceFreshnessBoundaryTest",
        "v2DocumentationCheck",
    )
}

tasks.register<Exec>("v2M1ActiveGraphCheck") {
    group = "verification"
    description = "Verify the final pure-V2 settings/runtime graph and V1/KoP-runtime absence."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m1-active-graph.sh")
}

tasks.register<Exec>("v2M1FastSourceCheck") {
    group = "verification"
    description = "Aggregate executed deterministic local M1 tests and the pure-V2 graph boundary."
    dependsOn(":nereus-domain:check", ":nereus-metadata-spi:check", ":nereus-metadata-oxia:check")
    dependsOn("v2M1FoundationDependencyCheck", "v2M1FoundationApiCheck")
    dependsOn("v2M1G1ValidatorSourceCheck", "v2M1ActiveGraphCheck")
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m1-fast.sh")
}

tasks.register("v2M1Check") {
    group = "verification"
    description = "Run the no-Docker/no-fork M1 fast gate including the final pure-V2/V1-absence graph."
    dependsOn("v2M1FastSourceCheck", "v2DocumentationCheck")
}

val v2M2KafkaK0StorageApiTest = project(":nereus-storage-api").tasks.named<Test>("test")
val v2M2KafkaK0BookKeeperTest = project(":nereus-storage-bookkeeper").tasks.named<Test>("test")
val v2M2KafkaK0KafkaModuleTest = project(":nereus-kafka-bookkeeper").tasks.named<Test>("test")

tasks.register<Exec>("v2M2KafkaK0ModuleSourceCheck") {
    group = "verification"
    description = "Verify the K0-M module graph, exact immutable N1 linkage, and filtered M2 publication boundary."
    dependsOn(v2M2KafkaK0StorageApiTest, v2M2KafkaK0BookKeeperTest, v2M2KafkaK0KafkaModuleTest)
    listOf("nereus-storage-api", "nereus-storage-bookkeeper", "nereus-kafka-bookkeeper").forEach { module ->
        dependsOn(
            project(":$module").tasks.named("jar"),
            project(":$module").tasks.named("sourcesJar"),
            project(":$module").tasks.named("generatePomFileForMavenJavaPublication"),
            project(":$module").tasks.named("generateMetadataFileForMavenJavaPublication"),
        )
    }
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m2-kafka-k0-module.sh")
}

tasks.register("v2M2KafkaK0ModuleCheck") {
    group = "verification"
    description = "Run the non-promotable, non-zero K0-M production module gate; no provider, codec, or M2 PASS."
    dependsOn("v2M2KafkaK0ModuleSourceCheck", "v2DocumentationCheck")
}

val v2M2KafkaK0ProviderApiTest = project(":nereus-storage-api").tasks.named<Test>("test")
val v2M2KafkaK0ProviderLifecycleTest = project(":nereus-storage-bookkeeper").tasks.named<Test>("test")

tasks.register<Exec>("v2M2KafkaK0ProviderSourceCheck") {
    group = "verification"
    description = "Verify the K0-P provider/session/capability/outcome/buffer-ownership production contracts."
    dependsOn(v2M2KafkaK0ProviderApiTest, v2M2KafkaK0ProviderLifecycleTest)
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m2-kafka-k0-provider.sh")
}

tasks.register("v2M2KafkaK0ProviderCheck") {
    group = "verification"
    description = "Run the non-promotable, non-zero K0-P provider contract gate; no real provider or M2 PASS."
    dependsOn("v2M2KafkaK0ProviderSourceCheck", "v2DocumentationCheck")
}

val v2M2KafkaK0WireTest = project(":nereus-kafka-bookkeeper").tasks.named<Test>("test")

tasks.register<Exec>("v2M2KafkaK0WireSourceCheck") {
    group = "verification"
    description = "Verify the closed NBKE2 v1 codec, machine projection, immutable goldens, and corruption matrix."
    dependsOn(v2M2KafkaK0WireTest)
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m2-kafka-k0-wire.sh")
}

tasks.register("v2M2KafkaK0WireCheck") {
    group = "verification"
    description = "Run the non-promotable, non-zero K0-W wire gate; no writer, runtime, K0-E receipt, or M2 PASS."
    dependsOn("v2M2KafkaK0WireSourceCheck", "v2DocumentationCheck")
}

val v2M2KafkaK0NumericTest = project(":nereus-kafka-bookkeeper").tasks.named<Test>("test")

tasks.register<Exec>("v2M2KafkaK0NumericSourceCheck") {
    group = "verification"
    description = "Verify checked DATA admission and the mandatory three-dimensional recovery envelope."
    dependsOn(v2M2KafkaK0NumericTest)
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m2-kafka-k0-numeric.sh")
}

tasks.register("v2M2KafkaK0NumericCheck") {
    group = "verification"
    description = "Run the non-promotable, non-zero K0-N numeric gate; no offset allocation, K0-E receipt, or M2 PASS."
    dependsOn("v2M2KafkaK0NumericSourceCheck", "v2DocumentationCheck")
}

val v2M2KafkaK0EvidenceBookKeeperTest = project(":nereus-storage-bookkeeper").tasks.named<Test>("test")
val v2M2KafkaK0EvidenceReceiptTest = project(":nereus-kafka-bookkeeper").tasks.named<Test>("test")

tasks.register<Exec>("v2M2KafkaK0EvidenceSourceCheck") {
    group = "verification"
    description = "Verify the exact Kafka/BookKeeper/image/config source tuple and closed non-promotable receipt parser."
    dependsOn(v2M2KafkaK0EvidenceBookKeeperTest, v2M2KafkaK0EvidenceReceiptTest)
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m2-kafka-k0-evidence.sh")
}

tasks.register("v2M2KafkaK0EvidenceCheck") {
    group = "verification"
    description = "Run the non-zero K0-E source/parser gate; no canonical aggregate receipt or M2 PASS is claimed."
    dependsOn("v2M2KafkaK0EvidenceSourceCheck", "v2DocumentationCheck")
}

tasks.register("v2M2KafkaInputsReceiptCheck") {
    group = "verification"
    description = "Parse one canonical Kafka Inputs receipt through the production closed-model validator."
    dependsOn(":nereus-kafka-bookkeeper:v2M2KafkaInputsReceiptCheck")
}

val v2M2KafkaInputsReceiptPath = providers.gradleProperty("v2M2KafkaInputsReceipt")

tasks.register<Exec>("v2M2KafkaInputsLiveSourceCheck") {
    group = "verification"
    description = "Bind the canonical Kafka Inputs receipt to the live clean evidence-only source descendant."
    dependsOn(
        "v2M2KafkaK0ModuleCheck",
        "v2M2KafkaK0ProviderCheck",
        "v2M2KafkaK0WireCheck",
        "v2M2KafkaK0NumericCheck",
        "v2M2KafkaK0EvidenceCheck",
        "v2M2KafkaInputsReceiptCheck",
    )
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m2-kafka-inputs-source.sh", v2M2KafkaInputsReceiptPath.get())
}

tasks.register("v2M2KafkaInputsCheck") {
    group = "verification"
    description = "Aggregate the five non-empty Kafka K0 input gates; non-promotable and no writer/runtime/M2 PASS."
    dependsOn("v2M2KafkaInputsLiveSourceCheck", "v2DocumentationCheck")
}

val v2M2KafkaK1Test = project(":nereus-kafka-bookkeeper").tasks.named<Test>("test")

tasks.register<Exec>("v2M2KafkaK1SourceCheck") {
    group = "verification"
    description = "Verify the pure K1 frontier/snapshot/commit-slot/fenced-publication implementation and tests."
    dependsOn(v2M2KafkaK1Test)
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m2-kafka-k1.sh")
}

tasks.register("v2M2KafkaK1Check") {
    group = "verification"
    description = "Run the non-zero non-promotable K1 publication gate; no writer, real BookKeeper, or M2 PASS."
    dependsOn("v2M2KafkaK1SourceCheck", "v2DocumentationCheck")
}

val v2M2KafkaK2Test = project(":nereus-kafka-bookkeeper").tasks.named<Test>("test")

tasks.register<Exec>("v2M2KafkaK2SourceCheck") {
    group = "verification"
    description = "Verify exact Kafka 4.3 assigned-RecordBatch facts, K2 cross-checks, and unchanged NBKE2 bytes."
    dependsOn(v2M2KafkaK2Test)
    usesService(kafkaCheckoutGate)
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m2-kafka-k2.sh", kafkaForkCheckoutPath.get())
}

tasks.register("v2M2KafkaK2Check") {
    group = "verification"
    description = "Run the non-zero K2 native-batch adapter gate; no appender, runtime activation, or M2 PASS."
    dependsOn("v2M2KafkaK2SourceCheck", "v2DocumentationCheck")
}

val v2M2KafkaK3Test = project(":nereus-kafka-bookkeeper").tasks.named<Test>("test")

tasks.register<Exec>("v2M2KafkaK3SourceCheck") {
    group = "verification"
    description = "Verify the K3 leader-epoch run lifecycle, one entry sequencer, fake provider, and retire cuts."
    dependsOn(v2M2KafkaK3Test)
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m2-kafka-k3.sh")
}

tasks.register("v2M2KafkaK3Check") {
    group = "verification"
    description = "Run the non-zero K3 fake-provider lifecycle gate; no DATA pipeline, real BK, or M2 PASS."
    dependsOn("v2M2KafkaK3SourceCheck", "v2DocumentationCheck")
}

val v2M2KafkaK4Test = project(":nereus-kafka-bookkeeper").tasks.named<Test>("test")

tasks.register<Exec>("v2M2KafkaK4SourceCheck") {
    group = "verification"
    description = "Verify K4 capacity-first admission, speculative deltas, bounded DATA I/O, and ordered completion."
    dependsOn(v2M2KafkaK4Test)
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m2-kafka-k4.sh")
}

tasks.register("v2M2KafkaK4Check") {
    group = "verification"
    description = "Run the non-zero K4 bounded-pipeline gate; no K5 publication, ACK, real BK, or M2 PASS."
    dependsOn("v2M2KafkaK4SourceCheck", "v2DocumentationCheck")
}

val v2M2KafkaK5Test = project(":nereus-kafka-bookkeeper").tasks.named<Test>("test")

tasks.register<Exec>("v2M2KafkaK5SourceCheck") {
    group = "verification"
    description = "Verify K5 producer, transaction, speculative, locator, and coherent publication cuts."
    dependsOn(v2M2KafkaK5Test)
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m2-kafka-k5.sh")
}

tasks.register("v2M2KafkaK5Check") {
    group = "verification"
    description = "Run the non-zero K5 coherent-publication gate; no ACK, HW/LSO advance, real BK, or M2 PASS."
    dependsOn("v2M2KafkaK5SourceCheck", "v2M2KafkaK4Check", "v2DocumentationCheck")
}

val v2M2KafkaK6Test = project(":nereus-kafka-bookkeeper").tasks.named<Test>("test")

tasks.register<Exec>("v2M2KafkaK6SourceCheck") {
    group = "verification"
    description = "Verify K6 packed lookup, targeted exact-entry reads, snapshot bounds, and disposable cursors."
    dependsOn(v2M2KafkaK6Test)
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m2-kafka-k6.sh")
}

tasks.register("v2M2KafkaK6Check") {
    group = "verification"
    description = "Run the non-zero K6 targeted-reader gate; no runtime, recovery, real BK, or M2 PASS."
    dependsOn("v2M2KafkaK6SourceCheck", "v2M2KafkaK5Check", "v2DocumentationCheck")
}

val v2M2KafkaK7Test = project(":nereus-kafka-bookkeeper").tasks.named<Test>("test")

tasks.register<Exec>("v2M2KafkaK7SourceCheck") {
    group = "verification"
    description = "Verify K7 checkpoint codec/store, response-loss reconciliation, and bounded takeover recovery."
    dependsOn(v2M2KafkaK7Test)
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m2-kafka-k7.sh")
}

tasks.register("v2M2KafkaK7Check") {
    group = "verification"
    description = "Run the non-zero K7 recovery gate; no HW recovery, Kafka runtime, real BK, or M2 PASS."
    dependsOn("v2M2KafkaK7SourceCheck", "v2M2KafkaK6Check", "v2DocumentationCheck")
}

val v2M2KafkaK8Test = project(":nereus-kafka-bookkeeper").tasks.named<Test>("test")

tasks.register<Exec>("v2M2KafkaK8SourceCheck") {
    group = "verification"
    description = "Verify K8 KRD1/KRO1, journal recovery, Observed/Applied, source, lag, and election cuts."
    dependsOn(v2M2KafkaK8Test)
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m2-kafka-k8.sh")
}

tasks.register("v2M2KafkaK8Check") {
    group = "verification"
    description = "Run the non-zero K8 replica kernel gate; no Kafka wire/runtime, durable disk adapter, ISR/HW, or M2 PASS."
    dependsOn("v2M2KafkaK8SourceCheck", "v2M2KafkaK7Check", "v2DocumentationCheck")
}

tasks.register<Exec>("v2M2KafkaK9PlanSourceCheck") {
    group = "verification"
    description = "Verify the predeclared K9 exact-image 10k/100k scale plan before any result is admitted."
    dependsOn(":nereus-kafka-bookkeeper:bookKeeperScaleClasses")
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m2-kafka-k9-plan.sh")
}

tasks.register("v2M2KafkaK9PlanCheck") {
    group = "verification"
    description = "Compile and verify the K9 plan and harness; this gate contains no scale result or scenario PASS."
    dependsOn("v2M2KafkaK9PlanSourceCheck", "v2M2KafkaK8Check", "v2DocumentationCheck")
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
    dependsOn("v2M1K1FocusedCheck", "v2M1P1FocusedCheck", "v2M1R1FocusedCheck", "v2M1G1ValidatorCheck")
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
    dependsOn("v2M1ExactSourceAggregateCheck", "v2DocumentationCheck")
}

val v2M1SourceTupleSha = providers.gradleProperty("v2M1SourceTupleSha")
val v2M1FastGateResultPath = providers.gradleProperty("v2M1FastGateResult")
    .orElse(layout.buildDirectory.file("v2-m1/gates/fast.json").map { it.asFile.absolutePath })
val v2M1ExactGateResultPath = providers.gradleProperty("v2M1ExactGateResult")
    .orElse(layout.buildDirectory.file("v2-m1/gates/exact-source.json").map { it.asFile.absolutePath })

tasks.register<JavaExec>("v2M1FastGateResult") {
    group = "verification"
    description = "Write the canonical PASS reference only after v2M1Check succeeds."
    dependsOn("v2M1Check", ":nereus-domain:jar")
    classpath = files(v2DomainJar)
    mainClass.set("com.nereusstream.domain.receipt.M1EvidenceCli")
    setArgs(
        listOf(
            "write-gate-result",
            "V2_M1_FAST",
            v2M1SourceTupleSha.get(),
            "PASS",
            v2M1FastGateResultPath.get(),
        ),
    )
}

tasks.register<JavaExec>("v2M1ExactSourceGateResult") {
    group = "verification"
    description = "Write the canonical PASS reference only after v2M1ExactSourceCheck succeeds."
    dependsOn("v2M1ExactSourceCheck", ":nereus-domain:jar")
    classpath = files(v2DomainJar)
    mainClass.set("com.nereusstream.domain.receipt.M1EvidenceCli")
    setArgs(
        listOf(
            "write-gate-result",
            "V2_M1_EXACT_SOURCE",
            v2M1SourceTupleSha.get(),
            "PASS",
            v2M1ExactGateResultPath.get(),
        ),
    )
}

val v2M1FinalIndexPath = providers.gradleProperty("v2M1FinalIndex")
tasks.register<Exec>("v2M1EvidenceFreshnessBoundaryTest") {
    group = "verification"
    description = "Run deterministic checkout-to-Final freshness boundary tests."
    workingDir = layout.projectDirectory.asFile
    commandLine("python3", "scripts/check-v2-m1-evidence-freshness-tests.py")
}

tasks.register<Exec>("v2M1N3EvidencePublisherBoundaryTest") {
    group = "verification"
    description = "Run deterministic JUnit-to-canonical-N3 evidence publisher boundary tests."
    workingDir = layout.projectDirectory.asFile
    commandLine("python3", "scripts/check-v2-m1-n3-evidence-publisher.py")
}

tasks.register<Exec>("v2M1EvidenceFreshnessCheck") {
    group = "verification"
    description = "Require Final evidence to bind this clean checkout through evidence-only descendant commits."
    dependsOn("v2M1EvidenceFreshnessBoundaryTest", "v2M1N3EvidencePublisherBoundaryTest")
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m1-evidence-freshness.sh", v2M1FinalIndexPath.get())
}

tasks.register<JavaExec>("v2M1FinalCheck") {
    group = "verification"
    description = "Resolve one canonical Final index without rerunning Fast, Exact Source, or any referenced suite."
    dependsOn(":nereus-domain:jar", "v2M1EvidenceFreshnessCheck")
    classpath = files(v2DomainJar)
    mainClass.set("com.nereusstream.domain.receipt.M1EvidenceCli")
    setArgs(listOf("validate-final", v2M1FinalIndexPath.get()))
}

tasks.named("check") {
    dependsOn("v2M1Check")
    dependsOn("v2M2KafkaK0ModuleCheck")
    dependsOn("v2M2KafkaK0ProviderCheck")
    dependsOn("v2M2KafkaK0WireCheck")
    dependsOn("v2M2KafkaK0NumericCheck")
    dependsOn("v2M2KafkaK0EvidenceCheck")
    dependsOn("v2M2KafkaK1Check")
}
