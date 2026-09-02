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

abstract class M3NestedGradleGateService : BuildService<BuildServiceParameters.None>

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
            "(?:-SNAPSHOT|-n1\\.[0-9a-f]{40}|-p1\\.[0-9a-f]{40}|-m2\\.[0-9a-f]{40}|-m3\\.[0-9a-f]{40})?",
    ).matches(configuredNereusVersion),
) {
    "nereusVersion must be X.Y.Z, X.Y.Z-SNAPSHOT, or source-qualified X.Y.Z-n1/p1/m2/m3.<40-lowercase-hex>"
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
if ("-m3." in configuredNereusVersion) {
    val allowedM3Modules = setOf(
        "nereus-bom",
        "nereus-domain",
        "nereus-metadata-spi",
        "nereus-metadata-oxia",
        "nereus-storage-api",
        "nereus-storage-bookkeeper",
        "nereus-storage-object",
        "nereus-storage-object-s3",
        "nereus-storage-object-vault",
        "nereus-kafka-bookkeeper",
        "nereus-pulsar-offload",
    )
    val allowedM3ArtifactTasks = setOf(
        "clean",
        "jar",
        "sourcesJar",
        "generatePomFileForMavenJavaPublication",
        "generateMetadataFileForMavenJavaPublication",
        "publishMavenJavaPublicationToDevelopmentRepository",
    )
    check(
        gradle.startParameter.taskNames.isNotEmpty() && gradle.startParameter.taskNames.all { requested ->
            val components = requested.split(':').filter(String::isNotEmpty)
            components.size == 2 && components[0] in allowedM3Modules && components[1] in allowedM3ArtifactTasks
        },
    ) {
        "The source-qualified M3 coordinate is restricted to the complete M3 production artifact closure"
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
val m3NestedGradleGate = gradle.sharedServices.registerIfAbsent(
    "nereusM3NestedGradleGate",
    M3NestedGradleGateService::class,
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
                url = rootProject.providers.gradleProperty("developmentRepository")
                    .map { rootProject.file(it).toURI() }
                    .getOrElse(rootProject.layout.buildDirectory.dir("development-repository").get().asFile.toURI())
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

val v2M2KafkaInputsReceiptPath =
    providers.gradleProperty("v2M2KafkaInputsReceipt")
        .orElse("docs/v2/evidence/v2-m2/kafka/k0-inputs/kafka-inputs.json")

tasks.register<JavaExec>("v2M2KafkaInputsReceiptCheck") {
    group = "verification"
    description = "Parse one canonical Kafka Inputs receipt through the production closed-model validator."
    dependsOn(":nereus-kafka-bookkeeper:classes")
    classpath = files(layout.projectDirectory.dir("nereus-kafka-bookkeeper/build/classes/java/main"))
    workingDir = layout.projectDirectory.asFile
    mainClass.set("com.nereusstream.kafka.bookkeeper.evidence.KafkaM2InputsReceiptCli")
    setArgs(listOf("validate", v2M2KafkaInputsReceiptPath.get()))
}

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

val v2M2KafkaK9DefaultsTest = project(":nereus-kafka-bookkeeper").tasks.named<Test>("test")

tasks.register<Exec>("v2M2KafkaK9DefaultsSourceCheck") {
    group = "verification"
    description = "Verify the complete K9 selected defaults, adapters, hierarchy, and independent projection."
    dependsOn(v2M2KafkaK9DefaultsTest)
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m2-kafka-k9-defaults.sh")
}

tasks.register("v2M2KafkaK9DefaultsCheck") {
    group = "verification"
    description = "Run the K9 selected-defaults gate; current-source real scale receipt and scenario PASS remain pending."
    dependsOn("v2M2KafkaK9DefaultsSourceCheck", "v2M2KafkaK9PlanCheck", "v2DocumentationCheck")
}

tasks.register<Exec>("v2M2KafkaK9EvidenceSourceCheck") {
    group = "verification"
    description = "Verify the current-source K9 real-fault, 10k/100k, artifact, and selected-default receipt."
    dependsOn(
        ":nereus-storage-bookkeeper:test",
        ":nereus-storage-bookkeeper:jar",
        ":nereus-storage-bookkeeper:sourcesJar",
        ":nereus-kafka-bookkeeper:test",
        ":nereus-kafka-bookkeeper:jar",
        ":nereus-kafka-bookkeeper:sourcesJar",
    )
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m2-kafka-k9-evidence.sh")
}

tasks.register("v2M2KafkaK9Check") {
    group = "verification"
    description = "Run the complete Kafka K9 evidence gate; K10 scenario promotion and Kafka Final remain separate."
    dependsOn(
        "v2M2KafkaK9EvidenceSourceCheck",
        "v2M2KafkaK9DefaultsCheck",
        "v2M2KafkaK2Check",
        "v2M2KafkaK3Check",
        "v2DocumentationCheck",
    )
}

val v2M2KafkaK10PolicyTest = project(":nereus-kafka-bookkeeper").tasks.named<Test>("test")

tasks.register<Exec>("v2M2KafkaK10PolicySourceCheck") {
    group = "verification"
    description = "Verify K10 production Final mechanics and the exact ten-scenario policy without publishing evidence."
    dependsOn(v2M2KafkaK10PolicyTest)
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m2-kafka-k10-policy.sh")
}

tasks.register("v2M2KafkaK10PolicyCheck") {
    group = "verification"
    description = "Run the K10 readiness gate; no scenario receipt, Kafka Final, or global M2 PASS is claimed."
    dependsOn("v2M2KafkaK10PolicySourceCheck", "v2M2KafkaK9PlanCheck", "v2DocumentationCheck")
}

val v2M2KafkaFinalReceiptPath =
    layout.projectDirectory.file("docs/v2/evidence/v2-m2/kafka/k10/kafka-final.json")

tasks.register<JavaExec>("v2M2KafkaFinalReceiptSourceCheck") {
    group = "verification"
    description = "Resolve the canonical Kafka M2 Final receipt through the production fail-closed resolver."
    dependsOn(":nereus-kafka-bookkeeper:classes")
    classpath = files(layout.projectDirectory.dir("nereus-kafka-bookkeeper/build/classes/java/main"))
    workingDir = layout.projectDirectory.asFile
    mainClass.set("com.nereusstream.kafka.bookkeeper.evidence.KafkaM2FinalReceiptCli")
    setArgs(
        listOf(
            "validate",
            layout.projectDirectory.asFile.absolutePath,
            v2M2KafkaFinalReceiptPath.asFile.absolutePath,
        ),
    )
}

tasks.register<Exec>("v2M2KafkaFinalEvidenceSourceCheck") {
    group = "verification"
    description = "Verify K10 current-source named results, attachments, freshness, and scenario publication."
    dependsOn(":nereus-kafka-bookkeeper:test", "v2M2KafkaFinalReceiptSourceCheck")
    workingDir = layout.projectDirectory.asFile
    commandLine("python3", "scripts/check-v2-m2-kafka-final-evidence.py")
}

tasks.register("v2M2KafkaFinalCheck") {
    group = "verification"
    description = "Run the Kafka-only M2 Final aggregate; Pulsar M2 and global v2M2Check remain separate."
    dependsOn(
        "v2M2KafkaFinalEvidenceSourceCheck",
        "v2M2KafkaK10PolicyCheck",
        "v2M2KafkaK9Check",
        "v2DocumentationCheck",
    )
}

tasks.register<Exec>("v2M2PulsarP0SourceCheck") {
    group = "verification"
    description = "Verify the non-promotable Pulsar M2-P0 attempt/key/provider/candidate input closure."
    dependsOn(":nereus-pulsar-offload:test")
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m2-pulsar-p0.sh")
}

tasks.register("v2M2PulsarP0Check") {
    group = "verification"
    description = "Run Pulsar M2-P0 inputs only; wire, runtime, evidence, scenarios, and M2 PASS remain pending."
    dependsOn("v2M2PulsarP0SourceCheck", "v2DocumentationCheck")
}

tasks.register<Exec>("v2M2PulsarP1SourceCheck") {
    group = "verification"
    description = "Verify canonical block-local NPD1/NPB1 bytes and corruption rejection without selecting defaults."
    dependsOn(":nereus-pulsar-offload:test")
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m2-pulsar-p1.sh")
}

tasks.register("v2M2PulsarP1Check") {
    group = "verification"
    description = "Run Pulsar M2-P1 codec only; NPO1, native integration, evidence, and M2 PASS remain pending."
    dependsOn("v2M2PulsarP1SourceCheck", "v2M2PulsarP0Check", "v2DocumentationCheck")
}

tasks.register<Exec>("v2M2PulsarP2SourceCheck") {
    group = "verification"
    description = "Verify canonical four-section NPO1 root bytes, bounds, coverage, and self-digest rejection."
    dependsOn(":nereus-pulsar-offload:test")
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m2-pulsar-p2.sh")
}

tasks.register("v2M2PulsarP2Check") {
    group = "verification"
    description = "Run Pulsar M2-P2 root codec only; publication, native integration, evidence, and M2 PASS remain pending."
    dependsOn("v2M2PulsarP2SourceCheck", "v2M2PulsarP1Check", "v2DocumentationCheck")
}

tasks.register<Exec>("v2M2PulsarP3SourceCheck") {
    group = "verification"
    description = "Verify data-before-root publication, response-loss proof, actual-reader cut, and root-first cleanup."
    dependsOn(":nereus-pulsar-offload:test")
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m2-pulsar-p3.sh")
}

tasks.register("v2M2PulsarP3Check") {
    group = "verification"
    description = "Run Pulsar M2-P3 publication only; native integration, selected defaults, evidence, and M2 PASS remain pending."
    dependsOn("v2M2PulsarP3SourceCheck", "v2M2PulsarP2Check", "v2DocumentationCheck")
}

tasks.register<Exec>("v2M2PulsarP4ObjectSourceCheck") {
    group = "verification"
    description = "Verify bounded NPO1 open, immutable data proof, targeted NPD1 reads, and streaming full revalidation."
    dependsOn(":nereus-pulsar-offload:test")
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m2-pulsar-p4-object.sh")
}

tasks.register("v2M2PulsarP4ObjectCheck") {
    group = "verification"
    description = "Run the Pulsar M2-P4 Object child only; dual-source pins, native integration, evidence, and M2 PASS remain pending."
    dependsOn("v2M2PulsarP4ObjectSourceCheck", "v2M2PulsarP3Check", "v2DocumentationCheck")
}

tasks.register<Exec>("v2M2PulsarP4DualSourceCheck") {
    group = "verification"
    description = "Verify whole-range fallback, exact-version pins, bounded drain, final revalidation, and irreversible BK deletion."
    dependsOn(":nereus-pulsar-offload:test")
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m2-pulsar-p4-dual.sh")
}

tasks.register("v2M2PulsarP4Check") {
    group = "verification"
    description = "Run Pulsar M2-P4 Object plus dual-source/delete semantics; native integration, evidence, and M2 PASS remain pending."
    dependsOn("v2M2PulsarP4DualSourceCheck", "v2M2PulsarP4ObjectCheck", "v2DocumentationCheck")
}

tasks.register<Exec>("v2M2PulsarP5NativeForkTest") {
    group = "verification"
    description = "Run the exact Pulsar-fork dual-source and source-deletion tests for the P5 native binding."
    mustRunAfter(":nereus-pulsar-offload:test")
    usesService(pulsarCheckoutGate)
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        file(pulsarCheckoutPath.get()).resolve("gradlew").absolutePath,
        ":managed-ledger:test",
        "--tests",
        "org.apache.bookkeeper.mledger.impl.DualSourceReadHandleTest",
        "--tests",
        "org.apache.bookkeeper.mledger.impl.OffloadLedgerDeleteTest",
        "--no-daemon",
    )
}

tasks.register<Exec>("v2M2PulsarP5SourceCheck") {
    group = "verification"
    description = "Verify the exact clean native fork plus bounded Nereus SourceSafeLedgerOffloader adapter."
    dependsOn(":nereus-pulsar-offload:test", "v2M2PulsarP5NativeForkTest")
    usesService(pulsarCheckoutGate)
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m2-pulsar-p5.sh", pulsarCheckoutPath.get())
}

tasks.register("v2M2PulsarP5Check") {
    group = "verification"
    description = "Run Pulsar M2-P5 exact-source provider integration; P6 evidence, Pulsar Final, and M2 PASS remain pending."
    dependsOn("v2M2PulsarP5SourceCheck", "v2M2PulsarP4Check", "v2DocumentationCheck")
}

tasks.register<Exec>("v2M2PulsarP6SourceCheck") {
    group = "verification"
    description = "Validate the source-bound P6 provider, candidate, native, and selected-policy receipts."
    dependsOn(":nereus-pulsar-offload:test", ":nereus-pulsar-offload:p6ProviderTest")
    usesService(pulsarCheckoutGate)
    workingDir = layout.projectDirectory.asFile
    commandLine("python3", "scripts/check-v2-m2-pulsar-p6.py", pulsarCheckoutPath.get())
}

tasks.register("v2M2PulsarP6Check") {
    group = "verification"
    description = "Run Pulsar M2-P6 provider/block evidence; Pulsar Final and global M2 PASS remain pending."
    dependsOn("v2M2PulsarP6SourceCheck", "v2M2PulsarP5Check", "v2DocumentationCheck")
}

val v2M2PulsarFinalPolicyTest = project(":nereus-pulsar-offload").tasks.named<Test>("test")

tasks.register<Exec>("v2M2PulsarFinalPolicySourceCheck") {
    group = "verification"
    description = "Verify the production Pulsar Final receipt mechanics and exact eleven-scenario policy."
    dependsOn(v2M2PulsarFinalPolicyTest)
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m2-pulsar-final-policy.sh")
}

tasks.register("v2M2PulsarFinalPolicyCheck") {
    group = "verification"
    description = "Run Pulsar Final readiness only; no scenario receipt, Pulsar Final, or global M2 PASS is claimed."
    dependsOn("v2M2PulsarFinalPolicySourceCheck", "v2DocumentationCheck")
}

val v2M2PulsarFinalReceiptPath =
    layout.projectDirectory.file("docs/v2/evidence/v2-m2/pulsar/final/pulsar-final.json")

tasks.register<JavaExec>("v2M2PulsarFinalReceiptSourceCheck") {
    group = "verification"
    description = "Resolve the canonical Pulsar M2 Final receipt through the production fail-closed resolver."
    dependsOn(":nereus-pulsar-offload:classes")
    classpath = files(layout.projectDirectory.dir("nereus-pulsar-offload/build/classes/java/main"))
    workingDir = layout.projectDirectory.asFile
    mainClass.set("com.nereusstream.pulsar.offload.evidence.PulsarM2FinalReceiptCli")
    setArgs(
        listOf(
            "validate",
            layout.projectDirectory.asFile.absolutePath,
            v2M2PulsarFinalReceiptPath.asFile.absolutePath,
        ),
    )
}

tasks.register<Exec>("v2M2PulsarFinalEvidenceSourceCheck") {
    group = "verification"
    description = "Verify Pulsar Final named results, attachments, source freshness, and scenario publication."
    dependsOn(":nereus-pulsar-offload:test", "v2M2PulsarFinalReceiptSourceCheck")
    usesService(pulsarCheckoutGate)
    workingDir = layout.projectDirectory.asFile
    commandLine("python3", "scripts/check-v2-m2-pulsar-final-evidence.py", pulsarCheckoutPath.get())
}

tasks.register("v2M2PulsarFinalCheck") {
    group = "verification"
    description = "Run the Pulsar-only M2 Final aggregate; broker activation, M8, and global M2 remain separate."
    dependsOn(
        "v2M2PulsarFinalEvidenceSourceCheck",
        "v2M2PulsarFinalPolicyCheck",
        "v2M2PulsarP6Check",
        "v2M2KafkaFinalCheck",
        "v2DocumentationCheck",
    )
}

tasks.register<Exec>("v2M2FinalSourceCheck") {
    group = "verification"
    description = "Verify all M2 prerequisites, child roots, exact 21-scenario union, and downstream boundaries."
    dependsOn(
        "v2M2KafkaInputsCheck",
        "v2M2KafkaK1Check",
        "v2M2KafkaFinalCheck",
        "v2M2PulsarFinalCheck",
    )
    workingDir = layout.projectDirectory.asFile
    commandLine("python3", "scripts/check-v2-m2-final.py")
}

tasks.register("v2M2Check") {
    group = "verification"
    description = "Run the global current-source V2 M2 aggregate; M3 Object WAL, M6 activation, and M8 parity remain separate."
    dependsOn("v2M2FinalSourceCheck", "v2DocumentationCheck")
}

tasks.register<Exec>("v2M3HistoricalM2FinalSourceCheck") {
    group = "verification"
    description = "Verify the immutable historical M2 Final input without relabelling it as current-source evidence."
    workingDir = layout.projectDirectory.asFile
    commandLine("python3", "scripts/check-v2-m3-inputs.py")
}

tasks.register("v2M3InputsCheck") {
    group = "verification"
    description = "Verify immutable M2 history and the accepted M3 implementation inputs before any M3 evidence run."
    dependsOn("v2M3HistoricalM2FinalSourceCheck", "v2DocumentationCheck")
}

tasks.register<Exec>("v2M3Nwg1WireSourceCheck") {
    group = "verification"
    description = "Verify the production NWG1 projection, sealed six-vector corpus, 114-row TSV, and exact bytes."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m3-nwg1-wire-source.sh")
    usesService(m3NestedGradleGate)
}

tasks.register<Exec>("v2M3Nwg1MutationCheck") {
    group = "verification"
    description = "Execute all 84 authored NWG1 mutations and 240 production verification paths."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m3-nwg1-mutation.sh")
    usesService(m3NestedGradleGate)
}

tasks.register<Exec>("v2M3ObjectWalStateTraceCheck") {
    group = "verification"
    description = "Execute and verify the closed 50-trace, 21-outcome Object-WAL state kernel."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m3-object-wal-state-trace.sh")
    usesService(m3NestedGradleGate)
}

tasks.register("v2M3Nwg1WireCheck") {
    group = "verification"
    description = "Run the non-promotable exact NWG1 A/B wire aggregate."
    dependsOn("v2M3Nwg1WireSourceCheck", "v2M3Nwg1MutationCheck", "v2DocumentationCheck")
}

val v2M3PulsarEvidenceWorktree = providers.gradleProperty("v2M3PulsarEvidenceWorktree")
    .orElse(providers.environmentVariable("NEREUS_M3_PULSAR_EVIDENCE_WORKTREE"))
    .orElse(providers.gradleProperty("pulsarCheckout"))
    .orElse(providers.environmentVariable("NEREUS_PULSAR_CHECKOUT"))
    .orElse(layout.projectDirectory.dir("../../nereusstream/pulsar-worktrees/nereus-v2-m3").asFile.absolutePath)

tasks.register<Exec>("v2M3ModuleApiSourceCheck") {
    group = "verification"
    description = "Run the serialized clean M3 module/API publication, JUnit/style, and external-consumer closure gate."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-v2-m3-module-api.sh", v2M3PulsarEvidenceWorktree.get())
    usesService(m3NestedGradleGate)
}

tasks.register("v2M3ModuleApiCheck") {
    group = "verification"
    description = "Run the exact-source M3 module/API closure gate."
    dependsOn("v2M3ModuleApiSourceCheck", "v2DocumentationCheck")
}

val oxiaClientCheckoutPath = providers.gradleProperty("oxiaClientCheckout")
    .orElse(providers.environmentVariable("NEREUS_OXIA_CLIENT_CHECKOUT"))
    .orElse(layout.projectDirectory.dir("../../nereusstream/oxia-client-java").asFile.absolutePath)
val oxiaServerCheckoutPath = providers.gradleProperty("oxiaServerCheckout")
    .orElse(providers.environmentVariable("NEREUS_OXIA_SERVER_CHECKOUT"))
    .orElse(layout.projectDirectory.dir("../../nereusstream/oxia").asFile.absolutePath)

val v2M3TestedCommit = providers.gradleProperty("v2M3TestedCommit")
val v2M3KafkaEvidenceWorktree = providers.gradleProperty("v2M3KafkaEvidenceWorktree")
    .orElse(providers.environmentVariable("NEREUS_M3_KAFKA_EVIDENCE_WORKTREE"))
    .orElse(layout.projectDirectory.dir("../../nereusstream/kafka-worktrees/nereus-v2-m3").asFile.absolutePath)
val v2M3OxiaServerEvidenceWorktree = providers.gradleProperty("v2M3OxiaServerEvidenceWorktree")
    .orElse(providers.environmentVariable("NEREUS_M3_OXIA_SERVER_EVIDENCE_WORKTREE"))
    .orElse(layout.projectDirectory.dir("../../nereusstream/oxia-worktrees/nereus-v2-m3").asFile.absolutePath)
val v2M3OxiaClientEvidenceWorktree = providers.gradleProperty("v2M3OxiaClientEvidenceWorktree")
    .orElse(providers.environmentVariable("NEREUS_M3_OXIA_CLIENT_EVIDENCE_WORKTREE"))
    .orElse(
        layout.projectDirectory.dir("../../nereusstream/oxia-client-java-worktrees/nereus-v2-m3")
            .asFile.absolutePath,
    )

val v2M3GovernanceTaskProviders = linkedMapOf(
    "v2M3ModuleApiContractTest" to "scripts/check-v2-m3-module-api-tests.py",
    "v2M3AllocatorProtocolContractTest" to "scripts/check-v2-m3-allocator-protocol-tests.py",
    "v2M3AllocatorV5CheckerContractTest" to "scripts/check-v2-m3-allocator-v5-tests.py",
    "v2M3InputsContractTest" to "scripts/check-v2-m3-inputs-tests.py",
    "v2M3M2RegressionContractTest" to "scripts/check-v2-m3-m2-regression-tests.py",
    "v2M3M2RegressionPublisherContractTest" to "scripts/publish-v2-m3-m2-regression-tests.py",
    "v2M3M2RegressionRunnerContractTest" to "scripts/run-v2-m3-m2-regression-tests.py",
    "v2M3ChildCheckerContractTest" to "scripts/check-v2-m3-child-tests.py",
    "v2M3ChildPublisherContractTest" to "scripts/publish-v2-m3-child-tests.py",
    "v2M3FinalCheckerContractTest" to "scripts/check-v2-m3-final-tests.py",
    "v2M3FinalPublisherContractTest" to "scripts/publish-v2-m3-final-tests.py",
).map { (taskName, script) ->
    tasks.register<Exec>(taskName) {
        group = "verification"
        description = "Run the non-empty fail-closed M3 governance contract: $script"
        workingDir = layout.projectDirectory.asFile
        commandLine("python3", script)
    }
}

tasks.register("v2M3GovernanceCheck") {
    group = "verification"
    description =
        "Run every M3 governance contract; no source, Provider, allocator, or Final result is synthesized."
    dependsOn(v2M3GovernanceTaskProviders)
}

tasks.register("v2M3OrdinarySourceCheck") {
    group = "verification"
    description = "Run every ordinary M3 source/JUnit/style gate with non-empty zero-failure/error/skip reports."
    dependsOn(
        "v2M3Nwg1WireSourceCheck",
        "v2M3Nwg1MutationCheck",
        "v2M3ObjectWalStateTraceCheck",
        "v2M3ModuleApiSourceCheck",
        "v2M3GovernanceCheck",
    )
}

tasks.register("v2M3SourceCheck") {
    group = "verification"
    description = "Run immutable inputs plus complete ordinary current-source M3 gates without claiming real evidence."
    dependsOn("v2M3InputsCheck", "v2M3OrdinarySourceCheck", "v2DocumentationCheck")
}

val v2M3M2RegressionOutputDirectory = providers.gradleProperty("v2M3M2RegressionOutputDirectory")

tasks.register<Exec>("v2M3M2RegressionSourceCheck") {
    group = "verification"
    description = "Execute all 25 trusted current-source M2 gates into one explicit fresh external directory."
    workingDir = layout.projectDirectory.asFile
    usesService(m3NestedGradleGate)
    doFirst {
        commandLine(
            "bash",
            "scripts/run-v2-m3-m2-regression.sh",
            "--execute",
            "--repo-root",
            layout.projectDirectory.asFile.absolutePath,
            "--tested-commit",
            v2M3TestedCommit.get(),
            "--kafka-worktree",
            v2M3KafkaEvidenceWorktree.get(),
            "--pulsar-worktree",
            v2M3PulsarEvidenceWorktree.get(),
            "--output-dir",
            v2M3M2RegressionOutputDirectory.get(),
        )
    }
}

val v2M3ProviderEvidenceOutput = providers.gradleProperty("v2M3ProviderEvidenceOutput")
    .map { file(it) }
    .orElse(
        project(":nereus-storage-object-s3").layout.buildDirectory
            .file("m3-evidence/c1-minio-provider.json")
            .map { it.asFile },
    )
val v2M3KmsEvidenceOutput = providers.gradleProperty("v2M3KmsEvidenceOutput")
    .map { file(it) }
    .orElse(
        project(":nereus-storage-object-vault").layout.buildDirectory
            .file("m3-evidence/vault-transit-kms.json")
            .map { it.asFile },
    )

tasks.register("v2M3RealProviderCheck") {
    group = "verification"
    description = "Run and seal exact-digest MinIO C1 evidence with one non-skipped Provider testcase."
    notCompatibleWithConfigurationCache("formal evidence checks an exclusive external receipt after execution")
    dependsOn(":nereus-storage-object-s3:realProviderTest")
    doLast {
        val output = v2M3ProviderEvidenceOutput.get()
        check(output.isFile && output.length() > 0) { "sealed real Provider evidence is absent or empty: $output" }
    }
}

tasks.register("v2M3RealKmsCheck") {
    group = "verification"
    description = "Run and seal exact-digest Vault Transit evidence with one non-skipped KMS testcase."
    notCompatibleWithConfigurationCache("formal evidence checks an exclusive external receipt after execution")
    dependsOn(":nereus-storage-object-vault:realKmsTest")
    doLast {
        val output = v2M3KmsEvidenceOutput.get()
        check(output.isFile && output.length() > 0) { "sealed real KMS evidence is absent or empty: $output" }
    }
}

tasks.register("v2M3RealProviderKmsCheck") {
    group = "verification"
    description = "Require both independently sealed real Provider and KMS execution receipts."
    dependsOn("v2M3RealProviderCheck", "v2M3RealKmsCheck")
}

tasks.register<Exec>("v2M3AllocatorRealEvidenceSourceCheck") {
    group = "verification"
    description = "Run the exact-source real Oxia/Pulsar allocator workload and sealed production verification chain."
    workingDir = layout.projectDirectory.asFile
    usesService(m3NestedGradleGate)
    doFirst {
        environment("NEREUS_M3_ALLOCATOR_EXPECTED_NEREUS_COMMIT", v2M3TestedCommit.get())
        environment(
            "NEREUS_M3_ALLOCATOR_OUTPUT_DIRECTORY",
            layout.projectDirectory.dir(
                "nereus-metadata-oxia/build/m3-allocator-evidence/formal/${v2M3TestedCommit.get()}",
            ).asFile.absolutePath,
        )
        commandLine(
            "bash",
            "scripts/run-v2-m3-real-allocator-evidence.sh",
            v2M3PulsarEvidenceWorktree.get(),
            v2M3OxiaServerEvidenceWorktree.get(),
            v2M3OxiaClientEvidenceWorktree.get(),
        )
    }
}

val v2M3AllocatorVerificationReceiptOutput = providers.gradleProperty("v2M3AllocatorVerificationReceiptOutput")

tasks.register<Exec>("v2M3AllocatorVerificationSeal") {
    group = "verification"
    description = "Seal and reparse the allocator verifier plus all thirteen external raw/source files."
    dependsOn("v2M3AllocatorRealEvidenceSourceCheck")
    workingDir = layout.projectDirectory.asFile
    doFirst {
        val commit = v2M3TestedCommit.get()
        val formal = layout.projectDirectory.dir(
            "nereus-metadata-oxia/build/m3-allocator-evidence/formal/$commit",
        ).asFile
        val executor = layout.projectDirectory.file(
            "nereus-metadata-oxia/build/m3-allocator-evidence/executor/$commit.json",
        ).asFile
        val oxiaClientJar = layout.projectDirectory.file(
            "gradle/locked-artifacts/oxia-client-java/091a42c2780d92da56e9ec1f02ce1c3d988adc16/" +
                "m2/io/github/oxia-db/oxia-client/0.9.4/oxia-client-0.9.4.jar",
        ).asFile
        val runtimeClasspath = layout.projectDirectory.file(
            "nereus-metadata-oxia/build/m3-allocator-evidence/runtime-classpath.txt",
        ).asFile
        check(runtimeClasspath.isFile && runtimeClasspath.length() > 0) {
            "allocator formal runtime classpath is absent or empty: $runtimeClasspath"
        }
        val runtimeArtifacts = runtimeClasspath.readLines()
            .filter(String::isNotBlank)
            .map { file(it).canonicalFile }
        fun exactRuntimeArtifact(directory: File, basenamePrefix: String): File {
            val exactDirectory = directory.canonicalFile
            val matches = runtimeArtifacts.filter { candidate ->
                candidate.parentFile == exactDirectory &&
                    candidate.name.startsWith(basenamePrefix) &&
                    candidate.name.endsWith(".jar") &&
                    !candidate.name.endsWith("-sources.jar")
            }
            check(matches.size == 1 && matches.single().isFile && matches.single().length() > 0) {
                "allocator runtime artifact is absent or ambiguous: directory=$exactDirectory " +
                    "prefix=$basenamePrefix matches=$matches"
            }
            return matches.single()
        }
        val runtimeDomain = exactRuntimeArtifact(
            layout.projectDirectory.dir("nereus-domain/build/libs").asFile,
            "nereus-domain-",
        )
        val runtimeMetadataOxia = exactRuntimeArtifact(
            layout.projectDirectory.dir("nereus-metadata-oxia/build/libs").asFile,
            "nereus-metadata-oxia-",
        )
        val runtimeMetadataSpi = exactRuntimeArtifact(
            layout.projectDirectory.dir("nereus-metadata-spi/build/libs").asFile,
            "nereus-metadata-spi-",
        )
        val testedEvidence = exactRuntimeArtifact(
            layout.projectDirectory.dir("nereus-metadata-oxia/build/libs").asFile,
            "nereus-v2-m3-real-allocator-evidence-",
        )
        val output = v2M3AllocatorVerificationReceiptOutput.orNull?.let { file(it) }
            ?: layout.buildDirectory.file("v2-m3/governed/allocator-raw-verification-$commit.json").get().asFile
        val command = mutableListOf(
            "python3",
            "scripts/publish-v2-m3-child.py",
            "--repo-root",
            layout.projectDirectory.asFile.absolutePath,
            "--tested-commit",
            commit,
            "--seal-allocator-verification",
            "--raw-evidence",
            formal.resolve("raw-verification.json").absolutePath,
            "--junit-xml",
            layout.projectDirectory.file(
                "nereus-metadata-oxia/build/test-results/realAllocatorRawVerificationTest/" +
                    "TEST-com.nereusstream.metadata.oxia.v2.allocator.evidence." +
                    "M3AllocatorRawEvidenceVerificationTest.xml",
            ).asFile.absolutePath,
        )
        linkedMapOf(
            "selection.nars" to formal.resolve("selection.nars"),
            "test.naea" to formal.resolve("test.naea"),
            "native.naea" to formal.resolve("native.naea"),
            "fault.naea" to formal.resolve("fault.naea"),
            "scale-10000.naea" to formal.resolve("scale-10000.naea"),
            "scale-100000.naea" to formal.resolve("scale-100000.naea"),
            "executorManifest" to executor,
            "oxiaClientJar" to oxiaClientJar,
            "runtimeDomainArtifact" to runtimeDomain,
            "runtimeMetadataOxiaArtifact" to runtimeMetadataOxia,
            "runtimeMetadataSpiArtifact" to runtimeMetadataSpi,
            "sourceLocks" to layout.projectDirectory.file("docs/v2/source-locks.json").asFile,
            "testedEvidenceArtifact" to testedEvidence,
        ).forEach { (name, path) ->
            command.addAll(listOf("--allocator-external-file", "$name=${path.absolutePath}"))
        }
        command.addAll(listOf("--sealed-output", output.absolutePath))
        commandLine(command)
    }
}

val v2M3AllocatorV2CheckpointPath = providers.gradleProperty("v2M3AllocatorV2CheckpointPath")
val v2M3AllocatorV2EvaluationPath = providers.gradleProperty("v2M3AllocatorV2EvaluationPath")
val v2M3AllocatorV2DiagnosticPath = providers.gradleProperty("v2M3AllocatorV2DiagnosticPath")
val v2M3AllocatorV2DiagnosticJUnitPath = providers.gradleProperty("v2M3AllocatorV2DiagnosticJUnitPath")
val v2M3AllocatorV2FormalJUnitPath = providers.gradleProperty("v2M3AllocatorV2FormalJUnitPath")
val v2M3AllocatorV2PromotionOutput = providers.gradleProperty("v2M3AllocatorV2PromotionOutput")
val v2M3AllocatorV2ExecutorArtifact = providers.gradleProperty("v2M3AllocatorV2ExecutorArtifact")
val v2M3AllocatorV2WorkloadPlan = providers.gradleProperty("v2M3AllocatorV2WorkloadPlan")
val v2M3AllocatorV2AttachmentDirectory = providers.gradleProperty("v2M3AllocatorV2AttachmentDirectory")
val v2M3AllocatorV2VerificationReceiptOutput =
    providers.gradleProperty("v2M3AllocatorV2VerificationReceiptOutput")

tasks.register<Exec>("v2M3AllocatorV2VerificationSeal") {
    group = "verification"
    description =
        "Run the offline V2 promotion reproof and seal NACP2/NAEV2/NADV2/JUnit/attachment authority for one future child."
    dependsOn(":nereus-metadata-oxia:realAllocatorV2PromotionCheck")
    workingDir = layout.projectDirectory.asFile
    doFirst {
        val attachmentDirectory = file(v2M3AllocatorV2AttachmentDirectory.get()).canonicalFile
        check(attachmentDirectory.isDirectory && !java.nio.file.Files.isSymbolicLink(attachmentDirectory.toPath())) {
            "allocator V2 execution attachment directory is absent or a link: $attachmentDirectory"
        }
        val attachments = attachmentDirectory.listFiles()
            ?.sortedBy { it.name }
            ?: emptyList()
        check(attachments.size in 1..328 && attachments.all {
            it.isFile && !java.nio.file.Files.isSymbolicLink(it.toPath()) && it.length() in 1..(16L * 1024L * 1024L)
        }) {
            "allocator V2 execution attachment inventory is empty, oversized, or not regular"
        }
        val command = mutableListOf(
            "python3",
            "scripts/publish-v2-m3-child.py",
            "--repo-root",
            layout.projectDirectory.asFile.absolutePath,
            "--tested-commit",
            v2M3TestedCommit.get(),
            "--seal-allocator-v2-verification",
            "--allocator-v2-checkpoint",
            file(v2M3AllocatorV2CheckpointPath.get()).absolutePath,
            "--allocator-v2-evaluation",
            file(v2M3AllocatorV2EvaluationPath.get()).absolutePath,
            "--allocator-v2-diagnostic",
            file(v2M3AllocatorV2DiagnosticPath.get()).absolutePath,
            "--allocator-v2-diagnostic-junit",
            file(v2M3AllocatorV2DiagnosticJUnitPath.get()).absolutePath,
            "--allocator-v2-formal-junit",
            file(v2M3AllocatorV2FormalJUnitPath.get()).absolutePath,
            "--allocator-v2-promotion-decision",
            file(v2M3AllocatorV2PromotionOutput.get()).absolutePath,
            "--allocator-v2-executor-artifact",
            file(v2M3AllocatorV2ExecutorArtifact.get()).absolutePath,
            "--allocator-v2-workload-plan",
            file(v2M3AllocatorV2WorkloadPlan.get()).absolutePath,
        )
        attachments.forEach { attachment ->
            command.addAll(listOf("--allocator-v2-execution-attachment", attachment.absolutePath))
        }
        command.addAll(
            listOf(
                "--sealed-output",
                file(v2M3AllocatorV2VerificationReceiptOutput.get()).absolutePath,
            ),
        )
        commandLine(command)
    }
}

val v2M3AllocatorV5CheckpointPath = providers.gradleProperty("v2M3AllocatorV5CheckpointPath")
val v2M3AllocatorV5EvaluationPath = providers.gradleProperty("v2M3AllocatorV5EvaluationPath")
val v2M3AllocatorV5DiagnosticPath = providers.gradleProperty("v2M3AllocatorV5DiagnosticPath")
val v2M3AllocatorV5SelectionPath = providers.gradleProperty("v2M3AllocatorV5SelectionPath")
val v2M3AllocatorV5FormalJUnitPath = providers.gradleProperty("v2M3AllocatorV5FormalJUnitPath")
val v2M3AllocatorV5PromotionOutput = providers.gradleProperty("v2M3AllocatorV5PromotionOutput")
val v2M3AllocatorV5ExecutorArtifact = providers.gradleProperty("v2M3AllocatorV5ExecutorArtifact")
val v2M3AllocatorV5DiagnosticJUnitDirectory =
    providers.gradleProperty("v2M3AllocatorV5DiagnosticJUnitDirectory")
val v2M3AllocatorV5DiagnosticRawDirectory =
    providers.gradleProperty("v2M3AllocatorV5DiagnosticRawDirectory")
val v2M3AllocatorV5AttachmentDirectory = providers.gradleProperty("v2M3AllocatorV5AttachmentDirectory")
val v2M3AllocatorV5VerificationReceiptOutput =
    providers.gradleProperty("v2M3AllocatorV5VerificationReceiptOutput")

tasks.register<Exec>("v2M3AllocatorV5VerificationSeal") {
    group = "verification"
    description =
        "Independently replay and seal source-bound NACP5/NAEV5/NADV5/NARS5/JUnit/raw/physical authority."
    workingDir = layout.projectDirectory.asFile
    doFirst {
        fun exactFiles(directory: File, suffix: String, count: Int, maximum: Long): List<File> {
            check(directory.isDirectory && !java.nio.file.Files.isSymbolicLink(directory.toPath())) {
                "allocator V5 governed directory is absent or a link: $directory"
            }
            val files = directory.listFiles()
                ?.filter { it.name.endsWith(suffix) }
                ?.sortedBy { it.name }
                ?: emptyList()
            check(files.size == count && files.all {
                it.isFile && !java.nio.file.Files.isSymbolicLink(it.toPath()) && it.length() in 1..maximum
            }) {
                "allocator V5 governed $suffix inventory differs: directory=$directory count=${files.size}"
            }
            return files
        }
        val junit = exactFiles(
            file(v2M3AllocatorV5DiagnosticJUnitDirectory.get()).canonicalFile,
            ".xml",
            10,
            16L * 1024L * 1024L,
        )
        val raw = exactFiles(
            file(v2M3AllocatorV5DiagnosticRawDirectory.get()).canonicalFile,
            ".json",
            19,
            16L * 1024L * 1024L,
        )
        val attachmentDirectory = file(v2M3AllocatorV5AttachmentDirectory.get()).canonicalFile
        check(attachmentDirectory.isDirectory && !java.nio.file.Files.isSymbolicLink(attachmentDirectory.toPath())) {
            "allocator V5 physical attachment directory is absent or a link: $attachmentDirectory"
        }
        val attachments = attachmentDirectory.listFiles()?.sortedBy { it.name } ?: emptyList()
        check(attachments.size in 1..720 && attachments.all {
            it.isFile && !java.nio.file.Files.isSymbolicLink(it.toPath()) &&
                it.length() in 1..(32L * 1024L * 1024L)
        }) {
            "allocator V5 physical attachment inventory is empty, oversized, or not regular"
        }
        val command = mutableListOf(
            "python3",
            "scripts/publish-v2-m3-child.py",
            "--repo-root",
            layout.projectDirectory.asFile.absolutePath,
            "--tested-commit",
            v2M3TestedCommit.get(),
            "--seal-allocator-v5-verification",
            "--allocator-v5-checkpoint",
            file(v2M3AllocatorV5CheckpointPath.get()).absolutePath,
            "--allocator-v5-evaluation",
            file(v2M3AllocatorV5EvaluationPath.get()).absolutePath,
            "--allocator-v5-diagnostic",
            file(v2M3AllocatorV5DiagnosticPath.get()).absolutePath,
            "--allocator-v5-selection",
            file(v2M3AllocatorV5SelectionPath.get()).absolutePath,
            "--allocator-v5-formal-junit",
            file(v2M3AllocatorV5FormalJUnitPath.get()).absolutePath,
            "--allocator-v5-promotion-decision",
            file(v2M3AllocatorV5PromotionOutput.get()).absolutePath,
            "--allocator-v5-executor-artifact",
            file(v2M3AllocatorV5ExecutorArtifact.get()).absolutePath,
        )
        junit.forEach { command.addAll(listOf("--allocator-v5-diagnostic-junit", it.absolutePath)) }
        raw.forEach { command.addAll(listOf("--allocator-v5-diagnostic-raw", it.absolutePath)) }
        attachments.forEach { command.addAll(listOf("--allocator-v5-execution-attachment", it.absolutePath)) }
        command.addAll(
            listOf(
                "--sealed-output",
                file(v2M3AllocatorV5VerificationReceiptOutput.get()).absolutePath,
            ),
        )
        commandLine(command)
    }
}

tasks.register("v2M3AllocatorV1CompatibilityCheck") {
    group = "verification"
    description = "Retain the strict V1 NARS1/NAEA1 governed parser path as compatibility-only authority."
    dependsOn("v2M3AllocatorVerificationSeal")
}

tasks.register("v2M3AllocatorCheck") {
    group = "verification"
    description = "Require one completed, uniquely selected V5 campaign and its source-bound governed verification."
    dependsOn("v2M3AllocatorV5VerificationSeal")
}

val v2M3LocalCapEvidenceOutputDirectory = providers.gradleProperty("v2M3LocalCapEvidenceOutputDirectory")

tasks.register<Exec>("v2M3LocalCapacityEvidenceCheck") {
    group = "verification"
    description = "Execute and seal the exact-source six-record allocation-free D1 local-cap evidence."
    notCompatibleWithConfigurationCache("formal D1 evidence checks live Git state and exclusive external outputs")
    workingDir = layout.projectDirectory.asFile
    usesService(m3NestedGradleGate)
    doFirst {
        commandLine(
            "bash",
            "scripts/run-v2-m3-local-cap-evidence.sh",
            v2M3TestedCommit.get(),
            v2M3PulsarEvidenceWorktree.get(),
            v2M3LocalCapEvidenceOutputDirectory.get(),
        )
    }
}

tasks.register("v2M3Nwg1CapacityCheck") {
    group = "verification"
    description = "Require ordinary local-cap coverage plus separate real Provider/KMS execution evidence."
    dependsOn(
        "v2M3OrdinarySourceCheck",
        "v2M3LocalCapacityEvidenceCheck",
        "v2M3RealProviderKmsCheck",
        "v2DocumentationCheck",
    )
}

val v2M3FinalReceipt = providers.gradleProperty("v2M3FinalReceipt")
    .orElse("docs/v2/evidence/v2-m3/final/m3-final.json")

tasks.register<Exec>("v2M3FinalSourceCheck") {
    group = "verification"
    description =
        "Reparse all eleven child receipts, raw attachments, source locks, and the exact 26-scenario allowlist."
    workingDir = layout.projectDirectory.asFile
    doFirst {
        commandLine(
            "python3",
            "scripts/check-v2-m3-final.py",
            "--repo-root",
            layout.projectDirectory.asFile.absolutePath,
            "--receipt",
            v2M3FinalReceipt.get(),
            "--expected-tested-commit",
            v2M3TestedCommit.get(),
        )
    }
}

val v2M3FinalCandidate = providers.gradleProperty("v2M3FinalCandidate")

tasks.register<Exec>("v2M3FinalPublish") {
    group = "verification"
    description = "Publish one prebuilt validated M3 Final candidate without overwrite."
    workingDir = layout.projectDirectory.asFile
    doFirst {
        commandLine(
            "python3",
            "scripts/publish-v2-m3-final.py",
            "--repo-root",
            layout.projectDirectory.asFile.absolutePath,
            "--candidate",
            v2M3FinalCandidate.get(),
            "--output",
            v2M3FinalReceipt.get(),
        )
    }
}

tasks.register("v2M3Check") {
    group = "verification"
    description = "Validate the exact-source M3 Final; formal evidence generation remains a separate clean-source run."
    dependsOn("v2M3FinalSourceCheck", "v2DocumentationCheck")
}

tasks.register<Exec>("v2M4HistoricalM3DependencyContractTest") {
    group = "verification"
    description = "Test M4's immutable historical M3 dependency without weakening the exact-source M3 contract."
    dependsOn("v2M3FinalCheckerContractTest")
    workingDir = layout.projectDirectory.asFile
    commandLine("python3", "scripts/check-v2-m4-historical-m3-tests.py")
}

tasks.register<Exec>("v2M4HistoricalM3DependencyCheck") {
    group = "verification"
    description = "Validate M4's immutable e5 M3 dependency without recertifying current HEAD as M3."
    workingDir = layout.projectDirectory.asFile
    commandLine("python3", "scripts/check-v2-m4-historical-m3.py")
}

tasks.register<Exec>("v2M4DesignContractTest") {
    group = "verification"
    description = "Run fail-closed contract tests for the governance-only M4 design freeze."
    workingDir = layout.projectDirectory.asFile
    commandLine("python3", "scripts/check-v2-m4-design-tests.py")
}

tasks.register<Exec>("v2M4DesignSourceCheck") {
    group = "verification"
    description = "Validate the frozen M4 design manifest and prove implementation/evidence have not started."
    workingDir = layout.projectDirectory.asFile
    commandLine("python3", "scripts/check-v2-m4-design.py")
}

tasks.register("v2M4DesignCheck") {
    group = "verification"
    description = "Hard-freeze M4 design inputs without implementation, evidence, scenario PASS, or Final authority."
    dependsOn(
        "v2DocumentationCheck",
        "v2M4HistoricalM3DependencyContractTest",
        "v2M4HistoricalM3DependencyCheck",
        "v2M4DesignContractTest",
        "v2M4DesignSourceCheck",
    )
}

tasks.register<Exec>("v2M4FrozenDesignInputsSourceCheck") {
    group = "verification"
    description = "Verify immutable M4-A/B/C/D and Grill 32-35 bytes after implementation starts."
    workingDir = layout.projectDirectory.asFile
    commandLine("python3", "scripts/check-v2-m4-frozen-inputs.py")
}

tasks.register("v2M4FrozenDesignInputsCheck") {
    group = "verification"
    description = "Verify frozen M4 inputs and immutable historical M3 without repeating the pre-start status claim."
    dependsOn(
        "v2M4HistoricalM3DependencyContractTest",
        "v2M4HistoricalM3DependencyCheck",
        "v2M4DesignContractTest",
        "v2M4FrozenDesignInputsSourceCheck",
    )
}

tasks.register("v2M4ReadKernelCheck") {
    group = "verification"
    description = "Run the M4 hot-path kernel, allocation, style, and frozen-design prerequisite checks."
    dependsOn(
        "v2M4FrozenDesignInputsCheck",
        ":nereus-storage-object:v2M4ReadKernelTest",
        ":nereus-storage-object:spotlessCheck",
        ":nereus-storage-object:checkstyleMain",
        ":nereus-storage-object:checkstyleTest",
    )
}

tasks.register("v2M4ControlPlaneCheck") {
    group = "verification"
    description = "Run M4 durable read-control, Oxia adapter, style, and frozen-design prerequisite checks."
    dependsOn(
        "v2M4FrozenDesignInputsCheck",
        ":nereus-storage-object:v2M4ControlPlaneTest",
        ":nereus-metadata-oxia:v2M4ReadControlOxiaAdapterTest",
        ":nereus-storage-object:spotlessCheck",
        ":nereus-storage-object:checkstyleMain",
        ":nereus-storage-object:checkstyleTest",
        ":nereus-metadata-oxia:spotlessCheck",
        ":nereus-metadata-oxia:checkstyleMain",
        ":nereus-metadata-oxia:checkstyleTest",
    )
}

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
