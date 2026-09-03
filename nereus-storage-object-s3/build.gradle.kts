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

import java.nio.file.Files

dependencies {
    api(project(":nereus-storage-object"))
    api(platform(libs.aws.sdk.v2.bom))
    api(libs.aws.sdk.v2.s3)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.platform.launcher)
}

fun exactGitOutput(vararg arguments: String): String {
    val process = ProcessBuilder(listOf("git", *arguments))
        .directory(rootProject.projectDir)
        .redirectErrorStream(true)
        .start()
    val output = process.inputStream.readBytes().toString(Charsets.UTF_8).trim()
    check(process.waitFor() == 0) { "git ${arguments.joinToString(" ")} failed" }
    return output
}

fun requireFormalEvidenceSource(testedCommit: String, rawEvidence: File, sealedEvidence: File) {
    check(Regex("[0-9a-f]{40}").matches(testedCommit)) {
        "real Provider evidence requires -Pv2M3TestedCommit=<exact 40-hex commit>"
    }
    val head = exactGitOutput("rev-parse", "HEAD")
    check(testedCommit == head) { "real Provider tested commit differs from exact HEAD: $testedCommit != $head" }
    val published = exactGitOutput("rev-parse", "origin/main")
    check(head == published) { "real Provider source HEAD is not the published origin/main: $head != $published" }
    check(exactGitOutput("status", "--porcelain=v1", "--untracked-files=all").isEmpty()) {
        "real Provider evidence requires a clean exact source worktree"
    }
    check(!rawEvidence.exists() && !Files.isSymbolicLink(rawEvidence.toPath())) {
        "real Provider raw evidence output already exists; run from a fresh build directory"
    }
    check(!sealedEvidence.exists() && !Files.isSymbolicLink(sealedEvidence.toPath())) {
        "real Provider sealed evidence output already exists; overwrite is forbidden"
    }
}

val realProviderTest by sourceSets.creating {
    java.srcDir("src/realProviderTest/java")
    compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
    runtimeClasspath += output + compileClasspath
}

configurations[realProviderTest.implementationConfigurationName]
    .extendsFrom(configurations.testImplementation.get())
configurations[realProviderTest.runtimeOnlyConfigurationName]
    .extendsFrom(configurations.testRuntimeOnly.get())

dependencies {
    add(realProviderTest.implementationConfigurationName, platform(libs.aws.sdk.v2.bom))
    add(realProviderTest.implementationConfigurationName, libs.aws.sdk.v2.s3)
}

tasks.register<Test>("realProviderTest") {
    group = "verification"
    description = "Run the exact-digest MinIO C1 Provider integration evidence."
    notCompatibleWithConfigurationCache(
        "formal evidence performs live Git admission and exclusive post-test receipt publication",
    )
    testClassesDirs = realProviderTest.output.classesDirs
    classpath = realProviderTest.runtimeClasspath
    useJUnitPlatform()
    maxParallelForks = 1
    maxHeapSize = "768m"
    outputs.upToDateWhen { false }
    val rawEvidence = layout.buildDirectory.file("m3-evidence/raw/c1-minio-provider.json").get().asFile
    val sealedEvidence = providers.gradleProperty("v2M3ProviderEvidenceOutput")
        .map { file(it) }
        .getOrElse(layout.buildDirectory.file("m3-evidence/c1-minio-provider.json").get().asFile)
    systemProperty(
        "nereus.m3.providerEvidenceOutput",
        rawEvidence.absolutePath,
    )
    systemProperty(
        "nereus.m3.testedCommit",
        providers.gradleProperty("v2M3TestedCommit").getOrElse("UNSET"),
    )
    systemProperty("nereus.m3.evidenceMode", "FORMAL")
    outputs.file(sealedEvidence)
    doFirst {
        requireFormalEvidenceSource(
            providers.gradleProperty("v2M3TestedCommit").getOrElse("UNSET"),
            rawEvidence,
            sealedEvidence,
        )
    }
    doLast {
        check(!sealedEvidence.exists() && !Files.isSymbolicLink(sealedEvidence.toPath())) {
            "real Provider sealed evidence output appeared during execution; overwrite is forbidden"
        }
        val junitXml = layout.buildDirectory.file(
            "test-results/realProviderTest/TEST-com.nereusstream.storage.object.s3.MinioC1RealProviderEvidenceTest.xml",
        ).get().asFile
        val command = listOf(
            "python3",
            rootProject.file("scripts/publish-v2-m3-child.py").absolutePath,
            "--tested-commit",
            providers.gradleProperty("v2M3TestedCommit").getOrElse("UNSET"),
            "--seal-real-kind",
            "PROVIDER_REAL_RECEIPT",
            "--raw-evidence",
            rawEvidence.absolutePath,
            "--junit-xml",
            junitXml.absolutePath,
            "--sealed-output",
            sealedEvidence.absolutePath,
        )
        check(ProcessBuilder(command).directory(rootProject.projectDir).inheritIO().start().waitFor() == 0) {
            "real Provider post-test execution receipt sealing failed"
        }
    }
}

tasks.register<Test>("diagnosticRealProviderTest") {
    group = "verification"
    description = "Run MinIO C1 integration diagnostics without producing admissible M3 evidence."
    testClassesDirs = realProviderTest.output.classesDirs
    classpath = realProviderTest.runtimeClasspath
    useJUnitPlatform()
    maxParallelForks = 1
    maxHeapSize = "768m"
    outputs.upToDateWhen { false }
    systemProperty("nereus.m3.evidenceMode", "DIAGNOSTIC")
    systemProperty("nereus.m3.testedCommit", "DIAGNOSTIC-NON-EVIDENCE")
}

tasks.register<Test>("v2M5VersionMatchDeleteRealProviderTest") {
    group = "verification"
    description = "Run the non-promotable M5-D version-match delete contract against exact-digest MinIO."
    testClassesDirs = realProviderTest.output.classesDirs
    classpath = realProviderTest.runtimeClasspath
    useJUnitPlatform()
    include("**/M5MinioVersionMatchDeleteTest.class")
    maxParallelForks = 1
    maxHeapSize = "512m"
    outputs.upToDateWhen { false }
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

tasks.withType<Jar>().configureEach {
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
}
