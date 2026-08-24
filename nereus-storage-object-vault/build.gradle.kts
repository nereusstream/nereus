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
        "real KMS evidence requires -Pv2M3TestedCommit=<exact 40-hex commit>"
    }
    val head = exactGitOutput("rev-parse", "HEAD")
    check(testedCommit == head) { "real KMS tested commit differs from exact HEAD: $testedCommit != $head" }
    val published = exactGitOutput("rev-parse", "origin/main")
    check(head == published) { "real KMS source HEAD is not the published origin/main: $head != $published" }
    check(exactGitOutput("status", "--porcelain=v1", "--untracked-files=all").isEmpty()) {
        "real KMS evidence requires a clean exact source worktree"
    }
    check(!rawEvidence.exists() && !Files.isSymbolicLink(rawEvidence.toPath())) {
        "real KMS raw evidence output already exists; run from a fresh build directory"
    }
    check(!sealedEvidence.exists() && !Files.isSymbolicLink(sealedEvidence.toPath())) {
        "real KMS sealed evidence output already exists; overwrite is forbidden"
    }
}

val realKmsTest by sourceSets.creating {
    java.srcDir("src/realKmsTest/java")
    compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
    runtimeClasspath += output + compileClasspath
}

configurations[realKmsTest.implementationConfigurationName]
    .extendsFrom(configurations.testImplementation.get())
configurations[realKmsTest.runtimeOnlyConfigurationName]
    .extendsFrom(configurations.testRuntimeOnly.get())

tasks.register<Test>("realKmsTest") {
    group = "verification"
    description = "Run exact-digest HashiCorp Vault Transit KMS integration evidence."
    testClassesDirs = realKmsTest.output.classesDirs
    classpath = realKmsTest.runtimeClasspath
    useJUnitPlatform()
    maxParallelForks = 1
    maxHeapSize = "512m"
    outputs.upToDateWhen { false }
    val rawEvidence = layout.buildDirectory.file("m3-evidence/raw/vault-transit-kms.json").get().asFile
    val sealedEvidence = providers.gradleProperty("v2M3KmsEvidenceOutput")
        .map { file(it) }
        .getOrElse(layout.buildDirectory.file("m3-evidence/vault-transit-kms.json").get().asFile)
    systemProperty(
        "nereus.m3.kmsEvidenceOutput",
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
            "real KMS sealed evidence output appeared during execution; overwrite is forbidden"
        }
        val junitXml = layout.buildDirectory.file(
            "test-results/realKmsTest/TEST-com.nereusstream.storage.object.vault.VaultTransitRealKmsEvidenceTest.xml",
        ).get().asFile
        val command = listOf(
            "python3",
            rootProject.file("scripts/publish-v2-m3-child.py").absolutePath,
            "--tested-commit",
            providers.gradleProperty("v2M3TestedCommit").getOrElse("UNSET"),
            "--seal-real-kind",
            "KMS_REAL_RECEIPT",
            "--raw-evidence",
            rawEvidence.absolutePath,
            "--junit-xml",
            junitXml.absolutePath,
            "--sealed-output",
            sealedEvidence.absolutePath,
        )
        check(ProcessBuilder(command).directory(rootProject.projectDir).inheritIO().start().waitFor() == 0) {
            "real KMS post-test execution receipt sealing failed"
        }
    }
}

tasks.register<Test>("diagnosticRealKmsTest") {
    group = "verification"
    description = "Run Vault Transit KMS integration diagnostics without producing admissible M3 evidence."
    testClassesDirs = realKmsTest.output.classesDirs
    classpath = realKmsTest.runtimeClasspath
    useJUnitPlatform()
    maxParallelForks = 1
    maxHeapSize = "512m"
    outputs.upToDateWhen { false }
    systemProperty("nereus.m3.evidenceMode", "DIAGNOSTIC")
    systemProperty("nereus.m3.testedCommit", "DIAGNOSTIC-NON-EVIDENCE")
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

tasks.withType<Jar>().configureEach {
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
}
