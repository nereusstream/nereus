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

dependencies {
    api(project(":nereus-storage-object"))
    api(project(":nereus-metadata-spi"))
    implementation(libs.zstd.jni)
    api(platform(libs.aws.sdk.v2.bom))
    api(libs.aws.sdk.v2.s3)
    compileOnlyApi(libs.pulsar.managed.ledger)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(libs.pulsar.managed.ledger)
    testRuntimeOnly(libs.junit.platform.launcher)
}

val p6ProviderTest by sourceSets.creating {
    java.srcDir("src/p6ProviderTest/java")
    compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
    runtimeClasspath += output + compileClasspath
}

configurations[p6ProviderTest.implementationConfigurationName]
    .extendsFrom(configurations.testImplementation.get())
configurations[p6ProviderTest.runtimeOnlyConfigurationName]
    .extendsFrom(configurations.testRuntimeOnly.get())

dependencies {
    add(p6ProviderTest.implementationConfigurationName, platform(libs.aws.sdk.v2.bom))
    add(p6ProviderTest.implementationConfigurationName, libs.aws.sdk.v2.s3)
    add(p6ProviderTest.implementationConfigurationName, libs.testcontainers.junit.jupiter)
    add(p6ProviderTest.implementationConfigurationName, libs.testcontainers.localstack)
}

tasks.register<Test>("p6ProviderTest") {
    group = "verification"
    description = "Run the explicit M2-P6 S3-compatible provider evidence suite."
    testClassesDirs = p6ProviderTest.output.classesDirs
    classpath = p6ProviderTest.runtimeClasspath
    useJUnitPlatform()
    include("**/S3PulsarOffloadObjectStoreV1Test.class")
    maxParallelForks = 1
    outputs.upToDateWhen { false }
}

tasks.register<Test>("p6EvidenceTest") {
    group = "verification"
    description = "Generate the M2-P6 S3-compatible candidate matrix evidence receipt."
    testClassesDirs = p6ProviderTest.output.classesDirs
    classpath = p6ProviderTest.runtimeClasspath
    useJUnitPlatform()
    include("**/PulsarP6CandidateEvidenceTest.class")
    maxParallelForks = 1
    maxHeapSize = "2048m"
    outputs.upToDateWhen { false }
    systemProperty(
        "nereus.p6.evidenceOutput",
        providers.gradleProperty("v2M2PulsarP6EvidenceOutput")
            .getOrElse(layout.buildDirectory.file("p6-evidence/pulsar-p6-candidate.json").get().asFile.absolutePath),
    )
    systemProperty(
        "nereus.p6.testedSourceCommit",
        providers.gradleProperty("v2M2PulsarP6TestedSourceCommit").getOrElse("UNSET"),
    )
    systemProperty(
        "nereus.p6.pulsarSourceCommit",
        providers.gradleProperty("v2M2PulsarP6PulsarSourceCommit").getOrElse("UNSET"),
    )
}

tasks.register<Test>("p6RealProviderTest") {
    group = "verification"
    description = "Run M2-P6 against the fixed external MinIO S3-compatible provider."
    testClassesDirs = p6ProviderTest.output.classesDirs
    classpath = p6ProviderTest.runtimeClasspath
    useJUnitPlatform()
    include("**/P6MinioProviderEvidenceTest.class")
    maxParallelForks = 1
    maxHeapSize = "1024m"
    outputs.upToDateWhen { false }
    systemProperty(
        "nereus.p6.realProviderOutput",
        providers.gradleProperty("v2M2PulsarP6RealProviderOutput")
            .getOrElse(layout.buildDirectory.file("p6-evidence/minio-provider.json").get().asFile.absolutePath),
    )
    systemProperty(
        "nereus.p6.testedSourceCommit",
        providers.gradleProperty("v2M2PulsarP6TestedSourceCommit").getOrElse("UNSET"),
    )
    systemProperty(
        "nereus.p6.minioImageReference",
        providers.gradleProperty("v2M2PulsarP6MinioImageReference").getOrElse("UNSET"),
    )
    systemProperty(
        "nereus.p6.minioImageDigest",
        providers.gradleProperty("v2M2PulsarP6MinioImageDigest").getOrElse("UNSET"),
    )
}

tasks.named<Test>("test") {
    maxHeapSize = "1024m"
}

val v2M3PulsarNativeReceiptOutput = providers.gradleProperty("v2M3PulsarNativeReceiptOutput")
val v2M3PulsarTestedSourceCommit = providers.gradleProperty("v2M3PulsarTestedSourceCommit")
val v2M3PulsarSourceRepository = providers.gradleProperty("v2M3PulsarSourceRepository")
val v2M3PulsarSourceCommit = providers.gradleProperty("v2M3PulsarSourceCommit")
val v2M3PulsarTestStartedAtUtc = providers.gradleProperty("v2M3PulsarTestStartedAtUtc")
val v2M3PulsarTestFinishedAtUtc = providers.gradleProperty("v2M3PulsarTestFinishedAtUtc")

tasks.register<JavaExec>("v2M3PulsarNativeReceiptEmit") {
    group = "verification"
    description = "Emit the source/JUnit-bound M3 Pulsar Object-WAL raw native receipt to an explicit untracked path."
    dependsOn(tasks.named("test"), tasks.named("classes"))
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalNativeResultV1")
    outputs.upToDateWhen { false }
    doFirst {
        val output = v2M3PulsarNativeReceiptOutput.orNull
            ?: throw GradleException("-Pv2M3PulsarNativeReceiptOutput is required")
        val outputPath = file(output).toPath().toAbsolutePath().normalize()
        val repositoryPath = rootProject.projectDir.toPath().toAbsolutePath().normalize()
        val buildPath = rootProject.layout.buildDirectory.get().asFile.toPath().toAbsolutePath().normalize()
        val moduleBuildPath = layout.buildDirectory.get().asFile.toPath().toAbsolutePath().normalize()
        if (outputPath.startsWith(repositoryPath)
            && !outputPath.startsWith(buildPath)
            && !outputPath.startsWith(moduleBuildPath)
        ) {
            throw GradleException("M3 raw receipt output must remain outside tracked repository paths")
        }
        args(
            "generate",
            rootProject.projectDir.absolutePath,
            v2M3PulsarTestedSourceCommit.orNull
                ?: throw GradleException("-Pv2M3PulsarTestedSourceCommit is required"),
            v2M3PulsarSourceRepository.orNull
                ?: throw GradleException("-Pv2M3PulsarSourceRepository is required"),
            v2M3PulsarSourceCommit.orNull
                ?: throw GradleException("-Pv2M3PulsarSourceCommit is required"),
            v2M3PulsarTestStartedAtUtc.orNull
                ?: throw GradleException("-Pv2M3PulsarTestStartedAtUtc is required"),
            v2M3PulsarTestFinishedAtUtc.orNull
                ?: throw GradleException("-Pv2M3PulsarTestFinishedAtUtc is required"),
            outputPath.toString(),
        )
    }
}

val v2M3PulsarNativeReceiptInput = providers.gradleProperty("v2M3PulsarNativeReceiptInput")

tasks.register<JavaExec>("v2M3PulsarNativeReceiptCheck") {
    group = "verification"
    description = "Strictly parse and self-hash-check one explicit M3 Pulsar Object-WAL raw native receipt."
    dependsOn(tasks.named("classes"))
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalNativeResultV1")
    outputs.upToDateWhen { false }
    doFirst {
        args(
            "parse",
            v2M3PulsarNativeReceiptInput.orNull
                ?: throw GradleException("-Pv2M3PulsarNativeReceiptInput is required"),
        )
    }
}

tasks.withType<Jar>().configureEach {
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
}
