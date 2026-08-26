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
    api(project(":nereus-storage-api"))
    implementation(project(":nereus-storage-bookkeeper"))
    api(project(":nereus-storage-object"))

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.platform.launcher)
}

val realBookKeeperTest by sourceSets.creating {
    java.srcDir("src/realBookKeeperTest/java")
    compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
    runtimeClasspath += output + compileClasspath
}

val bookKeeperScale by sourceSets.creating {
    java.srcDir("src/bookKeeperScale/java")
    compileClasspath += sourceSets.main.get().output + configurations.runtimeClasspath.get()
    runtimeClasspath += output + compileClasspath
}

configurations[realBookKeeperTest.implementationConfigurationName]
    .extendsFrom(configurations.testImplementation.get())
configurations[realBookKeeperTest.runtimeOnlyConfigurationName]
    .extendsFrom(configurations.testRuntimeOnly.get())

configurations[bookKeeperScale.implementationConfigurationName]
    .extendsFrom(configurations.implementation.get())

tasks.register<Test>("realBookKeeperTest") {
    group = "verification"
    description = "Run K3-K7 Kafka engine paths against the exact-image real BookKeeper provider."
    testClassesDirs = realBookKeeperTest.output.classesDirs
    classpath = realBookKeeperTest.runtimeClasspath
    useJUnitPlatform()
    maxParallelForks = 1
    outputs.upToDateWhen { false }
    systemProperty(
        "nereus.bookkeeper.metadataServiceUri",
        providers.gradleProperty("v2M2BookKeeperMetadataServiceUri").get(),
    )
}

tasks.register<JavaExec>("v2M2KafkaK9Scale") {
    group = "verification"
    description = "Run one predeclared K9 scale tier against the exact-image real BookKeeper cluster."
    dependsOn(tasks.named(bookKeeperScale.classesTaskName))
    classpath = bookKeeperScale.runtimeClasspath
    workingDir(rootProject.projectDir)
    mainClass.set("com.nereusstream.kafka.bookkeeper.evidence.KafkaBookKeeperScaleHarnessV1")
    args(
        providers.gradleProperty("v2M2KafkaK9ScalePlan").get(),
        providers.gradleProperty("v2M2KafkaK9ConformanceConfig").get(),
        providers.gradleProperty("v2M2KafkaK9ScaleTier").get(),
        providers.gradleProperty("v2M2KafkaK9ScaleOutput").get(),
        providers.gradleProperty("v2M2KafkaK9TestedSourceCommit").get(),
    )
    maxHeapSize = "1024m"
}

tasks.withType<Jar>().configureEach {
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
}

tasks.register<JavaExec>("v2M2KafkaInputsReceiptCheck") {
    group = "verification"
    description = "Parse one canonical Kafka Inputs receipt through the production closed-model validator."
    dependsOn(tasks.named("classes"))
    classpath = sourceSets.main.get().runtimeClasspath
    workingDir(rootProject.projectDir)
    mainClass.set("com.nereusstream.kafka.bookkeeper.evidence.KafkaM2InputsReceiptCli")
    args("validate", providers.gradleProperty("v2M2KafkaInputsReceipt").get())
}

tasks.register<JavaExec>("v2M2KafkaFinalReceiptCheck") {
    group = "verification"
    description = "Resolve one canonical Kafka M2 Final receipt through the production fail-closed validator."
    dependsOn(tasks.named("classes"))
    classpath = sourceSets.main.get().runtimeClasspath
    workingDir(rootProject.projectDir)
    mainClass.set("com.nereusstream.kafka.bookkeeper.evidence.KafkaM2FinalReceiptCli")
    args(
        "validate",
        rootProject.projectDir.absolutePath,
        providers.gradleProperty("v2M2KafkaFinalReceipt").get(),
    )
}

val v2M3KafkaNativeReceiptOutput = providers.gradleProperty("v2M3KafkaNativeReceiptOutput")
val v2M3KafkaTestedSourceCommit = providers.gradleProperty("v2M3KafkaTestedSourceCommit")
val v2M3KafkaSourceRepository = providers.gradleProperty("v2M3KafkaSourceRepository")
val v2M3KafkaSourceCommit = providers.gradleProperty("v2M3KafkaSourceCommit")
val v2M3KafkaTestStartedAtUtc = providers.gradleProperty("v2M3KafkaTestStartedAtUtc")
val v2M3KafkaTestFinishedAtUtc = providers.gradleProperty("v2M3KafkaTestFinishedAtUtc")

tasks.register<JavaExec>("v2M3KafkaNativeReceiptEmit") {
    group = "verification"
    description = "Emit the source/JUnit-bound M3 Kafka Object-WAL raw native receipt to an explicit untracked path."
    dependsOn(
        tasks.named("test"),
        tasks.named("classes"),
        tasks.named("spotlessCheck"),
        tasks.named("checkstyleMain"),
        tasks.named("checkstyleTest"),
    )
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.nereusstream.kafka.bookkeeper.object.evidence.KafkaObjectWalNativeResultV1")
    outputs.upToDateWhen { false }
    doFirst {
        val output = v2M3KafkaNativeReceiptOutput.orNull
            ?: throw GradleException("-Pv2M3KafkaNativeReceiptOutput is required")
        val outputPath = file(output).toPath().toAbsolutePath().normalize()
        val repositoryPath = rootProject.projectDir.toPath().toAbsolutePath().normalize()
        val rootBuildPath = rootProject.layout.buildDirectory.get().asFile.toPath().toAbsolutePath().normalize()
        val moduleBuildPath = layout.buildDirectory.get().asFile.toPath().toAbsolutePath().normalize()
        if (outputPath.startsWith(repositoryPath)
            && !outputPath.startsWith(rootBuildPath)
            && !outputPath.startsWith(moduleBuildPath)
        ) {
            throw GradleException("M3 raw receipt output must remain outside tracked repository paths")
        }
        args(
            "generate",
            rootProject.projectDir.absolutePath,
            v2M3KafkaTestedSourceCommit.orNull
                ?: throw GradleException("-Pv2M3KafkaTestedSourceCommit is required"),
            v2M3KafkaSourceRepository.orNull
                ?: throw GradleException("-Pv2M3KafkaSourceRepository is required"),
            v2M3KafkaSourceCommit.orNull
                ?: throw GradleException("-Pv2M3KafkaSourceCommit is required"),
            v2M3KafkaTestStartedAtUtc.orNull
                ?: throw GradleException("-Pv2M3KafkaTestStartedAtUtc is required"),
            v2M3KafkaTestFinishedAtUtc.orNull
                ?: throw GradleException("-Pv2M3KafkaTestFinishedAtUtc is required"),
            outputPath.toString(),
        )
    }
}

val v2M3KafkaNativeReceiptInput = providers.gradleProperty("v2M3KafkaNativeReceiptInput")

tasks.register<JavaExec>("v2M3KafkaNativeReceiptCheck") {
    group = "verification"
    description = "Strictly parse and self-hash-check one explicit M3 Kafka Object-WAL raw native receipt."
    dependsOn(tasks.named("classes"))
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.nereusstream.kafka.bookkeeper.object.evidence.KafkaObjectWalNativeResultV1")
    outputs.upToDateWhen { false }
    doFirst {
        args(
            "parse",
            v2M3KafkaNativeReceiptInput.orNull
                ?: throw GradleException("-Pv2M3KafkaNativeReceiptInput is required"),
        )
    }
}

tasks.register<JavaExec>("nwkcp1ProtocolFixtureEmitter") {
    group = "verification"
    description = "Emit the exact NWKCP1 Object and OPEN/TERMINAL Head protocol fixture."
    dependsOn(tasks.named("testClasses"))
    classpath = sourceSets.test.get().runtimeClasspath
    mainClass.set("com.nereusstream.kafka.bookkeeper.object.nwkcp1.Nwkcp1ProtocolFixtureV1Test")
    args(providers.gradleProperty("nwkcp1ProtocolFixtureOutput").get())
    outputs.upToDateWhen { false }
}
