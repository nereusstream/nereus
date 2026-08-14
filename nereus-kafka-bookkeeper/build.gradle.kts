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
