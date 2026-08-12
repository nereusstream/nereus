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

plugins {
    `java-test-fixtures`
}

val p1ArtifactJar by tasks.registering(Jar::class) {
    group = "build"
    description = "Build the source-qualified P1-only metadata capability artifact."
    archiveBaseName.set("nereus-metadata-oxia-p1")
    from(sourceSets.main.get().output)
    include("com/nereusstream/metadata/oxia/v2/**")
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
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
}

val oxiaCapabilitySpike by sourceSets.creating {
    compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
    runtimeClasspath += output + compileClasspath
}

configurations[oxiaCapabilitySpike.implementationConfigurationName].extendsFrom(
    configurations.testImplementation.get(),
)
configurations[oxiaCapabilitySpike.runtimeOnlyConfigurationName].extendsFrom(
    configurations.testRuntimeOnly.get(),
)

dependencies {
    api(project(":nereus-api"))
    implementation(project(":nereus-metadata-spi"))
    implementation(platform(libs.grpc.bom))
    implementation(platform(libs.opentelemetry.bom))
    implementation(platform(libs.opentelemetry.bom.alpha))
    implementation(libs.oxia.client)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.platform.launcher)

    add(oxiaCapabilitySpike.implementationConfigurationName, project())
    add(oxiaCapabilitySpike.implementationConfigurationName, platform(libs.grpc.bom))
    add(oxiaCapabilitySpike.implementationConfigurationName, platform(libs.opentelemetry.bom))
    add(oxiaCapabilitySpike.implementationConfigurationName, platform(libs.opentelemetry.bom.alpha))
    add(oxiaCapabilitySpike.implementationConfigurationName, libs.oxia.client)
    add(oxiaCapabilitySpike.implementationConfigurationName, libs.oxia.testcontainers)
    add(oxiaCapabilitySpike.implementationConfigurationName, libs.testcontainers.junit.jupiter)
    add(oxiaCapabilitySpike.implementationConfigurationName, libs.junit.jupiter)
    add(oxiaCapabilitySpike.implementationConfigurationName, libs.assertj)
    add(oxiaCapabilitySpike.runtimeOnlyConfigurationName, libs.junit.platform.launcher)
}

val oxiaIntegrationTest by sourceSets.creating {
    compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
    runtimeClasspath += output + compileClasspath
}

configurations[oxiaIntegrationTest.implementationConfigurationName].extendsFrom(
    configurations.testImplementation.get(),
)
configurations[oxiaIntegrationTest.runtimeOnlyConfigurationName].extendsFrom(
    configurations.testRuntimeOnly.get(),
)

dependencies {
    add(oxiaIntegrationTest.implementationConfigurationName, project())
    add(oxiaIntegrationTest.implementationConfigurationName, libs.oxia.testcontainers)
    add(oxiaIntegrationTest.implementationConfigurationName, libs.testcontainers.junit.jupiter)
    add(oxiaIntegrationTest.implementationConfigurationName, libs.junit.jupiter)
    add(oxiaIntegrationTest.implementationConfigurationName, libs.assertj)
    add(oxiaIntegrationTest.runtimeOnlyConfigurationName, libs.junit.platform.launcher)
}

tasks.register<Test>("oxiaIntegrationTest") {
    group = "verification"
    description = "Run the M7 production Oxia adapter integration gate."
    testClassesDirs = oxiaIntegrationTest.output.classesDirs
    classpath = oxiaIntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.test)
    useJUnitPlatform()
}

tasks.register<Test>("f4OxiaIntegrationTest") {
    group = "verification"
    description = "Run the F4-M1 metadata CAS, pagination, restart, and conditional-delete gate against real Oxia."
    testClassesDirs = oxiaIntegrationTest.output.classesDirs
    classpath = oxiaIntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.test)
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.metadata.oxia.F4MetadataStoreOxiaIntegrationTest")
    }
}

tasks.register<Test>("f9MetadataTest") {
    group = "verification"
    description = "Run the F9-M2 Kafka key, codec, binding, and authority deterministic contracts."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.metadata.oxia.Kafka*")
        includeTestsMatching("com.nereusstream.metadata.oxia.codec.Kafka*")
        includeTestsMatching("com.nereusstream.metadata.oxia.codec.StreamHeadV2CodecTest")
        includeTestsMatching("com.nereusstream.metadata.oxia.testing.Kafka*")
    }
}

tasks.register<Test>("f9OxiaIntegrationTest") {
    group = "verification"
    description =
        "Run the F9 Kafka binding CAS, registry scan, and checkpoint-quarantine reconnect gate against real Oxia."
    testClassesDirs = oxiaIntegrationTest.output.classesDirs
    classpath = oxiaIntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.test, tasks.named("f9MetadataTest"))
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.metadata.oxia.KafkaPartitionMetadataOxiaIntegrationTest")
    }
}

tasks.register<Test>("f9BindingScaleOxiaIntegrationTest") {
    group = "verification"
    description =
        "Run the F9-M7 16,384-binding, 64-shard pagination and reconnect gate against real Oxia."
    testClassesDirs = oxiaIntegrationTest.output.classesDirs
    classpath = oxiaIntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.test, tasks.named("f9OxiaIntegrationTest"))
    maxParallelForks = 1
    useJUnitPlatform()
    filter {
        includeTestsMatching(
            "com.nereusstream.metadata.oxia.KafkaBindingScaleIntegrationTest",
        )
    }
}

tasks.register<Test>("f9ActivationOxiaIntegrationTest") {
    group = "verification"
    description = "Run the F9-M6 Kafka activation/capability/readiness CAS gate against real Oxia."
    testClassesDirs = oxiaIntegrationTest.output.classesDirs
    classpath = oxiaIntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.test, tasks.named("f9MetadataTest"))
    useJUnitPlatform()
    filter {
        includeTestsMatching(
            "com.nereusstream.metadata.oxia.KafkaStorageActivationMetadataOxiaIntegrationTest",
        )
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

val oxiaCapabilitySpikeReportDir = layout.buildDirectory.dir("reports/oxia-capability-spike")

tasks.register<Test>("oxiaCapabilitySpike") {
    group = "verification"
    description = "Run the M0.5 Oxia Java client capability spike against a Testcontainers Oxia server."
    testClassesDirs = oxiaCapabilitySpike.output.classesDirs
    classpath = oxiaCapabilitySpike.runtimeClasspath
    shouldRunAfter(tasks.test)
    useJUnitPlatform()
    workingDir = projectDir
    outputs.dir(oxiaCapabilitySpikeReportDir)
}
