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
