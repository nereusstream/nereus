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
    api(project(":nereus-api"))
    api(project(":nereus-metadata-oxia"))
    api(project(":nereus-object-store"))
    testImplementation(testFixtures(project(":nereus-metadata-oxia")))
    testImplementation(testFixtures(project(":nereus-object-store")))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.platform.launcher)
}

val phase1IntegrationTest by sourceSets.creating {
    compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
    runtimeClasspath += output + compileClasspath
}

configurations[phase1IntegrationTest.implementationConfigurationName].extendsFrom(
    configurations.testImplementation.get(),
)
configurations[phase1IntegrationTest.runtimeOnlyConfigurationName].extendsFrom(
    configurations.testRuntimeOnly.get(),
)

dependencies {
    add(phase1IntegrationTest.implementationConfigurationName, platform(libs.grpc.bom))
    add(phase1IntegrationTest.implementationConfigurationName, platform(libs.opentelemetry.bom))
    add(phase1IntegrationTest.implementationConfigurationName, platform(libs.opentelemetry.bom.alpha))
    add(phase1IntegrationTest.implementationConfigurationName, libs.oxia.client)
    add(phase1IntegrationTest.implementationConfigurationName, libs.oxia.testcontainers)
    add(phase1IntegrationTest.implementationConfigurationName, libs.testcontainers.junit.jupiter)
    add(phase1IntegrationTest.implementationConfigurationName, libs.junit.jupiter)
    add(phase1IntegrationTest.implementationConfigurationName, libs.assertj)
    add(phase1IntegrationTest.runtimeOnlyConfigurationName, libs.junit.platform.launcher)
}

tasks.register<Test>("phase1IntegrationTest") {
    group = "verification"
    description = "Run the M8 final Core StreamStorage integration gate."
    testClassesDirs = phase1IntegrationTest.output.classesDirs
    classpath = phase1IntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.test)
    useJUnitPlatform()
}

tasks.register<Test>("bkM6GenerationScaleTest") {
    group = "verification"
    description = "Run the BK-M6 generation-zero and exact 4,096/4,097 candidate resolution boundaries."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.core.read.GenerationReadResolverTest")
    }
}
