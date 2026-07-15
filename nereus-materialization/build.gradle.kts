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
    implementation(project(":nereus-core"))
    implementation(project(":nereus-metadata-oxia"))
    implementation(project(":nereus-object-store"))

    testImplementation(testFixtures(project(":nereus-metadata-oxia")))
    testImplementation(testFixtures(project(":nereus-object-store")))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.platform.launcher)
}

val f4M2IntegrationTest by sourceSets.creating {
    compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
    runtimeClasspath += output + compileClasspath
}

configurations[f4M2IntegrationTest.implementationConfigurationName].extendsFrom(
    configurations.testImplementation.get(),
)
configurations[f4M2IntegrationTest.runtimeOnlyConfigurationName].extendsFrom(
    configurations.testRuntimeOnly.get(),
)

dependencies {
    add(f4M2IntegrationTest.implementationConfigurationName, project())
    add(f4M2IntegrationTest.implementationConfigurationName, libs.oxia.testcontainers)
    add(f4M2IntegrationTest.implementationConfigurationName, libs.testcontainers.junit.jupiter)
    add(f4M2IntegrationTest.implementationConfigurationName, libs.testcontainers.localstack)
    add(f4M2IntegrationTest.implementationConfigurationName, libs.commons.codec)
    add(f4M2IntegrationTest.implementationConfigurationName, libs.junit.jupiter)
    add(f4M2IntegrationTest.implementationConfigurationName, libs.assertj)
    add(f4M2IntegrationTest.runtimeOnlyConfigurationName, libs.junit.platform.launcher)
}

tasks.register<Test>("f4M2IntegrationTest") {
    group = "verification"
    description = "Run F4-M2 publication, restart, durable pin, and fallback against real Oxia and LocalStack."
    testClassesDirs = f4M2IntegrationTest.output.classesDirs
    classpath = f4M2IntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.test)
    useJUnitPlatform()
}
