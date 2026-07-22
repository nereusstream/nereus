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

val f4M3IntegrationTest by sourceSets.creating {
    compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
    runtimeClasspath += output + compileClasspath
}

configurations[f4M2IntegrationTest.implementationConfigurationName].extendsFrom(
    configurations.testImplementation.get(),
)
configurations[f4M2IntegrationTest.runtimeOnlyConfigurationName].extendsFrom(
    configurations.testRuntimeOnly.get(),
)
configurations[f4M3IntegrationTest.implementationConfigurationName].extendsFrom(
    configurations.testImplementation.get(),
)
configurations[f4M3IntegrationTest.runtimeOnlyConfigurationName].extendsFrom(
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

    add(f4M3IntegrationTest.implementationConfigurationName, project())
    add(f4M3IntegrationTest.implementationConfigurationName, libs.oxia.testcontainers)
    add(f4M3IntegrationTest.implementationConfigurationName, libs.testcontainers.junit.jupiter)
    add(f4M3IntegrationTest.implementationConfigurationName, libs.testcontainers.localstack)
    add(f4M3IntegrationTest.implementationConfigurationName, libs.junit.jupiter)
    add(f4M3IntegrationTest.implementationConfigurationName, libs.assertj)
    add(f4M3IntegrationTest.runtimeOnlyConfigurationName, libs.junit.platform.launcher)
}

tasks.register<Test>("f4M2IntegrationTest") {
    group = "verification"
    description = "Run F4-M2 publication, restart, durable pin, and fallback against real Oxia and LocalStack."
    testClassesDirs = f4M2IntegrationTest.output.classesDirs
    classpath = f4M2IntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.test)
    useJUnitPlatform()
}

tasks.register<Test>("f4M3IntegrationTest") {
    group = "verification"
    description = "Run F4-M3 worker, exact-source, Parquet publication, and restart against real Oxia/LocalStack."
    testClassesDirs = f4M3IntegrationTest.output.classesDirs
    classpath = f4M3IntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.test)
    useJUnitPlatform()
}

tasks.register<Test>("bkM6MixedSourceScaleTest") {
    group = "verification"
    description = "Run the BK-M6 mixed BK/Object 128-source and 1,048,576-record task boundary."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.materialization.BookKeeperMixedSourceTaskScaleTest")
    }
}
