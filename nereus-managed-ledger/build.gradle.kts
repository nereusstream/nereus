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
    implementation(project(":nereus-metadata-oxia"))
    api(project(":nereus-object-store"))
    api(libs.pulsar.managed.ledger)
    api(libs.pulsar.common)
    api(libs.pulsar.client.admin.api)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(platform(libs.grpc.bom))
    testImplementation(platform(libs.opentelemetry.bom))
    testImplementation(platform(libs.opentelemetry.bom.alpha))
    testImplementation(libs.oxia.client)
    testImplementation(project(":nereus-core"))
    testImplementation(testFixtures(project(":nereus-metadata-oxia")))
    testImplementation(testFixtures(project(":nereus-object-store")))
    testRuntimeOnly(libs.junit.platform.launcher)
}

val cursorS3IntegrationTest by sourceSets.creating {
    compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
    runtimeClasspath += output + compileClasspath
}

configurations[cursorS3IntegrationTest.implementationConfigurationName].extendsFrom(
    configurations.testImplementation.get(),
)
configurations[cursorS3IntegrationTest.runtimeOnlyConfigurationName].extendsFrom(
    configurations.testRuntimeOnly.get(),
)

dependencies {
    add(cursorS3IntegrationTest.implementationConfigurationName, project())
    add(cursorS3IntegrationTest.implementationConfigurationName, project(":nereus-object-store"))
    add(cursorS3IntegrationTest.implementationConfigurationName, platform(libs.aws.sdk.v2.bom))
    add(cursorS3IntegrationTest.implementationConfigurationName, libs.aws.sdk.v2.s3)
    add(cursorS3IntegrationTest.implementationConfigurationName, libs.testcontainers.junit.jupiter)
    add(cursorS3IntegrationTest.implementationConfigurationName, libs.testcontainers.localstack)
    add(cursorS3IntegrationTest.implementationConfigurationName, libs.junit.jupiter)
    add(cursorS3IntegrationTest.implementationConfigurationName, libs.assertj)
    add(cursorS3IntegrationTest.runtimeOnlyConfigurationName, libs.junit.platform.launcher)
}

tasks.register<Test>("cursorS3IntegrationTest") {
    group = "verification"
    description = "Run the F3 cursor snapshot store against pinned LocalStack S3."
    testClassesDirs = cursorS3IntegrationTest.output.classesDirs
    classpath = cursorS3IntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.test)
    useJUnitPlatform()
}
