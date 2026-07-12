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

dependencies {
    api(project(":nereus-api"))
    implementation(libs.aws.java.sdk.s3)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.platform.launcher)
}

val s3IntegrationTest by sourceSets.creating {
    compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
    runtimeClasspath += output + compileClasspath
}

configurations[s3IntegrationTest.implementationConfigurationName].extendsFrom(
    configurations.testImplementation.get(),
)
configurations[s3IntegrationTest.runtimeOnlyConfigurationName].extendsFrom(
    configurations.testRuntimeOnly.get(),
)

dependencies {
    add(s3IntegrationTest.implementationConfigurationName, project())
    add(s3IntegrationTest.implementationConfigurationName, libs.testcontainers.junit.jupiter)
    add(s3IntegrationTest.implementationConfigurationName, libs.testcontainers.localstack)
    add(s3IntegrationTest.implementationConfigurationName, libs.junit.jupiter)
    add(s3IntegrationTest.implementationConfigurationName, libs.assertj)
    add(s3IntegrationTest.runtimeOnlyConfigurationName, libs.junit.platform.launcher)
}

tasks.register<Test>("s3IntegrationTest") {
    group = "verification"
    description = "Run the F2 S3-compatible provider gate against pinned LocalStack S3."
    testClassesDirs = s3IntegrationTest.output.classesDirs
    classpath = s3IntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.test)
    useJUnitPlatform()
}
