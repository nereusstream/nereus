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
    api(project(":nereus-core"))
    api(project(":nereus-managed-ledger"))
    api(project(":nereus-materialization"))
    implementation(project(":nereus-bookkeeper"))
    api(libs.opentelemetry.api)
    implementation(project(":nereus-metadata-oxia"))
    implementation(project(":nereus-object-store"))

    testImplementation(testFixtures(project(":nereus-metadata-oxia")))
    testImplementation(testFixtures(project(":nereus-object-store")))
    testImplementation(testFixtures(project(":nereus-managed-ledger")))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(platform(libs.grpc.bom))
    testImplementation(platform(libs.opentelemetry.bom))
    testImplementation(platform(libs.opentelemetry.bom.alpha))
    testImplementation(libs.oxia.client)
    testRuntimeOnly(libs.junit.platform.launcher)
}

val f4M4IntegrationTest by sourceSets.creating {
    compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
    runtimeClasspath += output + compileClasspath
}

configurations[f4M4IntegrationTest.implementationConfigurationName].extendsFrom(
    configurations.testImplementation.get(),
)
configurations[f4M4IntegrationTest.runtimeOnlyConfigurationName].extendsFrom(
    configurations.testRuntimeOnly.get(),
)

dependencies {
    add(f4M4IntegrationTest.implementationConfigurationName, project())
    add(f4M4IntegrationTest.implementationConfigurationName, libs.oxia.testcontainers)
    add(f4M4IntegrationTest.implementationConfigurationName, libs.testcontainers.junit.jupiter)
    add(f4M4IntegrationTest.implementationConfigurationName, libs.testcontainers.localstack)
    add(f4M4IntegrationTest.implementationConfigurationName, platform(libs.aws.sdk.v2.bom))
    add(f4M4IntegrationTest.implementationConfigurationName, libs.aws.sdk.v2.s3)
    add(f4M4IntegrationTest.implementationConfigurationName, libs.junit.jupiter)
    add(f4M4IntegrationTest.implementationConfigurationName, libs.assertj)
    add(f4M4IntegrationTest.runtimeOnlyConfigurationName, libs.junit.platform.launcher)
}

tasks.register<Test>("f4M4IntegrationTest") {
    group = "verification"
    description = "Run F4-M4 activation and destructive restart recovery against real Oxia and LocalStack."
    testClassesDirs = f4M4IntegrationTest.output.classesDirs
    classpath = f4M4IntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.test)
    useJUnitPlatform()
}

val bkM2IntegrationTest by sourceSets.creating {
    compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
    runtimeClasspath += output + compileClasspath
}

val bkM3IntegrationTest by sourceSets.creating {
    compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
    runtimeClasspath += output + compileClasspath
}

configurations[bkM2IntegrationTest.implementationConfigurationName].extendsFrom(
    configurations.testImplementation.get(),
)
configurations[bkM2IntegrationTest.runtimeOnlyConfigurationName].extendsFrom(
    configurations.testRuntimeOnly.get(),
)
configurations[bkM3IntegrationTest.implementationConfigurationName].extendsFrom(
    configurations.testImplementation.get(),
)
configurations[bkM3IntegrationTest.runtimeOnlyConfigurationName].extendsFrom(
    configurations.testRuntimeOnly.get(),
)

dependencies {
    add(bkM2IntegrationTest.implementationConfigurationName, project())
    add(bkM2IntegrationTest.implementationConfigurationName, libs.pulsar.metadata)
    add(bkM2IntegrationTest.implementationConfigurationName, libs.bookkeeper.server)
    add(bkM2IntegrationTest.implementationConfigurationName, libs.oxia.testcontainers)
    add(bkM2IntegrationTest.implementationConfigurationName, libs.testcontainers.junit.jupiter)
    add(bkM2IntegrationTest.implementationConfigurationName, libs.junit.jupiter)
    add(bkM2IntegrationTest.implementationConfigurationName, libs.assertj)
    add(bkM2IntegrationTest.runtimeOnlyConfigurationName, libs.junit.platform.launcher)

    add(bkM3IntegrationTest.implementationConfigurationName, project())
    add(bkM3IntegrationTest.implementationConfigurationName, libs.pulsar.metadata)
    add(bkM3IntegrationTest.implementationConfigurationName, libs.bookkeeper.server)
    add(bkM3IntegrationTest.implementationConfigurationName, libs.oxia.testcontainers)
    add(bkM3IntegrationTest.implementationConfigurationName, libs.testcontainers.junit.jupiter)
    add(bkM3IntegrationTest.implementationConfigurationName, libs.testcontainers.localstack)
    add(bkM3IntegrationTest.implementationConfigurationName, platform(libs.aws.sdk.v2.bom))
    add(bkM3IntegrationTest.implementationConfigurationName, libs.aws.sdk.v2.s3)
    add(bkM3IntegrationTest.implementationConfigurationName, libs.junit.jupiter)
    add(bkM3IntegrationTest.implementationConfigurationName, libs.assertj)
    add(bkM3IntegrationTest.runtimeOnlyConfigurationName, libs.junit.platform.launcher)
}

tasks.register<Test>("bkM2IntegrationTest") {
    group = "verification"
    description = "Run BK-M2 allocation, shard, restart, rollover, and retention recovery against real services."
    testClassesDirs = bkM2IntegrationTest.output.classesDirs
    classpath = bkM2IntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.test)
    useJUnitPlatform()
}

tasks.register<Test>("bkM3IntegrationTest") {
    group = "verification"
    description = "Run BK async stable-head, fallback, fresh-runtime publication, and retirement against real services."
    testClassesDirs = bkM3IntegrationTest.output.classesDirs
    classpath = bkM3IntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.test)
    useJUnitPlatform()
    filter {
        excludeTestsMatching("*.syncObject*")
    }
}

tasks.register<Test>("bkM4IntegrationTest") {
    group = "verification"
    description = "Run BK sync completion, normal Object-read admission, and KNOWN_COMMITTED recovery cuts."
    testClassesDirs = bkM3IntegrationTest.output.classesDirs
    classpath = bkM3IntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.test)
    useJUnitPlatform()
    filter {
        includeTestsMatching("*.syncObject*")
    }
}
