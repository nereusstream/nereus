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

sourceSets {
    main {
        java.setSrcDirs(listOf("src/main/java"))
        java.include("com/nereusstream/metadata/oxia/v2/**")
    }
    test {
        java.setSrcDirs(listOf("src/test/java"))
        java.include("com/nereusstream/metadata/oxia/v2/**")
    }
}

val oxiaIntegrationTest by sourceSets.creating {
    java.setSrcDirs(listOf("src/oxiaIntegrationTest/java"))
    java.include("com/nereusstream/metadata/oxia/v2/**")
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
    implementation(project(":nereus-metadata-spi"))
    implementation(platform(libs.grpc.bom))
    implementation(platform(libs.opentelemetry.bom))
    implementation(platform(libs.opentelemetry.bom.alpha))
    implementation(libs.oxia.client)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.platform.launcher)

    add(oxiaIntegrationTest.implementationConfigurationName, project())
    add(oxiaIntegrationTest.implementationConfigurationName, libs.oxia.testcontainers)
    add(oxiaIntegrationTest.implementationConfigurationName, libs.testcontainers.junit.jupiter)
    add(oxiaIntegrationTest.implementationConfigurationName, libs.junit.jupiter)
    add(oxiaIntegrationTest.implementationConfigurationName, libs.assertj)
    add(oxiaIntegrationTest.runtimeOnlyConfigurationName, libs.junit.platform.launcher)
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

tasks.register<Test>("p1MetadataTest") {
    group = "verification"
    description = "Run the deterministic P1 V2 metadata capability unit gate."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.*")
        // R1 shares the V2 package but owns a separate receipt and exact-source gate.
        excludeTestsMatching("com.nereusstream.metadata.oxia.v2.codec.Nvr1RegistryAuthorityCodecTest")
        excludeTestsMatching("com.nereusstream.metadata.oxia.v2.capability.R1RegistryAuthorityTest")
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

tasks.register<Test>("r1MetadataTest") {
    group = "verification"
    description = "Run the deterministic R1 Registry domain bridge, interlock, evidence, and mutation gate."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.codec.Nvr1RegistryAuthorityCodecTest")
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.capability.R1RegistryAuthorityTest")
    }
}

tasks.register<Test>("r1OxiaIntegrationTest") {
    group = "verification"
    description = "Run R1 Registry create/CAS/restart conformance against source-locked real Oxia."
    testClassesDirs = oxiaIntegrationTest.output.classesDirs
    classpath = oxiaIntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.test)
    maxParallelForks = 1
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.metadata.oxia.v2.R1RegistryOxiaIntegrationTest")
    }
}
