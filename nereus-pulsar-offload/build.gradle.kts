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
    implementation(libs.zstd.jni)
    implementation(platform(libs.aws.sdk.v2.bom))
    implementation(libs.aws.sdk.v2.s3)
    compileOnly(libs.pulsar.managed.ledger)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(libs.pulsar.managed.ledger)
    testRuntimeOnly(libs.junit.platform.launcher)
}

val p6ProviderTest by sourceSets.creating {
    java.srcDir("src/p6ProviderTest/java")
    compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
    runtimeClasspath += output + compileClasspath
}

configurations[p6ProviderTest.implementationConfigurationName]
    .extendsFrom(configurations.testImplementation.get())
configurations[p6ProviderTest.runtimeOnlyConfigurationName]
    .extendsFrom(configurations.testRuntimeOnly.get())

dependencies {
    add(p6ProviderTest.implementationConfigurationName, platform(libs.aws.sdk.v2.bom))
    add(p6ProviderTest.implementationConfigurationName, libs.aws.sdk.v2.s3)
    add(p6ProviderTest.implementationConfigurationName, libs.testcontainers.junit.jupiter)
    add(p6ProviderTest.implementationConfigurationName, libs.testcontainers.localstack)
}

tasks.register<Test>("p6ProviderTest") {
    group = "verification"
    description = "Run the explicit M2-P6 S3-compatible provider evidence suite."
    testClassesDirs = p6ProviderTest.output.classesDirs
    classpath = p6ProviderTest.runtimeClasspath
    useJUnitPlatform()
    maxParallelForks = 1
    outputs.upToDateWhen { false }
}

tasks.named<Test>("test") {
    maxHeapSize = "1024m"
}

tasks.withType<Jar>().configureEach {
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
}
