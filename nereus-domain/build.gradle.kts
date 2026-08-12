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
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.withType<Jar>().configureEach {
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
}

tasks.register<Test>("r1RegistryDomainTest") {
    group = "verification"
    description = "Run the deterministic R1 Registry wire, capacity, evidence, and transition gate."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.domain.registry.RegistryCapacityEvidenceTest")
        includeTestsMatching("com.nereusstream.domain.registry.Nvr1RegistryCodecV1Test")
        includeTestsMatching("com.nereusstream.domain.registry.PulsarVirtualLedgerRegistryTransitionValidatorV1Test")
        includeTestsMatching("com.nereusstream.domain.registry.RegistryAdmissionEvidenceV1Test")
    }
}
