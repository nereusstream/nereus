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
    `base`
    `maven-publish`
}

group = providers.gradleProperty("nereusGroup").get()
version = providers.gradleProperty("nereusVersion").getOrElse("0.1.0-SNAPSHOT")

val javaLanguageVersion = providers.gradleProperty("javaVersion").map(String::toInt).getOrElse(21)

subprojects {
    group = rootProject.group
    version = rootProject.version
}

configure(subprojects.filter { it.name != "nereus-bom" }) {
    apply(plugin = "java-library")
    apply(plugin = "maven-publish")

    extensions.configure<JavaPluginExtension>("java") {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(javaLanguageVersion))
        }
        withSourcesJar()
    }

    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
        options.release.set(javaLanguageVersion)
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }

    extensions.configure<PublishingExtension>("publishing") {
        repositories {
            maven {
                name = "development"
                url = rootProject.layout.buildDirectory.dir("development-repository").get().asFile.toURI()
            }
        }
        publications {
            create<MavenPublication>("mavenJava") {
                from(components["java"])
                pom {
                    name.set(project.name)
                    description.set("Nereus module ${project.name}")
                    url.set("https://nereusstream.com")
                    licenses {
                        license {
                            name.set("Apache License, Version 2.0")
                            url.set("https://www.apache.org/licenses/LICENSE-2.0")
                        }
                    }
                    scm {
                        url.set("https://github.com/nereusstream/nereus")
                        connection.set("scm:git:https://github.com/nereusstream/nereus.git")
                    }
                }
            }
        }
    }
}

tasks.register("quickCheck") {
    group = "verification"
    description = "Fast scaffold check for Nereus."
    dependsOn("checkPhase0")
}

tasks.register<Exec>("checkPhase0") {
    group = "verification"
    description = "Verify the Phase 0 repository scaffold."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase0.sh")
}

val phase1L0Modules = listOf(
    ":nereus-api",
    ":nereus-core",
    ":nereus-metadata-oxia",
    ":nereus-object-store",
)

tasks.register<Exec>("checkPhase1L0Dependencies") {
    group = "verification"
    description = "Verify Phase 1 L0 modules stay protocol-neutral."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase1-l0-dependencies.sh")
}

tasks.register<Exec>("checkPhase1Namespace") {
    group = "verification"
    description = "Verify Java packages and Maven coordinates use the owned nereusstream.com namespace."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase1-namespace.sh")
}

tasks.register("phase1Check") {
    group = "verification"
    description = "Verify the Phase 1 Core StreamStorage scaffold."
    dependsOn("checkPhase0")
    dependsOn("checkPhase1L0Dependencies")
    dependsOn("checkPhase1Namespace")
    dependsOn(phase1L0Modules.map { "$it:test" })
    dependsOn(":nereus-metadata-oxia:compileOxiaCapabilitySpikeJava")
}

tasks.register("phase1FinalCheck") {
    group = "verification"
    description = "Run every ordinary and Docker-backed Phase 1 release gate."
    dependsOn("phase1Check")
    dependsOn(":nereus-metadata-oxia:oxiaCapabilitySpike")
    dependsOn(":nereus-metadata-oxia:oxiaIntegrationTest")
    dependsOn(":nereus-core:phase1IntegrationTest")
}

tasks.register("phase15Check") {
    group = "verification"
    description = "Verify the Phase 1.5 generic target, recovery, lifecycle, and compatibility foundation."
    dependsOn("phase1Check")
    dependsOn(phase1L0Modules.map { "$it:test" })
}

tasks.register("phase15FinalCheck") {
    group = "verification"
    description = "Run the production Oxia/Object WAL Phase 1.5 mixed-version and lifecycle gates."
    dependsOn("phase15Check")
    dependsOn("phase1FinalCheck")
}

val pulsarCheckoutPath = providers.gradleProperty("pulsarCheckout")
    .orElse(providers.environmentVariable("NEREUS_PULSAR_CHECKOUT"))
    .orElse(layout.projectDirectory.dir("../../nereusstream/pulsar").asFile.absolutePath)

tasks.register<Exec>("checkPulsarSourceLock") {
    group = "verification"
    description = "Verify the exact clean Pulsar fork checkout used by Phase 2."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-pulsar-source-lock.sh", pulsarCheckoutPath.get())
}

tasks.register("phase2M1Check") {
    group = "verification"
    description = "Verify the F2-M1 projection, Position, entry codec, and L0 request foundation."
    dependsOn("phase15Check")
    dependsOn("checkPulsarSourceLock")
    dependsOn(":nereus-metadata-oxia:test")
    dependsOn(":nereus-managed-ledger:test")
}

tasks.register("phase2M2Check") {
    group = "verification"
    description = "Verify the F2-M2 projection metadata model, codec, CAS, repair, and shared-runtime contracts."
    dependsOn("phase2M1Check")
    dependsOn(":nereus-metadata-oxia:test")
    dependsOn(":nereus-metadata-oxia:compileOxiaIntegrationTestJava")
}

tasks.register("phase2M2FinalCheck") {
    group = "verification"
    description = "Run the ordinary and Docker-backed real Oxia F2-M2 projection metadata gates."
    dependsOn("phase2M2Check")
    dependsOn(":nereus-metadata-oxia:oxiaIntegrationTest")
}
