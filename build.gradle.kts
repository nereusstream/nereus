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
val phase2DevelopmentVersion = "0.1.0-f2-dev"
val pulsarDevelopmentGateRequested = gradle.startParameter.taskNames.any { requested ->
    requested.substringAfterLast(':').startsWith("phase2")
        || requested.substringAfterLast(':').startsWith("phase3")
        // M1 consumes the final-gated F3 source composite. The Pulsar fork remains on the
        // frozen F2 development coordinate until the F4 broker rollout milestone changes both repos.
        || requested.substringAfterLast(':').startsWith("phase4")
        || requested.substringAfterLast(':') == "publishPhase2DevelopmentArtifacts"
}
version = gradle.startParameter.projectProperties["nereusVersion"]
    ?: if (pulsarDevelopmentGateRequested) {
        phase2DevelopmentVersion
    } else {
        providers.gradleProperty("nereusVersion").getOrElse("0.1.0-SNAPSHOT")
    }
if (pulsarDevelopmentGateRequested) {
    check(version.toString() == phase2DevelopmentVersion) {
        "Phase 2 development gates require version $phase2DevelopmentVersion, got $version"
    }
}

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
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
        withSourcesJar()
    }

    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
        options.release.set(if (name == "compileJava") 17 else javaLanguageVersion)
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
val pulsarExpectedHead = providers.gradleProperty("pulsarExpectedHead")
    .orElse("c2f7c22fdc562022b992a5c7aecb5fd5c02d318d")

tasks.register<Exec>("checkPulsarSourceLock") {
    group = "verification"
    description = "Verify the exact clean Pulsar fork checkout used by Phase 2."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-pulsar-source-lock.sh",
        pulsarCheckoutPath.get(),
        pulsarExpectedHead.get(),
    )
}

tasks.register<Exec>("checkPhase2StorageIsolation") {
    group = "verification"
    description = "Verify virtual-ledger routing cannot enter BookKeeper APIs and production has no local object path."
    dependsOn("checkPulsarSourceLock")
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase2-storage-isolation.sh",
        pulsarCheckoutPath.get(),
    )
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

val phase2PublishedModules = listOf(
    ":nereus-api",
    ":nereus-core",
    ":nereus-metadata-oxia",
    ":nereus-object-store",
    ":nereus-managed-ledger",
    ":nereus-pulsar-adapter",
)

tasks.register("publishPhase2DevelopmentArtifacts") {
    group = "verification"
    description = "Publish the exact Nereus F2 development coordinate for the Pulsar fork gate."
    dependsOn(phase2PublishedModules.map { "$it:publishAllPublicationsToDevelopmentRepository" })
}

val phase2DevelopmentRepository = layout.buildDirectory.dir("development-repository")
val pulsarGradleWrapper = file(pulsarCheckoutPath.get()).resolve("gradlew").absolutePath

tasks.register<Exec>("phase2PulsarCheck") {
    group = "verification"
    description = "Run ordinary Pulsar fork Nereus tests, stock persistence regressions, and checkstyle."
    dependsOn("checkPulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    // phase3FinalCheck runs with org.gradle.parallel=true and --rerun-tasks. Keep the first
    // nested Pulsar build behind every local task that consumes the included Pulsar outputs.
    mustRunAfter("phase3M3Check", "phase3M2FinalCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.*",
        "--tests", "org.apache.pulsar.broker.service.persistent.PersistentTopicNereusAdmissionTest",
        "--tests", "org.apache.pulsar.broker.admin.TopicPoliciesTest.testPersistencePolicyRejectsMissingTopic",
        "--tests", "org.apache.pulsar.broker.admin.TopicPoliciesTest.testGetPersistenceApplied",
        "--tests", "org.apache.pulsar.broker.admin.TopicPoliciesTest.testSetPersistence",
        "--tests", "org.apache.pulsar.broker.admin.TopicPoliciesTest.testRemovePersistence",
        "-PexcludedTestGroups=quarantine,flaky,broker-isolated",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase2Check") {
    group = "verification"
    description = "Run every ordinary Nereus F2 product and Pulsar fork gate."
    dependsOn("phase2M2Check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-pulsar-adapter:check")
    dependsOn("checkPhase2StorageIsolation")
    dependsOn("phase2PulsarCheck")
}

tasks.register<Exec>("phase2PulsarFinalCheck") {
    group = "verification"
    description = "Run the real two-broker Oxia/LocalStack/BookKeeper Nereus recovery gate."
    dependsOn("checkPulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase2PulsarCheck", "phase3M6Check")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusMultiBrokerIntegrationTest",
        "--rerun-tasks",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase2FinalCheck") {
    group = "verification"
    description = "Run every ordinary and Docker-backed Nereus F2 release gate."
    dependsOn("phase2Check")
    dependsOn("phase15FinalCheck")
    dependsOn("phase2M2FinalCheck")
    dependsOn(":nereus-object-store:s3IntegrationTest")
    dependsOn("phase2PulsarFinalCheck")
}

tasks.register("phase3M1Check") {
    group = "verification"
    description = "Verify the F3-M1 cursor metadata, activation marker, ack domain, and snapshot foundation."
    dependsOn("phase2M2Check")
    dependsOn("checkPulsarSourceLock")
    dependsOn(":nereus-metadata-oxia:test")
    dependsOn(":nereus-metadata-oxia:compileOxiaIntegrationTestJava")
    dependsOn(":nereus-managed-ledger:test")
    dependsOn(":nereus-object-store:test")
}

tasks.register("phase3M1FinalCheck") {
    group = "verification"
    description = "Run ordinary and Docker-backed real Oxia/ObjectStore F3-M1 foundation gates."
    dependsOn("phase3M1Check")
    dependsOn(":nereus-metadata-oxia:oxiaIntegrationTest")
    dependsOn(":nereus-managed-ledger:cursorS3IntegrationTest")
    dependsOn(":nereus-object-store:s3IntegrationTest")
}

tasks.register("phase3M2Check") {
    group = "verification"
    description = "Verify the F3-M2 cursor storage, retention, recovery, and failure-injection state machines."
    dependsOn("phase3M1Check")
    dependsOn(":nereus-managed-ledger:test")
    dependsOn(":nereus-managed-ledger:compileCursorM2IntegrationTestJava")
}

tasks.register("phase3M2FinalCheck") {
    group = "verification"
    description = "Run ordinary and Docker-backed real Oxia/ObjectStore F3-M2 recovery gates."
    dependsOn("phase3M2Check")
    dependsOn("phase3M1FinalCheck")
    dependsOn(":nereus-managed-ledger:cursorM2IntegrationTest")
}

tasks.register("phase3M3Check") {
    group = "verification"
    description = "Verify the F3-M3 durable cursor facade, lifecycle, reads, callbacks, and locked Pulsar API compile."
    dependsOn("phase3M2Check")
    dependsOn("checkPulsarSourceLock")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("phase3M4PulsarCheck") {
    group = "verification"
    description = "Run the exact F3-M4 Pulsar broker capability, admission, recovery, ack, and admin suites."
    dependsOn("checkPulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    dependsOn("phase3M3Check")
    mustRunAfter("phase2PulsarCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker-common:spotlessJavaCheck",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusTopicFeatureValidatorTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusAcknowledgeValidatorTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusCursorProtocolCapabilityTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusBrokerStorageConfigurationCursorTest",
        "--tests", "org.apache.pulsar.broker.service.persistent.NereusPersistentTopicCursorRecoveryTest",
        "--tests", "org.apache.pulsar.broker.service.persistent.NereusPersistentSubscriptionAckTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusConsumerAckOrderingTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusAdminOperationTest",
        "-PexcludedTestGroups=quarantine,flaky,broker-isolated",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase3M4Check") {
    group = "verification"
    description = "Verify the complete F3-M4 Pulsar broker integration against the exact local fork."
    dependsOn("phase3M3Check")
    dependsOn("phase3M4PulsarCheck")
}

tasks.register("phase3M5Check") {
    group = "verification"
    description = "Verify F3-M5 deterministic recovery cuts and the exact 10,000-cursor scale boundary."
    dependsOn("phase3M4Check")
    dependsOn(":nereus-managed-ledger:test")
}

tasks.register<Exec>("checkPhase3PulsarAdminRoutes") {
    group = "verification"
    description = "Audit every loaded, unloaded, and namespace Nereus admin route in the locked Pulsar fork."
    dependsOn("checkPulsarSourceLock")
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase3-pulsar-admin-routes.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("checkPhase3ContractSurface") {
    group = "verification"
    description = "Audit the code-level Phase 3 production/test inventory and completion invariants."
    dependsOn("checkPulsarSourceLock")
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase3-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("checkPhase3Documentation") {
    group = "verification"
    description = "Verify Phase 3 docs carry the implemented M6 contract, source lock, gates, and F4 handoff."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase3-documentation.sh")
}

tasks.register<Exec>("phase3M5PulsarFinalCheck") {
    group = "verification"
    description = "Run the real two-broker F3-M5 durable cursor recovery, expiry, and coexistence gate."
    dependsOn("checkPulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase3M4PulsarCheck", "phase3M2FinalCheck", "phase2PulsarFinalCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusCursorMultiBrokerIntegrationTest.preservesDurableCursorTruthAcrossUnloadFailoverRestartExpiryAndBookKeeper",
        "--rerun-tasks",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase3M5FinalCheck") {
    group = "verification"
    description = "Run every ordinary and real-service F3-M5 cursor recovery and retention gate."
    dependsOn("phase3M5Check")
    dependsOn("phase3M2FinalCheck")
    dependsOn("phase3M5PulsarFinalCheck")
}

tasks.register("phase3M6Check") {
    group = "verification"
    description = "Verify F3-M6 compatibility, incarnation, rollout, F4 handoff, and admin-route boundaries."
    dependsOn("phase3M5Check")
    dependsOn("checkPhase2StorageIsolation")
    dependsOn("checkPhase3ContractSurface")
    dependsOn("checkPhase3Documentation")
    dependsOn("checkPhase3PulsarAdminRoutes")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("phase3M6PulsarFinalCheck") {
    group = "verification"
    description = "Run the real two-broker F3-M6 MessageId, property, admin-route, and incarnation gate."
    dependsOn("checkPulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase3M5PulsarFinalCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusCursorMultiBrokerIntegrationTest.preservesMessageIdsPropertiesAndIncarnationAcrossCompatibilityCuts",
        "--rerun-tasks",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase3M6FinalCheck") {
    group = "verification"
    description = "Run every ordinary and real-service F3-M6 compatibility and F4-handoff gate."
    dependsOn("phase3M6Check")
    dependsOn("phase3M5FinalCheck")
    dependsOn("phase3M6PulsarFinalCheck")
}

tasks.register("phase3Check") {
    group = "verification"
    description = "Run every ordinary Phase 3 cursor/subscription product gate."
    dependsOn("phase3M6Check")
}

tasks.register("phase3FinalCheck") {
    group = "verification"
    description = "Run the complete Phase 1, 1.5, 2, and 3 release gate."
    dependsOn("phase3Check")
    dependsOn("phase2FinalCheck")
    dependsOn("phase3M6FinalCheck")
}

tasks.register<Exec>("checkPhase4ContractSurface") {
    group = "verification"
    description = "Audit the implemented F4-M1 production, test, transition, and golden surfaces."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-contract-surface.sh")
}

tasks.register<Exec>("checkPhase4Documentation") {
    group = "verification"
    description = "Verify current F4 implementation status, source lock, gates, and documentation links."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-documentation.sh")
}

tasks.register<Exec>("checkPhase4ModuleBoundaries") {
    group = "verification"
    description = "Verify the acyclic protocol-neutral F4 module dependency direction."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-module-boundaries.sh")
}

tasks.register<Exec>("checkPhase4PulsarSourceLock") {
    group = "verification"
    description = "Verify the exact clean local Pulsar source consumed by Phase 4."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-pulsar-source-lock.sh",
        pulsarCheckoutPath.get(),
        pulsarExpectedHead.get(),
    )
}

tasks.register<Exec>("checkPhase4GuardedObjectPut") {
    group = "verification"
    description = "Audit authorization immediately before every F4 provider PUT attempt."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-guarded-object-put.sh")
}

tasks.register("phase4M1Check") {
    group = "verification"
    description = "Verify F4-M1 metadata, object lifecycle/IO, durable pins/protections, and module boundaries."
    dependsOn("phase3M6Check")
    dependsOn("checkPhase4ContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn("checkPhase4GuardedObjectPut")
    dependsOn(":nereus-api:check")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-metadata-oxia:compileOxiaIntegrationTestJava")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-object-store:compileS3IntegrationTestJava")
}

tasks.register("phase4M1FinalCheck") {
    group = "verification"
    description = "Run the ordinary and Docker-backed real Oxia/LocalStack F4-M1 gates."
    dependsOn("phase4M1Check")
    dependsOn(":nereus-metadata-oxia:f4OxiaIntegrationTest")
    dependsOn(":nereus-object-store:s3IntegrationTest")
}

tasks.register<Exec>("checkPhase4M2ContractSurface") {
    group = "verification"
    description = "Audit the implemented F4-M2 publication, resolver, fallback, test, and documentation surfaces."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m2-contract-surface.sh")
}

tasks.register("phase4M2Check") {
    group = "verification"
    description = "Verify F4-M2 generation publication, committed reads, bounded retry, and quarantine propagation."
    dependsOn("phase4M1Check")
    dependsOn("checkPhase4M2ContractSurface")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-materialization:compileF4M2IntegrationTestJava")
    dependsOn(":nereus-metadata-oxia:check")
}

tasks.register("phase4M2FinalCheck") {
    group = "verification"
    description = "Run ordinary and Docker-backed real Oxia/LocalStack F4-M2 publication and fallback gates."
    dependsOn("phase4M2Check")
    dependsOn(":nereus-materialization:f4M2IntegrationTest")
}

tasks.register<Exec>("checkPhase4M3ContractSurface") {
    group = "verification"
    description = "Audit the implemented F4-M3 format, planner, worker, recovery, and test surfaces."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m3-contract-surface.sh")
}

tasks.register("phase4M3Check") {
    group = "verification"
    description = "Verify F4-M3 compacted formats, planning, workers, recovery, and bounded lifecycle."
    dependsOn("phase4M2Check")
    dependsOn("checkPhase4M3ContractSurface")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-materialization:compileF4M3IntegrationTestJava")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
}

tasks.register("phase4M3FinalCheck") {
    group = "verification"
    description = "Run ordinary and Docker-backed real Oxia/LocalStack F4-M3 materialization gates."
    dependsOn("phase4M3Check")
    dependsOn("phase4M2FinalCheck")
    dependsOn(":nereus-materialization:f4M3IntegrationTest")
}

tasks.register<Exec>("checkPhase4M4CheckpointContractSurface") {
    group = "verification"
    description = "Audit the in-progress F4-M4 NRC1 object-protocol implementation checkpoint."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-checkpoint-contract-surface.sh")
}

tasks.register("phase4M4CheckpointCheck") {
    group = "verification"
    description = "Verify the in-progress F4-M4 NRC1 streaming codec, strict reader, and metadata verifier."
    dependsOn("phase4M3Check")
    dependsOn("checkPhase4M4CheckpointContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-object-store:check")
}

tasks.register<Exec>("checkPhase4M4ProtectedAppendContractSurface") {
    group = "verification"
    description = "Audit the in-progress F4-M4 protected generation-zero append checkpoint."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-protected-append-contract-surface.sh")
}

tasks.register("phase4M4ProtectedAppendCheck") {
    group = "verification"
    description = "Verify exact intent/root/protection ordering for generation-zero append and recovery."
    dependsOn("phase4M4CheckpointCheck")
    dependsOn("checkPhase4M4ProtectedAppendContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-core:compilePhase1IntegrationTestJava")
    dependsOn(":nereus-materialization:compileF4M2IntegrationTestJava")
    dependsOn(":nereus-materialization:compileF4M3IntegrationTestJava")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-metadata-oxia:compileOxiaIntegrationTestJava")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M4RecoveryRootContractSurface") {
    group = "verification"
    description = "Audit the in-progress F4-M4 recovery-root publication and reconciliation checkpoint."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-recovery-root-contract-surface.sh")
}

tasks.register("phase4M4RecoveryRootCheck") {
    group = "verification"
    description = "Verify anchor-aware NRC1 root publication, response-loss recovery, and protection repair."
    dependsOn("phase4M4ProtectedAppendCheck")
    dependsOn("checkPhase4M4RecoveryRootContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
}

tasks.register<Exec>("checkPhase4M4CheckpointReplayContractSurface") {
    group = "verification"
    description = "Audit the in-progress F4-M4 checkpoint-aware append replay adapter."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-checkpoint-replay-contract-surface.sh")
}

tasks.register("phase4M4CheckpointReplayCheck") {
    group = "verification"
    description = "Verify root-stable pinned append replay across live commits and NRC1 checkpoint entries."
    dependsOn("phase4M4RecoveryRootCheck")
    dependsOn("checkPhase4M4CheckpointReplayContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
}

tasks.register<Exec>("checkPhase4M4CheckpointIndexRepairContractSurface") {
    group = "verification"
    description = "Audit the in-progress F4-M4 checkpoint-derived generation-index repair checkpoint."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-checkpoint-index-repair-contract-surface.sh")
}

tasks.register("phase4M4CheckpointIndexRepairCheck") {
    group = "verification"
    description = "Verify root-stable protected committed-index restoration from NRC1 checkpoint evidence."
    dependsOn("phase4M4CheckpointReplayCheck")
    dependsOn("checkPhase4M4CheckpointIndexRepairContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
}

tasks.register<Exec>("checkPhase4M4RetirementMetadataContractSurface") {
    group = "verification"
    description = "Audit the in-progress F4-M4 exact source/object-audit retirement metadata checkpoint."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-retirement-metadata-contract-surface.sh")
}

tasks.register("phase4M4RetirementMetadataCheck") {
    group = "verification"
    description = "Verify exact read-before-delete metadata retirement without enabling physical deletion."
    dependsOn("phase4M4CheckpointIndexRepairCheck")
    dependsOn("checkPhase4M4RetirementMetadataContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-metadata-oxia:check")
}

tasks.register<Exec>("checkPhase4M4GcPlanContractSurface") {
    group = "verification"
    description = "Audit the in-progress F4-M4 bounded reconstructable GC plan checkpoint."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-gc-plan-contract-surface.sh")
}

tasks.register("phase4M4GcPlanCheck") {
    group = "verification"
    description = "Verify GC configuration, candidate identity, and canonical restart-rebuild plan facts."
    dependsOn("phase4M4RetirementMetadataCheck")
    dependsOn("checkPhase4M4GcPlanContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-materialization:check")
}

tasks.register<Exec>("checkPhase4M4RootFenceContractSurface") {
    group = "verification"
    description = "Audit the in-progress F4-M4 reference-domain and physical-root fence checkpoint."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-root-fence-contract-surface.sh")
}

tasks.register("phase4M4RootFenceCheck") {
    group = "verification"
    description = "Verify reference-domain collection and recoverable MARK/DRAIN/DELETING root fencing."
    dependsOn("phase4M4GcPlanCheck")
    dependsOn("checkPhase4M4RootFenceContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-materialization:check")
}

tasks.register<Exec>("checkPhase4M4ReferenceDomainsContractSurface") {
    group = "verification"
    description = "Audit query-bound F4-M4 generation, append-recovery, and materialization reference domains."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-reference-domains-contract-surface.sh")
}

tasks.register("phase4M4ReferenceDomainsCheck") {
    group = "verification"
    description = "Verify stateless reference revalidation and exact removal binding over real metadata scans."
    dependsOn("phase4M4RootFenceCheck")
    dependsOn("checkPhase4M4ReferenceDomainsContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-materialization:check")
}

tasks.register<Exec>("checkPhase4M4ManagedLedgerDomainsContractSurface") {
    group = "verification"
    description = "Audit exact F2 projection and F3 cursor GC reference domains."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-managed-ledger-domains-contract-surface.sh")
}

tasks.register("phase4M4ManagedLedgerDomainsCheck") {
    group = "verification"
    description = "Verify composed generation markers and restart-safe projection/cursor authorities."
    dependsOn("phase4M4ReferenceDomainsCheck")
    dependsOn("checkPhase4M4ManagedLedgerDomainsContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
}

tasks.register<Exec>("checkPhase4M4RetirementJournalContractSurface") {
    group = "verification"
    description = "Audit manifest-last retirement journal persistence and PREPARE-before-MARK fencing."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-retirement-journal-contract-surface.sh")
}

tasks.register("phase4M4RetirementJournalCheck") {
    group = "verification"
    description = "Verify root-authenticated journal sealing, restart reload, and fail-closed intent admission."
    dependsOn("phase4M4ManagedLedgerDomainsCheck")
    dependsOn("checkPhase4M4RetirementJournalContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-materialization:check")
}

tasks.register("phase4M4DestructiveRecoveryCheck") {
    group = "verification"
    description = "Verify root-authenticated DELETING recovery, exact object deletion, and DELETED convergence."
    dependsOn("phase4M4RetirementJournalCheck")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-materialization:check")
}

tasks.register<Exec>("checkPhase4M4GenerationRetirementContractSurface") {
    group = "verification"
    description = "Audit typed source retirement and view-specific/below-trim higher-generation pre-drain fencing."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-generation-retirement-contract-surface.sh")
}

tasks.register("phase4M4GenerationRetirementCheck") {
    group = "verification"
    description = "Verify exact source deletion, view-specific/below-trim eligibility, higher pre-drain, and retirement recovery."
    dependsOn("phase4M4DestructiveRecoveryCheck")
    dependsOn("checkPhase4M4GenerationRetirementContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-api:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-materialization:check")
}
