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

import org.gradle.api.services.BuildService
import org.gradle.api.services.BuildServiceParameters

abstract class DockerIntegrationGateService : BuildService<BuildServiceParameters.None>

abstract class PulsarCheckoutGateService : BuildService<BuildServiceParameters.None>

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
val dockerIntegrationGate = gradle.sharedServices.registerIfAbsent(
    "nereusDockerIntegrationGate",
    DockerIntegrationGateService::class,
) {
    maxParallelUsages.set(1)
}
val pulsarCheckoutGate = gradle.sharedServices.registerIfAbsent(
    "nereusPulsarCheckoutGate",
    PulsarCheckoutGateService::class,
) {
    maxParallelUsages.set(1)
}
val dockerBackedSubprojectTasks = mapOf(
    ":nereus-core" to setOf("phase1IntegrationTest"),
    ":nereus-managed-ledger" to setOf("cursorS3IntegrationTest", "cursorM2IntegrationTest"),
    ":nereus-materialization" to setOf("f4M2IntegrationTest", "f4M3IntegrationTest"),
    ":nereus-metadata-oxia" to setOf(
        "oxiaCapabilitySpike",
        "oxiaIntegrationTest",
        "f4OxiaIntegrationTest",
    ),
    ":nereus-object-store" to setOf("s3IntegrationTest"),
    ":nereus-pulsar-adapter" to setOf("f4M4IntegrationTest"),
)
val dockerBackedPulsarExecTasks = setOf(
    "phase2PulsarFinalCheck",
    "phase3M5PulsarFinalCheck",
    "phase3M6PulsarFinalCheck",
    "phase4M4PhysicalGcMultiBrokerPulsarCheck",
    "phase4M5AsyncRetentionMultiBrokerPulsarCheck",
    "phase4M6TwoBrokerWorkerContentionPulsarCheck",
)
val pulsarCheckoutExecTasks = setOf(
    "phase2PulsarCheck",
    "phase2PulsarFinalCheck",
    "phase3M4PulsarCheck",
    "phase3M5PulsarFinalCheck",
    "phase3M6PulsarFinalCheck",
    "phase4M4PhysicalGcConfigPulsarCheck",
    "phase4M4PhysicalDeletionActivationPulsarCheck",
    "phase4M4ReadinessRolloverPulsarCheck",
    "phase4M4PhysicalGcMultiBrokerPulsarCheck",
    "phase4M5GenerationCapabilityPulsarCheck",
    "phase4M5RegistrationBackfillPulsarCheck",
    "phase4M5ActivationGuardPulsarCheck",
    "phase4M5PublicationActivationPulsarCheck",
    "phase4M5RetentionRuntimePulsarCheck",
    "phase4M5RetentionPolicyAdminPulsarCheck",
    "phase4M5AsyncRetentionMultiBrokerPulsarCheck",
    "phase4M6TwoBrokerWorkerContentionPulsarCheck",
)

tasks.matching { it.name in dockerBackedPulsarExecTasks }.configureEach {
    usesService(dockerIntegrationGate)
}

tasks.matching { it.name in pulsarCheckoutExecTasks }.configureEach {
    usesService(pulsarCheckoutGate)
}

subprojects {
    group = rootProject.group
    version = rootProject.version
    dockerBackedSubprojectTasks[path]?.let { taskNames ->
        tasks.matching { it.name in taskNames }.configureEach {
            usesService(dockerIntegrationGate)
        }
    }
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
    .orElse("eaf7b9a704890a9265c21f30d9f351e02d00c600")

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
    ":nereus-materialization",
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
        "--rerun-tasks",
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
        "--rerun-tasks",
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

tasks.register<Exec>("bookKeeperPrimaryWalDocumentationCheck") {
    group = "verification"
    description = "Verify the F1-BK code-level design, source locks, non-implementation status, and documentation links."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-bookkeeper-primary-wal-documentation.sh")
}

tasks.register<Exec>("checkBookKeeperModuleBoundaries") {
    group = "verification"
    description = "Verify BookKeeper provider code stays outside L0 and ManagedLedger implementation types."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-bookkeeper-module-boundaries.sh")
}

tasks.register("bookKeeperPrimaryWalM1Check") {
    group = "verification"
    description = "Verify the provider-neutral read/append seam and BookKeeper 4.18 adapter foundation."
    dependsOn("bookKeeperPrimaryWalDocumentationCheck")
    dependsOn("checkBookKeeperModuleBoundaries")
    dependsOn(":nereus-api:test")
    dependsOn(":nereus-core:test")
    dependsOn(":nereus-metadata-oxia:test")
    dependsOn(":nereus-object-store:test")
    dependsOn(":nereus-materialization:test")
    dependsOn(":nereus-bookkeeper:test")
}

tasks.register("bookKeeperPrimaryWalM1FinalCheck") {
    group = "verification"
    description = "Run BK-M1 plus every final-gated Phase 1.5 and Phase 4 predecessor."
    dependsOn("bookKeeperPrimaryWalM1Check")
    dependsOn("phase15FinalCheck")
    dependsOn("phase4FinalCheck")
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

tasks.register<Exec>("checkPhase4M4ActivationMetadataContractSurface") {
    group = "verification"
    description = "Audit the durable generation-protocol activation metadata foundation."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-activation-metadata-contract-surface.sh")
}

tasks.register("phase4M4ActivationMetadataCheck") {
    group = "verification"
    description = "Verify the exact cluster activation record, codec, monotonic CAS, and frozen vectors."
    dependsOn("phase4M4GenerationRetirementCheck")
    dependsOn("checkPhase4M4ActivationMetadataContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-metadata-oxia:check")
}

tasks.register<Exec>("checkPhase4M4GlobalDomainsContractSurface") {
    group = "verification"
    description = "Audit activation-gated ownerless global scope and future catalog sentinel."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-global-domains-contract-surface.sh")
}

tasks.register("phase4M4GlobalDomainsCheck") {
    group = "verification"
    description = "Verify all five ownerless global domains and future-domain fail-closed capability checks."
    dependsOn("phase4M4ActivationMetadataCheck")
    dependsOn("checkPhase4M4GlobalDomainsContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-managed-ledger:check")
}

tasks.register<Exec>("checkPhase4M4TombstoneRetirementContractSurface") {
    group = "verification"
    description = "Audit dual-absence, audit-order, and root-last DELETED tombstone retirement."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-tombstone-retirement-contract-surface.sh")
}

tasks.register("phase4M4TombstoneRetirementCheck") {
    group = "verification"
    description = "Verify restart-safe DELETED-root and Phase 1 object-audit retirement."
    dependsOn("phase4M4GlobalDomainsCheck")
    dependsOn("checkPhase4M4TombstoneRetirementContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-materialization:check")
}

tasks.register<Exec>("checkPhase4M4CursorProtectionContractSurface") {
    group = "verification"
    description = "Audit guarded cursor snapshot publication, permanent protection, and durable read pinning."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-cursor-protection-contract-surface.sh")
}

tasks.register("phase4M4CursorProtectionCheck") {
    group = "verification"
    description = "Verify the F4-protected cursor snapshot write/read frontier and runtime wiring."
    dependsOn("phase4M4TombstoneRetirementCheck")
    dependsOn("checkPhase4M4CursorProtectionContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-managed-ledger:compileCursorS3IntegrationTestJava")
    dependsOn(":nereus-managed-ledger:compileCursorM2IntegrationTestJava")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M4PhysicalRootBackfillContractSurface") {
    group = "verification"
    description = "Audit all-shard physical-root/cursor-root backfill and activation-proof closure."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-physical-root-backfill-contract-surface.sh")
}

tasks.register("phase4M4PhysicalRootBackfillCheck") {
    group = "verification"
    description = "Verify stable live-reference backfill, exact roots/protections, and dual activation proofs."
    dependsOn("phase4M4CursorProtectionCheck")
    dependsOn("checkPhase4M4PhysicalRootBackfillContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
}

tasks.register<Exec>("checkPhase4M4CursorSnapshotGcContractSurface") {
    group = "verification"
    description = "Audit complete cursor-snapshot inventory and post-drain GC revalidation."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-cursor-snapshot-gc-contract-surface.sh")
}

tasks.register("phase4M4CursorSnapshotGcCheck") {
    group = "verification"
    description = "Verify bounded cursor-snapshot candidate discovery and final authority revalidation."
    dependsOn("phase4M4PhysicalRootBackfillCheck")
    dependsOn("checkPhase4M4CursorSnapshotGcContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
}

tasks.register<Exec>("checkPhase4M4CursorGcExecutionContractSurface") {
    group = "verification"
    description = "Audit restart-reconstructable cursor-GC plan execution and safe runtime composition."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-cursor-gc-execution-contract-surface.sh")
}

tasks.register("phase4M4CursorGcExecutionCheck") {
    group = "verification"
    description = "Verify cursor snapshot MARK/drain/restart/delete execution with production deletion still default-off."
    dependsOn("phase4M4CursorSnapshotGcCheck")
    dependsOn("checkPhase4M4CursorGcExecutionContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M4ObjectInventoryContractSurface") {
    group = "verification"
    description = "Audit known-prefix orphan inventory, strict key inverses, and exact missing-root registration."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-object-inventory-contract-surface.sh")
}

tasks.register("phase4M4ObjectInventoryCheck") {
    group = "verification"
    description = "Verify old exact-HEAD orphan inventory registration without listing-based deletion authority."
    dependsOn("phase4M4CursorGcExecutionCheck")
    dependsOn("checkPhase4M4ObjectInventoryContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M4RegistrationRetirementContractSurface") {
    group = "verification"
    description = "Audit exact deleted-stream authority, owner drain ordering, and registration-last retirement."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-registration-retirement-contract-surface.sh")
}

tasks.register("phase4M4RegistrationRetirementCheck") {
    group = "verification"
    description = "Verify bounded stream-registration retirement and response-loss convergence without object deletion."
    dependsOn("phase4M4ObjectInventoryCheck")
    dependsOn("checkPhase4M4RegistrationRetirementContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M4LifecycleSchedulingContractSurface") {
    group = "verification"
    description = "Audit metadata-first root/registration/inventory scheduling and lifecycle recovery routing."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m4-lifecycle-scheduling-contract-surface.sh")
}

tasks.register("phase4M4LifecycleSchedulingCheck") {
    group = "verification"
    description = "Verify non-overlapping periodic physical-GC passes with restart-safe lifecycle routing."
    dependsOn("phase4M4RegistrationRetirementCheck")
    dependsOn("checkPhase4M4LifecycleSchedulingContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M4PhysicalGcConfigContractSurface") {
    group = "verification"
    description = "Audit exact broker physical-GC configuration mapping and provider consumption."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-physical-gc-config-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("phase4M4PhysicalGcConfigPulsarCheck") {
    group = "verification"
    description = "Run locked Pulsar physical-GC configuration formatting, style, and focused tests."
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase4M4LifecycleSchedulingCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker-common:spotlessJavaCheck",
        ":pulsar-broker-common:checkstyleMain",
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusBrokerStorageConfigurationTest",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky,broker-isolated",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase4M4PhysicalGcConfigCheck") {
    group = "verification"
    description = "Verify checkpoint AO exact broker GC mapping while coverage and physical deletion stay closed."
    dependsOn("phase4M4LifecycleSchedulingCheck")
    dependsOn("checkPhase4M4PhysicalGcConfigContractSurface")
    dependsOn("phase4M4PhysicalGcConfigPulsarCheck")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M4ObjectStoreCapabilityContractSurface") {
    group = "verification"
    description = "Audit the configured-scope guarded PUT/HEAD/LIST/exact-DELETE capability proof."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-object-store-capability-contract-surface.sh",
    )
}

tasks.register("phase4M4ObjectStoreCapabilityCheck") {
    group = "verification"
    description = "Verify checkpoint AP object-store destructive-protocol capability without activating deletion."
    dependsOn("phase4M4PhysicalGcConfigCheck")
    dependsOn("checkPhase4M4ObjectStoreCapabilityContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-object-store:check")
}

tasks.register<Exec>("checkPhase4M4PhysicalDeletionActivationContractSurface") {
    group = "verification"
    description = "Audit ordered proof composition, atomic delete activation, and restart scope fencing."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-physical-deletion-activation-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("phase4M4PhysicalDeletionActivationPulsarCheck") {
    group = "verification"
    description = "Run locked Pulsar physical-deletion activation sequencing, formatting, style, and tests."
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    // Both checkpoints invoke the same locked Pulsar checkout with --rerun-tasks.
    // Keep them serialized so one build cannot remove class outputs while the other compiles.
    mustRunAfter("phase4M4ObjectStoreCapabilityCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusManagedLedgerStorageGenerationActivationTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusBrokerStorageConfigurationTest",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky,broker-isolated",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase4M4PhysicalDeletionActivationCheck") {
    group = "verification"
    description = "Verify checkpoint AR product composition, atomic activation, and exact-scope restart recovery."
    dependsOn("phase4M4ObjectStoreCapabilityCheck")
    dependsOn("checkPhase4M4PhysicalDeletionActivationContractSurface")
    dependsOn("phase4M4PhysicalDeletionActivationPulsarCheck")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M4PhysicalDeletionIntegrationContractSurface") {
    group = "verification"
    description = "Audit shared ownerless reference-domain assembly and the real Oxia/LocalStack recovery fixture."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-physical-deletion-integration-contract-surface.sh",
    )
}

tasks.register("phase4M4PhysicalDeletionIntegrationCheck") {
    group = "verification"
    description = "Verify checkpoint AS real Oxia/LocalStack activation, scope fencing, and destructive restart recovery."
    dependsOn("phase4M4PhysicalDeletionActivationCheck")
    dependsOn("checkPhase4M4PhysicalDeletionIntegrationContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-pulsar-adapter:f4M4IntegrationTest")
}

tasks.register<Exec>("checkPhase4M4PostDeleteCrashRecoveryContractSurface") {
    group = "verification"
    description = "Audit the real post-DELETE/pre-root-CAS process-death recovery fixture."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-post-delete-crash-recovery-contract-surface.sh",
    )
}

tasks.register("phase4M4PostDeleteCrashRecoveryCheck") {
    group = "verification"
    description = "Verify checkpoint AT durable DELETING recovery after process death following real object DELETE."
    dependsOn("phase4M4PhysicalDeletionIntegrationCheck")
    dependsOn("checkPhase4M4PostDeleteCrashRecoveryContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-pulsar-adapter:f4M4IntegrationTest")
}

tasks.register<Exec>("checkPhase4M4DeletedCasResponseLossContractSurface") {
    group = "verification"
    description = "Audit exact reload after a real DELETED-root CAS applies but its response is lost."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-deleted-cas-response-loss-contract-surface.sh",
    )
}

tasks.register("phase4M4DeletedCasResponseLossCheck") {
    group = "verification"
    description = "Verify checkpoint AU real DELETED-root CAS response-loss convergence without repeated object DELETE."
    dependsOn("phase4M4PostDeleteCrashRecoveryCheck")
    dependsOn("checkPhase4M4DeletedCasResponseLossContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-pulsar-adapter:f4M4IntegrationTest")
}

tasks.register<Exec>("checkPhase4M4TwoWorkerConvergenceContractSurface") {
    group = "verification"
    description = "Audit deterministic two-runtime DELETING-CAS contention and exact-delete convergence."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-two-worker-convergence-contract-surface.sh",
    )
}

tasks.register("phase4M4TwoWorkerConvergenceCheck") {
    group = "verification"
    description = "Verify checkpoint AV two independent workers converge one durable delete intent against real services."
    dependsOn("phase4M4DeletedCasResponseLossCheck")
    dependsOn("checkPhase4M4TwoWorkerConvergenceContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-pulsar-adapter:f4M4IntegrationTest")
}

tasks.register<Exec>("checkPhase4M4AllShardRecoveryContractSurface") {
    group = "verification"
    description = "Audit all-shard mixed-lifecycle recovery and opaque object-list continuation semantics."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-all-shard-recovery-contract-surface.sh",
    )
}

tasks.register("phase4M4AllShardRecoveryCheck") {
    group = "verification"
    description = "Verify checkpoint AW all 256 root shards recover from durable metadata with empty object inventory."
    dependsOn("phase4M4TwoWorkerConvergenceCheck")
    dependsOn("checkPhase4M4AllShardRecoveryContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:f4M4IntegrationTest")
}

tasks.register<Exec>("checkPhase4M4RootScaleContractSurface") {
    group = "verification"
    description = "Audit the real-Oxia 1,001-root hot-shard pagination and fresh-process scale fixture."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-root-scale-contract-surface.sh",
    )
}

tasks.register("phase4M4RootScaleCheck") {
    group = "verification"
    description = "Verify checkpoint AX scans 1,001 roots in one shard plus every other root shard after restart."
    dependsOn("phase4M4AllShardRecoveryCheck")
    dependsOn("checkPhase4M4RootScaleContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:f4M4IntegrationTest")
}

tasks.register<Exec>("checkPhase4M4TombstoneScaleContractSurface") {
    group = "verification"
    description = "Audit the 10,000-root dual-window retirement and cancelled-deadline memory bound."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-tombstone-scale-contract-surface.sh",
    )
}

tasks.register("phase4M4TombstoneScaleCheck") {
    group = "verification"
    description = "Verify checkpoint AY retires 10,000 DELETED roots through two bounded absence windows."
    dependsOn("phase4M4RootScaleCheck")
    dependsOn("checkPhase4M4TombstoneScaleContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M4CursorGcScaleContractSurface") {
    group = "verification"
    description = "Audit stack-bounded 10,000-candidate visitation and exact 10,000-root cursor-snapshot GC."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-cursor-gc-scale-contract-surface.sh",
    )
}

tasks.register("phase4M4CursorGcScaleCheck") {
    group = "verification"
    description = "Verify checkpoint AZ classifies and deletes live/old/CAS-lost/deleted-cursor snapshots at 10,000 roots."
    dependsOn("phase4M4TombstoneScaleCheck")
    dependsOn("checkPhase4M4CursorGcScaleContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M4SourceProtectionCutContractSurface") {
    group = "verification"
    description = "Audit restart-safe source/protection retirement cuts and applied-delete response loss."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-source-protection-cut-contract-surface.sh",
    )
}

tasks.register("phase4M4SourceProtectionCutCheck") {
    group = "verification"
    description = "Verify checkpoint BA resumes exact DELETING journals after source/protection deletion cuts."
    dependsOn("phase4M4CursorGcScaleCheck")
    dependsOn("checkPhase4M4SourceProtectionCutContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:check")
    dependsOn(":nereus-pulsar-adapter:f4M4IntegrationTest")
}

tasks.register<Exec>("checkPhase4M4LatePutTombstoneContractSurface") {
    group = "verification"
    description = "Audit guarded Object-WAL retries, every tombstone cut, and external-key reappearance recovery."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-late-put-tombstone-contract-surface.sh",
    )
}

tasks.register("phase4M4LatePutTombstoneCheck") {
    group = "verification"
    description = "Verify checkpoint BB rejects stale first/retried PUTs and reclaims external post-root bytes."
    dependsOn("phase4M4SourceProtectionCutCheck")
    dependsOn("checkPhase4GuardedObjectPut")
    dependsOn("checkPhase4M4LatePutTombstoneContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:check")
    dependsOn(":nereus-pulsar-adapter:f4M4IntegrationTest")
}

tasks.register<Exec>("checkPhase4M4ReadinessRolloverContractSurface") {
    group = "verification"
    description = "Audit deletion-active readiness rollover without partial proof publication."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-readiness-rollover-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("phase4M4ReadinessRolloverPulsarCheck") {
    group = "verification"
    description = "Run locked Pulsar readiness-rollover bound formatting, style, and focused tests."
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase4M4LatePutTombstoneCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker-common:spotlessJavaCheck",
        ":pulsar-broker-common:checkstyleMain",
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusGenerationRegistrationBackfillTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusBrokerStorageConfigurationTest",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky,broker-isolated",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase4M4ReadinessRolloverCheck") {
    group = "verification"
    description = "Verify checkpoint BC atomically refreshes deletion authority after broker readiness changes."
    dependsOn("phase4M4LatePutTombstoneCheck")
    dependsOn("checkPhase4M4ReadinessRolloverContractSurface")
    dependsOn("phase4M4ReadinessRolloverPulsarCheck")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register("phase4M4Check") {
    group = "verification"
    description = "Run every ordinary and real-service Nereus F4-M4 product gate."
    dependsOn("phase4M4ReadinessRolloverCheck")
}

tasks.register<Exec>("checkPhase4M4FinalContractSurface") {
    group = "verification"
    description = "Audit stable L0 authority and the real two-broker physical-GC acceptance fixture."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m4-final-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("phase4M4PhysicalGcMultiBrokerPulsarCheck") {
    group = "verification"
    description = "Run the real two-broker Nereus physical-GC, MessageId, and BookKeeper coexistence gate."
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase4M4Check")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusPhysicalGcMultiBrokerIntegrationTest",
        "--rerun-tasks",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
        "-PtestRetryCount=0",
    )
}

tasks.register("phase4M4FinalCheck") {
    group = "verification"
    description = "Run the complete F4-M4 release gate including real two-broker physical deletion and failover."
    dependsOn("phase4M4Check")
    dependsOn("checkPhase4M4FinalContractSurface")
    dependsOn("phase4M4PhysicalGcMultiBrokerPulsarCheck")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
}

tasks.register<Exec>("checkPhase4M5RegistrationFrontierContractSurface") {
    group = "verification"
    description = "Audit exact managed-ledger registration before every topic-open return."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m5-registration-frontier-contract-surface.sh")
}

tasks.register("phase4M5RegistrationFrontierCheck") {
    group = "verification"
    description = "Verify the F4 registration new-write/open frontier and shared production wiring."
    dependsOn("phase4M4FinalCheck")
    dependsOn("checkPhase4M5RegistrationFrontierContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M5GenerationCapabilityContractSurface") {
    group = "verification"
    description = "Audit the locked Pulsar generation capability, readiness identity, and invalidation surface."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m5-generation-capability-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("phase4M5GenerationCapabilityPulsarCheck") {
    group = "verification"
    description = "Run the exact Pulsar generation capability/readiness formatting, style, and focused tests."
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase4M5RegistrationFrontierCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusGenerationProtocolCapabilityTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusCursorProtocolCapabilityTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusStorageBindingCapabilityTest",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky,broker-isolated",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase4M5GenerationCapabilityCheck") {
    group = "verification"
    description = "Verify checkpoint Y generation capability and deterministic stable broker readiness."
    dependsOn("phase4M5RegistrationFrontierCheck")
    dependsOn("checkPhase4M5GenerationCapabilityContractSurface")
    dependsOn("phase4M5GenerationCapabilityPulsarCheck")
    dependsOn("checkPhase4Documentation")
}

tasks.register<Exec>("checkPhase4M5RegistrationBackfillContractSurface") {
    group = "verification"
    description = "Audit the exact unloaded-topic registration candidate and canonical broker backfill surface."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m5-registration-backfill-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("phase4M5RegistrationBackfillPulsarCheck") {
    group = "verification"
    description = "Run the locked Pulsar registration-backfill formatting, style, and focused tests."
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase4M5GenerationCapabilityCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker-common:spotlessJavaCheck",
        ":pulsar-broker-common:checkstyleMain",
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusGenerationRegistrationBackfillTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusBrokerStorageConfigurationTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusGenerationProtocolCapabilityTest",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky,broker-isolated",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase4M5RegistrationBackfillCheck") {
    group = "verification"
    description = "Verify checkpoint Z canonical cold-topic registration traversal and bounded report."
    dependsOn("phase4M5GenerationCapabilityCheck")
    dependsOn("checkPhase4M5RegistrationBackfillContractSurface")
    dependsOn("phase4M5RegistrationBackfillPulsarCheck")
    dependsOn("checkPhase4Documentation")
    dependsOn(":nereus-managed-ledger:check")
}

tasks.register<Exec>("checkPhase4M5RegistrationProofContractSurface") {
    group = "verification"
    description = "Audit the exact broker-readiness handoff and product-owned durable registration proof CAS."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m5-registration-proof-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register("phase4M5RegistrationProofCheck") {
    group = "verification"
    description = "Verify checkpoint AA durable stream-registration backfill proof completion."
    dependsOn("phase4M5RegistrationBackfillCheck")
    dependsOn("checkPhase4M5RegistrationProofContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M5ActivationGuardContractSurface") {
    group = "verification"
    description = "Audit the product-owned generation activation guard and disabled-by-default broker switch."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m5-activation-guard-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("phase4M5ActivationGuardPulsarCheck") {
    group = "verification"
    description = "Run the locked Pulsar activation-switch formatting, style, and focused configuration test."
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase4M5RegistrationProofCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker-common:spotlessJavaCheck",
        ":pulsar-broker-common:checkstyleMain",
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusBrokerStorageConfigurationTest",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky,broker-isolated",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase4M5ActivationGuardCheck") {
    group = "verification"
    description = "Verify checkpoint AB exact generation activation admission and runtime composition."
    dependsOn("phase4M5RegistrationProofCheck")
    dependsOn("checkPhase4M5ActivationGuardContractSurface")
    dependsOn("phase4M5ActivationGuardPulsarCheck")
    dependsOn("checkPhase4Documentation")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M5PublicationActivationContractSurface") {
    group = "verification"
    description = "Audit proof-gated PREPARED-to-ACTIVE publication activation and broker sequencing."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m5-publication-activation-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("phase4M5PublicationActivationPulsarCheck") {
    group = "verification"
    description = "Run the locked Pulsar proof-to-publication activation formatting, style, and focused tests."
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase4M5ActivationGuardCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusGenerationRegistrationBackfillTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusManagedLedgerStorageGenerationActivationTest",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky,broker-isolated",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase4M5PublicationActivationCheck") {
    group = "verification"
    description = "Verify checkpoint AC publication-only cluster activation after exact registration proof."
    dependsOn("phase4M5ActivationGuardCheck")
    dependsOn("checkPhase4M5PublicationActivationContractSurface")
    dependsOn("phase4M5PublicationActivationPulsarCheck")
    dependsOn("checkPhase4Documentation")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M5AsyncObjectWalContractSurface") {
    group = "verification"
    description = "Audit the opt-in async Object-WAL acknowledgement and protected generation-zero repair surface."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m5-async-object-wal-contract-surface.sh")
}

tasks.register("phase4M5AsyncObjectWalCheck") {
    group = "verification"
    description = "Verify checkpoint AD async Object-WAL acknowledgement and protected read/restart repair."
    dependsOn("phase4M5PublicationActivationCheck")
    dependsOn("checkPhase4M5AsyncObjectWalContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-core:check")
}

tasks.register<Exec>("checkPhase4M5RetentionPlannerContractSurface") {
    group = "verification"
    description = "Audit the exact policy, stable candidate, and F3-delegated logical-retention surface."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-m5-retention-planner-contract-surface.sh")
}

tasks.register("phase4M5RetentionPlannerCheck") {
    group = "verification"
    description = "Verify checkpoint AG stable logical-retention planning and ownership-safe F3 trim delegation."
    dependsOn("phase4M5AsyncObjectWalCheck")
    dependsOn("checkPhase4M5RetentionPlannerContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-managed-ledger:check")
}

tasks.register<Exec>("checkPhase4M5RetentionRuntimeContractSurface") {
    group = "verification"
    description = "Audit the bounded retention lane, production ledger wiring, and Pulsar config mapping."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m5-retention-runtime-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("phase4M5RetentionRuntimePulsarCheck") {
    group = "verification"
    description = "Run the locked Pulsar logical-retention configuration formatting, style, and focused test."
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase4M5RetentionPlannerCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker-common:spotlessJavaCheck",
        ":pulsar-broker-common:checkstyleMain",
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusBrokerStorageConfigurationTest",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky,broker-isolated",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase4M5RetentionRuntimeCheck") {
    group = "verification"
    description = "Verify checkpoint AH bounded retention execution and production configuration composition."
    dependsOn("phase4M5RetentionPlannerCheck")
    dependsOn("checkPhase4M5RetentionRuntimeContractSurface")
    dependsOn("phase4M5RetentionRuntimePulsarCheck")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M5RetentionPolicyAdminContractSurface") {
    group = "verification"
    description = "Audit exact Pulsar retention policy projection and generation-gated admin routing."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m5-retention-policy-admin-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("phase4M5RetentionPolicyAdminPulsarCheck") {
    group = "verification"
    description = "Run locked Pulsar exact policy/admin formatting, style, and focused tests."
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase4M5RetentionRuntimeCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusTopicFeatureResolverTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusTopicFeatureValidatorTest",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusAdminOperationTest",
        "--tests", "org.apache.pulsar.broker.service.persistent.PersistentTopicNereusAdmissionTest",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky,broker-isolated",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
    )
}

tasks.register("phase4M5RetentionPolicyAdminCheck") {
    group = "verification"
    description = "Verify checkpoint AI exact topic policy admission and loaded/unloaded logical trim routing."
    dependsOn("phase4M5RetentionRuntimeCheck")
    dependsOn("checkPhase4M5RetentionPolicyAdminContractSurface")
    dependsOn("phase4M5RetentionPolicyAdminPulsarCheck")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M5FinalContractSurface") {
    group = "verification"
    description = "Audit the complete async Object-WAL, logical-retention, ownership-cut, and coexistence surface."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m5-final-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register("phase4M5Check") {
    group = "verification"
    description = "Run the complete ordinary F4-M5 async-profile and logical-retention gate."
    dependsOn("phase4M5RetentionPolicyAdminCheck")
    dependsOn("checkPhase4M5FinalContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("phase4M5AsyncRetentionMultiBrokerPulsarCheck") {
    group = "verification"
    description = "Run the retry-disabled real two-broker async Object-WAL and logical-retention gate."
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase4M5Check")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusAsyncRetentionMultiBrokerIntegrationTest",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
        "-PtestRetryCount=0",
    )
}

tasks.register("phase4M5FinalCheck") {
    group = "verification"
    description = "Run the complete F4-M5 release gate including real two-broker async retention and failover."
    dependsOn("phase4M5Check")
    dependsOn("checkPhase4M5FinalContractSurface")
    dependsOn("phase4M5AsyncRetentionMultiBrokerPulsarCheck")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
}

tasks.register<Exec>("checkPhase4M6RegistryScaleContractSurface") {
    group = "verification"
    description = "Audit exact 16,448-stream all-shard registry pagination and cold-restart evidence."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m6-registry-scale-contract-surface.sh",
    )
}

tasks.register("phase4M6RegistryScaleCheck") {
    group = "verification"
    description = "Verify checkpoint BH scans 257 registrations in each of 64 shards across cold restarts."
    dependsOn("phase4M5FinalCheck")
    dependsOn("checkPhase4M6RegistryScaleContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-materialization:check")
}

tasks.register<Exec>("checkPhase4M6TwoBrokerWorkerContentionContractSurface") {
    group = "verification"
    description = "Audit exact two-broker/two-worker contention, compressed-read, and coexistence evidence."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m6-two-broker-worker-contention-contract-surface.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("phase4M6TwoBrokerWorkerContentionPulsarCheck") {
    group = "verification"
    description = "Run the retry-disabled real two-broker/two-worker materialization contention gate."
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn("publishPhase2DevelopmentArtifacts")
    mustRunAfter("phase4M6RegistryScaleCheck")
    workingDir = file(pulsarCheckoutPath.get())
    commandLine(
        pulsarGradleWrapper,
        ":pulsar-broker:spotlessJavaCheck",
        ":pulsar-broker:checkstyleMain",
        ":pulsar-broker:checkstyleTest",
        ":pulsar-broker:test",
        "--tests", "org.apache.pulsar.broker.storage.nereus.NereusMaterializationContentionMultiBrokerIntegrationTest",
        "--rerun-tasks",
        "-PexcludedTestGroups=quarantine,flaky",
        "-PnereusDevelopmentRepository=${phase2DevelopmentRepository.get().asFile.absolutePath}",
        "-PtestFailFast=true",
        "-PtestRetryCount=0",
    )
}

tasks.register("phase4M6TwoBrokerWorkerContentionCheck") {
    group = "verification"
    description = "Verify checkpoint BI two-process materialization contention and exact compressed Pulsar reads."
    dependsOn("phase4M6RegistryScaleCheck")
    dependsOn("checkPhase4M6TwoBrokerWorkerContentionContractSurface")
    dependsOn("phase4M6TwoBrokerWorkerContentionPulsarCheck")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-materialization:check")
}

tasks.register<Exec>("checkPhase4M6AbandonedAppendIntentContractSurface") {
    group = "verification"
    description = "Audit protected-head ordering and full-proof abandoned append-intent retirement."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m6-abandoned-append-intent-contract-surface.sh",
    )
}

tasks.register("phase4M6AbandonedAppendIntentCheck") {
    group = "verification"
    description = "Verify checkpoint BJ protected append ordering and abandoned intent GC convergence."
    dependsOn("phase4M6TwoBrokerWorkerContentionCheck")
    dependsOn("checkPhase4M6AbandonedAppendIntentContractSurface")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register<Exec>("checkPhase4M6ScenarioEvidenceMatrix") {
    group = "verification"
    description = "Verify all 52 Phase 4 M6 scenarios map to real annotated tests and declared owning gates."
    workingDir = layout.projectDirectory.asFile
    commandLine(
        "bash",
        "scripts/check-phase4-m6-scenario-evidence-matrix.sh",
        pulsarCheckoutPath.get(),
    )
}

tasks.register<Exec>("checkPhase4FinalDockerIsolation") {
    group = "verification"
    description = "Verify every Docker-backed release task shares one bounded Gradle build service."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-final-docker-isolation.sh")
}

tasks.register<Exec>("checkPhase4FinalPulsarCheckoutIsolation") {
    group = "verification"
    description = "Verify every nested build of the locked Pulsar checkout shares one exclusive build service."
    workingDir = layout.projectDirectory.asFile
    commandLine("bash", "scripts/check-phase4-final-pulsar-checkout-isolation.sh")
}

tasks.register("phase4M6Check") {
    group = "verification"
    description = "Run the complete ordinary F4-M6 gate and the executable 52-scenario evidence audit."
    dependsOn("phase4M6AbandonedAppendIntentCheck")
    dependsOn("checkPhase4M6ScenarioEvidenceMatrix")
    dependsOn("checkPhase4Documentation")
    dependsOn("checkPhase4ModuleBoundaries")
    dependsOn("checkPhase4PulsarSourceLock")
    dependsOn(":nereus-api:check")
    dependsOn(":nereus-core:check")
    dependsOn(":nereus-managed-ledger:check")
    dependsOn(":nereus-materialization:check")
    dependsOn(":nereus-metadata-oxia:check")
    dependsOn(":nereus-object-store:check")
    dependsOn(":nereus-pulsar-adapter:check")
}

tasks.register("phase4M6FinalCheck") {
    group = "verification"
    description = "Run every ordinary, scale, and real-service F4-M6 acceptance gate."
    dependsOn("phase4M6Check")
    dependsOn("phase4M1FinalCheck")
    dependsOn("phase4M2FinalCheck")
    dependsOn("phase4M3FinalCheck")
    dependsOn("phase4M4FinalCheck")
    dependsOn("phase4M5FinalCheck")
    dependsOn("phase3FinalCheck")
    dependsOn("checkPhase4FinalDockerIsolation")
    dependsOn("checkPhase4FinalPulsarCheckoutIsolation")
}

tasks.register("phase4Check") {
    group = "verification"
    description = "Run every ordinary Phase 4 compaction, generation, retention, and GC gate."
    dependsOn("phase4M6Check")
}

tasks.register("phase4FinalCheck") {
    group = "verification"
    description = "Run the complete Phase 1 through Phase 4 release gate; this is the only Phase 4 completion claim."
    dependsOn("phase4Check")
    dependsOn("phase3FinalCheck")
    dependsOn("phase4M1FinalCheck")
    dependsOn("phase4M2FinalCheck")
    dependsOn("phase4M3FinalCheck")
    dependsOn("phase4M4FinalCheck")
    dependsOn("phase4M5FinalCheck")
    dependsOn("phase4M6FinalCheck")
    dependsOn("checkPhase4FinalDockerIsolation")
    dependsOn("checkPhase4FinalPulsarCheckoutIsolation")
}
