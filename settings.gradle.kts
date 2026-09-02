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

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        exclusiveContent {
            forRepository {
                maven {
                    name = "lockedOxiaO1"
                    url = uri(
                        rootDir.resolve(
                            "gradle/locked-artifacts/oxia-client-java/" +
                                "091a42c2780d92da56e9ec1f02ce1c3d988adc16/m2",
                        ),
                    )
                    metadataSources {
                        gradleMetadata()
                        mavenPom()
                        artifact()
                    }
                }
            }
            filter {
                includeModule("io.github.oxia-db", "oxia-client")
                includeModule("io.github.oxia-db", "oxia-client-api")
            }
        }
        exclusiveContent {
            forRepository {
                maven {
                    name = "lockedNereusN1"
                    url = uri(
                        rootDir.resolve(
                            "gradle/locked-artifacts/nereus-n1/" +
                                "330aaec349c51fb2ace52b1085e8a9e5a60b5e3e/m2",
                        ),
                    )
                    metadataSources {
                        gradleMetadata()
                        mavenPom()
                        artifact()
                    }
                }
            }
            filter {
                includeModule("com.nereusstream", "nereus-domain")
            }
        }
        mavenCentral()
    }
}

val configuredPulsarCheckout = providers.gradleProperty("pulsarCheckout").orNull
    ?: providers.environmentVariable("NEREUS_PULSAR_CHECKOUT").orNull
val conventionalPulsarCheckout = file("../../nereusstream/pulsar")
val configuredM3PulsarEvidenceWorktree =
    providers.gradleProperty("v2M3PulsarEvidenceWorktree").orNull
        ?: providers.environmentVariable("NEREUS_M3_PULSAR_EVIDENCE_WORKTREE").orNull
        ?: configuredPulsarCheckout
val conventionalM3PulsarEvidenceWorktree = file("../../nereusstream/pulsar-worktrees/nereus-v2-m3")
val m3DedicatedPulsarRequired = gradle.startParameter.taskNames.any { requested ->
    val task = requested.substringAfterLast(':')
    task.startsWith("v2M3ModuleApi") ||
        task == "v2M3Check" ||
        task.startsWith("v2M3Allocator") ||
        task.startsWith("realAllocator") ||
        task.contains("RealAllocator") ||
        task.startsWith("validateRealAllocatorV2") ||
        task.startsWith("sealRealAllocatorV2")
}
val pulsarSourceRequired = gradle.startParameter.taskNames.any { requested ->
    val task = requested.substringAfterLast(':')
    (!requested.contains(':') && task in setOf("assemble", "build", "check", "test"))
            || requested.contains(":nereus-pulsar-offload:")
            || task.startsWith("v2M2Pulsar")
            || task.startsWith("v2M3Pulsar")
            || task.startsWith("v2M3ModuleApi")
            || task in setOf("v2M3M2RegressionSourceCheck", "v2M3InputsCheck", "v2M3Check")
            || task in setOf("v2M4CurrentSourceIntegrationCheck", "v2M4EvidenceExecutionCheck")
            || task == "v2M2Check"
}
val m3PulsarEvidenceWorktree = configuredM3PulsarEvidenceWorktree?.let(::file)
    ?: conventionalM3PulsarEvidenceWorktree.takeIf {
        m3DedicatedPulsarRequired && it.resolve("settings.gradle.kts").isFile
    }
val pulsarCheckout = if (m3DedicatedPulsarRequired) {
    m3PulsarEvidenceWorktree
} else {
    configuredPulsarCheckout?.let(::file)
        ?: conventionalPulsarCheckout.takeIf {
            pulsarSourceRequired && it.resolve("settings.gradle.kts").isFile
        }
}

require(!m3DedicatedPulsarRequired || m3PulsarEvidenceWorktree != null) {
    "The M3 module/API and allocator evidence gates require the dedicated Pulsar evidence worktree via " +
            "-Pv2M3PulsarEvidenceWorktree=/path/to/pulsar, NEREUS_M3_PULSAR_EVIDENCE_WORKTREE, " +
            "or the conventional pulsar-worktrees/nereus-v2-m3 path."
}
require(pulsarCheckout != null || !pulsarSourceRequired) {
    "The requested task requires the exact Pulsar source composite. Set -PpulsarCheckout=/path/to/pulsar " +
            "or NEREUS_PULSAR_CHECKOUT; the native SourceSafeLedgerOffloader SPI is not a published artifact."
}

if (pulsarCheckout != null) {
    require(pulsarCheckout.resolve("settings.gradle.kts").isFile) {
        "pulsarCheckout must point at the exact Pulsar Gradle checkout: $pulsarCheckout"
    }
    includeBuild(pulsarCheckout)
}

rootProject.name = "nereus"

include("nereus-bom")
include("nereus-domain")
include("nereus-metadata-spi")
include("nereus-metadata-oxia")
include("nereus-storage-api")
include("nereus-storage-bookkeeper")
include("nereus-storage-object")
include("nereus-storage-object-s3")
include("nereus-storage-object-vault")
include("nereus-kafka-bookkeeper")
include("nereus-pulsar-offload")
