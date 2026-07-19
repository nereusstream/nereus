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
        mavenCentral()
        maven {
            url = uri("https://packages.confluent.io/maven/")
            content {
                includeGroupByRegex("io\\.confluent(\\..*)?")
            }
        }
    }
}

val configuredPulsarCheckout = providers.gradleProperty("pulsarCheckout").orNull
    ?: providers.environmentVariable("NEREUS_PULSAR_CHECKOUT").orNull
val conventionalPulsarCheckout = file("../../nereusstream/pulsar")
val pulsarCheckout = configuredPulsarCheckout?.let(::file)
    ?: conventionalPulsarCheckout.takeIf { it.resolve("settings.gradle.kts").isFile }
val pulsarSourceRequired = gradle.startParameter.taskNames.any { requested ->
    val task = requested.substringAfterLast(':')
    (!requested.contains(':') && task in setOf("assemble", "build", "check", "publish", "test"))
        || task.startsWith("phase2")
        || task.startsWith("phase3")
        || task.startsWith("phase4")
        || requested.contains(":nereus-managed-ledger:")
        || requested.contains(":nereus-pulsar-adapter:")
}

require(pulsarCheckout != null || !pulsarSourceRequired) {
    "The requested task requires the locked Pulsar source composite. Set -PpulsarCheckout=/path/to/pulsar " +
        "or NEREUS_PULSAR_CHECKOUT; the source-build selector is not a published Maven snapshot."
}

if (pulsarCheckout != null) {
    require(pulsarCheckout.resolve("settings.gradle.kts").isFile) {
        "pulsarCheckout must point at the locked Pulsar Gradle checkout: $pulsarCheckout"
    }
    includeBuild(pulsarCheckout)
}

rootProject.name = "nereus"

include("nereus-bom")
include("nereus-api")
include("nereus-core")
include("nereus-metadata-oxia")
include("nereus-object-store")
include("nereus-materialization")
include("nereus-bookkeeper")
include("nereus-managed-ledger")
include("nereus-pulsar-adapter")
include("nereus-kop-adapter")
