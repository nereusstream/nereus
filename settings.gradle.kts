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

rootProject.name = "nereus"

include("nereus-bom")
include("nereus-domain")
include("nereus-metadata-spi")
include("nereus-metadata-oxia")
include("nereus-storage-api")
include("nereus-storage-bookkeeper")
include("nereus-kafka-bookkeeper")
include("nereus-pulsar-offload")
