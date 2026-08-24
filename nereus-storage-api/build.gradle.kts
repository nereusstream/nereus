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

val n1Version = "0.2.0-n1.330aaec349c51fb2ace52b1085e8a9e5a60b5e3e"
val sourceQualifiedM3 = rootProject.version.toString().contains("-m3.")

dependencies {
    // M2 continues to consume the immutable N1 domain artifact. The complete M3 source-qualified
    // publication is deliberately different: its public Storage API must expose the current domain
    // contract, otherwise an external M3 consumer resolves an old domain through this API edge.
    if (sourceQualifiedM3) {
        api(project(":nereus-domain"))
    } else {
        api("com.nereusstream:nereus-domain:$n1Version")
    }

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.withType<Jar>().configureEach {
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
}
