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
    api(project(":nereus-api"))
    implementation(project(":nereus-metadata-oxia"))
    api(project(":nereus-object-store"))
    api(libs.pulsar.managed.ledger)
    api(libs.pulsar.common)
    api(libs.pulsar.client.admin.api)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(platform(libs.grpc.bom))
    testImplementation(platform(libs.opentelemetry.bom))
    testImplementation(platform(libs.opentelemetry.bom.alpha))
    testImplementation(libs.oxia.client)
    testImplementation(testFixtures(project(":nereus-metadata-oxia")))
    testRuntimeOnly(libs.junit.platform.launcher)
}
