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
    api(project(":nereus-storage-api")) {
        // M3 compiles against the current-source additive domain/API surface. The M2 storage-api project remains
        // unchanged and its immutable N1 dependency is exercised by the current-source M2 regression, but it must
        // not evict the current project domain from M3 consumers through Gradle version conflict resolution.
        exclude(group = "com.nereusstream", module = "nereus-domain")
    }
    api(project(":nereus-domain"))
    implementation(libs.zstd.jni)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.named<Test>("test") {
    useJUnitPlatform()
    maxHeapSize = "1024m"
}

tasks.register<JavaExec>("nwg1GoldenEmitter") {
    group = "verification"
    description = "Emit the NWG1 A corpus twice into an explicit external temporary directory."
    dependsOn(tasks.named("testClasses"))
    classpath = sourceSets.test.get().runtimeClasspath
    mainClass.set("com.nereusstream.storage.object.nwg1.Nwg1GoldenVectorEmitter")
    args(providers.gradleProperty("nwg1GoldenEmitterOutput").get())
    outputs.upToDateWhen { false }
}

tasks.withType<Jar>().configureEach {
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
}
