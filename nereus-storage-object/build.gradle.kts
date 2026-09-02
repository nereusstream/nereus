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

val v2M3LocalCapEvidenceTest = tasks.register<Test>("v2M3LocalCapEvidenceTest") {
    group = "verification"
    description = "Execute the exact six-record D1 local format-cap conformance suite."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    maxParallelForks = 1
    useJUnitPlatform()
    filter {
        listOf(
            "localFormulaRecordExercisesExactAndCartesianBoundaries",
            "localParserRecordExercisesLengthsFirstEnvelopeParser",
            "localCheckedArithmeticRecordExercisesOverflowAndNarrowing",
            "localKmsEnvelopeRecordExercisesRoundTripAndOversizeRejection",
            "localZstdRecordExercisesSemanticRoundTripWithoutExactOutputClaim",
            "localStreamingCounterRecordIsAnalyticalAndClaimsNoProviderTransfer",
        ).forEach { method ->
            includeTestsMatching(
                "com.nereusstream.storage.object.extent.CheckedExtentAccountingTest.$method",
            )
        }
    }
    outputs.upToDateWhen { false }
}

tasks.register<JavaExec>("v2M3LocalCapEvidenceEmit") {
    group = "verification"
    description = "Emit the exact-source D1 local-cap result after the governed six-test execution."
    dependsOn(v2M3LocalCapEvidenceTest, tasks.named("classes"))
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.nereusstream.storage.object.extent.ObjectWalLocalCapacityHarnessV1")
    outputs.upToDateWhen { false }
    doFirst {
        fun required(name: String): String = providers.gradleProperty(name).orNull
            ?: throw GradleException("-P$name is required for formal D1 local-cap evidence")
        val output = file(required("v2M3LocalCapEvidenceOutput")).toPath().toAbsolutePath().normalize()
        val repository = rootProject.projectDir.toPath().toAbsolutePath().normalize()
        if (output.startsWith(repository)) {
            throw GradleException("formal D1 local-cap output must remain outside the repository")
        }
        setArgs(
            listOf(
                output.toString(),
                required("v2M3LocalCapTestedCommit"),
                required("v2M3LocalCapHarnessSourceSha256"),
                required("v2M3LocalCapHarnessTestSourceSha256"),
            ) + (0..5).map { ordinal ->
                required("v2M3LocalCapComponentSourceSha256$ordinal")
            },
        )
    }
}

tasks.register<Test>("v2M4ReadKernelTest") {
    group = "verification"
    description = "Run the allocation-free M4 capture, source-plan, fallback, drain, and hazard-scan kernel suite."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    maxParallelForks = 1
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.storage.object.read.BindingReadM4KernelV1Test")
    }
    outputs.upToDateWhen { false }
}

tasks.withType<Jar>().configureEach {
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
}
