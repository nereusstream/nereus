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

val sourceQualifiedM3 = rootProject.version.toString().contains("-m3.")

dependencies {
    api(project(":nereus-storage-api"))
    // The public Cell-session implementation exposes BookKeeper client API types. Keep the historical
    // M2 runtime-only edge unchanged, but publish it as API for the M3 external-consumer closure.
    if (sourceQualifiedM3) {
        api(libs.bookkeeper.server)
    } else {
        implementation(libs.bookkeeper.server)
    }

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.platform.launcher)
}

val realBookKeeperTest by sourceSets.creating {
    java.srcDir("src/realBookKeeperTest/java")
    compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
    runtimeClasspath += output + compileClasspath
}

configurations[realBookKeeperTest.implementationConfigurationName]
    .extendsFrom(configurations.testImplementation.get())
configurations[realBookKeeperTest.runtimeOnlyConfigurationName]
    .extendsFrom(configurations.testRuntimeOnly.get())

tasks.register<Test>("realBookKeeperTest") {
    group = "verification"
    description = "Run the exact-image real BookKeeper Cell-session conformance suite."
    filter { includeTestsMatching("com.nereusstream.storage.bookkeeper.RealBookKeeperCellSessionV1RealTest") }
    testClassesDirs = realBookKeeperTest.output.classesDirs
    classpath = realBookKeeperTest.runtimeClasspath
    useJUnitPlatform()
    maxParallelForks = 1
    outputs.upToDateWhen { false }
    doFirst {
        val metadataServiceUri = providers.gradleProperty("v2M2BookKeeperMetadataServiceUri").orNull
            ?: error("v2M2BookKeeperMetadataServiceUri is required when realBookKeeperTest executes")
        systemProperty("nereus.bookkeeper.metadataServiceUri", metadataServiceUri)
    }
}

tasks.withType<Jar>().configureEach {
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
}

// Opt-in native create profile checks; the historical conformance task is unchanged.
tasks.register<Test>("v2M5NativeCreateTest") {
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    filter { includeTestsMatching("com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateSpecV2Test") }
    outputs.upToDateWhen { false }
}

tasks.register<Test>("v2M5NativeCreateRealTest") {
    group = "verification"
    testClassesDirs = realBookKeeperTest.output.classesDirs
    classpath = realBookKeeperTest.runtimeClasspath
    useJUnitPlatform()
    maxParallelForks = 1
    filter { includeTestsMatching("com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateV2RealTest") }
    outputs.upToDateWhen { false }
    doFirst {
        systemProperty("nereus.bookkeeper.metadataServiceUri",
            providers.gradleProperty("v2M2BookKeeperMetadataServiceUri").orNull
                ?: error("v2M2BookKeeperMetadataServiceUri is required for native M5 create fencing"))
    }
}


// Native namespace conformance composes actual Oxia and BK only in this test source set.
dependencies {
    add(realBookKeeperTest.implementationConfigurationName, project(":nereus-metadata-oxia"))
    add(realBookKeeperTest.implementationConfigurationName, project(":nereus-metadata-spi"))
    add(realBookKeeperTest.implementationConfigurationName, project(":nereus-storage-object"))
    add(realBookKeeperTest.implementationConfigurationName,
        project(path = ":nereus-storage-object", configuration = "m5DeleteTestFixtures"))
}


mapOf(
    "v2M5PhysicalNamespaceRealTest" to "bindAndWriteBeforeServerRestart",
    "v2M5PhysicalNamespaceRestartTest" to "readAfterServerRestart",
).forEach { (taskName, method) ->
    tasks.register<Test>(taskName) {
        group = "verification"
        testClassesDirs = realBookKeeperTest.output.classesDirs
        classpath = realBookKeeperTest.runtimeClasspath
        useJUnitPlatform()
        maxParallelForks = 1
        filter { includeTestsMatching("com.nereusstream.storage.bookkeeper.M5PhysicalNamespaceOxiaBookKeeperRealTest.$method") }
        outputs.upToDateWhen { false }
        doFirst {
            systemProperty("nereus.bookkeeper.metadataServiceUri",
                providers.gradleProperty("v2M2BookKeeperMetadataServiceUri").orNull ?: error("native BookKeeper URI is required"))
            systemProperty("nereus.m5.namespace.oxiaAddress",
                providers.gradleProperty("v2M5RetentionOxiaServiceAddress").orNull ?: error("native Oxia address is required"))
            systemProperty("nereus.m5.namespace.oxiaAliasAddress",
                providers.gradleProperty("v2M5NamespaceOxiaAliasAddress").orNull ?: error("native Oxia alias is required"))
            systemProperty("nereus.m5.namespace.foreignOxiaAddress",
                providers.gradleProperty("v2M5NamespaceForeignOxiaAddress").orNull ?: error("second native Oxia address is required"))
            systemProperty("nereus.m5.namespace.restartCheckpoint",
                providers.gradleProperty("v2M5NamespaceRestartCheckpoint").orNull ?: error("namespace restart checkpoint is required"))
        }
    }
}

// Native server-side GC epoch and exact ledger-version fencing. No M5 eligibility/Final authority is implied.
tasks.register<Test>("v2M5NativeDeleteRealTest") {
    group = "verification"
    testClassesDirs = realBookKeeperTest.output.classesDirs
    classpath = realBookKeeperTest.runtimeClasspath
    useJUnitPlatform()
    maxParallelForks = 1
    filter { includeTestsMatching("com.nereusstream.storage.bookkeeper.M5BookKeeperNativeDeleteV2RealTest") }
    outputs.upToDateWhen { false }
    doFirst {
        systemProperty("nereus.bookkeeper.metadataServiceUri",
            providers.gradleProperty("v2M2BookKeeperMetadataServiceUri").orNull
                ?: error("v2M2BookKeeperMetadataServiceUri is required for native M5 delete fencing"))
    }
}

mapOf("Write" to "writeBeforeServerRestart", "Read" to "readAfterServerRestart").forEach { (phase, method) ->
    tasks.register<Test>("v2M5NativeDeleteRestart${phase}Test") {
        group = "verification"
        testClassesDirs = realBookKeeperTest.output.classesDirs
        classpath = realBookKeeperTest.runtimeClasspath
        useJUnitPlatform()
        maxParallelForks = 1
        filter { includeTestsMatching("com.nereusstream.storage.bookkeeper.M5BookKeeperNativeDeleteV2RestartTest.$method") }
        outputs.upToDateWhen { false }
        doFirst {
            systemProperty("nereus.bookkeeper.metadataServiceUri",
                providers.gradleProperty("v2M2BookKeeperMetadataServiceUri").orNull
                    ?: error("native BookKeeper URI is required for delete restart verification"))
            systemProperty("nereus.m5.nativeDelete.restartCheckpoint",
                providers.gradleProperty("v2M5NativeDeleteRestartCheckpoint").orNull
                    ?: error("native delete restart checkpoint is required"))
        }
    }
}
