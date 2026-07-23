/* Licensed under the Apache License, Version 2.0 */

dependencies {
    api(project(":nereus-api"))
    implementation(project(":nereus-core"))
    implementation(project(":nereus-metadata-oxia"))
    implementation(project(":nereus-object-store"))
    implementation(project(":nereus-materialization"))

    testImplementation(testFixtures(project(":nereus-metadata-oxia")))
    testImplementation(testFixtures(project(":nereus-object-store")))
    testImplementation(libs.kafka.clients)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.platform.launcher)
}

val f9ProviderIntegrationTest by sourceSets.creating {
    compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
    runtimeClasspath += output + compileClasspath
}

configurations[f9ProviderIntegrationTest.implementationConfigurationName].extendsFrom(
    configurations.testImplementation.get(),
)
configurations[f9ProviderIntegrationTest.runtimeOnlyConfigurationName].extendsFrom(
    configurations.testRuntimeOnly.get(),
)

dependencies {
    add(f9ProviderIntegrationTest.implementationConfigurationName, project())
    add(f9ProviderIntegrationTest.implementationConfigurationName, testFixtures(project(":nereus-object-store")))
    add(f9ProviderIntegrationTest.implementationConfigurationName, libs.oxia.testcontainers)
    add(f9ProviderIntegrationTest.implementationConfigurationName, libs.testcontainers.junit.jupiter)
    add(f9ProviderIntegrationTest.implementationConfigurationName, libs.junit.jupiter)
    add(f9ProviderIntegrationTest.implementationConfigurationName, libs.assertj)
    add(f9ProviderIntegrationTest.runtimeOnlyConfigurationName, libs.junit.platform.launcher)
}

tasks.register<Test>("f9M2Test") {
    group = "verification"
    description = "Run F9-M2 deterministic Kafka binding, scanner, checkpoint, and recovery contracts."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
}

tasks.register<Test>("f9M2IntegrationTest") {
    group = "verification"
    description = "Run F9-M2 adapter restart/failure-cut integration contracts."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    shouldRunAfter(tasks.test, tasks.named("f9M2Test"))
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.kafka.*IntegrationTest")
    }
}

tasks.register<Test>("f9M3CodecTest") {
    group = "verification"
    description = "Run F9-M3 byte-exact codec, partition IO, and bounded async runtime contracts."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.kafka.codec.*")
        includeTestsMatching("com.nereusstream.kafka.partition.DefaultKafkaPartitionStorageTest")
        includeTestsMatching("com.nereusstream.kafka.runtime.KafkaBoundedAppendExecutorTest")
        includeTestsMatching("com.nereusstream.kafka.runtime.KafkaAppendFailureClassifierTest")
        includeTestsMatching("com.nereusstream.kafka.runtime.KafkaStorageAdmissionTest")
        includeTestsMatching("com.nereusstream.kafka.runtime.KafkaRuntimeResourcesTest")
        includeTestsMatching("com.nereusstream.kafka.runtime.DefaultNereusKafkaRuntimeTest")
        includeTestsMatching("com.nereusstream.kafka.runtime.NereusKafkaRuntimeFactoryTest")
        includeTestsMatching("com.nereusstream.kafka.runtime.NereusKafkaObjectWalRuntimeConfigurationTest")
        includeTestsMatching("com.nereusstream.kafka.runtime.NereusKafkaObjectWalRuntimeFactoryTest")
        includeTestsMatching("com.nereusstream.kafka.partition.KafkaFetchOperationTest")
        includeTestsMatching("com.nereusstream.kafka.fetch.KafkaFetchWaveOperationTest")
        includeTestsMatching("com.nereusstream.kafka.partition.KafkaPartitionLeaderManagerTest")
        includeTestsMatching("com.nereusstream.kafka.partition.KafkaStorageProfilePolicyTest")
        includeTestsMatching("com.nereusstream.kafka.partition.DefaultKafkaPartitionStorageManagerTest")
        includeTestsMatching("com.nereusstream.kafka.partition.DefaultKafkaPartitionOpenerTest")
        includeTestsMatching("com.nereusstream.kafka.partition.KafkaListOffsetsResolverTest")
    }
}

tasks.register<Test>("f9M3ProviderIntegrationTest") {
    group = "verification"
    description = "Run the F9-M3 provider-backed leader open/Produce/Fetch gate against real Oxia."
    testClassesDirs = f9ProviderIntegrationTest.output.classesDirs
    classpath = f9ProviderIntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.test, tasks.named("f9M3CodecTest"))
    useJUnitPlatform()
}

tasks.register<Test>("f9ProducerStatePropertyTest") {
    group = "verification"
    description = "Run the partial F9-M4 canonical producer and transaction checkpoint contracts."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.kafka.checkpoint.KafkaProducerTransactionStateCodecV1Test")
        includeTestsMatching("com.nereusstream.kafka.checkpoint.KafkaProducerStatePropertyTest")
    }
}

tasks.register<Test>("f9ActivationTest") {
    group = "verification"
    description = "Run F9 broker capability publication and ACTIVE/readiness admission contracts."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.kafka.activation.*")
    }
}
