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
        includeTestsMatching("com.nereusstream.kafka.partition.KafkaFetchOperationTest")
        includeTestsMatching("com.nereusstream.kafka.partition.KafkaPartitionLeaderManagerTest")
    }
}
