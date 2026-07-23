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

plugins {
    `java-test-fixtures`
}

dependencies {
    api(project(":nereus-api"))
    implementation(platform(libs.aws.sdk.v2.bom))
    implementation(libs.aws.sdk.v2.s3) {
        exclude(group = "software.amazon.awssdk", module = "apache5-client")
        exclude(group = "software.amazon.awssdk", module = "aws-crt-core")
    }
    implementation(libs.aws.sdk.v2.netty.client)
    implementation(libs.parquet.hadoop)
    implementation(libs.parquet.column)
    implementation(libs.hadoop.common) {
        // Nereus is embedded in broker processes that own their logging backend. Hadoop's
        // reload4j binding also rejects the nullable MDC values emitted by the Oxia client.
        exclude(group = "org.slf4j", module = "slf4j-reload4j")
    }
    implementation(libs.hadoop.mapreduce.client.core) {
        exclude(group = "org.slf4j", module = "slf4j-reload4j")
    }
    runtimeOnly(libs.zstd.jni)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.platform.launcher)
}

val s3IntegrationTest by sourceSets.creating {
    compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
    runtimeClasspath += output + compileClasspath
}

configurations[s3IntegrationTest.implementationConfigurationName].extendsFrom(
    configurations.testImplementation.get(),
)
configurations[s3IntegrationTest.runtimeOnlyConfigurationName].extendsFrom(
    configurations.testRuntimeOnly.get(),
)

dependencies {
    add(s3IntegrationTest.implementationConfigurationName, project())
    add(s3IntegrationTest.implementationConfigurationName, libs.testcontainers.junit.jupiter)
    add(s3IntegrationTest.implementationConfigurationName, libs.testcontainers.localstack)
    add(s3IntegrationTest.implementationConfigurationName, libs.junit.jupiter)
    add(s3IntegrationTest.implementationConfigurationName, libs.assertj)
    add(s3IntegrationTest.runtimeOnlyConfigurationName, libs.junit.platform.launcher)
}

tasks.register<Test>("s3IntegrationTest") {
    group = "verification"
    description = "Run the F2 S3-compatible provider gate against pinned LocalStack S3."
    testClassesDirs = s3IntegrationTest.output.classesDirs
    classpath = s3IntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.test)
    useJUnitPlatform()
}

tasks.register<Test>("rangedFormatTest") {
    group = "verification"
    description = "Run the F9-M1 Object WAL ranged and closed NCP2/NTC2 format contracts."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.objectstore.wal.WalObjectWriterReaderTest")
        includeTestsMatching("com.nereusstream.objectstore.compacted.Ncp2Ntc2GoldenAndCorruptionTest")
    }
}

tasks.register<Test>("rangedFormatS3IntegrationTest") {
    group = "verification"
    description = "Run the F9-M1 NCP2/NTC2 round trip against pinned LocalStack S3."
    testClassesDirs = s3IntegrationTest.output.classesDirs
    classpath = s3IntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.test, tasks.named("rangedFormatTest"))
    useJUnitPlatform()
    filter {
        includeTestsMatching(
            "com.nereusstream.objectstore.S3CompatibleObjectStoreLocalStackIntegrationTest.ncp2AndNtc2RoundTripThroughRealS3Provider",
        )
    }
}

tasks.register<Test>("kafkaCheckpointTest") {
    group = "verification"
    description = "Run the F9-M2 strict NKC1 format, corruption, and immutable publication contracts."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpoint*")
    }
}

tasks.register<Test>("kafkaCheckpointS3IntegrationTest") {
    group = "verification"
    description = "Run the F9-M2 NKC1 round trip against pinned LocalStack S3."
    testClassesDirs = s3IntegrationTest.output.classesDirs
    classpath = s3IntegrationTest.runtimeClasspath
    shouldRunAfter(tasks.test, tasks.named("kafkaCheckpointTest"))
    useJUnitPlatform()
    filter {
        includeTestsMatching(
            "com.nereusstream.objectstore.S3CompatibleObjectStoreLocalStackIntegrationTest.nkc1RoundTripThroughRealS3Provider",
        )
    }
}
