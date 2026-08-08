/* Licensed under the Apache License, Version 2.0 */
plugins {
    application
}

dependencies {
    implementation(project(":nereus-api"))
    implementation(project(":nereus-bookkeeper"))
    implementation(project(":nereus-metadata-oxia"))
    implementation(project(":nereus-object-store"))
    implementation(project(":nereus-pulsar-adapter"))

    implementation(platform(libs.grpc.bom))
    implementation(libs.oxia.client)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(testFixtures(project(":nereus-object-store")))
    testRuntimeOnly(libs.junit.platform.launcher)
}

application {
    mainClass.set("com.nereusstream.admin.NereusAdminMain")
}

tasks.test {
    useJUnitPlatform()
}
