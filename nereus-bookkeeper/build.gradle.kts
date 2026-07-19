plugins {
    `java-library`
}

dependencies {
    api(project(":nereus-api"))
    api(project(":nereus-core"))
    implementation(project(":nereus-metadata-oxia"))
    implementation(libs.bookkeeper.server)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.platform.launcher)
}
