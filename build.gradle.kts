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
    `base`
    `maven-publish`
}

group = "io.nereusstream"
version = providers.gradleProperty("nereusVersion").getOrElse("0.1.0-SNAPSHOT")

val javaLanguageVersion = providers.gradleProperty("javaVersion").map(String::toInt).getOrElse(21)

subprojects {
    group = rootProject.group
    version = rootProject.version

    repositories {
        mavenCentral()
    }
}

configure(subprojects.filter { it.name != "nereus-bom" }) {
    apply(plugin = "java-library")
    apply(plugin = "maven-publish")

    extensions.configure<JavaPluginExtension>("java") {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(javaLanguageVersion))
        }
        withSourcesJar()
    }

    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
        options.release.set(javaLanguageVersion)
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }

    extensions.configure<PublishingExtension>("publishing") {
        publications {
            create<MavenPublication>("mavenJava") {
                from(components["java"])
                pom {
                    name.set(project.name)
                    description.set("Nereus module ${project.name}")
                    url.set("https://github.com/nereusstream/nereus")
                    licenses {
                        license {
                            name.set("Apache License, Version 2.0")
                            url.set("https://www.apache.org/licenses/LICENSE-2.0")
                        }
                    }
                    scm {
                        url.set("https://github.com/nereusstream/nereus")
                        connection.set("scm:git:https://github.com/nereusstream/nereus.git")
                    }
                }
            }
        }
    }
}

tasks.register("quickCheck") {
    group = "verification"
    description = "Fast scaffold check for Nereus."
    dependsOn("checkPhase0")
}

tasks.register("checkPhase0") {
    group = "verification"
    description = "Verify the Phase 0 repository scaffold."
    doLast {
        val required = listOf(
            "README.md",
            "LICENSE",
            "NOTICE",
            "settings.gradle.kts",
            "build.gradle.kts",
            "gradle/libs.versions.toml",
            "docs/design/nereus-design-index.md",
            "docs/phase0/repository-plan.md",
            "docs/phase0/upstream-forks.md"
        )
        required.forEach { path ->
            check(file(path).exists()) { "Missing required file: $path" }
        }
        check(!file("integrations").exists()) { "Do not keep integrations/ in the main repo; use org forks." }
    }
}
