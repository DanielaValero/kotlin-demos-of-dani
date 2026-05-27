plugins {
    kotlin("jvm") version "2.0.21"
    application
}

group = "org.danikotlingdemos"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

val coroutinesVersion = "1.9.0"

dependencies {
    // Kotlin Coroutines
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$coroutinesVersion")

    // Testing
    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:$coroutinesVersion")
}

application {
    mainClass.set("org.danikotlingdemos.MainKt")
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(21)
}
