pluginManagement {
    val quarkusPluginVersion: String by settings
    val quarkusPluginId: String by settings
    val kotlinVersion: String by settings

    repositories {
        mavenCentral()
        gradlePluginPortal()
        mavenLocal()
    }
    plugins {
        id(quarkusPluginId) version quarkusPluginVersion
        id("org.jetbrains.kotlin.jvm") version kotlinVersion
        id("org.jetbrains.kotlin.plugin.allopen") version kotlinVersion
    }
}

rootProject.name = "scoop"
