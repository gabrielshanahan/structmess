plugins {
    java
    id("org.jetbrains.kotlin.jvm")
    id("org.jetbrains.kotlin.plugin.allopen")
    id("io.quarkus")
    id("com.ncorti.ktfmt.gradle") version "0.22.0"
    id("io.gitlab.arturbosch.detekt") version "1.23.8"
}

repositories {
    mavenCentral()
    mavenLocal()
}

val quarkusPlatformGroupId: String by project
val quarkusPlatformArtifactId: String by project
val quarkusPlatformVersion: String by project

dependencies {
    implementation(
        enforcedPlatform(
            "${quarkusPlatformGroupId}:${quarkusPlatformArtifactId}:${quarkusPlatformVersion}"
        )
    )
    implementation("io.quarkus:quarkus-rest")
    implementation("io.quarkus:quarkus-flyway")
    implementation("io.quarkus:quarkus-rest-jackson")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("io.quarkus:quarkus-jdbc-postgresql")
    implementation("io.quarkiverse.fluentjdbc:quarkus-fluentjdbc:1.0.0")
    implementation("io.quarkus:quarkus-reactive-pg-client")
    implementation("io.quarkus:quarkus-arc")
    implementation("io.quarkus:quarkus-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-stdlib")
    implementation("com.github.f4b6a3:uuid-creator:6.1.0")
    testImplementation("io.quarkus:quarkus-junit5")
    testImplementation("io.rest-assured:rest-assured")
}

group = "io.github.gabrielshanahan"

version = "1.0.0-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

allOpen {
    annotation("jakarta.ws.rs.Path")
    annotation("jakarta.enterprise.context.ApplicationScoped")
    annotation("io.quarkus.test.junit.QuarkusTest")
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
    kotlinOptions {
        jvmTarget = JavaVersion.VERSION_21.toString()
        javaParameters = true
    }
}

tasks.withType<Test> {
    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-parameters")
}

ktfmt { kotlinLangStyle() }

detekt {
    buildUponDefaultConfig = true
    config.setFrom(files("$projectDir/config/detekt/detekt.yml"))
    baseline = file("$projectDir/config/detekt/baseline.xml")
}

tasks.withType<io.gitlab.arturbosch.detekt.Detekt>().configureEach {
    reports {
        html.required.set(true)
        xml.required.set(false)
        txt.required.set(true)
        sarif.required.set(false)
        md.required.set(false)
    }
}

tasks.check { dependsOn("ktfmtCheck", "detekt") }

tasks.register("format") { dependsOn("ktfmtFormat") }
