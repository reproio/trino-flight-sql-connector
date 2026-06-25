plugins {
    `java-library`
    distribution
}

group = "io.repro.trino.plugin"
version = "0.1.0-SNAPSHOT"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(24))
    }
}

repositories {
    mavenCentral()
}

dependencies {
    // provided: Trino エンジンが classpath に持っているため zip に同梱しない
    compileOnly(libs.trino.spi)

    // プラグインに同梱
    implementation(libs.trino.base.jdbc)
    implementation(libs.trino.plugin.toolkit)
    implementation(libs.flight.sql.jdbc)

    // tests
    testImplementation(libs.trino.spi)
    testImplementation(libs.trino.base.jdbc)
    testImplementation(variantOf(libs.trino.base.jdbc) { classifier("tests") })
    testImplementation(libs.trino.testing)
    testImplementation(libs.trino.main)
    testImplementation(libs.flight.sql)
    testImplementation(variantOf(libs.flight.sql) { classifier("tests") })
    testImplementation(libs.derby)
    testImplementation(libs.commons.dbcp2)
    testImplementation(libs.commons.pool2)
    testImplementation(libs.commons.text)
    testImplementation(libs.commons.cli)
    testImplementation(libs.arrow.jdbc)
    testImplementation(libs.airlift.configuration.testing)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj.core)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.test {
    useJUnitPlatform()
    // Arrow / Flight が JDK 内部に reflective access するために必要
    jvmArgs(
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--sun-misc-unsafe-memory-access=allow",
    )
}

// === Trino プラグイン形式の zip ===
// runtimeClasspath は compileOnly を含まないので、それがそのまま "non-provided" 集合
distributions {
    create("trinoPlugin") {
        distributionBaseName.set("trino-flight-sql")
        contents {
            into("plugin/flight-sql") {
                from(tasks.named("jar"))
                from(configurations.runtimeClasspath)
            }
        }
    }
}

tasks.named("assemble") {
    dependsOn("trinoPluginDistZip")
}
