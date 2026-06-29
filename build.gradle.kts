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
    // Trino 476 (airbase 336) が classpath に持っている jackson のバージョンに揃える。
    // com.fasterxml.jackson.annotation.* は Trino のプラグインクラスローダで
    // parent-first のため、新しい jackson-databind を同梱すると JsonSerializeAs 等が
    // 親側 (古い jackson-annotations) で解決できず NoClassDefFoundError になる。
    // enforcedPlatform で BOM の version を強制 (annotations は 2.20, core/databind は 2.20.1)。
    implementation(enforcedPlatform(libs.jackson.bom))

    // provided: Trino エンジンが classpath に持っているため zip に同梱しない
    compileOnly(libs.trino.spi)

    // プラグイン同梱
    implementation(libs.trino.plugin.toolkit)
    implementation(libs.adbc.core)
    implementation(libs.adbc.driver.flight.sql)
    implementation(libs.adbc.driver.manager)
    implementation(libs.arrow.memory.core)
    implementation(libs.arrow.memory.netty)
    implementation(libs.arrow.vector)

    // tests
    testImplementation(libs.trino.spi)
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
    // ADBC / Arrow / Netty が JDK 内部に reflective access するために必要
    jvmArgs(
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
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
