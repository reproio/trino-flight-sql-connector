plugins {
    // Auto-provision JDK 24 (Trino 476 requirement) if not installed locally
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "trino-flight-sql-connector"
