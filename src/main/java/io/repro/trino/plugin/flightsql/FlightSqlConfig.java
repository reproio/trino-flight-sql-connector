package io.repro.trino.plugin.flightsql;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

import java.util.Optional;
import java.util.Properties;

/**
 * Flight SQL 固有の追加プロパティ。JDBC URL 本体は Trino の base-jdbc 共通の
 * {@code connection-url}（{@link io.trino.plugin.jdbc.BaseJdbcConfig}）で
 * 受け取る。例: {@code connection-url=jdbc:arrow-flight-sql://host:port/}
 */
public class FlightSqlConfig
{
    private boolean useEncryption = true;
    private boolean disableCertificateVerification;
    private boolean useSystemTrustStore;
    private String trustStorePath;
    private String trustStorePassword;
    private String token;
    private String defaultDatabase;

    public boolean isUseEncryption()
    {
        return useEncryption;
    }

    @Config("flight.use-encryption")
    @ConfigDescription("Use TLS for the connection (default true)")
    public FlightSqlConfig setUseEncryption(boolean useEncryption)
    {
        this.useEncryption = useEncryption;
        return this;
    }

    public boolean isDisableCertificateVerification()
    {
        return disableCertificateVerification;
    }

    @Config("flight.disable-certificate-verification")
    @ConfigDescription("Skip TLS certificate verification (test/dev only)")
    public FlightSqlConfig setDisableCertificateVerification(boolean disableCertificateVerification)
    {
        this.disableCertificateVerification = disableCertificateVerification;
        return this;
    }

    public boolean isUseSystemTrustStore()
    {
        return useSystemTrustStore;
    }

    @Config("flight.use-system-trust-store")
    @ConfigDescription("Use the OS trust store for TLS")
    public FlightSqlConfig setUseSystemTrustStore(boolean useSystemTrustStore)
    {
        this.useSystemTrustStore = useSystemTrustStore;
        return this;
    }

    public Optional<String> getTrustStorePath()
    {
        return Optional.ofNullable(trustStorePath);
    }

    @Config("flight.trust-store")
    @ConfigDescription("Path to the TLS trust store")
    public FlightSqlConfig setTrustStorePath(String trustStorePath)
    {
        this.trustStorePath = trustStorePath;
        return this;
    }

    public Optional<String> getTrustStorePassword()
    {
        return Optional.ofNullable(trustStorePassword);
    }

    @Config("flight.trust-store-password")
    @ConfigDescription("Password for the TLS trust store")
    @ConfigSecuritySensitive
    public FlightSqlConfig setTrustStorePassword(String trustStorePassword)
    {
        this.trustStorePassword = trustStorePassword;
        return this;
    }

    public Optional<String> getToken()
    {
        return Optional.ofNullable(token);
    }

    @Config("flight.token")
    @ConfigDescription("Bearer token for authentication (mutually exclusive with user/password)")
    @ConfigSecuritySensitive
    public FlightSqlConfig setToken(String token)
    {
        this.token = token;
        return this;
    }

    public Optional<String> getDefaultDatabase()
    {
        return Optional.ofNullable(defaultDatabase);
    }

    @Config("flight.default-database")
    @ConfigDescription("Default database, sent as gRPC header 'database'")
    public FlightSqlConfig setDefaultDatabase(String defaultDatabase)
    {
        this.defaultDatabase = defaultDatabase;
        return this;
    }

    public Properties buildConnectionProperties()
    {
        Properties props = new Properties();
        props.setProperty("useEncryption", Boolean.toString(useEncryption));
        props.setProperty("useSystemTrustStore", Boolean.toString(useSystemTrustStore));
        props.setProperty("disableCertificateVerification", Boolean.toString(disableCertificateVerification));
        getTrustStorePath().ifPresent(v -> props.setProperty("trustStore", v));
        getTrustStorePassword().ifPresent(v -> props.setProperty("trustStorePassword", v));
        getToken().ifPresent(v -> props.setProperty("token", v));
        getDefaultDatabase().ifPresent(v -> props.setProperty("database", v));
        return props;
    }
}
