package io.repro.trino.plugin.flightsql;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.NotNull;

import java.util.Map;

public class FlightSqlConfig
{
    public enum Dialect
    {
        DUCKDB,
        DERBY,
    }

    private String uri;
    private Boolean useEncryption;
    private boolean tlsSkipVerify;
    private String tlsTrustStorePath;
    private String username;
    private String password;
    private String authorizationHeader;
    private String rpcHeadersSpec = "";
    private String defaultDatabase;
    private Dialect dialect = Dialect.DUCKDB;

    @NotNull
    public String getUri()
    {
        return uri;
    }

    @Config("flight.uri")
    @ConfigDescription("Flight SQL server URI, e.g. grpc://host:port or grpc+tls://host:port")
    public FlightSqlConfig setUri(String uri)
    {
        this.uri = uri;
        return this;
    }

    public Boolean getUseEncryption()
    {
        return useEncryption;
    }

    @Config("flight.use-encryption")
    @ConfigDescription("Force TLS on/off; if unset, inferred from URI scheme (grpc+tls -> true)")
    public FlightSqlConfig setUseEncryption(Boolean useEncryption)
    {
        this.useEncryption = useEncryption;
        return this;
    }

    public boolean isTlsSkipVerify()
    {
        return tlsSkipVerify;
    }

    @Config("flight.tls.skip-verify")
    @ConfigDescription("Skip TLS certificate verification (dev/test only)")
    public FlightSqlConfig setTlsSkipVerify(boolean tlsSkipVerify)
    {
        this.tlsSkipVerify = tlsSkipVerify;
        return this;
    }

    public String getTlsTrustStorePath()
    {
        return tlsTrustStorePath;
    }

    @Config("flight.tls.trust-store-path")
    @ConfigDescription("Path to a PEM file containing trusted CA certificates")
    public FlightSqlConfig setTlsTrustStorePath(String tlsTrustStorePath)
    {
        this.tlsTrustStorePath = tlsTrustStorePath;
        return this;
    }

    public String getUsername()
    {
        return username;
    }

    @Config("flight.username")
    @ConfigDescription("Username for basic authentication")
    public FlightSqlConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    public String getPassword()
    {
        return password;
    }

    @Config("flight.password")
    @ConfigDescription("Password for basic authentication")
    @ConfigSecuritySensitive
    public FlightSqlConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public String getAuthorizationHeader()
    {
        return authorizationHeader;
    }

    @Config("flight.authorization-header")
    @ConfigDescription("Full Authorization header value, e.g. 'Bearer <token>'")
    @ConfigSecuritySensitive
    public FlightSqlConfig setAuthorizationHeader(String authorizationHeader)
    {
        this.authorizationHeader = authorizationHeader;
        return this;
    }

    public String getRpcHeaders()
    {
        return rpcHeadersSpec;
    }

    @Config("flight.rpc-headers")
    @ConfigDescription("Additional gRPC headers as comma-separated key:value pairs (e.g. x-tenant:foo,x-debug:1)")
    public FlightSqlConfig setRpcHeaders(String rpcHeadersSpec)
    {
        this.rpcHeadersSpec = rpcHeadersSpec == null ? "" : rpcHeadersSpec;
        return this;
    }

    public Map<String, String> getParsedRpcHeaders()
    {
        if (rpcHeadersSpec.isBlank()) {
            return Map.of();
        }
        ImmutableMap.Builder<String, String> headers = ImmutableMap.builder();
        for (String pair : Splitter.on(',').trimResults().omitEmptyStrings().split(rpcHeadersSpec)) {
            int idx = pair.indexOf(':');
            if (idx <= 0 || idx == pair.length() - 1) {
                throw new IllegalArgumentException("Invalid flight.rpc-headers entry (expected key:value): " + pair);
            }
            headers.put(pair.substring(0, idx).trim(), pair.substring(idx + 1).trim());
        }
        return headers.buildOrThrow();
    }

    public String getDefaultDatabase()
    {
        return defaultDatabase;
    }

    @Config("flight.default-database")
    @ConfigDescription("Default database name to advertise to the Flight SQL server (header)")
    public FlightSqlConfig setDefaultDatabase(String defaultDatabase)
    {
        this.defaultDatabase = defaultDatabase;
        return this;
    }

    public Dialect getDialect()
    {
        return dialect;
    }

    @Config("flight.dialect")
    @ConfigDescription("Remote SQL dialect for catalog/schema/table discovery (duckdb or derby)")
    public FlightSqlConfig setDialect(Dialect dialect)
    {
        this.dialect = dialect;
        return this;
    }
}
