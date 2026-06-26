package io.repro.trino.plugin.flightsql;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;


import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

class TestFlightSqlConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(FlightSqlConfig.class)
                .setUri(null)
                .setUseEncryption(null)
                .setTlsSkipVerify(false)
                .setTlsTrustStorePath(null)
                .setUsername(null)
                .setPassword(null)
                .setAuthorizationHeader(null)
                .setRpcHeaders("")
                .setDefaultDatabase(null)
                .setDialect(FlightSqlConfig.Dialect.DUCKDB));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("flight.uri", "grpc+tls://flight.example:32010")
                .put("flight.use-encryption", "true")
                .put("flight.tls.skip-verify", "true")
                .put("flight.tls.trust-store-path", "/etc/ssl/flight-ca.pem")
                .put("flight.username", "admin")
                .put("flight.password", "secret")
                .put("flight.authorization-header", "Bearer xyz")
                .put("flight.rpc-headers", "x-tenant:foo,x-debug:1")
                .put("flight.default-database", "analytics")
                .put("flight.dialect", "derby")
                .buildOrThrow();

        FlightSqlConfig expected = new FlightSqlConfig()
                .setUri("grpc+tls://flight.example:32010")
                .setUseEncryption(true)
                .setTlsSkipVerify(true)
                .setTlsTrustStorePath("/etc/ssl/flight-ca.pem")
                .setUsername("admin")
                .setPassword("secret")
                .setAuthorizationHeader("Bearer xyz")
                .setRpcHeaders("x-tenant:foo,x-debug:1")
                .setDefaultDatabase("analytics")
                .setDialect(FlightSqlConfig.Dialect.DERBY);

        assertFullMapping(properties, expected);
    }
}
