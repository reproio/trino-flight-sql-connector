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
                .setUseEncryption(true)
                .setDisableCertificateVerification(false)
                .setUseSystemTrustStore(false)
                .setTrustStorePath(null)
                .setTrustStorePassword(null)
                .setToken(null)
                .setDefaultDatabase(null));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("flight.use-encryption", "false")
                .put("flight.disable-certificate-verification", "true")
                .put("flight.use-system-trust-store", "true")
                .put("flight.trust-store", "/etc/ssl/flight.jks")
                .put("flight.trust-store-password", "trust-secret")
                .put("flight.token", "bearer-token")
                .put("flight.default-database", "analytics")
                .buildOrThrow();

        FlightSqlConfig expected = new FlightSqlConfig()
                .setUseEncryption(false)
                .setDisableCertificateVerification(true)
                .setUseSystemTrustStore(true)
                .setTrustStorePath("/etc/ssl/flight.jks")
                .setTrustStorePassword("trust-secret")
                .setToken("bearer-token")
                .setDefaultDatabase("analytics");

        assertFullMapping(properties, expected);
    }
}
