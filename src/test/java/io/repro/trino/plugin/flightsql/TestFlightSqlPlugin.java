package io.repro.trino.plugin.flightsql;

import com.google.common.collect.Iterables;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TestFlightSqlPlugin
{
    @Test
    void testCreateConnectorFactory()
    {
        Plugin plugin = new FlightSqlPlugin();
        ConnectorFactory factory = Iterables.getOnlyElement(plugin.getConnectorFactories());
        assertThat(factory.getName()).isEqualTo("flight_sql");
    }
}
