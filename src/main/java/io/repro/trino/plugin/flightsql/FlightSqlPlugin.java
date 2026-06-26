package io.repro.trino.plugin.flightsql;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

public class FlightSqlPlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new FlightSqlConnectorFactory());
    }
}
