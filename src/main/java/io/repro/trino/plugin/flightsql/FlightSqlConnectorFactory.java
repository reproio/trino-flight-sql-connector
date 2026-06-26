package io.repro.trino.plugin.flightsql;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.type.TypeManager;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class FlightSqlConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "flight_sql";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(config, "config is null");
        requireNonNull(context, "context is null");

        ClassLoader classLoader = getClass().getClassLoader();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new FlightSqlConnectorModule(),
                    binder -> {
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                    });

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(FlightSqlConnector.class);
        }
    }
}
