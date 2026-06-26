package io.repro.trino.plugin.flightsql;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.repro.trino.plugin.flightsql.arrow.ArrowToTrinoPageBuilder;
import io.repro.trino.plugin.flightsql.arrow.ArrowTypeMapper;
import io.repro.trino.plugin.flightsql.client.FlightSqlClient;
import io.repro.trino.plugin.flightsql.query.FlightSqlQueryBuilder;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class FlightSqlConnectorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(FlightSqlConfig.class);
        binder.bind(FlightSqlClient.class).in(Scopes.SINGLETON);
        binder.bind(ArrowTypeMapper.class).in(Scopes.SINGLETON);
        binder.bind(ArrowToTrinoPageBuilder.class).in(Scopes.SINGLETON);
        binder.bind(FlightSqlQueryBuilder.class).in(Scopes.SINGLETON);
        binder.bind(FlightSqlMetadata.class).in(Scopes.SINGLETON);
        binder.bind(FlightSqlSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(FlightSqlPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(FlightSqlConnector.class).in(Scopes.SINGLETON);
        jsonCodecBinder(binder).bindJsonCodec(FlightSqlSplit.class);
    }
}
