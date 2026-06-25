package io.repro.trino.plugin.flightsql;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class FlightSqlClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(FlightSqlConfig.class);
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(FlightSqlClient.class).in(Scopes.SINGLETON);
        // CredentialProviderModule は JdbcModule 側で既に install 済み
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public ConnectionFactory connectionFactory(
            BaseJdbcConfig baseJdbcConfig,
            FlightSqlConfig config,
            CredentialProvider credentialProvider,
            OpenTelemetry openTelemetry)
    {
        return DriverConnectionFactory.builder(
                        new FlightSqlJdbcDriverWrapper(),
                        baseJdbcConfig.getConnectionUrl(),
                        credentialProvider)
                .setConnectionProperties(config.buildConnectionProperties())
                .setOpenTelemetry(openTelemetry)
                .build();
    }
}
