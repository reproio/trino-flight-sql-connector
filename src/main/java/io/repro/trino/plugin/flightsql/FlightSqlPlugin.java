package io.repro.trino.plugin.flightsql;

import io.trino.plugin.jdbc.JdbcPlugin;

public class FlightSqlPlugin
        extends JdbcPlugin
{
    public FlightSqlPlugin()
    {
        super("flight_sql", FlightSqlClientModule::new);
    }
}
