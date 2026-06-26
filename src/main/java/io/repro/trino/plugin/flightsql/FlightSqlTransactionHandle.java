package io.repro.trino.plugin.flightsql;

import io.trino.spi.connector.ConnectorTransactionHandle;

public enum FlightSqlTransactionHandle
        implements ConnectorTransactionHandle
{
    INSTANCE
}
