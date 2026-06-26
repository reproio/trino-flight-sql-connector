package io.repro.trino.plugin.flightsql;

import io.repro.trino.plugin.flightsql.client.FlightSqlClient;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

import jakarta.inject.Inject;

import static java.util.Objects.requireNonNull;

public class FlightSqlConnector
        implements Connector
{
    private final FlightSqlMetadata metadata;
    private final FlightSqlSplitManager splitManager;
    private final FlightSqlPageSourceProvider pageSourceProvider;
    private final FlightSqlClient client;

    @Inject
    public FlightSqlConnector(
            FlightSqlMetadata metadata,
            FlightSqlSplitManager splitManager,
            FlightSqlPageSourceProvider pageSourceProvider,
            FlightSqlClient client)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return FlightSqlTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public void shutdown()
    {
        client.close();
    }
}
