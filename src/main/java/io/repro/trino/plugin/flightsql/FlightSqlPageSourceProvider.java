package io.repro.trino.plugin.flightsql;

import com.google.common.collect.ImmutableList;
import io.repro.trino.plugin.flightsql.arrow.ArrowToTrinoPageBuilder;
import io.repro.trino.plugin.flightsql.client.FlightSqlClient;
import io.repro.trino.plugin.flightsql.client.PartitionReader;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import jakarta.inject.Inject;

import java.util.Base64;
import java.util.List;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class FlightSqlPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final FlightSqlClient client;
    private final ArrowToTrinoPageBuilder pageBuilder;

    @Inject
    public FlightSqlPageSourceProvider(FlightSqlClient client, ArrowToTrinoPageBuilder pageBuilder)
    {
        this.client = requireNonNull(client, "client is null");
        this.pageBuilder = requireNonNull(pageBuilder, "pageBuilder is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        FlightSqlSplit fsplit = (FlightSqlSplit) split;
        ImmutableList.Builder<FlightSqlColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            handles.add((FlightSqlColumnHandle) column);
        }
        PartitionReader reader;
        try {
            if (fsplit.partitionDescriptorBase64().isPresent()) {
                byte[] descriptor = Base64.getDecoder().decode(fsplit.partitionDescriptorBase64().get());
                reader = client.readPartition(descriptor);
            }
            else {
                reader = client.executeQuery(fsplit.fallbackSql().orElseThrow());
            }
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to open Flight SQL partition reader: " + e.getMessage(), e);
        }
        return new FlightSqlPageSource(reader, pageBuilder, handles.build());
    }

    public static class Factory
            implements ConnectorPageSourceProviderFactory
    {
        private final FlightSqlPageSourceProvider provider;

        @Inject
        public Factory(FlightSqlPageSourceProvider provider)
        {
            this.provider = requireNonNull(provider, "provider is null");
        }

        @Override
        public ConnectorPageSourceProvider createPageSourceProvider()
        {
            return provider;
        }
    }
}
