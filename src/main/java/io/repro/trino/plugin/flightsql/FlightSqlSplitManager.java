package io.repro.trino.plugin.flightsql;

import com.google.common.collect.ImmutableList;
import io.repro.trino.plugin.flightsql.client.FlightSqlClient;
import io.repro.trino.plugin.flightsql.query.FlightSqlQueryBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import org.apache.arrow.vector.types.pojo.Field;

import jakarta.inject.Inject;

import java.util.Base64;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class FlightSqlSplitManager
        implements ConnectorSplitManager
{
    private final FlightSqlClient client;
    private final FlightSqlQueryBuilder queryBuilder;
    private final FlightSqlMetadata metadata;

    @Inject
    public FlightSqlSplitManager(FlightSqlClient client, FlightSqlQueryBuilder queryBuilder, FlightSqlMetadata metadata)
    {
        this.client = requireNonNull(client, "client is null");
        this.queryBuilder = requireNonNull(queryBuilder, "queryBuilder is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        FlightSqlTableHandle handle = (FlightSqlTableHandle) tableHandle;
        List<FlightSqlColumnHandle> columns = handle.projectedColumns()
                .orElseGet(() -> metadata.readColumns(handle.schemaTableName()));
        String sql = queryBuilder.build(handle, columns);
        FlightSqlClient.PartitionedExecuteResult result;
        try {
            result = client.executePartitioned(sql);
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to plan Flight SQL query: " + sql + " :: " + e.getMessage(), e);
        }
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        if (result.partitionDescriptors().isEmpty()) {
            splits.add(FlightSqlSplit.fallback(sql));
        }
        else {
            Base64.Encoder encoder = Base64.getEncoder();
            for (byte[] descriptor : result.partitionDescriptors()) {
                splits.add(FlightSqlSplit.partition(encoder.encodeToString(descriptor)));
            }
        }
        // Sanity check: the schema we will receive should at least match the SELECT count.
        if (result.schema() != null) {
            verifySchema(result.schema().getFields(), columns);
        }
        return new FixedSplitSource(splits.build());
    }

    private static void verifySchema(List<Field> arrowFields, List<FlightSqlColumnHandle> columns)
    {
        // With an empty projection (e.g. COUNT(*)) the QueryBuilder emits a dummy "SELECT 1", so the result always has one column
        int expected = columns.isEmpty() ? 1 : columns.size();
        if (arrowFields.size() != expected) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    "Flight SQL result column count " + arrowFields.size() + " does not match planned " + expected);
        }
    }

}
