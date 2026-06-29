package io.repro.trino.plugin.flightsql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.repro.trino.plugin.flightsql.arrow.ArrowTypeMapper;
import io.repro.trino.plugin.flightsql.client.FlightSqlClient;
import io.repro.trino.plugin.flightsql.query.FlightSqlQueryBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import jakarta.inject.Inject;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class FlightSqlMetadata
        implements ConnectorMetadata
{
    private static final Logger LOG = Logger.get(FlightSqlMetadata.class);

    private final FlightSqlClient client;
    private final ArrowTypeMapper typeMapper;
    private final FlightSqlQueryBuilder queryBuilder;
    private final ConcurrentMap<SchemaTableName, List<FlightSqlColumnHandle>> columnsCache = new ConcurrentHashMap<>();

    @Inject
    public FlightSqlMetadata(FlightSqlClient client, ArrowTypeMapper typeMapper, FlightSqlQueryBuilder queryBuilder)
    {
        this.client = requireNonNull(client, "client is null");
        this.typeMapper = requireNonNull(typeMapper, "typeMapper is null");
        this.queryBuilder = requireNonNull(queryBuilder, "queryBuilder is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        try {
            return ImmutableList.copyOf(client.listSchemaNames());
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to list schemas: " + e.getMessage(), e);
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Optional<io.trino.spi.connector.ConnectorTableVersion> startVersion,
            Optional<io.trino.spi.connector.ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new io.trino.spi.TrinoException(io.trino.spi.StandardErrorCode.NOT_SUPPORTED, "Versioned tables are not supported");
        }
        try {
            Set<String> tables = client.listTables(schemaTableName.getSchemaName());
            if (!tables.contains(schemaTableName.getTableName())) {
                return null;
            }
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to look up table " + schemaTableName + ": " + e.getMessage(), e);
        }
        return new FlightSqlTableHandle(schemaTableName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        FlightSqlTableHandle handle = (FlightSqlTableHandle) tableHandle;
        List<FlightSqlColumnHandle> columns = readColumns(handle.schemaTableName());
        List<ColumnMetadata> columnMetadata = columns.stream()
                .map(FlightSqlColumnHandle::toColumnMetadata)
                .collect(ImmutableList.toImmutableList());
        return new ConnectorTableMetadata(handle.schemaTableName(), columnMetadata);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        try {
            Iterable<String> schemas = schemaName.<Iterable<String>>map(List::of).orElseGet(() -> {
                try {
                    return client.listSchemaNames();
                }
                catch (Exception e) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to list schemas: " + e.getMessage(), e);
                }
            });
            ImmutableList.Builder<SchemaTableName> result = ImmutableList.builder();
            for (String schema : schemas) {
                for (String table : client.listTables(schema)) {
                    result.add(new SchemaTableName(schema, table));
                }
            }
            return result.build();
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to list tables: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        FlightSqlTableHandle handle = (FlightSqlTableHandle) tableHandle;
        List<FlightSqlColumnHandle> columns = readColumns(handle.schemaTableName());
        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        for (FlightSqlColumnHandle column : columns) {
            builder.put(column.columnName(), column);
        }
        return builder.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((FlightSqlColumnHandle) columnHandle).toColumnMetadata();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Constraint constraint)
    {
        FlightSqlTableHandle handle = (FlightSqlTableHandle) tableHandle;
        TupleDomain<ColumnHandle> newSummary = constraint.getSummary();
        FlightSqlQueryBuilder.SplitConstraint split = queryBuilder.split(newSummary);
        TupleDomain<ColumnHandle> merged = handle.constraint().intersect(split.pushable());
        if (merged.equals(handle.constraint())) {
            return Optional.empty();
        }
        FlightSqlTableHandle newHandle = handle.withConstraint(merged);
        return Optional.of(new ConstraintApplicationResult<>(newHandle, split.remaining(), constraint.getExpression(), false));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        FlightSqlTableHandle handle = (FlightSqlTableHandle) tableHandle;
        for (ConnectorExpression projection : projections) {
            if (!(projection instanceof Variable)) {
                return Optional.empty();
            }
        }
        LinkedHashMap<String, FlightSqlColumnHandle> referenced = new LinkedHashMap<>();
        for (ConnectorExpression projection : projections) {
            Variable variable = (Variable) projection;
            FlightSqlColumnHandle column = (FlightSqlColumnHandle) assignments.get(variable.getName());
            requireNonNull(column, () -> "no column for variable " + variable.getName());
            referenced.putIfAbsent(column.columnName(), column);
        }
        List<FlightSqlColumnHandle> projected = List.copyOf(referenced.values());
        if (handle.projectedColumns().equals(Optional.of(projected))) {
            return Optional.empty();
        }
        FlightSqlTableHandle newHandle = handle.withProjectedColumns(projected);
        ImmutableList.Builder<io.trino.spi.connector.Assignment> newAssignments = ImmutableList.builder();
        LinkedHashSet<String> seenVariables = new LinkedHashSet<>();
        for (ConnectorExpression projection : projections) {
            Variable variable = (Variable) projection;
            if (!seenVariables.add(variable.getName())) {
                continue;
            }
            FlightSqlColumnHandle column = (FlightSqlColumnHandle) assignments.get(variable.getName());
            newAssignments.add(new io.trino.spi.connector.Assignment(variable.getName(), column, column.trinoType()));
        }
        return Optional.of(new ProjectionApplicationResult<>(newHandle, projections, newAssignments.build(), false));
    }

    public List<FlightSqlColumnHandle> readColumns(SchemaTableName tableName)
    {
        List<FlightSqlColumnHandle> cached = columnsCache.get(tableName);
        if (cached != null) {
            return cached;
        }
        List<FlightSqlColumnHandle> loaded = loadColumns(tableName);
        // Cache only non-empty results so an empty Schema (likely a transient server issue or an
        // ArrowTypeMapper miss for every field) is retried on subsequent calls instead of being
        // memoised as "this table has no columns".
        if (!loaded.isEmpty()) {
            columnsCache.putIfAbsent(tableName, loaded);
        }
        return loaded;
    }

    private List<FlightSqlColumnHandle> loadColumns(SchemaTableName tableName)
    {
        Schema schema;
        try {
            schema = client.getTableSchema(tableName.getSchemaName(), tableName.getTableName());
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to read schema for " + tableName + ": " + e.getMessage(), e);
        }
        if (schema.getFields().isEmpty()) {
            LOG.warn("Flight SQL server returned an empty Arrow Schema for %s. "
                    + "Either the server short-circuited the empty SELECT (FlightInfo.schema not populated AND no DoGet) "
                    + "or our SELECT * WHERE 1=0 probe is being optimised away.", tableName);
            return List.of();
        }
        ImmutableList.Builder<FlightSqlColumnHandle> mapped = ImmutableList.builder();
        java.util.List<String> skipped = new java.util.ArrayList<>();
        for (Field field : schema.getFields()) {
            Optional<Type> trinoType = typeMapper.toTrinoType(field);
            if (trinoType.isEmpty()) {
                skipped.add(field.getName() + ":" + field.getType());
                continue;
            }
            mapped.add(new FlightSqlColumnHandle(field.getName().toLowerCase(), trinoType.get()));
        }
        List<FlightSqlColumnHandle> result = mapped.build();
        if (!skipped.isEmpty()) {
            LOG.warn("Skipped %d column(s) for %s with Arrow types unsupported by ArrowTypeMapper: %s",
                    skipped.size(), tableName, skipped);
        }
        if (result.isEmpty() && !schema.getFields().isEmpty()) {
            LOG.warn("All %d columns of %s were dropped due to unsupported Arrow types; "
                    + "DESC will return an empty column list.", schema.getFields().size(), tableName);
        }
        return result;
    }
}
