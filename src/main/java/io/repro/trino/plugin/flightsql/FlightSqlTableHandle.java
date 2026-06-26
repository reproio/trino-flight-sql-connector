package io.repro.trino.plugin.flightsql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record FlightSqlTableHandle(
        @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
        @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
        @JsonProperty("projectedColumns") Optional<List<FlightSqlColumnHandle>> projectedColumns)
        implements ConnectorTableHandle
{
    @JsonCreator
    public FlightSqlTableHandle
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        requireNonNull(constraint, "constraint is null");
        requireNonNull(projectedColumns, "projectedColumns is null");
        projectedColumns = projectedColumns.map(List::copyOf);
    }

    public FlightSqlTableHandle(SchemaTableName schemaTableName)
    {
        this(schemaTableName, TupleDomain.all(), Optional.empty());
    }

    public FlightSqlTableHandle withConstraint(TupleDomain<ColumnHandle> newConstraint)
    {
        return new FlightSqlTableHandle(schemaTableName, newConstraint, projectedColumns);
    }

    public FlightSqlTableHandle withProjectedColumns(List<FlightSqlColumnHandle> newProjected)
    {
        return new FlightSqlTableHandle(schemaTableName, constraint, Optional.of(newProjected));
    }
}
