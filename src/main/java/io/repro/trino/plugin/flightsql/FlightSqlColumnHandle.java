package io.repro.trino.plugin.flightsql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

public record FlightSqlColumnHandle(
        @JsonProperty("columnName") String columnName,
        @JsonProperty("trinoType") Type trinoType)
        implements ColumnHandle
{
    @JsonCreator
    public FlightSqlColumnHandle
    {
        requireNonNull(columnName, "columnName is null");
        requireNonNull(trinoType, "trinoType is null");
    }

    public ColumnMetadata toColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(columnName)
                .setType(trinoType)
                .build();
    }
}
