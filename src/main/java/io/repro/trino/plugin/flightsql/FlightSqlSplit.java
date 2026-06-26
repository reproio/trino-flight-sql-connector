package io.repro.trino.plugin.flightsql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.SizeOf;
import io.trino.spi.connector.ConnectorSplit;

import java.util.Optional;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public record FlightSqlSplit(
        @JsonProperty("partitionDescriptorBase64") Optional<String> partitionDescriptorBase64,
        @JsonProperty("fallbackSql") Optional<String> fallbackSql)
        implements ConnectorSplit
{
    private static final long INSTANCE_SIZE = instanceSize(FlightSqlSplit.class);

    @JsonCreator
    public FlightSqlSplit
    {
        requireNonNull(partitionDescriptorBase64, "partitionDescriptorBase64 is null");
        requireNonNull(fallbackSql, "fallbackSql is null");
        if (partitionDescriptorBase64.isPresent() == fallbackSql.isPresent()) {
            throw new IllegalArgumentException("Exactly one of partitionDescriptorBase64 / fallbackSql must be set");
        }
    }

    public static FlightSqlSplit partition(String partitionDescriptorBase64)
    {
        return new FlightSqlSplit(Optional.of(partitionDescriptorBase64), Optional.empty());
    }

    public static FlightSqlSplit fallback(String sql)
    {
        return new FlightSqlSplit(Optional.empty(), Optional.of(sql));
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + sizeOf(partitionDescriptorBase64, SizeOf::estimatedSizeOf)
                + sizeOf(fallbackSql, SizeOf::estimatedSizeOf);
    }
}
