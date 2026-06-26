package io.repro.trino.plugin.flightsql;

import com.google.common.collect.ImmutableList;
import io.repro.trino.plugin.flightsql.arrow.ArrowToTrinoPageBuilder;
import io.repro.trino.plugin.flightsql.client.PartitionReader;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class FlightSqlPageSource
        implements ConnectorPageSource
{
    private final PartitionReader reader;
    private final ArrowToTrinoPageBuilder pageBuilder;
    private final List<Type> trinoTypes;
    private final List<String> arrowFieldNames;

    private boolean finished;
    private long completedBytes;
    private long completedPositions;
    private long readTimeNanos;

    public FlightSqlPageSource(PartitionReader reader, ArrowToTrinoPageBuilder pageBuilder, List<FlightSqlColumnHandle> columns)
    {
        this.reader = requireNonNull(reader, "reader is null");
        this.pageBuilder = requireNonNull(pageBuilder, "pageBuilder is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        ImmutableList.Builder<String> names = ImmutableList.builder();
        for (FlightSqlColumnHandle column : columns) {
            types.add(column.trinoType());
            names.add(column.columnName());
        }
        this.trinoTypes = types.build();
        this.arrowFieldNames = names.build();
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (finished) {
            return null;
        }
        long start = System.nanoTime();
        try {
            if (!reader.reader().loadNextBatch()) {
                finished = true;
                return null;
            }
            VectorSchemaRoot root = reader.reader().getVectorSchemaRoot();
            Page page = pageBuilder.build(root, trinoTypes, arrowFieldNames);
            completedPositions += page.getPositionCount();
            completedBytes += page.getSizeInBytes();
            return SourcePage.create(page);
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to read Flight SQL batch: " + e.getMessage(), e);
        }
        finally {
            readTimeNanos += System.nanoTime() - start;
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return ConnectorPageSource.super.isBlocked();
    }

    @Override
    public void close()
            throws IOException
    {
        finished = true;
        try {
            reader.close();
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }
}
