package io.repro.trino.plugin.flightsql.client;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.vector.ipc.ArrowReader;

import static java.util.Objects.requireNonNull;

/**
 * Owns the AdbcConnection (and optionally AdbcStatement / QueryResult)
 * that backs the ArrowReader. Closing this releases everything.
 */
public final class PartitionReader
        implements AutoCloseable
{
    private final AdbcConnection connection;
    private final AdbcStatement statement;
    private final AdbcStatement.QueryResult queryResult;
    private final ArrowReader reader;

    PartitionReader(AdbcConnection connection, ArrowReader reader)
    {
        this(connection, null, null, reader);
    }

    PartitionReader(AdbcConnection connection, AdbcStatement statement, AdbcStatement.QueryResult queryResult)
    {
        this(connection, statement, queryResult, queryResult.getReader());
    }

    private PartitionReader(AdbcConnection connection, AdbcStatement statement, AdbcStatement.QueryResult queryResult, ArrowReader reader)
    {
        this.connection = requireNonNull(connection, "connection is null");
        this.statement = statement;
        this.queryResult = queryResult;
        this.reader = requireNonNull(reader, "reader is null");
    }

    public ArrowReader reader()
    {
        return reader;
    }

    @Override
    public void close()
            throws Exception
    {
        Exception last = null;
        try {
            reader.close();
        }
        catch (Exception e) {
            last = e;
        }
        if (queryResult != null) {
            try {
                queryResult.close();
            }
            catch (Exception e) {
                last = chain(last, e);
            }
        }
        if (statement != null) {
            try {
                statement.close();
            }
            catch (Exception e) {
                last = chain(last, e);
            }
        }
        try {
            connection.close();
        }
        catch (Exception e) {
            last = chain(last, e);
        }
        if (last != null) {
            throw last;
        }
    }

    private static Exception chain(Exception prev, Exception next)
    {
        if (prev == null) {
            return next;
        }
        prev.addSuppressed(next);
        return prev;
    }
}
