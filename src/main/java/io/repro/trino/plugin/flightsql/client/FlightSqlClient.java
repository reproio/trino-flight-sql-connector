package io.repro.trino.plugin.flightsql.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.repro.trino.plugin.flightsql.FlightSqlConfig;
import jakarta.annotation.PreDestroy;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.PartitionDescriptor;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlConnectionProperties;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

import jakarta.inject.Inject;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class FlightSqlClient
        implements AutoCloseable
{
    private final FlightSqlConfig config;
    private final BufferAllocator allocator;
    private final AdbcDatabase database;

    @Inject
    public FlightSqlClient(FlightSqlConfig config)
    {
        this.config = requireNonNull(config, "config is null");
        this.allocator = new RootAllocator();
        try {
            Map<String, Object> params = buildParameters(config);
            FlightSqlDriver driver = new FlightSqlDriver(allocator);
            this.database = driver.open(params);
        }
        catch (Exception e) {
            allocator.close();
            throw new RuntimeException("Failed to open Flight SQL ADBC database: " + e.getMessage(), e);
        }
    }

    private static Map<String, Object> buildParameters(FlightSqlConfig config)
            throws Exception
    {
        Map<String, Object> params = new HashMap<>();
        AdbcDriver.PARAM_URI.set(params, config.getUri());
        if (config.getUsername() != null) {
            AdbcDriver.PARAM_USERNAME.set(params, config.getUsername());
        }
        if (config.getPassword() != null) {
            AdbcDriver.PARAM_PASSWORD.set(params, config.getPassword());
        }
        if (config.isTlsSkipVerify()) {
            FlightSqlConnectionProperties.TLS_SKIP_VERIFY.set(params, Boolean.TRUE);
        }
        if (config.getTlsTrustStorePath() != null) {
            InputStream certs = Files.newInputStream(Paths.get(config.getTlsTrustStorePath()));
            FlightSqlConnectionProperties.TLS_ROOT_CERTS.set(params, certs);
        }
        if (config.getAuthorizationHeader() != null) {
            params.put(FlightSqlConnectionProperties.RPC_CALL_HEADER_PREFIX + "authorization", config.getAuthorizationHeader());
        }
        for (Map.Entry<String, String> entry : config.getParsedRpcHeaders().entrySet()) {
            params.put(FlightSqlConnectionProperties.RPC_CALL_HEADER_PREFIX + entry.getKey().toLowerCase(), entry.getValue());
        }
        if (config.getDefaultDatabase() != null) {
            params.put(FlightSqlConnectionProperties.RPC_CALL_HEADER_PREFIX + "database", config.getDefaultDatabase());
        }
        return params;
    }

    public BufferAllocator allocator()
    {
        return allocator;
    }

    public FlightSqlConfig config()
    {
        return config;
    }

    public AdbcConnection openConnection()
            throws AdbcException
    {
        return database.connect();
    }

    /**
     * ADBC's getObjects is not reliably implemented across Flight SQL servers
     * (notably FlightSqlExample). We therefore discover metadata via SQL queries
     * against well-known system catalogs, dispatched by `flight.dialect`.
     */
    /**
     * Returns lowercase schema names. Trino normalises connector-returned identifiers
     * to lower case for display and lookup, so we lowercase here and pass lowercase
     * (unquoted) names to the backend in generated SQL — Derby's parser uppercases
     * unquoted identifiers and matches its uppercase storage, while DuckDB/PostgreSQL
     * with their default lowercase storage match directly.
     */
    public Set<String> listSchemaNames()
            throws Exception
    {
        String sql = switch (config.getDialect()) {
            case DERBY -> "SELECT SCHEMANAME FROM SYS.SYSSCHEMAS";
            case DUCKDB -> "SELECT schema_name FROM information_schema.schemata";
        };
        return runLowerCaseStringQuery(sql);
    }

    public Set<String> listTables(String schemaName)
            throws Exception
    {
        String sql = switch (config.getDialect()) {
            case DERBY -> "SELECT T.TABLENAME FROM SYS.SYSTABLES T "
                    + "JOIN SYS.SYSSCHEMAS S ON T.SCHEMAID = S.SCHEMAID "
                    + "WHERE UPPER(S.SCHEMANAME) = '" + escape(schemaName.toUpperCase()) + "' AND T.TABLETYPE IN ('T', 'V')";
            case DUCKDB -> "SELECT table_name FROM information_schema.tables "
                    + "WHERE LOWER(table_schema) = '" + escape(schemaName.toLowerCase()) + "'";
        };
        return runLowerCaseStringQuery(sql);
    }

    /**
     * Returns the Arrow schema of a table via `SELECT * FROM schema.table WHERE 1=0`
     * (unquoted; rely on the backend's case-insensitive identifier parsing).
     */
    public Schema getTableSchema(String schemaName, String tableName)
            throws Exception
    {
        String sql = "SELECT * FROM " + schemaName + "." + tableName + " WHERE 1=0";
        PartitionedExecuteResult result = executePartitioned(sql);
        if (result.schema() == null) {
            throw new IllegalStateException("Flight SQL server returned no schema for " + schemaName + "." + tableName);
        }
        return result.schema();
    }

    public PartitionedExecuteResult executePartitioned(String sql)
            throws Exception
    {
        try (AdbcConnection conn = openConnection();
                AdbcStatement stmt = conn.createStatement()) {
            stmt.setSqlQuery(sql);
            AdbcStatement.PartitionResult partitionResult = stmt.executePartitioned();
            ImmutableList.Builder<byte[]> copies = ImmutableList.builder();
            for (PartitionDescriptor descriptor : partitionResult.getPartitionDescriptors()) {
                copies.add(toByteArray(descriptor.getDescriptor()));
            }
            return new PartitionedExecuteResult(partitionResult.getSchema(), copies.build(), partitionResult.getAffectedRows());
        }
    }

    public PartitionReader readPartition(byte[] descriptor)
            throws AdbcException
    {
        AdbcConnection conn = openConnection();
        try {
            ArrowReader reader = conn.readPartition(ByteBuffer.wrap(descriptor));
            return new PartitionReader(conn, reader);
        }
        catch (Exception e) {
            try {
                conn.close();
            }
            catch (Exception suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    public PartitionReader executeQuery(String sql)
            throws AdbcException
    {
        AdbcConnection conn = openConnection();
        try {
            AdbcStatement stmt = conn.createStatement();
            try {
                stmt.setSqlQuery(sql);
                AdbcStatement.QueryResult queryResult = stmt.executeQuery();
                return new PartitionReader(conn, stmt, queryResult);
            }
            catch (Exception e) {
                try {
                    stmt.close();
                }
                catch (Exception suppressed) {
                    e.addSuppressed(suppressed);
                }
                throw e;
            }
        }
        catch (Exception e) {
            try {
                conn.close();
            }
            catch (Exception suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    private Set<String> runLowerCaseStringQuery(String sql)
            throws Exception
    {
        Set<String> result = new LinkedHashSet<>();
        try (PartitionReader pr = executeQuery(sql)) {
            ArrowReader reader = pr.reader();
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                VarCharVector vec = (VarCharVector) root.getVector(0);
                for (int i = 0; i < root.getRowCount(); i++) {
                    if (!vec.isNull(i)) {
                        result.add(new String(vec.get(i), UTF_8).toLowerCase());
                    }
                }
            }
        }
        return ImmutableSet.copyOf(result);
    }

    private static byte[] toByteArray(ByteBuffer buffer)
    {
        ByteBuffer copy = buffer.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }

    private static String escape(String value)
    {
        return value.replace("'", "''");
    }

    @PreDestroy
    @Override
    public void close()
    {
        try {
            database.close();
        }
        catch (Exception ignored) {
            // best effort
        }
        finally {
            allocator.close();
        }
    }

    public record PartitionedExecuteResult(Schema schema, java.util.List<byte[]> partitionDescriptors, long affectedRows) {}
}
