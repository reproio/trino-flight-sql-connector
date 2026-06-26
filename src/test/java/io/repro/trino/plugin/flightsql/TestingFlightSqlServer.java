package io.repro.trino.plugin.flightsql;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.example.FlightSqlExample;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;

public class TestingFlightSqlServer
        implements Closeable
{
    private final BufferAllocator allocator;
    private final FlightSqlExample producer;
    private final FlightServer server;
    private final int port;

    public TestingFlightSqlServer()
            throws Exception
    {
        // Derby DB が前回実行の残骸として残っていれば削除
        FlightSqlExample.removeDerbyDatabaseIfExists(FlightSqlExample.DB_NAME);
        this.port = findFreePort();
        Location location = Location.forGrpcInsecure("localhost", port);
        this.allocator = new RootAllocator();
        this.producer = new FlightSqlExample(location, FlightSqlExample.DB_NAME);
        this.server = FlightServer.builder(allocator, location, producer).build();
        this.server.start();
    }

    public String getHost()
    {
        return "localhost";
    }

    public int getPort()
    {
        return port;
    }

    @Override
    public void close()
            throws IOException
    {
        try {
            server.close();
        }
        catch (Exception ignored) {
            // best effort
        }
        try {
            producer.close();
        }
        catch (Exception e) {
            // FlightSqlExample's producer occasionally leaks its allocator (Apache Arrow
            // server-side issue independent of this connector). Swallow to avoid false
            // teardown failures.
            System.err.println("[TestingFlightSqlServer] producer.close warning: " + e.getMessage());
        }
        try {
            allocator.close();
        }
        catch (Exception ignored) {
            // best effort
        }
        try {
            FlightSqlExample.removeDerbyDatabaseIfExists(FlightSqlExample.DB_NAME);
        }
        catch (Exception ignored) {
            // best effort
        }
    }

    private static int findFreePort()
            throws IOException
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
