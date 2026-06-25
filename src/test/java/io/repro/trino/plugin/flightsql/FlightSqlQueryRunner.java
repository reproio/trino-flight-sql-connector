package io.repro.trino.plugin.flightsql;

import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;

public final class FlightSqlQueryRunner
{
    private FlightSqlQueryRunner() {}

    public static QueryRunner createFlightSqlQueryRunner(TestingFlightSqlServer server)
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("flight")
                .setSchema("app")
                .build();
        QueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        try {
            queryRunner.installPlugin(new FlightSqlPlugin());
            queryRunner.createCatalog("flight", "flight_sql", Map.of(
                    "connection-url", "jdbc:flightsql://%s:%d/".formatted(server.getHost(), server.getPort()),
                    "flight.use-encryption", "false"));
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }
}
