package io.repro.trino.plugin.flightsql;

import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestFlightSqlConnectorSmokeTest
{
    private TestingFlightSqlServer flightServer;
    private QueryRunner queryRunner;

    @BeforeAll
    void setup()
            throws Exception
    {
        flightServer = new TestingFlightSqlServer();
        queryRunner = FlightSqlQueryRunner.createFlightSqlQueryRunner(flightServer);
    }

    @AfterAll
    void teardown()
            throws Exception
    {
        if (queryRunner != null) {
            queryRunner.close();
        }
        if (flightServer != null) {
            flightServer.close();
        }
    }

    @Test
    void showSchemas()
    {
        MaterializedResult result = queryRunner.execute("SHOW SCHEMAS FROM flight");
        assertThat(result.getRowCount()).isGreaterThan(0);
    }
}
