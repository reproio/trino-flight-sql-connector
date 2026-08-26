package io.repro.trino.plugin.flightsql;

import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

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
        assertThat(result.getOnlyColumnAsSet()).contains("app");
    }

    @Test
    void showTables()
    {
        MaterializedResult result = queryRunner.execute("SHOW TABLES FROM flight.app");
        assertThat(result.getOnlyColumnAsSet()).contains("inttable");
    }

    @Test
    void selectColumns()
    {
        MaterializedResult result = queryRunner.execute("SELECT id, value FROM flight.app.inttable");
        assertThat(result.getRowCount()).isGreaterThan(0);
    }

    @Test
    void predicatePushdown()
    {
        MaterializedResult result = queryRunner.execute("SELECT value FROM flight.app.inttable WHERE id = 1");
        assertThat(result.getRowCount()).isLessThanOrEqualTo(1);
    }

    @Test
    void countAll()
    {
        MaterializedResult expected = queryRunner.execute("SELECT id FROM flight.app.inttable");
        MaterializedResult result = queryRunner.execute("SELECT count(*) FROM flight.app.inttable");
        assertThat(result.getOnlyValue()).isEqualTo((long) expected.getRowCount());
    }

    @Test
    void countWithPredicate()
    {
        MaterializedResult result = queryRunner.execute("SELECT count(*) FROM flight.app.inttable WHERE id = 1");
        assertThat((long) result.getOnlyValue()).isLessThanOrEqualTo(1L);
    }

    @Test
    void selectTimeColumn()
    {
        // Derby TIME maps to Arrow Time32(MILLISECOND) -> Trino TIME(3)
        MaterializedResult result = queryRunner.execute("SELECT t FROM flight.app.timetable WHERE id = 1");
        assertThat(result.getOnlyValue()).isEqualTo(LocalTime.of(12, 34, 56));
    }

    @Test
    void selectTimestampColumn()
    {
        // FlightSqlExample converts JDBC results with a UTC calendar, so Derby TIMESTAMP maps to
        // Arrow Timestamp(MILLISECOND, "UTC") -> Trino TIMESTAMP(3) WITH TIME ZONE
        MaterializedResult result = queryRunner.execute("SELECT ts FROM flight.app.timetable WHERE id = 1");
        assertThat(result.getOnlyValue())
                .isEqualTo(ZonedDateTime.of(2024, 1, 2, 3, 4, 5, 123_000_000, ZoneId.of("UTC")));
    }

    @Test
    void selectNullTemporalValues()
    {
        MaterializedResult result = queryRunner.execute("SELECT t, ts FROM flight.app.timetable WHERE id = 2");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isNull();
        assertThat(result.getMaterializedRows().get(0).getField(1)).isNull();
    }
}
