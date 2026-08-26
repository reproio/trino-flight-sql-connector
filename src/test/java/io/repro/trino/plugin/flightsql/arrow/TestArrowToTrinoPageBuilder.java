package io.repro.trino.plugin.flightsql.arrow;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestArrowToTrinoPageBuilder
{
    private final ArrowToTrinoPageBuilder pageBuilder = new ArrowToTrinoPageBuilder();
    private BufferAllocator allocator;

    @BeforeAll
    void setup()
    {
        allocator = new RootAllocator();
    }

    @AfterAll
    void teardown()
    {
        allocator.close();
    }

    @Test
    void timeMillis()
    {
        TimeType type = TimeType.createTimeType(3);
        try (TimeMilliVector vector = new TimeMilliVector("t", allocator)) {
            vector.allocateNew(2);
            vector.set(0, 45_296_789); // 12:34:56.789
            vector.setNull(1);
            Block block = buildSingleColumnPage(vector, type, 2);
            assertThat(type.getLong(block, 0)).isEqualTo(45_296_789L * 1_000_000_000L);
            assertThat(block.isNull(1)).isTrue();
        }
    }

    @Test
    void timeMicros()
    {
        TimeType type = TimeType.createTimeType(6);
        try (TimeMicroVector vector = new TimeMicroVector("t", allocator)) {
            vector.allocateNew(1);
            vector.set(0, 45_296_789_012L); // 12:34:56.789012
            Block block = buildSingleColumnPage(vector, type, 1);
            assertThat(type.getLong(block, 0)).isEqualTo(45_296_789_012L * 1_000_000L);
        }
    }

    @Test
    void timestampNanos()
    {
        TimestampType type = TimestampType.createTimestampType(9);
        try (TimeStampNanoVector vector = new TimeStampNanoVector("ts", allocator)) {
            vector.allocateNew(3);
            vector.set(0, 1_704_164_645_123_456_789L); // 2024-01-02T03:04:05.123456789Z
            vector.set(1, -999L); // pre-epoch: floor division must round down
            vector.setNull(2);
            Block block = buildSingleColumnPage(vector, type, 3);
            LongTimestamp first = (LongTimestamp) type.getObject(block, 0);
            assertThat(first.getEpochMicros()).isEqualTo(1_704_164_645_123_456L);
            assertThat(first.getPicosOfMicro()).isEqualTo(789_000);
            LongTimestamp second = (LongTimestamp) type.getObject(block, 1);
            assertThat(second.getEpochMicros()).isEqualTo(-1L);
            assertThat(second.getPicosOfMicro()).isEqualTo(1_000);
            assertThat(block.isNull(2)).isTrue();
        }
    }

    @Test
    void timestampMillisWithTimeZone()
    {
        TimestampWithTimeZoneType type = TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3);
        Field field = new Field("ts", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")), List.of());
        try (TimeStampMilliTZVector vector = (TimeStampMilliTZVector) field.createVector(allocator)) {
            vector.allocateNew(2);
            vector.set(0, 1_704_164_645_123L); // 2024-01-02T03:04:05.123Z
            vector.setNull(1);
            Block block = buildSingleColumnPage(vector, type, 2);
            long packed = type.getLong(block, 0);
            assertThat(unpackMillisUtc(packed)).isEqualTo(1_704_164_645_123L);
            assertThat(unpackZoneKey(packed)).isEqualTo(TimeZoneKey.UTC_KEY);
            assertThat(block.isNull(1)).isTrue();
        }
    }

    @Test
    void timestampMicrosWithTimeZone()
    {
        TimestampWithTimeZoneType type = TimestampWithTimeZoneType.createTimestampWithTimeZoneType(6);
        Field field = new Field("ts", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "Asia/Tokyo")), List.of());
        try (TimeStampMicroTZVector vector = (TimeStampMicroTZVector) field.createVector(allocator)) {
            vector.allocateNew(1);
            vector.set(0, 1_704_164_645_123_456L); // 2024-01-02T03:04:05.123456Z
            Block block = buildSingleColumnPage(vector, type, 1);
            LongTimestampWithTimeZone value = (LongTimestampWithTimeZone) type.getObject(block, 0);
            assertThat(value.getEpochMillis()).isEqualTo(1_704_164_645_123L);
            assertThat(value.getPicosOfMilli()).isEqualTo(456_000_000);
            assertThat(value.getTimeZoneKey()).isEqualTo(TimeZoneKey.getTimeZoneKey("Asia/Tokyo").getKey());
        }
    }

    private Block buildSingleColumnPage(FieldVector vector, Type type, int rowCount)
    {
        vector.setValueCount(rowCount);
        try (VectorSchemaRoot root = VectorSchemaRoot.of(vector)) {
            root.setRowCount(rowCount);
            Page page = pageBuilder.build(root, List.of(type), List.of(vector.getName()));
            assertThat(page.getPositionCount()).isEqualTo(rowCount);
            return page.getBlock(0);
        }
    }
}
