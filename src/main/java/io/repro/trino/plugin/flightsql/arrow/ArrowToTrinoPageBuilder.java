package io.repro.trino.plugin.flightsql.arrow;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.math.BigInteger;
import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class ArrowToTrinoPageBuilder
{
    private static final long MICROS_PER_DAY = 86_400_000_000L;
    private static final long MILLIS_PER_DAY = 86_400_000L;

    public Page build(VectorSchemaRoot root, List<Type> trinoTypes, List<String> arrowFieldNames)
    {
        requireNonNull(root, "root is null");
        requireNonNull(trinoTypes, "trinoTypes is null");
        requireNonNull(arrowFieldNames, "arrowFieldNames is null");
        if (trinoTypes.size() != arrowFieldNames.size()) {
            throw new IllegalArgumentException("trinoTypes and arrowFieldNames must be the same length");
        }
        int rowCount = root.getRowCount();
        PageBuilder pageBuilder = new PageBuilder(rowCount, trinoTypes);
        pageBuilder.declarePositions(rowCount);
        for (int col = 0; col < trinoTypes.size(); col++) {
            FieldVector vector = findFieldVector(root, arrowFieldNames.get(col));
            if (vector == null) {
                throw new IllegalStateException("Arrow field not found in result: " + arrowFieldNames.get(col)
                        + " (available: " + root.getSchema().getFields().stream().map(f -> f.getName()).toList() + ")");
            }
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(col);
            appendColumn(trinoTypes.get(col), vector, blockBuilder, rowCount);
        }
        return pageBuilder.build();
    }

    private static FieldVector findFieldVector(VectorSchemaRoot root, String name)
    {
        FieldVector exact = root.getVector(name);
        if (exact != null) {
            return exact;
        }
        for (FieldVector v : root.getFieldVectors()) {
            if (v.getName().equalsIgnoreCase(name)) {
                return v;
            }
        }
        return null;
    }

    private void appendColumn(Type type, FieldVector vector, BlockBuilder blockBuilder, int rowCount)
    {
        if (BOOLEAN.equals(type)) {
            BitVector bv = (BitVector) vector;
            for (int i = 0; i < rowCount; i++) {
                if (bv.isNull(i)) {
                    blockBuilder.appendNull();
                }
                else {
                    BOOLEAN.writeBoolean(blockBuilder, bv.get(i) != 0);
                }
            }
            return;
        }
        if (TINYINT.equals(type)) {
            TinyIntVector tv = (TinyIntVector) vector;
            for (int i = 0; i < rowCount; i++) {
                if (tv.isNull(i)) {
                    blockBuilder.appendNull();
                }
                else {
                    TINYINT.writeLong(blockBuilder, tv.get(i));
                }
            }
            return;
        }
        if (SMALLINT.equals(type)) {
            SmallIntVector sv = (SmallIntVector) vector;
            for (int i = 0; i < rowCount; i++) {
                if (sv.isNull(i)) {
                    blockBuilder.appendNull();
                }
                else {
                    SMALLINT.writeLong(blockBuilder, sv.get(i));
                }
            }
            return;
        }
        if (INTEGER.equals(type)) {
            IntVector iv = (IntVector) vector;
            for (int i = 0; i < rowCount; i++) {
                if (iv.isNull(i)) {
                    blockBuilder.appendNull();
                }
                else {
                    INTEGER.writeLong(blockBuilder, iv.get(i));
                }
            }
            return;
        }
        if (BIGINT.equals(type)) {
            BigIntVector bv = (BigIntVector) vector;
            for (int i = 0; i < rowCount; i++) {
                if (bv.isNull(i)) {
                    blockBuilder.appendNull();
                }
                else {
                    BIGINT.writeLong(blockBuilder, bv.get(i));
                }
            }
            return;
        }
        if (REAL.equals(type)) {
            Float4Vector fv = (Float4Vector) vector;
            for (int i = 0; i < rowCount; i++) {
                if (fv.isNull(i)) {
                    blockBuilder.appendNull();
                }
                else {
                    REAL.writeLong(blockBuilder, Float.floatToIntBits(fv.get(i)));
                }
            }
            return;
        }
        if (DOUBLE.equals(type)) {
            Float8Vector dv = (Float8Vector) vector;
            for (int i = 0; i < rowCount; i++) {
                if (dv.isNull(i)) {
                    blockBuilder.appendNull();
                }
                else {
                    DOUBLE.writeDouble(blockBuilder, dv.get(i));
                }
            }
            return;
        }
        if (type instanceof DecimalType decimalType) {
            DecimalVector dv = (DecimalVector) vector;
            for (int i = 0; i < rowCount; i++) {
                if (dv.isNull(i)) {
                    blockBuilder.appendNull();
                    continue;
                }
                BigInteger unscaled = dv.getObject(i).unscaledValue();
                if (decimalType.isShort()) {
                    decimalType.writeLong(blockBuilder, unscaled.longValueExact());
                }
                else {
                    decimalType.writeObject(blockBuilder, Decimals.valueOf(unscaled));
                }
            }
            return;
        }
        if (VARCHAR.equals(type)) {
            VarCharVector vv = (VarCharVector) vector;
            for (int i = 0; i < rowCount; i++) {
                if (vv.isNull(i)) {
                    blockBuilder.appendNull();
                }
                else {
                    VARCHAR.writeSlice(blockBuilder, Slices.wrappedBuffer(vv.get(i)));
                }
            }
            return;
        }
        if (VARBINARY.equals(type)) {
            VarBinaryVector vb = (VarBinaryVector) vector;
            for (int i = 0; i < rowCount; i++) {
                if (vb.isNull(i)) {
                    blockBuilder.appendNull();
                }
                else {
                    VARBINARY.writeSlice(blockBuilder, Slices.wrappedBuffer(vb.get(i)));
                }
            }
            return;
        }
        if (DATE.equals(type)) {
            if (vector instanceof DateDayVector dv) {
                for (int i = 0; i < rowCount; i++) {
                    if (dv.isNull(i)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        DATE.writeLong(blockBuilder, dv.get(i));
                    }
                }
                return;
            }
            if (vector instanceof DateMilliVector dv) {
                for (int i = 0; i < rowCount; i++) {
                    if (dv.isNull(i)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        DATE.writeLong(blockBuilder, dv.get(i) / MILLIS_PER_DAY);
                    }
                }
                return;
            }
        }
        if (type instanceof TimeType timeType) {
            for (int i = 0; i < rowCount; i++) {
                if (vector.isNull(i)) {
                    blockBuilder.appendNull();
                }
                else {
                    timeType.writeLong(blockBuilder, timePicos(vector, i));
                }
            }
            return;
        }
        if (type instanceof TimestampType timestampType) {
            TimeStampVector timestampVector = asTimestampVector(vector);
            TimeUnit unit = timestampUnit(timestampVector);
            for (int i = 0; i < rowCount; i++) {
                if (timestampVector.isNull(i)) {
                    blockBuilder.appendNull();
                    continue;
                }
                long value = timestampVector.get(i);
                if (timestampType.isShort()) {
                    timestampType.writeLong(blockBuilder, toEpochMicros(value, unit));
                }
                else {
                    int picosOfMicro = unit == TimeUnit.NANOSECOND ? (int) Math.floorMod(value, 1_000L) * 1_000 : 0;
                    timestampType.writeObject(blockBuilder, new LongTimestamp(toEpochMicros(value, unit), picosOfMicro));
                }
            }
            return;
        }
        if (type instanceof TimestampWithTimeZoneType timestampTzType) {
            TimeStampVector timestampVector = asTimestampVector(vector);
            TimeUnit unit = timestampUnit(timestampVector);
            TimeZoneKey zoneKey = timeZoneKey(timestampVector);
            for (int i = 0; i < rowCount; i++) {
                if (timestampVector.isNull(i)) {
                    blockBuilder.appendNull();
                    continue;
                }
                long value = timestampVector.get(i);
                long epochMillis = toEpochMillis(value, unit);
                if (timestampTzType.isShort()) {
                    timestampTzType.writeLong(blockBuilder, packDateTimeWithZone(epochMillis, zoneKey));
                }
                else {
                    int picosOfMilli = switch (unit) {
                        case SECOND, MILLISECOND -> 0;
                        case MICROSECOND -> (int) Math.floorMod(value, 1_000L) * 1_000_000;
                        case NANOSECOND -> (int) Math.floorMod(value, 1_000_000L) * 1_000;
                    };
                    timestampTzType.writeObject(blockBuilder,
                            LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, picosOfMilli, zoneKey));
                }
            }
            return;
        }
        throw new UnsupportedOperationException("Unsupported Trino type for Arrow conversion: " + type);
    }

    // Trino TIME(p) values are picoseconds of day
    private static long timePicos(FieldVector vector, int index)
    {
        if (vector instanceof TimeSecVector v) {
            return v.get(index) * 1_000_000_000_000L;
        }
        if (vector instanceof TimeMilliVector v) {
            return v.get(index) * 1_000_000_000L;
        }
        if (vector instanceof TimeMicroVector v) {
            return v.get(index) * 1_000_000L;
        }
        if (vector instanceof TimeNanoVector v) {
            return v.get(index) * 1_000L;
        }
        throw new IllegalArgumentException("Unsupported time vector type: " + vector.getClass());
    }

    private static TimeStampVector asTimestampVector(FieldVector vector)
    {
        if (vector instanceof TimeStampVector timestampVector) {
            return timestampVector;
        }
        throw new IllegalArgumentException("Unsupported timestamp vector type: " + vector.getClass());
    }

    private static TimeUnit timestampUnit(TimeStampVector vector)
    {
        return ((ArrowType.Timestamp) vector.getField().getType()).getUnit();
    }

    private static TimeZoneKey timeZoneKey(TimeStampVector vector)
    {
        String timezone = ((ArrowType.Timestamp) vector.getField().getType()).getTimezone();
        if (timezone == null || timezone.isEmpty()) {
            return TimeZoneKey.UTC_KEY;
        }
        return TimeZoneKey.getTimeZoneKey(timezone);
    }

    private static long toEpochMicros(long value, TimeUnit unit)
    {
        return switch (unit) {
            case SECOND -> value * 1_000_000L;
            case MILLISECOND -> value * 1_000L;
            case MICROSECOND -> value;
            case NANOSECOND -> Math.floorDiv(value, 1_000L);
        };
    }

    private static long toEpochMillis(long value, TimeUnit unit)
    {
        return switch (unit) {
            case SECOND -> value * 1_000L;
            case MILLISECOND -> value;
            case MICROSECOND -> Math.floorDiv(value, 1_000L);
            case NANOSECOND -> Math.floorDiv(value, 1_000_000L);
        };
    }
}
