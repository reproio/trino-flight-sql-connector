package io.repro.trino.plugin.flightsql.arrow;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
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
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.math.BigInteger;
import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
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
        // Timestamp without TZ -> long microseconds for short timestamps (precision <= 6)
        if (type instanceof io.trino.spi.type.TimestampType tsType && !tsType.isShort() == false) {
            // short timestamp
            long[] micros = readTimestampMicros(vector, rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (vector.isNull(i)) {
                    blockBuilder.appendNull();
                }
                else {
                    tsType.writeLong(blockBuilder, micros[i]);
                }
            }
            return;
        }
        throw new UnsupportedOperationException("Unsupported Trino type for Arrow conversion: " + type);
    }

    private static long[] readTimestampMicros(FieldVector vector, int rowCount)
    {
        long[] out = new long[rowCount];
        if (vector instanceof TimeStampSecVector v) {
            for (int i = 0; i < rowCount; i++) {
                out[i] = v.isNull(i) ? 0L : v.get(i) * 1_000_000L;
            }
        }
        else if (vector instanceof TimeStampMilliVector v) {
            for (int i = 0; i < rowCount; i++) {
                out[i] = v.isNull(i) ? 0L : v.get(i) * 1_000L;
            }
        }
        else if (vector instanceof TimeStampMicroVector v) {
            for (int i = 0; i < rowCount; i++) {
                out[i] = v.isNull(i) ? 0L : v.get(i);
            }
        }
        else if (vector instanceof TimeStampNanoVector v) {
            for (int i = 0; i < rowCount; i++) {
                out[i] = v.isNull(i) ? 0L : v.get(i) / 1_000L;
            }
        }
        else if (vector instanceof TimeStampSecTZVector v) {
            for (int i = 0; i < rowCount; i++) {
                out[i] = v.isNull(i) ? 0L : v.get(i) * 1_000_000L;
            }
        }
        else if (vector instanceof TimeStampMilliTZVector v) {
            for (int i = 0; i < rowCount; i++) {
                out[i] = v.isNull(i) ? 0L : v.get(i) * 1_000L;
            }
        }
        else if (vector instanceof TimeStampMicroTZVector v) {
            for (int i = 0; i < rowCount; i++) {
                out[i] = v.isNull(i) ? 0L : v.get(i);
            }
        }
        else if (vector instanceof TimeStampNanoTZVector v) {
            for (int i = 0; i < rowCount; i++) {
                out[i] = v.isNull(i) ? 0L : v.get(i) / 1_000L;
            }
        }
        else {
            throw new IllegalArgumentException("Unsupported timestamp vector type: " + vector.getClass());
        }
        return out;
    }
}
