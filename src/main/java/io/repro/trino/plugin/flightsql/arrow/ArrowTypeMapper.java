package io.repro.trino.plugin.flightsql.arrow;

import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.Optional;

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

public class ArrowTypeMapper
{
    public Optional<Type> toTrinoType(Field field)
    {
        return toTrinoType(field.getType());
    }

    public Optional<Type> toTrinoType(ArrowType arrowType)
    {
        switch (arrowType.getTypeID()) {
            case Bool -> {
                return Optional.of(BOOLEAN);
            }
            case Int -> {
                ArrowType.Int intType = (ArrowType.Int) arrowType;
                if (!intType.getIsSigned()) {
                    return Optional.empty();
                }
                return switch (intType.getBitWidth()) {
                    case 8 -> Optional.of(TINYINT);
                    case 16 -> Optional.of(SMALLINT);
                    case 32 -> Optional.of(INTEGER);
                    case 64 -> Optional.of(BIGINT);
                    default -> Optional.empty();
                };
            }
            case FloatingPoint -> {
                ArrowType.FloatingPoint floatType = (ArrowType.FloatingPoint) arrowType;
                return switch (floatType.getPrecision()) {
                    case SINGLE -> Optional.of(REAL);
                    case DOUBLE -> Optional.of(DOUBLE);
                    default -> Optional.empty();
                };
            }
            case Decimal -> {
                ArrowType.Decimal decimal = (ArrowType.Decimal) arrowType;
                if (decimal.getPrecision() > Decimals.MAX_PRECISION) {
                    return Optional.empty();
                }
                return Optional.of(DecimalType.createDecimalType(decimal.getPrecision(), decimal.getScale()));
            }
            case Utf8, LargeUtf8 -> {
                return Optional.of(VARCHAR);
            }
            case Binary, LargeBinary, FixedSizeBinary -> {
                return Optional.of(VARBINARY);
            }
            case Date -> {
                return Optional.of(DATE);
            }
            case Time -> {
                ArrowType.Time time = (ArrowType.Time) arrowType;
                int precision = switch (time.getUnit()) {
                    case SECOND -> 0;
                    case MILLISECOND -> 3;
                    case MICROSECOND -> 6;
                    case NANOSECOND -> 9;
                };
                return Optional.of(TimeType.createTimeType(Math.min(precision, TimeType.MAX_PRECISION)));
            }
            case Timestamp -> {
                ArrowType.Timestamp ts = (ArrowType.Timestamp) arrowType;
                int precision = switch (ts.getUnit()) {
                    case SECOND -> 0;
                    case MILLISECOND -> 3;
                    case MICROSECOND -> 6;
                    case NANOSECOND -> 9;
                };
                if (ts.getTimezone() != null && !ts.getTimezone().isEmpty()) {
                    return Optional.of(TimestampWithTimeZoneType.createTimestampWithTimeZoneType(Math.min(precision, TimestampWithTimeZoneType.MAX_PRECISION)));
                }
                return Optional.of(TimestampType.createTimestampType(Math.min(precision, TimestampType.MAX_PRECISION)));
            }
            default -> {
                return Optional.empty();
            }
        }
    }
}
