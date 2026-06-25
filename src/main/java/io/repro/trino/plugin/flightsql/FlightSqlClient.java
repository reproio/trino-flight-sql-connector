package io.repro.trino.plugin.flightsql;

import com.google.inject.Inject;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.sql.Connection;
import java.sql.Types;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateColumnMappingUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultCharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;

public class FlightSqlClient
        extends BaseJdbcClient
{
    @Inject
    public FlightSqlClient(
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super(
                "\"",
                connectionFactory,
                queryBuilder,
                Set.of(),
                identifierMapping,
                queryModifier,
                false);
    }

    /**
     * 方言依存の境界点。Phase 6 で多バックエンド化する際は本メソッドを方言別
     * サブクラスへ移し、ここでは Flight SQL 共通の枠組み (switch のテンプレート)
     * のみ残す想定。
     */
    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        return switch (typeHandle.jdbcType()) {
            case Types.BIT, Types.BOOLEAN -> Optional.of(booleanColumnMapping());
            case Types.TINYINT -> Optional.of(tinyintColumnMapping());
            case Types.SMALLINT -> Optional.of(smallintColumnMapping());
            case Types.INTEGER -> Optional.of(integerColumnMapping());
            case Types.BIGINT -> Optional.of(bigintColumnMapping());
            case Types.REAL -> Optional.of(realColumnMapping());
            case Types.FLOAT, Types.DOUBLE -> Optional.of(doubleColumnMapping());
            case Types.NUMERIC, Types.DECIMAL -> mapDecimal(typeHandle);
            case Types.CHAR, Types.NCHAR -> Optional.of(
                    defaultCharColumnMapping(typeHandle.columnSize().orElse(CharType.MAX_LENGTH), true));
            case Types.VARCHAR, Types.NVARCHAR, Types.LONGVARCHAR, Types.LONGNVARCHAR -> Optional.of(
                    defaultVarcharColumnMapping(typeHandle.columnSize().orElse(VarcharType.UNBOUNDED_LENGTH), true));
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> Optional.of(varbinaryColumnMapping());
            case Types.DATE -> Optional.of(dateColumnMappingUsingLocalDate());
            case Types.TIME -> mapTime(typeHandle);
            case Types.TIMESTAMP -> mapTimestamp(typeHandle);
            // TIMESTAMP_WITH_TIMEZONE, ARRAY, STRUCT, MAP は Phase 2 以降
            default -> Optional.empty();
        };
    }

    private static Optional<ColumnMapping> mapDecimal(JdbcTypeHandle typeHandle)
    {
        int precision = typeHandle.columnSize().orElse(DecimalType.DEFAULT_PRECISION);
        int scale = typeHandle.decimalDigits().orElse(0);
        if (precision <= 0 || precision > Decimals.MAX_PRECISION || scale < 0 || scale > precision) {
            return Optional.empty();
        }
        return Optional.of(decimalColumnMapping(DecimalType.createDecimalType(precision, scale)));
    }

    private static Optional<ColumnMapping> mapTime(JdbcTypeHandle typeHandle)
    {
        int precision = typeHandle.decimalDigits().orElse(0);
        if (precision < 0 || precision > TimeType.MAX_PRECISION) {
            return Optional.empty();
        }
        return Optional.of(timeColumnMapping(TimeType.createTimeType(precision)));
    }

    private static Optional<ColumnMapping> mapTimestamp(JdbcTypeHandle typeHandle)
    {
        int precision = typeHandle.decimalDigits().orElse(0);
        if (precision < 0 || precision > TimestampType.MAX_SHORT_PRECISION) {
            return Optional.empty();
        }
        return Optional.of(timestampColumnMapping(TimestampType.createTimestampType(precision)));
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == io.trino.spi.type.BooleanType.BOOLEAN) {
            return WriteMapping.booleanMapping("boolean", booleanWriteFunction());
        }
        if (type == io.trino.spi.type.TinyintType.TINYINT) {
            return WriteMapping.longMapping("tinyint", tinyintWriteFunction());
        }
        if (type == io.trino.spi.type.SmallintType.SMALLINT) {
            return WriteMapping.longMapping("smallint", smallintWriteFunction());
        }
        if (type == io.trino.spi.type.IntegerType.INTEGER) {
            return WriteMapping.longMapping("integer", integerWriteFunction());
        }
        if (type == io.trino.spi.type.BigintType.BIGINT) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }
        if (type == io.trino.spi.type.RealType.REAL) {
            return WriteMapping.longMapping("real", realWriteFunction());
        }
        if (type == io.trino.spi.type.DoubleType.DOUBLE) {
            return WriteMapping.doubleMapping("double", doubleWriteFunction());
        }
        if (type instanceof DecimalType decimal) {
            String dataType = format("decimal(%d, %d)", decimal.getPrecision(), decimal.getScale());
            if (decimal.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimal));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimal));
        }
        if (type instanceof CharType charType) {
            return WriteMapping.sliceMapping("char(" + charType.getLength() + ")", charWriteFunction());
        }
        if (type instanceof VarcharType varcharType) {
            String dataType = varcharType.isUnbounded()
                    ? "varchar"
                    : "varchar(" + varcharType.getBoundedLength() + ")";
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (type == io.trino.spi.type.VarbinaryType.VARBINARY) {
            return WriteMapping.sliceMapping("varbinary", varbinaryWriteFunction());
        }
        if (type == io.trino.spi.type.DateType.DATE) {
            return WriteMapping.longMapping("date", dateWriteFunctionUsingLocalDate());
        }
        if (type instanceof TimeType timeType) {
            return WriteMapping.longMapping("time(" + timeType.getPrecision() + ")", timeWriteFunction(timeType.getPrecision()));
        }
        if (type instanceof TimestampType timestampType && timestampType.isShort()) {
            return WriteMapping.longMapping("timestamp(" + timestampType.getPrecision() + ")", timestampWriteFunction(timestampType));
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }
}
