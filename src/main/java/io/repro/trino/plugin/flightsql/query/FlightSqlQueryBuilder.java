package io.repro.trino.plugin.flightsql.query;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.repro.trino.plugin.flightsql.FlightSqlColumnHandle;
import io.repro.trino.plugin.flightsql.FlightSqlTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.math.BigInteger;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class FlightSqlQueryBuilder
{
    public String build(FlightSqlTableHandle table, List<FlightSqlColumnHandle> columns)
    {
        String selectList = columns.isEmpty()
                ? "1"
                : columns.stream()
                        .map(c -> quoteIdentifier(c.columnName()))
                        .collect(Collectors.joining(", "));
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(selectList);
        sql.append(" FROM ").append(qualifiedTableName(table));
        String whereClause = buildWhere(table.constraint());
        if (!whereClause.isEmpty()) {
            sql.append(" WHERE ").append(whereClause);
        }
        return sql.toString();
    }

    public String qualifiedTableName(FlightSqlTableHandle table)
    {
        return quoteIdentifier(table.schemaTableName().getSchemaName())
                + "."
                + quoteIdentifier(table.schemaTableName().getTableName());
    }

    public String buildWhere(TupleDomain<ColumnHandle> constraint)
    {
        if (constraint.isAll()) {
            return "";
        }
        if (constraint.isNone()) {
            return "1 = 0";
        }
        Map<ColumnHandle, Domain> domains = constraint.getDomains().orElseThrow();
        List<String> predicates = new ArrayList<>();
        for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
            FlightSqlColumnHandle column = (FlightSqlColumnHandle) entry.getKey();
            Domain domain = entry.getValue();
            Optional<String> predicate = encodeDomain(column, domain);
            predicate.ifPresent(predicates::add);
        }
        return String.join(" AND ", predicates);
    }

    /**
     * Split the constraint into (pushable, remaining). A domain that this builder
     * cannot fully express is left in `remaining`.
     */
    public SplitConstraint split(TupleDomain<ColumnHandle> constraint)
    {
        if (constraint.isAll() || constraint.isNone()) {
            return new SplitConstraint(constraint, TupleDomain.all());
        }
        Map<ColumnHandle, Domain> all = constraint.getDomains().orElseThrow();
        Map<ColumnHandle, Domain> pushable = new LinkedHashMap<>();
        Map<ColumnHandle, Domain> remaining = new LinkedHashMap<>();
        for (Map.Entry<ColumnHandle, Domain> entry : all.entrySet()) {
            FlightSqlColumnHandle column = (FlightSqlColumnHandle) entry.getKey();
            if (isSupported(column.trinoType()) && encodeDomain(column, entry.getValue()).isPresent()) {
                pushable.put(entry.getKey(), entry.getValue());
            }
            else {
                remaining.put(entry.getKey(), entry.getValue());
            }
        }
        return new SplitConstraint(TupleDomain.withColumnDomains(pushable), TupleDomain.withColumnDomains(remaining));
    }

    public boolean isSupported(Type type)
    {
        return type instanceof BooleanType
                || type instanceof TinyintType
                || type instanceof SmallintType
                || type instanceof IntegerType
                || type instanceof BigintType
                || type instanceof RealType
                || type instanceof DoubleType
                || type instanceof DecimalType
                || type instanceof DateType
                || type instanceof VarcharType;
    }

    private Optional<String> encodeDomain(FlightSqlColumnHandle column, Domain domain)
    {
        Type type = column.trinoType();
        if (!isSupported(type)) {
            return Optional.empty();
        }
        if (domain.isAll()) {
            return Optional.empty();
        }
        if (domain.isNone()) {
            return Optional.of("1 = 0");
        }
        String quotedColumn = quoteIdentifier(column.columnName());
        boolean nullAllowed = domain.isNullAllowed();
        boolean onlyNull = domain.isOnlyNull();
        if (onlyNull) {
            return Optional.of(quotedColumn + " IS NULL");
        }
        if (domain.getValues().isAll()) {
            return Optional.of(nullAllowed ? "" : quotedColumn + " IS NOT NULL");
        }
        List<Range> ranges = domain.getValues().getRanges().getOrderedRanges();
        List<String> rangeExprs = ImmutableList.copyOf(
                ranges.stream()
                        .map(r -> encodeRange(quotedColumn, type, r))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toList()));
        if (rangeExprs.size() != ranges.size()) {
            return Optional.empty();
        }
        String combined = rangeExprs.size() == 1 ? rangeExprs.get(0) : "(" + String.join(" OR ", rangeExprs) + ")";
        if (nullAllowed) {
            return Optional.of("(" + combined + " OR " + quotedColumn + " IS NULL)");
        }
        return Optional.of(combined);
    }

    private Optional<String> encodeRange(String quotedColumn, Type type, Range range)
    {
        if (range.isSingleValue()) {
            Optional<String> literal = formatLiteral(type, range.getSingleValue());
            return literal.map(value -> quotedColumn + " = " + value);
        }
        List<String> parts = new ArrayList<>(2);
        if (!range.isLowUnbounded()) {
            Optional<String> low = formatLiteral(type, range.getLowBoundedValue());
            if (low.isEmpty()) {
                return Optional.empty();
            }
            parts.add(quotedColumn + (range.isLowInclusive() ? " >= " : " > ") + low.get());
        }
        if (!range.isHighUnbounded()) {
            Optional<String> high = formatLiteral(type, range.getHighBoundedValue());
            if (high.isEmpty()) {
                return Optional.empty();
            }
            parts.add(quotedColumn + (range.isHighInclusive() ? " <= " : " < ") + high.get());
        }
        if (parts.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of("(" + String.join(" AND ", parts) + ")");
    }

    /**
     * Default DuckDB dialect literal formatting. Override in subclasses for
     * dialect-specific quirks (date/timestamp literal forms, etc.).
     */
    protected Optional<String> formatLiteral(Type type, Object value)
    {
        if (value == null) {
            return Optional.empty();
        }
        if (type instanceof BooleanType) {
            return Optional.of(((Boolean) value) ? "TRUE" : "FALSE");
        }
        if (type instanceof TinyintType || type instanceof SmallintType || type instanceof IntegerType || type instanceof BigintType) {
            return Optional.of(Long.toString((long) value));
        }
        if (type instanceof RealType) {
            float v = Float.intBitsToFloat(Math.toIntExact((long) value));
            return Optional.of(Float.toString(v));
        }
        if (type instanceof DoubleType) {
            return Optional.of(Double.toString((double) value));
        }
        if (type instanceof DecimalType decimal) {
            return Optional.of(formatDecimal(decimal, value));
        }
        if (type instanceof VarcharType) {
            String s = ((Slice) value).toStringUtf8();
            return Optional.of(quoteStringLiteral(s));
        }
        if (type instanceof DateType) {
            long days = (long) value;
            LocalDate date = LocalDate.ofEpochDay(days);
            return Optional.of("DATE '" + date + "'");
        }
        return Optional.empty();
    }

    private static String formatDecimal(DecimalType type, Object value)
    {
        BigInteger unscaled;
        if (type.isShort()) {
            unscaled = BigInteger.valueOf((long) value);
        }
        else {
            unscaled = ((Int128) value).toBigInteger();
        }
        return new java.math.BigDecimal(unscaled, type.getScale()).toPlainString();
    }

    /**
     * MVP keeps identifiers unquoted (lowercase) so that case-insensitive backends
     * (Derby normalises unquoted to upper case; DuckDB / PostgreSQL store lower case)
     * match. Identifiers containing special characters are out of scope for MVP.
     */
    protected String quoteIdentifier(String identifier)
    {
        return identifier;
    }

    protected String quoteStringLiteral(String value)
    {
        return "'" + value.replace("'", "''") + "'";
    }

    public record SplitConstraint(TupleDomain<ColumnHandle> pushable, TupleDomain<ColumnHandle> remaining) {}
}
