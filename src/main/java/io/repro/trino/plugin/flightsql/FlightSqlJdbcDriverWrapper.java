package io.repro.trino.plugin.flightsql;

import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Trino の {@code BaseJdbcConfig.connectionUrl} は {@code ^jdbc:[a-z0-9]+:}
 * という正規表現でチェックされるため、本物の Flight SQL JDBC subprotocol
 * {@code jdbc:arrow-flight-sql://} (ハイフン含む) は通らない。本ラッパーで
 * ハイフンなしの代替 subprotocol {@code jdbc:flightsql://} を受け付けて、
 * 内部で本物の {@link ArrowFlightJdbcDriver} に書き換えて委譲する。
 *
 * <p>ユーザは catalog properties に {@code connection-url=jdbc:flightsql://host:port/}
 * を書く。
 */
public class FlightSqlJdbcDriverWrapper
        implements Driver
{
    static final String WRAPPER_PREFIX = "jdbc:flightsql:";
    static final String REAL_PREFIX = "jdbc:arrow-flight-sql:";

    private final ArrowFlightJdbcDriver delegate = new ArrowFlightJdbcDriver();

    @Override
    public Connection connect(String url, Properties info)
            throws SQLException
    {
        if (!acceptsURL(url)) {
            return null;
        }
        return delegate.connect(rewrite(url), info);
    }

    @Override
    public boolean acceptsURL(String url)
    {
        return url != null && url.startsWith(WRAPPER_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
            throws SQLException
    {
        return delegate.getPropertyInfo(rewrite(url), info);
    }

    @Override
    public int getMajorVersion()
    {
        return delegate.getMajorVersion();
    }

    @Override
    public int getMinorVersion()
    {
        return delegate.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant()
    {
        return delegate.jdbcCompliant();
    }

    @Override
    public Logger getParentLogger()
            throws SQLFeatureNotSupportedException
    {
        return delegate.getParentLogger();
    }

    private static String rewrite(String url)
    {
        if (url == null || !url.startsWith(WRAPPER_PREFIX)) {
            return url;
        }
        return REAL_PREFIX + url.substring(WRAPPER_PREFIX.length());
    }
}
