# trino-flight-sql-connector

A Trino 476 connector that talks to any Flight SQL Server (DuckDB / SQLite /
Dremio / Spice / your own producer) through the Apache Arrow ADBC Flight SQL
driver.

It is implemented as a native connector that carries Arrow end to end, and it
maps the Flight SQL partition concept directly onto Trino splits, so reads fan
out across exactly as many endpoints as the server advertises.

> **Status**: MVP (Domain pushdown + projection pushdown + parallel endpoint
> reads). Intended for experimentation, not production use.

日本語版の README は [README-ja.md](README-ja.md) にあります。

## Requirements

| Item | Version |
|---|---|
| Trino | 476 |
| JDK | 24 (required by Trino 476) |
| Arrow Java | 19.0.0 |
| ADBC | 0.23.0 |
| Gradle | 9.x (wrapper included) |

## Architecture

```
Trino Coordinator                                Flight SQL Server
┌─────────────────────────────┐    gRPC      ┌──────────────────┐
│ FlightSqlMetadata           │  (ADBC →     │  CommandGet*     │
│   listSchemaNames / Tables  │   Flight SQL)│  (FlightInfo)    │
│   getColumnHandles  ────────┼─────────────►│  CommandStatement│
│   applyFilter / Projection  │              │     Query        │
│ FlightSqlSplitManager       │              └──────────────────┘
│   executePartitioned ───────┼──── returns N endpoint tickets
│   → FlightSqlSplit × N      │
└──────────┬──────────────────┘
           │ split per worker
           ▼
Trino Worker
┌─────────────────────────────┐
│ FlightSqlPageSource         │   ADBC readPartition(ticket)
│   ArrowReader.loadNextBatch ├──────────────────────────────►  Flight DoGet
│   → VectorSchemaRoot        │  ◄─ Arrow IPC stream
│   → ArrowToTrinoPageBuilder │
│   → Trino Page              │
└─────────────────────────────┘
```

- **Metadata**: schema and table listings are obtained with `executeQuery`
  against dialect-specific catalogs (`information_schema`, `SYS.SYSSCHEMAS`,
  …) selected by `flight.dialect=duckdb|derby`. Column schemas are taken from
  the Arrow Schema returned by `executePartitioned` on
  `SELECT * FROM s.t WHERE 1=0`, because ADBC's `getTableSchema` reports
  `NOT_IMPLEMENTED` on the Flight SQL implementations tested.
- **Pushdown**: `FlightSqlQueryBuilder` turns a `TupleDomain` into a `WHERE`
  clause (domain pushdown) and `projectedColumns` into a `SELECT` list
  (projection pushdown). Unsupported domains are handed back to Trino as
  `remainingFilter`.
- **Split**: each `PartitionDescriptor` (the ADBC equivalent of a Flight SQL
  `FlightEndpoint`) returned by `AdbcStatement.executePartitioned()` becomes one
  Trino split. A worker fetches the descriptor it was assigned via
  `AdbcConnection.readPartition(ByteBuffer)`.
- **Arrow → Trino Page**: `ArrowToTrinoPageBuilder` converts `FieldVector` to
  `Block` with a dedicated loop per type — no JDBC `ResultSet` in the path.

## Building

JDK 24 is required.

```bash
./gradlew test                # unit tests + Derby/FlightSqlExample smoke test
./gradlew trinoPluginDistZip  # build/distributions/trino-flight-sql-<gitVersion>.zip
```

The version comes from git tags via the `com.palantir.git-version` plugin
(equivalent to `git describe --tags --always --first-parent`). A commit without
a tag builds as a short SHA; a commit carrying an annotated tag such as
`v1.2.3` builds under that tag name. Builds made with uncommitted changes in
the working tree get a `.dirty` suffix.

Unpacking the generated zip into `$TRINO_HOME/` lays the required JARs down
under `$TRINO_HOME/plugin/flight-sql/`. `trino-spi` is deliberately excluded
from the zip and left to the Trino classpath, to preserve plugin classloader
isolation.

## Required JVM settings on the Trino server

When Arrow puts a gRPC response into a `DirectByteBuffer`, it needs either
`sun.misc.Unsafe`-based memory access or reflective access to the private
`DirectByteBuffer(long, int)` constructor. As of JDK 24 both are blocked by
default, so without extra configuration the very first metadata query fails
like this:

```
FlightRuntimeException: Failed to read message.
  Caused by: UnsupportedOperationException:
    sun.misc.Unsafe or java.nio.DirectByteBuffer.<init>(long, int) not available
      at org.apache.arrow.memory.util.MemoryUtil.directBuffer(...)
      at org.apache.arrow.flight.ArrowMessage.frame(...)
```

**Add the following to `$TRINO_HOME/etc/jvm.config` on every node of the Trino
cluster (coordinator and workers) and restart:**

```
--sun-misc-unsafe-memory-access=allow
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
```

These options are required by the Arrow / Netty stack bundled with this
connector; `tasks.test` in this repository passes the same set through
`jvmArgs`.

## Catalog configuration

Example `$TRINO_HOME/etc/catalog/flight.properties`:

```properties
connector.name=flight_sql

# Flight SQL Server to connect to
flight.uri=grpc://localhost:32010
# flight.uri=grpc+tls://flight.example.com:32010   # over TLS

# SQL dialect used for metadata queries (currently duckdb / derby)
flight.dialect=duckdb

# Optional: TLS settings
# flight.tls.skip-verify=false
# flight.tls.trust-store-path=/etc/ssl/flight-ca.pem

# Optional: authentication
# flight.username=admin
# flight.password=secret
# flight.authorization-header=Bearer eyJhbGc...   # meant to be exclusive with user/pass above

# Optional: extra gRPC headers (comma separated, key:value)
# flight.rpc-headers=x-tenant:foo,x-debug:1

# Optional: database header honored by some servers
# flight.default-database=mydb
```

### Configuration properties

| Key | Type | Default | Description |
|---|---|---|---|
| `flight.uri` | string | (required) | `grpc://host:port` or `grpc+tls://host:port` |
| `flight.dialect` | enum | `DUCKDB` | SQL dialect for metadata queries: `DUCKDB` / `DERBY` |
| `flight.use-encryption` | boolean | (derived from URI) | Explicitly enable/disable TLS |
| `flight.tls.skip-verify` | boolean | `false` | Skip TLS certificate verification (dev/test) |
| `flight.tls.trust-store-path` | string | null | CA bundle in PEM format |
| `flight.username` | string | null | Basic auth user |
| `flight.password` | string | null | Basic auth password (secret) |
| `flight.authorization-header` | string | null | Raw `Authorization` header value (secret) |
| `flight.rpc-headers` | string | `""` | Additional gRPC headers in `k:v,k:v` form |
| `flight.default-database` | string | null | gRPC `database` header |

## Trying it out

With a DuckDB Flight SQL Server running separately:

```sql
SHOW SCHEMAS FROM flight;
SHOW TABLES FROM flight.main;
SELECT col1, col2 FROM flight.main.some_table WHERE col1 > 10;

-- confirm that pushdown is applied
EXPLAIN SELECT col1 FROM flight.main.some_table WHERE col1 > 10;

-- confirm that split count matches partition count
EXPLAIN (TYPE DISTRIBUTED) SELECT * FROM flight.main.big_table;
```

The in-repo tests start Apache Arrow Java's `FlightSqlExample` (backed by
Derby) in `@BeforeAll`, set `flight.dialect=derby`, and verify:

- `SHOW SCHEMAS FROM flight` → contains `app`
- `SHOW TABLES FROM flight.app` → contains `inttable`
- `SELECT id, value FROM flight.app.inttable` → at least one row (projection pushdown)
- `SELECT value FROM flight.app.inttable WHERE id = 1` (domain pushdown)

## Supported types (MVP)

| Arrow Type | Trino Type |
|---|---|
| `Bool` | `BOOLEAN` |
| `Int(8/16/32/64, signed)` | `TINYINT` / `SMALLINT` / `INTEGER` / `BIGINT` |
| `FloatingPoint(SINGLE / DOUBLE)` | `REAL` / `DOUBLE` |
| `Decimal(p, s)` (p ≤ 38) | `DECIMAL(p, s)` |
| `Utf8` / `LargeUtf8` | `VARCHAR` |
| `Binary` / `LargeBinary` / `FixedSizeBinary` | `VARBINARY` |
| `Date(DAY)` / `Date(MILLI)` | `DATE` |
| `Time(SEC/MILLI/MICRO/NANO)` | `TIME(p)` |
| `Timestamp(unit, tz=null)` | `TIMESTAMP(p)` |
| `Timestamp(unit, tz!=null)` | `TIMESTAMP(p) WITH TIME ZONE` |

Unsupported Arrow types (List / Struct / Map / Union / unsigned integers) are
skipped in `getColumnHandles`.

## Limitations / known issues

- **Identifiers are all lowercased**: to match Trino's identifier case folding,
  schema / table / column names are lowercased. SQL is emitted unquoted, which
  works both for Derby (whose parser upcases) and for DuckDB / PostgreSQL
  (which match lowercase). **Mixed-case identifiers (e.g. `MyTable`) are not
  supported in the MVP.**
- **Metadata relies on dialect-specific SQL**: ADBC's `getObjects` /
  `getTableSchema` return empty or unimplemented results on at least
  `FlightSqlExample` and some other implementations, so SQL is used instead.
  Dialects other than `duckdb` / `derby` are currently unsupported.
- **Predicate pushdown** covers only `boolean`, `tinyint/smallint/integer/bigint`,
  `real/double`, `decimal`, `varchar` and `date`. `LIKE`, function calls and
  complex expressions are out of MVP scope.
- **Read-only**: INSERT / UPDATE / DELETE and ADBC `bulkIngest` are not
  implemented.
- **TestingFlightSqlServer swallows allocator leak warnings during teardown**:
  Apache Arrow 19.0.0's own `FlightSqlExample` occasionally leaks, which is not
  a problem of this connector.
- **An occasional `Memory was leaked by query. Memory leaked: (128)` WARN**:
  ADBC's `FlightSqlConnection` keeps a per-Location `FlightSqlClient` in a
  Caffeine cache (`expireAfterAccess=5min`), and when an entry is evicted — by
  `AdbcConnection.close()` or by the TTL — the inner `FlightClient` (Arrow
  Flight 18.x) detects a small leak (tens to hundreds of bytes). Caffeine's
  removal listener logs the WARN on a ForkJoinPool thread, but it is **log
  noise that does not affect query results**. It comes from Arrow Flight, so
  the connector has no way to suppress it; if the message is a nuisance,
  silence WARN for `com.github.benmanes.caffeine.cache.BoundedLocalCache` in
  logback/log4j.
- **JDK 24 reflective access**: needed along the ADBC + Arrow + Netty path, so
  `tasks.test` in `build.gradle.kts` passes `--add-opens` and
  `--sun-misc-unsafe-memory-access=allow` explicitly. **The same options must
  be added to the production Trino `etc/jvm.config`** (see "Required JVM
  settings on the Trino server" above). Without them the first Flight RPC fails
  with `UnsupportedOperationException: sun.misc.Unsafe or
  java.nio.DirectByteBuffer.<init>(long, int) not available` as the cause of
  `FlightRuntimeException: Failed to read message`. A `Memory was leaked by
  query` suppressed exception shows up in the same situation — it comes from
  the child allocator left mid-flight when `ArrowMessage` fails, and disappears
  once the Unsafe problem is fixed.

## Layout

```
src/main/java/io/repro/trino/plugin/flightsql/
├── FlightSqlPlugin.java                    # SPI entry point
├── FlightSqlConnectorFactory.java          # Bootstrap + Guice
├── FlightSqlConnectorModule.java
├── FlightSqlConnector.java                 # hosts ConnectorMetadata / SplitManager / PageSourceProvider
├── FlightSqlConfig.java                    # Airlift @Config
├── FlightSqlMetadata.java                  # listSchemaNames / getTableHandle / applyFilter / applyProjection
├── FlightSqlSplitManager.java              # executePartitioned → splits
├── FlightSqlSplit.java                     # ConnectorSplit (base64 partition descriptor or fallback SQL)
├── FlightSqlPageSourceProvider.java
├── FlightSqlPageSource.java                # ArrowReader → Trino Page
├── FlightSqlTableHandle.java               # SchemaTableName + TupleDomain + projectedColumns
├── FlightSqlColumnHandle.java
├── FlightSqlTransactionHandle.java         # autocommit only (enum singleton)
├── client/
│   ├── FlightSqlClient.java                # ADBC wrapper (executePartitioned / readPartition / per-dialect metadata SQL)
│   └── PartitionReader.java                # manages AdbcConnection + ArrowReader as a unit
├── query/
│   └── FlightSqlQueryBuilder.java          # TupleDomain → WHERE, projection → SELECT
└── arrow/
    ├── ArrowTypeMapper.java                # Arrow Field → Trino Type
    └── ArrowToTrinoPageBuilder.java        # VectorSchemaRoot → Page (dedicated loop per type)

src/test/java/io/repro/trino/plugin/flightsql/
├── TestingFlightSqlServer.java             # starts FlightSqlExample (Derby) in BeforeAll
├── FlightSqlQueryRunner.java               # installs this plugin into a DistributedQueryRunner
├── TestFlightSqlConnectorSmokeTest.java    # 4 cases (SHOW SCHEMAS/TABLES, SELECT, predicate pushdown)
├── TestFlightSqlConfig.java                # Airlift ConfigAssertions
└── TestFlightSqlPlugin.java                # verifies the connector factory name
```

## License

[Apache License 2.0](LICENSE) — Copyright 2026 Repro Inc.
