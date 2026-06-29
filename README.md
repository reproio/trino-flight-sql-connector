# trino-flight-sql-connector

Trino 476 から Apache Arrow ADBC の Flight SQL ドライバを経由して、
任意の Flight SQL Server (DuckDB / SQLite / Dremio / Spice / 自作 producer 等)
にアクセスするためのコネクタ。

Arrow を end-to-end で運ぶ native connector として実装されており、
Flight SQL の partition 概念をそのまま Trino split として扱うことで、
サーバが申告した endpoint 数だけ並列に読み出す。

> **状態**: MVP (Domain pushdown + projection pushdown + endpoint 並列読み)。
> プロダクション利用ではなく、検証用途。

## 動作環境

| 項目 | バージョン |
|---|---|
| Trino | 476 |
| JDK | 24 (Trino 476 の要件) |
| Arrow Java | 19.0.0 |
| ADBC | 0.23.0 |
| Gradle | 9.x (wrapper 同梱) |

## アーキテクチャ概要

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

- **Metadata**: 方言 (`flight.dialect=duckdb|derby`) ごとに `information_schema` /
  `SYS.SYSSCHEMAS` 等を `executeQuery` で叩いてスキーマ/テーブル一覧を取る。
  列スキーマは `SELECT * FROM s.t WHERE 1=0` の `executePartitioned` 結果の
  Arrow Schema を流用 (ADBC の `getTableSchema` は Flight SQL 実装が
  `NOT_IMPLEMENTED` のため使えない)。
- **Pushdown**: `FlightSqlQueryBuilder` が `TupleDomain` → `WHERE` 句を構築
  (Domain pushdown)、`projectedColumns` から `SELECT` リストを構築
  (projection pushdown)。サポート外の domain は `remainingFilter` として
  Trino 側に戻す。
- **Split**: ADBC `AdbcStatement.executePartitioned()` が返す
  `PartitionDescriptor` (= Flight SQL の `FlightEndpoint` 相当) 1 つを
  1 Trino split に対応付ける。worker は自分が割り当てられた descriptor を
  `AdbcConnection.readPartition(ByteBuffer)` で fetch する。
- **Arrow → Trino Page**: `ArrowToTrinoPageBuilder` が型ごとに専用ループで
  `FieldVector` → `Block` 変換 (JDBC `ResultSet` を経由しない)。

## ビルド

JDK 24 が必要。

```bash
./gradlew test                # 単体テスト + Derby/FlightSqlExample スモーク
./gradlew trinoPluginDistZip  # build/distributions/trino-flight-sql-<gitVersion>.zip
```

バージョンは `com.palantir.git-version` プラグインで git tag から取得する
(`git describe --tags --always --first-parent` 相当)。タグが無いコミットの
ビルドでは短い SHA、`v1.2.3` のような annotated tag を打ったコミットの
ビルドではそのタグ名がそのままバージョンになる。working tree に
uncommit な変更があるビルドは `<version>.dirty` というサフィックスが付く。

生成された zip を Trino サーバの `$TRINO_HOME/` に展開すれば
`$TRINO_HOME/plugin/flight-sql/` に必要 JAR が並ぶ。`trino-spi` は zip から
意図的に除外しており、Trino 本体側 classpath に任せる (plugin classloader
分離の維持のため)。

## Trino サーバ側の必須 JVM 設定

Arrow が gRPC レスポンスを `DirectByteBuffer` に詰める際、JDK 17+ では
`sun.misc.Unsafe` ベースのメモリアクセスか、`DirectByteBuffer(long, int)`
の非公開コンストラクタへのリフレクションが必要になる。JDK 24 以降は
両方がデフォルトでブロックされており、何も設定しないと以下のような
エラーで初回のメタデータクエリが失敗する:

```
FlightRuntimeException: Failed to read message.
  Caused by: UnsupportedOperationException:
    sun.misc.Unsafe or java.nio.DirectByteBuffer.<init>(long, int) not available
      at org.apache.arrow.memory.util.MemoryUtil.directBuffer(...)
      at org.apache.arrow.flight.ArrowMessage.frame(...)
```

**Trino クラスタの全ノード (coordinator + worker) の `$TRINO_HOME/etc/jvm.config`
に以下を追記して再起動する必要がある:**

```
--sun-misc-unsafe-memory-access=allow
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
```

このオプションは本コネクタが同梱する Arrow / Netty が要求するもので、
本リポジトリの `tasks.test` でも同じものを `jvmArgs` で渡している。

## カタログ設定

`$TRINO_HOME/etc/catalog/flight.properties` の例:

```properties
connector.name=flight_sql

# 接続先 Flight SQL Server
flight.uri=grpc://localhost:32010
# flight.uri=grpc+tls://flight.example.com:32010   # TLS 経由

# メタデータ取得用 SQL の方言 (現在は duckdb / derby)
flight.dialect=duckdb

# 任意: TLS 設定
# flight.tls.skip-verify=false
# flight.tls.trust-store-path=/etc/ssl/flight-ca.pem

# 任意: 認証
# flight.username=admin
# flight.password=secret
# flight.authorization-header=Bearer eyJhbGc...   # 上の user/pass と排他で使う想定

# 任意: gRPC ヘッダ追加 (comma 区切り、key:value)
# flight.rpc-headers=x-tenant:foo,x-debug:1

# 任意: 一部サーバが見る database ヘッダ
# flight.default-database=mydb
```

### 設定キー一覧

| Key | 型 | デフォルト | 説明 |
|---|---|---|---|
| `flight.uri` | string | (必須) | `grpc://host:port` か `grpc+tls://host:port` |
| `flight.dialect` | enum | `DUCKDB` | メタデータ取得 SQL の方言。`DUCKDB` / `DERBY` |
| `flight.use-encryption` | boolean | (URI 由来) | 明示的に TLS を有効/無効化 |
| `flight.tls.skip-verify` | boolean | `false` | TLS 証明書検証スキップ (dev/test) |
| `flight.tls.trust-store-path` | string | null | PEM 形式 CA バンドル |
| `flight.username` | string | null | basic 認証ユーザ |
| `flight.password` | string | null | basic 認証パスワード (秘匿) |
| `flight.authorization-header` | string | null | 任意の `Authorization` ヘッダ値 (秘匿) |
| `flight.rpc-headers` | string | `""` | 追加 gRPC ヘッダ、`k:v,k:v` 形式 |
| `flight.default-database` | string | null | gRPC `database` ヘッダ |

## 動作確認

DuckDB Flight SQL Server を別途用意した上で:

```sql
SHOW SCHEMAS FROM flight;
SHOW TABLES FROM flight.main;
SELECT col1, col2 FROM flight.main.some_table WHERE col1 > 10;

-- pushdown が効いていることを確認
EXPLAIN SELECT col1 FROM flight.main.some_table WHERE col1 > 10;

-- partition 数 = split 数を確認
EXPLAIN (TYPE DISTRIBUTED) SELECT * FROM flight.main.big_table;
```

リポジトリ内蔵のテストは Apache Arrow Java の `FlightSqlExample`
(Derby バック) を `@BeforeAll` で立ち上げ、`flight.dialect=derby` を
設定して以下のクエリを検証する:

- `SHOW SCHEMAS FROM flight` → `app` を含む
- `SHOW TABLES FROM flight.app` → `inttable` を含む
- `SELECT id, value FROM flight.app.inttable` → 1 行以上 (projection pushdown)
- `SELECT value FROM flight.app.inttable WHERE id = 1` (Domain pushdown)

## サポートしている型 (MVP)

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

未対応の Arrow 型 (List / Struct / Map / Union / 符号なし整数) は
`getColumnHandles` でスキップされる。

## 制約 / 既知の問題

- **識別子は全部 lowercase**: Trino の識別子 case-folding に合わせて、
  schema / table / column を lower-case 化して扱う。SQL は無クオートで
  発行するため、Derby は parser が大文字化、DuckDB / PostgreSQL は
  lowercase 一致でいずれも動く。**mixed-case の識別子 (例: `MyTable`) は
  MVP では非サポート**。
- **メタデータは方言依存の SQL**: ADBC の `getObjects` / `getTableSchema` が
  少なくとも `FlightSqlExample` / 一部の実装で空・未実装のため、SQL で
  代替している。`flight.dialect` が `duckdb` / `derby` 以外には現状非対応。
- **predicate pushdown**: `boolean`, `tinyint/smallint/integer/bigint`,
  `real/double`, `decimal`, `varchar`, `date` のみ。`LIKE` / 関数呼び出し /
  複雑式は MVP 範囲外 (Phase 3 で実装予定)。
- **read-only**: INSERT / UPDATE / DELETE / ADBC `bulkIngest` は未実装。
- **TestingFlightSqlServer は teardown でアロケータリーク警告を握りつぶす**:
  Apache Arrow 19.0.0 の `FlightSqlExample` 自体が稀にリークを残すが
  本コネクタの問題ではないため。
- **`Memory was leaked by query. Memory leaked: (128)` WARN が時々出る**:
  ADBC `FlightSqlConnection` が per-Location で `FlightSqlClient` を
  Caffeine キャッシュ (`expireAfterAccess=5min`) しており、`AdbcConnection.close()`
  か TTL 切れで evict されたタイミングで内部の `FlightClient` (Arrow Flight 18.x)
  が小さなリーク (数十〜数百バイト) を検出する。Caffeine の removal listener が
  ForkJoinPool 上で WARN を出すが、これは **クエリ実行結果には影響しない** ログノイズ。
  Arrow Flight 側の挙動なのでコネクタ側で抑制する手段はない。エラーが本当に
  気になる場合は logback/log4j で
  `com.github.benmanes.caffeine.cache.BoundedLocalCache` の WARN を抑制する。
- **JDK 24 reflective access**: ADBC + Arrow + Netty 経路で必要なため
  `build.gradle.kts` の `tasks.test` で `--add-opens` / `--sun-misc-unsafe-memory-access=allow`
  を明示している。プロダクションの Trino 側 `etc/jvm.config` にも
  **同じものを追加する必要がある** (詳細は上の
  「Trino サーバ側の必須 JVM 設定」セクション)。これらが無いと初回の
  Flight RPC で `UnsupportedOperationException: sun.misc.Unsafe or
  java.nio.DirectByteBuffer.<init>(long, int) not available` が
  `FlightRuntimeException: Failed to read message` の cause として
  発生する。なお同じ状況で suppressed 例外として `Memory was leaked
  by query` が出るが、これは ArrowMessage 失敗時の child allocator が
  中間状態で残るためで、Unsafe 問題が解決すれば一緒に消える。

## ロードマップ

| Phase | 内容 |
|---|---|
| 1 (現在) | Domain pushdown + projection pushdown + endpoint 並列読み |
| 2 | LIMIT pushdown、メタデータキャッシュの TTL 化、testcontainers 化 |
| 3 | 複雑式 pushdown (`ConnectorExpressionRule` 群、関数マッピング) |
| 4 | Aggregation pushdown |
| 5 | TopN / Join pushdown |
| 6 | 多方言対応のリファクタリング (`FlightSqlQueryBuilder` を抽象 + 方言別サブクラスに分割、`flight.dialect=generic` で ADBC `getObjects` を直接使うパス) |

## ディレクトリ構成

```
src/main/java/io/repro/trino/plugin/flightsql/
├── FlightSqlPlugin.java                    # SPI エントリ
├── FlightSqlConnectorFactory.java          # Bootstrap + Guice
├── FlightSqlConnectorModule.java
├── FlightSqlConnector.java                 # ConnectorMetadata / SplitManager / PageSourceProvider のホスト
├── FlightSqlConfig.java                    # Airlift @Config
├── FlightSqlMetadata.java                  # listSchemaNames / getTableHandle / applyFilter / applyProjection
├── FlightSqlSplitManager.java              # executePartitioned → splits
├── FlightSqlSplit.java                     # ConnectorSplit (base64 partition descriptor or fallback SQL)
├── FlightSqlPageSourceProvider.java
├── FlightSqlPageSource.java                # ArrowReader → Trino Page
├── FlightSqlTableHandle.java               # SchemaTableName + TupleDomain + projectedColumns
├── FlightSqlColumnHandle.java
├── FlightSqlTransactionHandle.java         # autocommit のみ (enum singleton)
├── client/
│   ├── FlightSqlClient.java                # ADBC ラッパ (executePartitioned / readPartition / 方言別メタデータ SQL)
│   └── PartitionReader.java                # AdbcConnection + ArrowReader 一体管理
├── query/
│   └── FlightSqlQueryBuilder.java          # TupleDomain → WHERE、projection → SELECT
└── arrow/
    ├── ArrowTypeMapper.java                # Arrow Field → Trino Type
    └── ArrowToTrinoPageBuilder.java        # VectorSchemaRoot → Page (型ごとの専用ループ)

src/test/java/io/repro/trino/plugin/flightsql/
├── TestingFlightSqlServer.java             # FlightSqlExample (Derby) を BeforeAll で起動
├── FlightSqlQueryRunner.java               # DistributedQueryRunner に本プラグインを install
├── TestFlightSqlConnectorSmokeTest.java    # 4 ケース (SHOW SCHEMAS/TABLES, SELECT, predicate pushdown)
├── TestFlightSqlConfig.java                # Airlift ConfigAssertions
└── TestFlightSqlPlugin.java                # connector factory 名検証
```

## ライセンス

(未設定)
