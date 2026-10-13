# StarRocks CDC Source Connector How-To Guide

## What It Does

The StarRocks CDC source connector is a Kafka Connect **source** connector — the reverse direction of the existing StarRocks sink connector in this repository. It streams change data capture (CDC) events out of StarRocks **shared-data (cloud-native)** tables into Kafka topics, one topic per captured table.

Internally it uses JDBC for StarRocks' bookmark and metadata operations, then reads the point-in-time snapshot (`[_BOOKMARK_<id>_]`) and incremental CHANGES window (`[_CHANGES_<base>_<head>_]`) through the configured JDBC or Arrow ADBC transport. Every record is emitted as a Debezium envelope (`before`/`after`/`source`/`op`/`ts_ms`/`transaction`, built by Debezium's own `io.debezium.data.Envelope`) with a schema-carrying Kafka Connect `Struct` key and value. Delivery is **at-least-once**.

---

## Prerequisites

- **A StarRocks shared-data (cloud-native) cluster.** The `[_BOOKMARK_]`/`[_CHANGES_]` query hints and the bookmark meta functions this connector depends on are not available on shared-nothing clusters.
- **`enable_bookmark_meta_functions` must be turned on** (it defaults to `false`). Enable it on each FE:
  ```sql
  ADMIN SET FRONTEND CONFIG ("enable_bookmark_meta_functions" = "true");
  ```
  Add the same line to `fe.conf` too, so it survives an FE restart.
- **The connecting user needs the system-level `OPERATE` privilege.** `bookmark_create`/`bookmark_renew`/`bookmark_release` are gated by this privilege and only execute on the current FE leader:
  ```sql
  GRANT OPERATE ON SYSTEM TO USER 'cdc_user'@'%';
  ```
  List every FE endpoint in `starrocks.jdbc.url`. The connector rotates when a bookmark call reports "must run on the FE leader" or a connection fails. Initial metadata and snapshot connections also try the next endpoint when an FE is unreachable; an interrupted read is retried through the task's normal recovery path. JDBC snapshot retries preserve the endpoint rotation and reopen the original bookmark on the next FE after a connection failure. Flight JDBC's unchecked driver errors are converted to SQL exceptions for the same recovery path.
- **PRIMARY KEY tables must have `enable_change_data_capture` set to `true`** — at `CREATE TABLE` time, or via `ALTER TABLE` beforehand:
  ```sql
  ALTER TABLE db1.orders SET ("enable_change_data_capture" = "true");
  ```
  This property only applies to shared-data PRIMARY KEY tables. Turn it on *before* the version range you need the connector to cover: rows written while it was off cannot be replayed, and any CHANGES window that straddles an "off" period fails as non-trackable (`CDC-ERROR-1 (CHANGE_NOT_TRACKABLE)`).
- **DUPLICATE KEY and AGGREGATE KEY tables work with no table property changes**, but only for insert-only workloads. A `DELETE`/`TRUNCATE` against one of these tables (or a dropped/rewritten partition, a schema-rewriting `ALTER`, or a bucket/reshard change on any table) makes any CHANGES window spanning it non-trackable — see [Limitations](#limitations).
- **UNIQUE KEY tables are not supported.** `CHANGES` cannot read them, and `StarRocksCdcSourceConnector` rejects the configuration outright at startup rather than letting a task fail later. The same applies to a table whose `TABLE_MODEL` the connector does not recognise (anything but `PRIMARY_KEYS`, `DUP_KEYS`, `AGG_KEYS`, `UNIQUE_KEYS`): it is refused by name rather than captured on a guess.
- **No FE version requirement beyond the above.** The connector only relies on already-shipped capabilities (`bookmark_create`/`bookmark_release`, the `[_CHANGES_]`/`[_BOOKMARK_]` query hints); there is no minimum-version check in the code. `bookmark_renew` is an optional enhancement: without it every renewal fails and is logged, and idle tables fall back to the no-renewal semantics (Limitation 1).

---

## Full Configuration Example

`starrocks.jdbc.url` points at an FE query endpoint — different from the sink connector's `starrocks.http.url`, which points at the HTTP/stream-load port. Its URL scheme also selects the transport; see [Choosing a transport](#choosing-a-transport).

| Key | Type | Default | Description |
|---|---|---|---|
| `starrocks.jdbc.url` | String | *(required)* | JDBC URL of the StarRocks FE endpoint(s). Comma-separate multiple FE hosts (e.g. `jdbc:mysql://fe1:9030,fe2:9030`) so the client can rotate to the leader for bookmark calls. The scheme selects the transport: `jdbc:mysql://` for the MySQL protocol, `jdbc:arrow-flight-sql://` for Arrow Flight SQL — see [Choosing a transport](#choosing-a-transport). |
| `source.read.transport` | String | `jdbc` | `jdbc` reads through `starrocks.jdbc.url`; `arrow-adbc` uses Arrow ADBC for snapshot and CHANGES queries. Bookmarks and metadata still use JDBC. |
| `starrocks.adbc.uri` | String | `""` | Required when `source.read.transport=arrow-adbc`; comma-separated Flight endpoints, for example `grpc+tcp://fe1:9408,grpc+tcp://fe2:9408`. |
| `starrocks.database.name` | String | *(required)* | The name of the source StarRocks database. |
| `starrocks.username` | String | *(required)* | The username used to connect to StarRocks. Needs the `OPERATE` privilege. |
| `starrocks.password` | Password | *(required)* | The password used to connect to StarRocks. Stored as a Kafka Connect `Password`, so it is masked in logs and the connector status API. |
| `starrocks.table.names` | String | *(required)* | Comma-separated list of table names, within `starrocks.database.name`, to capture changes from. |
| `starrocks.table2topic.map` | String | `""` (empty) | Optional mapping from table name to Kafka topic name, formatted as `table:topic,table:topic`. Tables not listed here fall back to `source.topic.prefix`. |
| `source.topic.prefix` | String | `sr` | Prefix used to derive a topic name for tables without an explicit `starrocks.table2topic.map` entry: the topic is `<prefix>.<database>.<table>`. |
| `source.snapshot.mode` | String | `initial` | Whether to take an initial snapshot before streaming changes. One of `initial`, `no_snapshot`. **`no_snapshot` cannot be combined with `source.nontrackable.policy=resnapshot`** — that pairing is rejected at startup with a `ConfigException`, because `resnapshot` rebuilds a table's position from a fresh snapshot and would silently drop changes with snapshots disabled. |
| `source.no.snapshot.missing.position.policy` | String | `fail` | What `no_snapshot` does when a table has neither a durable offset nor a bookmark held by this connector: `fail` requires explicit initialization; `latest` creates a new bookmark at the current version. `latest` also applies after an old reference expires, so it can skip changes. |
| `source.poll.interval.ms` | Long | `5000` | Milliseconds to sleep between polls after a poll that produced no records. |
| `source.snapshot.batch.size` | Int | `4096` | Maximum snapshot records returned per table in one `poll()`. The snapshot cursor stays open across batches; each active table uses a separate read connection. Does not limit CHANGES windows. |
| `source.bookmark.ttl.ms` | Long | `604800000` (7 days) | Time-to-live, in milliseconds, for the bookmark backing each table's position. Renewed automatically while the task polls, paced off the lease the server actually grants; the TTL is the backstop for a connector that stops polling — see [Limitations](#limitations). A non-positive value drops only the per-reference limit: the cluster ceiling `bookmark_reference_max_ttl_ms` still expires the bookmark, so renewal keeps running. Even once the server reports no expiry at all, renewal re-probes every five minutes, since that ceiling is a mutable config an operator can set at any time. |
| `source.nontrackable.policy` | String | `fail` | Action to take when a table's CHANGES window becomes non-trackable. One of `fail`, `resnapshot`. **`resnapshot` requires `source.snapshot.mode=initial`** (the default): combined with `source.snapshot.mode=no_snapshot` it is rejected at startup with a `ConfigException`, since there would be no snapshot to rebuild the table's position from. |
| `source.tombstones.on.delete` | Boolean | `false` | When `true`, emits an extra tombstone record (null value, same key) immediately following each delete record. |
| `source.max.retries` | Int | `3` | Maximum attempts per FE URL for a bookmark call. With multiple endpoints, connection failures and leader redirects move immediately to the next URL; other SQL failures retry on the same URL. |
| `source.poll.retry.timeout.ms` | Long | `600000` | How long a table's reads may keep failing across polls before the task fails instead of retrying. Each failed poll waits `source.poll.interval.ms` before the next try. `-1` retries forever; `0` fails on the first failed poll. |
| `source.connect.timeout.ms` | Int | `1000` | Positive connection timeout in milliseconds for bookmark and metadata JDBC calls and for JDBC or Arrow ADBC read connections. The sink connector uses a separate `connect.timeoutms` key. |
| `source.read.timeout.ms` | Int | `120000` | Maximum wait in milliseconds for an Arrow ADBC query, the next Flight batch, or CHANGES reader cleanup. Each wait has its own timeout; a timeout discards the read connection and retries on the next configured Flight endpoint. |
| `source.read.timings.enabled` | Boolean | `false` | Collect query, read/decode, and task record mapping timings. With DEBUG logging enabled for `StarRocksJdbcClient` or `StarRocksAdbcClient`, and `StarRocksCdcSourceTask`, emits read and per-table summaries. Adds overhead; use temporarily for diagnostics. |

A complete `connect-standalone`-style connector properties file:

```properties
name=starrocks-cdc-orders
connector.class=com.starrocks.connector.kafka.source.StarRocksCdcSourceConnector
# Standard Kafka Connect property: shards starrocks.table.names round-robin
# across this many StarRocksCdcSourceTask instances.
tasks.max=1

# Connection (comma-separate every FE host for leader rotation)
starrocks.jdbc.url=jdbc:mysql://fe1:9030,fe2:9030
starrocks.database.name=db1
starrocks.username=cdc_user
starrocks.password=changeme

# Capture scope and topic mapping
starrocks.table.names=orders,users
starrocks.table2topic.map=orders:my_orders_topic
source.topic.prefix=sr

# Behavior
source.snapshot.mode=initial
source.no.snapshot.missing.position.policy=fail
source.poll.interval.ms=5000
source.snapshot.batch.size=4096
source.bookmark.ttl.ms=604800000
source.nontrackable.policy=fail
source.tombstones.on.delete=false
source.max.retries=3
source.poll.retry.timeout.ms=600000
source.connect.timeout.ms=1000
source.read.timeout.ms=120000
source.read.timings.enabled=false
```

### Choosing a transport

With `source.read.transport=jdbc` (the default), the URL scheme selects MySQL or Arrow Flight JDBC. The table below compares those JDBC paths. Set `source.read.transport=arrow-adbc` to read snapshot and CHANGES results as Arrow batches; `starrocks.jdbc.url` remains the JDBC endpoint for bookmarks and metadata.

| | MySQL protocol | Arrow Flight SQL |
| --- | --- | --- |
| URL | `jdbc:mysql://fe1:9030,fe2:9030` | `jdbc:arrow-flight-sql://fe1:9408?useEncryption=false` |
| Port | FE `query_port` (9030) | FE `arrow_flight_port` |
| Result path | Rows funnel back **through the FE** | FE plans and authorizes; result batches stream **from the BEs**, columnar |
| Setup | None | Requires cluster config and a worker JVM flag (below) |

Choose the read path using the table's column types and an end-to-end measurement. Arrow ADBC avoids JDBC row getters, but producing Kafka Connect records and writing to Kafka can dominate the total time.

To use Arrow Flight SQL:

1. **Set a non-negative `arrow_flight_port` in both `fe.conf` and `be.conf`, then restart.** It defaults to `-1` (disabled) and is not a mutable config, so `ADMIN SET FRONTEND CONFIG` will not turn it on.
2. **Point the URL at that port**, not 9030, and keep `?useEncryption=false` unless you have configured TLS — the Arrow driver negotiates TLS by default and will fail against a plaintext server without it.
3. **Open `java.nio` to the Connect worker** (Java 9+), or Arrow cannot allocate its off-heap buffers:
   ```bash
   export KAFKA_OPTS="$KAFKA_OPTS --add-opens=java.base/java.nio=ALL-UNNAMED"
   ```
   StarRocks' own Arrow Flight documentation writes this flag as
   `--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED`. Naming the module is
   correct when Arrow sits on the JVM's *module path*, but here Arrow is loaded from the
   classpath by Connect's plugin classloader, so there is no named
   `org.apache.arrow.memory.core` module to open to. Only the `ALL-UNNAMED` half takes effect, and
   the module half earns a `WARNING: Unknown module` line on every JVM start — harmless, but not
   worth carrying.

Everything else is identical: the same CHANGES windows, the same bookmarks, the same envelope. The connector skips the MySQL-only streaming fetch-size call on this transport, since Arrow streams RecordBatches natively.

To use Arrow ADBC reads, retain a MySQL JDBC URL for bookmark and metadata calls and add:

```properties
starrocks.jdbc.url=jdbc:mysql://fe1:9030,fe2:9030
source.read.transport=arrow-adbc
starrocks.adbc.uri=grpc+tcp://fe1:9408
```

Enable `arrow_flight_port` and the worker's `java.nio` JVM flag as above. For FE failover, list each Flight endpoint in `starrocks.adbc.uri`. Failed ADBC connection or read attempts advance to the next endpoint; JDBC bookmark calls rotate independently through `starrocks.jdbc.url`.
ADBC requires columns whose types the connector can parse; a column carried as opaque server-rendered text is rejected during preflight. Use the JDBC read transport for that table.

To verify a Flight deployment, run the integration smoke test against your cluster with `SR_TRANSPORT=arrow-jdbc` or `SR_TRANSPORT=arrow-adbc`. It checks the delete-before-insert ordering, typed complex values, and temporal values through the selected read path.

Every record's key and value are schema-carrying Kafka Connect `Struct`s, so configure a schema-aware `key.converter`/`value.converter` (e.g. `JsonConverter` with `schemas.enable=true`, or an Avro/Protobuf converter backed by a schema registry).

---

## Message Format

Every record is a Debezium envelope, built by Debezium's own `io.debezium.data.Envelope` rather than assembled by this connector, so its fields appear in the canonical order that library defines:

```
before, after, source, op, ts_ms, transaction
```

`source` carries `db`, `table`, `row_version`, and `bookmark.{base, head}`. `transaction` is always `null` — StarRocks' CHANGES stream carries no transaction metadata — but the field is present in every record's schema, so consumers and schema registries see the same envelope shape any other Debezium connector produces. Field order is part of the schema identity a registry compares against, so treat the order above as the contract, not as formatting.

The examples below track one running scenario against a PRIMARY KEY table `db1.orders(id INT, name VARCHAR, qty INT)` with `id` as its key, captured as topic `sr.db1.orders`.

For readability, the JSON below shows each envelope's logical field values in that canonical order; the actual bytes on the wire depend on the converter configured above (e.g. a schema+payload envelope for `JsonConverter`).

### Snapshot rows (`op: "r"`)

The initial snapshot is a point-in-time read pinned to one bookmark, so every row it emits shares the same `bookmark.base`/`bookmark.head` — both equal the snapshot's pinning bookmark id. `row_version` is **always `0`** for snapshot rows: a pinned read spans partitions that each sit at their own version, so there is no single version to report. See [`row_version` across record types](#row_version-across-record-types) for why the bookmark id must not be used here.

```json
{
  "before": null,
  "after": { "id": 1, "name": "widget", "qty": 10 },
  "source": {
    "db": "db1",
    "table": "orders",
    "row_version": 0,
    "bookmark": { "base": 1000, "head": 1000 }
  },
  "op": "r",
  "ts_ms": 1700000000000,
  "transaction": null
}
```
Key: `{ "id": 1 }`. (A second `op: "r"` record follows for `id: 2`, same `source` values.)

### `row_version` across record types

`source.row_version` and `source.bookmark.{base,head}` are **two different numbering spaces**, and the difference matters when you write a consumer:

| Field | Space | Typical magnitude |
| --- | --- | --- |
| `row_version` on `op: "c"` / `op: "d"` | the partition's visible version — starts at 1, +1 per publish | small, e.g. `3`, `4`, `5` |
| `row_version` on `op: "r"` | none; always `0` | `0` |
| `bookmark.base` / `bookmark.head` | FE global id generator, shared with table and tablet ids | large, e.g. `11952` |

Do not compare a bookmark id against a `row_version`, and do not use `row_version` as a cross-record ordering key. Within one Kafka partition the broker's own offset order already gives you the correct sequence — snapshot rows first, then changes in commit order — so ordering by offset is both sufficient and safer. `row_version` is best treated as diagnostic metadata: it tells you which StarRocks publish a change came from, and it is what makes the two halves of an `UPDATE` recognizable as one event (they share it).

### Insert (`op: "c"`)

An `INSERT` of `(3, 'thingamajig', 30)`, picked up by the next poll (`base` is the previously-committed bookmark, `head` the bookmark this poll just created):

```json
{
  "before": null,
  "after": { "id": 3, "name": "thingamajig", "qty": 30 },
  "source": {
    "db": "db1",
    "table": "orders",
    "row_version": 4,
    "bookmark": { "base": 1000, "head": 1010 }
  },
  "op": "c",
  "ts_ms": 1700000005000,
  "transaction": null
}
```
Key: `{ "id": 3 }`.

### Update (`op: "d"` + `op: "c"`, same key)

There is no dedicated `op: "u"`. StarRocks' CHANGES window represents a SQL `UPDATE` as a delete-of-old-value paired with an insert-of-new-value **at the same `row_version`**, and the connector preserves that pairing verbatim: it emits the delete first, then the insert, for the same key. An `UPDATE ... SET qty = 300 WHERE id = 3` produces:

```json
{
  "before": { "id": 3, "name": "thingamajig", "qty": 30 },
  "after": null,
  "source": {
    "db": "db1",
    "table": "orders",
    "row_version": 5,
    "bookmark": { "base": 1010, "head": 1020 }
  },
  "op": "d",
  "ts_ms": 1700000010000,
  "transaction": null
}
```
```json
{
  "before": null,
  "after": { "id": 3, "name": "thingamajig", "qty": 300 },
  "source": {
    "db": "db1",
    "table": "orders",
    "row_version": 5,
    "bookmark": { "base": 1010, "head": 1020 }
  },
  "op": "c",
  "ts_ms": 1700000010001,
  "transaction": null
}
```
Both records share key `{ "id": 3 }` and `source.row_version: 5`, and the delete is always produced before the insert for the same key within a window.

### Delete (`op: "d"`)

A `DELETE FROM db1.orders WHERE id = 2`:

```json
{
  "before": { "id": 2, "name": "gadget", "qty": 20 },
  "after": null,
  "source": {
    "db": "db1",
    "table": "orders",
    "row_version": 6,
    "bookmark": { "base": 1020, "head": 1030 }
  },
  "op": "d",
  "ts_ms": 1700000015000,
  "transaction": null
}
```
Key: `{ "id": 2 }`. If `source.tombstones.on.delete=true`, this record is immediately followed by a **separate** tombstone record: it shares the delete record's key (and key schema), destination topic, and Kafka Connect's internal `sourcePartition`/`sourceOffset` bookkeeping (the connector's own resume checkpoint, not the Kafka broker-assigned partition/offset), but it is its own Kafka message with a `null` value and `null` value schema — the usual Kafka Connect signal for downstream log compaction.

### Keys and unkeyed tables

The Kafka record key is a `Struct` built from the table's key columns -- the primary key of a PRIMARY KEY table, the aggregate key of an AGGREGATE KEY table, the sort key of a DUPLICATE KEY table; `information_schema.columns.COLUMN_KEY` marks them for every model -- using the same key schema for every record from that table. A DUPLICATE KEY sort key is not unique, so several rows can share one Kafka key; see [Limitations](#limitations) for what that means for compaction and tombstones. A table with no key column produces records with a `null` key.

### Column types

| StarRocks | Connect schema |
| --- | --- |
| `TINYINT` / `SMALLINT` / `INT` / `BIGINT` | `INT8` / `INT16` / `INT32` / `INT64` |
| `LARGEINT` | `org.apache.kafka.connect.data.Decimal` at scale 0 — 128-bit, so `INT64` would silently truncate. JsonConverter writes Decimal as base64 unless you set `value.converter.decimal.format=NUMERIC` |
| `FLOAT` / `DOUBLE` | `FLOAT32` / `FLOAT64` |
| `DECIMAL` | `org.apache.kafka.connect.data.Decimal`, scale from the column |
| `CHAR` / `VARCHAR` | `STRING` |
| `DATE` / `DATETIME` | `STRING` in StarRocks' own format, `2026-08-05` / `2026-08-05 12:34:56[.ffffff]` (fraction only when non-zero), logical names `com.starrocks.data.Date` / `.DateTime`. DATETIME carries no zone and none is claimed |
| `BINARY` / `VARBINARY` | `BYTES` (base64 in JSON), nested inside ARRAY/MAP/STRUCT too: the connector runs `SET binary_encoding_format = 'hex', binary_encoding_level = 'nested'` on its MySQL-protocol connections, so the cluster's global values do not change the result |
| `JSON` | `STRING` named `io.debezium.data.Json` |
| `ARRAY` / `MAP` / `STRUCT` | Native `array` / `map` / `struct` schemas -- a JSON array / object / object on the wire. Element types follow this table and nested elements are always optional. Map keys are always STRING (JsonConverter renders a map as an object only then); struct fields keep their declared order. The nesting comes from `information_schema.columns.COLUMN_TYPE`; a column whose COLUMN_TYPE does not parse (a struct field with a COMMENT, for one) falls back to `STRING` with the logical name `com.starrocks.data.Array` / `.Map` / `.Struct`, and the connector warns once at startup |
| `HLL` / `BITMAP` / `PERCENTILE` | **rejected at startup** |

**MAP key encoding (all transports).** Keys remain Connect STRINGs so JSON maps remain objects. A SQL NULL key becomes `\N`; a non-null key whose text starts with a backslash gets one extra leading backslash. For example, literal `null` stays `null`, literal `\N` becomes `\\N`, and literal `\\N` becomes `\\\N`. Consumers decode the exact `\N` sentinel as NULL and remove one leading backslash from keys starting with two backslashes. These are string contents before normal JSON escaping. Binary keys remain lowercase hex. Compared with earlier versions, existing string keys beginning with a backslash now have that extra escape; ordinary keys are unchanged.

**Complex types whose `COLUMN_TYPE` cannot be parsed are carried as their text form**, without a nested Connect schema. The value is preserved exactly as StarRocks renders it; what a consumer does not get is a nested schema it could project into. The logical name exists so a consumer can tell structured text from an ordinary string without knowing the source table.

`ARRAY`, `MAP` and `STRUCT` deliberately do **not** claim `io.debezium.data.Json`. They render in a JSON-like shape, but that name would promise every value parses as JSON, and that has not been verified at the edges — NULLs, embedded quotes, deep nesting. The name says what the column is; it makes no promise about how the text parses.

`HLL`, `BITMAP` and `PERCENTILE` hold aggregate sketches rather than values: a plain `SELECT` of one returns nothing a consumer can interpret or load back. A table containing one is refused during preflight, with the offending column named, rather than started and streamed as a non-value. Views cannot be captured either (they have no table model), so either leave the table out of `starrocks.table.names` or drop the column.

---

## Semantics

### Starting `no_snapshot` without a position

Set `source.no.snapshot.missing.position.policy=latest` to start automatically at the current StarRocks version when a table has neither a durable Connect offset nor a bookmark still held by this connector. It streams subsequent changes; it does not emit rows already present in the table. This also applies after an outage long enough for the bookmark reference to expire: changes between the old and new positions are skipped. A missing offset with a live held bookmark still resumes from that bookmark. This policy does not override a durable offset or recover a non-trackable CHANGES window. The default, `fail`, keeps the task stopped for an explicit decision.

To use `fail` and establish a baseline manually, follow these steps:

For a new `no_snapshot` connector on a Connect worker with offset management support (Kafka 3.6+), create the connector and stop it with `PUT /connectors/starrocks-cdc-orders/stop` before initializing offsets. Its task may fail before the stop; that is expected and emits nothing. For each captured table, run `SELECT bookmark_create('db1', 'orders', 'kc:starrocks-cdc-orders', '604800000')` against the StarRocks FE leader. Use the returned numeric bookmark id in a `PATCH /connectors/starrocks-cdc-orders/offsets` request while the connector is stopped:

```json
{"offsets":[{"partition":{"db":"db1","table":"orders"},"offset":{"bookmark_id":11955,"snapshot_done":true}}]}
```

The holder must be `kc:` followed by the connector `name`, and the TTL argument is `source.bookmark.ttl.ms`. Initialize every table, confirm with `GET /connectors/starrocks-cdc-orders/offsets`, then `PUT /connectors/starrocks-cdc-orders/resume` before the granted bookmark lease expires. The baseline begins at bookmark creation; changes before it are outside this capture. If a task starts with no durable offset but the holder still has bookmarks, it resumes from the oldest one. With `fail`, a missing position stops startup after an intentional reset as well. [Kafka Connect's REST API](https://kafka.apache.org/36/kafka-connect/user-guide/) supports altering source offsets while stopped.

- **Exactly-once is available only with `no_snapshot` and `source.no.snapshot.missing.position.policy=fail`.** Run on a distributed worker with `exactly.once.source.support=enabled` (Kafka 3.3+): Connect then writes each poll's records and their offset in one producer transaction, so a crash before the commit aborts the batch and a `read_committed` consumer never sees it. With `source.snapshot.mode=initial` the connector reports `UNSUPPORTED` and the worker refuses to run it exactly-once. Snapshot rows carry `snapshot_done=false` on purpose, so a restart redoes the snapshot whole rather than resuming a half-delivered one. With the `latest` missing-position policy the connector also reports `UNSUPPORTED`, because it may skip changes when no position survives.

- **Resetting offsets also releases the FE references.** With the connector stopped, `DELETE /connectors/{name}/offsets` makes `alterOffsets` release the holder's bookmarks for stored partitions consisting of exactly the two non-empty string fields `db` and `table`. An empty reset request covers all configured tables. A table that has not reached a durable offset is absent from a DELETE containing other stored tables; reset it explicitly with a PATCH naming its partition and a null offset. Removing a stale partition shape, including one with extra fields, does not release bookmarks for the canonical table partition and needs no valid connector configuration. FE cleanup requires a configuration that parses and an FE that answers. If a release fails, Connect does not update its offsets, but earlier FE releases are **not rolled back**; the reset is not atomic across FE and Connect. Retry the reset while the connector remains stopped. In `no_snapshot`, the next start follows the missing-position policy: `fail` requires initialization, `latest` begins at the current version. PATCH with explicit offsets validates the canonical partition shape, membership in the configured tables, a non-negative numeric `bookmark_id`, and a boolean `snapshot_done`.
- **At-least-once delivery (the default).** During task operation, `commit()` authorizes release only for bookmarks strictly below both the durable offset read back from Connect's offset store and the task's in-memory position. It queues those releases for the next `poll()` to execute; calling `commit()` alone is not evidence of a flush, since Connect also calls it when no flush happened. If the task crashes and restarts before that flush completes, the same CHANGES window (or the same snapshot) is replayed on restart, producing duplicate records. Downstream consumers should apply changes idempotently keyed on the record key, taking the records in Kafka partition order — a replay re-delivers the same records in the same order, so last-write-wins per key converges — or read from a compacted topic. Do **not** deduplicate by requiring `source.row_version` to increase: it is not comparable across record types (see [`row_version` across record types](#row_version-across-record-types)), and both halves of an `UPDATE` legitimately share one value.
- **Snapshot-to-incremental handoff.** With `source.snapshot.mode=initial`, the first poll for a fresh table creates a bookmark and opens a pinned `[_BOOKMARK_<id>_]` cursor. Each poll returns at most `source.snapshot.batch.size` `op: "r"` rows for that table; every snapshot row, including the last, carries `snapshot_done=false` in its Kafka Connect offset. The cursor stays open between polls and the task marks the snapshot done only after reaching EOF. The next poll streams incremental changes with the same bookmark id as its base — no version is skipped or double-counted between snapshot and streaming — and the first change record it produces is the first record whose offset carries `snapshot_done=true`. That flag only becomes the table's durably-committed offset once Kafka Connect's own offset-flush cycle persists it. A crash or transient read failure before then replays the entire snapshot at the **original bookmark**; batches already delivered may be duplicated. A durable unfinished offset supplies that bookmark. Before the first offset flush, recovery uses the oldest bookmark still held by this connector instead. Without a usable durable position, failure to list held references stops startup rather than assuming a fresh table. If the original bookmark can no longer be read, the configured `source.nontrackable.policy` applies; the default `fail` does not advance to a newer version silently.
- **`source.nontrackable.policy`** controls what happens when a snapshot bookmark or CHANGES window can no longer be read (see [Limitations](#limitations) for causes):
  - `fail` (default): the task throws and stops. An operator must intervene — for example by raising `source.bookmark.ttl.ms`, fixing the underlying cause, or switching this table to `resnapshot`.
  - `resnapshot`: the table's in-memory position is discarded and rebuilt from a brand-new snapshot on the very next poll, fully automatically. This is not free: a large table means a large re-read, and every key still present in the table is redelivered as a fresh `op: "r"` record even if it was already streamed once — but this cannot reconstruct deletes across the lost position. Downstream state may need explicit reconciliation or rebuilding to remove keys absent from the new snapshot.

---

## Limitations

1. **A connector renews its position while it keeps polling; long gaps between polls still lose it.** Each poll runs the renewal check, and a renewal round -- covering every bookmark the task still holds, since any of them can become the offset a restart resumes from -- fires once per third of the granted lease, capped at five minutes -- though a round can only happen at the top of a poll, so the next poll after the renewal interval elapses performs the check, so a table that produces no new version keeps its position for as long as the task is polling. Renewals are paced at a third of the lease the server granted -- not the value configured, which a cluster-side ceiling (`bookmark_reference_max_ttl_ms`) can cap -- and a failed renewal is retried rather than treated as a lost position -- a round in which every renewal failed backs off exponentially, from one poll interval up to a minute until a lease is known and a third of it afterwards. What renewal cannot cover is a task that is not polling: Kafka Connect's pause stops `poll()`, and bookmark renewals run on that poll thread, so a connector paused (or stopped, or deleted) for longer than the granted lease -- the smaller of `source.bookmark.ttl.ms` and the cluster's `bookmark_reference_max_ttl_ms`, either of them non-positive meaning no limit from that side -- still loses the bookmark, and the next CHANGES read then fails as non-trackable and triggers `fail`/`resnapshot`. The same holds for anything that keeps one poll running longer than the granted lease -- a slow snapshot batch or a large CHANGES window, say -- since no renewal can run mid-poll. Deployments that pause connectors for long stretches, or have individual polls that take longer than the lease, should raise `source.bookmark.ttl.ms` or set `source.nontrackable.policy=resnapshot`; note that `resnapshot` requires `source.snapshot.mode=initial` (the default), since the two together with `no_snapshot` are rejected at startup with a `ConfigException` -- with no snapshot to rebuild from, the table would silently skip every change between the expired base bookmark and the new one. Renewal needs `bookmark_renew` on the cluster; against a StarRocks without it, every renewal fails and is logged, and idle tables behave as they did before.
2. **`ts_ms` is processing time, not commit time.** It is the wall-clock time the connector built the record, not when the change was committed in StarRocks.
3. **DUPLICATE KEY / AGGREGATE KEY tables carry a non-unique key and are insert-only.** Their Kafka key is the sort key or the aggregate key. A DUPLICATE KEY sort key admits duplicates, so several rows share one Kafka key: partitioning and per-key ordering hold, but log compaction would drop rows that are not duplicates, and with `source.tombstones.on.delete=true` a tombstone would delete every row sharing the deleted row's key. They are also only trackable for pure insert workloads: a `DELETE` against a DUPLICATE KEY table, for instance, makes that CHANGES window non-trackable (StarRocks reports `CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): CDC for DUP_KEYS does not support delete`). AGGREGATE KEY tables report pre-aggregated values (per the table's own aggregate functions), not raw inserted rows.
4. **An empty table's snapshot is repeated harmlessly after a restart.** Per the snapshot-crash invariant above, a restart before `snapshot_done` is durably `true` redoes the whole snapshot; for a table with no rows yet, that is a no-op query with no side effect beyond a wasted round trip.
5. **A `resnapshot` can redeliver rows and leave stale downstream keys.** It reads the current table after a position is lost. Rows still present are re-emitted, but keys deleted since the lost position produce no deletion record. Reconcile or rebuild downstream state when choosing this policy. Normal snapshot retry/restart keeps the original bookmark and does not take this shortcut.
6. **Causes of "non-trackable," and what the policy actually covers.** Beyond idle TTL expiry (item 1), a CHANGES window also becomes non-trackable when the underlying data changed shape since the base bookmark: a dropped or truncated partition/table, a schema-rewriting `ALTER TABLE` (e.g. a type change requiring a slow schema change), a bucket count or resharding change, a partition/tablet hint that no longer resolves, or a window spanning a period when `enable_change_data_capture` was toggled off and back on. StarRocks reports these in two different shapes — a BE execution-time `CDC-ERROR-<n> (CODE)` error, or an FE planning-time rejection whose message contains the phrase "not trackable" — and `NonTrackableException.classify` recognizes both of those, as well as a stale/expired bookmark reference, routing all of them to the configured `source.nontrackable.policy`. Any *other* SQL error — one `classify` does not recognize — is not covered by this policy at all, whatever `source.nontrackable.policy` says: the table is retried on every poll, with a `source.poll.interval.ms` pause before each retry, and once it has kept failing for `source.poll.retry.timeout.ms` (10 minutes by default) the task fails. That is what turns a permanent error — a dropped column, a revoked privilege, bookmark meta functions switched off — into a FAILED task rather than a RUNNING one that never delivers.
7. **A whole CHANGES window is still materialized in the worker's heap.** Snapshots are returned in batches of at most `source.snapshot.batch.size` records per table per poll, but a window accumulated over a long outage is still held at once and can exhaust the worker's heap. Keep `source.poll.interval.ms` short enough for the expected change volume. A snapshot retains one read connection per active table until EOF, and a long pause between batches can still outlive its bookmark lease.
8. **Deleting a connector leaves its live bookmarks held until their TTL expires.** There is no unregister step, so the bookmarks a task still holds when the connector is removed keep pinning their versions against vacuum for up to the granted lease (`source.bookmark.ttl.ms`, 7 days by default, capped by the cluster's `bookmark_reference_max_ttl_ms`) — and indefinitely when neither sets a limit. A task *restart* is fine: at start the task lists every reference its holder still has on the table in `information_schema.table_bookmark_references` and takes them all back — the bookmark it resumes from, heads whose windows never became durable, releases a predecessor never got to issue — so they are released by the normal commit cycle instead of waiting out their TTL. A deletion is not: nothing runs afterwards to do that.
9. **Columns added to a base table after the task started are not captured until the task restarts.** Each table's schema is resolved once, at task start, so a newly added column simply does not appear in the emitted records. A *dropped* column is worse: because the connector projects columns explicitly, every subsequent poll fails with an `Unknown column` error. Restart the connector's tasks after any DDL on a captured table.
10. **Avro requires a topic name that is a valid Avro schema name.** Record schema names are derived from the topic name, so a hyphenated topic — most easily introduced through `starrocks.table2topic.map` — breaks `AvroConverter`. Use underscores rather than hyphens in `starrocks.table2topic.map` values and in `source.topic.prefix` whenever the worker's converter is Avro.
11. **DATE/DATETIME preserve the stored calendar fields, without timezone conversion.** Both JDBC drivers read DATE through `getString()`. MySQL JDBC also reads DATETIME as text; Flight JDBC reads it as `LocalDateTime` and formats it without constructing a `java.sql.Timestamp`. ADBC reads date and timestamp vectors directly. This avoids JVM timezone shifts and DST-gap normalization. JDBC regression tests cover UTC, Asia/Shanghai and America/Los_Angeles, including DST transitions and dates around the Gregorian cutover. All three transports are checked for exact DATE/DATETIME strings in the cluster smoke test.
12. **Recovery without a usable durable position requires a readable reference list.** This applies to `initial` when no valid bookmark offset exists, and to `no_snapshot` when no completed offset exists. If `information_schema.table_bookmark_references` cannot be read, startup fails, including with `latest`; the task cannot safely assume that no held bookmark exists. With an empty list, `initial` starts a fresh snapshot, while `no_snapshot` follows `fail` or `latest`.

---

## Quick Start

This walks through the same `db1.orders` scenario used in [Message Format](#message-format), end to end.

### 1. One-time cluster setup

```sql
ADMIN SET FRONTEND CONFIG ("enable_bookmark_meta_functions" = "true");
GRANT OPERATE ON SYSTEM TO USER 'root'@'%';
```

### 2. Create a PRIMARY KEY table with CDC enabled, and load some rows

```sql
CREATE TABLE db1.orders (
    id INT NOT NULL,
    name VARCHAR(64),
    qty INT
) PRIMARY KEY (id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("enable_change_data_capture" = "true");

INSERT INTO db1.orders VALUES (1, 'widget', 10), (2, 'gadget', 20);
```

### 3. Build the plugin and start `connect-standalone`

```bash
mvn clean package -DskipTests
cp target/starrocks-connector-for-kafka-*-with-dependencies.jar /path/to/kafka-connect/plugins/
```

Use the `source.properties` file from [Full Configuration Example](#full-configuration-example) above (adjust `starrocks.jdbc.url`/credentials for your cluster), then:

```bash
connect-standalone.sh worker.properties source.properties
```

### 4. Watch the snapshot arrive

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic sr.db1.orders --from-beginning
```

Two `op: "r"` records appear, one per row already in the table.

### 5. Make changes and watch them stream in

```sql
INSERT INTO db1.orders VALUES (3, 'thingamajig', 30);
UPDATE db1.orders SET qty = 300 WHERE id = 3;
DELETE FROM db1.orders WHERE id = 2;
```

Within `source.poll.interval.ms` (default 5 seconds), the same consumer prints, in order: one `op: "c"` (the insert), an `op: "d"` immediately followed by an `op: "c"` for `id: 3` (the update), and one `op: "d"` (the delete) — closing the loop from a StarRocks table change to a Kafka message.
