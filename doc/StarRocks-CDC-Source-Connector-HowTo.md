# StarRocks CDC Source Connector How-To Guide

## What It Does

The StarRocks CDC source connector is a Kafka Connect **source** connector — the reverse direction of the existing StarRocks sink connector in this repository. It streams change data capture (CDC) events out of StarRocks **shared-data (cloud-native)** tables into Kafka topics, one topic per captured table.

Internally it drives StarRocks' existing bookmark and CHANGES infrastructure over the FE's MySQL protocol (JDBC): it pins a position with the `bookmark_create`/`bookmark_release` meta functions, takes a point-in-time initial snapshot via the `[_BOOKMARK_<id>_]` query hint, and streams incremental changes via the `[_CHANGES_<base>_<head>_]` query hint. Every record is emitted as a Debezium-style envelope (`op`/`before`/`after`/`source`/`ts_ms`) with a schema-carrying Kafka Connect `Struct` key and value. Delivery is **at-least-once**.

---

## Prerequisites

- **A StarRocks shared-data (cloud-native) cluster.** The `[_BOOKMARK_]`/`[_CHANGES_]` query hints and the bookmark meta functions this connector depends on are not available on shared-nothing clusters.
- **`enable_bookmark_meta_functions` must be turned on** (it defaults to `false`). Enable it on each FE:
  ```sql
  ADMIN SET FRONTEND CONFIG ("enable_bookmark_meta_functions" = "true");
  ```
  Add the same line to `fe.conf` too, so it survives an FE restart.
- **The connecting user needs the system-level `OPERATE` privilege.** `bookmark_create`/`bookmark_release` are gated by this privilege and only execute on the current FE leader:
  ```sql
  GRANT OPERATE ON SYSTEM TO USER 'cdc_user'@'%';
  ```
  You do not need to point the connector at the leader yourself — list every FE endpoint in `starrocks.jdbc.url` and the connector transparently rotates to the leader when a bookmark call reports "must run on the FE leader".
- **PRIMARY KEY tables must have `enable_change_data_capture` set to `true`** — at `CREATE TABLE` time, or via `ALTER TABLE` beforehand:
  ```sql
  ALTER TABLE db1.orders SET ("enable_change_data_capture" = "true");
  ```
  This property only applies to shared-data PRIMARY KEY tables. Turn it on *before* the version range you need the connector to cover: rows written while it was off cannot be replayed, and any CHANGES window that straddles an "off" period fails as non-trackable (`CDC-ERROR-1 (CHANGE_NOT_TRACKABLE)`).
- **DUPLICATE KEY and AGGREGATE KEY tables work with no table property changes**, but only for insert-only workloads. A `DELETE`/`TRUNCATE` against one of these tables (or a dropped/rewritten partition, a schema-rewriting `ALTER`, or a bucket/reshard change on any table) makes any CHANGES window spanning it non-trackable — see [Limitations](#limitations).
- **UNIQUE KEY tables are not supported.** `CHANGES` cannot read them, and `StarRocksCdcSourceConnector` rejects the configuration outright at startup rather than letting a task fail later.
- **No FE version requirement beyond the above.** The connector only relies on already-shipped capabilities (`bookmark_create`/`bookmark_release`, the `[_CHANGES_]`/`[_BOOKMARK_]` query hints); there is no minimum-version check in the code.

---

## Full Configuration Example

`starrocks.jdbc.url` points at the FE's **MySQL protocol port** (`9030` by default) — this is different from the sink connector's `starrocks.http.url`, which points at the HTTP/stream-load port.

| Key | Type | Default | Description |
|---|---|---|---|
| `starrocks.jdbc.url` | String | *(required)* | JDBC URL of the StarRocks FE MySQL protocol endpoint(s). Comma-separate multiple FE hosts (e.g. `jdbc:mysql://fe1:9030,fe2:9030`) so the client can rotate to the leader for bookmark calls. |
| `starrocks.database.name` | String | *(required)* | The name of the source StarRocks database. |
| `starrocks.username` | String | *(required)* | The username used to connect to StarRocks. Needs the `OPERATE` privilege. |
| `starrocks.password` | Password | *(required)* | The password used to connect to StarRocks. Stored as a Kafka Connect `Password`, so it is masked in logs and the connector status API. |
| `starrocks.table.names` | String | *(required)* | Comma-separated list of table names, within `starrocks.database.name`, to capture changes from. |
| `starrocks.table2topic.map` | String | `""` (empty) | Optional mapping from table name to Kafka topic name, formatted as `table:topic,table:topic`. Tables not listed here fall back to `source.topic.prefix`. |
| `source.topic.prefix` | String | `sr` | Prefix used to derive a topic name for tables without an explicit `starrocks.table2topic.map` entry: the topic is `<prefix>.<database>.<table>`. |
| `source.snapshot.mode` | String | `initial` | Whether to take an initial snapshot before streaming changes. One of `initial`, `no_snapshot`. **`no_snapshot` cannot be combined with `source.nontrackable.policy=resnapshot`** — that pairing is rejected at startup with a `ConfigException`, because `resnapshot` rebuilds a table's position from a fresh snapshot and would silently drop changes with snapshots disabled. |
| `source.poll.intervalms` | Long | `5000` | Milliseconds to sleep between polls after a poll that produced no records. |
| `source.bookmark.ttlms` | Long | `604800000` (7 days) | Time-to-live, in milliseconds, for the bookmark backing each table's position. This is a lease backstop, not a renewable lease in this version — see [Limitations](#limitations). |
| `source.nontrackable.policy` | String | `fail` | Action to take when a table's CHANGES window becomes non-trackable. One of `fail`, `resnapshot`. **`resnapshot` requires `source.snapshot.mode=initial`** (the default): combined with `source.snapshot.mode=no_snapshot` it is rejected at startup with a `ConfigException`, since there would be no snapshot to rebuild the table's position from. |
| `source.netchanges` | Boolean | `false` | When `true`, sets the session variable `enable_cdc_net_change=true` so StarRocks collapses intermediate changes and returns only the final net change per key within a window. |
| `source.tombstones.on.delete` | Boolean | `false` | When `true`, emits an extra tombstone record (null value, same key) immediately following each delete record. |
| `source.maxretries` | Int | `3` | Number of attempts for a bookmark call before giving up (mirrors the sink's `sink.maxretries`). |
| `connect.timeoutms` | Int | `1000` | Milliseconds before a connection attempt to StarRocks times out (same key name as the sink connector). |

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
source.poll.intervalms=5000
source.bookmark.ttlms=604800000
source.nontrackable.policy=fail
source.netchanges=false
source.tombstones.on.delete=false
source.maxretries=3
connect.timeoutms=1000
```

Every record's key and value are schema-carrying Kafka Connect `Struct`s, so configure a schema-aware `key.converter`/`value.converter` (e.g. `JsonConverter` with `schemas.enable=true`, or an Avro/Protobuf converter backed by a schema registry).

---

## Message Format

Every record is a Debezium-style envelope: `op` / `before` / `after` / `source` / `ts_ms`, where `source` carries `db`, `table`, `row_version`, and `bookmark.{base, head}`. The examples below track one running scenario against a PRIMARY KEY table `db1.orders(id INT, name VARCHAR, qty INT)` with `id` as its key, captured as topic `sr.db1.orders`.

For readability, the JSON below shows each envelope's logical field values; the actual bytes on the wire depend on the converter configured above (e.g. a schema+payload envelope for `JsonConverter`).

### Snapshot rows (`op: "r"`)

The initial snapshot is a point-in-time read pinned to one bookmark, so every row it emits shares the same `row_version` and `bookmark.base`/`bookmark.head` — all three equal the snapshot's pinning bookmark id (a snapshot read has no independent per-row version the way a CHANGES window does):

```json
{
  "op": "r",
  "before": null,
  "after": { "id": 1, "name": "widget", "qty": 10 },
  "source": {
    "db": "db1",
    "table": "orders",
    "row_version": 1000,
    "bookmark": { "base": 1000, "head": 1000 }
  },
  "ts_ms": 1700000000000
}
```
Key: `{ "id": 1 }`. (A second `op: "r"` record follows for `id: 2`, same `source` values.)

### Insert (`op: "c"`)

An `INSERT` of `(3, 'thingamajig', 30)`, picked up by the next poll (`base` is the previously-committed bookmark, `head` the bookmark this poll just created):

```json
{
  "op": "c",
  "before": null,
  "after": { "id": 3, "name": "thingamajig", "qty": 30 },
  "source": {
    "db": "db1",
    "table": "orders",
    "row_version": 1010,
    "bookmark": { "base": 1000, "head": 1010 }
  },
  "ts_ms": 1700000005000
}
```
Key: `{ "id": 3 }`.

### Update (`op: "d"` + `op: "c"`, same key)

There is no dedicated `op: "u"`. StarRocks' CHANGES window represents a SQL `UPDATE` as a delete-of-old-value paired with an insert-of-new-value **at the same `row_version`**, and the connector preserves that pairing verbatim: it emits the delete first, then the insert, for the same key. An `UPDATE ... SET qty = 300 WHERE id = 3` produces:

```json
{
  "op": "d",
  "before": { "id": 3, "name": "thingamajig", "qty": 30 },
  "after": null,
  "source": {
    "db": "db1",
    "table": "orders",
    "row_version": 1020,
    "bookmark": { "base": 1010, "head": 1020 }
  },
  "ts_ms": 1700000010000
}
```
```json
{
  "op": "c",
  "before": null,
  "after": { "id": 3, "name": "thingamajig", "qty": 300 },
  "source": {
    "db": "db1",
    "table": "orders",
    "row_version": 1020,
    "bookmark": { "base": 1010, "head": 1020 }
  },
  "ts_ms": 1700000010001
}
```
Both records share key `{ "id": 3 }` and `source.row_version: 1020`, and the delete is always produced before the insert for the same key within a window.

### Delete (`op: "d"`)

A `DELETE FROM db1.orders WHERE id = 2`:

```json
{
  "op": "d",
  "before": { "id": 2, "name": "gadget", "qty": 20 },
  "after": null,
  "source": {
    "db": "db1",
    "table": "orders",
    "row_version": 1030,
    "bookmark": { "base": 1020, "head": 1030 }
  },
  "ts_ms": 1700000015000
}
```
Key: `{ "id": 2 }`. If `source.tombstones.on.delete=true`, this record is immediately followed by a **separate** tombstone record: it shares the delete record's key (and key schema), destination topic, and Kafka Connect's internal `sourcePartition`/`sourceOffset` bookkeeping (the connector's own resume checkpoint, not the Kafka broker-assigned partition/offset), but it is its own Kafka message with a `null` value and `null` value schema — the usual Kafka Connect signal for downstream log compaction.

### Keys and unkeyed tables

The Kafka record key is a `Struct` built from just the table's primary key column(s), using the same key schema for every record from that table. DUPLICATE KEY and AGGREGATE KEY tables have no primary key, so their records carry a `null` key — see [Limitations](#limitations) for what that means for ordering.

---

## Semantics

- **At-least-once delivery.** A table's older bookmarks are only ever released from the task's `commit()` callback, which Kafka Connect invokes after the offsets carried by previously-returned records have been durably flushed — never eagerly during `poll()`. If the task crashes and restarts before that flush completes, the same CHANGES window (or the same snapshot) is replayed on restart, producing duplicate records. Downstream consumers should either apply changes idempotently keyed on the record key (e.g. treating `source.row_version` as monotonic per key) or read from a compacted topic.
- **Snapshot-to-incremental handoff.** With `source.snapshot.mode=initial`, the first poll for a table creates a bookmark and streams the table through `[_BOOKMARK_<id>_]`, emitting `op: "r"` rows — every one of them, including the last, carries `snapshot_done=false` in its Kafka Connect offset; there is no distinguished "final" snapshot row. The in-memory "snapshot done" flag flips as soon as that poll returns, so the very next poll streams incremental changes with that same bookmark id as its base — no version is skipped or double-counted between snapshot and streaming — and the first change record it produces is the first record whose offset carries `snapshot_done=true`. That flag only becomes the table's durably-committed offset once Kafka Connect's own offset-flush cycle persists it, on its own timer, independently of this connector. A crash before that flush leaves the durable offset at `snapshot_done=false` (or no offset at all), so restart redoes the entire snapshot from scratch rather than resuming halfway.
- **`source.nontrackable.policy`** controls what happens when a CHANGES window can no longer be read (see [Limitations](#limitations) for causes):
  - `fail` (default): the task throws and stops. An operator must intervene — for example by raising `source.bookmark.ttlms`, fixing the underlying cause, or switching this table to `resnapshot`.
  - `resnapshot`: the table's in-memory position is discarded and rebuilt from a brand-new snapshot on the very next poll, fully automatically. This is not free: a large table means a large re-read, and every key still present in the table is redelivered as a fresh `op: "r"` record even if it was already streamed once — acceptable under at-least-once, but worth accounting for on large or frequently-resnapshotted tables.

---

## Limitations

1. **Idle tables can lose their position.** This version has no bookmark lease renewal: a table that produces no new version for longer than `source.bookmark.ttlms` (default 7 days) has its bookmark expire on the StarRocks side. The next poll's CHANGES read against that stale range then fails as non-trackable, triggering `fail`/`resnapshot`. Tables with bursty or long idle periods should raise `source.bookmark.ttlms` or set `source.nontrackable.policy=resnapshot` to self-heal automatically. Note that the `resnapshot` mitigation is only available with `source.snapshot.mode=initial` (the default): `source.nontrackable.policy=resnapshot` together with `source.snapshot.mode=no_snapshot` is rejected at startup with a `ConfigException`, because self-healing is exactly what that pairing cannot do — with no snapshot to rebuild from, the table would silently skip every change between the expired base bookmark and the new one. On a `no_snapshot` deployment, raising `source.bookmark.ttlms` is the only mitigation for this limitation. (Lease renewal is on the roadmap as a follow-up, not implemented here.)
2. **`ts_ms` is processing time, not commit time.** It is the wall-clock time the connector built the record, not when the change was committed in StarRocks.
3. **DUPLICATE KEY / AGGREGATE KEY tables have no key and are insert-only.** Their records carry a `null` Kafka key, so Kafka's default partitioner assigns partitions with no ordering guarantee across them. They are also only trackable for pure insert workloads: a `DELETE` against a DUPLICATE KEY table, for instance, makes that CHANGES window non-trackable (StarRocks reports `CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): CDC for DUP_KEYS does not support delete`). AGGREGATE KEY tables report pre-aggregated values (per the table's own aggregate functions), not raw inserted rows.
4. **An empty table's snapshot is repeated harmlessly after a restart.** Per the snapshot-crash invariant above, a restart before `snapshot_done` is durably `true` redoes the whole snapshot; for a table with no rows yet, that is a no-op query with no side effect beyond a wasted round trip.
5. **A `resnapshot` can redeliver already-emitted changes.** Any change records already produced for a window before it was found non-trackable are not un-sent; the subsequent full resnapshot may redeliver the same keys again. This is within the at-least-once contract, but downstream consumers should be prepared for it.
6. **Causes of "non-trackable," and what the policy actually covers.** Beyond idle TTL expiry (item 1), a CHANGES window also becomes non-trackable when the underlying data changed shape since the base bookmark: a dropped or truncated partition/table, a schema-rewriting `ALTER TABLE` (e.g. a type change requiring a slow schema change), a bucket count or resharding change, a partition/tablet hint that no longer resolves, or a window spanning a period when `enable_change_data_capture` was toggled off and back on. StarRocks reports these in two different shapes — a BE execution-time `CDC-ERROR-<n> (CODE)` error, or an FE planning-time rejection whose message contains the phrase "not trackable" — and `NonTrackableException.classify` recognizes both of those, as well as a stale/expired bookmark reference, routing all of them to the configured `source.nontrackable.policy`. Any *other* SQL error — one `classify` does not recognize — is not covered by this policy at all: it always fails the task outright, regardless of whether `source.nontrackable.policy` is `fail` or `resnapshot`.
7. **A whole snapshot, or a whole CHANGES window, is materialized in the worker's heap.** Each `poll()` builds a single in-memory batch — there is no batching or pagination — so all the records of an entire snapshot, or of a window that accumulated over a long outage, are held at once and can exhaust the Kafka Connect worker's heap. Bound it by keeping `source.poll.intervalms` short so each window stays small, and by snapshotting large tables during quiet periods.
8. **Every task restart leaves the previously-committed bookmark unreleased until its TTL expires.** A bookmark is only released once Kafka Connect has confirmed the offset that supersedes it, so a task that stops before that happens — a rebalance, a worker restart, a config edit — leaves its last bookmark held. That bookmark keeps pinning its version against vacuum for up to `source.bookmark.ttlms` (7 days by default). Deleting the connector leaks its live bookmarks the same way: there is no unregister step. Restarting often on a large, busy table therefore retains more versions than the table's own vacuum settings would suggest.
9. **Columns added to a base table after the task started are not captured until the task restarts.** Each table's schema is resolved once, at task start, so a newly added column simply does not appear in the emitted records. A *dropped* column is worse: because the connector projects columns explicitly, every subsequent poll fails with an `Unknown column` error. Restart the connector's tasks after any DDL on a captured table.
10. **Avro requires a topic name that is a valid Avro schema name.** Record schema names are derived from the topic name, so a hyphenated topic — most easily introduced through `starrocks.table2topic.map` — breaks `AvroConverter`. Use underscores rather than hyphens in `starrocks.table2topic.map` values and in `source.topic.prefix` whenever the worker's converter is Avro.

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

Within `source.poll.intervalms` (default 5 seconds), the same consumer prints, in order: one `op: "c"` (the insert), an `op: "d"` immediately followed by an `op: "c"` for `id: 3` (the update), and one `op: "d"` (the delete) — closing the loop from a StarRocks table change to a Kafka message.
