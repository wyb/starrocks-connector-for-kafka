# CDC Source Connector Integration Smoke Test

End-to-end check of the StarRocks CDC source connector against live StarRocks
and Kafka. Unit tests cover the state machine with a fake client; this harness
is what exercises real JDBC, real CHANGES windows, real bookmarks, and the
packaged plugin jar.

Two entry points:

| Script | Use when |
| --- | --- |
| `smoke.sh` | You have Docker and want the whole environment created for you |
| `smoke-cluster.sh` | You already have StarRocks and Kafka clusters (no Docker) |

The two share their assertions but not their step numbers: `smoke.sh` has steps 0–8,
`smoke-cluster.sh` has 0–11, and only 0 and 3–6 line up. Steps 10 and 11 — column types and the preflight refusal of
aggregate-sketch columns — exist only in `smoke-cluster.sh`, which is the harness
that actually gets run against real clusters. Porting them to the Docker variant
means rewriting them against `docker compose exec`, and untested shell in a script
nobody runs is worse than a documented gap.

`smoke-cluster.sh` additionally runs preflight checks that only matter against
a real cluster — `run_mode=shared_data`, `enable_bookmark_meta_functions`, the
`OPERATE` privilege, and whether the FE you pointed at is the leader — each with
the exact remediation statement in the failure message. It creates a uniquely
named throwaway database (`cdc_smoke_<timestamp>_<pid>`) and drops it, plus its
topic, on every exit path. Pass `KEEP_ON_FAILURE=1` to keep them for triage.

```bash
SR_HOST=fe-leader SR_USER=root SR_PASSWORD=secret \
KAFKA_BOOTSTRAP=broker1:9092 KAFKA_BIN=/opt/kafka/bin \
./smoke-cluster.sh
```

Knobs: `SR_PORT` (9030), `SR_USER` (root), `SR_PASSWORD` (empty), `TOPIC_RF` (1),
`KAFKA_EXTRA_PROPS` (a file of extra worker properties, appended verbatim — this
is where SASL/SSL settings go), `KEEP_ON_FAILURE`.

`SR_TRANSPORT=arrow-flight` (with `SR_ARROW_PORT`, default 9408) runs the identical
assertions over the Arrow Flight SQL transport instead of the MySQL protocol. It checks the FE's
`arrow_flight_port` first and fails with the remedy if the cluster has it disabled, and it injects
the `--add-opens` flag Arrow needs into the worker JVM. Running both settings is the only way to
know a transport change has not moved the delete-before-insert ordering or the temporal reads.

## Transport benchmark

Two entry points compare the MySQL protocol with Arrow Flight SQL on the same cluster. Neither
is part of `mvn test`.

`TransportBench` (a JUnit class under `src/test/java`, selected only by name) drives
`StarRocksJdbcClient` directly: for every table it pins one bookmark, reads the snapshot at it
over both transports, alternating which goes first, and if `bench.mutation.sql` is given runs
that once and reads the resulting CHANGES window over both. It also times `bookmark_create` on
an unchanged table, the idle poll's fixed cost, per transport. It reports p50 and min wall time,
rows/s, process CPU, allocation and RSS, and writes the same table to `target/transport-bench.txt`.

`bench-transport.sh` runs this benchmark end to end: it creates `cdc_read_bench` with two
equal-row-count tables, invokes `mvn -q test -Dtest=TransportBench`, then drops the database
even if the benchmark fails. Both tables have the same column order: the shared scalar columns
are followed by `a`, `m`, `st`, and `j`. In `perf_complex` these are an eight-element ARRAY,
a three-entry MAP, a STRUCT, and JSON; in `perf_scalar` they are VARCHAR columns containing
corresponding text. This aligns the column count and approximates the text payload. JSON uses
`getString()` in `ValueReader`, so its type contrast does not exercise the nested-value parser.
If `cdc_read_bench` already exists, the script exits without changing it:

```bash
SR_HOST=127.0.0.1 SR_PORT=9039 SR_ARROW_PORT=9498 SR_USER=root \
  BENCH_ROWS=1000000 ./src/test/integration/bench-transport.sh
```

Knobs: `SR_PASSWORD` (empty), `BENCH_BUCKETS` (8), `BENCH_ROUNDS` (5; first discarded),
`BENCH_READ_TIMINGS` (0). With `BENCH_READ_TIMINGS=1`, the report also shows p50 time spent in
`ResultSet.next()` and `ValueReader.readRow()` for each read. These per-row clock calls add
overhead, so use `BENCH_READ_TIMINGS=0` for throughput comparisons:

```bash
SR_HOST=127.0.0.1 SR_PORT=9039 SR_ARROW_PORT=9498 SR_USER=root \
  BENCH_ROWS=1000000 BENCH_READ_TIMINGS=1 ./src/test/integration/bench-transport.sh
```

For temporary diagnostics in a Connect worker, set
`source.read.timings.enabled=true` on the source connector and enable DEBUG logging for
`com.starrocks.connector.kafka.source.StarRocksJdbcClient` and
`com.starrocks.connector.kafka.source.StarRocksCdcSourceTask`. The client logs `query_ms`,
`next_ms`, and `decode_ms`; the task logs `map_ms` and per-table `table_ms` for each emitted
snapshot batch or CHANGES window. The connector setting is off by default. Turn it off after
collecting the data because timing every row affects throughput.
The report remains in `target/transport-bench.txt` after cleanup. Compare MySQL and Arrow
within each table; the difference between tables also includes storage and wire-format costs,
so it does not isolate the text parser's cost. `TransportBench` can still be invoked directly
with `-Dbench.*` against existing tables when data must remain available.

`bench-cluster.sh` measures StarRocks → Connect → Kafka for scalar and complex tables over each
transport. It creates `cdc_bench` for the run and refuses to run if that database already exists;
the database is removed during cleanup after a successful create. It uses the same schema and
values as `bench-transport.sh`, with a separate table for
each shape/transport pair so one UPDATE cannot affect another run. The scalar table has VARCHAR
text in the `a`, `m`, `st`, and `j` columns; the complex table uses ARRAY, MAP, STRUCT, and JSON.
Each run uses the same worker settings and its own topic. Reports identify pairs such as
`scalar/mysql` and `complex/arrow-flight`. By default, each table has 5 million snapshot rows.
After the snapshot, the script updates 50,000 rows and then a disjoint 500,000 rows, producing
100,000 daily and 1 million burst CHANGES records respectively (DELETE + INSERT per update).
Each phase waits for its exact Kafka offset before the next update. The report separates
worker startup, snapshot delivery, and both CHANGES deliveries, using Kafka topic end offsets; it also
shows Connect poll time from JMX and worker RSS/CPU. A persistent JMX sampler also records
poll/write progress, outstanding records, producer queue and request latency, waiting threads,
and errors once per second. Its per-phase report shows tail drain and peak backlog; raw samples
remain in `target/bench-metrics-<run-id>/`. These producer latencies are rolling metrics and may
carry observations across the snapshot/CHANGES boundary. The record-count check does not validate
payloads, so run `smoke-cluster.sh` for correctness first. If a worker dies or the benchmark fails,
available worker and JMX logs are copied to the same metrics directory before temporary files are removed.
`KEEP_ON_FAILURE=1` additionally retains the generated database, topics, and temporary files.
The script requires a fresh topic for each run and never deletes a topic it did not create.

```bash
SR_HOST=fe-leader SR_USER=root SR_PASSWORD=secret \
KAFKA_BOOTSTRAP=broker1:9092 KAFKA_BIN=/opt/kafka/bin \
BENCH_ROWS=5000000 BENCH_SHAPES='scalar complex' \
BENCH_TRANSPORTS='mysql arrow-flight' ./bench-cluster.sh
```

Build a fresh plugin jar with `mvn -DskipTests package` before the benchmark. The snapshot
returns at most `BENCH_SNAPSHOT_BATCH_SIZE` records per poll (default 4096). For a comparison,
repeat the benchmark with `BENCH_TRANSPORTS='arrow-flight mysql'` to reveal order/cache effects.
Use `BENCH_SHAPES=scalar` or `BENCH_SHAPES=complex` to run just one shape.
Use `BENCH_DAILY_UPDATE_ROWS` and `BENCH_BURST_UPDATE_ROWS` to adjust the two update sizes;
their sum must fit in `BENCH_ROWS`. To run one custom CHANGES phase instead, set
`BENCH_MUTATION` and its expected `BENCH_MUTATION_RECORDS`; set `BENCH_MUTATION=''` to test only
the snapshot.
The CHANGES timeline reports SQL completion, Task start/end, Connect poll/write totals, and the
Kafka offset target as seconds from mutation start. Poll/write timestamps are sampled once per
second; the Kafka offset target is observed by a one-second polling loop. Raw epoch timestamps
are saved to `change-timeline.tsv` beside the JMX samples. Task timestamps require
`BENCH_READ_TIMINGS=1`, which also prints summed query/next/decode/map/table timings for snapshot
and data-bearing CHANGES reads. Snapshot Task timings sum active batches, excluding gaps between
polls. The script enables DEBUG for both CDC classes when it supplies its own Kafka logging config; if you set
`KAFKA_LOG4J_OPTS`, that config must enable them. Leave timing off for throughput comparisons.
The script needs `javac` to build its dependency-free JMX sampler.
`BENCH_DB`/`BENCH_TABLE` can be used for a single transport only; that run is labeled `existing`.
The default mutation changes every row of the existing table.

The rest of this page covers the Docker variant.

## Prerequisites

- Docker with the Compose v2 plugin, daemon running (`docker info` must show a
  `Server:` section).
- A **shared-data** StarRocks image. The default
  `starrocks/allin1-ubuntu:latest` is a convenience only — the connector needs
  bookmark meta functions plus `[_CHANGES_]` / `[_BOOKMARK_]` support, so point
  `STARROCKS_IMAGE` at a build that has them:

  ```bash
  STARROCKS_IMAGE=my-registry/celerdata-allin1:4.2 ./smoke.sh
  ```

- A Kafka image with the Connect scripts on board (default `apache/kafka:3.7.0`,
  override with `KAFKA_IMAGE`).
- The shaded plugin jar built first:

  ```bash
  cd ../../.. && mvn -DskipTests package
  ```

## Run

```bash
./smoke.sh
```

The script tears the containers down on exit, including on failure.

## What it asserts

| Step | Assertion |
| --- | --- |
| 0 | The primary shaded jar contains both the connector class and `org/mariadb/jdbc/Driver.class`, and is staged as the *only* jar in `target/smoke-plugin/` |
| 2-3 | A PRIMARY KEY table with `enable_change_data_capture=true` produces topic `sr.smoke.orders` |
| 5 | Initial snapshot emits exactly 3 `op="r"` records |
| 5 | `UPDATE` emits `op="d"` (before image `v=20`) **before** `op="c"` (after image `v=200`) |
| 5 | `DELETE` emits `op="d"` |
| 6 | Killing and restarting the worker resumes from the committed offset: the new row surfaces as `op="c"` and the snapshot is **not** replayed |
| 10 | *(cluster only)* `VARBINARY` arrives as base64 `AQL/`, i.e. Connect `BYTES`, in both a snapshot record and a before-image. `0x0102ff` is not valid UTF-8 on purpose: read through `getString()` the `0xff` would come back as U+FFFD, and nothing would throw, because schema and read would agree on `STRING`. `JSON`, `ARRAY`, `MAP` and `STRUCT` values arrive intact, and one whole record is printed so their actual rendering is visible rather than assumed |
| 11 | *(cluster only)* Preflight refuses a table with an `HLL` column, naming it. This also proves the StarRocks type name reaches the connector on the transport under test — the guard reads `information_schema.DATA_TYPE`, which on the MySQL protocol is a second query merged onto the driver's own view |

One table carries every case on purpose. The temporal and complex-type columns
ride the very records the ordering and delete assertions inspect, so a value that
only survives a snapshot — but not a before-image — still fails. Putting them in a
table of their own would have produced `op=r` records and nothing else.

Table-to-task fan-out (more than one captured table) is **not** covered by either
harness.

Step 5's ordering assertion is the one that matters most: the BE emits the
version chain newest-first with INSERT before DELETE inside a version, so the
connector's mandatory `ORDER BY __ROW_VERSION__, __CHANGE_TYPE__ DESC` is what
turns that into consumer-correct commit order. If that clause ever regresses,
this assertion fails.

## Files

| File | Purpose |
| --- | --- |
| `common.sh` | Sourced by both harnesses: shared paths, the timezone fixtures, `step`/`fail`/`note`, and the plugin-jar verification and staging |
| `docker-compose.yml` | StarRocks (shared-data) + Kafka (KRaft single node); image tags overridable |
| `source.properties` | Connector config, mounted into the Kafka container |
| `smoke.sh` | The run: preflight, seed, start worker, mutate, consume, assert, restart, assert |
| `bench-transport.sh` | Create scalar and complex tables, run `TransportBench`, then remove its database |
| `bench-cluster.sh` | End-to-end transport benchmark against existing clusters; see above |

`common.sh` deliberately holds only what is byte-identical between the two
harnesses. `sr_sql`, `cleanup`, `start_worker` and `stop_worker` share names but
not bodies — one drives containers through `docker compose exec`, the other a
local `mysql` client and a local process — so they stay in their own scripts.
Merging same-named but differently-implemented functions would not be
de-duplication.

The Connect worker properties are generated by `smoke.sh` at run time; Connect
itself runs as `connect-standalone` inside the Kafka container with
`../../../target/smoke-plugin` mounted at `/plugins`.

That directory is created by `smoke.sh` at step 0 and holds a copy of the primary
shaded jar and nothing else — Connect treats every entry under `plugin.path` as a
separate plugin location, and `target/` after a `mvn package` holds several
copies of the connector (the primary jar, the `-with-dependencies` jar,
`original-*.jar` with **no** bundled JDBC driver, plus `classes/` and the assembly
directories). Mounting `target/` wholesale made it arbitrary which one the worker
loaded, so a run could fail with `IllegalStateException: mariadb-java-client
driver not found on classpath` while the jar under test was perfectly fine. The
directory is removed again by the cleanup trap.
