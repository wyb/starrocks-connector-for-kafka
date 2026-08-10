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

They share steps 0–9. Steps 10 and 11 — column types and the preflight refusal of
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
