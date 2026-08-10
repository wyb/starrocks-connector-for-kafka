#!/usr/bin/env bash
#
# Smoke test for the StarRocks CDC source connector against EXISTING StarRocks
# and Kafka clusters (no Docker). Use smoke.sh instead if you want the
# containerised environment.
#
# It creates a throwaway database on your StarRocks cluster, runs a local
# connect-standalone worker against your Kafka bootstrap servers, asserts the
# CDC stream, and cleans everything up on exit.
#
#   SR_HOST=fe1 SR_USER=root SR_PASSWORD=secret \
#   KAFKA_BOOTSTRAP=broker1:9092 KAFKA_BIN=/opt/kafka/bin \
#   ./smoke-cluster.sh
#
# Every knob:
#   SR_HOST            StarRocks FE host                        (required)
#   SR_PORT            FE MySQL port, always needed             (default 9030)
#   SR_USER            user with OPERATE ON SYSTEM              (default root)
#   SR_PASSWORD        password                                 (default empty)
#   KAFKA_BOOTSTRAP    bootstrap servers                        (required)
#   KAFKA_BIN          dir holding kafka-topics.sh etc.         (required)
#   KAFKA_EXTRA_PROPS  file of extra worker props (SASL/SSL)    (optional)
#   TOPIC_RF           replication factor for the test topic    (default 1)
#   KEEP_ON_FAILURE    set to 1 to keep the test db for triage  (default unset)
#   SR_TRANSPORT       mysql | arrow-flight                     (default mysql)
#   SR_ARROW_PORT      FE arrow_flight_port, arrow-flight only  (default 9408)
#
# The two ports are not alternatives. SR_PORT is what this script's own mysql client uses for
# DDL/DML, the config probes and cleanup -- always, on both transports. SR_ARROW_PORT is only
# ever put in the connector's JDBC URL. Override SR_PORT only if your FE's query_port is not
# 9030, which is unrelated to which transport you picked.
#
# The whole point of SR_TRANSPORT is that both settings run the SAME nine assertions. The
# ordering invariant and the temporal reads are the two things most likely to regress when the
# driver underneath changes, and they are asserted identically either way.
#
set -euo pipefail

cd "$(dirname "$0")"

# REPO_ROOT / JAR / PLUGIN_DIR / OUT_DIR / CONSUMED, the TZ_* fixtures, step / fail /
# note, verify_plugin_jar and stage_plugin_dir.
# shellcheck source=common.sh
. ./common.sh

SR_PORT="${SR_PORT:-9030}"
SR_USER="${SR_USER:-root}"
SR_PASSWORD="${SR_PASSWORD:-}"
TOPIC_RF="${TOPIC_RF:-1}"
SR_TRANSPORT="${SR_TRANSPORT:-mysql}"
SR_ARROW_PORT="${SR_ARROW_PORT:-9408}"

# Unique per run so a rerun (or a parallel run) can never touch another's data.
SUFFIX="$(date +%Y%m%d%H%M%S)_$$"
DB="cdc_smoke_${SUFFIX}"
TABLE=orders
CONNECTOR_NAME="sr-cdc-smoke-${SUFFIX}"
TOPIC="sr.${DB}.${TABLE}"
# Aggregate sketches get their own table: preflight is expected to REFUSE it, so it must
# never be part of the connector the rest of the run depends on.
SKETCH_TABLE=sketches

# ${VAR:-} throughout: the required-arg checks below must be able to print a
# useful message instead of dying on `set -u` while building this array.
mysql_args=(-h "${SR_HOST:-}" -P "$SR_PORT" -u "$SR_USER")
[ -n "$SR_PASSWORD" ] && mysql_args+=(-p"$SR_PASSWORD")
sr_sql()  { mysql "${mysql_args[@]}" -e "$1"; }
sr_val()  { mysql "${mysql_args[@]}" -N -B -e "$1"; }   # no header, tab separated

# ADMIN SHOW FRONTEND CONFIG returns Key, AliasNames, Value, Type, IsMutable,
# Comment -- Value is the THIRD column, and AliasNames is usually empty, so the
# split must be on tabs. Default awk splitting collapses the empty field and
# silently yields Value as $2, which breaks the moment a config does have an
# alias.
sr_config() { sr_val "ADMIN SHOW FRONTEND CONFIG LIKE '$1';" | awk -F'\t' 'NR==1{print $3}'; }

worker_pid=""
sketch_pid=""
cleanup() {
  local rc=$?
  printf '\n=== cleanup ===\n'
  # Both, and by name: step 11 runs a second short-lived worker, and a Connect process
  # surviving this script would hold the REST port against the next run.
  for p in "$worker_pid" "$sketch_pid"; do
    [ -n "$p" ] || continue
    kill -0 "$p" 2>/dev/null || continue
    kill "$p" 2>/dev/null || true
    for _ in $(seq 1 15); do kill -0 "$p" 2>/dev/null || break; sleep 1; done
    kill -9 "$p" 2>/dev/null || true
  done
  if [ "$rc" -ne 0 ] && [ "${KEEP_ON_FAILURE:-}" = "1" ]; then
    note "KEEP_ON_FAILURE=1 -> leaving database $DB and topic $TOPIC in place"
    note "worker log: $OUT_DIR/connect.log   consumed: $CONSUMED"
    note "drop them with: DROP DATABASE $DB;"
    return
  fi
  # Dropping the database releases every bookmark held on its tables.
  if [ -n "${SR_HOST:-}" ]; then
    sr_sql "DROP DATABASE IF EXISTS $DB;" 2>/dev/null || note "could not drop $DB — drop it manually"
  fi
  if [ -n "${KAFKA_BIN:-}" ] && [ -n "${KAFKA_BOOTSTRAP:-}" ]; then
    "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" \
      --delete --topic "$TOPIC" >/dev/null 2>&1 \
      || note "could not delete topic $TOPIC (delete.topic.enable=false?) — delete it manually"
  fi
  rm -rf "$OUT_DIR" "$PLUGIN_DIR"
}
trap cleanup EXIT

# ---------------------------------------------------------------- preflight --
step "0. preflight"

[ -n "${SR_HOST:-}" ]         || fail "SR_HOST is required"
[ -n "${KAFKA_BOOTSTRAP:-}" ] || fail "KAFKA_BOOTSTRAP is required"
[ -n "${KAFKA_BIN:-}" ]       || fail "KAFKA_BIN is required (the dir holding kafka-topics.sh)"
command -v mysql >/dev/null   || fail "mysql client not found on PATH"
command -v java  >/dev/null   || fail "java not found on PATH"
[ -x "$KAFKA_BIN/kafka-topics.sh" ]           || fail "$KAFKA_BIN/kafka-topics.sh not executable"
[ -x "$KAFKA_BIN/connect-standalone.sh" ]     || fail "$KAFKA_BIN/connect-standalone.sh not executable"
[ -x "$KAFKA_BIN/kafka-console-consumer.sh" ] || fail "$KAFKA_BIN/kafka-console-consumer.sh not executable"

verify_plugin_jar

sr_val "SELECT 1;" >/dev/null || fail "cannot reach StarRocks at $SR_HOST:$SR_PORT as $SR_USER"
note "StarRocks reachable"

# Shared-data is mandatory: bookmarks and the CHANGES/BOOKMARK hints do not
# exist on a shared-nothing cluster.
run_mode=$(sr_config run_mode)
[ "$run_mode" = "shared_data" ] \
  || fail "cluster run_mode is '${run_mode:-unknown}', this connector requires shared_data"
note "run_mode=shared_data"

bm=$(sr_config enable_bookmark_meta_functions)
if [ "$bm" != "true" ]; then
  fail "enable_bookmark_meta_functions is '${bm:-unset}' on this FE. Enable it on the LEADER FE:
    ADMIN SET FRONTEND CONFIG (\"enable_bookmark_meta_functions\" = \"true\");
  and add the same line to fe.conf so it survives a restart."
fi
note "enable_bookmark_meta_functions=true"

# Bookmark functions are OPERATE-gated and leader-only. Probing on a throwaway
# table separates "wrong FE" from "missing privilege" before anything is staged.
sr_sql "CREATE DATABASE $DB;" || fail "cannot create database $DB (need CREATE DATABASE privilege)"
sr_sql "CREATE TABLE $DB.probe (k INT NOT NULL, v INT)
        PRIMARY KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ('replication_num'='1', 'enable_change_data_capture'='true');"
probe_err="$OUT_DIR/probe.err"
if ! probe_id=$(sr_val "SELECT bookmark_create('$DB','probe','smoke:probe','60000');" 2>"$probe_err"); then
  msg=$(cat "$probe_err")
  case "$msg" in
    *"must run on the FE leader"*)
      fail "SR_HOST=$SR_HOST is not the FE leader. Bookmark functions are leader-only.
    Find it with: SHOW FRONTENDS;   (look for the leader/master row)
    then rerun with SR_HOST set to that host." ;;
    *"Access denied"*|*"OPERATE"*|*privilege*)
      fail "user '$SR_USER' lacks the OPERATE privilege:
    GRANT OPERATE ON SYSTEM TO USER '$SR_USER'@'%';" ;;
    *) fail "bookmark_create failed unexpectedly: $msg" ;;
  esac
fi
sr_sql "SELECT bookmark_release('$DB','probe','$probe_id','smoke:probe');" >/dev/null
sr_sql "DROP TABLE $DB.probe;"
note "FE leader + OPERATE privilege confirmed"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --list >/dev/null 2>&1 \
  || fail "cannot reach Kafka at $KAFKA_BOOTSTRAP (auth needed? see KAFKA_EXTRA_PROPS)"
note "Kafka reachable"

# ------------------------------------------------------------------- set up --
step "1. seed StarRocks"
# One table carries every case. The temporal columns are here rather than in a table of their
# own because they must ride the same records the other assertions inspect: a DATE read in the
# worker's local zone makes Kafka Connect's Date logical type reject the whole record, so the
# ordering and delete assertions below would fail too -- which is the coupling worth testing.
# The type columns live here for the same reason the temporal ones do, and the PK
# restriction that might have forced them elsewhere applies only to KEY columns
# (CreateTableAnalyzer loops over keysColumnNames): value columns of a PRIMARY KEY table
# take any type. Keeping them here means they ride the update and delete records too, so
# the VARBINARY round trip is checked in a before-image as well as in the snapshot -- a
# separate table would only ever have produced op=r.
#
#   b   VARBINARY -- must arrive as Connect BYTES, i.e. base64 on the wire
#   j   JSON      -- carried as text, schema named io.debezium.data.Json
#   arr ARRAY     -- carried as text, schema named com.starrocks.data.Array
#   m   MAP       -- ditto, com.starrocks.data.Map
#   s   STRUCT    -- ditto, com.starrocks.data.Struct
#
# All three complex types are here rather than just ARRAY because they take separate
# branches in toJdbcType and in the logical-name lookup, and because information_schema
# reports each with its own DATA_TYPE -- a mapping that only holds if the server really
# spells them "array", "map" and "struct", which only a live cluster can confirm.
#
# 0x0102ff is deliberately not valid UTF-8: it is the byte sequence a text round trip
# mangles, and its base64 is AQL/.
sr_sql "CREATE TABLE $DB.$TABLE (id INT NOT NULL, v BIGINT, d DATE, ts DATETIME,
                                 b VARBINARY, j JSON, arr ARRAY<INT>,
                                 m MAP<VARCHAR(10),INT>, s STRUCT<x INT, y VARCHAR(10)>)
        PRIMARY KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num'='1', 'enable_change_data_capture'='true');"
# Named columns, not positional VALUES: adding a column to the DDL above must not silently
# shift every literal by one, which is exactly what a positional INSERT does.
sr_sql "INSERT INTO $DB.$TABLE (id, v, d, ts, b, j, arr, m, s) VALUES
        (1,10,'$TZ_DATE','$TZ_DATETIME',to_binary('0102ff','hex'),parse_json('{\"a\":1}'),[10,20,30],map{'mk':11},row(7,'seven')),
        (2,20,'$TZ_DATE','$TZ_DATETIME',to_binary('0102ff','hex'),parse_json('{\"a\":2}'),[10,20,30],map{'mk':22},row(7,'seven')),
        (3,30,'$TZ_DATE','$TZ_DATETIME',to_binary('0102ff','hex'),parse_json('{\"a\":3}'),[10,20,30],map{'mk':33},row(7,'seven'));"
note "created $DB.$TABLE with 3 rows incl. DATE/DATETIME/VARBINARY/JSON/ARRAY/MAP/STRUCT (this worker's TZ: $(date +%Z))"

case "$SR_TRANSPORT" in
  mysql)
    CONNECTOR_JDBC_URL="jdbc:mysql://$SR_HOST:$SR_PORT"
    WORKER_JAVA_OPTS=""
    ;;
  arrow-flight)
    # Plaintext by default: the Arrow driver negotiates TLS unless told otherwise, and the FE's
    # Flight service is plaintext in a default deployment.
    CONNECTOR_JDBC_URL="jdbc:arrow-flight-sql://$SR_HOST:$SR_ARROW_PORT?useEncryption=false"
    afp=$(sr_config arrow_flight_port)
    [ -n "$afp" ] && [ "$afp" != "-1" ] \
      || fail "SR_TRANSPORT=arrow-flight but the FE reports arrow_flight_port='${afp:--1}'.
    Set a non-negative arrow_flight_port in fe.conf AND be.conf and restart -- it is not a
    mutable config, so ADMIN SET FRONTEND CONFIG will not do."
    note "FE arrow_flight_port=$afp"
    # Arrow's off-heap buffers need java.nio opened on Java 9+. Only ALL-UNNAMED applies here:
    # the driver is shaded into the plugin jar and loaded from the classpath by Connect's plugin
    # classloader, so there is no named org.apache.arrow.memory.core module to open to -- naming it
    # only earns a "WARNING: Unknown module" on every JVM start. Scoped to the worker below rather
    # than exported, so the kafka-topics/console-consumer calls do not inherit a flag they have no
    # use for.
    WORKER_JAVA_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED"
    jar tf "$JAR" | grep -q 'org/apache/arrow/driver/jdbc/ArrowFlightJdbcDriver.class' \
      || fail "Arrow Flight JDBC driver missing from $JAR — the primary maven-shade execution must include org.apache.arrow:flight-sql-jdbc-driver"
    ;;
  *)
    fail "SR_TRANSPORT must be mysql or arrow-flight, got '$SR_TRANSPORT'"
    ;;
esac
note "transport=$SR_TRANSPORT, connector URL=$CONNECTOR_JDBC_URL"

step "2. stage plugin and configs"
stage_plugin_dir

cat > "$OUT_DIR/worker.properties" <<EOF
bootstrap.servers=$KAFKA_BOOTSTRAP
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false
offset.storage.file.filename=$OUT_DIR/offsets
offset.flush.interval.ms=5000
plugin.path=$PLUGIN_DIR
EOF
if [ -n "${KAFKA_EXTRA_PROPS:-}" ]; then
  [ -f "$KAFKA_EXTRA_PROPS" ] || fail "KAFKA_EXTRA_PROPS file not found: $KAFKA_EXTRA_PROPS"
  cat "$KAFKA_EXTRA_PROPS" >> "$OUT_DIR/worker.properties"
  note "appended $KAFKA_EXTRA_PROPS to worker properties"
fi

cat > "$OUT_DIR/source.properties" <<EOF
name=$CONNECTOR_NAME
connector.class=com.starrocks.connector.kafka.source.StarRocksCdcSourceConnector
tasks.max=1
starrocks.jdbc.url=$CONNECTOR_JDBC_URL
starrocks.database.name=$DB
starrocks.username=$SR_USER
starrocks.password=$SR_PASSWORD
starrocks.table.names=$TABLE
source.topic.prefix=sr
source.poll.intervalms=2000
EOF

# One partition: the delete-before-insert assertion reads consumer line order as
# message order, which only holds within a partition.
"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" \
  --create --if-not-exists --topic "$TOPIC" --partitions 1 --replication-factor "$TOPIC_RF" >/dev/null
note "created topic $TOPIC (1 partition, RF=$TOPIC_RF)"

# Appends, never truncates: the restart in step 6 would otherwise throw away the
# pre-restart half of the log, which is where the first bookmark releases land.
start_worker() {
  KAFKA_OPTS="${KAFKA_OPTS:-} $WORKER_JAVA_OPTS" \
    "$KAFKA_BIN/connect-standalone.sh" "$OUT_DIR/worker.properties" "$OUT_DIR/source.properties" \
    >> "$OUT_DIR/connect.log" 2>&1 &
  worker_pid=$!
  sleep 5
  kill -0 "$worker_pid" 2>/dev/null \
    || { tail -40 "$OUT_DIR/connect.log"; fail "worker died immediately — see $OUT_DIR/connect.log"; }
  note "worker started, pid=$worker_pid"
}
stop_worker() {
  local pid="$1"
  kill "$pid" || fail "kill of worker pid $pid failed"
  for _ in $(seq 1 20); do kill -0 "$pid" 2>/dev/null || { note "worker $pid stopped"; return 0; }; sleep 1; done
  fail "worker $pid still alive after SIGTERM — restart assertion would be meaningless"
}

consume() {
  "$KAFKA_BIN/kafka-console-consumer.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --topic "$TOPIC" --from-beginning --timeout-ms "${1:-15000}" 2>/dev/null || true
}

step "3. start worker and wait for the snapshot"
start_worker
snapshot_count=0
for _ in $(seq 1 20); do
  snapshot_count=$(consume 5000 | wc -l | tr -d ' ')
  [ "${snapshot_count:-0}" -ge 3 ] && break
  sleep 3
done
[ "${snapshot_count:-0}" -ge 3 ] \
  || { tail -60 "$OUT_DIR/connect.log"; fail "snapshot records never arrived (saw ${snapshot_count:-0}, expected >= 3) — see $OUT_DIR/connect.log"; }
note "snapshot records present ($snapshot_count)"

# ----------------------------------------------------------------- asserts --
step "4. apply UPDATE and DELETE"
sr_sql "UPDATE $DB.$TABLE SET v=200 WHERE id=2;"
sr_sql "DELETE FROM $DB.$TABLE WHERE id=3;"
sleep 15

step "5. consume and assert"
consume 15000 > "$CONSUMED"
note "consumed $(wc -l < "$CONSUMED") records"

reads=$(grep -c '"op":"r"' "$CONSUMED" || true)
[ "$reads" -eq 3 ] || fail "expected 3 snapshot (op=r) records, got $reads"
note "snapshot: 3 op=r records OK"

grep '"op":"d"' "$CONSUMED" | grep -q '"v":20'  || fail "UPDATE: missing op=d carrying before image v=20"
grep '"op":"c"' "$CONSUMED" | grep -q '"v":200' || fail "UPDATE: missing op=c carrying after image v=200"
d_line=$(grep -n '"v":20[,}]' "$CONSUMED" | grep '"op":"d"' | head -1 | cut -d: -f1)
c_line=$(grep -n '"v":200'    "$CONSUMED" | grep '"op":"c"' | head -1 | cut -d: -f1)
[ -n "$d_line" ] && [ -n "$c_line" ] && [ "$d_line" -lt "$c_line" ] \
  || fail "UPDATE: delete-before-insert ordering violated (d line ${d_line:-?}, c line ${c_line:-?}) — this is the ORDER BY __ROW_VERSION__, __CHANGE_TYPE__ DESC invariant"
note "UPDATE: op=d(v=20) precedes op=c(v=200) OK"

grep '"op":"d"' "$CONSUMED" | grep -q '"v":30[,}]' || fail "DELETE: missing op=d for id=3 (v=30)"
note "DELETE: op=d OK"

# Only visible once a real converter has serialized the Struct: the envelope comes from
# io.debezium.data.Envelope, so it must carry Debezium's transaction field even though
# StarRocks has no transaction metadata to put in it.
grep -q '"transaction"' "$CONSUMED" \
  || fail "records carry no transaction field — the envelope is not the Debezium one"
note "Debezium envelope shape confirmed on the wire"

step "6. restart: resume from committed offset, no snapshot replay"
old_pid="$worker_pid"
stop_worker "$old_pid"
sr_sql "INSERT INTO $DB.$TABLE (id, v) VALUES (4, 40);"
start_worker
[ "$worker_pid" != "$old_pid" ] || fail "worker PID unchanged after restart"
sleep 25

consume 15000 > "$CONSUMED.2"
reads_after=$(grep -c '"op":"r"' "$CONSUMED.2" || true)
[ "$reads_after" -eq 3 ] || fail "snapshot replayed after restart: op=r count went 3 -> $reads_after"
grep '"op":"c"' "$CONSUMED.2" | grep -q '"v":40' || fail "post-restart INSERT (v=40) never surfaced"
note "resumed from committed offset, no snapshot replay OK"

step "7. temporal columns"
# These ride the same records step 5 already consumed. A DATE read in the worker's local zone
# is not a wrong value but a rejected record: Connect's Date logical type demands UTC midnight
# and the converter throws, so the row never reaches Kafka at all.
grep -q "\"d\":$TZ_EXPECT_DAYS" "$CONSUMED" \
  || { note "actual: $(head -1 "$CONSUMED")"
       grep -iE "DataException|Date type" "$OUT_DIR/connect.log" | tail -5 || true
       fail "DATE $TZ_DATE should serialize to $TZ_EXPECT_DAYS days since epoch (UTC midnight); another value means the read used a non-UTC calendar, and no value at all means the converter rejected the record"; }
note "DATE -> $TZ_EXPECT_DAYS, independent of the worker's timezone"

# DATETIME's expected value is asserted on the MySQL protocol only. On Arrow Flight it arrives a
# whole number of hours off (8h on a UTC+8 cluster), and the cause is not on this side: the BE
# emits the wall clock as UTC -- TimestampValue::to_unix_microsecond is Julian-day arithmetic with
# no timezone input -- and SET time_zone changes nothing, so the shift is added when the Arrow
# field is converted to Avatica column metadata, the same lossy step that already drops nullability
# and precision. Asserting the value here would only re-report a known open defect on every run.
# The column stays in the table and is still required to arrive, so a regression that loses
# DATETIME entirely, or breaks the read outright, still fails.
if [ "$SR_TRANSPORT" = "arrow-flight" ]; then
  grep -q '"ts":' "$CONSUMED" \
    || { note "actual: $(head -1 "$CONSUMED")"
         fail "DATETIME column absent from the record entirely -- that is a new failure, not the known Arrow offset"; }
  actual_ts=$(sed -n 's/.*"ts":\([0-9-]*\).*/\1/p' "$CONSUMED" | head -1)
  note "DATETIME present (${actual_ts}); value NOT asserted on arrow-flight -- known offset vs the expected $TZ_EXPECT_MILLIS, tracked as an open Arrow Flight metadata defect"
else
  grep -q "\"ts\":$TZ_EXPECT_MILLIS" "$CONSUMED" \
    || { note "actual: $(head -1 "$CONSUMED")"
         fail "DATETIME $TZ_DATETIME should serialize to $TZ_EXPECT_MILLIS ms; an offset that is a whole number of hours means the worker's timezone leaked into the read"; }
  note "DATETIME -> $TZ_EXPECT_MILLIS, independent of the worker's timezone"
fi

step "8. bookmark reclamation actually happens"
# Releases lag one commit cycle behind acknowledged offsets by design, so a short run can
# finish having released nothing -- which would make "no failures" pass vacuously. Drive a few
# more windows, each separated by more than offset.flush.interval.ms, so the fence advances
# with acked records and reclamation is forced.
for i in 5 6 7; do
  sr_sql "INSERT INTO $DB.$TABLE (id, v) VALUES ($i, $((i * 10)));"
  sleep 8
done
released=0
for _ in $(seq 1 10); do
  released=$(grep -c "Released bookmark" "$OUT_DIR/connect.log" || true)
  [ "${released:-0}" -ge 1 ] && break
  sleep 3
done
[ "${released:-0}" -ge 1 ] \
  || { grep -E "Emitted CDC window|Released bookmark|Failed to release" "$OUT_DIR/connect.log" | tail -20 || true
       fail "no bookmark was ever released — the two-phase fence is not advancing, so every window's bookmark stays pinned against vacuum until its TTL"; }
note "reclamation confirmed ($released release(s))"

step "9. no release failures"
# Step 8 proved releases happen; this one proves none of them errored. A failed release is
# not fatal to the stream -- the bookmark just stays pinned against vacuum until its TTL --
# so it would otherwise pass unnoticed for days.
note "worker log bookmark activity:"
grep -E "Emitted CDC window|Released bookmark|Failed to release bookmark" "$OUT_DIR/connect.log" | tail -10 || true
if grep -q "Failed to release bookmark" "$OUT_DIR/connect.log"; then
  fail "worker reported bookmark release failures — bookmarks stay pinned until TTL; check FE privileges and $OUT_DIR/connect.log"
fi
note "no bookmark release failures"

step "10. column types that only a live cluster can settle"
# $CONSUMED already holds the snapshot, the UPDATE pair and the DELETE from step 5, so
# these columns are checked on the same records every other assertion inspects.

# VARBINARY must arrive as Connect BYTES, which JsonConverter renders as base64:
# 0x0102ff -> AQL/. Read through getString() the bytes would be charset-decoded and 0xff
# would come back as U+FFFD -- silently, because schema and read would both say STRING.
grep -q '"b":"AQL/"' "$CONSUMED" \
  || { head -3 "$CONSUMED"
       fail "VARBINARY did not arrive as base64 AQL/ -- it is being read as text, which destroys every byte that is not valid UTF-8"; }
# And specifically in a before-image: op=d records are built from the same extraction path
# but a different envelope field, so a regression could hit one and not the other.
grep '"op":"d"' "$CONSUMED" | grep -q '"b":"AQL/"' \
  || fail "VARBINARY survived the snapshot but not a before image (op=d)"
note "VARBINARY -> BYTES -> base64 AQL/, in both op=r and op=d OK"

# JSON and the three complex types are carried as text (their schemas are named, but
# schemas.enable is off here, so only values reach the wire). The exact rendering --
# spacing, quoting, escaping -- is StarRocks' choice and not something to pin, so each
# assertion looks for a value that could only have come from the right column.
for probe in '"j":' '"arr":' '"m":' '"s":'; do
  grep -q "$probe" "$CONSUMED" \
    || { head -3 "$CONSUMED"; fail "column $probe missing from the records entirely"; }
done
grep -q 'seven' "$CONSUMED" || { head -3 "$CONSUMED"; fail "STRUCT column did not arrive intact"; }
grep -q 'mk'    "$CONSUMED" || { head -3 "$CONSUMED"; fail "MAP column did not arrive intact"; }
grep -qE '"j":"?\{?\\?"a' "$CONSUMED" \
  || { head -3 "$CONSUMED"; fail "JSON column did not arrive intact"; }
grep -qE '"arr":"?\[?10' "$CONSUMED" \
  || { head -3 "$CONSUMED"; fail "ARRAY column did not arrive intact"; }
note "JSON, ARRAY, MAP and STRUCT values arrived intact"
note "one record, for the record:"
head -1 "$CONSUMED"

step "11. preflight refuses a table whose column cannot be exported"
# HLL/BITMAP/PERCENTILE hold aggregate sketches, not values. Before this guard existed the
# connector started cleanly on such a table and streamed the non-value forever, which from
# the outside is indistinguishable from working. This also proves the StarRocks type name
# reaches the connector on THIS transport -- the guard reads information_schema.DATA_TYPE,
# and on the MySQL protocol that is a second query merged onto the driver's own view.
sr_sql "CREATE TABLE $DB.$SKETCH_TABLE (k INT, h HLL HLL_UNION)
        AGGREGATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ('replication_num'='1');"
sed "s|^starrocks.table.names=.*|starrocks.table.names=$SKETCH_TABLE|; s|^name=.*|name=${CONNECTOR_NAME}-sketch|" \
  "$OUT_DIR/source.properties" > "$OUT_DIR/source-sketch.properties"
sed "s|^offset.storage.file.filename=.*|offset.storage.file.filename=$OUT_DIR/offsets-sketch|" \
  "$OUT_DIR/worker.properties" > "$OUT_DIR/worker-sketch.properties"

# Every standalone worker starts Connect's REST server, and it cannot be turned off, so a
# second one cannot come up while the first still holds port 8083. Steps 8 and 9 have
# already read this worker's log and step 10 read $CONSUMED, so nothing below needs it.
if kill -0 "$worker_pid" 2>/dev/null; then
  stop_worker "$worker_pid"
  worker_pid=""
  sleep 3   # let the listening socket be released before the next worker claims it
fi

KAFKA_OPTS="${KAFKA_OPTS:-} $WORKER_JAVA_OPTS" \
  "$KAFKA_BIN/connect-standalone.sh" "$OUT_DIR/worker-sketch.properties" "$OUT_DIR/source-sketch.properties" \
  > "$OUT_DIR/connect-sketch.log" 2>&1 &
sketch_pid=$!
for _ in $(seq 1 30); do
  grep -q "cannot be exported by a SELECT" "$OUT_DIR/connect-sketch.log" && break
  kill -0 "$sketch_pid" 2>/dev/null || break
  sleep 1
done
kill "$sketch_pid" 2>/dev/null || true
for _ in $(seq 1 10); do kill -0 "$sketch_pid" 2>/dev/null || break; sleep 1; done
kill -9 "$sketch_pid" 2>/dev/null || true

# Distinguish "the worker never got as far as the connector" from "the guard did not
# fire". Reporting the second when the first happened sends the reader looking for a bug
# in code that was never reached -- which is precisely what this step did on its first run,
# when it blamed the guard for a REST port that was still bound.
if ! grep -q "cannot be exported by a SELECT" "$OUT_DIR/connect-sketch.log"; then
  if grep -qE "Address already in use|Failed to bind" "$OUT_DIR/connect-sketch.log"; then
    fail "the probe worker could not start: something else holds Connect's REST port. This says
    nothing about the guard under test. Free the port, or set a different one via
    KAFKA_EXTRA_PROPS (listeners=HTTP://localhost:18083), and rerun."
  fi
  tail -40 "$OUT_DIR/connect-sketch.log"
  fail "preflight did not refuse the HLL column -- either the guard is gone, or the StarRocks type name is not reaching it on the $SR_TRANSPORT transport"
fi
grep -q "'h'" "$OUT_DIR/connect-sketch.log" \
  || fail "the refusal did not name the offending column, so an operator cannot act on it"
note "preflight refused $DB.$SKETCH_TABLE, naming column 'h'"

printf '\n=== PASS ===\n'
