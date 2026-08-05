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
REPO_ROOT="$(cd ../../.. && pwd)"
JAR="$REPO_ROOT/target/starrocks-connector-for-kafka-1.0.5.jar"
PLUGIN_DIR="$REPO_ROOT/target/smoke-plugin"
OUT_DIR="$(mktemp -d)"
CONSUMED="$OUT_DIR/consumed.json"

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

# Fixed literals so the expected wire values below are constants, not derived at run time.
# 2026-08-05T00:00:00Z is 20670 days after the epoch; 2026-08-05T12:34:56Z is 1785933296000 ms.
# Both are what a UTC-Calendar read must produce regardless of the worker's own timezone.
TZ_DATE="2026-08-05"
TZ_DATETIME="2026-08-05 12:34:56"
TZ_EXPECT_DAYS=20670
TZ_EXPECT_MILLIS=1785933296000

step() { printf '\n=== %s ===\n' "$1"; }
fail() { printf '\nFAIL: %s\n' "$1" >&2; exit 1; }
note() { printf '  %s\n' "$1"; }

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
cleanup() {
  local rc=$?
  printf '\n=== cleanup ===\n'
  if [ -n "$worker_pid" ] && kill -0 "$worker_pid" 2>/dev/null; then
    kill "$worker_pid" 2>/dev/null || true
    for _ in $(seq 1 15); do kill -0 "$worker_pid" 2>/dev/null || break; sleep 1; done
    kill -9 "$worker_pid" 2>/dev/null || true
  fi
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

[ -f "$JAR" ] || fail "plugin jar missing at $JAR — run: (cd $REPO_ROOT && mvn -DskipTests package)"
jar tf "$JAR" | grep -q 'com/starrocks/connector/kafka/source/StarRocksCdcSourceConnector.class' \
  || fail "connector class missing from $JAR"
jar tf "$JAR" | grep -q 'org/mariadb/jdbc/Driver.class' \
  || fail "mariadb JDBC driver missing from $JAR — the primary maven-shade execution must include org.mariadb.jdbc:mariadb-java-client"
# ChangeRecordMapper builds every record with io.debezium.data.Envelope, whose class-init also
# pulls TransactionMonitor and SchemaNameAdjuster. Unit tests run on the full compile classpath
# and cannot see a missing shade include; it surfaces only here or as a NoClassDefFoundError in
# a real worker.
for cls in io/debezium/data/Envelope.class \
           io/debezium/pipeline/txmetadata/TransactionMonitor.class \
           io/debezium/util/SchemaNameAdjuster.class; do
  jar tf "$JAR" | grep -q "^$cls$" \
    || fail "$cls missing from $JAR — the primary maven-shade execution must include io.debezium:debezium-core"
done
note "plugin jar OK (connector + JDBC driver + Debezium envelope classes)"

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
sr_sql "CREATE TABLE $DB.$TABLE (id INT NOT NULL, v BIGINT, d DATE, ts DATETIME)
        PRIMARY KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num'='1', 'enable_change_data_capture'='true');"
sr_sql "INSERT INTO $DB.$TABLE VALUES
        (1,10,'$TZ_DATE','$TZ_DATETIME'),
        (2,20,'$TZ_DATE','$TZ_DATETIME'),
        (3,30,'$TZ_DATE','$TZ_DATETIME');"
note "created $DB.$TABLE with 3 rows incl. DATE/DATETIME (this worker's TZ: $(date +%Z))"

case "$SR_TRANSPORT" in
  mysql)
    CONNECTOR_JDBC_URL="jdbc:mysql://$SR_HOST:$SR_PORT"
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
    # Arrow's off-heap buffers need this on Java 9+, in the worker JVM that smoke.sh forks below.
    export KAFKA_OPTS="${KAFKA_OPTS:-} --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"
    jar tf "$JAR" | grep -q 'org/apache/arrow/driver/jdbc/ArrowFlightJdbcDriver.class' \
      || fail "Arrow Flight JDBC driver missing from $JAR — the primary maven-shade execution must include org.apache.arrow:flight-sql-jdbc-driver"
    ;;
  *)
    fail "SR_TRANSPORT must be mysql or arrow-flight, got '$SR_TRANSPORT'"
    ;;
esac
note "transport=$SR_TRANSPORT, connector URL=$CONNECTOR_JDBC_URL"

step "2. stage plugin and configs"
rm -rf "$PLUGIN_DIR"; mkdir -p "$PLUGIN_DIR"; cp "$JAR" "$PLUGIN_DIR/"
note "staged $(basename "$JAR") as the only plugin in $PLUGIN_DIR"

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

step "7. temporal columns survive a non-UTC worker"
# These ride the same records step 5 already consumed. A DATE read in the worker's local zone
# is not a wrong value but a rejected record: Connect's Date logical type demands UTC midnight
# and the converter throws, so the row never reaches Kafka at all.
grep -q "\"d\":$TZ_EXPECT_DAYS" "$CONSUMED" \
  || { note "actual: $(head -1 "$CONSUMED")"
       grep -iE "DataException|Date type" "$OUT_DIR/connect.log" | tail -5 || true
       fail "DATE $TZ_DATE should serialize to $TZ_EXPECT_DAYS days since epoch (UTC midnight); another value means the read used a non-UTC calendar, and no value at all means the converter rejected the record"; }
grep -q "\"ts\":$TZ_EXPECT_MILLIS" "$CONSUMED" \
  || { note "actual: $(head -1 "$CONSUMED")"
       fail "DATETIME $TZ_DATETIME should serialize to $TZ_EXPECT_MILLIS ms; an offset that is a whole number of hours means the worker's timezone leaked into the read"; }
note "DATE -> $TZ_EXPECT_DAYS and DATETIME -> $TZ_EXPECT_MILLIS, independent of the worker's timezone"

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

printf '\n=== PASS ===\n'
