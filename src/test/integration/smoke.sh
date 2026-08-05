#!/usr/bin/env bash
#
# End-to-end smoke test for the StarRocks CDC source connector.
#
# Verifies, against live StarRocks + Kafka containers:
#   1. the shaded plugin jar carries both the connector and its JDBC driver
#   2. an initial snapshot produces one op="r" record per existing row
#   3. UPDATE produces an adjacent op="d" + op="c" pair on the same key
#   4. DELETE produces an op="d"
#   5. killing and restarting the worker resumes from the committed offset
#      without replaying the snapshot
#
# Usage:  ./smoke.sh            (uses default images)
#         STARROCKS_IMAGE=my/img:tag ./smoke.sh
#
set -euo pipefail

cd "$(dirname "$0")"
REPO_ROOT="$(cd ../../.. && pwd)"
JAR="$REPO_ROOT/target/starrocks-connector-for-kafka-1.0.5.jar"
# Dedicated single-jar plugin directory, mounted at /plugins by docker-compose.yml.
PLUGIN_DIR="$REPO_ROOT/target/smoke-plugin"
OUT_DIR="$(mktemp -d)"
CONSUMED="$OUT_DIR/consumed.json"
DB=smoke
TABLE=orders
TOPIC=sr.smoke.orders

step()  { printf '\n=== %s ===\n' "$1"; }
fail()  { printf 'FAIL: %s\n' "$1" >&2; exit 1; }
sr_sql() { docker compose exec -T starrocks mysql -h127.0.0.1 -P9030 -uroot -e "$1"; }
kafka()  { docker compose exec -T kafka "$@"; }

cleanup() {
  printf '\n=== cleanup ===\n'
  docker compose down -v >/dev/null 2>&1 || true
  rm -rf "$OUT_DIR" "$PLUGIN_DIR"
}
trap cleanup EXIT

step "0. preflight: plugin jar contents"
[ -f "$JAR" ] || fail "plugin jar not found at $JAR — run: mvn -DskipTests package"
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
echo "plugin jar OK (connector + JDBC driver + Debezium envelope classes present)"

# Stage exactly one jar as the only plugin location. target/ itself must NOT be
# mounted: after `mvn package` it also holds the -with-dependencies jar, the
# original-*.jar (connector present, JDBC driver ABSENT -> IllegalStateException
# at worker startup), classes/ and the assembly dirs. Connect scans each of those
# as a separate plugin location, so which copy of the connector wins is arbitrary.
rm -rf "$PLUGIN_DIR"
mkdir -p "$PLUGIN_DIR"
cp "$JAR" "$PLUGIN_DIR/"
echo "staged $(basename "$JAR") as the only plugin in $PLUGIN_DIR"

step "1. start containers"
docker compose up -d
echo "waiting for StarRocks and Kafka health..."
for _ in $(seq 1 90); do
  sr_state=$(docker compose ps starrocks --format '{{.Health}}' 2>/dev/null || echo "")
  kf_state=$(docker compose ps kafka --format '{{.Health}}' 2>/dev/null || echo "")
  [ "$sr_state" = "healthy" ] && [ "$kf_state" = "healthy" ] && break
  sleep 5
done
[ "${sr_state:-}" = "healthy" ] || fail "StarRocks did not become healthy"
[ "${kf_state:-}" = "healthy" ] || fail "Kafka did not become healthy"

step "2. seed StarRocks"
sr_sql "ADMIN SET FRONTEND CONFIG (\"enable_bookmark_meta_functions\" = \"true\");"
sr_sql "CREATE DATABASE IF NOT EXISTS $DB;"
sr_sql "CREATE TABLE $DB.$TABLE (id INT NOT NULL, v BIGINT)
        PRIMARY KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num'='1', 'enable_change_data_capture'='true');"
sr_sql "INSERT INTO $DB.$TABLE VALUES (1,10),(2,20),(3,30);"
echo "seeded 3 rows"

step "3. generate worker config and start connect-standalone"
cat > "$OUT_DIR/worker.properties" <<EOF
bootstrap.servers=kafka:9092
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false
offset.storage.file.filename=/tmp/sr-cdc-smoke.offsets
offset.flush.interval.ms=5000
plugin.path=/plugins
EOF
docker compose cp "$OUT_DIR/worker.properties" kafka:/tmp/worker.properties
docker compose cp ./source.properties kafka:/tmp/source.properties

# Record the worker's PID inside the container. `&` forks, so $! is the child,
# and the connect-standalone -> kafka-run-class -> java chain is all `exec`, which
# preserves that PID. Matching by name does NOT work: the final argv contains the
# class `org.apache.kafka.connect.cli.ConnectStandalone`, never the hyphenated
# script name, so `pkill -f connect-standalone` silently matches nothing.
start_worker() {
  docker compose exec -d kafka bash -c \
    'nohup /opt/kafka/bin/connect-standalone.sh /tmp/worker.properties /tmp/source.properties > /tmp/connect.log 2>&1 & echo $! > /tmp/connect.pid'
  sleep 2
  worker_pid=$(docker compose exec -T kafka cat /tmp/connect.pid 2>/dev/null | tr -d '\r\n' || true)
  [ -n "$worker_pid" ] || fail "worker PID was not recorded — connect-standalone did not start"
  docker compose exec -T kafka kill -0 "$worker_pid" 2>/dev/null \
    || { docker compose exec -T kafka tail -40 /tmp/connect.log || true; fail "worker (pid $worker_pid) died immediately after start"; }
  echo "worker started, pid=$worker_pid"
}

# Graceful SIGTERM so Connect flushes offsets, then prove the process is really
# gone. Without this proof a failed kill would leave the original worker running
# and the no-replay assertion below would pass for the wrong reason.
stop_worker() {
  local pid="$1"
  docker compose exec -T kafka kill "$pid" || fail "kill of worker pid $pid failed"
  for _ in $(seq 1 15); do
    docker compose exec -T kafka kill -0 "$pid" 2>/dev/null || { echo "worker pid $pid stopped"; return 0; }
    sleep 1
  done
  fail "worker pid $pid still alive 15s after SIGTERM — restart assertion would be meaningless"
}

# One partition, created explicitly: the d-before-c ordering assertion reads
# consumer output line order as message order, which only holds within a partition.
kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic "$TOPIC" --partitions 1 --replication-factor 1

start_worker

# Wait for the snapshot records themselves, not for the topic to exist — the
# topic was pre-created above, so its presence proves nothing about the connector.
echo "waiting for snapshot records in $TOPIC..."
snapshot_count=0
for _ in $(seq 1 20); do
  snapshot_count=$(kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic "$TOPIC" --from-beginning --timeout-ms 5000 2>/dev/null | wc -l | tr -d ' ')
  [ "${snapshot_count:-0}" -ge 3 ] && break
  sleep 3
done
[ "${snapshot_count:-0}" -ge 3 ] \
  || { docker compose exec -T kafka tail -50 /tmp/connect.log || true; fail "snapshot records never arrived (saw ${snapshot_count:-0}, expected >= 3)"; }
echo "snapshot records present ($snapshot_count)"

step "4. apply UPDATE and DELETE"
sr_sql "UPDATE $DB.$TABLE SET v=200 WHERE id=2;"
sr_sql "DELETE FROM $DB.$TABLE WHERE id=3;"
sleep 15

step "5. consume and assert"
kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic "$TOPIC" --from-beginning --timeout-ms 15000 > "$CONSUMED" 2>/dev/null || true
echo "consumed $(wc -l < "$CONSUMED") records"

reads=$(grep -c '"op":"r"' "$CONSUMED" || true)
[ "$reads" -eq 3 ] || fail "expected 3 snapshot (op=r) records, got $reads"
echo "snapshot: 3 op=r records OK"

# UPDATE on id=2 must surface as a delete of the old image followed by the
# new one; ORDER BY __ROW_VERSION__, __CHANGE_TYPE__ DESC guarantees d first.
grep '"op":"d"' "$CONSUMED" | grep -q '"v":20' || fail "UPDATE: missing op=d carrying the before image v=20"
grep '"op":"c"' "$CONSUMED" | grep -q '"v":200' || fail "UPDATE: missing op=c carrying the after image v=200"
d_line=$(grep -n '"v":20[,}]' "$CONSUMED" | grep '"op":"d"' | head -1 | cut -d: -f1)
c_line=$(grep -n '"v":200' "$CONSUMED" | grep '"op":"c"' | head -1 | cut -d: -f1)
[ -n "$d_line" ] && [ -n "$c_line" ] && [ "$d_line" -lt "$c_line" ] \
  || fail "UPDATE: delete-before-insert ordering violated (d at line ${d_line:-?}, c at line ${c_line:-?})"
echo "UPDATE: op=d(v=20) precedes op=c(v=200) OK"

grep '"op":"d"' "$CONSUMED" | grep -q '"v":30[,}]' || fail "DELETE: missing op=d for id=3 (v=30)"
echo "DELETE: op=d OK"

# Only visible once a real converter has serialized the Struct: the envelope comes from
# io.debezium.data.Envelope, so it must carry Debezium's transaction field even though
# StarRocks has no transaction metadata to put in it.
grep -q '"transaction"' "$CONSUMED" \
  || fail "records carry no transaction field — the envelope is not the Debezium one"
echo "Debezium envelope shape confirmed on the wire"

step "6. crash recovery: no snapshot replay"
old_pid="$worker_pid"
stop_worker "$old_pid"
sr_sql "INSERT INTO $DB.$TABLE VALUES (4,40);"
start_worker
[ "$worker_pid" != "$old_pid" ] || fail "worker PID unchanged after restart — the old process was never replaced"
sleep 25

kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic "$TOPIC" --from-beginning --timeout-ms 15000 > "$CONSUMED.2" 2>/dev/null || true

reads_after=$(grep -c '"op":"r"' "$CONSUMED.2" || true)
[ "$reads_after" -eq 3 ] || fail "snapshot replayed after restart: op=r count went from 3 to $reads_after"
grep '"op":"c"' "$CONSUMED.2" | grep -q '"v":40' || fail "post-restart INSERT (v=40) never surfaced"
echo "restart: resumed from committed offset, no snapshot replay OK"

printf '\n=== PASS ===\n'
