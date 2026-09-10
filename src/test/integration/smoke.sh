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

# docker compose needs docker-compose.yml in $PWD, so this harness must run from
# its own directory regardless of where it was invoked from.
cd "$(dirname "$0")"

# REPO_ROOT / JAR / PLUGIN_DIR / OUT_DIR / CONSUMED, the TZ_* fixtures, step / fail /
# note, verify_plugin_jar and stage_plugin_dir.
# shellcheck source=common.sh
. ./common.sh

DB=smoke
TABLE=orders
TOPIC=sr.smoke.orders

sr_sql() { docker compose exec -T starrocks mysql -h127.0.0.1 -P9030 -uroot -e "$1"; }
kafka()  { docker compose exec -T kafka "$@"; }

cleanup() {
  printf '\n=== cleanup ===\n'
  # down -v destroys the container holding /tmp/connect.log, and most assertions below fail
  # without printing it. Keep a copy so a failed run leaves something to read.
  if docker compose cp kafka:/tmp/connect.log "$KEPT_LOG" >/dev/null 2>&1; then
    echo "worker log kept at $KEPT_LOG"
  fi
  docker compose down -v >/dev/null 2>&1 || true
  rm -rf "$OUT_DIR" "$PLUGIN_DIR"
}
KEPT_LOG="${KEPT_LOG:-${TMPDIR:-/tmp}/smoke-connect.log}"
trap cleanup EXIT

step "0. preflight: plugin jar contents"
verify_plugin_jar
stage_plugin_dir

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
# Bookmarks and the CHANGES/BOOKMARK hints do not exist on a shared-nothing cluster, and an image
# that ignores RUN_MODE fails 60s later as "snapshot records never arrived" instead.
run_mode=$(sr_sql "ADMIN SHOW FRONTEND CONFIG LIKE 'run_mode';" 2>/dev/null | awk -F'\t' 'NR==2{print $3}' || true)
[ "${run_mode:-}" = "shared_data" ] \
  || fail "cluster run_mode is '${run_mode:-unknown}', this connector requires shared_data"
sr_sql "ADMIN SET FRONTEND CONFIG (\"enable_bookmark_meta_functions\" = \"true\");"
sr_sql "CREATE DATABASE IF NOT EXISTS $DB;"
# One table carries every case. A DATE read in the worker's local zone makes Connect's Date
# logical type reject the whole record, so the other assertions fail with it -- deliberate.
sr_sql "CREATE TABLE $DB.$TABLE (id INT NOT NULL, v BIGINT, d DATE, ts DATETIME, flag BOOLEAN, tiny TINYINT)
        PRIMARY KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num'='1', 'enable_change_data_capture'='true');"
sr_sql "INSERT INTO $DB.$TABLE (id, v, d, ts, flag, tiny) VALUES
        (1,10,'$TZ_DATE','$TZ_DATETIME',true,1),
        (2,20,'$TZ_DATE','$TZ_DATETIME',true,1),
        (3,30,'$TZ_DATE','$TZ_DATETIME',true,1);"
echo "seeded 3 rows incl. DATE/DATETIME/BOOLEAN/TINYINT (container TZ applies to the worker, not this shell)"

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

# `&` forks and the connect-standalone -> kafka-run-class -> java chain is all `exec`, so $!
# is the JVM. Matching by name does not work: argv carries the class name, never the
# hyphenated script, so `pkill -f connect-standalone` silently matches nothing.
start_worker() {
  docker compose exec -d kafka bash -c \
    'nohup /opt/kafka/bin/connect-standalone.sh /tmp/worker.properties /tmp/source.properties >> /tmp/connect.log 2>&1 & echo $! > /tmp/connect.pid'
  sleep 2
  worker_pid=$(docker compose exec -T kafka cat /tmp/connect.pid 2>/dev/null | tr -d '\r\n' || true)
  [ -n "$worker_pid" ] || fail "worker PID was not recorded — connect-standalone did not start"
  docker compose exec -T kafka kill -0 "$worker_pid" 2>/dev/null \
    || { docker compose exec -T kafka tail -40 /tmp/connect.log || true; fail "worker (pid $worker_pid) died immediately after start"; }
  echo "worker started, pid=$worker_pid"
}

# Graceful SIGTERM so Connect flushes offsets, then prove the process is really gone --
# otherwise a failed kill leaves the old worker running and no-replay passes for the wrong reason.
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
  # || true: kafka-console-consumer exits non-zero on its own --timeout-ms path, and under
  # pipefail this bare assignment would kill the script before the diagnostic below runs.
  snapshot_count=$(kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic "$TOPIC" --from-beginning --timeout-ms 5000 2>/dev/null | wc -l | tr -d ' ' || true)
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
# Anchored: '"v":20' alone also matches '"v":200', so a connector that put the AFTER image in the
# delete -- the very swap this step exists to catch -- would satisfy an unanchored grep.
grep '"op":"d"' "$CONSUMED" | grep -q '"v":20[,}]' || fail "UPDATE: missing op=d carrying the before image v=20"
grep '"op":"c"' "$CONSUMED" | grep -q '"v":200[,}]' || fail "UPDATE: missing op=c carrying the after image v=200"
# Both greps are tied to id=2: the ORDER BY has no key term, so two rows changed in one
# transaction interleave as d,d,c,c and adjacency would fail on correct output. || true
# because under pipefail a grep matching nothing kills the script before the fail below.
d_line=$(grep -n '"id":2[,}]' "$CONSUMED" | grep '"v":20[,}]' | grep '"op":"d"' | head -1 | cut -d: -f1 || true)
c_line=$(grep -n '"id":2[,}]' "$CONSUMED" | grep '"v":200[,}]' | grep '"op":"c"' | head -1 | cut -d: -f1 || true)
[ -n "$d_line" ] && [ -n "$c_line" ] && [ "$d_line" -lt "$c_line" ] \
  || fail "UPDATE: delete-before-insert ordering violated for id=2 (d at line ${d_line:-?}, c at line ${c_line:-?})"
echo "UPDATE: op=d(v=20) precedes op=c(v=200) OK"

grep '"op":"d"' "$CONSUMED" | grep -q '"v":30[,}]' || fail "DELETE: missing op=d for id=3 (v=30)"
echo "DELETE: op=d OK"

# The column list now comes only from information_schema, so both transports must agree on it.
# BOOLEAN is the case that used to diverge: FE names its DATA_TYPE "tinyint" and only its
# COLUMN_TYPE "tinyint(1)", so keying off the name alone yields int8 here and boolean there.
grep -q '"flag":true' "$CONSUMED" \
  || fail "BOOLEAN column did not arrive as a JSON boolean -- it is being carried as int8 (got: $(grep -o '\"flag\":[^,}]*' "$CONSUMED" | head -1))"
grep -q '"tiny":1[,}]' "$CONSUMED" \
  || fail "TINYINT column did not arrive as a number; BOOLEAN's tinyint(1) rule must not swallow plain TINYINT"
echo "BOOLEAN -> boolean, TINYINT -> int8 OK"

# Only visible once a real converter has serialized the Struct: the envelope comes from
# io.debezium.data.Envelope, so it must carry Debezium's transaction field even though
# StarRocks has no transaction metadata to put in it.
grep -q '"transaction"' "$CONSUMED" \
  || fail "records carry no transaction field — the envelope is not the Debezium one"
echo "Debezium envelope shape confirmed on the wire"

# A DATE read in the worker's local zone is not a wrong value but a rejected record: Connect's
# Date logical type demands UTC midnight and the converter throws, so the row never arrives.
grep -q "\"d\":$TZ_EXPECT_DAYS" "$CONSUMED" \
  || { docker compose exec -T kafka grep -iE "DataException|Date type" /tmp/connect.log | tail -5 || true
       fail "DATE $TZ_DATE should serialize to $TZ_EXPECT_DAYS days since epoch (UTC midnight); another value means a non-UTC calendar, no value means the converter rejected the record"; }
grep -q "\"ts\":$TZ_EXPECT_MILLIS" "$CONSUMED" \
  || fail "DATETIME $TZ_DATETIME should serialize to $TZ_EXPECT_MILLIS ms; an offset that is a whole number of hours means the worker's timezone leaked into the read"
echo "temporal columns OK: DATE -> $TZ_EXPECT_DAYS, DATETIME -> $TZ_EXPECT_MILLIS"

step "6. crash recovery: no snapshot replay"
old_pid="$worker_pid"
stop_worker "$old_pid"
sr_sql "INSERT INTO $DB.$TABLE (id, v) VALUES (4, 40);"
start_worker
[ "$worker_pid" != "$old_pid" ] || fail "worker PID unchanged after restart — the old process was never replaced"
sleep 25

kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic "$TOPIC" --from-beginning --timeout-ms 15000 > "$CONSUMED.2" 2>/dev/null || true

reads_after=$(grep -c '"op":"r"' "$CONSUMED.2" || true)
[ "$reads_after" -eq 3 ] || fail "snapshot replayed after restart: op=r count went from 3 to $reads_after"
grep '"op":"c"' "$CONSUMED.2" | grep -q '"v":40' || fail "post-restart INSERT (v=40) never surfaced"
echo "restart: resumed from committed offset, no snapshot replay OK"

step "7. bookmark reclamation actually happens"
# Steps 7 and 8 only read an append-only log, so a worker that died after the restart would sail
# through both.
docker compose exec -T kafka kill -0 "$worker_pid" 2>/dev/null \
  || fail "the restarted worker is no longer running (pid $worker_pid)"
# The log is opened with >> and spans both worker lifetimes, so a count of >=1 is satisfied by a
# release from before the restart and the inserts below would prove nothing.
released_before=$(docker compose exec -T kafka grep -c "Released bookmark" /tmp/connect.log 2>/dev/null | tr -d '\r' || true)
released_before=${released_before:-0}
# A release waits for the bookmark to fall below the offset the store has actually flushed, so a
# short run can finish having released nothing -- which would make "no failures" pass vacuously.
for i in 5 6 7; do
  sr_sql "INSERT INTO $DB.$TABLE (id, v) VALUES ($i, $((i * 10)));"
  sleep 8
done
released=0
for _ in $(seq 1 10); do
  released=$(docker compose exec -T kafka grep -c "Released bookmark" /tmp/connect.log 2>/dev/null | tr -d '\r' || true)
  [ "${released:-0}" -gt "$released_before" ] && break
  sleep 3
done
[ "${released:-0}" -gt "$released_before" ] \
  || { docker compose exec -T kafka grep -E "Emitted CDC window|Released bookmark|Failed to release" /tmp/connect.log | tail -20 || true
       fail "no bookmark was released after the restart (still $released_before) — the two-phase fence is not advancing"; }
echo "reclamation confirmed ($((released - released_before)) new release(s) after the restart)"

step "8. no release failures"
# A negative grep passes when the log is missing or empty, and this is the last step before PASS.
docker compose exec -T kafka grep -q "Emitted CDC window" /tmp/connect.log \
  || fail "/tmp/connect.log holds no 'Emitted CDC window' line; there is nothing to check for failures"
# "Failed to release preflight probe bookmark" splits the two words, so match on the pattern.
if docker compose exec -T kafka grep -qE "Failed to release .*bookmark" /tmp/connect.log; then
  docker compose exec -T kafka grep -E "Failed to release .*bookmark" /tmp/connect.log | tail -10 || true
  fail "worker reported bookmark release failures — bookmarks stay pinned until TTL"
fi
echo "no bookmark release failures"

printf '\n=== PASS ===\n'
