#!/usr/bin/env bash
#
# End-to-end transport benchmark for the StarRocks CDC source connector against EXISTING
# StarRocks and Kafka clusters: identical data in separate tables, the same worker settings,
# one connect-standalone run per transport, and the time for records to land in Kafka.
#
#   SR_HOST=fe-leader SR_USER=root SR_PASSWORD=secret \
#   KAFKA_BOOTSTRAP=broker1:9092 KAFKA_BIN=/opt/kafka/bin \
#   BENCH_ROWS=1000000 ./bench-cluster.sh
#
# Every knob:
#   SR_HOST / SR_PORT / SR_USER / SR_PASSWORD     as in smoke-cluster.sh (leader FE, OPERATE user)
#   SR_ARROW_PORT        FE arrow_flight_port                        (default: FE config)
#   KAFKA_BOOTSTRAP / KAFKA_BIN / KAFKA_EXTRA_PROPS / TOPIC_RF        as in smoke-cluster.sh
#   BENCH_TRANSPORTS     space-separated, run in this order          (default "mysql arrow-flight")
#   BENCH_ROWS           rows to generate                            (default 1000000)
#   BENCH_BUCKETS        table buckets                               (default 8)
#   BENCH_PARTITIONS     topic partitions                            (default 1)
#   BENCH_HEAP           worker -Xmx                              (default 4g)
#   BENCH_SNAPSHOT_BATCH_SIZE  records returned per snapshot poll    (default 4096)
#   BENCH_POLL_INTERVAL_MS  source.poll.interval.ms                  (default 2000)
#   BENCH_MUTATION       SQL run after the snapshot, {db}/{table} substituted; empty skips
#                        the CHANGES phase        (default "UPDATE {db}.{table} SET v = v + 1")
#   BENCH_MUTATION_RECORDS  records the mutation is expected to emit (default 2*BENCH_ROWS:
#                        a PK update is a DELETE half and an INSERT half per row)
#   BENCH_TIMEOUT_SEC    per phase                                   (default 3600)
#   BENCH_SETTLE_SEC     wait after each phase to detect extra records (default 2 poll intervals)
#   BENCH_DB / BENCH_TABLE  use an existing table for a SINGLE transport; BENCH_ROWS is
#                        read with COUNT(*), the database is not dropped, and
#                        BENCH_MUTATION must fit that table's columns
#   BENCH_JMX_PORT       worker JMX port for the Connect metrics     (default 9999)
#   KEEP_ON_FAILURE      set to 1 to keep the db and topics for triage
#
# Reports per transport: worker-start-to-last-snapshot, first-to-last-snapshot, and
# mutation-start-to-last-change durations and rows/s; Connect poll time from JMX, worker
# peak RSS and CPU. The generated tables have identical contents; run in both transport orders
# to estimate cache and order effects.
#
set -euo pipefail
export LC_NUMERIC=C

cd "$(dirname "$0")"
# shellcheck source=common.sh
. ./common.sh

SR_PORT="${SR_PORT:-9030}"
SR_USER="${SR_USER:-root}"
SR_PASSWORD="${SR_PASSWORD:-}"
SR_ARROW_PORT="${SR_ARROW_PORT:-}"
TOPIC_RF="${TOPIC_RF:-1}"
BENCH_TRANSPORTS="${BENCH_TRANSPORTS:-mysql arrow-flight}"
BENCH_ROWS="${BENCH_ROWS:-1000000}"
BENCH_BUCKETS="${BENCH_BUCKETS:-8}"
BENCH_PARTITIONS="${BENCH_PARTITIONS:-1}"
BENCH_HEAP="${BENCH_HEAP:-4g}"
BENCH_SNAPSHOT_BATCH_SIZE="${BENCH_SNAPSHOT_BATCH_SIZE:-4096}"
BENCH_POLL_INTERVAL_MS="${BENCH_POLL_INTERVAL_MS:-2000}"
# An explicitly empty BENCH_MUTATION skips the CHANGES phase; unset means the default.
if [ "${BENCH_MUTATION+x}" != x ]; then BENCH_MUTATION='UPDATE {db}.{table} SET v = v + 1'; fi
BENCH_TIMEOUT_SEC="${BENCH_TIMEOUT_SEC:-3600}"
BENCH_SETTLE_SEC="${BENCH_SETTLE_SEC:-}"
BENCH_JMX_PORT="${BENCH_JMX_PORT:-9999}"
# Every kafka-*.sh goes through kafka-run-class.sh, which binds JMX_PORT when it is in the
# environment; an exported one would make each tool invocation fight the worker for the port.
unset JMX_PORT

SUFFIX="$(date +%Y%m%d%H%M%S)_$$"
if [ -n "${BENCH_DB:-}" ] && [ -n "${BENCH_TABLE:-}" ]; then
  DB="$BENCH_DB"; OWN_DB=0
else
  DB="cdc_bench_${SUFFIX}"; OWN_DB=1
fi

bench_table_for() {
  if [ "$OWN_DB" = 1 ]; then
    printf 'perf_%s\n' "${1//-/_}"
  else
    printf '%s\n' "$BENCH_TABLE"
  fi
}

mysql_args=(-h "${SR_HOST:-}" -P "$SR_PORT" -u "$SR_USER")
[ -n "$SR_PASSWORD" ] && mysql_args+=(-p"$SR_PASSWORD")
sr_sql()  { mysql "${mysql_args[@]}" -e "$1"; }
sr_val()  { mysql "${mysql_args[@]}" -N -B -e "$1"; }
sr_config() { sr_val "ADMIN SHOW FRONTEND CONFIG LIKE '$1';" | awk -F'\t' 'NR==1{print $3}'; }

worker_pid=""
sampler_pid=""
topics=()
cleanup() {
  local rc=$?
  printf '\n=== cleanup ===\n'
  for p in "$sampler_pid" "$worker_pid"; do
    [ -n "$p" ] || continue
    kill -0 "$p" 2>/dev/null || continue
    kill "$p" 2>/dev/null || true
    for _ in $(seq 1 15); do kill -0 "$p" 2>/dev/null || break; sleep 1; done
    kill -9 "$p" 2>/dev/null || true
  done
  if [ "$rc" -ne 0 ] && [ "${KEEP_ON_FAILURE:-}" = "1" ]; then
    note "KEEP_ON_FAILURE=1 -> leaving database $DB and topics ${topics[*]:-} in place; logs in $OUT_DIR"
    return
  fi
  if [ "$OWN_DB" = 1 ] && [ -n "${SR_HOST:-}" ]; then
    sr_sql "DROP DATABASE IF EXISTS $DB;" 2>/dev/null || note "could not drop $DB — drop it manually"
  fi
  if [ -n "${KAFKA_BIN:-}" ] && [ -n "${KAFKA_BOOTSTRAP:-}" ]; then
    for t in "${topics[@]:-}"; do
      [ -n "$t" ] || continue
      "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --delete --topic "$t" >/dev/null 2>&1 \
        || note "could not delete topic $t — delete it manually"
    done
  fi
  rm -rf "$OUT_DIR" "$PLUGIN_DIR"
}
trap cleanup EXIT

# ---------------------------------------------------------------- preflight --
step "0. preflight"
[ -n "${SR_HOST:-}" ]         || fail "SR_HOST is required"
[ -n "${KAFKA_BOOTSTRAP:-}" ] || fail "KAFKA_BOOTSTRAP is required"
[ -n "${KAFKA_BIN:-}" ]       || fail "KAFKA_BIN is required (the dir holding kafka-topics.sh)"
if { [ -n "${BENCH_DB:-}" ] && [ -z "${BENCH_TABLE:-}" ]; } \
    || { [ -z "${BENCH_DB:-}" ] && [ -n "${BENCH_TABLE:-}" ]; }; then
  fail "BENCH_DB and BENCH_TABLE must be set together"
fi
case "$BENCH_ROWS" in ''|*[!0-9]*) fail "BENCH_ROWS must be a positive integer" ;; esac
[ "$BENCH_ROWS" -gt 0 ] || fail "BENCH_ROWS must be positive"
case "$BENCH_SNAPSHOT_BATCH_SIZE" in ''|*[!0-9]*) fail "BENCH_SNAPSHOT_BATCH_SIZE must be a positive integer" ;; esac
[ "$BENCH_SNAPSHOT_BATCH_SIZE" -gt 0 ] || fail "BENCH_SNAPSHOT_BATCH_SIZE must be positive"
case "$BENCH_POLL_INTERVAL_MS" in ''|*[!0-9]*) fail "BENCH_POLL_INTERVAL_MS must be a positive integer" ;; esac
[ "$BENCH_POLL_INTERVAL_MS" -gt 0 ] || fail "BENCH_POLL_INTERVAL_MS must be positive"
if [ -z "$BENCH_SETTLE_SEC" ]; then
  BENCH_SETTLE_SEC=$(( (2 * BENCH_POLL_INTERVAL_MS + 999) / 1000 ))
fi
case "$BENCH_SETTLE_SEC" in ''|*[!0-9]*) fail "BENCH_SETTLE_SEC must be a non-negative integer" ;; esac
command -v mysql >/dev/null   || fail "mysql client not found on PATH"
command -v java  >/dev/null   || fail "java not found on PATH"
[ -x "$KAFKA_BIN/kafka-topics.sh" ]       || fail "$KAFKA_BIN/kafka-topics.sh not executable"
[ -x "$KAFKA_BIN/connect-standalone.sh" ] || fail "$KAFKA_BIN/connect-standalone.sh not executable"
[ -x "$KAFKA_BIN/kafka-run-class.sh" ]    || fail "$KAFKA_BIN/kafka-run-class.sh not executable"
verify_plugin_jar
sr_val "SELECT 1;" >/dev/null || fail "cannot reach StarRocks at $SR_HOST:$SR_PORT as $SR_USER"
[ "$(sr_config run_mode || true)" = "shared_data" ] || fail "this connector requires run_mode=shared_data"
[ "$(sr_config enable_bookmark_meta_functions || true)" = "true" ] \
  || fail "enable_bookmark_meta_functions is off on this FE; see smoke-cluster.sh for the remedy"
transport_count=0
seen_transports=" "
for tr in $BENCH_TRANSPORTS; do
  case "$seen_transports" in *" $tr "*) fail "BENCH_TRANSPORTS repeats '$tr'" ;; esac
  seen_transports="$seen_transports$tr "
  transport_count=$((transport_count + 1))
  case "$tr" in
    mysql) ;;
    arrow-flight)
      afp=$(sr_config arrow_flight_port || true)
      [ -n "$afp" ] && [ "$afp" != "-1" ] || fail "arrow_flight_port is disabled on the FE (set it in fe.conf and be.conf, restart)"
      [ -n "$SR_ARROW_PORT" ] || SR_ARROW_PORT="$afp"
      jar tf "$JAR" | grep -Fx 'org/apache/arrow/driver/jdbc/ArrowFlightJdbcDriver.class' >/dev/null \
        || fail "Arrow Flight JDBC driver missing from $JAR" ;;
    *) fail "BENCH_TRANSPORTS entries must be mysql or arrow-flight, got '$tr'" ;;
  esac
done
[ "$transport_count" -gt 0 ] || fail "BENCH_TRANSPORTS must name at least one transport"
if [ "$OWN_DB" = 0 ] && [ "$transport_count" -ne 1 ]; then
  fail "BENCH_DB/BENCH_TABLE can benchmark one transport only; comparisons need separate generated tables"
fi
"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --list >/dev/null 2>&1 \
  || fail "cannot reach Kafka at $KAFKA_BOOTSTRAP"
note "StarRocks and Kafka reachable; transports: $BENCH_TRANSPORTS"

# ------------------------------------------------------------------ the data --
step "1. data"
if [ "$OWN_DB" = 1 ]; then
  sr_sql "CREATE DATABASE $DB;"
  # Each transport gets the same data in its own table. Its mutation cannot affect the next run.
  for transport in $BENCH_TRANSPORTS; do
    TABLE=$(bench_table_for "$transport")
    sr_sql "CREATE TABLE $DB.$TABLE (id BIGINT NOT NULL, v BIGINT, d DECIMAL(18,2), s VARCHAR(64),
                                     dt DATETIME, a ARRAY<INT>)
            PRIMARY KEY(id) DISTRIBUTED BY HASH(id) BUCKETS $BENCH_BUCKETS
            PROPERTIES ('replication_num'='1', 'enable_change_data_capture'='true');"
    t0=$(date +%s)
    sr_sql "INSERT INTO $DB.$TABLE
            SELECT generate_series, generate_series * 7, generate_series / 100.0, concat('v', generate_series),
                   date_add('2026-01-01 00:00:00', INTERVAL generate_series SECOND),
                   [generate_series, generate_series + 1, generate_series + 2]
            FROM TABLE(generate_series(1, $BENCH_ROWS));"
    actual=$(sr_val "SELECT COUNT(*) FROM $DB.$TABLE;")
    [ "$actual" = "$BENCH_ROWS" ] || fail "$DB.$TABLE has $actual rows; expected $BENCH_ROWS"
    note "generated $BENCH_ROWS rows into $DB.$TABLE in $(( $(date +%s) - t0 )) s"
  done
else
  BENCH_ROWS=$(sr_val "SELECT COUNT(*) FROM $DB.$BENCH_TABLE;")
  [ "$BENCH_ROWS" -gt 0 ] || fail "existing $DB.$BENCH_TABLE is empty"
  note "using existing $DB.$BENCH_TABLE with $BENCH_ROWS rows"
fi
BENCH_MUTATION_RECORDS="${BENCH_MUTATION_RECORDS:-$(( BENCH_ROWS * 2 ))}"
case "$BENCH_MUTATION_RECORDS" in ''|*[!0-9]*) fail "BENCH_MUTATION_RECORDS must be a positive integer" ;; esac
[ "$BENCH_MUTATION_RECORDS" -gt 0 ] || fail "BENCH_MUTATION_RECORDS must be positive"

stage_plugin_dir

# ------------------------------------------------------------------ helpers --
# Sub-second where the shell has it (bash 5), whole seconds otherwise: BSD date has no %N.
now() { if [ -n "${EPOCHREALTIME:-}" ]; then printf '%s\n' "$EPOCHREALTIME"; else date +%s; fi; }

end_offset_sum() {  # $1 topic -> sum of end offsets over its partitions
  local out=""
  if [ -x "$KAFKA_BIN/kafka-get-offsets.sh" ]; then
    out=$("$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$1" --time -1 2>/dev/null) || out=""
  else
    out=$("$KAFKA_BIN/kafka-run-class.sh" kafka.tools.GetOffsetShell --broker-list "$KAFKA_BOOTSTRAP" --topic "$1" --time -1 2>/dev/null) || out=""
  fi
  printf '%s\n' "$out" | awk -F: 'NF>=3 {s+=$NF} END {print s+0}'
}

# Waits for exactly $2 records; prints seconds from $3 to first, first to target, and $3 to target.
wait_for_records() {
  local topic="$1" target="$2" start="$3" first="" n
  while :; do
    n=$(end_offset_sum "$topic")
    if [ -z "$first" ] && [ "$n" -gt 0 ]; then first=$(now); fi
    if [ "$n" -gt "$target" ]; then
      fail "topic $topic has $n records, more than the expected $target; benchmark result is invalid"
    fi
    if [ "$n" -eq "$target" ]; then
      awk -v s="$start" -v f="$first" -v e="$(now)" 'BEGIN {printf "%.1f %.1f %.1f\n", f - s, e - f, e - s}'
      return 0
    fi
    if awk -v s="$start" -v e="$(now)" -v t="$BENCH_TIMEOUT_SEC" 'BEGIN {exit !(e - s > t)}'; then
      fail "timed out after ${BENCH_TIMEOUT_SEC}s waiting for $target records in $topic (have $n) — see $OUT_DIR/connect-*.log"
    fi
    kill -0 "$worker_pid" 2>/dev/null || { tail -40 "$OUT_DIR/connect-$transport.log"; fail "worker died — see $OUT_DIR/connect-$transport.log"; }
    sleep 1
  done
}

assert_count_stable() {
  local topic="$1" expected="$2" actual
  sleep "$BENCH_SETTLE_SEC"
  actual=$(end_offset_sum "$topic")
  [ "$actual" -eq "$expected" ] \
    || fail "topic $topic has $actual records after settling; expected exactly $expected"
}

# Peak RSS (kB) and CPU seconds of a pid, sampled once a second into $2 until it exits.
start_sampler() {
  ( max=0
    while kill -0 "$1" 2>/dev/null; do
      r=$(ps -o rss= -p "$1" 2>/dev/null | tr -d ' ')
      c=$(ps -o cputime= -p "$1" 2>/dev/null | tr -d ' ')
      [ -n "$r" ] && [ "$r" -gt "$max" ] && max=$r
      secs=$(printf '%s' "$c" | awk -F'[-:]' '{ n=NF; s=$n; if (n>=2) s+=$(n-1)*60; if (n>=3) s+=$(n-2)*3600; if (n>=4) s+=$(n-3)*86400; print s+0 }')
      printf '%s %s\n' "$max" "$secs" > "$2"
      sleep 1
    done ) &
  sampler_pid=$!
}

jmx_metrics() {  # $1 connector name -> "avg max polled written" or "- - - -"
  local url="service:jmx:rmi:///jndi/rmi://127.0.0.1:$BENCH_JMX_PORT/jmxrmi" obj out=""
  obj="kafka.connect:type=source-task-metrics,connector=$1,task=0"
  for cls in org.apache.kafka.tools.JmxTool kafka.tools.JmxTool; do
    out=$("$KAFKA_BIN/kafka-run-class.sh" "$cls" --jmx-url "$url" --object-name "$obj" --one-time true \
            --report-format properties 2>/dev/null) && [ -n "$out" ] && break
    out=""
  done
  if [ -z "$out" ]; then echo "- - - -"; return; fi
  # properties format: <object name, itself full of '='>:<attribute>=<value>, so take the last field.
  printf '%s\n' "$out" | awk -F= '
    /poll-batch-avg-time-ms/  {a=$NF} /poll-batch-max-time-ms/ {m=$NF}
    /source-record-poll-total/ {p=$NF} /source-record-write-total/ {w=$NF}
    END {printf "%.1f %.1f %d %d\n", a, m, p, w}'
}

# ---------------------------------------------------------------- the runs --
results=()
for transport in $BENCH_TRANSPORTS; do
  step "run: $transport"
  TABLE=$(bench_table_for "$transport")
  case "$transport" in
    mysql)        url="jdbc:mysql://$SR_HOST:$SR_PORT"; opens="" ;;
    arrow-flight) url="jdbc:arrow-flight-sql://$SR_HOST:$SR_ARROW_PORT?useEncryption=false"
                  opens="--add-opens=java.base/java.nio=ALL-UNNAMED" ;;
  esac
  connector="sr-cdc-bench-$transport-$SUFFIX"
  prefix="bench-$transport"
  topic="$prefix.$DB.$TABLE"
  topics+=("$topic")
  "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --create --if-not-exists \
    --topic "$topic" --partitions "$BENCH_PARTITIONS" --replication-factor "$TOPIC_RF" >/dev/null

  cat > "$OUT_DIR/worker-$transport.properties" <<EOF
bootstrap.servers=$KAFKA_BOOTSTRAP
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false
value.converter.decimal.format=NUMERIC
offset.storage.file.filename=$OUT_DIR/offsets-$transport
offset.flush.interval.ms=5000
plugin.path=$PLUGIN_DIR
EOF
  [ -n "${KAFKA_EXTRA_PROPS:-}" ] && cat "$KAFKA_EXTRA_PROPS" >> "$OUT_DIR/worker-$transport.properties"
  cat > "$OUT_DIR/source-$transport.properties" <<EOF
name=$connector
connector.class=com.starrocks.connector.kafka.source.StarRocksCdcSourceConnector
tasks.max=1
starrocks.jdbc.url=$url
starrocks.database.name=$DB
starrocks.username=$SR_USER
starrocks.password=$SR_PASSWORD
starrocks.table.names=$TABLE
source.topic.prefix=$prefix
source.poll.interval.ms=$BENCH_POLL_INTERVAL_MS
source.snapshot.batch.size=$BENCH_SNAPSHOT_BATCH_SIZE
EOF

  worker_started=$(now)
  KAFKA_HEAP_OPTS="-Xms$BENCH_HEAP -Xmx$BENCH_HEAP" JMX_PORT="$BENCH_JMX_PORT" \
  KAFKA_OPTS="${KAFKA_OPTS:-} $opens" \
    "$KAFKA_BIN/connect-standalone.sh" "$OUT_DIR/worker-$transport.properties" "$OUT_DIR/source-$transport.properties" \
    >> "$OUT_DIR/connect-$transport.log" 2>&1 &
  worker_pid=$!
  start_sampler "$worker_pid" "$OUT_DIR/sampler-$transport"
  note "worker pid=$worker_pid, heap=$BENCH_HEAP, url=$url"

  timing=$(wait_for_records "$topic" "$BENCH_ROWS" "$worker_started")
  read -r to_first snap_stream_secs snap_total_secs <<<"$timing"
  note "snapshot: first record in $to_first s; all $BENCH_ROWS in $snap_total_secs s from worker start ($snap_stream_secs s after first)"
  assert_count_stable "$topic" "$BENCH_ROWS"

  chg_secs="-"; mutation_sql_secs="-"
  if [ -n "$BENCH_MUTATION" ]; then
    sql="${BENCH_MUTATION//\{db\}/$DB}"; sql="${sql//\{table\}/$TABLE}"
    mutation_started=$(now)
    sr_sql "$sql;"
    mutation_committed=$(now)
    mutation_sql_secs=$(awk -v s="$mutation_started" -v e="$mutation_committed" 'BEGIN {printf "%.1f", e - s}')
    timing=$(wait_for_records "$topic" "$(( BENCH_ROWS + BENCH_MUTATION_RECORDS ))" "$mutation_started")
    read -r _ _ chg_secs <<<"$timing"
    note "changes: $BENCH_MUTATION_RECORDS records in $chg_secs s from mutation start (SQL $mutation_sql_secs s)"
    assert_count_stable "$topic" "$(( BENCH_ROWS + BENCH_MUTATION_RECORDS ))"
  fi

  metrics=$(jmx_metrics "$connector")
  read -r jmx_avg jmx_max jmx_polled jmx_written <<<"$metrics"
  kill "$worker_pid"; for _ in $(seq 1 30); do kill -0 "$worker_pid" 2>/dev/null || break; sleep 1; done
  kill -9 "$worker_pid" 2>/dev/null || true
  wait "$worker_pid" 2>/dev/null || true
  worker_pid=""
  sleep 1; kill "$sampler_pid" 2>/dev/null || true
  wait "$sampler_pid" 2>/dev/null || true; sampler_pid=""
  rss_kb=0; cpu_s=0
  [ -f "$OUT_DIR/sampler-$transport" ] && read -r rss_kb cpu_s < "$OUT_DIR/sampler-$transport"
  results+=("$transport $to_first $snap_total_secs $snap_stream_secs $mutation_sql_secs $chg_secs $jmx_avg $jmx_max $jmx_polled $jmx_written $rss_kb $cpu_s")
done

# ----------------------------------------------------------------- report --
step "report ($BENCH_ROWS rows per table; changes phase expects $BENCH_MUTATION_RECORDS records; snapshot batch $BENCH_SNAPSHOT_BATCH_SIZE; poll interval ${BENCH_POLL_INTERVAL_MS} ms)"
printf '%-13s %9s %14s %15s %11s %14s %13s %13s %13s %13s %9s %9s %9s %8s\n' \
  transport first-rec-s snapshot-e2e-s snapshot-stream-s snap-rows/s mutation-sql-s changes-e2e-s chg-records/s poll-avg-ms poll-max-ms polled written rss-MB cpu-s
for r in "${results[@]}"; do
  read -r tr first snap snap_stream mutation_sql chg avg mx polled written rss cpu <<<"$r"
  snap_rate=$(awk -v n="$BENCH_ROWS" -v s="$snap" 'BEGIN {if (s > 0) printf "%.0f", n / s; else print "-"}')
  chg_rate=$(awk -v n="$BENCH_MUTATION_RECORDS" -v s="$chg" 'BEGIN {if (s ~ /^[0-9.]+$/ && s > 0) printf "%.0f", n / s; else print "-"}')
  rss_mb=$(awk -v k="$rss" 'BEGIN {printf "%.0f", k / 1024}')
  printf '%-13s %9s %14s %15s %11s %14s %13s %13s %13s %13s %9s %9s %9s %8s\n' \
    "$tr" "$first" "$snap" "$snap_stream" "$snap_rate" "$mutation_sql" "$chg" "$chg_rate" "$avg" "$mx" "$polled" "$written" "$rss_mb" "$cpu"
done
cat <<'EOF'

  first-rec-s       worker start to first record (plugin scan, preflight, first poll)
  snapshot-e2e-s    worker start to last snapshot record; snap-rows/s uses this duration
  snapshot-stream-s first to last snapshot record, excluding startup
  mutation-sql-s     duration of the StarRocks UPDATE itself
  changes-e2e-s     mutation start to last change record, including SQL and CDC delivery
  poll-*-ms     Connect's whole-run poll-batch-avg/max-time-ms, before conversion and produce
  rss/cpu       the worker process, peak RSS and total CPU seconds over the whole run
EOF
