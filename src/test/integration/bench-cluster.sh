#!/usr/bin/env bash
#
# End-to-end transport benchmark for the StarRocks CDC source connector against EXISTING
# StarRocks and Kafka clusters: the same table, the same worker settings, one connect-standalone
# run per transport, and the time it takes for every record to land in Kafka.
#
#   SR_HOST=fe-leader SR_USER=root SR_PASSWORD=secret \
#   KAFKA_BOOTSTRAP=broker1:9092 KAFKA_BIN=/opt/kafka/bin \
#   BENCH_ROWS=1000000 ./bench-cluster.sh
#
# Every knob:
#   SR_HOST / SR_PORT / SR_USER / SR_PASSWORD     as in smoke-cluster.sh (leader FE, OPERATE user)
#   SR_ARROW_PORT        FE arrow_flight_port                        (default 9408)
#   KAFKA_BOOTSTRAP / KAFKA_BIN / KAFKA_EXTRA_PROPS / TOPIC_RF        as in smoke-cluster.sh
#   BENCH_TRANSPORTS     space-separated, run in this order          (default "mysql arrow-flight")
#   BENCH_ROWS           rows to generate                            (default 1000000)
#   BENCH_BUCKETS        table buckets                               (default 8)
#   BENCH_PARTITIONS     topic partitions                            (default 1)
#   BENCH_HEAP           worker -Xmx; the snapshot is one batch      (default 4g)
#   BENCH_POLL_INTERVAL_MS  source.poll.interval.ms                  (default 2000)
#   BENCH_MUTATION       SQL run after the snapshot, {db}/{table} substituted; empty skips
#                        the CHANGES phase        (default "UPDATE {db}.{table} SET v = v + 1")
#   BENCH_MUTATION_RECORDS  records the mutation is expected to emit (default 2*BENCH_ROWS:
#                        a PK update is a DELETE half and an INSERT half per row)
#   BENCH_TIMEOUT_SEC    per phase                                   (default 3600)
#   BENCH_DB / BENCH_TABLE  use an existing table instead of generating one; BENCH_ROWS is
#                        then read with COUNT(*), the database is not dropped, and
#                        BENCH_MUTATION must fit that table's columns
#   BENCH_JMX_PORT       worker JMX port for the Connect metrics     (default 9999)
#   KEEP_ON_FAILURE      set to 1 to keep the db and topics for triage
#
# What it reports per transport: worker start to first record, first record to the last
# snapshot record, the CHANGES phase from the mutation's commit to the last record, rows/s
# for both, the worker's poll-batch-avg/max-time-ms and record totals from JMX, and the
# worker's peak RSS and CPU seconds. Everything in the pipeline after poll() -- JSON
# conversion, the producer, the broker -- is identical on both sides, so a small gap here
# with a large gap in TransportBench means the pipeline, not the transport, is the bottleneck.
#
set -euo pipefail
export LC_NUMERIC=C

cd "$(dirname "$0")"
# shellcheck source=common.sh
. ./common.sh

SR_PORT="${SR_PORT:-9030}"
SR_USER="${SR_USER:-root}"
SR_PASSWORD="${SR_PASSWORD:-}"
SR_ARROW_PORT="${SR_ARROW_PORT:-9408}"
TOPIC_RF="${TOPIC_RF:-1}"
BENCH_TRANSPORTS="${BENCH_TRANSPORTS:-mysql arrow-flight}"
BENCH_ROWS="${BENCH_ROWS:-1000000}"
BENCH_BUCKETS="${BENCH_BUCKETS:-8}"
BENCH_PARTITIONS="${BENCH_PARTITIONS:-1}"
BENCH_HEAP="${BENCH_HEAP:-4g}"
BENCH_POLL_INTERVAL_MS="${BENCH_POLL_INTERVAL_MS:-2000}"
# An explicitly empty BENCH_MUTATION skips the CHANGES phase; unset means the default.
if [ "${BENCH_MUTATION+x}" != x ]; then BENCH_MUTATION='UPDATE {db}.{table} SET v = v + 1'; fi
BENCH_TIMEOUT_SEC="${BENCH_TIMEOUT_SEC:-3600}"
BENCH_JMX_PORT="${BENCH_JMX_PORT:-9999}"
# Every kafka-*.sh goes through kafka-run-class.sh, which binds JMX_PORT when it is in the
# environment; an exported one would make each tool invocation fight the worker for the port.
unset JMX_PORT

SUFFIX="$(date +%Y%m%d%H%M%S)_$$"
if [ -n "${BENCH_DB:-}" ] && [ -n "${BENCH_TABLE:-}" ]; then
  DB="$BENCH_DB"; TABLE="$BENCH_TABLE"; OWN_DB=0
else
  DB="cdc_bench_${SUFFIX}"; TABLE=perf; OWN_DB=1
fi

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
for tr in $BENCH_TRANSPORTS; do
  case "$tr" in
    mysql) ;;
    arrow-flight)
      afp=$(sr_config arrow_flight_port || true)
      [ -n "$afp" ] && [ "$afp" != "-1" ] || fail "arrow_flight_port is disabled on the FE (set it in fe.conf and be.conf, restart)"
      jar tf "$JAR" | grep -q 'org/apache/arrow/driver/jdbc/ArrowFlightJdbcDriver.class' \
        || fail "Arrow Flight JDBC driver missing from $JAR" ;;
    *) fail "BENCH_TRANSPORTS entries must be mysql or arrow-flight, got '$tr'" ;;
  esac
done
"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --list >/dev/null 2>&1 \
  || fail "cannot reach Kafka at $KAFKA_BOOTSTRAP"
note "StarRocks and Kafka reachable; transports: $BENCH_TRANSPORTS"

# ------------------------------------------------------------------ the data --
step "1. data"
if [ "$OWN_DB" = 1 ]; then
  sr_sql "CREATE DATABASE $DB;"
  # Scalars, a DATETIME and one ARRAY: representative, not exhaustive. Shape-specific gaps
  # (nested types, VARBINARY) are TransportBench's job; this run measures the pipeline.
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
  note "generated $BENCH_ROWS rows into $DB.$TABLE in $(( $(date +%s) - t0 )) s"
else
  BENCH_ROWS=$(sr_val "SELECT COUNT(*) FROM $DB.$TABLE;")
  note "using existing $DB.$TABLE with $BENCH_ROWS rows"
fi
BENCH_MUTATION_RECORDS="${BENCH_MUTATION_RECORDS:-$(( BENCH_ROWS * 2 ))}"
# Warm the page cache once so the first transport does not pay for the cold read alone.
sr_val "SELECT COUNT(*) FROM $DB.$TABLE;" >/dev/null

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

# Waits until the topic holds $2 records; prints "<seconds to first record> <seconds to target>"
# measured from now. Fails past BENCH_TIMEOUT_SEC.
wait_for_records() {
  local topic="$1" target="$2" start first="" n
  start=$(now)
  while :; do
    n=$(end_offset_sum "$topic")
    if [ -z "$first" ] && [ "$n" -gt 0 ]; then first=$(now); fi
    if [ "$n" -ge "$target" ]; then
      awk -v s="$start" -v f="$first" -v e="$(now)" 'BEGIN {printf "%.1f %.1f\n", f - s, e - f}'
      return 0
    fi
    if awk -v s="$start" -v e="$(now)" -v t="$BENCH_TIMEOUT_SEC" 'BEGIN {exit !(e - s > t)}'; then
      fail "timed out after ${BENCH_TIMEOUT_SEC}s waiting for $target records in $topic (have $n) — see $OUT_DIR/connect-*.log"
    fi
    kill -0 "$worker_pid" 2>/dev/null || { tail -40 "$OUT_DIR/connect-$transport.log"; fail "worker died — see $OUT_DIR/connect-$transport.log"; }
    sleep 1
  done
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
EOF

  KAFKA_HEAP_OPTS="-Xms$BENCH_HEAP -Xmx$BENCH_HEAP" JMX_PORT="$BENCH_JMX_PORT" \
  KAFKA_OPTS="${KAFKA_OPTS:-} $opens" \
    "$KAFKA_BIN/connect-standalone.sh" "$OUT_DIR/worker-$transport.properties" "$OUT_DIR/source-$transport.properties" \
    >> "$OUT_DIR/connect-$transport.log" 2>&1 &
  worker_pid=$!
  start_sampler "$worker_pid" "$OUT_DIR/sampler-$transport"
  note "worker pid=$worker_pid, heap=$BENCH_HEAP, url=$url"

  timing=$(wait_for_records "$topic" "$BENCH_ROWS")
  read -r to_first snap_secs <<<"$timing"
  note "snapshot: $to_first s until the first record, then $BENCH_ROWS rows in $snap_secs s"

  chg_secs="-"
  if [ -n "$BENCH_MUTATION" ]; then
    sql="${BENCH_MUTATION//\{db\}/$DB}"; sql="${sql//\{table\}/$TABLE}"
    sr_sql "$sql;"
    timing=$(wait_for_records "$topic" "$(( BENCH_ROWS + BENCH_MUTATION_RECORDS ))")
    read -r _ chg_secs <<<"$timing"
    note "changes: $BENCH_MUTATION_RECORDS records in $chg_secs s after the mutation committed (includes up to one poll interval of waiting)"
  fi

  metrics=$(jmx_metrics "$connector")
  read -r jmx_avg jmx_max jmx_polled jmx_written <<<"$metrics"
  kill "$worker_pid"; for _ in $(seq 1 30); do kill -0 "$worker_pid" 2>/dev/null || break; sleep 1; done
  kill -9 "$worker_pid" 2>/dev/null || true
  worker_pid=""
  sleep 1; kill "$sampler_pid" 2>/dev/null || true; sampler_pid=""
  rss_kb=0; cpu_s=0
  [ -f "$OUT_DIR/sampler-$transport" ] && read -r rss_kb cpu_s < "$OUT_DIR/sampler-$transport"
  results+=("$transport $to_first $snap_secs $chg_secs $jmx_avg $jmx_max $jmx_polled $jmx_written $rss_kb $cpu_s")
done

# ----------------------------------------------------------------- report --
step "report ($BENCH_ROWS rows; changes phase expects $BENCH_MUTATION_RECORDS records; poll interval ${BENCH_POLL_INTERVAL_MS} ms)"
printf '%-13s %9s %11s %10s %11s %10s %13s %13s %9s %9s %9s %8s\n' \
  transport first-rec-s snapshot-s snap-rows/s changes-s chg-rows/s poll-avg-ms poll-max-ms polled written rss-MB cpu-s
for r in "${results[@]}"; do
  read -r tr first snap chg avg mx polled written rss cpu <<<"$r"
  snap_rate=$(awk -v n="$BENCH_ROWS" -v s="$snap" 'BEGIN {if (s > 0) printf "%.0f", n / s; else print "-"}')
  chg_rate=$(awk -v n="$BENCH_MUTATION_RECORDS" -v s="$chg" 'BEGIN {if (s ~ /^[0-9.]+$/ && s > 0) printf "%.0f", n / s; else print "-"}')
  rss_mb=$(awk -v k="$rss" 'BEGIN {printf "%.0f", k / 1024}')
  printf '%-13s %9s %11s %10s %11s %10s %13s %13s %9s %9s %9s %8s\n' \
    "$tr" "$first" "$snap" "$snap_rate" "$chg" "$chg_rate" "$avg" "$mx" "$polled" "$written" "$rss_mb" "$cpu"
done
cat <<'EOF'

  first-rec-s   worker start to the first record in the topic (plugin scan, preflight, first poll)
  snapshot-s    first record to the last snapshot record; rows/s from it
  changes-s     mutation commit to the last change record; includes up to one poll interval of idle wait
  poll-*-ms     Connect's own poll-batch-avg/max-time-ms: what poll() cost, before conversion and produce
  rss/cpu       the worker process, peak RSS and total CPU seconds over the whole run
EOF
