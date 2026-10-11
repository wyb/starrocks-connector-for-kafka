#!/usr/bin/env bash
#
# End-to-end transport benchmark for the StarRocks CDC source connector against EXISTING
# StarRocks and Kafka clusters: scalar/complex data in separate tables per transport, the same
# worker settings, one connect-standalone run per shape/transport, and time to land in Kafka.
#
#   SR_HOST=fe-leader SR_USER=root SR_PASSWORD=secret \
#   KAFKA_BOOTSTRAP=broker1:9092 KAFKA_BIN=/opt/kafka/bin \
#   BENCH_ROWS=5000000 ./bench-cluster.sh
#
# Every knob:
#   SR_HOST / SR_PORT / SR_USER / SR_PASSWORD     as in smoke-cluster.sh (leader FE, OPERATE user)
#   SR_ARROW_PORT        FE arrow_flight_port                        (default: FE config)
#   KAFKA_BOOTSTRAP / KAFKA_BIN / KAFKA_EXTRA_PROPS / TOPIC_RF        as in smoke-cluster.sh
#   BENCH_TRANSPORTS     mysql, arrow-flight, arrow-adbc in run order (default all three)
#   BENCH_SHAPES         space-separated, scalar and/or complex      (default "scalar complex")
#   BENCH_ROWS           rows to generate                            (default 5000000)
#   BENCH_BUCKETS        table buckets                               (default 8)
#   BENCH_PARTITIONS     topic partitions                            (default 1)
#   BENCH_HEAP           worker -Xmx                              (default 4g)
#   BENCH_SNAPSHOT_BATCH_SIZE  records returned per snapshot poll    (default 4096)
#   BENCH_POLL_INTERVAL_MS  source.poll.interval.ms                  (default 2000)
#   BENCH_DAILY_UPDATE_ROWS  rows updated in the first window         (default 50000)
#   BENCH_BURST_UPDATE_ROWS  rows updated in the second window        (default 500000)
#                        Generated tables update disjoint id ranges; each UPDATE emits two
#                        CHANGES records per row. Their sum must not exceed BENCH_ROWS.
#   BENCH_MUTATION       Optional custom SQL, {db}/{table} substituted; replaces both default
#                        windows with one CHANGES phase. Empty skips CHANGES.
#                        Existing BENCH_DB/BENCH_TABLE runs default to a full-table UPDATE.
#   BENCH_MUTATION_RECORDS  expected records for a custom/full-table UPDATE (default 2*BENCH_ROWS)
#   BENCH_TIMEOUT_SEC    per phase                                   (default 3600)
#   BENCH_SETTLE_SEC     wait after each phase to detect extra records (default 2 poll intervals)
#   BENCH_DB / BENCH_TABLE  use an existing table for a SINGLE transport instead of cdc_bench;
#                        BENCH_ROWS is read with COUNT(*), the database is not dropped, and
#                        BENCH_MUTATION must fit that table's columns
#   BENCH_JMX_PORT       worker JMX port for the Connect metrics     (default 9999)
#   BENCH_READ_TIMINGS   enable connector read-stage DEBUG timing    (default 0; requires DEBUG logger)
#   KEEP_ON_FAILURE      set to 1 to keep the db and topics for triage
#
# Reports per transport: worker-start-to-last-snapshot, first-to-last-snapshot, and
# mutation-start-to-last-change durations and rows/s; Connect poll time from JMX, worker
# peak RSS and CPU. The CHANGES timeline aligns SQL, Task, Connect and Kafka milestones;
# BENCH_READ_TIMINGS also prints summed connector read stages. The generated tables have
# identical contents within each shape; run in both transport orders to estimate cache effects.
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
BENCH_TRANSPORTS="${BENCH_TRANSPORTS:-mysql arrow-flight arrow-adbc}"
BENCH_SHAPES="${BENCH_SHAPES:-scalar complex}"
BENCH_ROWS="${BENCH_ROWS:-5000000}"
BENCH_DAILY_UPDATE_ROWS="${BENCH_DAILY_UPDATE_ROWS:-50000}"
BENCH_BURST_UPDATE_ROWS="${BENCH_BURST_UPDATE_ROWS:-500000}"
BENCH_BUCKETS="${BENCH_BUCKETS:-8}"
BENCH_PARTITIONS="${BENCH_PARTITIONS:-1}"
BENCH_HEAP="${BENCH_HEAP:-4g}"
BENCH_SNAPSHOT_BATCH_SIZE="${BENCH_SNAPSHOT_BATCH_SIZE:-4096}"
BENCH_POLL_INTERVAL_MS="${BENCH_POLL_INTERVAL_MS:-2000}"
# An explicitly empty BENCH_MUTATION skips CHANGES. Unset uses two generated windows.
if [ "${BENCH_MUTATION+x}" = x ]; then
  mutation_mode=custom
else
  mutation_mode=default
  BENCH_MUTATION='UPDATE {db}.{table} SET v = v + 1'
fi
BENCH_TIMEOUT_SEC="${BENCH_TIMEOUT_SEC:-3600}"
BENCH_SETTLE_SEC="${BENCH_SETTLE_SEC:-}"
BENCH_JMX_PORT="${BENCH_JMX_PORT:-9999}"
BENCH_READ_TIMINGS="${BENCH_READ_TIMINGS:-0}"
# Every kafka-*.sh goes through kafka-run-class.sh, which binds JMX_PORT when it is in the
# environment; an exported one would make each tool invocation fight the worker for the port.
unset JMX_PORT

SUFFIX="$(date +%Y%m%d%H%M%S)_$$"
METRICS_DIR="$REPO_ROOT/target/bench-metrics-$SUFFIX"
if [ -n "${BENCH_DB:-}" ] && [ -n "${BENCH_TABLE:-}" ]; then
  DB="$BENCH_DB"; OWN_DB=0
else
  DB=cdc_bench; OWN_DB=1
fi
DB_CREATED=0

bench_table_for() {  # shape, transport
  if [ "$OWN_DB" = 1 ]; then
    printf 'perf_%s_%s\n' "$1" "${2//-/_}"
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
jmx_pid=""
topics=()
cleanup() {
  local rc=$? preserve_out=0 saved=0 log
  printf '\n=== cleanup ===\n'
  for p in "$sampler_pid" "$jmx_pid" "$worker_pid"; do
    [ -n "$p" ] || continue
    kill -0 "$p" 2>/dev/null || continue
    kill "$p" 2>/dev/null || true
    for _ in $(seq 1 15); do kill -0 "$p" 2>/dev/null || break; sleep 1; done
    kill -9 "$p" 2>/dev/null || true
  done
  if [ "$rc" -ne 0 ]; then
    if mkdir -p "$METRICS_DIR"; then
      for log in "$OUT_DIR"/connect-*.log "$OUT_DIR"/jmx-*.log "$OUT_DIR"/jmx-*.tsv; do
        [ -f "$log" ] || continue
        if cp "$log" "$METRICS_DIR/"; then saved=1; else preserve_out=1; fi
      done
      [ "$saved" = 0 ] || note "failure logs saved in $METRICS_DIR"
    else
      preserve_out=1
    fi
  fi
  if [ "$rc" -ne 0 ] && [ "${KEEP_ON_FAILURE:-}" = "1" ]; then
    note "KEEP_ON_FAILURE=1 -> leaving database $DB and topics ${topics[*]:-} in place; logs in $OUT_DIR"
    return
  fi
  if [ "$DB_CREATED" = 1 ] && [ -n "${SR_HOST:-}" ]; then
    sr_sql "DROP DATABASE IF EXISTS $DB;" 2>/dev/null || note "could not drop $DB — drop it manually"
  fi
  if [ -n "${KAFKA_BIN:-}" ] && [ -n "${KAFKA_BOOTSTRAP:-}" ]; then
    for t in "${topics[@]:-}"; do
      [ -n "$t" ] || continue
      "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --delete --topic "$t" >/dev/null 2>&1 \
        || note "could not delete topic $t — delete it manually"
    done
  fi
  if [ "$preserve_out" = 1 ]; then
    note "some logs could not be copied; original logs remain in $OUT_DIR"
  else
    rm -rf "$OUT_DIR"
  fi
  rm -rf "$PLUGIN_ROOT"
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
for n in "$BENCH_DAILY_UPDATE_ROWS" "$BENCH_BURST_UPDATE_ROWS"; do
  case "$n" in ''|*[!0-9]*) fail "BENCH_DAILY_UPDATE_ROWS and BENCH_BURST_UPDATE_ROWS must be positive integers" ;; esac
  [ "$n" -gt 0 ] || fail "BENCH_DAILY_UPDATE_ROWS and BENCH_BURST_UPDATE_ROWS must be positive"
done
if [ "$OWN_DB" = 1 ] && [ "$mutation_mode" = default ]; then
  [ "$(( BENCH_DAILY_UPDATE_ROWS + BENCH_BURST_UPDATE_ROWS ))" -le "$BENCH_ROWS" ] \
    || fail "daily + burst update rows exceed BENCH_ROWS; reduce the update sizes"
fi
case "$BENCH_SNAPSHOT_BATCH_SIZE" in ''|*[!0-9]*) fail "BENCH_SNAPSHOT_BATCH_SIZE must be a positive integer" ;; esac
[ "$BENCH_SNAPSHOT_BATCH_SIZE" -gt 0 ] || fail "BENCH_SNAPSHOT_BATCH_SIZE must be positive"
case "$BENCH_POLL_INTERVAL_MS" in ''|*[!0-9]*) fail "BENCH_POLL_INTERVAL_MS must be a positive integer" ;; esac
[ "$BENCH_POLL_INTERVAL_MS" -gt 0 ] || fail "BENCH_POLL_INTERVAL_MS must be positive"
[[ "$BENCH_READ_TIMINGS" = 0 || "$BENCH_READ_TIMINGS" = 1 ]] \
  || fail "BENCH_READ_TIMINGS must be 0 or 1"
if [ "$BENCH_READ_TIMINGS" = 1 ]; then read_timings_bool=true; else read_timings_bool=false; fi
if [ -z "$BENCH_SETTLE_SEC" ]; then
  BENCH_SETTLE_SEC=$(( (2 * BENCH_POLL_INTERVAL_MS + 999) / 1000 ))
fi
case "$BENCH_SETTLE_SEC" in ''|*[!0-9]*) fail "BENCH_SETTLE_SEC must be a non-negative integer" ;; esac
command -v mysql >/dev/null   || fail "mysql client not found on PATH"
command -v java  >/dev/null   || fail "java not found on PATH"
command -v javac >/dev/null   || fail "javac not found on PATH"
[ -x "$KAFKA_BIN/kafka-topics.sh" ]       || fail "$KAFKA_BIN/kafka-topics.sh not executable"
[ -x "$KAFKA_BIN/connect-standalone.sh" ] || fail "$KAFKA_BIN/connect-standalone.sh not executable"
[ -x "$KAFKA_BIN/kafka-run-class.sh" ]    || fail "$KAFKA_BIN/kafka-run-class.sh not executable"
verify_plugin_jar "$BENCH_TRANSPORTS"
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
    arrow-flight|arrow-adbc)
      afp=$(sr_config arrow_flight_port || true)
      [ -n "$afp" ] && [ "$afp" != "-1" ] || fail "arrow_flight_port is disabled on the FE (set it in fe.conf and be.conf, restart)"
      [ -n "$SR_ARROW_PORT" ] || SR_ARROW_PORT="$afp" ;;
    *) fail "BENCH_TRANSPORTS entries must be mysql, arrow-flight or arrow-adbc, got '$tr'" ;;
  esac
done
[ "$transport_count" -gt 0 ] || fail "BENCH_TRANSPORTS must name at least one transport"
if [ "$OWN_DB" = 0 ] && [ "$transport_count" -ne 1 ]; then
  fail "BENCH_DB/BENCH_TABLE can benchmark one transport only; comparisons need separate generated tables"
fi
if [ "$OWN_DB" = 1 ]; then
  run_shapes="$BENCH_SHAPES"
  shape_count=0
  seen_shapes=" "
  for shape in $run_shapes; do
    case "$shape" in scalar|complex) ;; *) fail "BENCH_SHAPES entries must be scalar or complex, got '$shape'" ;; esac
    case "$seen_shapes" in *" $shape "*) fail "BENCH_SHAPES repeats '$shape'" ;; esac
    seen_shapes="$seen_shapes$shape "
    shape_count=$((shape_count + 1))
  done
  [ "$shape_count" -gt 0 ] || fail "BENCH_SHAPES must name at least one shape"
else
  [ "${BENCH_SHAPES:-scalar complex}" = "scalar complex" ] \
    || fail "BENCH_SHAPES does not apply with BENCH_DB/BENCH_TABLE"
  run_shapes=existing
fi
"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --list >/dev/null 2>&1 \
  || fail "cannot reach Kafka at $KAFKA_BOOTSTRAP"
note "StarRocks and Kafka reachable; shapes: $run_shapes; transports: $BENCH_TRANSPORTS"
if [ "$OWN_DB" = 1 ] && [ "$mutation_mode" = default ]; then
  note "workload per table: $BENCH_ROWS snapshot rows; $(( BENCH_DAILY_UPDATE_ROWS * 2 )) daily and $(( BENCH_BURST_UPDATE_ROWS * 2 )) burst change records"
fi

# ------------------------------------------------------------------ the data --
step "1. data"
if [ "$OWN_DB" = 1 ]; then
  existing=$(sr_val "SELECT SCHEMA_NAME FROM information_schema.schemata WHERE SCHEMA_NAME = '$DB';")
  [ -z "$existing" ] || fail "$DB already exists; refusing to replace or remove it"
  sr_sql "CREATE DATABASE $DB;"
  DB_CREATED=1
  array_values="generate_series, generate_series + 1, generate_series + 2,
                generate_series + 3, generate_series + 4, generate_series + 5,
                generate_series + 6, generate_series + 7"
  json_text="concat('{\"id\":', generate_series, ',\"v\":\"v', generate_series, '\"}')"
  for shape in $run_shapes; do
    if [ "$shape" = complex ]; then
      extra_columns=", a ARRAY<INT>, m MAP<INT,BIGINT>, st STRUCT<x BIGINT, y VARCHAR(64)>, j JSON"
      extra_values=", [$array_values],
                     map{1:generate_series, 2:generate_series + 1, 3:generate_series + 2},
                     row(generate_series, concat('v', generate_series)),
                     parse_json($json_text)"
    else
      extra_columns=", a VARCHAR(128), m VARCHAR(128), st VARCHAR(128), j VARCHAR(128)"
      extra_values=", concat('[', generate_series, ',', generate_series + 1, ',',
                            generate_series + 2, ',', generate_series + 3, ',',
                            generate_series + 4, ',', generate_series + 5, ',',
                            generate_series + 6, ',', generate_series + 7, ']'),
                     concat('{1:', generate_series, ',2:', generate_series + 1,
                            ',3:', generate_series + 2, '}'),
                     concat('{\"x\":', generate_series, ',\"y\":\"v', generate_series, '\"}'),
                     json_string(parse_json($json_text))"
    fi
    for transport in $BENCH_TRANSPORTS; do
      TABLE=$(bench_table_for "$shape" "$transport")
      sr_sql "CREATE TABLE $DB.$TABLE (
                id BIGINT NOT NULL, v BIGINT, d DECIMAL(18,2), s VARCHAR(64), dt DATETIME$extra_columns)
              PRIMARY KEY(id) DISTRIBUTED BY HASH(id) BUCKETS $BENCH_BUCKETS
              PROPERTIES ('replication_num'='1', 'enable_change_data_capture'='true');"
      t0=$(date +%s)
      sr_sql "INSERT INTO $DB.$TABLE (id, v, d, s, dt, a, m, st, j)
              SELECT generate_series, generate_series * 7, generate_series / 100.0,
                     concat('v', generate_series),
                     date_add('2026-01-01 00:00:00', INTERVAL generate_series SECOND)$extra_values
              FROM TABLE(generate_series(1, $BENCH_ROWS));"
      actual=$(sr_val "SELECT COUNT(*) FROM $DB.$TABLE;")
      [ "$actual" = "$BENCH_ROWS" ] || fail "$DB.$TABLE has $actual rows; expected $BENCH_ROWS"
      note "generated $BENCH_ROWS rows into $DB.$TABLE in $(( $(date +%s) - t0 )) s"
    done
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
javac -d "$OUT_DIR" BenchJmxSampler.java
bench_log4j_opts="${KAFKA_LOG4J_OPTS:-}"
if [ "$BENCH_READ_TIMINGS" = 1 ] && [ -z "$bench_log4j_opts" ]; then
  if [ -f "$KAFKA_BIN/../config/connect-log4j2.yaml" ]; then
    cat > "$OUT_DIR/bench-log4j2.properties" <<'EOF'
status = error
name = CdcBench
appender.console.type = Console
appender.console.name = STDOUT
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = [%d{ISO8601}] %-5p %c - %m%n
rootLogger.level = info
rootLogger.appenderRef.console.ref = STDOUT
logger.jdbc.name = com.starrocks.connector.kafka.source.StarRocksJdbcClient
logger.jdbc.level = debug
logger.adbc.name = com.starrocks.connector.kafka.source.StarRocksAdbcClient
logger.adbc.level = debug
logger.task.name = com.starrocks.connector.kafka.source.StarRocksCdcSourceTask
logger.task.level = debug
EOF
    bench_log4j_opts="-Dlog4j2.configurationFile=file:$OUT_DIR/bench-log4j2.properties"
  else
    cat > "$OUT_DIR/bench-log4j.properties" <<'EOF'
log4j.rootLogger=INFO, stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=[%d{ISO8601}] %-5p %c - %m%n
log4j.logger.com.starrocks.connector.kafka.source.StarRocksJdbcClient=DEBUG
log4j.logger.com.starrocks.connector.kafka.source.StarRocksAdbcClient=DEBUG
log4j.logger.com.starrocks.connector.kafka.source.StarRocksCdcSourceTask=DEBUG
EOF
    bench_log4j_opts="-Dlog4j.configuration=file:$OUT_DIR/bench-log4j.properties"
  fi
fi

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

# Waits for exactly $2 records; prints first/start, target/first, target/start, and target time.
wait_for_records() {
  local topic="$1" target="$2" start="$3" first="" n
  while :; do
    n=$(end_offset_sum "$topic")
    if [ -z "$first" ] && [ "$n" -gt 0 ]; then first=$(now); fi
    if [ "$n" -gt "$target" ]; then
      fail "topic $topic has $n records, more than the expected $target; benchmark result is invalid"
    fi
    if [ "$n" -eq "$target" ]; then
      awk -v s="$start" -v f="$first" -v e="$(now)" \
        'BEGIN {printf "%.1f %.1f %.1f %.6f\n", f - s, e - f, e - s, e}'
      return 0
    fi
    if awk -v s="$start" -v e="$(now)" -v t="$BENCH_TIMEOUT_SEC" 'BEGIN {exit !(e - s > t)}'; then
      fail "timed out after ${BENCH_TIMEOUT_SEC}s waiting for $target records in $topic (have $n) — see $METRICS_DIR/connect-$run_id.log"
    fi
    kill -0 "$worker_pid" 2>/dev/null || { tail -40 "$OUT_DIR/connect-$run_id.log" >&2; fail "worker died — see $METRICS_DIR/connect-$run_id.log"; }
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

# The drain is the sampled time between the target poll count and target write count. It is
# only the tail after reading finishes; snapshot reading and Kafka writes overlap before then.
summarize_jmx_phase() {  # file, start, observed end, cumulative target
  awk -F '\t' -v start="$2" -v finish="$3" -v target="$4" -v settle="$BENCH_SETTLE_SEC" '
    NR == 1 || $1 < start || $1 > finish + settle {next}
    $2 != "-" {
      if ($2 + 0 >= target && !polled_at) polled_at = $1
    }
    $3 != "-" && $3 + 0 >= target && !written_at {written_at = $1}
    $4 != "-" {active_samples++; if ($4 + 0 > active_max) active_max = $4 + 0}
    $5 != "-" {queue_samples++; queue_sum += $5}
    $6 != "-" {queue_peak_samples++; if ($6 + 0 > queue_max) queue_max = $6 + 0}
    $7 != "-" {request_samples++; request_sum += $7}
    $8 != "-" {request_peak_samples++; if ($8 + 0 > request_max) request_max = $8 + 0}
    $9 != "-" {waiting_samples++; if ($9 + 0 > waiting_max) waiting_max = $9 + 0}
    $10 != "-" {
      if (!have_errors) {first_errors = $10 + 0; have_errors = 1}
      last_errors = $10 + 0
    }
    END {
      drain = polled_at && written_at ? sprintf("%.1f", written_at - polled_at) : "-"
      active = active_samples ? sprintf("%.0f", active_max) : "-"
      queue_avg = queue_samples ? sprintf("%.1f", queue_sum / queue_samples) : "-"
      queue_peak = queue_peak_samples ? sprintf("%.1f", queue_max) : "-"
      request_avg = request_samples ? sprintf("%.1f", request_sum / request_samples) : "-"
      request_peak = request_peak_samples ? sprintf("%.1f", request_max) : "-"
      waiting = waiting_samples ? sprintf("%.0f", waiting_max) : "-"
      errors = have_errors ? sprintf("%.0f", last_errors - first_errors) : "-"
      print drain, active, queue_avg, queue_peak, request_avg, request_peak, waiting, errors
    }' "$1"
}

jmx_target_time() {  # file, phase start, observed end, cumulative target, TSV column
  awk -F '\t' -v start="$2" -v finish="$3" -v target="$4" -v column="$5" \
      -v settle="$BENCH_SETTLE_SEC" '
    NR > 1 && $1 >= start && $1 <= finish + settle && $column != "-" && $column + 0 >= target {
      printf "%.3f\n", $1; found = 1; exit
    }
    END {if (!found) print "-"}' "$1"
}

task_window_times() {  # worker log, table, mutation start, observed Kafka end
  awk -v table="$2" -v start="$3" -v finish="$4" '
    function metric(key, line, pattern) {
      pattern = key "=[0-9]+"
      if (match(line, pattern)) return substr(line, RSTART + length(key) + 1,
                                               RLENGTH - length(key) - 1) + 0
      return 0
    }
    index($0, "CDC read task phase=changes table=" table " ") {
      s = metric("start_epoch_ms", $0)
      e = metric("end_epoch_ms", $0)
      if (s / 1000 >= start && s / 1000 <= finish && e >= s) {
        if (!first || s < first) first = s
        if (e > last) last = e
      }
    }
    END {if (first) printf "%.3f %.3f\n", first / 1000, last / 1000; else print "- -"}' "$1"
}

read_stage_summary() {  # worker log, phase, table; sum per-snapshot-batch Task timings
  awk -v phase="$2" -v table="$3" '
    function metric(key, line, pattern) {
      pattern = key "=[0-9.eE+-]+"
      if (match(line, pattern)) return substr(line, RSTART + length(key) + 1,
                                               RLENGTH - length(key) - 1) + 0
      return 0
    }
    function cell(value, present) {return present ? sprintf("%.1f", value) : "-"}
    index($0, "CDC read " phase " table=" table " ") {
      if (phase == "changes" && metric("rows", $0) == 0) next
      query += metric("query_ms", $0); next_ms += metric("next_ms", $0)
      decode += metric("decode_ms", $0); client++
    }
    index($0, "CDC read task phase=" phase " table=" table " ") {
      map_ms += metric("map_ms", $0); table_ms += metric("table_ms", $0); task++
    }
    END {print cell(query, client), cell(next_ms, client), cell(decode, client),
               cell(map_ms, task), cell(table_ms, task)}' "$1"
}

since_mutation() {  # event epoch seconds, mutation start epoch seconds
  awk -v event="$1" -v start="$2" 'BEGIN {
    if (event == "-") print "-"; else printf "%.1f\n", event - start
  }'
}

# ---------------------------------------------------------------- the runs --
results=()
change_results=()
jmx_results=()
read_results=()
change_timelines=()
for shape in $run_shapes; do
for transport in $BENCH_TRANSPORTS; do
  scenario="$shape/$transport"
  run_id="${shape}_${transport//-/_}"
  step "run: $scenario"
  TABLE=$(bench_table_for "$shape" "$transport")
  case "$transport" in
    mysql)        url="jdbc:mysql://$SR_HOST:$SR_PORT"; opens=""; read_transport=jdbc; adbc_uri="" ;;
    arrow-flight) url="jdbc:arrow-flight-sql://$SR_HOST:$SR_ARROW_PORT?useEncryption=false"
                  opens="--add-opens=java.base/java.nio=ALL-UNNAMED"; read_transport=jdbc; adbc_uri="" ;;
    arrow-adbc)  url="jdbc:mysql://$SR_HOST:$SR_PORT"
                  opens="--add-opens=java.base/java.nio=ALL-UNNAMED"
                  read_transport=arrow-adbc; adbc_uri="grpc+tcp://$SR_HOST:$SR_ARROW_PORT" ;;
  esac
  connector="sr-cdc-bench-$shape-$transport-$SUFFIX"
  prefix="bench-$shape-$transport"
  topic="$prefix.$DB.$TABLE"
  "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --create \
    --topic "$topic" --partitions "$BENCH_PARTITIONS" --replication-factor "$TOPIC_RF" >/dev/null \
    || fail "could not create fresh topic $topic; remove a leftover topic or wait for deletion to finish"
  topics+=("$topic")

  cat > "$OUT_DIR/worker-$run_id.properties" <<EOF
bootstrap.servers=$KAFKA_BOOTSTRAP
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false
value.converter.decimal.format=NUMERIC
offset.storage.file.filename=$OUT_DIR/offsets-$run_id
offset.flush.interval.ms=5000
plugin.path=$PLUGIN_ROOT
EOF
  [ -n "${KAFKA_EXTRA_PROPS:-}" ] && cat "$KAFKA_EXTRA_PROPS" >> "$OUT_DIR/worker-$run_id.properties"
  cat > "$OUT_DIR/source-$run_id.properties" <<EOF
name=$connector
connector.class=com.starrocks.connector.kafka.source.StarRocksCdcSourceConnector
tasks.max=1
starrocks.jdbc.url=$url
source.read.transport=$read_transport
starrocks.adbc.uri=$adbc_uri
starrocks.database.name=$DB
starrocks.username=$SR_USER
starrocks.password=$SR_PASSWORD
starrocks.table.names=$TABLE
source.topic.prefix=$prefix
source.poll.interval.ms=$BENCH_POLL_INTERVAL_MS
source.snapshot.batch.size=$BENCH_SNAPSHOT_BATCH_SIZE
source.read.timings.enabled=$read_timings_bool
EOF

  worker_started=$(now)
  KAFKA_HEAP_OPTS="-Xms$BENCH_HEAP -Xmx$BENCH_HEAP" JMX_PORT="$BENCH_JMX_PORT" \
  KAFKA_LOG4J_OPTS="$bench_log4j_opts" \
  KAFKA_OPTS="${KAFKA_OPTS:-} $opens" \
    "$KAFKA_BIN/connect-standalone.sh" "$OUT_DIR/worker-$run_id.properties" "$OUT_DIR/source-$run_id.properties" \
    >> "$OUT_DIR/connect-$run_id.log" 2>&1 &
  worker_pid=$!
  start_sampler "$worker_pid" "$OUT_DIR/sampler-$run_id"
  jmx_file="$OUT_DIR/jmx-$run_id.tsv"
  java -cp "$OUT_DIR" BenchJmxSampler "$BENCH_JMX_PORT" "$connector" \
    > "$jmx_file" 2> "$OUT_DIR/jmx-$run_id.log" &
  jmx_pid=$!
  if [ "$transport" = arrow-adbc ]; then
    note "worker pid=$worker_pid, heap=$BENCH_HEAP, jdbc=$url, adbc=$adbc_uri"
  else
    note "worker pid=$worker_pid, heap=$BENCH_HEAP, url=$url"
  fi

  timing=$(wait_for_records "$topic" "$BENCH_ROWS" "$worker_started")
  read -r to_first snap_stream_secs snap_total_secs snap_finished <<<"$timing"
  note "snapshot: first record in $to_first s; all $BENCH_ROWS in $snap_total_secs s from worker start ($snap_stream_secs s after first)"
  assert_count_stable "$topic" "$BENCH_ROWS"
  snap_jmx=$(summarize_jmx_phase "$jmx_file" "$worker_started" "$snap_finished" "$BENCH_ROWS")
  jmx_results+=("$scenario snapshot $snap_jmx")
  if [ "$BENCH_READ_TIMINGS" = 1 ]; then
    read_results+=("$scenario snapshot $(read_stage_summary "$OUT_DIR/connect-$run_id.log" snapshot "$DB.$TABLE")")
  fi

  change_phases=()
  if [ "$OWN_DB" = 1 ] && [ "$mutation_mode" = default ]; then
    change_phases+=("daily $BENCH_DAILY_UPDATE_ROWS $(( BENCH_DAILY_UPDATE_ROWS * 2 )) 0")
    change_phases+=("burst $BENCH_BURST_UPDATE_ROWS $(( BENCH_BURST_UPDATE_ROWS * 2 )) $BENCH_DAILY_UPDATE_ROWS")
  elif [ -n "$BENCH_MUTATION" ]; then
    change_phases+=("custom 0 $BENCH_MUTATION_RECORDS 0")
  fi
  target="$BENCH_ROWS"
  if [ "${#change_phases[@]}" -gt 0 ]; then
  for change_phase in "${change_phases[@]}"; do
    read -r phase_name update_rows phase_records range_start <<<"$change_phase"
    if [ "$phase_name" = custom ]; then
      sql="${BENCH_MUTATION//\{db\}/$DB}"; sql="${sql//\{table\}/$TABLE}"
    else
      range_end=$(( range_start + update_rows ))
      sql="UPDATE $DB.$TABLE SET v = v + 1 WHERE id > $range_start AND id <= $range_end"
    fi
    log_start=$(wc -l < "$OUT_DIR/connect-$run_id.log")
    mutation_started=$(now)
    sr_sql "$sql;"
    mutation_committed=$(now)
    mutation_sql_secs=$(awk -v s="$mutation_started" -v e="$mutation_committed" 'BEGIN {printf "%.1f", e - s}')
    target=$(( target + phase_records ))
    timing=$(wait_for_records "$topic" "$target" "$mutation_started")
    read -r _ _ chg_secs chg_finished <<<"$timing"
    note "$phase_name changes: $phase_records records in $chg_secs s from mutation start (SQL $mutation_sql_secs s)"
    assert_count_stable "$topic" "$target"
    change_results+=("$scenario $phase_name $phase_records $mutation_sql_secs $chg_secs")
    chg_jmx=$(summarize_jmx_phase "$jmx_file" "$mutation_started" "$chg_finished" "$target")
    jmx_results+=("$scenario changes/$phase_name $chg_jmx")
    poll_finished=$(jmx_target_time "$jmx_file" "$mutation_started" "$chg_finished" "$target" 2)
    write_finished=$(jmx_target_time "$jmx_file" "$mutation_started" "$chg_finished" "$target" 3)
    task_times="- -"
    if [ "$BENCH_READ_TIMINGS" = 1 ]; then
      log_end=$(wc -l < "$OUT_DIR/connect-$run_id.log")
      stage_log="$OUT_DIR/stage-$run_id-$phase_name.log"
      if [ "$log_end" -gt "$log_start" ]; then
        sed -n "$(( log_start + 1 )),${log_end}p" "$OUT_DIR/connect-$run_id.log" > "$stage_log"
      else
        : > "$stage_log"
      fi
      read_results+=("$scenario changes/$phase_name $(read_stage_summary "$stage_log" changes "$DB.$TABLE")")
      task_times=$(task_window_times "$stage_log" "$DB.$TABLE" "$mutation_started" "$chg_finished")
    fi
    read -r task_started task_finished <<<"$task_times"
    change_timelines+=("$scenario changes/$phase_name $mutation_started $mutation_committed $task_started $task_finished $poll_finished $write_finished $chg_finished")
  done
  fi

  metrics=$(jmx_metrics "$connector")
  read -r jmx_avg jmx_max jmx_polled jmx_written <<<"$metrics"
  kill "$jmx_pid" 2>/dev/null || true
  wait "$jmx_pid" 2>/dev/null || true
  jmx_pid=""
  [ "$(wc -l < "$jmx_file")" -gt 1 ] \
    || fail "no JMX source-task samples for $connector; see $OUT_DIR/jmx-$run_id.log"
  mkdir -p "$METRICS_DIR"
  cp "$jmx_file" "$METRICS_DIR/$run_id.tsv"
  cp "$OUT_DIR/jmx-$run_id.log" "$METRICS_DIR/$run_id-sampler.log"
  kill "$worker_pid"; for _ in $(seq 1 30); do kill -0 "$worker_pid" 2>/dev/null || break; sleep 1; done
  kill -9 "$worker_pid" 2>/dev/null || true
  wait "$worker_pid" 2>/dev/null || true
  if [ "$BENCH_READ_TIMINGS" = 1 ]; then
    cp "$OUT_DIR/connect-$run_id.log" "$METRICS_DIR/connect-$run_id.log"
  fi
  worker_pid=""
  sleep 1; kill "$sampler_pid" 2>/dev/null || true
  wait "$sampler_pid" 2>/dev/null || true; sampler_pid=""
  rss_kb=0; cpu_s=0
  [ -f "$OUT_DIR/sampler-$run_id" ] && read -r rss_kb cpu_s < "$OUT_DIR/sampler-$run_id"
  results+=("$scenario $to_first $snap_total_secs $snap_stream_secs $jmx_avg $jmx_max $jmx_polled $jmx_written $rss_kb $cpu_s")
done
done
note "raw JMX samples saved in $METRICS_DIR"
if [ "$BENCH_READ_TIMINGS" = 1 ]; then note "worker logs saved in $METRICS_DIR"; fi

if [ "$BENCH_READ_TIMINGS" = 1 ]; then
  step "Connector read stages (summed ms)"
  printf '%-22s %-14s %11s %11s %11s %11s %11s\n' scenario phase query next decode map table
  for r in "${read_results[@]}"; do
    read -r tr phase query next_ms decode map_ms table_ms <<<"$r"
    printf '%-22s %-14s %11s %11s %11s %11s %11s\n' \
      "$tr" "$phase" "$query" "$next_ms" "$decode" "$map_ms" "$table_ms"
  done
  note "CHANGES excludes empty reads; snapshot Task sums exclude gaps between batches; table includes read and map"
  note "next means ResultSet.next() for JDBC and Arrow batch loading for ADBC"
fi

if [ "${#change_timelines[@]}" -gt 0 ]; then
  step "CHANGES timeline (seconds from mutation start)"
  printf '%-22s %-14s %10s %12s %10s %10s %11s %11s\n' \
    scenario phase sql-done task-start task-end poll-total write-total kafka-offset
  timeline_file="$METRICS_DIR/change-timeline.tsv"
  printf 'scenario\tphase\tmutation_start_epoch_s\tsql_done_epoch_s\ttask_start_epoch_s\ttask_end_epoch_s\tpoll_total_epoch_s\twrite_total_epoch_s\tkafka_offset_epoch_s\n' > "$timeline_file"
  for r in "${change_timelines[@]}"; do
    read -r tr phase mutation_started mutation_committed task_started task_finished poll_finished write_finished chg_finished <<<"$r"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$tr" "$phase" "$mutation_started" "$mutation_committed" "$task_started" "$task_finished" \
      "$poll_finished" "$write_finished" "$chg_finished" >> "$timeline_file"
    printf '%-22s %-14s %10s %12s %10s %10s %11s %11s\n' \
      "$tr" "$phase" "$(since_mutation "$mutation_committed" "$mutation_started")" \
      "$(since_mutation "$task_started" "$mutation_started")" \
      "$(since_mutation "$task_finished" "$mutation_started")" \
      "$(since_mutation "$poll_finished" "$mutation_started")" \
      "$(since_mutation "$write_finished" "$mutation_started")" \
      "$(since_mutation "$chg_finished" "$mutation_started")"
  done
  note "task start/end require BENCH_READ_TIMINGS=1; JMX and Kafka offset targets are sampled"
fi

step "Connect/producer JMX (1 s samples)"
printf '%-22s %-14s %9s %12s %13s %13s %15s %15s %12s %8s\n' \
  scenario phase drain-s active-max queue-avg-ms queue-max-ms request-avg-ms request-max-ms waiting-max errors
for r in "${jmx_results[@]}"; do
  read -r tr phase drain active qavg qmax ravg rmax waiting errors <<<"$r"
  printf '%-22s %-14s %9s %12s %13s %13s %15s %15s %12s %8s\n' \
    "$tr" "$phase" "$drain" "$active" "$qavg" "$qmax" "$ravg" "$rmax" "$waiting" "$errors"
done
cat <<'EOF'
  drain-s       first sample with all records polled to first sample with all records written;
                snapshot reads and writes overlap, so this is only the tail drain
  active-max    largest sampled count of polled records not yet written to Kafka
  queue/request sampled producer rolling averages and maxima (ms); values may cross phase boundaries
  waiting-max   largest sampled producer waiting-threads; errors is the sampled error-total increase
  -             metric unavailable or the 1 s sampler missed the target transition
EOF

# ----------------------------------------------------------------- report --
step "report ($BENCH_ROWS snapshot rows per table; snapshot batch $BENCH_SNAPSHOT_BATCH_SIZE; poll interval ${BENCH_POLL_INTERVAL_MS} ms)"
printf '%-22s %9s %14s %15s %11s %13s %13s %9s %9s %9s %8s\n' \
  scenario first-rec-s snapshot-e2e-s snapshot-stream-s snap-rows/s poll-avg-ms poll-max-ms polled written rss-MB cpu-s
for r in "${results[@]}"; do
  read -r tr first snap snap_stream avg mx polled written rss cpu <<<"$r"
  snap_rate=$(awk -v n="$BENCH_ROWS" -v s="$snap" 'BEGIN {if (s > 0) printf "%.0f", n / s; else print "-"}')
  rss_mb=$(awk -v k="$rss" 'BEGIN {printf "%.0f", k / 1024}')
  printf '%-22s %9s %14s %15s %11s %13s %13s %9s %9s %9s %8s\n' \
    "$tr" "$first" "$snap" "$snap_stream" "$snap_rate" "$avg" "$mx" "$polled" "$written" "$rss_mb" "$cpu"
done
if [ "${#change_results[@]}" -gt 0 ]; then
  printf '\n'
  printf '%-22s %-8s %10s %14s %14s %13s\n' \
    scenario phase records mutation-sql-s changes-e2e-s chg-records/s
  for r in "${change_results[@]}"; do
    read -r tr phase records mutation_sql chg <<<"$r"
    chg_rate=$(awk -v n="$records" -v s="$chg" 'BEGIN {if (s > 0) printf "%.0f", n / s; else print "-"}')
    printf '%-22s %-8s %10s %14s %14s %13s\n' \
      "$tr" "$phase" "$records" "$mutation_sql" "$chg" "$chg_rate"
  done
fi
cat <<'EOF'

  first-rec-s       worker start to first record (plugin scan, preflight, first poll)
  snapshot-e2e-s    worker start to last snapshot record; snap-rows/s uses this duration
  snapshot-stream-s first to last snapshot record, excluding startup
  mutation-sql-s     duration of that phase's StarRocks UPDATE
  changes-e2e-s     that phase's mutation start to last change record, including SQL and CDC delivery
  poll-*-ms     Connect's whole-run poll-batch-avg/max-time-ms, before conversion and produce
  rss/cpu       the worker process, peak RSS and total CPU seconds over the whole run
EOF
