#!/usr/bin/env bash
# Benchmark scalar and complex values over JDBC and optional Java ADBC paths on an existing
# StarRocks cluster. BENCH_TRANSPORTS selects mysql, arrow-flight, or arrow-adbc.
# Owns cdc_read_bench and removes it on exit.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
SR_HOST="${SR_HOST:-}"
SR_PORT="${SR_PORT:-9030}"
SR_ARROW_PORT="${SR_ARROW_PORT:-9408}"
SR_USER="${SR_USER:-root}"
SR_PASSWORD="${SR_PASSWORD:-}"
BENCH_ROWS="${BENCH_ROWS:-1000000}"
BENCH_BUCKETS="${BENCH_BUCKETS:-8}"
BENCH_ROUNDS="${BENCH_ROUNDS:-5}"
BENCH_READ_TIMINGS="${BENCH_READ_TIMINGS:-0}"
BENCH_TRANSPORTS="${BENCH_TRANSPORTS:-mysql arrow-flight arrow-adbc}"
BENCH_DB="cdc_read_bench"

[ -n "$SR_HOST" ] || { echo 'SR_HOST is required' >&2; exit 1; }
command -v mysql >/dev/null || { echo 'mysql client not found on PATH' >&2; exit 1; }
command -v mvn >/dev/null || { echo 'mvn not found on PATH' >&2; exit 1; }
for setting in BENCH_ROWS BENCH_BUCKETS BENCH_ROUNDS; do
  value="${!setting}"
  case "$value" in ''|*[!0-9]*) echo "$setting must be a positive integer" >&2; exit 1 ;; esac
  [ "$value" -gt 0 ] || { echo "$setting must be a positive integer" >&2; exit 1; }
done
[ "$BENCH_ROUNDS" -gt 1 ] || { echo 'BENCH_ROUNDS must be at least 2 (first round is discarded)' >&2; exit 1; }
[[ "$BENCH_READ_TIMINGS" = 0 || "$BENCH_READ_TIMINGS" = 1 ]] || { echo 'BENCH_READ_TIMINGS must be 0 or 1' >&2; exit 1; }
selected_transports=' '
transport_count=0
transport_csv=''
for transport in $BENCH_TRANSPORTS; do
  case "$transport" in
    mysql|arrow-flight|arrow-adbc) ;;
    *) echo "BENCH_TRANSPORTS has unknown transport '$transport'" >&2; exit 1 ;;
  esac
  case "$selected_transports" in *" $transport "*) echo "BENCH_TRANSPORTS repeats '$transport'" >&2; exit 1 ;; esac
  selected_transports="$selected_transports$transport "
  transport_count=$((transport_count + 1))
  transport_csv="${transport_csv:+$transport_csv,}$transport"
done
[ "$transport_count" -gt 0 ] || { echo 'BENCH_TRANSPORTS must name at least one transport' >&2; exit 1; }

mysql_args=(-h "$SR_HOST" -P "$SR_PORT" -u "$SR_USER")
[ -n "$SR_PASSWORD" ] && mysql_args+=(-p"$SR_PASSWORD")
sr_sql() { mysql "${mysql_args[@]}" -e "$1"; }
sr_val() { mysql "${mysql_args[@]}" -N -B -e "$1"; }

existing=$(sr_val "SELECT SCHEMA_NAME FROM information_schema.schemata WHERE SCHEMA_NAME = '$BENCH_DB';")
[ -z "$existing" ] || {
  echo "$BENCH_DB already exists; refusing to replace or remove it" >&2
  exit 1
}

owned_db=0
cleanup() {
  local status=$?
  trap - EXIT
  if [ "$owned_db" = 1 ]; then
    echo "Cleaning up $BENCH_DB"
    if ! sr_sql "DROP DATABASE $BENCH_DB;"; then
      echo "Could not drop $BENCH_DB; remove it manually" >&2
      status=1
    fi
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

sr_sql "CREATE DATABASE $BENCH_DB;"
owned_db=1

array_values="generate_series, generate_series + 1, generate_series + 2,
              generate_series + 3, generate_series + 4, generate_series + 5,
              generate_series + 6, generate_series + 7"
json_text="concat('{\"id\":', generate_series, ',\"v\":\"v', generate_series, '\"}')"

for shape in scalar complex; do
  table="perf_$shape"
  extra_names=", a, m, st, j"
  if [ "$shape" = complex ]; then
    extra_columns=", a ARRAY<INT>, m MAP<INT,BIGINT>, st STRUCT<x BIGINT, y VARCHAR(64)>, j JSON"
    extra_values=", [$array_values],
                   map{1:generate_series, 2:generate_series + 1, 3:generate_series + 2},
                   row(generate_series, concat('v', generate_series)),
                   parse_json($json_text)"
  else
    # Match column order and approximate the text representation of the complex values.
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

  sr_sql "CREATE TABLE $BENCH_DB.$table (
            id BIGINT NOT NULL, v BIGINT, d DECIMAL(18,2), s VARCHAR(64), dt DATETIME$extra_columns)
          PRIMARY KEY(id) DISTRIBUTED BY HASH(id) BUCKETS $BENCH_BUCKETS
          PROPERTIES ('replication_num'='1', 'enable_change_data_capture'='true');"
  sr_sql "INSERT INTO $BENCH_DB.$table (id, v, d, s, dt$extra_names)
          SELECT generate_series, generate_series * 7, generate_series / 100.0,
                 concat('v', generate_series),
                 date_add('2026-01-01 00:00:00', INTERVAL generate_series SECOND)$extra_values
          FROM TABLE(generate_series(1, $BENCH_ROWS));"
  actual=$(sr_val "SELECT COUNT(*) FROM $BENCH_DB.$table;")
  [ "$actual" = "$BENCH_ROWS" ] || { echo "$table has $actual rows; expected $BENCH_ROWS" >&2; exit 1; }
  echo "Prepared $BENCH_DB.$table: $actual rows"
done

echo
echo "Running TransportBench for perf_scalar and perf_complex; transports: $BENCH_TRANSPORTS"
(
  cd "$REPO_ROOT"
  mvn -q test -Dtest=TransportBench \
    "-Dbench.mysql.url=jdbc:mysql://$SR_HOST:$SR_PORT" \
    "-Dbench.arrow.url=jdbc:arrow-flight-sql://$SR_HOST:$SR_ARROW_PORT?useEncryption=false" \
    "-Dbench.adbc.uri=grpc+tcp://$SR_HOST:$SR_ARROW_PORT" \
    "-Dbench.transports=$transport_csv" \
    "-Dbench.db=$BENCH_DB" "-Dbench.tables=perf_scalar,perf_complex" \
    "-Dbench.user=$SR_USER" "-Dbench.password=$SR_PASSWORD" "-Dbench.rounds=$BENCH_ROUNDS" \
    "-Dbench.read.timings=$([ "$BENCH_READ_TIMINGS" = 1 ] && echo true || echo false)" \
    '-Dbench.mutation.sql=UPDATE {db}.{table} SET v = v + 1'
)
