#!/usr/bin/env bash
# Shared by smoke.sh (Docker) and smoke-cluster.sh (existing cluster).
#
# Only what is genuinely identical between the two lives here. Their sr_sql,
# cleanup, start_worker and stop_worker happen to share names but not bodies --
# one drives containers through `docker compose exec`, the other a local mysql
# client and a local process -- so those stay where they are. Pulling same-named
# but differently-implemented functions into one place would be a merge, not a
# de-duplication.
#
# Sourced, not executed: expects the caller to have set -euo pipefail.

# Resolved from this file's own location rather than the caller's $PWD, so it is
# right no matter where the entry script was invoked from.
_COMMON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$_COMMON_DIR/../../.." && pwd)"
JAR="$REPO_ROOT/target/starrocks-connector-for-kafka-1.0.5.jar"
PLUGIN_DIR="$REPO_ROOT/target/smoke-plugin"
OUT_DIR="$(mktemp -d)"
CONSUMED="$OUT_DIR/consumed.json"

# Temporal fixtures. The expected values are timezone-independent on purpose:
# Connect's Date logical type is days since the UTC epoch and Timestamp is UTC
# millis, so a worker in any zone must produce exactly these. A whole-hour
# deviation means the worker's local zone leaked into the JDBC read.
TZ_DATE="2026-08-05"
TZ_DATETIME="2026-08-05 12:34:56"
TZ_EXPECT_DAYS=20670
TZ_EXPECT_MILLIS=1785933296000

step() { printf '\n=== %s ===\n' "$1"; }
fail() { printf '\nFAIL: %s\n' "$1" >&2; exit 1; }
note() { printf '  %s\n' "$1"; }

# The packaging assertions that apply to both harnesses. The Arrow driver check
# is deliberately not here: only smoke-cluster.sh can select that transport, and
# it runs the check conditionally.
verify_plugin_jar() {
  [ -f "$JAR" ] || fail "plugin jar not found at $JAR -- run: (cd $REPO_ROOT && mvn -DskipTests package)"
  jar tf "$JAR" | grep -q 'com/starrocks/connector/kafka/source/StarRocksCdcSourceConnector.class' \
    || fail "connector class missing from $JAR"
  jar tf "$JAR" | grep -q 'org/mariadb/jdbc/Driver.class' \
    || fail "mariadb JDBC driver missing from $JAR -- the primary maven-shade execution must include org.mariadb.jdbc:mariadb-java-client"
  # ChangeRecordMapper builds every record with io.debezium.data.Envelope, whose class-init also
  # pulls TransactionMonitor and SchemaNameAdjuster. Unit tests run on the full compile classpath
  # and cannot see a missing shade include; it surfaces only here or as a NoClassDefFoundError in
  # a real worker.
  for cls in io/debezium/data/Envelope.class \
             io/debezium/pipeline/txmetadata/TransactionMonitor.class \
             io/debezium/util/SchemaNameAdjuster.class; do
    jar tf "$JAR" | grep -q "^$cls$" \
      || fail "$cls missing from $JAR -- the primary maven-shade execution must include io.debezium:debezium-core"
  done
  # The version the connector will report over Connect's REST API comes from this
  # filtered resource; without it every version() answers "unknown".
  jar tf "$JAR" | grep -q '^starrocks-connector.properties$' \
    || fail "starrocks-connector.properties missing from $JAR -- the <resources> filtering block in pom.xml is what puts it there"
  note "plugin jar OK (connector + JDBC driver + Debezium envelope + version resource)"
}

# Stage exactly one jar as the only plugin location. target/ itself must NOT be
# used: after `mvn package` it also holds the -with-dependencies jar, the
# original-*.jar (connector present, JDBC driver ABSENT -> IllegalStateException
# at worker startup), classes/ and the assembly dirs. Connect scans each of those
# as a separate plugin location, so which copy of the connector wins is arbitrary.
stage_plugin_dir() {
  rm -rf "$PLUGIN_DIR"
  mkdir -p "$PLUGIN_DIR"
  cp "$JAR" "$PLUGIN_DIR/"
  note "staged $(basename "$JAR") as the only plugin in $PLUGIN_DIR"
}
