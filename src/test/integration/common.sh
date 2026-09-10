#!/usr/bin/env bash
# Shared by smoke.sh and smoke-cluster.sh -- only what is byte-identical between them.
# sr_sql, cleanup, start_worker and stop_worker share names but not bodies (containers
# vs local processes) and stay in their own scripts; see README.md.
# Sourced, not executed; the caller has set -euo pipefail.

# From this file's own location, so it holds wherever the entry script was invoked from.
_COMMON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$_COMMON_DIR/../../.." && pwd)"
JAR="$REPO_ROOT/target/starrocks-connector-for-kafka-1.0.5.jar"
PLUGIN_DIR="$REPO_ROOT/target/smoke-plugin"
OUT_DIR="$(mktemp -d)"
CONSUMED="$OUT_DIR/consumed.json"

# Timezone-independent on purpose: Connect's Date is days since the UTC epoch and
# Timestamp is UTC millis, so any worker must produce exactly these. A whole-hour
# deviation means the worker's local zone leaked into the read.
TZ_DATE="2026-08-05"
TZ_DATETIME="2026-08-05 12:34:56"
TZ_EXPECT_DAYS=20670
TZ_EXPECT_MILLIS=1785933296000

step() { printf '\n=== %s ===\n' "$1"; }
fail() { printf '\nFAIL: %s\n' "$1" >&2; exit 1; }
note() { printf '  %s\n' "$1"; }

# Both harnesses. The Arrow driver check is not here: only smoke-cluster.sh can select
# that transport, and it checks conditionally.
verify_plugin_jar() {
  [ -f "$JAR" ] || fail "plugin jar not found at $JAR -- run: (cd $REPO_ROOT && mvn -DskipTests package)"
  jar tf "$JAR" | grep -q 'com/starrocks/connector/kafka/source/StarRocksCdcSourceConnector.class' \
    || fail "connector class missing from $JAR"
  jar tf "$JAR" | grep -q 'org/mariadb/jdbc/Driver.class' \
    || fail "mariadb JDBC driver missing from $JAR -- the primary maven-shade execution must include org.mariadb.jdbc:mariadb-java-client"
  # Envelope's class-init also pulls these two. Unit tests run on the full compile classpath
  # and cannot see a missing shade include; it surfaces only here, or in a real worker.
  for cls in io/debezium/data/Envelope.class \
             io/debezium/pipeline/txmetadata/TransactionMonitor.class \
             io/debezium/util/SchemaNameAdjuster.class; do
    jar tf "$JAR" | grep -q "^$cls$" \
      || fail "$cls missing from $JAR -- the primary maven-shade execution must include io.debezium:debezium-core"
  done
  # Without this filtered resource every version() answers "unknown".
  jar tf "$JAR" | grep -q '^starrocks-connector.properties$' \
    || fail "starrocks-connector.properties missing from $JAR -- the <resources> filtering block in pom.xml is what puts it there"
  note "plugin jar OK (connector + JDBC driver + Debezium envelope + version resource)"
}

# Exactly one jar as the only plugin location. target/ must NOT be used directly: it also
# holds the -with-dependencies jar and original-*.jar (no JDBC driver -> IllegalStateException
# at startup), and Connect scans each as a separate plugin, so which one wins is arbitrary.
stage_plugin_dir() {
  rm -rf "$PLUGIN_DIR"
  mkdir -p "$PLUGIN_DIR"
  cp "$JAR" "$PLUGIN_DIR/"
  note "staged $(basename "$JAR") as the only plugin in $PLUGIN_DIR"
}
