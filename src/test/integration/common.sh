#!/usr/bin/env bash
# Shared by smoke.sh and smoke-cluster.sh -- only what is byte-identical between them.
# sr_sql, cleanup, start_worker and stop_worker share names but not bodies (containers
# vs local processes) and stay in their own scripts; see README.md.
# Sourced, not executed; the caller has set -euo pipefail.

# From this file's own location, so it holds wherever the entry script was invoked from.
_COMMON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$_COMMON_DIR/../../.." && pwd)"
JAR="$REPO_ROOT/target/starrocks-connector-for-kafka-1.0.5.jar"
PACKAGE_PLUGIN_DIR="$REPO_ROOT/target/starrocks-connector-for-kafka-1.0.5-package/share/java/starrocks-kafka-connector"
PLUGIN_ROOT="$REPO_ROOT/target/smoke-plugins"
PLUGIN_DIR="$PLUGIN_ROOT/starrocks-cdc"
OUT_DIR="$(mktemp -d)"
CONSUMED="$OUT_DIR/consumed.json"

# DATE and DATETIME arrive as text preserving the stored calendar fields, independent of
# the worker timezone: JDBC uses text/LocalDateTime and ADBC reads temporal vectors directly.
# A whole-hour shift means the local zone leaked into the read; a missing .123456 means
# the microseconds were cut.
TZ_DATE="2026-08-05"
TZ_DATETIME="2026-08-05 12:34:56.123456"
TZ_EXPECT_DATE="2026-08-05"
TZ_EXPECT_DATETIME="2026-08-05 12:34:56.123456"

# 38 nines: past INT64 by 19 digits, so a truncating mapping cannot produce it, yet inside
# Decimal128(38,0) -- which is what BE hands the Arrow Flight transport, and which LARGEINT's
# real ceiling of 2^127-1 would overflow.
BIG_INT=99999999999999999999999999999999999999

step() { printf '\n=== %s ===\n' "$1"; }
fail() { printf '\nFAIL: %s\n' "$1" >&2; exit 1; }
note() { printf '  %s\n' "$1"; }

# Read the exact runtime version from the direct dependency in pom.xml. Checking a fixed
# filename here goes stale when a driver is upgraded, even when the package contains it.
pom_dependency_version() {
  awk -v artifact="$1" '
    /<dependency>/ { in_dependency = 1; id = ""; version = "" }
    in_dependency && /<artifactId>/ {
      id = $0
      sub(/.*<artifactId>/, "", id)
      sub(/<\/artifactId>.*/, "", id)
    }
    in_dependency && /<version>/ {
      version = $0
      sub(/.*<version>/, "", version)
      sub(/<\/version>.*/, "", version)
      gsub(/[[:space:]]/, "", version)
    }
    /<\/dependency>/ {
      if (in_dependency && id == artifact && version != "") {
        print version
        found = 1
        exit
      }
      in_dependency = 0
    }
    END { if (!found) exit 1 }
  ' "$REPO_ROOT/pom.xml"
}

# Both harnesses. The container smoke test uses the default MySQL transport.
verify_plugin_jar() {
  local transports transport version read_driver="MariaDB JDBC"
  read -r -a transports <<< "${1:-mysql}"
  [ -f "$JAR" ] || fail "plugin jar not found at $JAR -- run: (cd $REPO_ROOT && mvn -DskipTests package)"
  jar tf "$JAR" | grep -Fx 'com/starrocks/connector/kafka/source/StarRocksCdcSourceConnector.class' >/dev/null \
    || fail "connector class missing from $JAR"
  [ -f "$PACKAGE_PLUGIN_DIR/mariadb-java-client-3.3.3.jar" ] \
    || fail "MariaDB JDBC driver missing from $PACKAGE_PLUGIN_DIR"
  [ -f "$PACKAGE_PLUGIN_DIR/debezium-core-1.8.1.Final.jar" ] \
    || fail "Debezium Core missing from $PACKAGE_PLUGIN_DIR"
  # Envelope's class initialization also needs these classes from Debezium Core.
  for cls in io/debezium/data/Envelope.class \
             io/debezium/pipeline/txmetadata/TransactionMonitor.class \
             io/debezium/util/SchemaNameAdjuster.class; do
    jar tf "$PACKAGE_PLUGIN_DIR/debezium-core-1.8.1.Final.jar" | grep -Fx "$cls" >/dev/null \
      || fail "$cls missing from the packaged Debezium Core jar"
  done
  # Without this filtered resource every version() answers "unknown".
  jar tf "$JAR" | grep -Fx 'starrocks-connector.properties' >/dev/null \
    || fail "starrocks-connector.properties missing from $JAR -- the <resources> filtering block in pom.xml is what puts it there"
  for transport in "${transports[@]}"; do
    case "$transport" in
      mysql) ;;
      arrow-flight)
        version=$(pom_dependency_version flight-sql-jdbc-core) \
          || fail "cannot find the Arrow Flight JDBC version in pom.xml"
        [ -f "$PACKAGE_PLUGIN_DIR/flight-sql-jdbc-core-$version.jar" ] \
          || fail "Arrow Flight JDBC driver $version missing from $PACKAGE_PLUGIN_DIR -- run: (cd $REPO_ROOT && mvn -DskipTests package)"
        read_driver="$read_driver + Arrow Flight JDBC"
        ;;
      arrow-adbc)
        version=$(pom_dependency_version adbc-driver-flight-sql) \
          || fail "cannot find the Arrow ADBC version in pom.xml"
        [ -f "$PACKAGE_PLUGIN_DIR/adbc-driver-flight-sql-$version.jar" ] \
          || fail "Arrow ADBC driver $version missing from $PACKAGE_PLUGIN_DIR -- run: (cd $REPO_ROOT && mvn -DskipTests package)"
        read_driver="$read_driver + Arrow ADBC"
        ;;
      *) fail "unknown plugin transport: $transport" ;;
    esac
  done
  note "plugin OK (connector + $read_driver + Debezium envelope + version resource)"
}

# Stage the packaged plugin directory, which contains the shaded project jar and its remaining
# runtime dependencies. target/ itself also contains alternate project jars and cannot be used.
stage_plugin_dir() {
  [ -f "$PACKAGE_PLUGIN_DIR/$(basename "$JAR")" ] \
    || fail "packaged plugin not found at $PACKAGE_PLUGIN_DIR -- run: (cd $REPO_ROOT && mvn -DskipTests package)"
  rm -rf "$PLUGIN_ROOT"
  mkdir -p "$PLUGIN_DIR"
  cp "$PACKAGE_PLUGIN_DIR"/*.jar "$PLUGIN_DIR/"
  note "staged packaged plugin jars in $PLUGIN_DIR"
}
