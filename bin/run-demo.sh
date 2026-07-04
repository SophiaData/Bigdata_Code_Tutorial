#!/usr/bin/env bash
# bin/run-demo.sh - run a flink-demo main class with the full runtime classpath, including
# <scope>provided</scope> Flink jars that IDEA's "Run" action skips (causing a silent exit-0
# due to NoClassDefFoundError on Flink internals).
#
# Usage: bin/run-demo.sh <fully.qualified.MainClass>
# Example: bin/run-demo.sh io.sophiadata.flink.streaming.IncrementMapFunction
#
# Implementation: builds the test-scope classpath via Maven dependency:build-classpath,
# then invokes java directly with target/classes + that classpath. This matches what
# Flink unit tests see and avoids the gotchas of exec:java not honoring provided scope.

set -euo pipefail

if [ "$#" -lt 1 ]; then
    echo "usage: $(basename "$0") <fqn-main-class>" >&2
    echo "       $(basename "$0") io.sophiadata.flink.streaming.IncrementMapFunction" >&2
    exit 2
fi

# Force JDK 11 regardless of what the caller has JAVA_HOME set to. The shell often has
# JAVA_HOME=zulu-8 from older setups, and using JDK 8 causes NoClassDefFoundError at
# startup (Flink classes are compiled to class file 55.0).
DEFAULT_JAVA_HOME=""
if [ ! -x "$DEFAULT_JAVA_HOME/bin/java" ]; then
    echo "error: default JDK 11 not found at $DEFAULT_JAVA_HOME" >&2
    echo "       export JAVA_HOME=/path/to/jdk11 before running this script" >&2
    exit 2
fi
export JAVA_HOME="$DEFAULT_JAVA_HOME"
export PATH="$JAVA_HOME/bin:$PATH"

cd "$(dirname "$0")/.."

CLASS="$1"
shift || true

# Make sure the module is compiled (no-op if already up to date).
./mvnw -pl flink-demo -am -q compile test-compile

# Build a test-scope classpath that includes compile + runtime + test + provided.
CP_FILE="$(mktemp -t flink-demo-cp.XXXXXX)"
trap 'rm -f "$CP_FILE"' EXIT
./mvnw -pl flink-demo dependency:build-classpath \
    -DincludeScope=test \
    -Dmdep.outputFile="$CP_FILE" \
    -q

CP="flink-demo/target/classes:flink-demo/target/test-classes:$(cat "$CP_FILE")"
exec "$JAVA_HOME/bin/java" -cp "$CP" "$CLASS" "$@"