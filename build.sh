#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Build this project against a chosen Flink version line.
#
# Usage:
#   ./build.sh 1.20        Flink 1.20.0 + Flink CDC 3.6.0-1.20                  [default]
#   ./build.sh 2.2         Flink 2.2.0  + Flink CDC 3.6.0-2.2
#   ./build.sh 1.20 test   run `test` instead of the default `verify`
#   ./build.sh all         build both lines to prove the source tree is version-agnostic
#
# Both lines use Flink CDC 3.x (package org.apache.flink.cdc.*), so there is no CDC compatibility
# shim; only dependency coordinates differ.
#
# Flag meanings:
#   -Pflink-1.20 / -Pflink-2.2   switch Flink, CDC and JDBC connector versions
#   -Pflink20 / -Pflink2         select the version-specific source set in flink-compat
#   -Pflink2-test-guava          work around upstream FLINK-39429 (Flink 2.x only)
#
# The source-set flag is needed separately from the version flag because Maven cannot switch a child
# module's source roots from the parent, and profile activation does not cascade.

set -euo pipefail

LINE="${1:-1.20}"
GOAL="${2:-verify}"

case "$LINE" in
  1.20)
    PROFILES="-Pflink-1.20"
    ;;
  2.2)
    # flink2-test-guava appends the CDC connector's guava31 shade to the test classpath; without it
    # the end-to-end test dies with NoClassDefFoundError on Flink 2.2. See FLINK-39429.
    PROFILES="-Pflink-2.2 -Pflink2 -Pflink2-test-guava"
    ;;
  all)
    echo "==> Building Flink 1.20.0 / CDC 3.6.0-1.20"
    ./mvnw -Pflink-1.20 "$GOAL"
    echo "==> Building Flink 2.2.0 / CDC 3.6.0-2.2"
    ./mvnw -Pflink-2.2 -Pflink2 -Pflink2-test-guava "$GOAL"
    echo "==> Both version lines built successfully."
    exit 0
    ;;
  *)
    echo "Unknown version line: $LINE" >&2
    echo "Expected one of: 1.20 | 2.2 | all" >&2
    exit 2
    ;;
esac

echo "==> mvnw $PROFILES $GOAL"
exec ./mvnw $PROFILES "$GOAL"
