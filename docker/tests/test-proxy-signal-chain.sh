#!/bin/sh

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Verifies the launch chain execs through to the JVM so a container SIGTERM
# reaches the Java process (its shutdown hook), rather than being absorbed by an
# intermediate shell. Two levels of assurance:
#   1. static: every hop's final launch line uses `exec`
#   2. dynamic: a fake runserver.sh + fake java prove the launcher PID is replaced
#      and a TERM to the launcher is delivered to the fake java.

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "${SCRIPT_DIR}/../.." && pwd)
BIN="${REPO_ROOT}/distribution/bin"
DOCKER_SCRIPTS="${REPO_ROOT}/docker/scripts"

FAILURES=0
pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1" >&2; FAILURES=$((FAILURES + 1)); }

# --- static assertions: final launch line of each hop must exec ---

assert_final_exec() {
  file="$1"
  pattern="$2"
  label="$3"
  line=$(grep -E "$pattern" "$file" | grep -v '^[[:space:]]*#' | tail -1)
  case "$line" in
    exec\ *) pass "$label execs: $line" ;;
    *) fail "$label does not exec (line: '$line')" ;;
  esac
}

assert_final_exec "${BIN}/mqproxy" 'runserver\.sh' "mqproxy -> runserver"
assert_final_exec "${BIN}/runserver.sh" '"\$JAVA"' "runserver.sh -> java"
assert_final_exec "${DOCKER_SCRIPTS}/runserver-customize.sh" '\$JAVA ' "runserver-customize.sh -> java"

# entrypoint already execs mqproxy; assert we did not regress it
if grep -qE '^\s*exec \./mqproxy' "${DOCKER_SCRIPTS}/docker-entrypoint.sh"; then
  pass "docker-entrypoint execs mqproxy"
else
  fail "docker-entrypoint no longer execs mqproxy"
fi

# --- dynamic assertion: TERM to the launcher reaches the final java process ---

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# Fake runserver.sh that (like the real one, after our fix) execs the fake java.
cat > "$WORK/runserver.sh" <<EOF
#!/bin/sh
exec "$WORK/java" "\$@"
EOF
chmod +x "$WORK/runserver.sh"

# Fake java: record its own PID, install a TERM handler, then wait.
cat > "$WORK/java" <<EOF
#!/bin/sh
echo \$\$ > "$WORK/java.pid"
trap 'echo term > "$WORK/java.termed"; exit 0' TERM
# Busy-wait so the process stays alive until signalled.
while true; do sleep 0.2; done
EOF
chmod +x "$WORK/java"

# A launcher that execs the fake runserver (mirrors mqproxy's fixed final line).
cat > "$WORK/launcher.sh" <<EOF
#!/bin/sh
exec sh "$WORK/runserver.sh" ProxyStartup
EOF
chmod +x "$WORK/launcher.sh"

sh "$WORK/launcher.sh" &
launcher_pid=$!

# Wait for the fake java to record its PID.
i=0
while [ ! -f "$WORK/java.pid" ] && [ $i -lt 50 ]; do
  sleep 0.1
  i=$((i + 1))
done

if [ ! -f "$WORK/java.pid" ]; then
  fail "fake java never started"
else
  java_pid=$(cat "$WORK/java.pid")
  # Because every hop execs, the launcher PID is reused by java: they are equal.
  if [ "$launcher_pid" = "$java_pid" ]; then
    pass "exec chain preserves PID ($launcher_pid == $java_pid)"
  else
    fail "PID not preserved: launcher=$launcher_pid java=$java_pid (a shell absorbed the signal)"
  fi

  # Send TERM to the launcher PID; with exec it is the java process.
  kill -TERM "$launcher_pid" 2>/dev/null || true
  i=0
  while [ ! -f "$WORK/java.termed" ] && [ $i -lt 50 ]; do
    sleep 0.1
    i=$((i + 1))
  done
  if [ -f "$WORK/java.termed" ]; then
    pass "TERM to launcher reached the java process"
  else
    fail "TERM to launcher did not reach java"
    kill -KILL "$launcher_pid" 2>/dev/null || true
  fi
fi

if [ "$FAILURES" -eq 0 ]; then
  echo "ALL SIGNAL-CHAIN TESTS PASSED"
  exit 0
fi
echo "$FAILURES signal-chain test(s) failed" >&2
exit 1
