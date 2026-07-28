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

# Exercises mqproxyctl against a fake curl so its argv, exit codes and body
# handling are verified without a running admin server.

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
CTL="${SCRIPT_DIR}/../../distribution/bin/mqproxyctl"
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

FAILURES=0
pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1" >&2; FAILURES=$((FAILURES + 1)); }

# A fake curl that echoes a scripted body to the -o file and prints a scripted
# HTTP code. Driven by env vars FAKE_CODE / FAKE_BODY so each case controls it.
make_fake_curl() {
  cat > "$WORK/curl" <<'EOF'
#!/bin/sh
out=""
prev=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then out="$a"; fi
  prev="$a"
done
[ -n "$out" ] && printf '%s' "${FAKE_BODY:-}" > "$out"
printf '%s' "${FAKE_CODE:-200}"
EOF
  chmod +x "$WORK/curl"
}
make_fake_curl
export CURL="$WORK/curl"

# --- version: json shape and text shape, no server needed ---
out=$("$CTL" version --output json)
case "$out" in
  *'"apiVersion":"v1"'*) case "$out" in
    *'"drainCapable":true'*) pass "version json has apiVersion and drainCapable" ;;
    *) fail "version json missing drainCapable: $out" ;;
  esac ;;
  *) fail "version json missing apiVersion: $out" ;;
esac

out=$("$CTL" version)
case "$out" in
  *"apiVersion=v1"*) pass "version text form" ;;
  *) fail "version text form: $out" ;;
esac

# --- drain: 202 with runId, no --wait => success and body echoed ---
FAKE_CODE=202 FAKE_BODY='{"runId":"admin-1"}' "$CTL" drain > "$WORK/o" 2>&1 && rc=0 || rc=$?
if [ "$rc" -eq 0 ] && grep -q 'admin-1' "$WORK/o"; then
  pass "drain 202 succeeds and echoes runId"
else
  fail "drain 202 rc=$rc body=$(cat "$WORK/o")"
fi

# --- drain: 200 also success ---
FAKE_CODE=200 FAKE_BODY='{"runId":"admin-2"}' "$CTL" drain >/dev/null 2>&1 && rc=0 || rc=$?
[ "$rc" -eq 0 ] && pass "drain 200 succeeds" || fail "drain 200 rc=$rc"

# --- drain: 409 (lifecycle off) is a hard failure ---
FAKE_CODE=409 FAKE_BODY='{"error":"off"}' "$CTL" drain >/dev/null 2>&1 && rc=0 || rc=$?
[ "$rc" -ne 0 ] && pass "drain 409 fails non-zero (rc=$rc)" || fail "drain 409 should fail"

# --- drain: 503 is a hard failure ---
FAKE_CODE=503 FAKE_BODY='' "$CTL" drain >/dev/null 2>&1 && rc=0 || rc=$?
[ "$rc" -ne 0 ] && pass "drain 503 fails non-zero (rc=$rc)" || fail "drain 503 should fail"

# --- drain --wait: graceful terminal states succeed ---
FAKE_CODE=200 FAKE_BODY='{"runId":"admin-drained","state":"DRAINED"}' \
  "$CTL" drain --wait --timeout 1 >"$WORK/o" 2>"$WORK/e" && rc=0 || rc=$?
if [ "$rc" -eq 0 ] && grep -q '"state":"DRAINED"' "$WORK/o"; then
  pass "drain --wait succeeds for DRAINED"
else
  fail "drain --wait DRAINED rc=$rc stdout=$(cat "$WORK/o") stderr=$(cat "$WORK/e")"
fi

FAKE_CODE=200 FAKE_BODY='{"runId":"admin-stopped","state":"STOPPED"}' \
  "$CTL" drain --wait --timeout 1 >"$WORK/o" 2>"$WORK/e" && rc=0 || rc=$?
if [ "$rc" -eq 0 ] && grep -q '"state":"STOPPED"' "$WORK/o"; then
  pass "drain --wait succeeds for STOPPED"
else
  fail "drain --wait STOPPED rc=$rc stdout=$(cat "$WORK/o") stderr=$(cat "$WORK/e")"
fi

# --- drain --wait: forced termination must be visible to automation ---
FAKE_CODE=200 FAKE_BODY='{"runId":"admin-forced","state":"FORCE_DRAINING"}' \
  "$CTL" drain --wait --timeout 1 >"$WORK/o" 2>"$WORK/e" && rc=0 || rc=$?
if [ "$rc" -eq 6 ] && grep -qi 'forced' "$WORK/e"; then
  pass "drain --wait FORCE_DRAINING fails explicitly (rc=$rc)"
else
  fail "drain --wait FORCE_DRAINING rc=$rc stdout=$(cat "$WORK/o") stderr=$(cat "$WORK/e")"
fi

FAKE_CODE=200 FAKE_BODY='{"runId":"admin-forced-stopped","state":"STOPPED","forced":true}' \
  "$CTL" drain --wait --timeout 1 >"$WORK/o" 2>"$WORK/e" && rc=0 || rc=$?
if [ "$rc" -eq 6 ] && grep -qi 'forced' "$WORK/e"; then
  pass "drain --wait preserves forced failure after STOPPED (rc=$rc)"
else
  fail "drain --wait forced STOPPED rc=$rc stdout=$(cat "$WORK/o") stderr=$(cat "$WORK/e")"
fi

# --- status: 200 prints body ---
FAKE_CODE=200 FAKE_BODY='{"state":"READY"}' "$CTL" status > "$WORK/o" 2>&1 && rc=0 || rc=$?
if [ "$rc" -eq 0 ] && grep -q 'READY' "$WORK/o"; then
  pass "status 200 prints state"
else
  fail "status 200 rc=$rc body=$(cat "$WORK/o")"
fi

# --- status: 403 fails non-zero ---
FAKE_CODE=403 FAKE_BODY='{"error":"loopback only"}' "$CTL" status >/dev/null 2>&1 && rc=0 || rc=$?
[ "$rc" -ne 0 ] && pass "status 403 fails non-zero (rc=$rc)" || fail "status 403 should fail"

# --- unknown subcommand => usage + non-zero ---
"$CTL" bogus >/dev/null 2>&1 && rc=0 || rc=$?
[ "$rc" -ne 0 ] && pass "unknown subcommand fails non-zero (rc=$rc)" || fail "unknown subcommand should fail"

if [ "$FAILURES" -eq 0 ]; then
  echo "ALL MQPROXYCTL TESTS PASSED"
  exit 0
fi
echo "$FAILURES mqproxyctl test(s) failed" >&2
exit 1
