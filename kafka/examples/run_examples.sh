#!/usr/bin/env bash
#
# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
#
# Start the example server, produce records, and check that they are consumed.
#
# Usage:
#   ./run_examples.sh                  # same as "service" with trpc_python.yaml
#   ./run_examples.sh service [conf]   # start trpc_main.py, run checks, stop the server
#   ./run_examples.sh batch [conf]     # same as service, but trpc_main.py --batch
#   ./run_examples.sh modes            # "service" once per process/thread model config
#   ./run_examples.sh all              # modes, then batch with trpc_python.yaml
#   ./run_examples.sh list             # list example names and config files
#
# A conf path is a file name under this directory, or an absolute path.
# Kafka must already be listening on 127.0.0.1:9092. The Greeter listens on
# 8001. The Kafka service does not bind 8000.
#
# Environment:
#   PYTHON       interpreter to use (default: ../.venv/bin/python, then ../venv/bin/python, then python3)
#   RPC_NUM      concurrent SayHello calls (default: 20)
#   PRODUCE_NUM  records kafka_client.py sends (default: 20)
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# Package root first. A sibling checkout of the SDK (../trpc_sdk) is inserted
# ahead of site-packages so this example uses that source.
PYTHONPATH="${PKG_DIR}${PYTHONPATH:+:${PYTHONPATH}}"
SDK_PARENT="$(cd "${PKG_DIR}/.." && pwd)"
if [[ -f "${SDK_PARENT}/trpc_sdk/http/__init__.py" ]]; then
  PYTHONPATH="${SDK_PARENT}:${PYTHONPATH}"
fi
export PYTHONPATH

if [[ -n "${PYTHON:-}" ]]; then
  PY="${PYTHON}"
elif [[ -x "${PKG_DIR}/.venv/bin/python" ]]; then
  PY="${PKG_DIR}/.venv/bin/python"
elif [[ -x "${PKG_DIR}/venv/bin/python" ]]; then
  PY="${PKG_DIR}/venv/bin/python"
else
  PY="$(command -v python3)"
fi

SERVER_LOG="${SCRIPT_DIR}/.server_run.log"
CONSUMED_LOG="${SCRIPT_DIR}/.consumed.log"
export TRPC_KAFKA_CONSUMED_LOG="${CONSUMED_LOG}"

SERVER_PID=""
HOST="127.0.0.1"
PORT=8001
BROKER_PORT=9092
RPC_NUM="${RPC_NUM:-20}"
PRODUCE_NUM="${PRODUCE_NUM:-20}"

CONFIGS=(
  trpc_python.yaml
  trpc_python_merge_threads.yaml
  trpc_python_separate_thread.yaml
  trpc_python_separate.yaml
  trpc_python_merge_processes.yaml
)

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*" >&2; }
ok()  { printf '\033[1;32m[PASS]\033[0m %s\n' "$*" >&2; }
bad() { printf '\033[1;31m[FAIL]\033[0m %s\n' "$*" >&2; }

port_open() {
  local port="$1"
  "${PY}" -c "import socket; s=socket.create_connection(('${HOST}', ${port}), 0.2); s.close()" >/dev/null 2>&1
}

require_broker() {
  if port_open "${BROKER_PORT}"; then
    return 0
  fi
  bad "Kafka is not listening on ${HOST}:${BROKER_PORT}"
  return 1
}

wait_ready() {
  local tries=60
  while (( tries-- > 0 )); do
    port_open "${PORT}" && return 0
    if [[ -n "${SERVER_PID}" ]] && ! kill -0 "${SERVER_PID}" 2>/dev/null; then
      return 1
    fi
    sleep 0.5
  done
  return 1
}

wait_port_free() {
  local tries=20
  while (( tries-- > 0 )); do
    port_open "${PORT}" || return 0
    sleep 0.5
  done
  return 1
}

resolve_conf() {
  local conf="${1:-${SCRIPT_DIR}/trpc_python.yaml}"
  if [[ "${conf}" != /* ]]; then
    if [[ -f "${SCRIPT_DIR}/${conf}" ]]; then
      conf="${SCRIPT_DIR}/${conf}"
    fi
  fi
  if [[ ! -f "${conf}" ]]; then
    bad "config not found: ${conf}"
    return 1
  fi
  printf '%s\n' "${conf}"
}

start_server() {
  local conf="$1"
  local batch="${2:-}"
  if port_open "${PORT}"; then
    bad "port ${PORT} is already in use; stop that process first"
    return 1
  fi
  : > "${CONSUMED_LOG}"
  : > "${SERVER_LOG}"
  local -a cmd=("${PY}" trpc_main.py --conf "${conf}")
  if [[ "${batch}" == "batch" ]]; then
    cmd+=(--batch)
  fi
  log "starting server: $(basename "${conf}")${batch:+ (batch)}"
  # `exec` makes the subshell become the python process, so $! is its pid.
  ( cd "${SCRIPT_DIR}" && exec "${cmd[@]}" ) >>"${SERVER_LOG}" 2>&1 &
  SERVER_PID=$!
  if ! wait_ready; then
    bad "server did not start listening on ${PORT}; last log lines:"
    tail -n 40 "${SERVER_LOG}" || true
    stop_server
    return 1
  fi
}

stop_server() {
  # EXIT also calls this when no server was started (for example `list`).
  [[ -z "${SERVER_PID}" ]] && return 0
  pkill -TERM -P "${SERVER_PID}" 2>/dev/null || true
  kill -TERM "${SERVER_PID}" 2>/dev/null || true
  local _
  for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
    kill -0 "${SERVER_PID}" 2>/dev/null || break
    sleep 0.5
  done
  pkill -KILL -P "${SERVER_PID}" 2>/dev/null || true
  kill -KILL "${SERVER_PID}" 2>/dev/null || true
  wait "${SERVER_PID}" 2>/dev/null || true
  SERVER_PID=""
  if ! wait_port_free; then
    bad "port ${PORT} still in use after stopping the server"
    return 1
  fi
}

keys_present() {
  local num="$1"
  "${PY}" -c '
import json
import sys

path = sys.argv[1]
num = int(sys.argv[2])
found = set()
try:
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            key = record.get("key")
            if isinstance(key, str):
                found.add(key)
except FileNotFoundError:
    sys.exit(1)
needed = {"key-%d" % i for i in range(num)}
sys.exit(0 if needed <= found else 1)
' "${CONSUMED_LOG}" "${num}"
}

wait_consumed() {
  local num="$1"
  local tries=120
  while (( tries-- > 0 )); do
    keys_present "${num}" && return 0
    if [[ -n "${SERVER_PID}" ]] && ! kill -0 "${SERVER_PID}" 2>/dev/null; then
      return 1
    fi
    sleep 0.5
  done
  return 1
}

run_client() {
  local label="$1"
  shift
  local rc=0
  log "running ${label}"
  ( cd "${SCRIPT_DIR}" && "${PY}" "$@" ) || rc=$?
  if [[ ${rc} -eq 0 ]]; then
    ok "${label}"
  else
    bad "${label} (exit ${rc})"
  fi
  return ${rc}
}

run_once() {
  local label="$1"
  local conf rc=0 batch_flag=""
  conf="$(resolve_conf "${2:-}")" || return 1
  require_broker || return 1
  if [[ "${label}" == "batch" ]]; then
    batch_flag="batch"
  fi
  start_server "${conf}" "${batch_flag}" || return 1
  log "running checks against ${HOST}:${PORT}"
  run_client "test_service_rpc" test_service_rpc.py --num "${RPC_NUM}" --target "ip://${HOST}:${PORT}" || rc=1
  if [[ ${rc} -eq 0 ]]; then
    run_client "kafka_client" kafka_client.py --num "${PRODUCE_NUM}" --conf "${conf}" || rc=1
  fi
  if [[ ${rc} -eq 0 ]]; then
    log "waiting for consumed records"
    if wait_consumed "${PRODUCE_NUM}"; then
      ok "consumed ${PRODUCE_NUM} records"
    else
      bad "consumed log missing key-0..key-$((PRODUCE_NUM - 1)) (see ${CONSUMED_LOG})"
      rc=1
    fi
  fi
  stop_server || rc=1
  if [[ ${rc} -eq 0 ]]; then
    ok "${label} $(basename "${conf}")"
  else
    bad "${label} $(basename "${conf}") (see ${SERVER_LOG})"
  fi
  return ${rc}
}

run_modes() {
  local rc=0 conf
  for conf in "${CONFIGS[@]}"; do
    run_once service "${conf}" || rc=1
  done
  return ${rc}
}

run_all() {
  local rc=0
  run_modes || rc=1
  run_once batch || rc=1
  return ${rc}
}

no_extra_args() {
  local name="$1"
  local extra="${2:-}"
  local hint="$3"
  if [[ -n "${extra}" ]]; then
    bad "${name} only accepts the example name (${hint})"
    return 2
  fi
}

trap 'stop_server' EXIT INT TERM

main() {
  case "${1:-service}" in
    list)
      echo "Available examples: service batch modes all"
      echo "Configs:"
      printf '  %s\n' "${CONFIGS[@]}"
      ;;
    service)
      run_once service "${2:-}"
      ;;
    batch)
      run_once batch "${2:-}"
      ;;
    modes)
      no_extra_args modes "${2:-}" "runs service against every config" || return 2
      run_modes
      ;;
    all)
      no_extra_args all "${2:-}" "runs modes, then batch" || return 2
      run_all
      ;;
    *)
      bad "unknown example: $1 (try: ./run_examples.sh list)"
      return 2
      ;;
  esac
}

main "$@"
