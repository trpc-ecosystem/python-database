#!/usr/bin/env bash
#
# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
#
# Run the tRPC-Redis examples against a local Redis server on 127.0.0.1:6379.
#
# Usage:
#   ./run_examples.sh                  # same as "service"
#   ./run_examples.sh client           # run redis_client.py (no Greeter server)
#   ./run_examples.sh service [conf]   # start trpc_main.py, run test_service_rpc.py, stop the server
#   ./run_examples.sh all              # client, then the default service
#   ./run_examples.sh list             # list example names
#
# A conf path is a file name under this directory, or an absolute path.
# The server listen port is 8000, matching the trpc_python*.yaml files.
#
# Environment:
#   PYTHON   interpreter to use (default: ../venv/bin/python, then python3)
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
export PYTHONPATH="${PKG_DIR}${PYTHONPATH:+:${PYTHONPATH}}"

if [[ -n "${PYTHON:-}" ]]; then
  PY="${PYTHON}"
elif [[ -x "${PKG_DIR}/venv/bin/python" ]]; then
  PY="${PKG_DIR}/venv/bin/python"
else
  PY="$(command -v python3)"
fi

SERVER_LOG="${SCRIPT_DIR}/.server_run.log"
SERVER_PID=""
HOST="127.0.0.1"
PORT=8000

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*" >&2; }
ok()  { printf '\033[1;32m[PASS]\033[0m %s\n' "$*" >&2; }
bad() { printf '\033[1;31m[FAIL]\033[0m %s\n' "$*" >&2; }

port_open() {
  "${PY}" -c "import socket; s=socket.create_connection(('${HOST}', ${PORT}), 0.2); s.close()" >/dev/null 2>&1
}

wait_ready() {
  local tries=60
  while (( tries-- > 0 )); do
    port_open && return 0
    if [[ -n "${SERVER_PID}" ]] && ! kill -0 "${SERVER_PID}" 2>/dev/null; then
      return 1
    fi
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
  if port_open; then
    bad "port ${PORT} is already in use; stop that process first"
    return 1
  fi
  log "starting server: ${conf}"
  : > "${SERVER_LOG}"
  # `exec` makes the subshell become the python process, so $! is its pid.
  ( cd "${SCRIPT_DIR}" && exec "${PY}" trpc_main.py --conf "${conf}" ) >>"${SERVER_LOG}" 2>&1 &
  SERVER_PID=$!
  if ! wait_ready; then
    bad "server did not start listening on ${PORT}; last log lines:"
    tail -n 30 "${SERVER_LOG}" || true
    stop_server
    return 1
  fi
}

stop_server() {
  [[ -z "${SERVER_PID}" ]] && return 0
  pkill -TERM -P "${SERVER_PID}" 2>/dev/null || true
  kill -TERM "${SERVER_PID}" 2>/dev/null || true
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    kill -0 "${SERVER_PID}" 2>/dev/null || break
    sleep 0.5
  done
  pkill -KILL -P "${SERVER_PID}" 2>/dev/null || true
  kill -KILL "${SERVER_PID}" 2>/dev/null || true
  for _ in 1 2 3 4 5 6 7 8; do
    port_open || break
    sleep 0.5
  done
  SERVER_PID=""
}

run_client() {
  log "running example: client (redis_client.py)"
  local rc=0
  ( cd "${SCRIPT_DIR}" && "${PY}" redis_client.py ) || rc=$?
  if [[ ${rc} -eq 0 ]]; then
    ok "client"
  else
    bad "client (exit ${rc})"
  fi
  return ${rc}
}

run_service() {
  local conf
  conf="$(resolve_conf "${1:-}")" || return 1
  start_server "${conf}" || return 1
  log "running example: service (test_service_rpc.py)"
  local rc=0
  ( cd "${SCRIPT_DIR}" && "${PY}" test_service_rpc.py --target "ip://${HOST}:${PORT}" ) || rc=$?
  stop_server
  if [[ ${rc} -eq 0 ]]; then
    ok "service"
  else
    bad "service (exit ${rc}); server log: ${SERVER_LOG}"
  fi
  return ${rc}
}

trap 'stop_server' EXIT INT TERM

main() {
  case "${1:-service}" in
    list)
      echo "Available examples: client service"
      ;;
    client)
      run_client
      ;;
    service)
      run_service "${2:-}"
      ;;
    all)
      local rc=0
      run_client || rc=1
      run_service || rc=1
      return ${rc}
      ;;
    *)
      bad "unknown example: $1 (try: ./run_examples.sh list)"
      return 2
      ;;
  esac
}

main "$@"
