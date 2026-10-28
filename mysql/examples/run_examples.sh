#!/usr/bin/env bash
#
# Tencent is pleased to support the open source community by making tRPC-Python-Database available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python-Database is licensed under Apache-2.0.
#
# Run the tRPC-MySQL examples against a local MySQL server.
#
# Usage:
#   ./run_examples.sh                # same as "service"
#   ./run_examples.sh service [conf] # start trpc_main.py, run test_service.py, stop the server
#   ./run_examples.sh modes          # "service" once per process/thread model config
#   ./run_examples.sh playground     # run playground.py (no server)
#   ./run_examples.sh all            # service, then playground
#   ./run_examples.sh list           # list example names
#
# Environment:
#   PYTHON     interpreter to use (default: ../venv/bin/python, then python3)
#   MYSQL_DSN  DSN for playground.py (the server reads trpc_python.yaml)
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

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
PORT=8080

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
ok()  { printf '\033[1;32m[PASS]\033[0m %s\n' "$*"; }
bad() { printf '\033[1;31m[FAIL]\033[0m %s\n' "$*"; }

port_open() { (exec 3<>"/dev/tcp/${HOST}/${PORT}") 2>/dev/null; }

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

run_service() {
  local conf="${1:-${SCRIPT_DIR}/trpc_python.yaml}"
  start_server "${conf}" || return 1
  log "running example: service (test_service.py)"
  local rc=0
  ( cd "${SCRIPT_DIR}" && "${PY}" test_service.py --addr "${HOST}:${PORT}" ) || rc=$?
  stop_server
  if [[ ${rc} -eq 0 ]]; then
    ok "service"
  else
    bad "service (exit ${rc}); server log: ${SERVER_LOG}"
  fi
  return ${rc}
}

run_modes() {
  local rc=0 conf
  for conf in trpc_python.yaml trpc_python_single_process_merge.yaml trpc_python_single_process_separate.yaml \
              trpc_python_multi_process_separate.yaml trpc_python_multi_process_merge.yaml; do
    run_service "${SCRIPT_DIR}/${conf}" || rc=1
  done
  return ${rc}
}

run_playground() {
  log "running example: playground (playground.py)"
  local rc=0
  ( cd "${SCRIPT_DIR}" && "${PY}" playground.py ) || rc=$?
  if [[ ${rc} -eq 0 ]]; then
    ok "playground"
  else
    bad "playground (exit ${rc})"
  fi
  return ${rc}
}

trap 'stop_server' EXIT INT TERM

main() {
  case "${1:-service}" in
    list)
      echo "Available examples: service modes playground"
      ;;
    service)
      run_service "${2:-}"
      ;;
    modes)
      run_modes
      ;;
    playground)
      run_playground
      ;;
    all)
      local rc=0
      run_service || rc=1
      run_playground || rc=1
      return ${rc}
      ;;
    *)
      bad "unknown example: $1 (try: ./run_examples.sh list)"
      return 2
      ;;
  esac
}

main "$@"
