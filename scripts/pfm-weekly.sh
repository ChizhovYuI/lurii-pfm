#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/data/logs"
LOG_FILE="${LOG_DIR}/pfm-weekly.log"

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required but was not found in PATH." >&2
  exit 1
fi

mkdir -p "${LOG_DIR}"

{
  echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Starting weekly PFM pipeline"
  cd "${PROJECT_ROOT}"
  uv run pfm run
  echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Weekly PFM pipeline completed"
} >>"${LOG_FILE}" 2>&1
