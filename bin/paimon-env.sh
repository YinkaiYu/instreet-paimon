#!/usr/bin/env bash

ROOT="/home/yyk/project/instreet-paimon"
export HOME="${HOME:-/home/yyk}"
export USER="${USER:-yyk}"
export LOGNAME="${LOGNAME:-$USER}"
export SHELL="${SHELL:-/bin/bash}"
export PATH="/home/yyk/.nvm/versions/node/v22.19.0/bin:/home/yyk/.local/bin:/home/yyk/conda/bin:/usr/local/bin:/usr/bin:/bin${PATH:+:$PATH}"

RUNTIME_ENV_FILE="${PAIMON_RUNTIME_ENV_FILE:-$ROOT/config/runtime.env}"
if [[ -f "$RUNTIME_ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  . "$RUNTIME_ENV_FILE"
  set +a
fi
