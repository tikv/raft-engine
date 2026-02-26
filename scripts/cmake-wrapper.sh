#!/usr/bin/env bash
set -euo pipefail

if [[ $# -gt 0 ]]; then
  case "$1" in
    --build|--install|--open|--help|--version)
      exec cmake "$@"
      ;;
  esac
fi

exec cmake -DCMAKE_POLICY_VERSION_MINIMUM=3.5 "$@"
