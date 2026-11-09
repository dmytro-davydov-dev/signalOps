#!/bin/sh
# Create .env from .env.example with random local passwords and tokens.
# Refuses to overwrite an existing .env. Prints only the operator token, which
# the UI asks for; the other values stay in .env.
set -eu

cd "$(dirname "$0")/.."

if [ -e .env ]; then
  echo ".env already exists; remove it first to regenerate" >&2
  exit 1
fi

random() {
  LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom | head -c 40
}

operator_token=$(random)
umask 077
sed \
  -e "s|change-me-local-only|$(random)|" \
  -e "s|change-me-runtime-local-only|$(random)|" \
  -e "s|change-me-evaluator-local-only|$(random)|" \
  -e "s|change-me-grafana-local-only|$(random)|" \
  -e "s|<logs-token>|$(random)|" \
  -e "s|<traces-token>|$(random)|" \
  -e "s|<deployments-token>|$(random)|" \
  -e "s|<operator-token>|${operator_token}|" \
  .env.example >.env

echo "Wrote .env (mode 600)."
echo "Operator token for the UI and API: ${operator_token}"
