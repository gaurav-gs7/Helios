#!/usr/bin/env bash
set -euo pipefail
docker compose -f deploy/compose.yaml kill worker
