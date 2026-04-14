#!/usr/bin/env bash
set -euo pipefail

docker compose build
docker compose run --rm simulator python -m simulator.test
docker compose run --rm cadlabs python -c "import radcad, cadCAD_tools; print('cadlabs env ok')"
