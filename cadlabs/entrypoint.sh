# DEPRECATED -- do NOT use this entrypoint anymore

#!/usr/bin/env bash
set -euo pipefail

REPO_URL="${CADLABS_REPO_URL:-https://github.com/CADLabs/ethereum-economic-model.git}"
REPO_REF="${CADLABS_REPO_REF:-main}"
REPO_DIR="/workspace/cadlabs-model"
VENV_DIR="/workspace/.venv-cadlabs"
MARKER_FILE="${VENV_DIR}/.install-state"

mkdir -p /workspace/shared/output /workspace/shared/data

if [ ! -d "${REPO_DIR}/.git" ]; then
  git clone --depth 1 --branch "${REPO_REF}" "${REPO_URL}" "${REPO_DIR}"
else
  git -C "${REPO_DIR}" fetch --depth 1 origin "${REPO_REF}"
  git -C "${REPO_DIR}" checkout "${REPO_REF}"
  git -C "${REPO_DIR}" pull --ff-only origin "${REPO_REF}"
fi

PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
REPO_COMMIT="$(git -C "${REPO_DIR}" rev-parse HEAD)"
INSTALL_STATE="${REPO_URL}|${REPO_REF}|${REPO_COMMIT}|python-${PYTHON_VERSION}"

if [ ! -x "${VENV_DIR}/bin/python" ]; then
  python -m venv "${VENV_DIR}"
fi

. "${VENV_DIR}/bin/activate"

if [ ! -f "${MARKER_FILE}" ] || [ "$(cat "${MARKER_FILE}")" != "${INSTALL_STATE}" ]; then
  python -m pip install --upgrade pip setuptools wheel

  if [ -f "${REPO_DIR}/requirements.txt" ]; then
    python -m pip install -r "${REPO_DIR}/requirements.txt"
  fi

  if [ -f "${REPO_DIR}/requirements-dev.txt" ]; then
    python -m pip install -r "${REPO_DIR}/requirements-dev.txt"
  fi

  if [ -f "${REPO_DIR}/setup.py" ] || [ -f "${REPO_DIR}/pyproject.toml" ]; then
    python -m pip install -e "${REPO_DIR}"
  fi

  printf '%s' "${INSTALL_STATE}" > "${MARKER_FILE}"
else
  echo "Reusing existing CADLabs virtualenv at ${VENV_DIR}"
fi

cd "${REPO_DIR}"
exec "$@"
