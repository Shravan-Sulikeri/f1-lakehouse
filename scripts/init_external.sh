#!/usr/bin/env bash
set -euo pipefail

ROOT_VOLUME="/Volumes/SAMSUNG"
ENV_FILE=".env"
FALLBACK_ENV_FILE=".env.example"
ENV_SOURCE=""

if [[ ! -d "$ROOT_VOLUME" ]]; then
  echo "External volume /Volumes/SAMSUNG is not mounted. Please mount it and re-run."
  exit 1
fi

if [[ -f "$ENV_FILE" ]]; then
  ENV_SOURCE="$ENV_FILE"
elif [[ -f "$FALLBACK_ENV_FILE" ]]; then
  ENV_SOURCE="$FALLBACK_ENV_FILE"
else
  echo "No environment file found (.env or .env.example). Cannot determine EXTERNAL_DATA_ROOT."
  exit 1
fi

# shellcheck disable=SC1090
source "$ENV_SOURCE"

if [[ -z "${EXTERNAL_DATA_ROOT:-}" ]]; then
  echo "EXTERNAL_DATA_ROOT is not set in $ENV_SOURCE."
  exit 1
fi

echo "Preparing external data root at $EXTERNAL_DATA_ROOT"
mkdir -p "$EXTERNAL_DATA_ROOT"

subdirs=(bronze silver gold cache warehouse ollama)
for dir in "${subdirs[@]}"; do
  target="$EXTERNAL_DATA_ROOT/$dir"
  mkdir -p "$target"
  echo "Ensured directory $target"
done

cache_dir="$EXTERNAL_DATA_ROOT/cache"
test_file="$cache_dir/.write_test.$$"

touch "$test_file"
rm -f "$test_file"

echo "Write permissions verified in $cache_dir"
echo "External data root initialization completed successfully."
