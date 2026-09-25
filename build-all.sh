#!/usr/bin/env bash
# Builds the jar for every service before `docker compose build`, since each
# Dockerfile just copies a pre-built target/*.jar (no Maven build inside Docker,
# to avoid needing a GitHub Packages token inside the image build).
#
# Requires: all repos cloned as siblings of this one (see README), and
# common-libs already installed locally (or GitHub Packages auth configured) so
# api-gateway/user-service/catalog-service can resolve com.nexus:common-*.
set -euo pipefail

cd "$(dirname "$0")"

for repo in common-libs discovery-server api-gateway user-service catalog-service; do
  if [ ! -d "../$repo" ]; then
    echo "Missing ../$repo -- clone it as a sibling of this infra repo first." >&2
    exit 1
  fi
done

echo "==> common-libs: mvn clean install"
(cd ../common-libs && mvn -q clean install -DskipTests)

for repo in discovery-server api-gateway user-service catalog-service; do
  echo "==> $repo: mvn clean package"
  (cd "../$repo" && mvn -q clean package -DskipTests)
done

echo "All jars built. Now run: docker compose build && docker compose up"
