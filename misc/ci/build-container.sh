#!/usr/bin/env bash
# Builds the RDF Fusion container image from a released binary tarball and pushes it to Codeberg.
#
# Usage: build-container.sh <binary-tarball> <version>
#
#   binary-tarball  e.g., rdf-fusion-0.3.1-x86_64-unknown-linux-gnu.tar.gz (from build-binary.sh)
#   version         e.g., 0.3.1 (without the `release/` prefix)
#
# Environment:
#   CODEBERG_PACKAGES_TOKEN  Codeberg access token with the `write:package` scope. If unset, the
#                            image is only built locally (useful for testing).
#
# The image is also tagged `<major>.<minor>` if the version is the newest release of its minor
# line and `latest` if it is the newest release overall. Releases are the `release/*` tags of the
# `origin` remote, so re-publishing an old release or a backport never moves these tags backwards.
set -euo pipefail

TARBALL="$1"
VERSION="$2"

REGISTRY="codeberg.org"
REGISTRY_USERNAME="tschwarzinger"
IMAGE="$REGISTRY/tschwarzinger/rdf-fusion"
CONTAINERFILE="$(cd "$(dirname "$0")/../.." && pwd)/Containerfile"

if [[ ! "$VERSION" =~ ^([0-9]+)\.([0-9]+)\.[0-9]+$ ]]; then
    echo "Expected a plain version like 0.3.1, got '$VERSION'"
    exit 1
fi
MINOR_VERSION="${BASH_REMATCH[1]}.${BASH_REMATCH[2]}"

# Plain release versions, including $VERSION in case its tag is not pushed yet.
RELEASES=$({
    git ls-remote --tags --refs origin 'release/*' | sed 's|.*refs/tags/release/||'
    echo "$VERSION"
} | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' | sort -uV)
NEWEST=$(tail -n 1 <<<"$RELEASES")
NEWEST_IN_MINOR=$(awk -F . -v minor="$MINOR_VERSION" '$1 "." $2 == minor' <<<"$RELEASES" | tail -n 1)

TAGS=("$VERSION")
if [ "$VERSION" = "$NEWEST_IN_MINOR" ]; then
    TAGS+=("$MINOR_VERSION")
fi
if [ "$VERSION" = "$NEWEST" ]; then
    TAGS+=("latest")
fi

# Build context: the binary and an empty data directory.
CONTEXT=$(mktemp -d)
trap 'rm -rf "$CONTEXT"' EXIT
tar -xzf "$TARBALL" -C "$CONTEXT" rdf-fusion
chmod 0755 "$CONTEXT/rdf-fusion"
mkdir "$CONTEXT/rootfs"
# Group-writable, see the Containerfile.
mkdir -m 0775 "$CONTEXT/rootfs/data"

TAG_ARGS=()
for TAG in "${TAGS[@]}"; do
    TAG_ARGS+=(-t "$IMAGE:$TAG")
done
podman build --platform linux/amd64 -f "$CONTAINERFILE" \
    --build-arg RDF_FUSION_VERSION="$VERSION" "${TAG_ARGS[@]}" "$CONTEXT"

if [ -z "${CODEBERG_PACKAGES_TOKEN:-}" ]; then
    echo "CODEBERG_PACKAGES_TOKEN is not set; built $IMAGE (${TAGS[*]}) without pushing"
    exit 0
fi
echo "$CODEBERG_PACKAGES_TOKEN" | podman login "$REGISTRY" -u "$REGISTRY_USERNAME" --password-stdin
for TAG in "${TAGS[@]}"; do
    podman push "$IMAGE:$TAG"
done
echo "Pushed $IMAGE with tags: ${TAGS[*]}"
