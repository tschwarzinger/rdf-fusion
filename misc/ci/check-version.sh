#!/usr/bin/env bash
set -e

REF_NAME="$1"

VERSION=$(grep -m 1 "^version = " Cargo.toml | cut -d '"' -f 2)
EXPECTED_VERSION=$(echo "$REF_NAME" | sed 's|^refs/tags/release/||' | sed 's|^release/||' | sed 's/^v//')
if [ "$VERSION" != "$EXPECTED_VERSION" ]; then
  echo "Error: Version mismatch. Cargo.toml has $VERSION, but tag is $REF_NAME (expected $EXPECTED_VERSION)"
  exit 1
fi
echo "Version $VERSION matches tag $REF_NAME"
