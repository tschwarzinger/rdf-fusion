#!/usr/bin/env bash
set -e

TARGET="$1"
if [ -z "$TARGET" ]; then
    echo "Usage: $0 <target>"
    exit 1
fi

VERSION=$(grep -m 1 "^version = " Cargo.toml | cut -d '"' -f 2)
cargo build --profile release --bin rdf-fusion --target "$TARGET"
BINARY_DIR="target/$TARGET/release"
TARBALL="target/rdf-fusion-$VERSION-$TARGET.tar.gz"
tar -czf "$TARBALL" -C "$BINARY_DIR" rdf-fusion
echo "Created $TARBALL"
