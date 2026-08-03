#!/usr/bin/env bash
set -e

mkdir -p target
git archive --format=tar.gz -o target/rdf-fusion-source.tar.gz HEAD
echo "Source archive created at target/rdf-fusion-source.tar.gz"
