#!/usr/bin/env bash
set -e


# Publish
(cd lib/common && cargo publish)
(cd lib/encoding && cargo publish)
(cd lib/extensions && cargo publish)
(cd lib/compute && cargo publish)
(cd lib/functions && cargo publish)
(cd lib/logical && cargo publish)
(cd lib/physical && cargo publish)
(cd lib/storage && cargo publish)
(cd lib/execution && cargo publish)
(cd lib/rdf-fusion && cargo publish)
(cd lib/web && cargo publish)
(cd cli && cargo publish)
(cd bench && cargo publish)
