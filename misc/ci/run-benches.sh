#!/bin/bash
set -eo pipefail

STEP=$1
LABELS=$2

BSBM_SUITE=("bsbm_explore" "bsbm_business_intelligence")
SCALAR_UDFS_SUITE=("scalar" "scalar_binary")

all_benchmarks() {
    cargo bench --no-run --message-format=json 2>/dev/null \
        | jq -r 'select(.reason == "compiler-artifact" and (.target.kind | index("bench"))) | .target.name' \
        | sort -u
}

BENCHES_TO_RUN=()
# LABELS is comma separated, e.g., "bsbm,scalar-udfs"
IFS=',' read -ra LABEL_ARRAY <<< "$LABELS"
for label in "${LABEL_ARRAY[@]}"; do
    case "$label" in
        all)
            BENCHES_TO_RUN+=($(all_benchmarks))
            ;;
        bsbm)
            BENCHES_TO_RUN+=("${BSBM_SUITE[@]}")
            ;;
        scalar-udfs)
            BENCHES_TO_RUN+=("${SCALAR_UDFS_SUITE[@]}")
            ;;
        bsbm-explore)
            BENCHES_TO_RUN+=("bsbm_explore")
            ;;
        bsbm-bi)
            BENCHES_TO_RUN+=("bsbm_business_intelligence")
            ;;
        *)
            BENCHES_TO_RUN+=("$label")
            ;;
    esac
done

if [[ ${#BENCHES_TO_RUN[@]} -eq 0 ]]; then
    echo "No benchmarks to run."
    exit 0
fi

# Unique benches
# Using associative array or simply sort -u
UNIQUE_BENCHES=($(echo "${BENCHES_TO_RUN[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' '))

# TODO: Remove once crit benchmarks are independent of env variables
export RDF_FUSION_STORAGE_DELTA_ASSUME_SINGLE_NODE=true

for bench in "${UNIQUE_BENCHES[@]}"; do
    if [[ "$STEP" == "main" ]]; then
        cargo bench --bench "$bench" -- --save-baseline "main-$bench"
    elif [[ "$STEP" == "candidate" ]]; then
        cargo bench --bench "$bench" -- --save-baseline "candidate-$bench"
    elif [[ "$STEP" == "compare" ]]; then
        critcmp "main-$bench" "candidate-$bench"
    else
        echo "Unknown step: $STEP"
        exit 1
    fi
done
