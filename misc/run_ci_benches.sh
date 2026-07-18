#!/bin/bash
set -eo pipefail

STEP=$1
LABELS=$2

ALL_BENCHMARKS=("scalar" "scalar_binary" "store" "bsbm_explore" "bsbm_business_intelligence")
BSBM_SUITE=("bsbm_explore" "bsbm_business_intelligence")
SCALAR_UDFS_SUITE=("scalar" "scalar_binary")

RUN_ALL=0
BENCHES_TO_RUN=()

# LABELS is comma separated, e.g., "bug,bench/bsbm,enhancement"
IFS=',' read -ra LABEL_ARRAY <<< "$LABELS"
for label in "${LABEL_ARRAY[@]}"; do
    if [[ "$label" == "bench" ]]; then
        RUN_ALL=1
    elif [[ "$label" == bench/* ]]; then
        bench_name="${label#bench/}"
        if [[ "$bench_name" == "bsbm" ]]; then
            BENCHES_TO_RUN+=("${BSBM_SUITE[@]}")
        elif [[ "$bench_name" == "scalar_udfs" ]]; then
            BENCHES_TO_RUN+=("${SCALAR_UDFS_SUITE[@]}")
        else
            BENCHES_TO_RUN+=("$bench_name")
        fi
    fi
done

if [[ $RUN_ALL -eq 1 ]]; then
    BENCHES_TO_RUN=("${ALL_BENCHMARKS[@]}")
fi

if [[ ${#BENCHES_TO_RUN[@]} -eq 0 ]]; then
    echo "No benchmarks to run."
    exit 0
fi

# Unique benches
# Using associative array or simply sort -u
UNIQUE_BENCHES=($(echo "${BENCHES_TO_RUN[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' '))


for bench in "${UNIQUE_BENCHES[@]}"; do
    if [[ "$STEP" == "main" ]]; then
        # TODO: Remove once crit benchmarks are independent of env variables
        set -gx RDF_FUSION_STORAGE_DELTA_ASSUME_SINGLE_NODE true
        cargo bench --bench "$bench" -- --save-baseline "main-$bench"
    elif [[ "$STEP" == "candidate" ]]; then
        # TODO: Remove once crit benchmarks are independent of env variables
        set -gx RDF_FUSION_STORAGE_DELTA_ASSUME_SINGLE_NODE true
        cargo bench --bench "$bench" -- --save-baseline "candidate-$bench"
    elif [[ "$STEP" == "compare" ]]; then
        critcmp "main-$bench" "candidate-$bench"
    else
        echo "Unknown step: $STEP"
        exit 1
    fi
done
