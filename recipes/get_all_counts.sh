#!/usr/bin/env bash

set -euo pipefail

usage () {
    echo "get_all_counts.sh DUMP_BEFORE DUMP_AFTER [OUTPUT_FOLDER]"
}


compute_script_entropy () {
    local input_file=$1
    local output_file=$2
    local script="paranames/analysis/script_entropy_with_cache.sh"
    bash $script $input_file $output_file
}


[ $# -lt 2 ] && usage && exit 1

dump_before=$1
dump_after=$2
default_output_folder=$(dirname $dump_after)/../extra_data
output_folder=${3:-$default_output_folder}

mkdir -pv $output_folder || exit 1

echo "Computing counts for each language and type"
counts_output_file=$output_folder/counts_per_language_and_type.tsv
bash paranames/analysis/counts_per_lang_and_type.sh $dump_after | xsv fmt -t"\t" | tqdm > $counts_output_file

# maybe: counts for lumped languages and types

# script entropy before
echo "Computing script entropy before script standardization"
entropy_before_output_file=$output_folder/entropy_per_language_before_script_standardization.tsv
compute_script_entropy \
    $dump_before \
    $entropy_before_output_file &

# script entropy after
echo "Computing script entropy after script standardization"
entropy_after_output_file=$output_folder/entropy_per_language_after_script_standardization.tsv
compute_script_entropy \
    $dump_after \
    $entropy_after_output_file &

wait

# counts of PER/LOC/ORG overlap
echo "Computing overlap counts of PER/LOC/ORG"
overlap_counts_output_file=$output_folder/overlap_counts.json
python paranames/analysis/compute_overlap_counts.py \
    --input-file $dump_after \
    --input-format tsv \
    --output-file $overlap_counts_output_file \
    --output-format json
