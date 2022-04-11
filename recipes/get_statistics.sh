#!/usr/bin/env bash

set -euo pipefail

usage () {
    echo "get_all_counts.sh PARANAMES_TSV [PARANAMES_TSV_RAW] [OUTPUT_FOLDER]"
}


compute_script_entropy () {
    local input_file=$1
    local output_file=$2
    local script="paranames/analysis/script_entropy_with_cache.sh"
    bash $script $input_file $output_file
}


[ $# -lt 1 ] && usage && exit 1

paranames_dump=$1
paranames_dump_raw=${2:-none}
default_output_folder=$(dirname $paranames_dump)/../extra_data
output_folder=${3:-$default_output_folder}

mkdir -pv $output_folder || exit 1

echo "Computing counts for each language and type"
counts_output_file=$output_folder/counts_per_language_and_type.tsv
bash paranames/analysis/counts_per_lang_and_type.sh $paranames_dump | xsv fmt -t"\t" | tqdm > $counts_output_file

# script entropy before
if [ "${paranames_dump_raw}" != "none" ]
then
    echo "Computing script entropy before script standardization"
    entropy_before_output_file=$output_folder/entropy_per_language_before_script_standardization.tsv
    compute_script_entropy \
        $paranames_dump_raw \
        $entropy_before_output_file &
else
    echo "Raw dump not given. Skipping script entropy computation..."
fi

# script entropy after
echo "Computing script entropy after script standardization"
entropy_after_output_file=$output_folder/entropy_per_language_after_script_standardization.tsv
compute_script_entropy \
    $paranames_dump \
    $entropy_after_output_file &

wait

# counts of PER/LOC/ORG overlap
echo "Computing overlap counts of PER/LOC/ORG"
overlap_counts_output_file=$output_folder/overlap_counts.json
python paranames/analysis/compute_overlap_counts.py \
    --input-file $paranames_dump \
    --input-format tsv \
    --output-file $overlap_counts_output_file \
    --output-format json
