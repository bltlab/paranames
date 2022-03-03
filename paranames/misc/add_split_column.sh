#!/usr/bin/env bash

# Split a tsv file rows into train/dev/test sets, taking care to split based on Wikidata ID

input_tsv=$1
output_tsv=$2
wikidata_id_column=${3:-wikidata_id}

[ $# -lt 2 ] && echo "Usage: add_split_column.sh INPUT_TSV OUTPUT_TSV" && exit 1

# Split fractions
train_frac=0.8
dev_frac=0.1
test_frac=0.1

# change this to point to the train/dev/test split script
split_script=$HOME/paranames/paranames/misc/random_train_test_split.py

# get unique ids and number of unique ids
unique_ids_file=$(mktemp -p /tmp unique_ids_XXXXX)
cut -f1 ${input_tsv} | sort | uniq > ${unique_ids_file}
n_unique_ids=$(wc -l ${unique_ids_file} | cut -f1 -d" ")

# create random train/dev/test splits
splits_file=$(mktemp -p /tmp train_dev_test_splits_XXXXX)
python ${split_script} \
    --train-frac ${train_frac} \
    --dev-frac ${dev_frac} \
    --test-frac ${test_frac} \
    -n ${n_unique_ids} > ${splits_file}

# join them together with paste
unique_ids_with_splits_file=$(mktemp -p /tmp unique_ids_XXXXX)
unique_ids_with_splits_and_header_file=${unique_ids_with_splits_file}.with_header
paste ${unique_ids_file} ${splits_file} > ${unique_ids_with_splits_file}

echo "${wikidata_id_column}	split" > ${unique_ids_with_splits_and_header_file}
cat ${unique_ids_with_splits_file} >> ${unique_ids_with_splits_and_header_file}

xsv join -d'\t' \
    ${wikidata_id_column} ${input_tsv} \
    ${wikidata_id_column} ${unique_ids_with_splits_and_header_file} \
    | xsv select 1-5,'split' | xsv fmt -t'\t' > ${output_tsv}
