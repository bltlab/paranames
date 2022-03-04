#!/usr/bin/env bash

set -euo pipefail

usage () {
    echo "Usage: bash separate_folder_dump.sh LANGUAGES OUTPUT_FOLDER [ENTITY_TYPES=PER,LOC,ORG DB_NAME=wikidata_db COLLECTION_NAME=wikidata_simple COLLAPSE_LANGUAGES=no NUM_WORKERS=n_cpus]"
}

[ $# -lt 4 ] && usage && exit 1

langs="${1}"
output_folder="${2}"
entity_types=$(echo "${3:-PER,LOC,ORG}" | tr "," " ")
db_name="${4:-wikidata_db}"
collection_name="${5:-wikidata_simple}"
default_format="tsv"
should_collapse_languages=${6:-no}

default_num_workers=$(nproc)
num_workers=${7:-$default_num_workers}

extra_data_folder="${output_folder}"/extra_data

mkdir --verbose -p $output_folder/combined
mkdir --verbose -p $extra_data_folder

# NOTE: edit this to increase/decrease threshold for excluding small languages
default_name_threshold=1

# NOTE: add comma separted list here to exclude languages
exclude_these_langs=""

# Change to "yes" to disambiguate entity types
should_disambiguate_types="no"

dump () {

    local conll_type=$1
    local langs=$2
    local db_name=$3
    local collection_name=$4
    local output="${output_folder}/${conll_type}.tsv"

    if [ "${langs}" = "all" ]
    then
        langs_flag=""
        strict_flag=""
    else
        langs_flag="-l ${langs}"
        strict_flag="--strict"
    fi

    if [ -z "${exclude_these_langs}" ]
    then
        echo "[INFO] No languages being excluded."
        #exclude_langs_flag="-L en"
        exclude_langs_flag=""
    else
        echo "[INFO] Excluding ${exclude_these_langs//,/, }."
        exclude_langs_flag="-L ${exclude_these_langs}"
    fi

    # dump everything into one file
    python paranames/io/wikidata_dump_transliterations.py \
        $strict_flag \
        -t "${conll_type}" $langs_flag \
        -f "${default_format}" \
        -d "tab" \
        --database-name "${db_name}" \
        --collection-name "${collection_name}" \
        -o - $exclude_langs_flag > "${output}"

}

postprocess () {
    local input_file=$1
    local output_file=$2
    local should_disambiguate=${3:-yes}
    local should_collapse=${4:-no}

    if [ "${should_disambiguate}" = "yes" ]
    then
        echo "Disambiguating entity types!"
        disamb_flag="--should-disambiguate-entity-types"
    else
        echo "Not disambiguating entity types!"
        disamb_flag=""
    fi

    if [ "${should_collapse}" = "yes" ]
    then
        echo "Collapsing language codes to top-level only!"
        collapse_flag="--should-collapse-languages"
    else
        echo "Language codes left as-is!"
        collapse_flag=""
    fi

    python paranames/io/postprocess.py \
        -i $input_file -o $output_file -f $default_format \
        -m $default_name_threshold $disamb_flag $collapse_flag --should-remove-parentheses

    }

standardize_script () {
    local input_file=$1
    local output_file=$2
    local num_workers=$3

    # apply script standardization
    python paranames/io/script_standardization.py \
        -i $input_file -o $output_file -f tsv \
        --filtered-names-output-file "${extra_data_folder}/filtered_names.tsv" \
        --write-filtered-names --num-workers $num_workers
}

separate_by_language () {
    
    local filtered_file=$1
    python paranames/io/separate_by_language.py \
        --input-file $filtered_file \
        --lang-column language \
        --io-format $default_format \
        --use-subfolders \
        --verbose
}


csv2tsv () {
    xsv fmt -t"\t"
}

combine_tsv_files () {
    local glob="$@"
    xsv cat rows -d"\t" ${glob} | csv2tsv
}

# Step 1: Dump and parallelize across types
echo "[1/5] Extract from MongoDB..."
for conll_type in $entity_types
do
    dump $conll_type $langs $db_name $collection_name &
done
wait

# Step 2: combine into one TSV
combined_tsv="${output_folder}/combined_postprocessed.tsv"
echo "[2/5] Combining TSV files together into $combined_tsv"
combine_tsv_files ${output_folder}/*.tsv > $combined_tsv

# Step 3: Apply post-processing steps
echo "[3/5] Running postprocess.py"
combined_postprocessed_tsv="${output_folder}/combined_postprocessed.tsv"
postprocess $combined_tsv $combined_postprocessed_tsv $should_disambiguate_types $should_collapse_languages

# Step 4: Script standardization
echo "[4/5] Script standardization"
combined_script_standardized_tsv="${output_folder}/combined_script_standardized.tsv"
standardize_script \
    $combined_postprocessed_tsv \
    $combined_script_standardized_tsv \
    $num_workers

# Step 5: Separate into subfolders by language
echo "[5/5] Separate into subfolders by language..."
echo "Destination: ${combined_script_standardized_tsv}"
separate_by_language $combined_script_standardized_tsv

mv --verbose ${output_folder}/{PER,LOC,ORG,combined}*.tsv ${output_folder}/combined
