#!/usr/bin/env bash

set -exuo pipefail

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
voting_method="baseline"

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
    local vote_aggregation_method=$3
    local num_workers=$4

    # apply script standardization
    python paranames/io/script_standardization.py \
        -i $input_file -o $output_file -f tsv \
        --vote-aggregation-method $vote_aggregation_method \
        --filtered-names-output-file "${extra_data_folder}/filtered_names.tsv" \
        --write-filtered-names --num-workers $num_workers
}

standardize_names () {
    local input_file=$1
    local output_file=$2
    local permuter_type=$3
    local conll_type=$4

    # apply script standardization
    python paranames/io/name_standardization.py \
        -i $input_file -o $output_file -f tsv \
        --human-readable-langs-path ~/paranames/data/human_readable_lang_names.json \
        --permuter-type $permuter_type --corpus-stats-output ${extra_data_folder}/standardize_names_stats_$conll_type \
        --debug-mode --num-workers $num_workers --corpus-require-english
}

compute_script_entropy () {
    local input_file=$1
    local output_file=$2
    local script="paranames/analysis/script_entropy_with_cache.sh"
    bash $script $input_file $output_file
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

separate_by_entity_type () {
    local input_file=$1
    local conll_type=$2
    local folder=$(dirname "${input_file}")
    local type_column=${3:-type}
    xsv search -d"\t" -s "${type_column}" "${conll_type}" < "${input_file}" | csv2tsv
}

echo "Extract & clean everything for each type"
for conll_type in $entity_types
do
    dump $conll_type $langs $db_name $collection_name &
done
wait

# combine everything into one tsv for script standardization
combined_tsv="${output_folder}/combined_postprocessed.tsv"
combine_tsv_files ${output_folder}/*.tsv > $combined_tsv

combined_postprocessed_tsv="${output_folder}/combined_postprocessed.tsv"
postprocess $combined_tsv $combined_postprocessed_tsv $should_disambiguate_types $should_collapse_languages


# script standardization: remove parentheses from everything
combined_script_standardized_tsv="${output_folder}/combined_script_standardized_${voting_method}.tsv"
standardize_script \
    $combined_postprocessed_tsv \
    $combined_script_standardized_tsv \
    $voting_method $num_workers

# separate into PER,LOC,ORG for name permutations
for conll_type in $entity_types
do
    separate_by_entity_type $combined_script_standardized_tsv $conll_type \
        > "${output_folder}/${conll_type}_script_standardized_${voting_method}.tsv" &
done
wait

echo "Combine everything into one big tsv"
final_combined_output="${output_folder}/combined_script_standardized_${voting_method}.tsv"
echo "Destination: ${final_combined_output}"

rm -vrf $final_combined_output
combine_tsv_files ${output_folder}/*_script_standardized_${voting_method}.tsv > $final_combined_output
separate_by_language $final_combined_output

mv --verbose ${output_folder}/{PER,LOC,ORG,combined}*.tsv ${output_folder}/combined
