#!/usr/bin/env bash

set -euo pipefail

usage () {
    echo "Usage: bash dump_subclass_instance_ofs.sh LANGUAGES OUTPUT_FOLDER [ENTITY_TYPES=PER,LOC,ORG DB_NAME=wikidata_db COLLECTION_NAME=wikidata_simple MONGODB_PORT=27017 COLLAPSE_LANGUAGES=no KEEP_INTERMEDIATE_FILES=no NUM_WORKERS=1 DISABLE_SUBCLASSING=no]"
}

[ $# -lt 4 ] && usage && exit 1

langs="${1}"
output_folder="${2}"
entity_types=$(echo "${3:-PER,LOC,ORG}" | tr "," " ")
db_name="${4:-paranames_db}"
collection_name="${5:-paranames}"
mongodb_port="${6:-27017}"
should_collapse_languages=${7:-no}
should_keep_intermediate_files=${8:-no}
default_format="jsonl"

num_workers=${9:-1}
should_disable_subclassing=${10:-no}

if [ "${should_keep_intermediate_files}" = "yes" ]
then
    intermediate_output_folder=$output_folder
else
    intermediate_output_folder=$(mktemp -d /tmp/paranames_intermediate_files_XXXXX)
fi

# NOTE: edit this to increase/decrease threshold for excluding small languages
default_name_threshold=1


# The default languages to exclude are codes that appear in Wikidata but
# do not have a language code listed here:
# https://www.wikidata.org/wiki/Help:Wikimedia_language_codes/lists/all

# NOTE: change this comma separted list here to exclude certain languages
exclude_these_langs="bag,bas,bkm,blk,gur,mcn,nyn"

# Change to "yes" to disambiguate entity types
should_disambiguate_types="no"

mkdir -p $output_folder

dump () {

    local conll_type=$1
    local langs=$2
    local db_name=$3
    local collection_name=$4
    local mongodb_port=$5
    local disable_subclassing=$6
    local output="${output_folder}/${conll_type}.jsonl"

    if [ "${langs}" = "all" ]
    then
        langs_flag=""
        strict_flag=""
    else
        langs_flag="-l ${langs}"
        strict_flag="--strict"
    fi

    if [ "${should_disable_subclassing}" = "yes" ]
    then
        echo "[INFO] Disabling use of subclass information."
        disable_subclass_flag="--disable-subclass"
    else
        echo "[INFO] No subclass information disabled."
        disable_subclass_flag=""
    fi

    # dump everything into one file
    python paranames/io/wikidata_dump_instance_ofs.py \
        $strict_flag \
        -t "${conll_type}" $langs_flag \
        --database-name "${db_name}" \
        --collection-name "${collection_name}" \
        --mongodb-port "${mongodb_port}" \
        -o - $disable_subclass_flag > "${output}"


}

for conll_type in $entity_types
do
    dump $conll_type $langs $db_name $collection_name $mongodb_port $should_disable_subclassing &
done
wait
