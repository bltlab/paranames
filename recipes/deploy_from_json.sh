#!/usr/bin/env bash

# deploy_from_json.sh
## Deploy script for Wikidata

set -euo pipefail

usage () {
    echo "Usage: bash deploy_from_json.sh INPUT DB COLLECTION [CHUNKSIZE=5000] [N_WORKERS=nproc]"
}

# Make sure we have enough command line arguments
[ $# -lt 3 ] && usage && exit 1

## Constant
IO_SCRIPT_FOLDER=paranames/io

## Command line arguments
INPUT_JSON=${1}
DB_NAME=${2}
COLL_NAME=${3}
CHUNKSIZE=${4:-10000}
DEFAULT_CPUS=$(nproc)
N_WORKERS=${5:-$DEFAULT_CPUS}

# ingest into mongo db
python $IO_SCRIPT_FOLDER/wikidata_bulk_insert.py \
    -d "${INPUT_JSON}" \
    --database-name "${DB_NAME}" \
    --collection-name "${COLL_NAME}" \
    -w ${N_WORKERS} -c ${CHUNKSIZE} \
    --simple-records --debug

# create indices
for field in "instance_of" "languages" "id" "name"
do
    python $IO_SCRIPT_FOLDER/create_index.py -db "${DB_NAME}" -c "${COLL_NAME}" -f "${field}"
done

# create "subclasses" collection
python $IO_SCRIPT_FOLDER/wikidata_subclasses.py \
    --entity-ids "Q43229,Q5,Q82794" \
    -db ${DB_NAME} -c subclasses
