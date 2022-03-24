#!/usr/bin/env bash

set -euo pipefail

## Command line arguments
input_json=${1}
output_folder=${2}
n_workers=${3:-1}

## Constants
db_name=paranames_db_022822
collection_name=paranames
mongodb_port=27617
chunk_size=50000

# "all" for everything. alternatively: comma-separated list
langs="all" 

entity_types="PER,LOC,ORG"
default_format="tsv"
should_collapse_languages="no"
should_keep_intermediate_files="no"

## Ingest input JSON
python -m rich "[bold underline]Ingesting input JSON:[/]" -p
recipes/ingest.sh \
    $input_json \
    $db_name \
    $collection_name \
    $chunk_size \
    $n_workers \
    $mongodb_port

## Dump all entities in all languages
python -m rich "[bold underline]Dumping all PER/LOC/ORG from ${db_name}.${collection_name}:[/]" -p
recipes/dump.sh \
    $langs $output_folder $entity_types \
    $db_name $collection_name $mongodb_port \
    $should_collapse_languages \
    $should_keep_intermediate_files \
    $n_workers
