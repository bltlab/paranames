#!/usr/bin/env bash

set -euo pipefail

## Command line arguments
input_json=${1}
output_folder=${2}
n_workers=${3:-1}

## Constants
db_name=paranames_db
collection_name=paranames
chunk_size=20000

# "all" for everything. alternatively: comma-separated list
langs="all" 

entity_types="PER,LOC,ORG"
default_format="tsv"
should_collapse_languages="no"

## Ingest input JSON
rich "[bold underline]Ingesting input JSON:[/]" -p
recipes/ingest.sh \
    $input_json \
    $db_name \
    $collection_name \
    $chunk_size \
    $n_workers

## Dump all entities in all languages
rich "[bold underline]Dumping all PER/LOC/ORG:[/]" -p
recipes/dump.sh \
    $langs $output_folder $entity_types \
    $db_name $collection_name \
    $should_collapse_languages \
    $n_workers
