#!/usr/bin/env bash

set -euo pipefail

HUMAN_READABLE_LANGS_PATH="$HOME/paranames/data/human_readable_lang_names.json"

usage () {
    echo "Usage: bash ./get_script_counts.sh TSV_PATH ALIAS_COL_IX LANG_COL_IX"
}

[ $# -lt 1 ] && usage && exit 1

analyze_most_common () {
    ## get all unique aliases
    tail +2 $DUMP | cut -f $ALIAS_COL_IX | sort | uniq > $ALIASES

    ## infer all paranames
    ./paranames/analysis/infer_script_most_common < $ALIASES > $SCRIPTS

    ## combine into big flie
    paste $ALIASES $SCRIPTS > $TSV
}


DUMP=$1
ALIAS_COL_IX=${2:-3}
LANG_COL_IX=${3:-4}

## use fast command line tools to cache all aliases and scripts in a temp directory
TMPDIR=$(dirname $DUMP)/script_information
mkdir -p $TMPDIR
echo $TMPDIR
ALIASES=$TMPDIR/all_aliases.txt
SCRIPTS=$TMPDIR/all_aliases_scripts.txt
LANG_SCRIPTS=$TMPDIR/all_languages_script_histograms.tsv
TSV=$TMPDIR/all_aliases_with_script.tsv


## get unique languages
LANGUAGES=$(tail +2 $DUMP | cut -f $LANG_COL_IX | sort | uniq | tr '\n' ' ')

analyze_most_common
