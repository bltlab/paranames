#!/usr/bin/env bash

set -euo pipefail

HUMAN_READABLE_LANGS_PATH="$HOME/paranames/data/human_readable_lang_names_from_sparql.json"

usage () {
    echo "Usage: bash ./script_analysis.sh TSV_PATH OUTPUT_FILE ALIAS_COL_IX LANG_COL_IX"
}

[ $# -lt 2 ] && usage && exit 1

analyze_most_common () {

    ## get number of lines
    nlines=$(wc -l $DUMP | cut -f1 -d' ')

    ## get all unique aliases
    tail +2 $DUMP | cut -f $ALIAS_COL_IX | tqdm --total=$nlines | sort | uniq > $ALIASES
    n_aliases=$(wc -l $ALIASES | cut -f1 -d' ')

    ## infer all paranames
    cat $ALIASES | tqdm --total $n_aliases | ./paranames/analysis/infer_script_most_common > $SCRIPTS

    ## combine into big flie
    paste $ALIASES $SCRIPTS > $TSV

    ## then use python script to analyze the script statistics, entropy etc
    python paranames/analysis/compute_script_entropy.py \
        --input-file $DUMP \
        --cache-path $TSV \
        --output-file $OUTPUT_FILE \
        --human-readable-langs-path "$HUMAN_READABLE_LANGS_PATH"
}


DUMP=$1
OUTPUT_FILE=$2
ALIAS_COL_IX=${3:-3}
LANG_COL_IX=${4:-4}

## use fast command line tools to cache all aliases and scripts in a temp directory
TMPDIR=$(mktemp -d)
echo $TMPDIR
ALIASES=$TMPDIR/all_aliases.txt
SCRIPTS=$TMPDIR/all_aliases_scripts.txt
LANG_SCRIPTS=$TMPDIR/all_languages_script_histograms.tsv
TSV=$TMPDIR/all_aliases_with_script.tsv


## get unique languages
#LANGUAGES=$(tail +2 $DUMP | cut -f $LANG_COL_IX | sort | uniq | tr '\n' ' ')

analyze_most_common
