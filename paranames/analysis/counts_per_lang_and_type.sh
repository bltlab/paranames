#!/usr/bin/env bash

[ $# -lt 1 ] && echo "Need TSV file!" && exit 1

tsv_file=$1
nlines=$(wc -l $tsv_file | cut -f1 -d' ')

printf "count,language_code,type\n"
tail +2 $tsv_file |         # skip header row of TSV file
    cut -f4,5 |             # grab language and type
    tqdm --total=$nlines |  #
    sort |                  # sort alphabetically
    tqdm |                  #
    uniq -c |               # count repeated lines
    sort -nr |              # numeric sort + reverse order
    sed 's/^\s*//' |        # get rid of leading whitespace
    sed "s/\s+$//g" |       # get rid of trailing whitespace
    tr ' \t' ',' |          # convert tabs and spaces to commas
    csvgrep -c 2 -r '.+'    # only retain rows that have nonempty 
                            # values for the "language" column.
