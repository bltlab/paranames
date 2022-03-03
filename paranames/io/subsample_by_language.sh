#!/usr/bin/env bash

# NOTE: the use of "rg" with a relatively simple regex can cause other fields besides the language field
# to be matched as well if another field has a value equal to the language in question

input_tsv=$1
output_tsv=${2:-subsampled.tsv}
n_per_lang=${3:-10000}

echo "Getting header"
header=$(head -n 1 ${input_tsv})

echo "Getting unique languages in ${input_tsv}"
unique_languages=$(cut -f4 $input_tsv | sort | uniq | tr '\n' ' ')
temp_output_folder=$(mktemp -d subsampled_XXXXX -p /tmp)

echo "Subsampling..."
for lang in $unique_languages
do
    tail +2 ${input_tsv} | rg -N "\t${lang}\t" | shuf -n $n_per_lang > $temp_output_folder/subsample_$lang.tsv &
done
wait

echo "Concatenating..."
echo "${header}" > ${output_tsv}
cat ${temp_output_folder}/subsample*.tsv >> ${output_tsv}
rm -rf ${temp_output_folder}
