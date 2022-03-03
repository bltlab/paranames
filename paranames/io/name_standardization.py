#!/usr/bin/env python

from typing import Dict
import itertools as it

import click
import numpy as np
import pandas as pd
from paranames.util import read, write
import paranames.util.script as s
from p_tqdm import p_map
import orjson
import attr


def standardize_names(
    data: pd.DataFrame,
    language_column: str,
    alias_column: str,
    id_column: str,
    type_column: str,
    english_column: str,
    num_workers: int,
    chunksize: int,
    human_readable_lang_names: Dict[str, str],
    permuter_type: str,
    corpus_stats_output: str,
    debug_mode: bool = False,
    chunk_rows: bool = False,
    corpus_require_english: bool = False,
    corpus_filter_blank: bool = False,
    corpus_preserve_fastalign_data: bool = False,
    *args,
    **kwargs,
) -> pd.DataFrame:

    permuter_class = {
        "comma": s.PermuteFirstComma,
        "edit_distance": s.PermuteLowestDistance,
        "remove_parenthesis_permute_comma": s.RemoveParenthesisPermuteComma,
        "remove_parenthesis_edit_distance": s.RemoveParenthesisPermuteLowestDistance,
        "remove_parenthesis": s.ParenthesisRemover,
    }[permuter_type]

    num_rows, num_columns = data.shape

    if chunk_rows:
        corpus_chunks = (chunk for chunk in np.array_split(data, chunksize))
    else:
        corpus_chunks = (chunk for chunk in (data,))

    name_loader = s.TransliteratedNameLoader(
        language_column=language_column,
        wikidata_id_column=id_column,
        debug_mode=False,
    )
    names = list(
        it.chain.from_iterable(p_map(name_loader, corpus_chunks, num_cpus=num_workers))
    )

    # Only measure alignments when edit distance is involved
    should_align = bool("edit_distance" in permuter_type)

    print("[standardize_names] Creating pooled corpus...")
    pooled_corpus = s.Corpus(
        names=names,
        language="all",
        permuter_class=permuter_class,
        debug_mode=debug_mode,
        normalize_histogram=True,
        ignore_punctuation=True,
        ignore_numbers=False,
        align_with_english=should_align,
        fastalign_verbose=True,
        permuter_inplace=True,
        find_best_token_permutation=True,
        analyze_unicode=False,
        preserve_fastalign_data=corpus_preserve_fastalign_data,
        require_english=corpus_require_english,
        filter_out_blank=corpus_filter_blank,
        num_workers=num_workers,
    )

    print("[standardize_names] Computing corpus statistics...")

    def corpus_stats_rows():
        for lang, stats_per_lang in pooled_corpus.stats.items():
            lang_long = human_readable_lang_names.get(lang, lang)
            avg_alignments = stats_per_lang.mean_cross_alignments
            total_permuted = stats_per_lang.total_permuted
            total_surviving = stats_per_lang.total_surviving
            yield {
                "language": lang_long,
                "mean_crossing_alignments": avg_alignments,
                "n_changed": total_permuted,
                "n_unchanged": total_surviving,
            }

    write(
        data=corpus_stats_rows(),
        output_file=corpus_stats_output,
        io_format="tsv",
        index=False,
        mode="dict_writer",
        dict_writer_field_names=[
            "language",
            "mean_crossing_alignments",
            "n_changed",
            "n_unchanged",
        ],
        verbose=debug_mode,
        n_rows=len(pooled_corpus.stats.items()),
    )

    print("[standardize_names] Replacing old names...")
    clean_data = pd.DataFrame.from_records(
        (attr.asdict(n) for n in pooled_corpus.names),
        columns=[
            id_column,
            "english_text",
            "text",
            language_column,
            "conll_type",
            "anomalous",
            "is_unchanged",
        ],
    ).rename(
        columns={
            "english_text": english_column,
            "text": alias_column,
            "conll_type": type_column,
        }
    )
    data = clean_data

    # finally mask out rows with now empty labels
    labels_nonempty = data[alias_column].apply(lambda s: bool(s))
    data = data[labels_nonempty]

    return data


@click.command()
@click.option("--input-file", "-i")
@click.option("--output-file", "-o")
@click.option("--io-format", "-f", default="tsv")
@click.option("--id-column", "-id", default="wikidata_id")
@click.option("--type-column", "-t", default="type")
@click.option("--alias-column", "-a", default="alias")
@click.option("--english-column", "-e", default="eng")
@click.option("--language-column", "-l", default="language")
@click.option("--num-workers", type=int, default=2)
@click.option("--chunksize", type=int, default=15000)
@click.option("--human-readable-langs-path", required=True)
@click.option("--permuter-type", required=True, type=click.Choice(s.permuter_types))
@click.option("--corpus-require-english", is_flag=True, help="deprecated")
@click.option("--corpus-filter-blank", is_flag=True, help="deprecated")
@click.option("--debug-mode", is_flag=True)
@click.option("--corpus-stats-output", required=True)
@click.option("--chunk_rows", is_flag=True)
@click.option("--preserve-fastalign-data", is_flag=True)
def main(
    input_file,
    output_file,
    io_format,
    id_column,
    type_column,
    alias_column,
    english_column,
    language_column,
    num_workers,
    chunksize,
    human_readable_langs_path,
    permuter_type,
    corpus_require_english,
    corpus_filter_blank,
    debug_mode,
    corpus_stats_output,
    chunk_rows,
    preserve_fastalign_data,
):

    # read in human readable language names
    with open(human_readable_langs_path, encoding="utf8") as f:
        human_readable_lang_names = orjson.loads(f.read())

    # read in data and sort it by language

    if debug_mode:
        print("Reading data...")
    data = read(input_file, io_format=io_format)

    # need to sort by language to ensure ordered chunks

    if debug_mode:
        print("Sorting by language column...")
    data = data.sort_values(language_column)

    # standardize names
    if debug_mode:
        print("Standardizing names...")
    data = standardize_names(
        data,
        id_column=id_column,
        type_column=type_column,
        english_column=english_column,
        alias_column=alias_column,
        language_column=language_column,
        num_workers=num_workers,
        chunksize=chunksize,
        permuter_type=permuter_type,
        human_readable_lang_names=human_readable_lang_names,
        debug_mode=debug_mode,
        corpus_stats_output=corpus_stats_output,
        chunk_rows=chunk_rows,
        corpus_require_english=True,  # corpus_require_english,
        corpus_filter_blank=True,  # corpus_filter_blank,
        corpus_preserve_fastalign_data=preserve_fastalign_data,
    )

    # write to disk
    write(data, output_file, io_format=io_format)


if __name__ == "__main__":
    main()
