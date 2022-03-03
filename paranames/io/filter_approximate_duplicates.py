#!/usr/bin/env python

from typing import Tuple

import click
import pandas as pd
from paranames.util import read, write
from p_tqdm import p_map

import datasketch as ds

pd.set_option("display.max_rows", None)


def approximate_jaccard_similarity(word1: str, word2: str) -> float:
    m1, m2 = ds.MinHash(), ds.MinHash()

    for c in word1:
        m1.update(c.encode("utf-8"))

    for c in word2:
        m2.update(c.encode("utf-8"))

    return m1.jaccard(m2)


def filter_names_jaccard(
    data: pd.DataFrame,
    threshold: float,
    english_column: str,
    alias_column: str,
    num_workers: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    approx_similarities = pd.Series(
        p_map(
            approximate_jaccard_similarity,
            data[english_column],
            data[alias_column],
            num_cpus=num_workers,
        )
    )

    not_too_similar_mask = approx_similarities < threshold

    output = data[not_too_similar_mask]
    flagged = data[~not_too_similar_mask]

    return output, flagged


@click.command()
@click.option("--input-file", "-i")
@click.option("--output-file", "-o")
@click.option("--io-format", "-f", default="tsv")
@click.option("--alias-column", "-a", default="alias")
@click.option("--english-column", "-e", default="eng")
@click.option("--similarity-threshold", "-s", default=0.95)
@click.option("--debug-mode", is_flag=True)
@click.option("--num-workers", type=int, default=16)
def main(
    input_file,
    output_file,
    io_format,
    alias_column,
    english_column,
    similarity_threshold,
    debug_mode,
    num_workers,
):

    # read in data and sort it by language

    if debug_mode:
        print("Reading data...")
    data = read(input_file, io_format=io_format)

    if debug_mode:
        print("Filtering...")
    filtered_data, too_similar = filter_names_jaccard(
        data,
        threshold=similarity_threshold,
        english_column=english_column,
        alias_column=alias_column,
        num_workers=num_workers,
    )

    if debug_mode:
        print(f"{too_similar.shape[0]} / {data.shape[0]} flagged as duplicates")
        print(
            too_similar[too_similar[english_column] != too_similar[alias_column]][
                [english_column, alias_column]
            ]
        )

    # write to disk
    write(filtered_data, output_file, io_format=io_format)


if __name__ == "__main__":
    main()
