#!/usr/bin/env python

from typing import Dict, Tuple, Generator, Any, Iterable
from functools import partial
from pathlib import Path
import tempfile
import multiprocessing as mp

import click
import pandas as pd
from rich import print
from paranames.util import read, write
import paranames.util.script as s


def validate_name(
    name_language_tuple: Tuple[str, str],
    allowed_scripts: Dict[str, Dict[str, str]],
    icu_mode: bool = False,
) -> bool:
    name, language = name_language_tuple

    ua = s.UnicodeAnalyzer(ignore_punctuation=True, ignore_numbers=True)

    if language not in allowed_scripts:
        return True

    return (
        ua.most_common_icu_script(name)

        if icu_mode
        else ua.most_common_unicode_block(name)
    ) in allowed_scripts[language]


def validate_names(tuples, allowed_scripts, icu_mode=True, num_workers=60):
    _validate_name = partial(
        validate_name, allowed_scripts=allowed_scripts, icu_mode=icu_mode
    )
    with mp.Pool(num_workers) as pool:
        mask_iterable = pool.map(_validate_name, tuples)

    return mask_iterable


def standardize_script_manual(
    data, scripts_file, alias_column, language_column, num_workers, *args, **kwargs
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    scripts = read(scripts_file, "tsv")
    allowed_scripts_per_lang = {
        lang: set(s.strip() for s in scr.split(","))

        for lang, scr in zip(scripts.language_code, scripts.scripts_to_keep)
    }

    print(f"[standardize_script_manual] Creating tuples of (name, language)")
    name_lang_tuples = zip(data[alias_column], data[language_column])

    print(
        f"[standardize_script_manual] Creating valid name mask using {num_workers} workers"
    )
    mask_iterable = validate_names(
        name_lang_tuples,
        allowed_scripts=allowed_scripts_per_lang,
        icu_mode=True,
        num_workers=num_workers,
    )

    valid_rows_mask = pd.Series(mask_iterable, index=data.index)

    valid = data[valid_rows_mask]
    filtered = data[~valid_rows_mask]

    return valid, filtered


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
@click.option("--write-filtered-names", is_flag=True)
@click.option("--filtered-names-output-file", default="")
@click.option("--compute-script-entropy", is_flag=True)
@click.option(
    "--scripts-file",
    "-s",
    default="~/paranames/data/scripts_to_keep.tsv",
)
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
    write_filtered_names,
    filtered_names_output_file,
    compute_script_entropy,
    scripts_file,
):

    # read in data and sort it by language
    data = read(input_file, io_format=io_format)

    data, filtered = standardize_script_manual(
        data,
        id_column=id_column,
        type_column=type_column,
        language_column=language_column,
        num_workers=num_workers,
        chunksize=chunksize,
        scripts_file=scripts_file,
        alias_column=alias_column,
    )

    if write_filtered_names:

        if not filtered_names_output_file:
            _, filtered_names_output_file = tempfile.mkstemp()
        filtered_names_path = Path(filtered_names_output_file)
        containing_folder = filtered_names_path.parents[0]

        if not containing_folder.exists():
            filtered_names_path.mkdir(parents=True)

        write(
            filtered,
            filtered_names_path,
            io_format="tsv",
        )

        print(f"Filtered names written to {filtered_names_output_file}")

    # write to disk
    write(data, output_file, io_format=io_format)


if __name__ == "__main__":
    main()
