import multiprocessing as mp
import os
import pathlib
from functools import partial

import click
from rich import print

from paranames.util import read, write


def get_output_filename(
    input_path: str, language: str, use_subfolders: bool = False
) -> pathlib.Path:
    input_folder = os.path.dirname(input_path)
    input_file = os.path.basename(input_path)
    prefix, extension = os.path.splitext(input_file)

    if use_subfolders:
        output_path = f"{input_folder}/{language}/{prefix}_{language}{extension}"
    else:
        output_path = f"{input_folder}/{prefix}_{language}{extension}"

    return pathlib.Path(output_path)


def write_subset(lang_subset_tuple, input_file, use_subfolders, verbose, io_format):
    lang, subset = lang_subset_tuple
    output_file = get_output_filename(input_file, lang, use_subfolders)
    output_folder = pathlib.Path(os.path.dirname(output_file))

    if not output_folder.exists():
        if verbose:
            print(f"{output_folder} not found. Creating with mkdir...")
        output_folder.mkdir()

    write(subset, output_file, io_format)


@click.command()
@click.option("--lang-column", "-c", default="language", help="Language column")
@click.option("--input-file", "-i", required=True)
@click.option(
    "--io-format",
    "-f",
    type=click.Choice(["csv", "jsonl", "tsv"]),
    default="tsv",
    help="I/O format",
)
@click.option(
    "--use-subfolders",
    "-s",
    is_flag=True,
    help="Separate-language files should go in their own subfolders",
)
@click.option("--verbose", "-v", is_flag=True)
@click.option("--num-workers", default=1, type=int)
def main(
    lang_column, input_file, io_format, use_subfolders, verbose, num_workers
) -> None:

    data = read(input_file, io_format)

    lang_subset_tuples = [(lang, df) for lang, df in data.groupby(lang_column)]

    if num_workers == 1:
        for lang, filtered in lang_subset_tuples:
            write_subset(
                (lang, filtered),
                input_file=input_file,
                use_subfolders=use_subfolders,
                verbose=verbose,
                io_format=io_format,
            )
    else:
        _write_subset = partial(
            write_subset,
            input_file=input_file,
            use_subfolders=use_subfolders,
            verbose=verbose,
            io_format=io_format,
        )
        with mp.Pool(num_workers) as pool:
            print(f"Parallelizing output to {num_workers} workers")
            pool.map(_write_subset, lang_subset_tuples)


if __name__ == "__main__":
    main()
