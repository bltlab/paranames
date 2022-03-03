#!/usr/bin/env python

from collections import defaultdict, Counter
from typing import Dict, Set, DefaultDict

import click
from paranames.util import read, orjson_dump
from rich import print
from rich.progress import track


def compute_overlap_counts(
    df,
    wikidata_id_column="wikidata_id",
    conll_type_column="type",
    language_column="language",
) -> Dict[str, int]:
    counts: DefaultDict[str, Set[str]] = defaultdict(set)
    for wikidata_id, conll_type in track(
        zip(df[wikidata_id_column], df[conll_type_column]), total=df.shape[0]
    ):
        counts[wikidata_id].add(conll_type)

    overlap_counts = Counter("-".join(sorted(types)) for types in track(counts.values(), total=len(counts)))
    return overlap_counts


@click.command()
@click.option(
    "--input-file",
    "-i",
    required=True,
    type=click.Path(
        exists=True, file_okay=True, dir_okay=False, readable=True, allow_dash=True
    ),
)
@click.option(
    "--input-format",
    required=True,
    type=click.Choice(["tsv", "csv"]),
)
@click.option(
    "--output-file",
    "-o",
    required=True,
    type=click.Path(file_okay=True, dir_okay=False, readable=True, allow_dash=True),
)
@click.option(
    "--output-format",
    default="json",
    type=click.Choice(["json"]),
)
@click.option("--id-column", default="wikidata_id")
@click.option("--type-column", default="type")
@click.option("--language-column", default="language")
def main(
    input_file,
    input_format,
    output_file,
    output_format,
    id_column,
    type_column,
    language_column,
):
    print(f"[compute_overlap_counts] Loading dump from {input_file}")
    df = read(input_file, input_format)
    print("[compute_overlap_counts] Computing overlap counts...")
    overlap_counts = compute_overlap_counts(
        df,
        wikidata_id_column=id_column,
        conll_type_column=type_column,
        language_column=language_column,
    )
    print(f"[compute_overlap_counts] Writing to {output_file}")
    with open(output_file, "w", encoding="utf-8") as fout:
        fout.write(orjson_dump(overlap_counts))


if __name__ == "__main__":
    main()
