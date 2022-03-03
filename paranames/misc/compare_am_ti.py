import sys
import os
import math
import csv
from typing import IO, Generator, List, Dict, Any, Union, Iterable

import pandas as pd
import numpy as np
import click


def read(input_file: str, io_format: str) -> pd.DataFrame:
    if io_format == "csv":
        return pd.read_csv(input_file, encoding="utf-8")
    else:
        return pd.read_json(input_file, "records", encoding="utf-8")


def write(data: pd.DataFrame, output_file: str, io_format: str) -> None:
    if io_format == "csv":
        return data.to_csv(output_file, encoding="utf-8", index=False)
    else:
        return data.to_json(output_file, "records", encoding="utf-8", index=False)


def get_output_filename(input_file: str, language: str) -> str:
    prefix, extension = os.path.splitext(input_file)

    return f"{prefix}.{language}{extension}"


@click.command()
@click.option("--input-file", "-i", required=True)
@click.option("--output-file", "-o", required=True)
@click.option("--lang-column", "-c", default="language", help="Language column")
@click.option("--alias-column", "-c", default="alias", help="Alias column")
@click.option(
    "--io-format",
    "-f",
    type=click.Choice(["csv", "jsonl"]),
    default="csv",
    help="I/O format",
)
def main(input_file, output_file, lang_column, alias_column, io_format):

    data = read(input_file, io_format).rename(columns={"name": "eng"})
    uniq_langs = data[lang_column].unique()

    def deduplicate(subset_df):
        out = {}

        for ix, row in subset_df.reset_index().iterrows():
            lang = row["language"]
            out[lang] = row[lang]
            out["id"] = row["id"]
            out["type"] = row["type"]
            out["eng"] = row["eng"]
            out["url"] = row["url"]

        return out

    def alias_to_lang_col(row):
        row[row[lang_column]] = row[alias_column]

        return row

    def generate_url(_id):
        return f"https://www.wikidata.org/wiki/{_id}"

    aggregated = data.apply(alias_to_lang_col, axis=1)
    aggregated["url"] = aggregated.wikidata_id.apply(generate_url)
    column_ordering = ["id", "type", "eng"] + [lang for lang in uniq_langs] + ["url"]
    aggregated = aggregated[column_ordering + ["language"]]
    rows = aggregated.groupby(["id", "type"]).apply(deduplicate).tolist()
    aggregated = pd.DataFrame(rows)[column_ordering]

    aggregated["ti_is_eng"] = aggregated.ti == aggregated.eng
    aggregated["am_is_eng"] = aggregated.am == aggregated.eng
    aggregated["ti_is_am"] = aggregated.am == aggregated.ti

    final_column_ordering = (
        ["id", "type", "eng"]
        + [lang for lang in uniq_langs]
        + ["ti_is_eng", "am_is_eng", "ti_is_am", "url"]
    )
    aggregated = aggregated[final_column_ordering]

    write(aggregated, output_file, io_format)


if __name__ == "__main__":
    main()
