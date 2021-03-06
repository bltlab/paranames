#!/usr/bin/env python

import re
from pathlib import Path
from typing import Pattern, Union

import click
import orjson
import pandas as pd
from rich import print
from rich.progress import track

from paranames.util import read, write


def keep_above_threshold(
    data: pd.DataFrame, language_column: str, threshold: int
) -> pd.DataFrame:
    old_nrows = data.shape[0]
    num_names_per_lang = data[language_column].value_counts().to_dict()
    lang_big_enough = data[language_column].apply(
        lambda l: num_names_per_lang.get(l, 0) > threshold
    )

    filtered_out_langs = data[~lang_big_enough][language_column].unique()

    for lang in filtered_out_langs:
        cnt = num_names_per_lang[lang]
        print(f'[postprocess] Filtered out: "{lang}" ({cnt} <= {threshold})')
    output = data[lang_big_enough]
    new_nrows = output.shape[0]

    print("Small language filtering complete")
    print(f"No. of rows, original: {old_nrows}")
    print(f"No. of rows, filtered: {new_nrows}")
    print(f"Rows removed = {old_nrows - new_nrows}")

    return output


def apply_entity_disambiguation_rules(
    data: pd.DataFrame,
    id_column: str = "wikidata_id",
    type_column: str = "type",
) -> pd.DataFrame:

    # count how many types for each id
    id_to_ntypes_df = (
        data[[id_column, type_column]]
        .drop_duplicates()
        .groupby(id_column)
        .type.size()
        .reset_index()
        .rename(columns={type_column: "n_types"})
    )

    # join this to the original data frame
    old_nrows = data.shape[0]
    data = data.merge(id_to_ntypes_df, on=id_column)

    # if id is in this dict, it will have several types
    id_to_types = (
        data[data.n_types > 1][[id_column, type_column]]
        .drop_duplicates()
        .groupby(id_column)
        .apply(lambda df: "-".join(sorted(df.type.unique())))
    )

    # if there are no duplicates, get rid of n_types column and return

    if id_to_types.empty:
        data = data.drop(columns="n_types")

        return data
    else:
        id_to_types = id_to_types.to_dict()

    # encode actual disambiguation rules
    entity_disambiguation_rules = {
        "LOC-ORG": "LOC",
        "LOC-ORG-PER": "ORG",
        "ORG-PER": "ORG",
        "LOC-PER": "PER",
    }

    # compose the above two relations
    id_to_canonical_type = {
        _id: entity_disambiguation_rules.get(type_str)
        for _id, type_str in id_to_types.items()
    }

    # replace with canonical types, non-ambiguous ones get None
    canonical_types = data[id_column].apply(
        lambda _id: id_to_canonical_type.get(_id, None)
    )

    # put the old non-ambiguous types back in
    new_types = [
        old_type if new_type is None else new_type
        for old_type, new_type in zip(data.type, canonical_types)
    ]

    data[type_column] = new_types

    # finally drop the extra column we created
    data = data.drop(columns="n_types")

    # also drop duplicate rows
    data = data.drop_duplicates()

    # final check to make sure no id has more than 1 type
    assert all(
        data[[id_column, type_column]].drop_duplicates().groupby(id_column).type.size()
        == 1
    )
    new_nrows = data.shape[0]

    # print out some information to the user
    print("Disambiguation complete")
    print(f"No. of rows, original: {old_nrows}")
    print(f"No. of rows, filtered: {new_nrows}")
    print(f"Rows removed = {old_nrows - new_nrows}")

    return data


def remove_parentheses(
    data: pd.DataFrame, english_column: str, label_column: str, regex: Pattern
) -> pd.DataFrame:
    data[english_column] = data[english_column].apply(
        lambda e: regex.sub("", e).strip()
    )
    data[label_column] = data[label_column].apply(lambda a: regex.sub("", a).strip())

    return data


def collapse_language_codes(data, language_column: str = "language"):
    print(
        f"[collapse_language_codes] Collapsing all language codes across {data.shape[0]}..."
    )
    collapsed = [l.split("-")[0] for l in data[language_column]]
    data[language_column] = collapsed

    return data


def rename_language_codes(
    data: pd.DataFrame,
    language_column: str,
    languages_to_rename_path: Union[Path, str],
) -> pd.DataFrame:
    print("[rename_language_codes] Renaming language codes...")
    with open(languages_to_rename_path, encoding="utf-8") as fin:
        rewrite_these = orjson.loads(fin.read())

    new_lang_column = []
    renamed = set()
    for lc in track(data[language_column], total=data.shape[0]):
        if lc in rewrite_these and lc not in renamed:
            print(f"[collapse_language_codes] Renaming {lc} to {rewrite_these[lc]}")
            renamed.add(lc)
        new_lang_column.append(rewrite_these.get(lc, lc))

    data[language_column] = new_lang_column
    return data


@click.command()
@click.option("--input-file", "-i")
@click.option("--output-file", "-o")
@click.option("--io-format", "-f", default="tsv")
@click.option("--id-column", "-id", default="wikidata_id")
@click.option("--type-column", "-t", default="type")
@click.option("--label-column", "-a", default="label")
@click.option("--english-column", "-e", default="name")
@click.option("--language-column", "-l", default="language")
@click.option("--min-names-threshold", "-m", default=0)
@click.option(
    "--languages-to-rename-path",
    default="./data/language_codes_to_rename.json",
    type=click.Path(exists=True, dir_okay=False, readable=True),
)
@click.option("--should-disambiguate-entity-types", "-d", is_flag=True, default=False)
@click.option("--should-remove-parentheses", "-r", is_flag=True, default=False)
@click.option("--should-collapse-languages", "-c", is_flag=True, default=False)
def main(
    input_file,
    output_file,
    io_format,
    id_column,
    type_column,
    label_column,
    english_column,
    language_column,
    min_names_threshold,
    languages_to_rename_path,
    should_disambiguate_entity_types,
    should_remove_parentheses,
    should_collapse_languages,
):

    # read in data
    data = read(input_file, io_format=io_format).astype(str)

    # drop rows that are not entities (e.g. P-ids)
    data = data[data[id_column].str.startswith("Q")]

    # change <english_column> to "eng"
    data = data.rename(columns={english_column: "eng"})

    # rename certain language codes
    data = rename_language_codes(
        data,
        language_column=language_column,
        languages_to_rename_path=languages_to_rename_path,
    )

    # drop languages with fewer than minimum threshold of names

    if min_names_threshold > 0:
        data = keep_above_threshold(data, language_column, min_names_threshold)

    # filter rows using entity disambiguation rules

    if should_disambiguate_entity_types:
        data = apply_entity_disambiguation_rules(
            data, id_column=id_column, type_column=type_column
        )

    # remove parentheses from english and label columns

    if should_remove_parentheses:
        re_parenthesis = re.compile(r"\(.*\)")
        data = remove_parentheses(
            data, english_column="eng", label_column=label_column, regex=re_parenthesis
        )

    # collapse sub-languages into top-level language codes if needed

    if should_collapse_languages:
        data = collapse_language_codes(data, language_column=language_column)

    # write to disk
    write(data, output_file, io_format=io_format)


if __name__ == "__main__":
    main()
