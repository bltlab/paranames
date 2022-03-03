#!/usr/bin/env python

# Computes basic statistics based on filtered names and

from typing import Iterable
from pathlib import Path

from sklearn.metrics import (
    confusion_matrix,
    classification_report,
    precision_score,
    recall_score,
    f1_score,
    accuracy_score,
)
from paranames.util import read, orjson_dump
from tqdm import tqdm
import editdistance
import pandas as pd
import numpy as np
import click

SCRIPT_STANDARDIZATION_CHUNKSIZE = 10
NAME_STANDARDIZATION_CHUNKSIZE = 30

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)


def f1(src: str, tgt: str) -> float:
    def lcs(src: str, tgt: str) -> float:
        lcs = 0.5 * ((len(src) + len(tgt)) - editdistance.eval(src, tgt))

        return lcs

    try:
        rec = lcs(src, tgt) / len(tgt)
    except ZeroDivisionError:
        rec = 0
    prec = lcs(src, tgt) / len(src)
    try:
        return 2 * ((rec * prec) / (rec + prec))
    except ZeroDivisionError:
        return 0


def mean_f1(references: Iterable[str], hypotheses: Iterable[str]) -> float:
    return 100 * np.mean([f1(ref, hyp) for ref, hyp in zip(references, hypotheses)])


def script_standardization_stats(
    filtered_names_tsv: pd.DataFrame,
    script_anecdata_folder: str,
    id_column: str,
    language_column: str,
) -> None:
    unique_languages = set()
    script_anecdata_path = Path(script_anecdata_folder)
    script_anecdata_files = script_anecdata_path.glob("anecdata_*.tsv")
    for f in script_anecdata_files:
        unique_languages.add(str(f.name).replace("anecdata_", "").replace(".tsv", ""))
    removed_ids_per_lang = {
        language: set(
            filtered_names_tsv[filtered_names_tsv[language_column] == language][
                id_column
            ]
        )
        for language in unique_languages
    }

    stats = {"global": {}}
    y_true_global = []
    y_pred_global = []
    for lang in sorted(unique_languages):
        anecdata_tsv_path = script_anecdata_path / f"anecdata_{lang}.tsv"
        tsv = read(anecdata_tsv_path, "tsv", chunksize=SCRIPT_STANDARDIZATION_CHUNKSIZE)
        should, shouldnt = next(tsv), next(tsv)
        dont_remove_ids, should_remove_ids = set(should[id_column]), set(
            shouldnt[id_column]
        )
        removed_ids = removed_ids_per_lang[lang]

        stats[lang] = {}
        y_true = [False for _id in dont_remove_ids] + [
            True for _id in should_remove_ids
        ]
        y_pred = [_id in removed_ids for _id in dont_remove_ids] + [
            _id in removed_ids for _id in should_remove_ids
        ]
        y_true_global.extend(y_true)
        y_pred_global.extend(y_pred)
        acc = accuracy_score(y_true, y_pred)
        prec = precision_score(y_true, y_pred, pos_label=True)
        rec = recall_score(y_true, y_pred, pos_label=True)
        f1 = f1_score(y_true, y_pred, pos_label=True)
        stats[f"{lang}"]["accuracy"] = acc
        stats[f"{lang}"]["precision"] = prec
        stats[f"{lang}"]["recall"] = rec
        stats[f"{lang}"]["f1"] = f1

    acc = accuracy_score(y_true_global, y_pred_global)
    prec = precision_score(y_true_global, y_pred_global, pos_label=True)
    rec = recall_score(y_true_global, y_pred_global, pos_label=True)
    f1 = f1_score(y_true_global, y_pred_global, pos_label=True)
    stats["global"]["accuracy"] = acc
    stats["global"]["precision"] = prec
    stats["global"]["recall"] = rec
    stats["global"]["f1"] = f1

    print(stats)


def name_standardization_stats(
    final_tsv: pd.DataFrame,
    name_anecdata_folder: str,
    id_column: str,
    language_column: str,
) -> None:
    print("Indexing dump with wikidata ID and language")
    final_tsv.set_index([language_column, id_column], inplace=True)

    unique_languages = set()
    name_anecdata = Path(name_anecdata_folder)
    all_anecdata_aliases = []
    all_final_tsv_aliases = []
    should_reorder_files = (name_anecdata / "should_reorder").glob("anecdata_*.tsv")
    shouldnt_reorder_files = (name_anecdata / "shouldnt_reorder").glob("anecdata_*.tsv")
    for file_list in (should_reorder_files, shouldnt_reorder_files):
        for f in file_list:
            unique_languages.add(str(f.name).split("_")[1].replace(".tsv", ""))

    f1_scores = {}
    accuracies = {}
    for language in tqdm(sorted(unique_languages)):
        print(language)
        should_reorder_tsv = read(
            name_anecdata / "should_reorder" / f"anecdata_{language}.tsv", "tsv"
        )
        shouldnt_reorder_tsv = read(
            name_anecdata / "shouldnt_reorder" / f"anecdata_{language}.tsv",
            "tsv",
        )
        whole_anecdata_for_lang = (
            pd.concat([should_reorder_tsv, shouldnt_reorder_tsv], ignore_index=True)
            .set_index([language_column, id_column])
            .loc[language]
        )
        anecdata_aliases = whole_anecdata_for_lang.alias.tolist()

        final_tsv_lang_subset = final_tsv.loc[language]
        try:
            final_tsv_lang_id_subset = final_tsv_lang_subset.loc[
                whole_anecdata_for_lang.index
            ]
        except:
            import ipdb

            ipdb.set_trace()

        final_tsv_aliases = final_tsv_lang_id_subset.alias.tolist()
        all_anecdata_aliases.extend(anecdata_aliases)
        all_final_tsv_aliases.extend(final_tsv_aliases)

        references = anecdata_aliases
        hypotheses = final_tsv_aliases

        accuracy = 100 * np.mean([int(r == h) for r, h in zip(references, hypotheses)])
        accuracies[f"{language}"] = accuracy

        mean_f1_score = mean_f1(references, hypotheses)
        f1_scores[f"{language}"] = mean_f1_score

    references = all_anecdata_aliases
    hypotheses = all_final_tsv_aliases
    accuracy = 100 * np.mean([int(r == h) for r, h in zip(references, hypotheses)])
    accuracies["global"] = accuracy
    mean_f1_score = mean_f1(references, hypotheses)
    f1_scores["global"] = mean_f1_score
    print("Accuracy:")
    print(accuracies)
    print("F1:")
    print(f1_scores)


@click.command()
@click.option("--script-standardization", is_flag=True)
@click.option("--name-standardization", is_flag=True)
@click.option("--filtered-names-tsv", type=click.Path())
@click.option("--final-resource-tsv", type=click.Path())
@click.option("--script-anecdata-folder", type=click.Path())
@click.option("--name-anecdata-folder", type=click.Path())
@click.option("--id-column", type=str, default="wikidata_id")
@click.option("--language-column", type=str, default="language")
def main(
    script_standardization,
    name_standardization,
    filtered_names_tsv,
    final_resource_tsv,
    script_anecdata_folder,
    name_anecdata_folder,
    id_column,
    language_column,
):

    if script_standardization:
        filtered_names_tsv_file = read(filtered_names_tsv, "tsv")
        script_standardization_stats(
            filtered_names_tsv_file,
            script_anecdata_folder,
            id_column=id_column,
            language_column=language_column,
        )

    if name_standardization:
        final_tsv_file = read(final_resource_tsv, "tsv")
        name_standardization_stats(
            final_tsv_file,
            name_anecdata_folder,
            id_column=id_column,
            language_column=language_column,
        )


if __name__ == "__main__":
    main()
