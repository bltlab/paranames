import csv
from typing import (
    List,
    Dict,
    Optional,
    Tuple,
    Set,
    DefaultDict,
)
from collections import defaultdict, OrderedDict

from tqdm import tqdm
from paranames.util import read
import pandas as pd
import numpy as np
import click
import attr

from multiprocessing import Pool


def output_matrix(matrix_dict, delimiter, languages, f, index_cols=None):
    """Unpacks the matrix_dict pivot table and adds columns for each language."""

    # need to rename id => wikidata_id due to a conflicting language code "id"

    if not index_cols:
        index_cols = ["wikidata_id", "eng", "type"]
    else:
        index_cols = [c if c != "id" else "wikidata_id" for c in index_cols]

    field_names = index_cols + [l for l in languages]
    writer = csv.DictWriter(
        f, delimiter=delimiter, fieldnames=field_names, extrasaction="ignore"
    )

    writer.writeheader()

    def rows(matrix_dict):
        for index_values, d in tqdm(matrix_dict.items()):

            row = {
                # TODO: update dumps to use wikidata_id column and remove this if-else
                (col if col != "id" else "wikidata_id"): value
                for col, value in zip(index_cols, index_values)
            }

            ## use sorted(languages) to make sure order is preserved
            row.update({l: d.get(l, "") for l in sorted(str(l) for l in languages)})

            yield row

    writer.writerows(rows(matrix_dict))


@attr.s
class ChunkProcessor:

    index_cols: Optional[List[str]] = attr.ib(repr=False)
    value_col: str = attr.ib(default="alias")
    language_col: str = attr.ib(default="language")

    def __attrs_post_init(self):

        if not self.index_cols:

            # By default, use wikidata dumping columns
            self.index_cols = ["wikidata_id", "type", "eng", self.language_col]

        assert (
            self.index_cols[-1] == self.language_col
        ), f"Last index column must be language_col={self.language_col}, got {self.index_cols[-1]}."

    def unravel(
        self, d: Dict[Tuple[str, str, str, str], str]
    ) -> List[Dict[Tuple[str, ...], Dict[str, str]]]:
        """Extracts language from the index, and outputs a list of dicts
        that map the index (minus language) to a dict of language -> value mappings."""

        out = []

        for index_keys, value in d.items():
            short_index, lang = tuple(index_keys[:-1]), index_keys[-1]
            out.append({short_index: {lang: value}})

        return out

    def __call__(self, data: pd.DataFrame):

        unique_langs: Set[str] = set()
        matrix_dict: DefaultDict[Tuple[str, ...], Dict[str, str]] = defaultdict(dict)

        chunk_dict = (
            data[data.wikidata_id.str.startswith("Q")]
            .set_index(self.index_cols)[self.value_col]
            .to_dict()
        )

        for entity_alias_dict in self.unravel(chunk_dict):
            for entity_index, values in entity_alias_dict.items():
                if np.nan in values:
                    raise ValueError(
                        "nan found in the languages, check nan handling in csv loading!"
                    )

                unique_langs = unique_langs.union(values)

                matrix_dict[entity_index].update(values)

        return matrix_dict, unique_langs


@click.command()
@click.option("--input-file", "-i", required=True)
@click.option("--output-file", "-o", required=True)
@click.option(
    "--io-format",
    "-f",
    type=click.Choice(["csv", "tsv", "jsonl"]),
    default="tsv",
)
@click.option("--chunksize", "-c", type=int, default=1000)
@click.option("--n-jobs", "-n", type=int, default=10)
@click.option("--index-columns", default="wikidata_id,type,eng,language")
@click.option("--language-column", default="language")
@click.option("--value-column", default="alias")
def main(
    input_file,
    output_file,
    io_format,
    chunksize,
    n_jobs,
    index_columns,
    language_column,
    value_column,
):

    data_chunks = read(input_file, io_format, chunksize=chunksize)
    full_matrix_dict = defaultdict(dict)
    index_cols = index_columns.split(",")

    chunk_processor = ChunkProcessor(
        index_cols=index_cols,
        language_col=language_column,
        value_col=value_column,
    )

    print("Computing all the disjoint matrix dicts")
    # TODO: use p_map
    with Pool(n_jobs) as pool:
        matrix_dicts_unique_languages = pool.map(
            func=chunk_processor, iterable=data_chunks
        )

    print("Done! Now joining them to one big dict")
    unique_langs = set()

    for md, ul in matrix_dicts_unique_languages:
        unique_langs = unique_langs.union(ul)
        full_matrix_dict.update(md)

    # convert to OrderedDict to preserve order
    full_matrix_dict = OrderedDict(full_matrix_dict)

    print(f"Done! Now writing to disk under {output_file}")
    with open(output_file, "w") as tsv_out:
        output_matrix(
            full_matrix_dict,
            delimiter="\t",
            languages=unique_langs,
            f=tsv_out,
            index_cols=[c for c in index_cols if c != language_column],
        )


if __name__ == "__main__":
    main()
