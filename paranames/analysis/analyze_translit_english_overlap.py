from typing import Iterable, List
from collections import Counter

from paranames.util import read
from tqdm import tqdm
import pandas as pd
import orjson
import click

IO_FORMATS = ["tsv", "csv"]
DEFAULT_SEED = 1917


def load_file(input_file: str, io_format: str) -> pd.DataFrame:
    dump = read(input_file, io_format)
    return dump


def orjson_load(p: str):
    with open(p, encoding="utf-8") as f:
        return orjson.loads(f.read())


def a_contains_b(a_iter: Iterable[str], b_iter: Iterable[str]) -> List[bool]:
    return [a in b for a, b in zip(a_iter, b_iter)]


@click.command()
@click.option(
    "--input-file",
    "-i",
    type=click.Path(
        exists=True, file_okay=True, dir_okay=False, readable=True, allow_dash=True
    ),
)
@click.option("--io-format", "-f", type=click.Choice(IO_FORMATS), default="tsv")
@click.option(
    "--alias-column",
    default="alias",
    help="Name of columns containing non-English names",
)
@click.option(
    "--eng-column", default="eng", help="Name of column containing English names"
)
@click.option(
    "--language-column", default="language", help="Name of column containing language"
)
@click.option("--human-readable-langs-path", required=True)
@click.option(
    "--random-seed",
    default=DEFAULT_SEED,
    type=int,
    help="Random seed for subsampling rows if needed.",
)
@click.option("--analyze-english-overlap-per-lang", is_flag=True)
@click.option("--analyze-lengths", is_flag=True)
def main(
    input_file: str,
    io_format: str,
    alias_column: str,
    eng_column: str,
    language_column: str,
    human_readable_langs_path: str,
    random_seed: int,
    analyze_english_overlap_per_lang: bool,
    analyze_lengths: bool,
) -> None:

    hrl = orjson_load(human_readable_langs_path)
    dump = load_file(input_file, io_format)

    n_rows, _ = dump.shape
    print(f"There are a total of {n_rows} rows")

    print("How many aliases are exactly equal to English?")
    dump["alias_equals_eng"] = (dump[alias_column] == dump[eng_column]).astype(int)
    n_equal_eng = dump.alias_equals_eng.sum()
    frac_equal_eng = round(100 * dump.alias_equals_eng.mean(), 2)
    print(f"[global] {n_equal_eng} / {n_rows} ({frac_equal_eng} %)")

    if analyze_english_overlap_per_lang:
        fracs_per_language = (
            dump.groupby(language_column).alias_equals_eng.describe().reset_index()
        )

        for _, row in fracs_per_language.iterrows():
            lang = row["language"]
            n_rows = int(row["count"])
            frac_rows = row["mean"]
            n_equal_eng = int(n_rows * frac_rows)
            frac_equal_eng = round(100 * frac_rows, 2)
            print(
                f"[{hrl.get(lang, lang)}] {n_equal_eng} / {n_rows} ({frac_equal_eng} %)"
            )

    if analyze_lengths:
        # We can safely copy here since (presumably) this is only a small fraction of total rows
        not_equal_eng = dump[~dump.alias_equals_eng].copy()
        n_not_equal, _ = not_equal_eng.shape

        print("Computing alias lengths")
        not_equal_eng["alias_length"] = not_equal_eng[alias_column].str.len()
        print("Computing english lengths")
        not_equal_eng["eng_length"] = not_equal_eng[eng_column].str.len()

        print("Computing length differences")
        not_equal_eng["length_diff"] = (
            not_equal_eng.alias_length - not_equal_eng.eng_length
        )

        not_equal_eng["alias_category"] = [
            "equal" if d == 0 else ("shorter" if d < 0 else "longer")
            for d in not_equal_eng.length_diff
        ]
        print("Are aliases longer/shorter than English?")
        alias_category_counts = Counter(not_equal_eng.alias_category)
        for cat, n_cat in alias_category_counts.items():
            frac_cat = round(100 * (n_cat / n_not_equal), 2)
            print(f"[{cat}] {n_cat} / {n_not_equal} ({frac_cat}%)")

        not_equal_eng["eng_contains_alias"] = a_contains_b(
            not_equal_eng[alias_column], not_equal_eng[eng_column]
        )
        not_equal_eng["alias_contains_eng"] = a_contains_b(
            not_equal_eng[eng_column], not_equal_eng[alias_column]
        )

        print("Are aliases substrings of English or vice versa?")
        print(
            not_equal_eng.groupby(
                ["alias_category", "eng_contains_alias", "alias_contains_eng"]
            ).size()
        )


if __name__ == "__main__":
    main()
