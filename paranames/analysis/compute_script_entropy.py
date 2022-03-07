import pandas as pd
import click
import orjson

from paranames.util import read
import dask.dataframe as dd
import scipy.stats as sps


def label_script_tuples(f_cache):
    for line in f_cache:
        try:
            label, script = line.strip().split("\t")
            yield label, script
        except:
            continue


def compute_entropy(series):
    """Compute base-2 entropy of categorical distribution sample"""

    return sps.entropy(series.value_counts(), base=2)


@click.command()
@click.option("--input-file", "-i", required=True)
@click.option("--output-file", "-o", required=True)
@click.option(
    "--cache-path",
    required=True,
    help="Path to cached label -> script mappings",
)
@click.option("--io-format", "-f", default="tsv")
@click.option("--num-workers", "-w", default=12, type=int)
@click.option("--human-readable-langs-path", required=True)
@click.option("--id-column", "-id", default="wikidata_id")
@click.option("--type-column", "-t", default="type")
@click.option("--label-column", "-a", default="label")
@click.option("--english-column", "-e", default="name")
@click.option("--language-column", "-l", default="language")
def main(
    input_file: str,
    output_file: str,
    cache_path: str,
    io_format: str,
    num_workers: int,
    human_readable_langs_path: str,
    id_column: str,
    type_column: str,
    label_column: str,
    english_column: str,
    language_column: str,
) -> None:

    script_column = "script"

    # only csv/tsv supported for now
    assert io_format in ["csv", "tsv"]

    label_to_script = pd.read_csv(
        cache_path, sep="\t", names=[label_column, script_column]
    )

    with open(human_readable_langs_path, encoding="utf8") as f:
        human_readable_names = orjson.loads(f.read())

    data = read(input_file, io_format=io_format)

    data["language_long"] = data[language_column].apply(
        lambda l: human_readable_names.get(l, l)
    )

    data = pd.merge(data, label_to_script, on=label_column, how="left")

    script_entropies = dd.from_pandas(
        data.groupby([language_column, "language_long"])[script_column]
        .apply(compute_entropy)
        .round(3)
        .sort_values(ascending=False)
        .reset_index()
        .rename(
            columns={
                "language_long": language_column,
                "script": "script_entropy",
                language_column: "language_code",
            }
        ),
        chunksize=5000,
    )
    script_entropies.compute()
    dd.to_csv(
        script_entropies,
        output_file,
        single_file=True,  # only one output file
        index=False,  # no line numbers
        sep="\t"      # tab-separated
    )


if __name__ == "__main__":
    main()
