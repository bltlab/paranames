from paranames.util import read, write
import pandas as pd
import click

# quick and dirty script to analyze changes in script entropy

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)


@click.command()
@click.option("--before-file", "-b")
@click.option("--after-file", "-a")
@click.option("--filtered-file", "-ff")
@click.option("--output-file", "-o", default="")
@click.option("--io-format", "-f", default="csv")
def main(before_file, after_file, filtered_file, output_file, io_format):

    before = read(before_file, io_format).set_index(["language_code", "language"])
    after = read(after_file, io_format).set_index(["language_code", "language"])

    combined = before.join(after, lsuffix="_before", rsuffix="_after").rename(
        columns={
            "script_entropy_before": "before",
            "script_entropy_after": "after",
        }
    )
    combined["change"] = combined.after - combined.before

    print(combined[combined.change != 0])

    print("Summary statistics, change in script entropy")
    print(combined.change.describe())

    filtered = read(filtered_file, "tsv")
    print(f"There are {filtered.shape[0]} filtered names")


if __name__ == "__main__":
    main()
