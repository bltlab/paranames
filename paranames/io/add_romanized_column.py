import sys

from paranames.util.script import UniversalRomanizer
from paranames.util import read, write
import editdistance
import click


@click.command()
@click.option(
    "-a",
    "--alias-column",
    default="alias",
    help="Name of column containing the transliterated names.",
)
@click.option(
    "-e",
    "--english-column",
    default="eng",
    help="Name of column containing the English names.",
)
@click.option("-f", "--io-format", default="tsv")
@click.option("-n", "--num-workers", default=1, type=int)
@click.option("-ed", "--with-edit-distance", is_flag=True)
def main(alias_column, english_column, io_format, num_workers, with_edit_distance):
    uroman = UniversalRomanizer(num_workers=num_workers)
    with sys.stdin as standard_input, sys.stdout as standard_output:
        data = read(input_file=standard_input, io_format=io_format)
        columns = list(data.columns)
        alias_ix = columns.index(alias_column)
        data["romanized"] = uroman(data[alias_column])
        columns.insert(alias_ix, "romanized")
        data = data[columns]
        if with_edit_distance:
            data["edit_distance"] = [
                editdistance.eval(a.lower(), r.lower())
                for a, r in zip(data[alias_column], data["romanized"])
            ]
            data.sort_values("edit_distance", ascending=False, inplace=True)
        write(data, standard_output, io_format)


if __name__ == "__main__":
    main()
