import pathlib
import os

from paranames.util import read, write
import click


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
def main(lang_column, input_file, io_format, use_subfolders, verbose):

    data = read(input_file, io_format)

    for lang in data[lang_column].unique():
        filtered = data[data[lang_column] == lang]

        output_file = get_output_filename(input_file, lang, use_subfolders)
        output_folder = pathlib.Path(os.path.dirname(output_file))

        if not output_folder.exists():
            if verbose:
                print(f"{output_folder} not found. Creating with mkdir...")
            output_folder.mkdir()

        write(filtered, output_file, io_format)


if __name__ == "__main__":
    main()
