import paranames.util.script as s
import pandas as pd
import numpy as np
import click
import orjson

from p_tqdm import p_map
import itertools as it

default_human_readable_langs_path = (
    "/home/jonne/wikidata-munger/data/human_readable_lang_names.json"
)

permuter_types = [
    "comma",
    "edit_distance",
    "remove_parenthesis_permute_comma",
    "remove_parenthesis",
]


@click.command()
@click.option("--input-file", "-i")
@click.option("--language-column", "-lc", default="language")
@click.option("--random-seed", "-s", type=int, default=1917)
@click.option("--human-readable-langs-path", default=default_human_readable_langs_path)
@click.option(
    "--permuter-type",
    type=click.Choice(permuter_types),
    default="edit_distance",
)
@click.option("--debug-mode", is_flag=True, help="Debug mode: only use 10 rows of data")
@click.option("--parallelize", is_flag=True, help="Parallelize using num_workers CPUs")
@click.option("--num-workers", type=int, default=2)
@click.option("--chunksize", type=int, default=15000)
@click.option("--num-debug-chunks", type=int, default=pow(10, 10))
@click.option(
    "--write-permuted-names",
    is_flag=True,
    help="Write permuted names to output folder specified by the --names-output-folder flag",
)
@click.option("--names-output-folder", default="/tmp/test_alignments_names")
def main(
    input_file,
    language_column,
    random_seed,
    permuter_type,
    human_readable_langs_path,
    debug_mode,
    parallelize,
    num_workers,
    chunksize,
    num_debug_chunks,
    write_permuted_names,
    names_output_folder,
):

    # set seed
    np.random.seed(random_seed)

    # load human readable language information
    with open(human_readable_langs_path, encoding="utf8") as f:
        human_readable_lang_names = orjson.loads(f.read())

    # get the right class for permuting tokens
    permuter_class = {
        "comma": s.PermuteFirstComma,
        "edit_distance": s.PermuteLowestDistance,
        "remove_parenthesis_permute_comma": s.RemoveParenthesisPermuteComma,
        "remove_parenthesis": s.ParenthesisRemover,
    }[permuter_type]
    permuter = permuter_class(
        debug_mode=debug_mode,
        inplace=True,
    )

    # read in corpus and subset
    corpus_chunks = pd.read_csv(
        input_file,
        chunksize=chunksize,
        encoding="utf-8",
        delimiter="\t",
        na_values=set(
            [
                "",
                "#N/A",
                "#N/A N/A",
                "#NA",
                "-1.#IND",
                "-1.#QNAN",
                "-NaN",
                "1.#IND",
                "1.#QNAN",
                "<NA>",
                "N/A",
                "NA",
                "NULL",
                "NaN",
                "n/a",
                "null",
            ]
        ),
        keep_default_na=False,
    )

    if debug_mode:
        corpus_chunks = [
            chunk for chunk, _ in zip(corpus_chunks, range(num_debug_chunks))
        ]

    name_loader = s.TransliteratedNameLoader(
        language_column=language_column, debug_mode=False  # debug_mode,
    )

    print(f"Name Loader: {name_loader}")

    print(f"Loading names using p_map and {num_workers} workers...")
    names = list(
        it.chain.from_iterable(p_map(name_loader, corpus_chunks, num_cpus=num_workers))
    )

    # permute names
    print("Permuting names...")
    names = permuter(names)

    # write out
    name_writer = s.NameWriter(out_folder=names_output_folder, debug_mode=debug_mode)
    name_writer.write(
        {"name_permutations": names},
        unicode_block_mode=False,
        write_permutations_mode=True,
    )


if __name__ == "__main__":
    main()
