from typing import Dict, Type, List

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


def compute_crossing_alignments(
    names: List[s.TransliteratedName],
    permuter_cls: Type[s.NameProcessor],
    language_column: str,
    human_readable_lang_names: Dict[str, str],
    find_best_token_permutation: bool = False,
    preserve_fastalign_data: bool = False,
    debug_mode: bool = False,
    write_permuted_names: bool = True,
    names_output_folder: str = "/tmp/test_alignments_names",
):

    print("[compute_crossing_alignments] Creating pooled corpus...")
    big_corpus = s.Corpus(
        names=names,
        language="all",
        normalize_histogram=True,
        ignore_punctuation=True,
        ignore_numbers=False,
        align_with_english=True,
        fastalign_verbose=True,
        permuter_class=permuter_cls,
        permuter_inplace=True,
        find_best_token_permutation=find_best_token_permutation,
        analyze_unicode=False,
        preserve_fastalign_data=preserve_fastalign_data,
        debug_mode=debug_mode,
        out_folder=names_output_folder,
    )

    if write_permuted_names:
        print(
            f"[compute_crossing_alignments] Writing names out to {big_corpus.out_folder}:"
        )
        big_corpus.write_permutations()

    print(
        "[compute_crossing_alignments] Avg. number of crossing alignments per language:"
    )

    for lang, stats_per_lang in big_corpus.stats.items():
        lang_long = human_readable_lang_names.get(lang, lang)
        avg_alignments = stats_per_lang.mean_cross_alignments
        print(f"{lang_long}\t{avg_alignments}")

    print(
        "[compute_crossing_alignments] Number of permuted / surviving words per language:"
    )

    for lang, stats_per_lang in big_corpus.stats.items():
        lang_long = human_readable_lang_names.get(lang, lang)
        total_permuted = stats_per_lang.total_permuted
        total_surviving = stats_per_lang.total_surviving
        print(f"{lang_long}\t{total_permuted}\t{total_surviving}")


@click.command()
@click.option("--input-file", "-i")
@click.option("--language-column", "-lc", default="language")
@click.option("--random-seed", "-s", type=int, default=1917)
@click.option("--human-readable-langs-path", default=default_human_readable_langs_path)
@click.option(
    "--permuter-type",
    type=click.Choice(s.permuter_types),
    default="edit_distance",
)
@click.option("--debug-mode", is_flag=True, help="Debug mode: only use 10 rows of data")
@click.option("--parallelize", is_flag=True, help="Parallelize using num_workers CPUs")
@click.option(
    "--permute-tokens",
    is_flag=True,
    help="Permute tokens to find the best ordering ",
)
@click.option("--num-workers", type=int, default=2)
@click.option("--chunksize", type=int, default=15000)
@click.option(
    "--preserve-fastalign-data",
    is_flag=True,
    help="Do not delete the aligner training data",
)
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
    permute_tokens,
    num_workers,
    chunksize,
    preserve_fastalign_data,
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
        "remove_parenthesis_edit_distance": s.RemoveParenthesisPermuteLowestDistance,
        "remove_parenthesis": s.ParenthesisRemover,
    }[permuter_type]

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
        dtype=str
    )

    if debug_mode:
        corpus_chunks = [
            chunk for chunk, _ in zip(corpus_chunks, range(num_debug_chunks))
        ]

    name_loader = s.TransliteratedNameLoader(
        language_column=language_column, debug_mode=False
    )

    print(f"Name Loader: {name_loader}")

    print(f"Loading names using p_map and {num_workers} workers...")
    names = list(
        it.chain.from_iterable(p_map(name_loader, corpus_chunks, num_cpus=num_workers))
    )

    compute_crossing_alignments(
        names,
        permuter_class,
        language_column,
        human_readable_lang_names=human_readable_lang_names,
        find_best_token_permutation=permute_tokens,
        preserve_fastalign_data=preserve_fastalign_data,
        debug_mode=debug_mode,
        write_permuted_names=write_permuted_names,
        names_output_folder=names_output_folder,
    )


if __name__ == "__main__":
    main()
