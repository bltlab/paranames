from pathlib import Path

import click
import matplotlib.pyplot as plt
from paranames.util import read, maybe_infer_io_format
import orjson

rotation_angle = 90
width, height = 12, 6


def plot_zipf_distribution(
    df,
    n_langs=50,
    title="",
    use_log_y=False,
    stacked=False,
    save_path="",
    width=12,
    height=6,
    language_column="language",
    language_code_column="language_code",
    conll_type_column="type",
    without_english=False,
    tail=False,
):

    if without_english:
        df = df[~df[language_column].str.contains("English")]

    out = df.set_index(
        language_column if not stacked else [language_column, conll_type_column]
    )

    if language_code_column in out.columns:
        out.drop(language_code_column, 1, inplace=True)

    if stacked:
        out = out.reset_index().groupby([language_column, conll_type_column]).sum()
        out = out.unstack(-1).fillna(0).astype(int)
        out.columns = [entity_type for _, entity_type in out.columns.to_flat_index()]
        out["total"] = out.sum(axis=1)
        out.sort_values("total", ascending=False, inplace=True)
        out.drop("total", 1, inplace=True)
        out = out.head(n_langs) if not tail else out.tail(n_langs)
        out.plot(
            kind="bar",
            title=title,
            logy=use_log_y,
            stacked=stacked,
            rot=rotation_angle,
            figsize=(width, height),
            xlabel="",
            ylabel="Count",
        )
    else:
        out = out["count"].sort_values(ascending=False)
        out = out.head(n_langs) if not tail else out.tail(n_langs)
        out.plot(
            kind="bar",
            title=title,
            logy=use_log_y,
            stacked=stacked,
            rot=rotation_angle,
            figsize=(width, height),
            xlabel="",
            ylabel="Count",
        )

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path)
    else:
        plt.show()

    plt.clf()


def plot_entropy_distribution(
    df,
    title="",
    save_path="",
    width=12,
    height=6,
):

    df = df.rename(
        columns={
            "script_entropy_before": "Script entropy (before)",
            "script_entropy_after": "Script entropy (after)",
        }
    )

    df.plot(
        kind="scatter",
        x="Script entropy (before)",
        y="Script entropy (after)",
        figsize=(width, height),
        title=title,
    )

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path)

    else:
        plt.show()

    plt.clf()


def remove_unknown_languages(df, language_column, language_code_column):
    return df[df[language_column] != df[language_code_column]]


@click.command()
@click.option(
    "--counts-table-path",
    help="Path to CSV of counts per language/entity type",
    required=True,
)
@click.option(
    "--entropy-table-path",
    help="[DEPRECATED] Path to CSV of entropy per language",
    required=False,
)
@click.option(
    "--collapse-types",
    is_flag=True,
    help="Collapse entity types & report aggregated counts per language",
)
@click.option(
    "--log-scale",
    is_flag=True,
    help="Use log scale on the y-axis",
)
@click.option(
    "--n-languages-counts",
    help="Number of languages to plot in count graph.",
    default=50,
)
@click.option(
    "--n-languages-entropy",
    help="[DEPRECATED] Number of languages to plot in entropy graph.",
    default=50,
)
@click.option(
    "--output-folder",
    help="Folder to output plots. Will be created if doesn't exist.",
    default="",
)
@click.option("--counts-width", default=12)
@click.option("--counts-height", default=6)
@click.option("--entropy-width", default=12, help="deprecated")
@click.option("--entropy-height", default=4, help="deprecated")
@click.option("--entropy-threshold", default=0.1, help="deprecated")
@click.option("--remove-xticks-entropy", is_flag=True)
@click.option("--prune-entropy-plot", is_flag=True, help="deprecated")
@click.option("--n-bins", default=20)
@click.option("--language-column", default="language")
@click.option("--language-code-column", default="lang_code")
@click.option("--conll-type-column", default="type")
@click.option(
    "--human-readable-languages-path",
    default="data/human_readable_lang_names_from_sparql.json",
)
@click.option("--without-english", is_flag=True, help="Don't include English")
@click.option("--tail", is_flag=True, help="Look at smallest languages instead")
@click.option(
    "--drop-unknown-languages",
    is_flag=True,
    help="Drop languages for which there is no human-readable language name",
)
def main(
    counts_table_path,
    entropy_table_path,
    collapse_types,
    log_scale,
    n_languages_counts,
    n_languages_entropy,
    output_folder,
    counts_width,
    counts_height,
    entropy_width,
    entropy_height,
    entropy_threshold,
    remove_xticks_entropy,
    prune_entropy_plot,
    n_bins,
    language_column,
    language_code_column,
    conll_type_column,
    human_readable_languages_path,
    without_english,
    tail,
    drop_unknown_languages,
):

    count_table = read(
        counts_table_path,
        io_format=maybe_infer_io_format(counts_table_path),
    )
    count_table[language_code_column] = [
        lc if type(lc) == str else "nan" for lc in count_table[language_code_column]
    ]

    if language_column not in count_table.columns:
        with open(human_readable_languages_path, encoding="utf-8") as fin:
            hrl = orjson.loads(fin.read())

            count_table[language_column] = [
                hrl.get(lc, lc) for lc in count_table[language_code_column]
            ]

    if drop_unknown_languages:
        count_table = remove_unknown_languages(
            count_table, language_column, language_code_column
        )

    if output_folder:

        output_folder = Path(output_folder)

        if not output_folder.exists():
            output_folder.mkdir()

        zipf_output_file = output_folder / "zipf_count_distribution.png"

        if entropy_table_path:
            entropy_output_file = output_folder / "entropy_scatterplot.png"

    else:
        zipf_output_file = ""
        entropy_output_file = ""

    if collapse_types:
        count_table = count_table.groupby(language_column)["count"].sum().reset_index()


    plot_zipf_distribution(
        count_table,
        stacked=not collapse_types,
        n_langs=n_languages_counts,
        save_path=zipf_output_file,
        use_log_y=log_scale,
        width=counts_width,
        height=counts_height,
        without_english=without_english,
        tail=tail,
    )

    if entropy_table_path:
        entropy_table = read(
            entropy_table_path,
            io_format=maybe_infer_io_format(entropy_table_path),
        )

        if drop_unknown_languages:
            entropy_table = remove_unknown_languages(
                entropy_table, language_column, language_code_column
            )

        plot_entropy_distribution(
            entropy_table,
            save_path=entropy_output_file,
            width=counts_width,
            height=counts_height,
        )


if __name__ == "__main__":
    main()
