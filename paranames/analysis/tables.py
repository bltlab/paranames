from paranames.util import read, write, maybe_infer_io_format
import click


def add_commas(x):
    if isinstance(x, int):
        return f"{x:,}"
    else:
        return x


def create_count_table(
    df,
    n_langs=50,
    use_log_y=False,
    stacked=False,
    save_path="",
):
    out = df.set_index("language" if not stacked else ["language", "type"]).drop(
        "language_code", 1
    )

    if stacked:
        out = out.unstack(-1).fillna(0).astype(int)
        out.columns = [entity_type for _, entity_type in out.columns.to_flat_index()]
        out["total"] = out.sum(axis=1)
        out.sort_values("total", ascending=False, inplace=True)
        out.drop("total", 1, inplace=True)

        return out.head(n_langs)
    else:
        return (
            out.sort_values("count", ascending=False)["count"].head(n_langs).to_latex()
        )


def add_english_match(count_table, english_match_table):
    english_match_table["english_match"] = english_match_table.english_match.apply(
        lambda x: f"{round(100*x, 3)}%"
    )

    return count_table.merge(
        english_match_table, how="left", left_index=True, right_index=True
    )


def create_entropy_table(df, n_langs=50, use_log_y=False, save_path=""):
    return (
        df.sort_values("script_entropy", ascending=False)
        .head(n_langs)
        .set_index("language")
        .script_entropy
    )


@click.command()
@click.option(
    "--counts-table-path",
    help="Path to CSV of counts per language/entity type",
    required=True,
)
@click.option(
    "--entropy-table-path",
    help="Path to CSV of entropy per language",
    required=True,
)
@click.option(
    "--english-match-table-path",
    help="Path to CSV of English match/overlap",
    required=True,
)
@click.option(
    "--collapse-types",
    is_flag=True,
    help="Collapse entity types & report aggregated counts per language",
)
@click.option(
    "--n-languages-counts",
    help="Number of languages to include in count table.",
    default=50,
)
@click.option(
    "--n-languages-entropy",
    help="Number of languages to include in entropy table.",
    default=50,
)
@click.option(
    "--longtable-counts", is_flag=True, help="Counts table in longtable format"
)
@click.option(
    "--longtable-entropy",
    is_flag=True,
    help="Entropy table in longtable format",
)
@click.option("--io-format", "-f", default="")
def main(
    counts_table_path,
    entropy_table_path,
    english_match_table_path,
    collapse_types,
    n_languages_counts,
    n_languages_entropy,
    longtable_counts,
    longtable_entropy,
    io_format,
):

    count_table = read(
        counts_table_path,
        io_format=maybe_infer_io_format(counts_table_path, io_format),
    )

    entropy_table = read(
        entropy_table_path,
        io_format=maybe_infer_io_format(entropy_table_path, io_format),
    )

    count_table = count_table.merge(
        entropy_table[["language", "language_code"]],
        on="language_code",
        how="left",
    )[["language", "language_code", "type", "count"]]

    english_match_table = read(
        english_match_table_path,
        io_format=maybe_infer_io_format(english_match_table_path),
    )

    english_match_table = english_match_table.merge(
        entropy_table[["language", "language_code"]],
        on="language_code",
        how="left",
    )[["language", "english_match"]].set_index("language")

    if collapse_types:
        count_table = count_table.groupby("language_code")["count"].sum().reset_index()
        count_table = create_count_table(
            count_table,
            n_langs=n_languages_counts,
        )

    else:
        count_table = create_count_table(
            count_table,
            stacked=True,
            n_langs=n_languages_counts,
        )

    entropy_table = create_entropy_table(
        entropy_table,
        n_langs=n_languages_entropy,
    )
    entropy_table.index.name = "Language"
    entropy_table.name = "Script entropy (bits)"

    count_table = (
        add_english_match(count_table, english_match_table)
        .applymap(add_commas)
        .rename(columns={"english_match": "English match (%)"})
    )

    count_table.index.name = "Language"

    print(count_table.to_latex(longtable=longtable_counts))
    print()
    print(entropy_table.to_latex(longtable=longtable_entropy))


if __name__ == "__main__":
    main()
