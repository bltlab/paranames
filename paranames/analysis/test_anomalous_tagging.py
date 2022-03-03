from pathlib import Path

from paranames.util import read, maybe_infer_io_format
import paranames.util.script as s
import pandas as pd
import numpy as np
import click

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)

distance_measure_choices = [
    "kullback_leibler",
    "jensen_shannon",
    "bhattacharyya",
]


@click.command()
@click.option("--input-file", "-i", required=True)
@click.option("--output-folder", "-o", required=True, default="")
@click.option("--io-format", "-f", default="tsv")
@click.option(
    "--distance-measure",
    "-d",
    default="jensen_shannon",
    type=click.Choice(distance_measure_choices),
)
@click.option(
    "--anomalous-data-folder",
    "-a",
    default="/home/jonne/datasets/wikidata/flyingsquid/",
)
@click.option("--n-noise-words", type=int, default=1000)
@click.option("--critical-value", "-c", default=0.1)
@click.option("--strip", is_flag=True)
@click.option("--no-normalize", is_flag=True)
@click.option("--ignore-punctuation", is_flag=True)
@click.option("--language-column", default="language")
@click.option("--alias-column", default="alias")
@click.option("--english-text-column", default="alias")
@click.option("--id-column", default="wikidata_id")
def main(
    input_file: str,
    output_folder: str,
    io_format: str,
    distance_measure: str,
    anomalous_data_folder: str,
    n_noise_words: int,
    critical_value: float,
    strip: bool,
    no_normalize: bool,
    ignore_punctuation: bool,
    language_column: str,
    alias_column: str,
    english_text_column: str,
    id_column: str,
) -> None:

    # only csv/tsv supported for now
    assert io_format in ["csv", "tsv"]

    # unicode analyzer
    ua = s.UnicodeAnalyzer(
        strip=strip,
        normalize_histogram=not no_normalize,
        ignore_punctuation=ignore_punctuation,
    )

    print("Reading in corpus")
    data = read(input_file, io_format=maybe_infer_io_format(input_file))
    unique_languages = data.language.unique()

    anomalous = {}

    print("Reading in hand-labeled anomalous data, if any")

    for language in unique_languages:
        try:
            anomalous[language] = {
                line.split("\t")[0].strip()
                for line in open(
                    Path(anomalous_data_folder) / f"{language}_anomalous.txt"
                )
            }
        except:
            continue

    corpora = {}

    for language in unique_languages:
        print(f"[{language}] Creating corpus...")
        subset = data[data.language == language]
        corpora[language] = s.Corpus(
            out_folder=(
                Path(output_folder) or Path(f"/tmp/flyingsquid-test/{language}")
            ),
            names=[
                s.TransliteratedName(
                    text=row[alias_column],
                    language=language,
                    english_text=row[english_text_column],
                    wikidata_id=row[id_column],
                    unicode_analyzer=ua,
                    is_unchanged=True,
                )
                for _, row in subset.iterrows()
            ],
            language=language,
            normalize_histogram=True,
            ignore_punctuation=True,
            ignore_numbers=False,
            # align_with_english=True
        )

    # add in data for negative sampling
    print("Adding in data for negative sampling")

    if n_noise_words > 0:
        for language, corpus in corpora.items():
            anomalous_words = anomalous.get(language, {})

            if not anomalous_words:
                names_in_this_lang = set(
                    data[data.language == language][alias_column].unique()
                )
                names_in_other_langs = set(
                    data[data.language != language][alias_column].unique()
                )

                non_overlapping_names = names_in_other_langs - names_in_this_lang

                anomalous_words = np.random.choice(
                    np.array(list(non_overlapping_names)),
                    size=n_noise_words,
                    replace=False,
                )

            corpus.add_words(
                [
                    s.TransliteratedName(
                        text=w,
                        language=language,
                        noise_sample=True,
                        unicode_analyzer=ua,
                        anomalous=True,
                        is_unchanged=True,
                    )
                    for w in anomalous_words
                ]
            )

            anomalous[language] = set(anomalous_words)
    else:
        anomalous[language] = set()

    for language, corpus in corpora.items():

        print(f"[{language}] Done. Predicting...")

        # anomalous if most common unicode block is not expected one
        incorrect_block_tagger = s.IncorrectBlockTagger(
            expected_block=corpus.most_common_unicode_block
        )

        # anomalous if given block is missing
        missing_block_tagger = s.MissingBlockTagger(
            missing_block=corpus.most_common_unicode_block
        )

        # anomalous if JSD from language prototype is greater than a critical value
        distance_based_tagger = s.JSDTagger(
            per_language_distribution=corpus.prototype,
            critical_value=critical_value,
            distance_measure="jensen_shannon",
        )

        hiragana_katakana_tagger = s.HiraganaKatakanaTagger()
        cjk_tagger = s.CJKTagger()

        true_labels = []

        for name in corpus.names:
            if name.anomalous or name.text in anomalous.get(language, {}):
                true_labels.append(1)
            elif name.anomalous == None:
                true_labels.append(0)
            else:
                true_labels.append(-1)

        def get_preds(tagger):
            return list(yield_preds(tagger))

        def yield_preds(tagger):
            for w in tagger(corpus.names):
                if w.anomalous is None:
                    yield 0
                elif w.anomalous:
                    yield 1
                else:
                    yield -1

        ibt_preds = get_preds(incorrect_block_tagger)
        mbt_preds = get_preds(missing_block_tagger)
        dbt_preds = get_preds(distance_based_tagger)
        hk_preds = get_preds(hiragana_katakana_tagger)
        cjk_preds = get_preds(cjk_tagger)

        noisy_votes = np.vstack(
            [ibt_preds, mbt_preds, dbt_preds, hk_preds, cjk_preds]
        ).T

        majority_vote_preds = (noisy_votes.mean(axis=1) > 0).astype(int)

        tagged_names = [
            s.TransliteratedName(
                text=w.text,
                unicode_analyzer=w.unicode_analyzer,
                anomalous=bool(pred > 0),
                language=w.language,
                noise_sample=w.noise_sample,
                is_unchanged=w.is_unchanged,
            )
            for w, pred in zip(corpus.names, majority_vote_preds)
        ]

        for (name, pred) in zip(corpus.names, majority_vote_preds):
            gold = name.anomalous
            neg = name.noise_sample
            pred = bool(pred > 0)

            if gold and not pred:
                print(f"[{language}] Anomalous but not tagged: {name.text}")
            elif pred and gold is False and not neg:
                print(f"[{language}] Non-anomalous but tagged: {name.text}")

        corpus.names = tagged_names
        corpus.write_anomaly_info(write_noise_samples=False)


if __name__ == "__main__":
    main()
