#!/usr/bin/env python

"""Subsamples the input data to create a reasonably sized
and less imbalanced training/dev/test set."""

from collections import Counter
from typing import Optional

from paranames.util import read, write
from tqdm import tqdm
import pandas as pd
import numpy as np
import click
import attr

SUPPORTED_SAMPLERS = [
    "uniform",
    "empirical",
    "exponential_smoothing",
    "balanced_groupby",
]
SUPPORTED_FORMATS = ["tsv", "jsonl", "csv"]

DEFAULT_SEED = 1917


@attr.s
class Sampler:
    """General class representing a sampler that samples documents according
    to the process of first sampling a language and then a document in the given language."""

    random_seed: int = attr.ib(default=DEFAULT_SEED)

    def __attrs_post_init__(self):
        self.random_state = np.random.RandomState(
            np.random.MT19937(np.random.SeedSequence(self.random_seed))
        )


@attr.s
class GroupBySampler(Sampler):
    """Group by a column and uniformly sample n rows from each group."""

    num_groups: int = attr.ib(default=-1)
    groupby_column: str = attr.ib(default="language")

    def __call__(self, df: pd.DataFrame, n: Optional[int]) -> pd.DataFrame:
        if self.num_groups < 0:
            self.num_groups = 1

        if n is None:
            n = df.shape[0]

        rows = []
        num_per_group = n // self.num_groups

        for group_value in df[self.groupby_column].unique():
            subset = df[df[self.groupby_column] == group_value]

            if num_per_group > subset.shape[0]:
                rows.append(subset)
            else:
                rows.append(subset.sample(num_per_group))

        out = pd.concat(rows, ignore_index=True)

        return out


@attr.s
class EmpiricalDistributionSampler(Sampler):
    """Uniformly samples rows of pandas DataFrame, effectively drawing
    samples according to the empirical language distribution of the data set.

    Returns another DataFrame."""

    def __call__(
        self, df: pd.DataFrame, n: Optional[int], *args, **kwargs
    ) -> pd.DataFrame:

        return df.sample(n, random_state=self.random_state)


@attr.s
class UniformDistributionSampler(Sampler):
    """Uniformly samples languages from the corpus and then rows from each language.

    Returns another DataFrame."""

    language_column: Optional[str] = attr.ib(default="language")

    def __call__(
        self,
        df: pd.DataFrame,
        n: Optional[int],
        language_column: Optional[str] = None,
    ) -> pd.DataFrame:

        if not n:
            return df

        # sample a random number of names to draw from each language
        lc = language_column or self.language_column
        n_samples_per_language: Counter = Counter(df[lc].sample(n))

        # sample the prescribed number of names for each language
        sampled_rows = pd.concat(
            [
                df[df[lc] == lang].sample(n_samples, random_state=self.random_state)
                for lang, n_samples in n_samples_per_language.items()
            ],
            ignore_index=True,
        )

        return sampled_rows


@attr.s
class ExponentSmoothedSampler(Sampler):
    """Re-weights the language distribution with exponential smoothing
    similar to what was done with multilingual BERT, and draws samples
    from the re-weighted language distribution.

    Returns another DataFrame."""

    language_column: Optional[str] = attr.ib(default="language")
    smoothing_factor: float = attr.ib(default=0.7)

    def __call__(
        self,
        df: pd.DataFrame,
        n: Optional[int],
        language_column: Optional[str] = None,
    ) -> pd.DataFrame:

        if not n:
            return df

        lc = language_column or self.language_column
        language_distribution = df[lc].value_counts(normalize=True)
        smoothed_language_weights = language_distribution ** self.smoothing_factor

        weights_for_sampling = df[lc].apply(lambda l: smoothed_language_weights[l])

        sampled_rows = df.sample(
            n=n, weights=weights_for_sampling, random_state=self.random_state
        )

        return sampled_rows


@click.command()
@click.option("--input-file", "-i", required=True)
@click.option("--output-file", "-o", required=True)
@click.option("--io-format", "-f", type=click.Choice(SUPPORTED_FORMATS))
@click.option("--chunksize", "-c", type=int, default=-1)
@click.option("--sampler", "-s", type=click.Choice(SUPPORTED_SAMPLERS))
@click.option("--debug-mode", "-d", is_flag=True)
@click.option("--random-seed", "-r", type=int, default=DEFAULT_SEED)
@click.option("--num-samples", "-n", type=int, default=-1)
@click.option("--smoothing-factor", "-S", type=float, default=0.7)
@click.option("--groupby-column", "-g", default="is_unchanged")
@click.option(
    "--languages-to-use",
    "-l",
    default="",
    help="Comma separated list of languages to filter down to before sampling",
)
@click.option("--min-names-per-lang", type=int, default=-1)
def main(
    input_file,
    output_file,
    io_format,
    chunksize,
    sampler,
    debug_mode,
    random_seed,
    num_samples,
    smoothing_factor,
    groupby_column,
    languages_to_use,
    min_names_per_lang,
):

    if debug_mode:
        print(f"Loading data from {input_file}")
    data = read(input_file, io_format, chunksize=chunksize if chunksize > 0 else None)

    if chunksize > 0 and debug_mode:
        data = next(data)
    elif chunksize > 0 and not debug_mode:
        if debug_mode:
            print("Loading chunks...")
        data = pd.concat(tqdm(chunk for chunk in data))

    if languages_to_use:
        if debug_mode:
            print("Filtering down languages...")
        keep_these = set(languages_to_use.split(","))
        data = data[data.language.isin(keep_these)]

    if sampler == "exponential_smoothing":
        subsampler = ExponentSmoothedSampler(
            smoothing_factor=smoothing_factor, random_seed=random_seed
        )
    elif sampler == "balanced_groupby":
        subsampler = GroupBySampler(
            groupby_column=groupby_column,
            num_groups=data[groupby_column].nunique(),
        )
    else:
        subsampler_cls = {
            "empirical": EmpiricalDistributionSampler,
            "uniform": UniformDistributionSampler,
        }[sampler](random_seed)

    if debug_mode:
        print(f"Subsampling data with {subsampler}...")

    sub = subsampler(data, num_samples)

    if min_names_per_lang > 0:
        print(f"Dropping languages that have fewer than {min_names_per_lang} names")
        lang_counts = sub.language.value_counts().to_dict()
        should_keep = sub.language.apply(lambda l: lang_counts[l] >= min_names_per_lang)
        sub = sub[should_keep]

    if debug_mode:
        print(f"Writing to {output_file}...")
    write(data=sub, output_file=output_file, io_format=io_format, index=False)


if __name__ == "__main__":
    main()
