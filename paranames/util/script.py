import csv
import itertools as it
import re
import subprocess
import tempfile as tf
import unicodedata as ud
from collections import Counter
from pathlib import Path
from typing import (
    Generator,
    List,
    Union,
    Dict,
    Tuple,
    Type,
    Callable,
    Iterable,
    Optional,
)

import icu
import pandas as pd
from tqdm import tqdm
from unicodeblock import blocks


class UnicodeAnalyzer:
    def __init__(
        self,
        strip: bool = False,
        ignore_punctuation: bool = False,
        ignore_numbers: bool = False,
        normalize_histogram: bool = True,
    ) -> None:
        self.strip = strip
        self.ignore_punctuation = ignore_punctuation
        self.normalize_histogram = normalize_histogram
        self.ignore_numbers = ignore_numbers

        # Skip punctuation depending on settings
        self.punctuation_cond = (
            lambda w: not self.is_punctuation(str(w))

            if self.ignore_punctuation
            else True
        )

        # Skip digits depending on settings
        self.digit_cond = (
            lambda w: not self.is_number(str(w)) if self.ignore_numbers else True
        )

    def is_punctuation(self, s: str) -> bool:
        is_punc = ud.category(s).startswith("P")
        is_symb = ud.category(s).startswith("S")

        return is_punc or is_symb

    def is_number(self, c: str) -> bool:
        return ud.category(c).startswith("N")

    def maybe_strip(self, word: str) -> str:
        return str(word).strip() if self.strip else str(word)

    def histogram(self, word: str, icu_mode: bool = False) -> Counter:
        histogram = self.icu_scripts(word) if icu_mode else self.unicode_blocks(word)

        if self.normalize_histogram:
            total = sum(histogram.values())

            for block, count in histogram.items():
                histogram[block] = count / total

        return histogram

    def unicode_blocks(self, word: str) -> Counter:

        return Counter(
            blocks.of(c)

            for c in self.maybe_strip(word)

            if blocks.of(c) and self.punctuation_cond(c) and self.digit_cond(c)
        )

    def most_common_unicode_block(self, word: str) -> str:
        try:
            return self.unicode_blocks(word).most_common(1)[0][0]
        except IndexError:
            return ""

    def unicode_block_histogram(
        self,
        word: str,
    ) -> Counter:
        return self.histogram(word, icu_mode=False)

    def get_icu_script(self, c: str) -> str:
        return icu.Script.getScript(c).getName()

    def icu_scripts(self, word: str) -> Counter:

        return Counter(
            self.get_icu_script(c)

            for c in self.maybe_strip(word)

            if self.get_icu_script(c)
            and self.punctuation_cond(c)
            and self.digit_cond(c)
        )

    def most_common_icu_script(self, word: str) -> str:
        try:
            return self.icu_scripts(word).most_common(1)[0][0]
        except IndexError:
            return ""

    def icu_script_histogram(
        self,
        word: str,
    ) -> Counter:
        return self.histogram(word, icu_mode=True)
