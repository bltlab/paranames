import csv
import itertools
import json
from pathlib import Path
from typing import Union, Optional, Dict, Any, Iterable, List

import orjson
import pandas as pd
from tqdm import tqdm


def maybe_infer_io_format(file_path: str, io_format: Optional[str] = None) -> str:
    if io_format:
        return io_format
    else:
        return Path(file_path).suffix.lstrip(".")


def read(
    input_file: str,
    io_format: str,
    typ: str = "frame",
    chunksize: Union[int, None] = None,
    column_names: Optional[List[str]] = None,
    **kwargs,
) -> pd.DataFrame:
    if io_format in ["csv", "tsv"]:
        return pd.read_csv(
            input_file,
            encoding="utf-8",
            delimiter="\t" if io_format == "tsv" else ",",
            chunksize=chunksize,
            na_values=set(
                [
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
            names=column_names,
            **kwargs,
        )
    elif io_format == "jsonl":
        return pd.read_json(
            input_file,
            "records",
            encoding="utf-8",
            typ=typ,
            lines=True,
            chunksize=chunksize,
            **kwargs,
        )
    elif io_format == "json":
        return pd.read_json(
            input_file, encoding="utf-8", typ=typ, chunksize=chunksize, **kwargs
        )


def write_csv_writer(
    data: Union[Iterable[Dict[str, Any]], pd.DataFrame],
    output_file: str,
    io_format: str,
    index: bool = False,
    *,
    dict_writer_field_names: Optional[List[str]] = None,
    verbose: bool = False,
    n_rows: Optional[int] = None,
) -> None:

    with open(output_file, "w") as f_out:
        writer = csv.DictWriter(
            f_out,
            fieldnames=dict_writer_field_names,
            extrasaction="ignore",
            delimiter="\t" if io_format == "tsv" else ",",
        )
        writer.writeheader()
        rows = tqdm(data, total=(n_rows or None)) if verbose else data
        for row in rows:
            writer.writerow(row)


def write_pandas(
    data: pd.DataFrame, output_file: str, io_format: str, index: bool = False
) -> None:
    if io_format in ["csv", "tsv"]:
        return data.to_csv(
            output_file,
            sep="\t" if io_format == "tsv" else ",",
            encoding="utf-8",
            index=index,
        )
    else:
        return data.to_json(output_file, "records", encoding="utf-8", index=False)


def write(
    data: Union[Iterable[Dict[str, Any]], pd.DataFrame],
    output_file: str,
    io_format: str,
    index: bool = False,
    *,
    mode: Optional[str] = "pandas",
    dict_writer_field_names: Optional[List[str]] = None,
    verbose: bool = False,
    n_rows: Optional[int] = None,
) -> None:
    if mode == "pandas":
        write_pandas(data, output_file, io_format, index)
    elif mode == "dict_writer":
        write_csv_writer(
            data=data,
            output_file=output_file,
            io_format=io_format,
            dict_writer_field_names=dict_writer_field_names,
            verbose=verbose,
            n_rows=n_rows,
        )
    else:
        raise ValueError(f"[write] Mode {mode} not supported!")


def orjson_dump(d: dict) -> str:
    """Dumps a dictionary in UTF-8 format using orjson"""
    json_bytes: bytes = orjson.dumps(d)
    json_utf8 = json_bytes.decode("utf8")

    return json_utf8


def json_dump(d: dict) -> str:
    """Dumps a dictionary in UTF-8 foprmat using json

    Effectively the `json` counterpart to `orjson_dump`.
    """

    return json.dumps(d, ensure_ascii=False)


def chunks(iterable, size, should_enumerate=False):
    """Source: https://alexwlchan.net/2018/12/iterating-in-fixed-size-chunks/"""
    it = iter(iterable)
    ix = 0

    while True:
        chunk = tuple(itertools.islice(it, size))

        if not chunk:
            break
        yield (ix, chunk) if should_enumerate else chunk
        ix += 1
