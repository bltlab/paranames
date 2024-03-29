import csv
import math
import os
import sys
from collections import defaultdict
from typing import IO, Iterable

import click
from pymongo import MongoClient
from tqdm import tqdm

import paranames.util.wikidata as w
from paranames.util import orjson_dump


def output_jsonl(
    document: w.WikidataRecord,
    f: IO,
    languages: Iterable[str],
    not_languages: Iterable[str],
    conll_type: str,
    strict: bool = False,
    row_number: int = 0,
    *args,
    **kwargs,
) -> None:
    wikidata_id = document.wikidata_id
    name = document.name
    language_set = set(languages)
    not_language_set = set(not_languages)

    for lang, label in document.labels.items():

        not_in_include_set = strict and lang not in language_set
        in_exclude_set = lang in not_language_set

        if not_in_include_set or in_exclude_set:
            continue
        row = orjson_dump(
            {
                "wikidata_id": wikidata_id,
                "name": name,
                "label": label,
                "language": lang,
                "type": conll_type,
            }
        )
        f.write(f"{row}\n")


def output_csv(
    document: w.WikidataRecord,
    f: IO,
    languages: Iterable[str],
    not_languages: Iterable[str],
    conll_type: str,
    strict: bool = False,
    row_number: int = 0,
    delimiter: str = ",",
    *args,
    **kwargs,
) -> None:
    language_set = set(languages)
    not_language_set = set(not_languages)
    wikidata_id = document.wikidata_id
    name = document.name
    writer = csv.DictWriter(
        f,
        delimiter=delimiter,
        fieldnames=["wikidata_id", "name", "label", "language", "type"],
        extrasaction="ignore",
    )

    if row_number == 0:
        writer.writeheader()

    rows = (
        {
            "wikidata_id": wikidata_id,
            "name": name,
            "label": label,
            "language": lang,
            "type": conll_type,
        }
        for lang, label in document.labels.items()
    )

    if strict:
        rows = (
            row
            for row in rows
            if row["language"] in language_set
            and row["language"] not in not_language_set
        )
    else:
        rows = (row for row in rows if row["language"] not in not_language_set)

    writer.writerows(rows)


def resolve_output_file(output_file: str, mode="a") -> IO:

    output_is_stdout = bool(not output_file or output_file == "-")

    if output_is_stdout:
        return sys.stdout
    else:
        abs_output = os.path.abspath(output_file)

        return open(abs_output, mode, encoding="utf-8")


conll_type_to_wikidata_id = {"PER": "Q5", "LOC": "Q82794", "ORG": "Q43229"}


@click.command()
@click.option("--mongodb-uri", default="", help="MongoDB URI")
@click.option(
    "--mongodb-port",
    type=int,
    help="MongoDB port",
)
@click.option("--database-name", default="wikidata_db", help="Database name")
@click.option("--collection-name", default="wikidata_simple", help="Collection name")
@click.option(
    "--subclass-coll-name",
    default="subclasses",
    help="Subclass collection name",
)
@click.option(
    "--output-format",
    "-f",
    type=click.Choice(["jsonl", "csv", "tsv"]),
    default="jsonl",
)
@click.option(
    "--output-file",
    "-o",
    default="-",
    help="Output file. If empty or '-', defaults to stdout.",
)
@click.option(
    "--delimiter",
    "-d",
    type=click.Choice([",", "\t", "tab"]),
    default=",",
    help="Delimiter for CSV output. Can be comma or tab. Defaults to comma.",
)
@click.option(
    "--conll-type",
    "-t",
    type=click.Choice([t for t in conll_type_to_wikidata_id]),
    default=",",
)
@click.option(
    "--languages",
    "-l",
    default="en",
    help="Comma-separated list of languages to include",
)
@click.option(
    "--not-languages",
    "-L",
    default="",
    help="Comma-separated list of languages to exclude",
)
@click.option("--ids", "-i", default="", help="Only search for these IDs")
@click.option(
    "--num-docs",
    "-n",
    type=float,
    default=math.inf,
    help="Number of documents to output",
)
@click.option(
    "--strict",
    "-s",
    is_flag=True,
    help="Strict mode: Only output names in languages specified using the -l flag.",
)
@click.option(
    "--disable-subclass-per",
    is_flag=True,
    help="Disable subclassing for PER.",
)
@click.option(
    "--disable-subclass-loc",
    is_flag=True,
    help="Disable subclassing for LOC.",
)
@click.option(
    "--disable-subclass-org",
    is_flag=True,
    help="Disable subclassing for ORG.",
)
def main(
    mongodb_uri,
    mongodb_port,
    database_name,
    collection_name,
    subclass_coll_name,
    output_format,
    output_file,
    delimiter,
    conll_type,
    languages,
    not_languages,
    ids,
    num_docs,
    strict,
    disable_subclass_per,
    disable_subclass_loc,
    disable_subclass_org,
):

    # parse some input args
    languages = "" if languages == "-" else languages
    not_languages = "" if not_languages == "-" else not_languages
    language_list = languages.split(",")
    not_language_list = not_languages.split(",")
    id_list = ids.split(",")
    output = output_jsonl if output_format == "jsonl" else output_csv
    delimiter = "\t" if delimiter == "tab" else delimiter

    # form connections to mongo db
    client = (
        MongoClient(mongodb_uri)
        if mongodb_uri
        else MongoClient(host=None, port=mongodb_port)
    )
    subclasses = client[database_name][subclass_coll_name]
    db = client[database_name][collection_name]

    # formulate a list of all valid instance-of classes
    parent_wikidata_id = conll_type_to_wikidata_id[conll_type]

    disable_subclass = {
        "PER": disable_subclass_per,
        "LOC": disable_subclass_loc,
        "ORG": disable_subclass_org,
    }[conll_type]

    if disable_subclass:
        print(f"Disabling subclassing for {conll_type}", file=sys.stderr)
        valid_instance_ofs = [parent_wikidata_id]
    else:
        subclass_dict = subclasses.find_one({"id": parent_wikidata_id})
        valid_instance_ofs = subclass_dict["subclasses"]

    # fetch results from mongodb
    filter_dict = defaultdict(dict)
    filter_dict["instance_of"].update({"$in": valid_instance_ofs})

    if languages:
        filter_dict["languages"].update({"$in": language_list})

    if ids:
        filter_dict["id"].update({"$in": id_list})

    results = (doc for doc in db.find(filter_dict))

    with resolve_output_file(output_file) as fout:
        for ix, doc in tqdm(enumerate(results)):
            if ix < num_docs:
                output(
                    w.WikidataRecord(doc, simple=True),
                    f=fout,
                    languages=language_list,
                    not_languages=not_language_list,
                    conll_type=conll_type,
                    strict=strict,
                    row_number=ix,
                    delimiter=delimiter,
                )
            else:
                break


if __name__ == "__main__":
    main()
