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
    conll_type: str,
    include_instance_of: bool = False,
    *args,
    **kwargs,
) -> None:
    wikidata_id = document.wikidata_id
    instance_of = document.instance_ofs
    name = document.name

    rec = {
        "wikidata_id": wikidata_id,
        "name": name,
        "type": conll_type,
        "instance_of": instance_of,
    }

    row = orjson_dump(rec)
    f.write(f"{row}\n")


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
    "--output-file",
    "-o",
    default="-",
    type=click.File(mode="a"),
    help="Output file. Defaults to stdout.",
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
    "--disable-subclass",
    is_flag=True,
    help="Disable subclassing when assigning entity types.",
)
@click.option(
    "--include-instance-of",
    is_flag=True,
    help="Include instance-of information for each row",
)
def main(
    mongodb_uri,
    mongodb_port,
    database_name,
    collection_name,
    subclass_coll_name,
    output_file,
    delimiter,
    conll_type,
    languages,
    not_languages,
    ids,
    num_docs,
    strict,
    disable_subclass,
    include_instance_of,
):

    # parse some input args
    languages = "" if languages == "-" else languages
    not_languages = "" if not_languages == "-" else not_languages
    language_list = languages.split(",")
    id_list = ids.split(",")
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
    if disable_subclass:
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

    for ix, doc in tqdm(enumerate(results)):
        if ix < num_docs:
            output_jsonl(
                document=w.WikidataRecord(doc, simple=True),
                f=output_file,
                conll_type=conll_type,
                include_instance_of=include_instance_of,
            )
        else:
            break


if __name__ == "__main__":
    main()
