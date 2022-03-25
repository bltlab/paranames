#!/usr/bin/env python3

"""
Get Wikidata subclass ontology as JSON and insert them to MongoDB

"""

from typing import Dict, Any

import click
from pymongo import MongoClient
from qwikidata.sparql import get_subclasses_of_item

from paranames.util import orjson_dump


def grab_subclasses(entity_id: str) -> Dict[str, Any]:
    subclasses = get_subclasses_of_item(entity_id)

    return {"id": entity_id, "subclasses": subclasses}


@click.command()
@click.option(
    "--entity-ids",
    default="",
    help="Comma-separated list of IDs whose subclasses we want",
    required=True,
)
@click.option("--database-name", "-db", required=True)
@click.option("--collection-name", "-c", required=True)
@click.option("--mongodb-port", "-p", required=True, type=int)
@click.option("--to-stdout", is_flag=True)
def main(entity_ids, database_name, collection_name, mongodb_port, to_stdout) -> None:

    documents = [grab_subclasses(eid) for eid in entity_ids.split(",")]

    if to_stdout:
        for document in documents:
            print(orjson_dump(document))
    else:
        client = MongoClient(port=mongodb_port)
        collection = client[database_name][collection_name]
        collection.insert_many(documents)


if __name__ == "__main__":
    main()
