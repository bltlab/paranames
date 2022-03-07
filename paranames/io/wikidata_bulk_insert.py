#!/usr/bin/env python3

import math
import multiprocessing as mp

import click
from paranames.util.wikidata import (
    WikidataMongoIngesterWorker,
    DEFAULT_MONGODB_PORT,
)
from pymongo import MongoClient


def use_single_worker(worker, port=DEFAULT_MONGODB_PORT):
    client = MongoClient(port=port)
    worker.establish_mongo_client(client)
    worker()


@click.command()
@click.option("--dump-file", "-d", help="Path to dump file")
@click.option("--database-name", default="wikidata_db", help="Database name")
@click.option(
    "--collection-name", default="parallel_ingest_test", help="Collection name"
)
@click.option("--num-workers", "-w", type=int, help="Number of workers")
@click.option("--cache-size", "-c", type=int, default=1000, help="Cache size")
@click.option(
    "--max-docs",
    "-m",
    type=float,
    default=math.inf,
    help="Max number of documents to ingest",
)
@click.option("--debug", is_flag=True)
@click.option(
    "--simple-records",
    is_flag=True,
    help="Keep only name, id, aliases, instance_ofs, and languages",
)
def main(
    dump_file,
    database_name,
    collection_name,
    num_workers,
    cache_size,
    max_docs,
    debug,
    simple_records,
) -> None:

    workers = [
        WikidataMongoIngesterWorker(
            name=f"WikidataMongoIngestWorker{i}",
            input_path=dump_file,
            database_name=database_name,
            collection_name=collection_name,
            read_every=num_workers,
            start_at=i,
            cache_size=cache_size,
            max_docs=max_docs,
            debug=debug,
            simple_records=simple_records,
        )
        for i in range(1, num_workers + 1)
    ]

    with mp.Pool(processes=num_workers) as pool:
        pool.map(use_single_worker, workers)

    # finally non-parallel error logging
    print("JSON decode error summary:")

    for worker in workers:
        worker.error_summary()


if __name__ == "__main__":
    main()
