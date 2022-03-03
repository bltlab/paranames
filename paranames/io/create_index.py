import time

from pymongo import MongoClient
import paranames.util.wikidata as w
import click


@click.command()
@click.option("--database-name", "-db", default="wikidata_db")
@click.option("--collection-name", "-c", default="wikidata_simple")
@click.option("--index-field", "-f", required=True)
@click.option("--mongodb-port", "-p", type=int, default=w.DEFAULT_MONGODB_PORT)
def main(
    database_name: str,
    collection_name: str,
    index_field: str,
    mongodb_port: int,
) -> None:
    client = MongoClient(port=mongodb_port)
    db = client[database_name][collection_name]
    print(f'Creating index for field: "{index_field}"')
    t_start = time.time()
    db.create_index(index_field)
    t_end = time.time()
    elapsed = round(t_end - t_start, 3)
    print(f"Done! Time elapsed (sec): {elapsed}")


if __name__ == "__main__":
    main()
