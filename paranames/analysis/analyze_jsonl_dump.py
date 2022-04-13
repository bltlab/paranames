#!/usr/bin/env python

from collections import defaultdict, Counter
from typing import Iterable, Dict
import sys

import click
import orjson
import pymongo
from tqdm import tqdm
from paranames.util import orjson_dump


def look_up_names(
    names_to_look_up: Iterable[str],
    mongodb_port: int,
    db_name: str,
    collection_name: str,
) -> Dict[str, str]:
    id_to_name = {}
    client = pymongo.MongoClient(port=mongodb_port)
    db = client[db_name]
    coll = db[collection_name]
    results = coll.find({"id": {"$in": [wid for wid in names_to_look_up]}})
    n = 0
    for entity in results:
        id_to_name[entity["id"]] = entity["name"]
        n += 1

    if n < 1:
        raise ValueError(
            f"No results found in MongoDB! db={db_name}, collection={collection_name}, port={mongodb_port}"
        )
    return id_to_name


@click.command()
@click.option(
    "--input-file",
    type=click.Path(readable=True, dir_okay=False, file_okay=True, allow_dash=True),
    required=True,
)
@click.option(
    "--output-folder",
    type=click.Path(writable=True, dir_okay=True, file_okay=False, allow_dash=False),
    required=True,
)
@click.option("--mongodb-port", required=True, type=int)
@click.option("--db-name", required=True)
@click.option("--collection-name", required=True)
@click.option("--wikidata-id-key", default="wikidata_id")
@click.option("--name-key", default="name")
@click.option("--type-key", default="type")
@click.option("--instance-of-key", default="instance_of")
def main(
    input_file,
    output_folder,
    mongodb_port,
    db_name,
    collection_name,
    wikidata_id_key,
    name_key,
    type_key,
    instance_of_key,
    *args,
    **kwargs,
):

    types_per_id = defaultdict(set)
    instance_ofs_per_type = defaultdict(Counter)
    instance_ofs_human_readable = defaultdict(Counter)
    look_up_these = set()
    overlap_statistics_output_file = Path(output_folder) / "overlap_statistics.json"

    with click.open_file(input_file, encoding="utf-8") as fin:
        for line in tqdm(fin, desc="Reading lines from JSONL"):
            jsonl = orjson.loads(line)

            wikidata_id = jsonl[wikidata_id_key]
            instance_of = jsonl[instance_of_key]
            conll_type = jsonl[type_key]

            types_per_id[wikidata_id].add(conll_type)
            instance_ofs_per_type[conll_type].update(instance_of)
            look_up_these.update(instance_of)

    print(
        f"Querying {db_name}.{collection_name} for human-readable entity names...",
        file=sys.stderr,
    )
    id_to_name = look_up_names(
        look_up_these,
        mongodb_port=mongodb_port,
        db_name=db_name,
        collection_name=collection_name,
    )

    for conll_type, counter in tqdm(
        instance_ofs_per_type.items(),
        desc="Converting instance-ofs to human readable form",
    ):
        new_counter = Counter()
        for wikidata_id, count in counter.items():
            new_counter[id_to_name.get(wikidata_id, wikidata_id)] = count
        instance_ofs_human_readable[conll_type] = new_counter

    for conll_type, counter in tqdm(
        instance_ofs_per_type.items(),
        desc="Outputting instance-of statistics...",
    ):
        instance_of_histogram_output_file = (
            output_folder / f"{conll_type}_instance_of_counts.txt"
        )
        with click.open_file(
            instance_of_histogram_output_file, encoding="utf-8", mode="a"
        ) as hist_out:
            for wid, count in instance_ofs_human_readable[conll_type].most_common():
                hist_out.write(f"{wid}\t{count}\n")

    overlap_statistics = Counter()
    for wid, type_set in tqdm(
        types_per_id.items(), desc="Computing overlap statistics"
    ):
        type_str = "-".join(sorted(type_set))
        overlap_statistics[type_str] += 1

    with click.open_file(
        overlap_statistics_output_file, encoding="utf-8", mode="w"
    ) as stats_out:
        stats_out.write(orjson_dump(overlap_statistics))


if __name__ == "__main__":
    main()
