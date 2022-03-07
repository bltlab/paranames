import orjson
import math

from typing import Generator, Set, List, Union, Dict, Any
from pymongo import MongoClient
from paranames.util import orjson_dump
from tqdm import tqdm

DEFAULT_MONGODB_PORT = 27617


class WikidataRecord:
    def __init__(
        self, record: dict, default_lang: str = "en", simple: bool = False
    ) -> None:
        self.simple = simple
        self.record = record
        self.default_lang = default_lang
        self.parse_ids()
        self.parse_instance_of()
        self.parse_labels()
        self.parse_languages()
        self.parse_ipa()

    def parse_ids(self) -> None:
        self.wikidata_id = self.record["id"]
        try:
            self.mongo_id = self.record["_id"]
        except KeyError:
            self.mongo_id = None

    def parse_instance_of(self) -> None:
        if self.simple:
            self.instance_ofs = self.record["instance_of"]
        else:
            try:
                self.instance_ofs = set(
                    iof["mainsnak"]["datavalue"]["value"]["id"]

                    for iof in self.record["claims"]["P31"]
                )
            except KeyError:
                self.instance_ofs = set()

    def parse_labels(self) -> None:
        if self.simple:
            self.labels = self.record["labels"]
        else:
            self.labels = {
                lang: d["value"] for lang, d in self.record["labels"].items()
            }

    def parse_languages(self) -> None:
        if self.simple:
            self._languages = self.record["languages"]
        else:
            self._languages = {lang for lang in self.labels}

    def parse_ipa(self) -> None:
        pass

    @property
    def languages(self) -> Set[str]:
        """Returns a set of languages in which the entity has a transliteration."""

        return self._languages

    @property
    def name(self) -> str:
        try:
            if not hasattr(self, "_name"):
                self._name = self.labels[self.default_lang]

            return self._name
        except KeyError:
            return self.wikidata_id

    def instance_of(self, classes: Set[str]) -> bool:
        """Checks whether the record is an instance of a set of classes"""

        return len(self.instance_ofs.intersection(classes)) > 0

    def to_dict(self, simple=False) -> dict:
        if simple:
            return {
                "id": self.wikidata_id,
                "name": self.name,
                "labels": self.labels,
                "instance_of": list(self.instance_ofs),
                "languages": list(self._languages),
            }
        else:
            return self.record

    def to_json(self, simple=True) -> str:
        return orjson_dump(self.to_dict(simple))

    def __str__(self) -> str:
        return f'WikidataRecord(name="{self.name}", id="{self.wikidata_id}, mongo_id={self.mongo_id} instance_of={self.instance_ofs})"'

    def __repr__(self) -> str:
        return str(self)


class WikidataMongoDB:
    """Class for interfacing with Wikidata dump ingested into a MongoDB instance."""

    def __init__(
        self,
        database_name: str = "wikidata_db",
        collection_name: str = "wikidata",
    ) -> None:
        self.database_name = database_name
        self.collection_name = collection_name
        self.client = MongoClient(port=DEFAULT_MONGODB_PORT)
        self.collection = self.client[self.database_name][self.collection_name]

    def find_matching_docs(
        self,
        filter_dict: Union[dict, None] = None,
        n: Union[float, int] = math.inf,
        as_record: bool = False,
        simple: bool = False,
    ) -> Generator[Union[Dict[str, Any], WikidataRecord], None, None]:
        """Generator to yield at most n documents matching conditions in filter_dict."""

        if filter_dict is None:
            # by default, find everything that is an instance of something
            filter_dict = {"claims.P31": {"$exists": True}}

        for ix, doc in enumerate(self.collection.find(filter_dict)):
            if ix < n:
                yield WikidataRecord(doc, simple=simple) if as_record else doc
            else:
                break


class WikidataMongoIngesterWorker:
    """Class to handle reading every Nth line of Wikidata
    and ingesting them to MongoDB."""

    def __init__(
        self,
        name: str,
        input_path: str,
        database_name: str,
        collection_name: str,
        read_every: int = 1,
        start_at: int = 0,
        cache_size: int = 100,
        max_docs: Union[float, int] = math.inf,
        error_log_path: str = "",
        debug: bool = False,
        simple_records: bool = False,
    ) -> None:

        # naming and error logging related attributes
        self.name = name
        self.error_log_path = (
            error_log_path if error_log_path else f"/tmp/{self.name}.error.log"
        )

        # reading-related attributes
        self.input_path = input_path
        self.start_at = start_at
        self.next_read_at = start_at
        self.read_every = read_every

        # database-related attributes
        self.database_name = database_name
        self.collection_name = collection_name

        # caching-related attributes
        self.cache_size = cache_size
        self.cache_used = 0
        self.cache: List[Union[str, Dict[Any, Any]]] = []

        # misc attributes
        self.max_docs = max_docs
        self.n_decode_errors = 0
        self.debug = debug
        self.simple_records = simple_records

    def establish_mongo_client(self, client) -> None:
        self.client = client
        self.db = self.client[self.database_name][self.collection_name]

    def write(self) -> None:
        """Writes cache contents (JSON list) to MongoDB"""

        if self.cache:
            if self.debug:
                print(f"Worker {self.name} inserting to MongoDB...")
            self.db.insert_many(self.cache)
            self.cache = []
            self.cache_used = len(self.cache)
        else:
            print(f"Cache empty for worker {self.name}. Not writing...")

    @property
    def cache_full(self) -> bool:
        """The cache is defined to be full when its size
        is at least as large as self.cache_size."""

        if self.cache_used >= self.cache_size:
            if self.debug:
                print(
                    f"Cache full for worker {self.name}. Used: {self.cache_used}, Size: {self.cache_size}"
                )

            return True
        else:
            return False

    def error_summary(self) -> None:
        print(f"Worker {self.name}, JSON decode errors: {self.n_decode_errors}")

    def __call__(self) -> None:
        """Main method for invoking the read procedure.

        Iterates over Wikidata JSON dump (decompressed),
        reads every Nth line starting from a given line,
        caches the ingested lines, and bulk inserts them
        to a specified MongoDB collection as required."""

        with open(self.input_path, encoding="utf-8") as f:
            for line_nr, line in tqdm(enumerate(f, start=1)):

                # if we're too early, skip

                if line_nr < self.start_at:
                    continue

                # if we're past the max, stop

                if line_nr > self.max_docs:
                    self.write()

                    break

                # if we're exactly at the right spot, parse JSON

                if line_nr == self.next_read_at:
                    try:
                        doc = orjson.loads(line.rstrip(",\n"))
                        record = WikidataRecord(doc)
                        self.cache.append(record.to_dict(simple=self.simple_records))
                        self.cache_used += 1
                    except orjson.JSONDecodeError:
                        # in case of decode error, log it and keep going
                        self.n_decode_errors += 1

                        continue

                    # in either case, take note of next line to read at
                    self.next_read_at += self.read_every

                # always write if our cache is full

                if self.cache_full:
                    self.write()

            # finally write one more time
            self.write()
