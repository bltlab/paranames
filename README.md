# ParaNames: A multilingual resource for parallel names

This repository contains releases for the ParaNames corpus, as introduced in
[Sälevä, J. and Lignos, C., 2022. ParaNames: A Massively Multilingual Entity Name Corpus. arXiv preprint arXiv:2202.14035](https://arxiv.org/abs/2202.14035).

Please cite as:
```
@article{saleva2022paranames,
  title={ParaNames: A Massively Multilingual Entity Name Corpus},
  author={S{\"a}lev{\"a}, Jonne and Lignos, Constantine},
  journal={arXiv preprint arXiv:2202.14035},
  year={2022}
}
```

See the [Releases page](https://github.com/bltlab/paranames/releases) for the downloadable release.

# Using the data release

## Release format

The corpus is released as a gzipped TSV file which is produced by the pipeline included in this repository.

## Release notes

### Repeated entities

In current releases, any entity that is associated with multiple named entity types (PER, LOC, ORG) in the Wikidata type hierarchy will appear multiple times in the output, once with each type. This affects less than 3% of the entities in the data.

If you want a unique set of entities, you should deduplicate the data using the `wikidata_id` field.

If you only want to use entities that are associated with a single named entity type, you should remove any `wikidata_id` that appears in multiple rows.


# Using the code

First, install the following non-Python dependencies:

- MongoDB
- [xsv](https://github.com/BurntSushi/xsv)
- ICU support for your computer (e.g. `libicu-dev`)

Next, install ParaNames and its Python dependencies by running `pip install -e .`.

It is recommended that you use a Conda environment for package management.

## Creating the ParaNames corpus

To create a corpus following our approach, follow the steps below:

1. Download the latest Wikidata dump from the [Wikimedia page](https://dumps.wikimedia.org/wikidatawiki/entities/) and extract it. Note that this may take up several TB of disk space.
2. Use `recipes/paranames_pipeline.sh` which ingests the Wikidata JSON to MongoDB and then dumps and postprocesses it to our final TSV resource.

The call to `recipes/paranames_pipelin.sh` works as follows:

```
recipes/paranames_pipeline.sh <path_to_extracted_json_dump> <output_folder>
```

### Notes

Inside of the pipeline script, there are several options for customization:

- Setting `should_lump_languages=yes` will cause Wikimedia language codes to be "collapsed" to the top-level Wikimedia language code, i.e. `kk-cyrl` will be converted to `kk`, `en-ca` to `en` etc.

- Within [`recipes/dump.sh`](https://github.com/bltlab/paranames/blob/main/recipes/separate_folder_dump.sh), it is also possible to define languages to be excluded and whether entity types should be disambiguated. By default, no languages are excluded and no disambiguation is done.

- After the pipeline completes, `<output_folder>` will contain one folder per language, inside of which is a TSV file containing the subset of names in that language. Combined TSVs with names in all languages are available in the `combined` folder.
