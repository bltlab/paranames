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

We currently only have a data release available, but will release our code soon.


# Using the data release

## Release format

The corpus is released as a gzipped TSV file which is produced by the pipeline included in this repository.

## Release notes

### Repeated entities

In current releases, any entity that is associated with multiple named entity types (PER, LOC, ORG) in the Wikidata type hierarchy will appear multiple times in the output, once with each type. This affects less than 3% of the entities in the data.

If you want a unique set of entities, you should deduplicate the data using the `wikidata_id` field.

If you only want to use entities that are associated with a single named entity type, you should remove any `wikidata_id` that appears in multiple rows.


# Using the code

Coming soon!
