# OurResource

This repository contains information about the OurResource corpus and associated experiments. OurResource consisting of parallel names of over 12 million named entities in over 400 languages.

Please cite as:
```
TODO
```

## Releases

### Format

The corpus is released as a gzipped TSV file which contains the following columns:

* `wikidata_id`: the Wikidata ID of the entity
* `eng`: the English name of the entity
* `label`: the name of the entity in the language of the row
* `language`: the language of the row
* `type`: the type of the entity (PER, LOC, ORG)

Some example rows are shown below:
```
wikidata_id     eng     label   language        type
Q181893 Fredericksburg and Spotsylvania National Military Park  Fredericksburg and Spotsylvania National Military Park  mg      LOC
Q257160 Burgheim        Burgheim        fr      LOC
Q508851 Triefenstein    Triefenstein    nl      LOC
Q303923 Ruhstorf an der Rott    Ruhstorf an der Rott    bar     LOC
Q284696 Oberelsbach     Oberelsbach     wo      LOC
Q550561 Triftern        Թրիֆթերն        hy      LOC
Q529488 Reisbach        Reisbach        fr      LOC
Q385427 Stadtlauringen  Stadtlauringen  ia      LOC
Q505327 Wildflecken     Wildflecken     id      LOC
Q505288 Ipsheim Իպսհայմ hy      LOC
```

### Notes

#### Date

The experiments are based on the Wikipedia dump from `2022-08-22`.

#### Repeated entities

In current releases, any entity that is associated with multiple named entity types (PER, LOC, ORG) in the Wikidata type hierarchy will appear multiple times in the output, once with each type.
This affects less than 3% of the entities in the data.

If you want a unique set of entities, you should deduplicate the data using the `wikidata_id` field.

If you only want to use entities that are associated with a single named entity type, you should remove any `wikidata_id` that appears in multiple rows.


# Creating a new release

**NOTE**: the code will be released upon publication

### Dependencies

First, install the following non-Python dependencies:

- MongoDB
- [xsv](https://github.com/BurntSushi/xsv)
- ICU support for your computer (e.g. `libicu-dev`)

Next, install OurResource and its Python dependencies by running `pip install -e .`.

It is recommended that you use a Conda environment for package management.

### Running the pipeline

To create a corpus following our approach, follow the steps below:

1. Download the latest Wikidata dump from the [Wikimedia page](https://dumps.wikimedia.org/wikidatawiki/entities/) and extract it. Note that this may take up several TB of disk space.
2. Use `recipes/whole_pipeline.sh` which ingests the Wikidata JSON to MongoDB and then dumps and postprocesses it to our final TSV resource.

The call to `recipes/whole_pipeline.sh` works as follows:

```
recipes/whole_pipeline.sh <path_to_extracted_json_dump> <output_folder> <n_workers>
```

Set the number of workers based on the number of CPUs your machine has.
By default, only 1 CPU is used.

The output folder will contain one subfolder per language, inside of which `paranames_<language_code>.tsv` can be found.
The entire resource is located in `<output_folder>/combined/our_resource.tsv`.

### Notes


OurResource offers several options for customization:

- If your MongoDB instance uses a non-standard port, you should change the value of `mongodb_port` accordingly inside `whole_pipeline.sh`.

- Setting `should_collapse_languages=yes` will cause Wikimedia language codes to be "collapsed" to the top-level Wikimedia language code, i.e. `kk-cyrl` will be converted to `kk`, `en-ca` to `en` etc.

- Setting `should_keep_intermediate_files=yes` will cause intermediate files to be deleted. This includes the raw per-type TSV dumps (`{PER,LOC,ORG}.tsv`) from MongoDB, as well as outputs of `postprocess.py`.

- Within `recipes/dump.sh`, it is also possible to define languages to be excluded and whether entity types should be disambiguated. By default, no languages are excluded and no disambiguation is done.

- After the pipeline completes, `<output_folder>` will contain one folder per language, inside of which is a TSV file containing the subset of names in that language. Combined TSVs with names in all languages are available in the `combined` folder.
