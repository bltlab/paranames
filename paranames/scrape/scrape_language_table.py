import sys

import click
import pandas as pd
from qwikidata.sparql import return_sparql_query_results
from paranames.util import orjson_dump


def create_en_wikipedia_url(wikipedia_url: str):
    tail = wikipedia_url.split("|")[-1].replace("]]", "").replace(" ", "_")

    return f"https://en.wikipedia.org/wiki/{tail}"


def get_language_codes_sparql():

    lang_codes_query = """
    SELECT
      ?item 
      ?c (CONTAINS(?c,"-") as ?subtag)
      ?wdlabelen
      (CONCAT("[[:en:",?enwikipeda,"\u007C",?enwikipeda,"]]") as ?wikipedia_link_en)
      ?lang
      ?wdlabelinlang
      (CONCAT("[[:",?lang,":",?wikipeda,"\u007C",?wikipeda,"]]") as ?wikipedia_link)
    WHERE
    {
      VALUES ?lang { "fr" }
      ?item wdt:P424 ?c .
      hint:Prior hint:rangeSafe true .
      MINUS{?item wdt:P31 wd:Q47495990}
      MINUS{?item wdt:P31/wdt:P279* wd:Q14827288} #exclude Wikimedia projects
      MINUS{?item wdt:P31/wdt:P279* wd:Q17442446} #exclude Wikimedia internal stuff
      OPTIONAL { ?item rdfs:label ?wdlabelinlang . FILTER( lang(?wdlabelinlang)= "fr" ) }
      OPTIONAL { ?item rdfs:label ?wdlabelen . FILTER(lang(?wdlabelen)="en") }
      OPTIONAL { [] schema:about ?item ; schema:inLanguage ?lang; schema:isPartOf / wikibase:wikiGroup "wikipedia" ; schema:name ?wikipeda } 
      OPTIONAL { [] schema:about ?item ; schema:inLanguage "en"; schema:isPartOf / wikibase:wikiGroup "wikipedia" ; schema:name ?enwikipeda } 
    }
    ORDER BY ?c
    """

    lang_dicts = return_sparql_query_results(lang_codes_query)["results"]["bindings"]
    output = []
    for ld in lang_dicts:
        row = {}
        row["wikidata_url"] = ld["item"]["value"]
        row["lang_code"] = ld["c"]["value"]
        row["language"] = ld.get("wdlabelen", {}).get("value", row["lang_code"])
        row["is_subtag"] = {"true": True, "false": False}.get(
            ld["subtag"]["value"], False
        )
        row["en_wikipedia_link"] = create_en_wikipedia_url(
            ld.get("wikipedia_link_en", {}).get("value", "")
        )

        output.append(row)

    return output


@click.command()
@click.option(
    "--url",
    "-u",
    default="https://en.wikipedia.org/wiki/List_of_Wikipedias#List",
    help="URL to scrape",
)
@click.option(
    "--columns",
    "-c",
    default="lang_code,language,is_subtag,en_wikipedia_link,wikidata_url",
    help="Comma-separated list of column names",
)
@click.option("--african-only", is_flag=True)
@click.option("--abbrev-only", is_flag=True)
@click.option("--mapping-only", is_flag=True)
@click.option("--output-tsv", is_flag=True)
def main(url, columns, african_only, abbrev_only, mapping_only, output_tsv):

    output = get_language_codes_sparql()
    df = pd.DataFrame.from_records(output, columns=columns.split(","))

    if abbrev_only:
        print("\n".join(df.lang_code.unique()))
    elif mapping_only:
        out = {}
        for row in df.to_dict(orient="records"):
            out[row["lang_code"]] = row["language"]
        print(orjson_dump(out))
    elif output_tsv:
        with sys.stdout as stdout:
            df.to_csv(
                stdout,
                sep="\t",
                encoding="utf-8",
                index=False,
            )

    else:
        for row in df.to_dict(orient="records"):
            print(orjson_dump(row))


if __name__ == "__main__":
    main()
