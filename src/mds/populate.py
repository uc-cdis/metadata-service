import asyncio
from argparse import Namespace
from typing import Any, Dict, List
from collections import Counter
from mds.agg_mds import datastore, adapters
from mds.agg_mds.mds import pull_mds
from mds.agg_mds.commons import MDSInstance, AdapterMDSInstance, Commons, parse_config
from mds import config, logger
from pathlib import Path
import argparse
import sys
import json


def parse_args(argv: List[str]) -> Namespace:
    """
    Parse argument from command line in this case the input file to read from
    the output file to write to (if needed) and the command
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="config file to use", type=str, required=True)
    parser.add_argument(
        "--hostname", help="hostname of server", type=str, default="localhost"
    )
    parser.add_argument("--port", help="port of server", type=int, default=6379)
    known_args, unknown_args = parser.parse_known_args(argv)
    return known_args


async def populate_metadata(name: str, common, results):
    mds_arr = [{k: v} for k, v in results.items()]

    total_items = len(mds_arr)

    # prefilter to remove entries not matching a certain field.
    if hasattr(common, "select_field") and common.select_field is not None:
        mds_arr = await filter_entries(common, mds_arr)

    tags = {}

    # inject common_name field into each entry
    for x in mds_arr:
        key = next(iter(x.keys()))
        entry = next(iter(x.values()))

        def normalize(entry: dict) -> Any:
            if (
                not hasattr(common, "columns_to_fields")
                or common.columns_to_fields is None
            ):
                return entry

            for column, field in common.columns_to_fields.items():
                if field == column:
                    continue
                if field in entry[common.study_data_field]:
                    entry[common.study_data_field][column] = entry[
                        common.study_data_field
                    ][field]
            return entry

        entry = normalize(entry)

        # add the common field and url to the entry
        entry[common.study_data_field]["commons_name"] = name

        # add to tags
        for t in entry[common.study_data_field]["tags"]:
            if t["category"] not in tags:
                tags[t["category"]] = set()
            tags[t["category"]].add(t["name"])

    # process tags set to list
    for k, v in tags.items():
        tags[k] = list(tags[k])

    keys = list(results.keys())
    info = {"commons_url": common.commons_url}
    await datastore.update_metadata(
        name, mds_arr, keys, tags, info, common.study_data_field
    )


async def main(commons_config: Commons, hostname: str, port: int) -> None:
    """
    Given a config structure, pull all metadata from each one in the config and cache into the following
    structure:
    {
       "commons_name" : {
            "metadata" : [ array of metadata entries ],
            "field_mapping" : { dictionary of field_name to column_name },
            "guids: [ array of guids, used to index into the metadata array ],
            "tags": { 'category' : [ values ] },
            "commons_url" : "url of commons portal"
        },
        "..." : {
        }
    """

    if not config.USE_AGG_MDS:
        print("aggregate MDS disabled")
        exit(1)

    await datastore.init(hostname, port)
    await datastore.drop_all()

    for name, common in commons_config.gen3_commons.items():
        logger.info(f"populating {name} using Gen3 MDS connector")
        results = pull_mds(common.mds_url, common.guid_type)
        await populate_metadata(name, common, results)

    for name, common in commons_config.adapter_commons.items():
        logger.info(f"populating {name} using adapter: common.adapter")
        results = adapters.get_metadata(
            common.adapter,
            common.mds_url,
            common.filters,
            common.field_mappings,
            common.per_item_values,
            common.keep_original_fields,
        )
        await populate_metadata(name, common, results)

    res = await datastore.get_status()
    print(res)
    await datastore.close()


async def filter_entries(
    common: MDSInstance, mds_arr: List[Dict[Any, Any]]
) -> List[Dict[Any, Any]]:
    """
    Filter metadata based on the select filter object defined in
    the config file. An example:
        "select_field": {
            "field_name" : "commons" ,
            "field_value" : "Proteomic Data Commons"
        }
        where only the records with the commons field === "Proteomic Data Commons" are added.
        Note the function assumes the field exists in all of the entries in the mds_arr parameter
    """
    filtered = []
    for x in mds_arr:
        key = next(iter(x.keys()))
        entry = next(iter(x.values()))
        if common.study_data_field not in entry:
            logger.warning(
                f'"{common.study_data_field}" not found in response from {common.mds_url} for {common.commons_url}. Skipping.'
            )
            continue
        value = entry[common.study_data_field].get(
            common.select_field["field_name"], None
        )
        if value is None or common.select_field["field_value"] != value:
            continue
        filtered.append({key: entry})
    return filtered


def parse_config_from_file(path: Path) -> Commons:
    with open(path, "rt") as infile:
        return parse_config(json.load(infile))


if __name__ == "__main__":
    """
    Runs a "populate" procedure. Assumes the datastore is ready.
    """
    args: Namespace = parse_args(sys.argv)
    commons = parse_config_from_file(Path(args.config))
    asyncio.run(main(commons_config=commons, hostname=args.hostname, port=args.port))
