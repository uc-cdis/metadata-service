import asyncio
from argparse import Namespace
from typing import Any, Dict, List
from collections import Counter
from mds.agg_mds import datastore
from mds.agg_mds.mds import pull_mds
from mds.agg_mds.commons import MDSInstance, Commons, parse_config_from_file
from mds import config
from pathlib import Path
import argparse
import sys


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


async def insert_data(commons_config):
    for name, common in commons_config.commons.items():
        results = pull_mds(common.mds_url)
        mds_arr = [{k: v} for k, v in results.items()]

        total_items = len(mds_arr)

        # prefilter to remove entries not matching a certain field.
        if common.select_field is not None:
            mds_arr = await filter_entries(common, mds_arr)

        tags = {}

        # inject common_name field into each entry
        for x in mds_arr:
            key = next(iter(x.keys()))
            entry = next(iter(x.values()))

            def normalize(entry: Dict[Any, Any]):
                for column, field in common.columns_to_fields.items():
                    if field == column:
                        continue
                    if column in entry["gen3_discovery"]:
                        entry["gen3_discovery"][field] = entry["gen3_discovery"][column]
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

        def normalize(entry: Dict[Any, Any]):
            # The entry is an object with one top-level key. Its own id.
            id = list(entry.keys())[0]
            for column, field in common.columns_to_fields.items():
                if field == column:
                    continue
                if column in entry[id]["gen3_discovery"]:
                    entry[id]["gen3_discovery"][field] = entry[id]["gen3_discovery"][
                        column
                    ]
            return entry

        data = [normalize(x) for x in mds_arr]

        keys = list(results.keys())
        info = {"commons_url": common.commons_url}
        res = await datastore.update_metadata(name, mds_arr, keys, tags, info)


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

    await insert_data(commons_config)

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
        value = entry[common.study_data_field].get(
            common.select_field["field_name"], None
        )
        if value is None or common.select_field["field_value"] != value:
            continue
        filtered.append({key: entry})
    return filtered


if __name__ == "__main__":
    """
    Runs a "populate" procedure. Assumes the datastore is ready.
    """
    args: Namespace = parse_args(sys.argv)
    commons = parse_config_from_file(Path(args.config))
    asyncio.run(main(commons_config=commons, hostname=args.hostname, port=args.port))
