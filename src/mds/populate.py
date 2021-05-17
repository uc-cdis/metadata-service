import asyncio
from argparse import Namespace
from typing import Any, Dict, List
from collections import Counter
from mds.agg_mds.mds import pull_mds
from mds.agg_mds.commons import MDSInstance, Commons, parse_config_from_file
from mds.agg_mds.redis_cache import redis_cache
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
        "--hostname", help="hostname of redis server", type=str, default="localhost"
    )
    parser.add_argument("--port", help="port of redis server", type=int, default=6379)
    known_args, unknown_args = parser.parse_known_args(argv)
    return known_args


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

    await redis_cache.init_cache(hostname, port)
    await redis_cache.json_sets("commons", [])

    for name, common in commons_config.commons.items():
        results = pull_mds(common.mds_url)
        mds_arr = [{k: v} for k, v in results.items()]

        total_items = len(mds_arr)

        # prefilter to remove entries not matching a certain field.
        if common.select_field is not None:
            mds_arr = await filter_entries(common, mds_arr)

        tags = {}
        aggregations = {
            x: {"count": 0, "missing": 0, "sum": 0, "notANumber": 0}
            for x in commons_config.aggregation
        }
        # inject common_name field into each entry
        for x in mds_arr:
            key = next(iter(x.keys()))
            entry = next(iter(x.values()))

            # add the common field and url to the entry
            entry[common.study_data_field]["commons_name"] = name

            # add to tags
            for t in entry[common.study_data_field]["tags"]:
                if t["category"] not in tags:
                    tags[t["category"]] = set()
                tags[t["category"]].add(t["name"])

            # build aggregation counts on column fields
            for aggName in commons_config.aggregation:
                if (
                    aggName not in common.columns_to_fields.keys()
                    or common.columns_to_fields[aggName]
                    not in entry[common.study_data_field]
                ):
                    aggregations[aggName]["missing"] += 1
                else:
                    aggregations[aggName]["count"] += 1
                    try:
                        value = int(
                            entry[common.study_data_field][
                                common.columns_to_fields[aggName]
                            ]
                        )
                        aggregations[aggName]["sum"] += value
                    except ValueError:
                        aggregations[aggName]["notANumber"] += 1

            # process tags set to list
        for k, v in tags.items():
            tags[k] = list(tags[k])

        # build index of keys. which is used to compute the index into the .metadata array
        # Admittedly a hack but will be faster than using json path, until the release of RedisJson v1.2
        keys = list(results.keys())
        info = {"commons_url": common.commons_url}
        await redis_cache.update_metadata(
            name, mds_arr, common.columns_to_fields, keys, tags, info, aggregations
        )

    res = await redis_cache.get_status()
    print(res)
    await redis_cache.close()


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
    Runs a redis "populate" procedure. Assumes Redis is already running.
    """
    args: Namespace = parse_args(sys.argv)
    commons = parse_config_from_file(Path(args.config))
    asyncio.run(main(commons_config=commons, hostname=args.hostname, port=args.port))
