import asyncio
import redis
from mds.agg_mds import datastore
from mds import config, logger
from urllib.parse import urlparse
from mds.agg_mds.commons import MDSInstance, ColumnsToFields, Commons, parse_config
from pathlib import Path
import requests
import json
from typing import Any, Optional
import argparse
import sys

# Okay, so what I need to do in this file is create a redis subscription
# Every time there is an upate on it, use it to update ES

def parse_config_from_file(path: Path) -> Optional[Commons]:
    if not path.exists():
        logger.error(f"configuration file: {path} does not exist")
        return None
    try:
        return parse_config(path.read_text())
    except IOError as ex:
        logger.error(f"cannot read configuration file {path}: {ex}")
        raise ex

commons = parse_config_from_file(Path("./agg_mds_config.json"))

if not config.USE_AGG_MDS:
    logger.info("aggregate MDS disabled")
    exit(1)

# 1. Init connections
# Setup connection to ES
# Might need to be worried about config?
# Not sure, but I know populate runs inside the pod right, so this should be okay
url_parts = urlparse(config.ES_ENDPOINT)

# Copied this function from populate.py, need to modify it to work for my needs
async def populate_metadata(name: str, common, results, use_temp_index=False):
    mds_arr = [{k: v} for k, v in results.items()]

    total_items = len(mds_arr)

    if total_items == 0:
        logger.warning(f"populating {name} aborted as there are no items to add")
        return

    tags = {}

    # inject common_name field into each entry
    for x in mds_arr:
        key = next(iter(x.keys()))
        entry = next(iter(x.values()))

        def normalize(entry: dict) -> Any:
            # normalize study level metadata field names
            if common.study_data_field != config.AGG_MDS_DEFAULT_STUDY_DATA_FIELD:
                entry[config.AGG_MDS_DEFAULT_STUDY_DATA_FIELD] = entry.pop(
                    common.study_data_field
                )
            # normalize variable level metadata field names, if available
            if (
                common.data_dict_field is not None
                and common.data_dict_field != config.AGG_MDS_DEFAULT_DATA_DICT_FIELD
            ):
                entry[config.AGG_MDS_DEFAULT_DATA_DICT_FIELD] = entry.pop(
                    common.data_dict_field
                )

            if (
                not hasattr(common, "columns_to_fields")
                or common.columns_to_fields is None
            ):
                return entry

            for column, field in common.columns_to_fields.items():
                if field == column:
                    continue
                if isinstance(field, ColumnsToFields):
                    entry[config.AGG_MDS_DEFAULT_STUDY_DATA_FIELD][
                        column
                    ] = field.get_value(entry[config.AGG_MDS_DEFAULT_STUDY_DATA_FIELD])
                else:
                    if field in entry[config.AGG_MDS_DEFAULT_STUDY_DATA_FIELD]:
                        entry[config.AGG_MDS_DEFAULT_STUDY_DATA_FIELD][column] = entry[
                            config.AGG_MDS_DEFAULT_STUDY_DATA_FIELD
                        ][field]
            return entry

        entry = normalize(entry)

        # add the common field, selecting the name or an override (i.e. commons_name) and url to the entry

        entry[config.AGG_MDS_DEFAULT_STUDY_DATA_FIELD]["commons_name"] = (
            common.commons_name
            if hasattr(common, "commons_name") and common.commons_name is not None
            else name
        )

        # add to tags
        for t in entry[config.AGG_MDS_DEFAULT_STUDY_DATA_FIELD].get("tags") or {}:
            if "category" not in t:
                continue
            if t["category"] not in tags:
                tags[t["category"]] = set()
            if "name" in t:
                tags[t["category"]].add(t["name"])

    # process tags set to list
    for k, v in tags.items():
        tags[k] = list(tags[k])

    keys = list(results.keys())
    info = {"commons_url": common.commons_url}

    # print(name)
    # print(mds_arr)
    # print(keys)
    # print(tags)
    # print(info)
    # print(use_temp_index)

    await datastore.update_metadata(name, mds_arr, keys, tags, info, use_temp_index)

async def do_work():
    await datastore.init(hostname=url_parts.hostname, port=url_parts.port)

    # Setup connection to Redis
    # Gonna hard-code one ip address for now, will fix with config later
    redis_client = redis.Redis(host='alt.data-one.dev.planx-pla.net', port=6379, db=0)
    channel = 'my_channel'

    # 2. Make redis spin
    pubsub = redis_client.pubsub()
    pubsub.subscribe(channel)
    print(f"Subscribed to {channel}. Waiting for messages...")
    for message in pubsub.listen():
        if message['type'] == 'message':
            message_data = message['data'].decode('utf-8')
            print(f"Received: {message_data}")
            message_array = message_data.split()
            rest_route = message_array[0]
            guid = message_array[1]
            # post
            if rest_route == "POST":
                print(f"Getting https://data-one.dev.planx-pla.net/mds/metadata/{guid}")

                # 3. Make switch statement to update ES according to redis updates
                # POST
                # get the data
                response = requests.get(f"https://data-one.dev.planx-pla.net/mds/metadata/{guid}")
                json_data = json.loads(response.text)

                # Add to ES
                for name, common in commons.adapter_commons.items():
                    await populate_metadata(message_data, common, json_data, False)
                
            # put
            elif rest_route == "PUT":
                print("PUT not implemented")
                pass

            # delete
            elif rest_route == "DELETE":
                print("DELETE not implemented")
                pass

asyncio.run(do_work())

# do_work() should never stop, right?

# (not code, note to self) 4. Verify the updates are working on ES as expected

# (not code, note to self) 5. Scale