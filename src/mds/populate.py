import asyncio
import requests
from argparse import Namespace
from pathvalidate import ValidationError, sanitize_filepath, validate_filepath
from typing import Any, Dict, List, Optional
from mds.agg_mds import datastore, adapters
from mds.agg_mds.mds import pull_mds
from mds.agg_mds.commons import MDSInstance, ColumnsToFields, Commons, parse_config
from mds import config, logger
from pathlib import Path
from urllib.parse import urlparse
import argparse
import sys


def parse_args(argv: List[str]) -> Namespace:
    """
    Parse argument from command line in this case the input file to read from
    the output file to write to (if needed) and the command
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="config file to use", type=str, required=True)
    known_args, unknown_args = parser.parse_known_args(argv)
    return known_args


async def populate_metadata(name: str, common, results, use_temp_index=False):
    mds_arr = [{k: v} for k, v in results.items()]

    total_items = len(mds_arr)

    if total_items == 0:
        logger.warning(f"populating {name} aborted as there are no items to add")
        return

    # prefilter to remove entries not matching a certain field.
    if hasattr(common, "select_field") and common.select_field is not None:
        mds_arr = await filter_entries(common, mds_arr)

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

    await datastore.update_metadata(name, mds_arr, keys, tags, info, use_temp_index)


async def populate_info(commons_config: Commons, use_temp_index=False) -> None:
    agg_info = {
        key: value.to_dict() for key, value in commons_config.aggregations.items()
    }
    await datastore.update_global_info("aggregations", agg_info, use_temp_index)

    if commons_config.configuration.schema:
        json_schema = {
            k: v.to_schema(all_fields=True)
            for k, v in commons_config.configuration.schema.items()
        }
        await datastore.update_global_info("schema", json_schema, use_temp_index)
    await populate_drs_info(commons_config, use_temp_index)


async def populate_drs_info(commons_config: Commons, use_temp_index=False) -> None:
    if commons_config.configuration.settings.cache_drs:
        server = commons_config.configuration.settings.drs_indexd_server
        if server is not None:
            drs_data = adapters.get_metadata("drs_indexd", server, None)

            for id, entry in drs_data.get("cache", {}).items():
                await datastore.update_global_info(id, entry, use_temp_index)


async def populate_config(commons_config: Commons, use_temp_index=False) -> None:
    array_definition = {
        "array": [
            field
            for field, value in commons_config.configuration.schema.items()
            if value.type == "array"
        ]
    }

    await datastore.update_config_info(array_definition, use_temp_index)


async def main(commons_config: Commons) -> None:
    """
    Given a config structure, pull all metadata from each one in the config and cache into the following
    structure:
    {
        "configuration" : {
                schema: { dict of data schema for normalized fields }
                settings: { dict of additional configuration properties }
        },
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
        logger.info("aggregate MDS disabled")
        exit(1)

    url_parts = urlparse(config.ES_ENDPOINT)

    await datastore.init(hostname=url_parts.hostname, port=url_parts.port)

    # build mapping table for commons index

    field_mapping = {
        "mappings": {
            "properties": {
                config.AGG_MDS_DEFAULT_STUDY_DATA_FIELD: {
                    "type": "nested",
                    "properties": {
                        k: v.to_schema(True)
                        for k, v in commons_config.configuration.schema.items()
                    },
                }
            }
        }
    }

    await datastore.drop_all_temp_indexes()
    await datastore.create_temp_indexes(commons_mapping=field_mapping)

    mdsCount = 0

    # using node cards for agg mds
    node_adapters = {}
    try:
        node_card_url = "https://revproxy-service/meshcard/nodecard"
        response = requests.get(node_card_url, verify=False)
        if response.status_code == 200:
            nodes = response.json()
            for n in nodes:
                adapter = n["metadata_adapters"]["BRH"]
                commons = n["id"]
                node_adapters[commons] = adapter
    except Exception as e:
        pass

    try:
        for name, common in commons_config.gen3_commons.items():
            logger.info(f"Populating {name} using Gen3 MDS connector")
            results = pull_mds(common.mds_url, common.guid_type)
            logger.info(f"Received {len(results)} from {name}")
            if len(results) > 0:
                mdsCount += len(results)
                await populate_metadata(name, common, results, use_temp_index=True)

        for name, common in commons_config.adapter_commons.items():
            logger.info(f"Populating {name} using adapter: {common.adapter}")
            results = adapters.get_metadata(
                common.adapter,
                common.mds_url,
                common.filters,
                common.config,
                common.field_mappings,
                common.per_item_values,
                common.keep_original_fields,
                common.global_field_filters,
                schema=commons_config.configuration.schema,
            )
            logger.info(f"Received {len(results)} from {name}")
            if len(results) > 0:
                mdsCount += len(results)
                await populate_metadata(name, common, results, use_temp_index=True)

        for name, common in node_adapters.items():
            a = common.get("adapter", None)
            logger.info(f"Populating {name} using adapter: {a}")
            results = adapters.get_metadata(
                common.get("adapter"),
                common.get("mds_url"),
                common.get("filters", None),
                common.get("config"),
                common.get("field_mappings", None),
                common.get("per_item_values", None),
                common.get("keep_original_fields", True),
                common.get("global_field_filters", None),
                schema=commons_config.configuration.schema,
            )
            logger.info(f"Received {len(results)} from {name}")
            if len(results) > 0:
                mdsCount += len(results)
                await populate_metadata(name, common, results, use_temp_index=True)

        if mdsCount == 0:
            logger.info(
                "Could not obtain any metadata from any adapters. Existing indexes are left in place."
            )
            return

        # populate global information index
        await populate_info(commons_config, use_temp_index=True)
        # populate array index information to support guppy
        await populate_config(commons_config, use_temp_index=True)

    except Exception as ex:
        logger.error(
            "Error occurred during mds population. Existing indexes are left in place."
        )
        logger.error(ex)
        raise ex

    logger.info(f"Temp indexes populated successfully. Proceeding to clone")
    # All temp indexes created without error, drop current real index, clone temp to real index and then drop temp index
    try:
        await datastore.drop_all_non_temp_indexes()  # TODO: rename indexes to old
        await datastore.create_indexes(commons_mapping=field_mapping)
        await datastore.clone_temp_indexes_to_real_indexes()
        await datastore.drop_all_temp_indexes()
    except Exception as ex:
        logger.error("Error occurred during cloning.")
        logger.error(ex)
        raise ex

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
    logger.info(
        f"Loaded {len(filtered)} entries from {common.mds_url} for {common.commons_url}."
    )
    return filtered


def parse_config_from_file(path: Path) -> Optional[Commons]:
    if not path.exists():
        logger.error(f"configuration file: {path} does not exist")
        return None
    try:
        return parse_config(path.read_text())
    except IOError as ex:
        logger.error(f"cannot read configuration file {path}: {ex}")
        raise ex


def is_valid_path(path: str) -> bool:
    try:
        validate_filepath(path, platform="auto")
    except ValidationError as e:
        logger.error(f"Validation error in config file path: {e}")
        raise e
    if path != sanitize_filepath(path):
        logger.error(f"Unsafe config file path: {path}")
        return False
    return True


if __name__ == "__main__":
    """
    Runs a "populate" procedure. Assumes the datastore is ready.
    """
    args: Namespace = parse_args(sys.argv)
    if not is_valid_path(args.config):
        exit()
    commons = parse_config_from_file(Path(args.config))
    asyncio.run(main(commons_config=commons))
