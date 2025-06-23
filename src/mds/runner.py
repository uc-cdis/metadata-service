import asyncio
from argparse import Namespace
from pathvalidate import ValidationError, sanitize_filepath, validate_filepath
from typing import Any, Dict, List, Optional
from mds.agg_mds import adapters
from mds.agg_mds.mds import pull_mds
from mds.agg_mds.commons import MDSInstance, ColumnsToFields, Commons, parse_config
from mds import config, logger
from pathlib import Path
import json
import argparse
import sys

import requests
import datetime
import json
import re
import httpx
import logging
import pandas as pd


def parse_args(argv: List[str]) -> Namespace:
    """
    Parse argument from the command line in this case the input file to read from
    the output file to write to (if needed) and the command
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="config file to use", type=str, required=True)
    known_args, unknown_args = parser.parse_known_args(argv)
    return known_args


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


async def main(commons_config: Commons) -> None:
    if not config.USE_AGG_MDS:
        logger.info("aggregate MDS disabled")
        exit(1)

    mdsCount = 0
    try:
        for name, common in commons_config.gen3_commons.items():
            logger.info(f"Populating {name} using Gen3 MDS connector")
            results = pull_mds(common.mds_url, common.guid_type)
            logger.info(f"Received {len(results)} from {name}")

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

            # Write results to a JSON file
            output_filename = f"{name}_results.json"
            with open(output_filename, "w") as json_file:
                json.dump(results, json_file, indent=4)
            logger.info(f"Results written to {output_filename}")

    except Exception as ex:
        logger.error("Error occurred during mds population.")
        logger.error(ex)
        raise ex


if __name__ == "__main__":
    """
    Runs a "populate" procedure. Assumes the datastore is ready.
    """
    args: Namespace = parse_args(sys.argv)
    if not is_valid_path(args.config):
        exit()
    commons = parse_config_from_file(Path(args.config))
    asyncio.run(main(commons_config=commons))
