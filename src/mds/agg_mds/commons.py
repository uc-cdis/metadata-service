from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
from typing import Dict, List, Optional, Union
from pathlib import Path
from datetime import datetime
import json


@dataclass_json
@dataclass
class ColumnsToFields:
    """
    A more complex mapping object for mapping column names to MDS fields
    allows to explictly mark a field as missing, a default value and it's expected type
    """

    name: str
    missing: bool = False
    default: str = ""
    type: str = "string"


@dataclass_json
@dataclass
class MDSInstance:
    mds_url: str
    commons_url: str
    columns_to_fields: Dict[str, str]
    study_data_field: str = "gen3_discovery"
    select_field: Optional[Dict[str, str]] = None


@dataclass_json
@dataclass
class Commons:
    commons: Dict[str, MDSInstance]
    aggregation: List[str] = field(
        default_factory=lambda: ["_unique_id", "_subjects_count"]
    )


def parse_config(data: dict) -> Commons:
    """
    parses a aggregated config which defines the list of MDS services and the mapping of field to column names
    for the Ecosystem browser. Returns a dictionary of MDSInfo entries
    """

    return Commons.from_dict({"commons": data})


def parse_config_from_file(path: Path) -> Commons:
    with open(path, "rt") as infile:
        return parse_config(json.load(infile))
