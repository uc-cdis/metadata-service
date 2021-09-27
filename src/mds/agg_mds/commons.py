from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
from typing import Any, Dict, List, Optional
from datetime import datetime
import json


@dataclass_json
@dataclass
class ColumnsToFields:
    """
    A more complex mapping object for mapping column names to MDS fields
    allows to explictly mark a field as missing, a default value and it's resources type
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
    columns_to_fields: Optional[Dict[str, Any]] = None
    study_data_field: str = "gen3_discovery"
    guid_type: str = "discovery_metadata"
    select_field: Optional[Dict[str, str]] = None


@dataclass_json
@dataclass
class AdapterMDSInstance:
    mds_url: str
    commons_url: str
    adapter: str
    filters: Optional[Dict[str, Any]] = None
    field_mappings: Optional[Dict[str, Any]] = None
    per_item_values: Optional[Dict[str, Any]] = None
    study_data_field: str = "gen3_discovery"
    keep_original_fields: bool = True
    global_field_filters: List[str] = field(default_factory=list)


@dataclass_json
@dataclass
class Commons:
    gen3_commons: Dict[str, MDSInstance]
    adapter_commons: Dict[str, AdapterMDSInstance]
    aggregation: List[str] = field(
        default_factory=lambda: ["_unique_id", "_subjects_count"]
    )


def parse_config(data: Dict[str, Any]) -> Commons:
    """
    parses a aggregated config which defines the list of MDS services and the mapping of field to column names
    for the Ecosystem browser. Returns a dictionary of MDSInfo entries
    """

    return Commons.from_dict(
        {
            "gen3_commons": data.get("gen3_commons", dict()),
            "adapter_commons": data.get("adapter_commons", dict()),
        }
    )
