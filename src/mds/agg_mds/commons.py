from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
from typing import Any, Dict, List, Optional, Union


@dataclass_json
@dataclass
class ColumnsToFields:
    """
    A more complex mapping object for mapping column names to MDS fields
    allows to explicitly mark a field as missing, a default value and it's resources type
    """

    name: str
    missing: bool = False
    default: str = ""
    type: str = "string"

    def get_value(self, info: dict):
        return info.get(self.name, self.default)


@dataclass_json
@dataclass
class FieldAggregation:
    """
    Provides a description of what fields to compute summary information.
    The default assumes computing the sum of the field, assuming it is a number
    the functions supported are: sum and count
    """

    type: str = "number"
    function: str = "sum"


@dataclass_json
@dataclass
class FieldDefinition:
    """
    Provides a description of a field defined in the metadata
    While other fields are defined dynamically, these help "tune"
    certain fields
    * type: one of string, number, object, nested (deeper object)
    * aggregate: aggregation is available
    """

    type: str = "string"
    aggregate: bool = False


@dataclass_json
@dataclass
class MDSInstance:
    mds_url: str
    commons_url: str
    columns_to_fields: Optional[
        Union[Dict[str, str], Dict[str, ColumnsToFields]]
    ] = None
    study_data_field: str = "gen3_discovery"
    guid_type: str = "discovery_metadata"
    select_field: Optional[Dict[str, str]] = None

    def __post_init__(self):
        for name, value in self.columns_to_fields.items():
            if isinstance(value, dict):
                self.columns_to_fields[name] = ColumnsToFields.from_dict(value)


@dataclass_json
@dataclass
class AdapterMDSInstance:
    mds_url: str
    commons_url: str
    adapter: str
    config: Dict[str, Any] = field(default_factory=dict)
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
    aggregations: Optional[Dict[str, FieldAggregation]]
    fields: Optional[Dict[str, FieldDefinition]]


def parse_config(data: Dict[str, Any]) -> Commons:
    """
    parses a aggregated config which defines the list of MDS services and the mapping of field to column names
    for the Ecosystem browser. Returns a dictionary of MDSInfo entries
    """

    return Commons.from_dict(
        {
            "gen3_commons": data.get("gen3_commons", {}),
            "adapter_commons": data.get("adapter_commons", {}),
            "aggregations": data.get("aggregations", {}),
            "fields": data.get("fields", {}),
        }
    )
