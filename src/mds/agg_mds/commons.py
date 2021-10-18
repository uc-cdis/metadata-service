from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
from typing import Any, Dict, List, Optional, Union, TypeVar


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
    chart: str = "text"


FieldDefinition = TypeVar("FieldDefinition")


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
    properties: Optional[Dict[str, FieldDefinition]] = None

    ES_TYPE_MAPPING = {
        "array": "nested",
        "string": "text",
        "integer": "long",
    }

    def __post_init__(self):
        if self.properties is not None:
            self.properties = {
                k: FieldDefinition.from_dict(v) for k, v in self.properties.items()
            }

    def to_schema(self, es_types: bool = False):
        res = {
            "type": FieldDefinition.ES_TYPE_MAPPING.get(self.type, self.type)
            if es_types
            else self.type
        }
        if self.properties is not None:
            res["properties"] = {
                k: v.to_schema(True) for k, v in self.properties.items()
            }
        return res


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
        if self.columns_to_fields is None:
            return

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
class Config:
    fields: Optional[Dict[str, FieldDefinition]] = field(default_factory=dict)
    settings: Optional[Dict[str, Any]] = field(default_factory=dict)
    aggregations: Optional[Dict[str, FieldAggregation]] = field(default_factory=dict)
    search_settings: Optional[Dict[str, FieldAggregation]] = field(default_factory=dict)


@dataclass_json
@dataclass
class Commons:
    configuration: Optional[Config] = None
    gen3_commons: Dict[str, MDSInstance] = field(default_factory=dict)
    adapter_commons: Dict[str, AdapterMDSInstance] = field(default_factory=dict)
    aggregations: Optional[Dict[str, FieldAggregation]] = field(default_factory=dict)
    fields: Optional[Dict[str, FieldDefinition]] = field(default_factory=dict)


def parse_config(data: Dict[str, Any]) -> Commons:
    """
    parses a aggregated config which defines the list of MDS services and the mapping of field to column names
    for the Ecosystem browser. Returns a dictionary of MDSInfo entries
    """

    return Commons.from_json(data)
