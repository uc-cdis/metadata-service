from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
from typing import Any, Dict, List, Optional, Union, TypeVar
from mds import logger
import json


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


def string_to_array(s: str) -> Optional[List[str]]:
    if s == "":
        return []
    return [s]


def array_to_string(arr: Optional[list]) -> Optional[str]:
    if arr is None:
        return None
    return "".join(arr)


def string_to_integer(s: str) -> int:
    return int(s) if s.isnumeric() else None


def string_to_number(s: str) -> Optional[float]:
    try:
        return float(s)
    except ValueError:
        return None


def string_to_dict(s: str) -> Optional[Dict[Any, Any]]:
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return None


def dict_to_array(d: dict) -> List[Dict[Any, Any]]:
    return [d]


@dataclass_json
@dataclass
class FieldDefinition:
    """
    Provides a description of a field defined in the metadata
    While other fields are defined dynamically, these help "tune"
    certain fields
    * type: one of string, number, object, nested (deeper object)
    """

    type: str = "string"
    description: str = ""
    default: Optional[Any] = None
    properties: Optional[Dict[str, FieldDefinition]] = None
    items: Optional[Dict[str, str]] = None

    ES_TYPE_MAPPING = {
        "array": "nested",
        "object": "nested",
        "string": "keyword",
        "integer": "long",
        "number": "float",
    }

    FIELD_NORMALIZATION = {
        "string_to_number": string_to_number,
        "string_to_integer": string_to_integer,
        "string_to_object": string_to_dict,
        "object_to_array": dict_to_array,
        "string_to_array": string_to_array,
        "array_to_string": array_to_string,
    }

    MAP_TYPE_TO_JSON_SCHEMA_TYPES = {
        "str": "string",
        "int": "integer",
        "list": "array",
        "dict": "object",
    }

    def has_default_value(self):
        return self.default is not None

    def __post_init__(self):
        if self.properties is not None:
            self.properties = {
                k: FieldDefinition.from_dict(v) for k, v in self.properties.items()
            }

    def get_es_type(self):
        field_type = FieldDefinition.ES_TYPE_MAPPING.get(self.type, self.type)
        if self.type == "array" and self.items and self.items["type"] == "string":
            field_type = "keyword"

        if field_type == "keyword":
            return {
                "type": field_type,
                "fields": {
                    "analyzed": {
                        "type": "text",
                        "analyzer": "ngram_analyzer",
                        "search_analyzer": "search_analyzer",
                        "term_vector": "with_positions_offsets",
                    }
                },
            }

        return {"type": field_type}

    def to_schema(self, es_types: bool = False, all_fields: bool = False):
        """
        Maps the FieldDefinition to either a JSON schema or an Elasticsearch mapping
        """
        res = self.get_es_type() if es_types else {"type": self.type}
        if self.properties is not None:
            res["properties"] = {
                k: v.to_schema(es_types, all_fields) for k, v in self.properties.items()
            }
        if all_fields:
            if self.items is not None:
                res["items"] = self.items
            if self.description is not None:
                res["description"] = self.description
            if self.default is not None:
                res["default"] = self.default
        return res

    def normalize_value(self, value) -> Any:
        value_type = FieldDefinition.MAP_TYPE_TO_JSON_SCHEMA_TYPES.get(
            type(value).__name__, type(value).__name__
        )

        if value_type == self.type:
            return value

        conversion = f"{value_type}_to_{self.type}"
        converter = FieldDefinition.FIELD_NORMALIZATION.get(conversion, None)
        if converter is None:
            logger.warning(
                f"warning normalizing {value} via converter {conversion} not applied."
            )
            return value
        return converter(value)


@dataclass_json
@dataclass
class MDSInstance:
    """
    Handles pulling and processing data from a Gen3 metadata-service
    """

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
class Settings:
    cache_drs: bool = False
    drs_indexd_server: str = "https://dataguids.org"
    timestamp_entry: bool = False


@dataclass_json
@dataclass
class Config:
    settings: Optional[Dict[str, Settings]] = field(default_factory=dict)
    schema: Optional[Dict[str, FieldDefinition]] = field(default_factory=dict)
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

    def __post_init__(self):
        if self.configuration is None:
            self.configuration = Config(settings=Settings())


def parse_config(data: str) -> Commons:
    """
    parses an aggregated config which defines the list of MDS services and the mapping of field to column names
    for the Ecosystem browser. Returns a dictionary of MDSInfo entries
    """

    return Commons.from_json(data)
