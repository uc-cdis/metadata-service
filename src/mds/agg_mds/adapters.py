import collections.abc
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Tuple, Union, Optional
from jsonpath_ng import parse, JSONPathError
import httpx
import xmltodict
import bleach
import logging
import re
from tenacity import (
    retry,
    RetryError,
    wait_random_exponential,
    stop_after_attempt,
    retry_if_exception_type,
    before_sleep_log,
)
from mds import logger


def strip_email(text: str):
    if not isinstance(text, str):
        return text
    rgx = r"[\w.+-]+@[\w-]+\.[\w.-]+"
    matches = re.findall(rgx, text)
    for cur_word in matches:
        text = text.replace(cur_word, "")
    return text


def strip_html(s: str):
    if not isinstance(s, str):
        return s
    return bleach.clean(s, tags=[], strip=True)


def add_icpsr_source_url(study_id: str):
    if not isinstance(study_id, str):
        return study_id
    return f"https://www.icpsr.umich.edu/web/NAHDAP/studies/{study_id}"


def add_clinical_trials_source_url(study_id: str):
    if not isinstance(study_id, str):
        return study_id
    return f"https://clinicaltrials.gov/ct2/show/{study_id}"


def uppercase(s: str):
    if not isinstance(s, str):
        return s
    return s.upper()


def prepare_cidc_description(desc: str):
    desc = re.sub("\t|\n", "", desc)
    desc = re.sub("&nbsp;|&thinsp;", " ", desc)
    return desc


def aggregate_pdc_file_count(record: list):
    file_count = 0
    if record:
        for x in record:
            file_count += x["files_count"]
    return file_count


def normalize_value(value: str, mapping: Optional[Dict[str, str]] = None):
    """
    Normalizes the input value based on the given mapping.

    This function checks if the input `value` is a string, and if a `mapping`
    dictionary is provided. If both conditions are met, the function attempts
    to find the `value` in the given `mapping`. If a match is found, it returns
    the corresponding mapped value. If no match is found, or if `value` is not
    a string or no mapping is provided, it returns the original value.

    Args:
        value: str
            The input value to be normalized.
        mapping: Optional[Dict[str, str]]
            An optional dictionary that maps specific values to their desired
            normalized equivalents.

    Returns:
        str
            The normalized value if a mapping is provided and the value is found
            in the mapping; otherwise, the original input value.
    """
    return mapping.get(value, value) if isinstance(value, str) and mapping else value


def normalize_tags(
    tags: List[Dict[str, str]], mapping: Optional[Dict[str, str]] = None
):
    """
    Maps the 'name' field of dictionaries in a list based on matching 'category' using a mapping.

    Args:
        items: A list of dictionaries, each containing 'name' and 'category' keys.
        mapping: A dictionary where the key is a category and the value is another dictionary
                 mapping old names to new names.

    Returns:
        A new list of dictionaries with updated 'name' values where mappings are applied.
    """
    if not mapping:
        return tags

    updated_tags = []
    for tag in tags:
        if "name" in tag and "category" in tag:
            category = tag["category"]
            name = tag["name"]
            # Update name if category and name are in the mapping
            new_name = mapping.get(category, {}).get(name, name)
            updated_tags.append({**tag, "name": new_name})
        else:
            # If tag does not contain 'name' or 'category', keep it unchanged
            updated_tags.append(tag)

    return updated_tags


class FieldFilters:
    filters = {
        "strip_html": strip_html,
        "strip_email": strip_email,
        "add_icpsr_source_url": add_icpsr_source_url,
        "add_clinical_trials_source_url": add_clinical_trials_source_url,
        "uppercase": uppercase,
        "prepare_cidc_description": prepare_cidc_description,
        "aggregate_pdc_file_count": aggregate_pdc_file_count,
        "normalize_value": normalize_value,
        "normalize_tags": normalize_tags,
    }

    @classmethod
    def execute(cls, name, value, params=None):
        if name not in FieldFilters.filters:
            logger.warning(f"filter {name} not found: returning original value.")
            return value
        if params is not None:
            return FieldFilters.filters[name](value, params)
        return FieldFilters.filters[name](value)


def get_json_path_value(
    expression: str,
    item: dict,
    has_default_value: bool = False,
    default_value: str = "",
) -> Union[str, List[Any]]:
    """
    Given a JSON Path expression and a dictionary, using the path expression
    to find the value. If not found return and default value define return it, else
    return None
    """

    if expression is None:
        return default_value if has_default_value else None

    try:
        jsonpath_expr = parse(expression)

    except JSONPathError as exc:
        logger.error(
            f"Invalid JSON Path expression {exc} . See https://github.com/json-path/JsonPath. Returning ''"
        )
        return default_value if has_default_value else None

    v = jsonpath_expr.find(item)
    if len(v) == 0:  # nothing found, deal with this
        return default_value if has_default_value else None

    if len(v) == 1:  # convert array length 1 to a value
        return v[0].value

    return [x.value for x in v]  # join list


def flatten(dictionary, parent_key=False, separator="."):
    """
    Turn a nested dictionary into a flattened dictionary
    :param dictionary: The dictionary to flatten
    :param parent_key: The string to prepend to dictionary's keys
    :param separator: The string used to separate flattened keys
    :return: A flattened dictionary
    """

    items = []
    for key, value in dictionary.items():
        new_key = str(parent_key) + separator + key if parent_key else key
        if isinstance(value, collections.abc.MutableMapping):
            items.extend(flatten(value, False, separator).items())
        else:
            items.append((new_key, value))
    return dict(items)


class RemoteMetadataAdapter(ABC):
    """
    Abstract base class for a Metadata adapter. You must implement getRemoteDataAsJson to return a possibly empty
    dictionary and normalizeToGen3MDSField to get closer to the resources Gen3 MDS format, although this will be subject
    to change
    """

    @abstractmethod
    def getRemoteDataAsJson(self, **kwargs) -> Tuple[Dict, str]:
        """needs to be implemented in derived class"""

    @abstractmethod
    def normalizeToGen3MDSFields(self, data, **kwargs) -> Dict:
        """needs to be implemented in derived class"""

    @staticmethod
    def mapFields(item: dict, mappings: dict, global_filters=None, schema=None) -> dict:
        """
        Given a MetaData entry as a dict, and dictionary describing fields to add
        and optionally where to map an item entry from.
        The thinking is: do not remove/alter original data but add fields to "normalize" it
        for use in a Gen3 Metadata service.

        The mapping dictionary is of the form:
            field: value
        which will set the field and the default value
        There is support for JSON path syntax if the string starts with "path:"
        as in "path:OverallOfficial[0].OverallOfficialName"
        or for more complex operations:
        field: {
            path: JSON Path
            filters: [process field filters]
            default_value(optional): Any Value
        }

        :param item: dictionary to map fields to
        :param mappings: dictionary describing fields to add
        :return:
        """

        if schema is None:
            schema = {}

        if global_filters is None:
            global_filters = []

        results = {}

        for key, value in mappings.items():
            try:
                jsonpath_expr = parse(key)
            except JSONPathError as exc:
                logger.error(
                    f"Invalid JSON Path expression {exc} found as key . See https://github.com/json-path/JsonPath. Skipping this field"
                )
                continue
            key_entries_in_schema = jsonpath_expr.find(schema)

            if isinstance(value, dict):  # have a complex assignment
                expression = value.get("path", None)

                has_default_value = False
                default_value = None
                # get adapter's default value if set
                if "default" in value:
                    has_default_value = True
                    default_value = value["default"]

                # get schema default value if set
                if has_default_value is False:
                    if (
                        len(key_entries_in_schema)
                        and key_entries_in_schema[0].value.default is not None
                    ):
                        has_default_value = True
                        default_value = key_entries_in_schema[0].value.default

                field_value = get_json_path_value(
                    expression, item, has_default_value, default_value
                )

                filters = value.get("filters", [])
                filterParams = value.get("filterParams", {})
                for flt in filters:
                    if flt in filterParams:
                        field_value = FieldFilters.execute(
                            flt, field_value, filterParams[flt]
                        )
                    else:
                        field_value = FieldFilters.execute(flt, field_value)

            elif isinstance(value, str) and "path:" in value:
                # process as json path
                expression = value.split("path:")[1]

                has_default_value = False
                default_value = None
                if (
                    len(key_entries_in_schema)
                    and key_entries_in_schema[0].value.default is not None
                ):
                    has_default_value = True
                    default_value = key_entries_in_schema[0].value.default

                field_value = get_json_path_value(
                    expression, item, has_default_value, default_value
                )
            else:
                field_value = value

            for gf in global_filters:
                field_value = FieldFilters.execute(gf, field_value)
            if len(key_entries_in_schema):
                field_value = key_entries_in_schema[0].value.normalize_value(
                    field_value
                )
            # set to default if conversion failed and a default value is available
            if field_value is None:
                if has_default_value:
                    field_value = default_value
                else:
                    logger.warning(
                        f"{key} = None{', is not in the schema,' if key not in schema else ''} "
                        f"and has no default value. Consider adding {key} to the schema"
                    )
            jsonpath_expr.update_or_create(results, field_value)
        return results

    @staticmethod
    def setPerItemValues(items: dict, perItemValues: dict):
        for id, values in perItemValues.items():
            if id in items:
                for k, v in values.items():
                    if k in items[id]["gen3_discovery"]:
                        items[id]["gen3_discovery"][k] = v

    def getMetadata(self, **kwargs):
        json_data = self.getRemoteDataAsJson(**kwargs)
        return self.normalizeToGen3MDSFields(json_data, **kwargs)


class MPSAdapter(RemoteMetadataAdapter):
    """
    Simple adapter for MPS
    boiler plate taken from IPSRDublin adapter and added MPS-specific needs

    parameters: filters which currently should be study_ids=id,id,id...
    """

    @retry(
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(httpx.TimeoutException),
        wait=wait_random_exponential(multiplier=1, max=10),
    )
    def getRemoteDataAsJson(self, **kwargs) -> Tuple[Dict, str]:
        """needs to be implemented in derived class"""
        results = {"results": []}
        if "filters" not in kwargs or kwargs["filters"] is None:
            return results

        mds_url = kwargs.get("mds_url", None)
        if mds_url is None:
            return results

        study_ids = kwargs["filters"].get("study_ids", [])

        if len(study_ids) > 0:
            for id in study_ids:
                try:
                    # get url request put data into datadict
                    url = f"{mds_url}/{id}/"
                    response = httpx.get(url)
                    response.raise_for_status()

                    data_dict = response.json()
                    results["results"].append(data_dict)

                except httpx.TimeoutException as exc:
                    logger.error(
                        f"An timeout error occurred while requesting {exc.request.url}."
                    )
                    raise
                except httpx.HTTPError as exc:
                    logger.error(
                        f"An HTTP error {exc.response.status_code if exc.response is not None else ''} occurred while requesting {exc.request.url}. Skipping {id}"
                    )
                    break
                except Exception as exc:
                    logger.error(
                        f"An error occurred while requesting {mds_url} {exc}. Skipping {id}"
                    )
                    break

        return results

    @staticmethod
    def addGen3ExpectedFields(item, mappings, keepOriginalFields, globalFieldFilters):
        results = item
        if mappings is not None:
            # mapFields fxn: can use as is or can overwrite if need more specialized version
            mapped_fields = RemoteMetadataAdapter.mapFields(
                item, mappings, globalFieldFilters
            )
            if keepOriginalFields:
                results.update(mapped_fields)
            else:
                results = mapped_fields
        # add MPS specific stuff if needed (eg turning list of investigators into one string)
        return results

    def normalizeToGen3MDSFields(self, data, **kwargs) -> Dict:
        """needs to be implemented in derived class"""
        mappings = kwargs.get("mappings", None)
        keepOriginalFields = kwargs.get("keepOriginalFields", True)
        globalFieldFilters = kwargs.get("globalFieldFilters", [])

        results = {}
        for item in data["results"]:  # iterate through studies
            normalized_item = MPSAdapter.addGen3ExpectedFields(
                item, mappings, keepOriginalFields, globalFieldFilters
            )
            # TODO: is there a certain standard for identifiers or
            # is it just some pattern that ensures uniqueness?
            identifier = f"MPS_study_{item['id']}"

            results[identifier] = {
                "_guid_type": "discovery_metadata",
                "gen3_discovery": normalized_item,
            }

        perItemValues = kwargs.get("perItemValues", None)
        if perItemValues is not None:
            RemoteMetadataAdapter.setPerItemValues(results, perItemValues)

        return results


class ISCPSRDublin(RemoteMetadataAdapter):
    """
    Simple adapter for ICPSR
    parameters: filters which currently should be study_ids=id,id,id...
    """

    @retry(
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(httpx.TimeoutException),
        wait=wait_random_exponential(multiplier=1, max=10),
    )
    def getRemoteDataAsJson(self, **kwargs) -> Dict:
        results = {"results": []}
        if "filters" not in kwargs or kwargs["filters"] is None:
            return results

        mds_url = kwargs.get("mds_url", None)
        if mds_url is None:
            return results

        study_ids = kwargs["filters"].get("study_ids", [])

        if len(study_ids) > 0:
            for id in study_ids:
                try:
                    url = f"{mds_url}?verb=GetRecord&metadataPrefix=oai_dc&identifier={id}"
                    response = httpx.get(url)
                    response.raise_for_status()

                    xmlData = response.text
                    data_dict = xmltodict.parse(xmlData)
                    results["results"].append(data_dict)
                except httpx.TimeoutException as exc:
                    logger.error(
                        f"An timeout error occurred while requesting {exc.request.url}."
                    )
                    raise
                except httpx.HTTPError as exc:
                    logger.error(
                        f"An HTTP error {exc.response.status_code if exc.response is not None else ''} occurred while requesting {exc.request.url}. Skipping {id}"
                    )
                except ValueError as exc:
                    logger.error(
                        f"An error occurred while requesting {mds_url} {exc}. Skipping {id}"
                    )
                except Exception as exc:
                    logger.error(
                        f"An error occurred while requesting {mds_url} {exc}. Skipping {id}"
                    )
        logger.debug("Results: ")
        logger.debug(results)

        return results

    @staticmethod
    def buildIdentifier(id: str):
        return id.replace("http://doi.org/", "").replace("dc:", "")

    @staticmethod
    def addGen3ExpectedFields(
        item, mappings, keepOriginalFields, globalFieldFilters, schema
    ):
        results = item
        if mappings is not None:
            mapped_fields = RemoteMetadataAdapter.mapFields(
                item, mappings, globalFieldFilters, schema
            )
            if keepOriginalFields:
                results.update(mapped_fields)
            else:
                results = mapped_fields

        if isinstance(results.get("investigators"), list):
            results["investigators"] = ",".join(results["investigators"])
        if isinstance(results.get("investigators_name"), list):
            results["investigators_name"] = ",".join(results["investigators_name"])
        return results

    def normalizeToGen3MDSFields(self, data, **kwargs) -> Dict[str, Any]:
        """
        Iterates over the response from the Metadate service and extracts/maps
        required fields using the optional mapping dictionary and optionally sets
        peritem values.
        :param data:
        :return:
        """

        mappings = kwargs.get("mappings", None)
        keepOriginalFields = kwargs.get("keepOriginalFields", True)
        globalFieldFilters = kwargs.get("globalFieldFilters", [])
        schema = kwargs.get("schema", {})

        results = {}
        for record in data["results"]:
            item = {}
            for key, value in record["OAI-PMH"]["GetRecord"]["record"]["metadata"][
                "oai_dc:dc"
            ].items():
                if "dc:" in key:
                    if "dc:identifier" in key:
                        identifier = ISCPSRDublin.buildIdentifier(value[1])
                        item["identifier"] = identifier
                        item["ipcsr_study_id"] = value[0]
                    else:
                        item[str.replace(key, "dc:", "")] = value
            normalized_item = ISCPSRDublin.addGen3ExpectedFields(
                item, mappings, keepOriginalFields, globalFieldFilters, schema
            )
            results[item["identifier"]] = {
                "_guid_type": "discovery_metadata",
                "gen3_discovery": normalized_item,
            }

        perItemValues = kwargs.get("perItemValues", None)
        if perItemValues is not None:
            RemoteMetadataAdapter.setPerItemValues(results, perItemValues)

        return results


class ClinicalTrials(RemoteMetadataAdapter):
    """
    Simple adapter for ClinicalTrials API
    Expected Parameters:
        term: the search term (required)
        batchSize: number of studies to pull in a single call, default=100 and therefor optional
        maxItems: maxItems to pull, currently more of a guildline as it possible there will be more items returned
                  since the code below does not reduce the size of the results array, default = None
    """

    @retry(
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(httpx.TimeoutException),
        wait=wait_random_exponential(multiplier=1, max=10),
    )
    def getRemoteDataAsJson(self, **kwargs) -> Dict:
        results = {"results": []}

        mds_url = kwargs.get("mds_url", None)
        if mds_url is None:
            logger.debug("Clinical result with mds_url is None: ")
            logger.debug(results)
            return results

        if "filters" not in kwargs or kwargs["filters"] is None:
            logger.debug("Clinical result with filters is None: ")
            logger.debug(results)
            return results

        term = kwargs["filters"].get("term", None)

        if term == None:
            logger.debug("Clinical result with term is None: ")
            logger.debug(results)
            return results

        term = term.replace(" ", "+")

        batchSize = kwargs["filters"].get("batchSize", 100)
        maxItems = kwargs["filters"].get("maxItems", None)
        offset = 1
        remaining = 1
        limit = min(maxItems, batchSize) if maxItems is not None else batchSize

        while remaining > 0:
            try:
                response = httpx.get(
                    f"{mds_url}?expr={term}"
                    f"&fmt=json&min_rnk={offset}&max_rnk={offset + limit - 1}"
                )
                response.raise_for_status()

                data = response.json()
                if "FullStudiesResponse" not in data:
                    # something is not right with the response
                    raise ValueError("unknown response.")

                if data["FullStudiesResponse"]["NStudiesFound"] == 0:
                    # search term did not find a value, leave now
                    break

                # first time through set remaining
                if offset == 1:
                    remaining = data["FullStudiesResponse"]["NStudiesFound"]
                    # limit maxItems to the total number of items if maxItems is greater
                    if maxItems is not None:
                        maxItems = maxItems if maxItems < remaining else remaining

                numReturned = data["FullStudiesResponse"]["NStudiesReturned"]
                results["results"].extend(data["FullStudiesResponse"]["FullStudies"])
                if maxItems is not None and len(results["results"]) >= maxItems:
                    logger.debug(
                        "Clinical result with maxItems is not None and result more than maxItems: "
                    )
                    logger.debug(results)
                    return results
                remaining = remaining - numReturned
                offset += numReturned
                limit = min(remaining, batchSize)

            except httpx.TimeoutException as exc:
                logger.error(f"An timeout error occurred while requesting {mds_url}.")
                raise
            except httpx.HTTPError as exc:
                logger.error(
                    f"An HTTP error {exc.response.status_code if exc.response is not None else ''} occurred while requesting {exc.request.url}. Returning {len(results['results'])} results"
                )
                break  # need to break here as cannot be assured of leaving while loop
            except ValueError as exc:
                logger.error(
                    f"An error occurred while requesting {mds_url} {exc}. Returning {len(results['results'])} results."
                )
                break
            except Exception as exc:
                logger.error(
                    f"An error occurred while requesting {mds_url} {exc}. Returning {len(results['results'])} results."
                )
                break
        logger.debug("Clinical result: ")
        logger.debug(results)
        return results

    @staticmethod
    def addGen3ExpectedFields(
        item, mappings, keepOriginalFields, globalFieldFilters, schema
    ):
        """
        Map item fields to gen3 normalized fields
        using the mapping and adding the location
        """
        results = item
        if mappings is not None:
            mapped_fields = RemoteMetadataAdapter.mapFields(
                item, mappings, globalFieldFilters, schema
            )
            if keepOriginalFields:
                results.update(mapped_fields)
            else:
                results = mapped_fields

        location = ""
        if "Location" in item and len(item["Location"]) > 0:
            location = (
                f"{item['Location'][0].get('LocationFacility', '')}, "
                f"{item['Location'][0].get('LocationCity', '')}, "
                f"{item['Location'][0].get('LocationState', '')}"
            )
        results["location"] = location

        return results

    def normalizeToGen3MDSFields(self, data, **kwargs) -> Dict[str, Any]:
        """
        Iterates over the response.
        :param data:
        :return:
        """

        mappings = kwargs.get("mappings", None)
        keepOriginalFields = kwargs.get("keepOriginalFields", True)
        globalFieldFilters = kwargs.get("globalFieldFilters", [])
        schema = kwargs.get("schema", {})

        results = {}
        for item in data["results"]:
            item = item["Study"]
            item = flatten(item)
            normalized_item = ClinicalTrials.addGen3ExpectedFields(
                item, mappings, keepOriginalFields, globalFieldFilters, schema
            )
            results[item["NCTId"]] = {
                "_guid_type": "discovery_metadata",
                "gen3_discovery": normalized_item,
            }

        perItemValues = kwargs.get("perItemValues", None)
        if perItemValues is not None:
            RemoteMetadataAdapter.setPerItemValues(results, perItemValues)

        return results


class PDAPS(RemoteMetadataAdapter):
    """
    Simple adapter for PDAPS
    """

    @retry(
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(httpx.TimeoutException),
        wait=wait_random_exponential(multiplier=1, max=10),
    )
    def getRemoteDataAsJson(self, **kwargs) -> Dict:
        results = {"results": []}

        mds_url = kwargs.get("mds_url", None)
        if mds_url is None:
            return results

        mds_url = mds_url.rstrip("/")

        if "filters" not in kwargs or kwargs["filters"] is None:
            return results

        datasets = kwargs["filters"].get("datasets", None)

        if datasets == None:
            return results

        for id in datasets:
            try:
                response = httpx.get(
                    f"{mds_url}/siteitem/{id}/get_by_dataset?site_key=56e805b9d6c9e75c1ac8cb12"
                )
                response.raise_for_status()
                results["results"].append(response.json())

            except httpx.TimeoutException as exc:
                logger.error(f"An timeout error occurred while requesting {mds_url}.")
                raise
            except httpx.HTTPError as exc:
                logger.error(
                    f"An HTTP error {exc.response.status_code if exc.response is not None else ''} occurred while requesting {exc.request.url}. Skipping {id}"
                )

        return results

    @staticmethod
    def addGen3ExpectedFields(
        item, mappings, keepOriginalFields, globalFieldFilters, schema
    ):
        """
        Maps the items fields into Gen3 resources fields
        if keepOriginalFields is False: only those fields will be included in the final entry
        """
        results = item
        if mappings is not None:
            mapped_fields = RemoteMetadataAdapter.mapFields(
                item, mappings, globalFieldFilters, schema
            )
            if keepOriginalFields:
                results.update(mapped_fields)
            else:
                results = mapped_fields

        if isinstance(results.get("investigators"), list):
            results["investigators"] = ",".join(results["investigators"])
        if isinstance(results.get("investigators_name"), list):
            results["investigators_name"] = ",".join(results["investigators_name"])
        return results

    def normalizeToGen3MDSFields(self, data, **kwargs) -> Dict[str, Any]:
        """
        Iterates over the response.
        :param data:
        :return:
        """

        mappings = kwargs.get("mappings", None)
        keepOriginalFields = kwargs.get("keepOriginalFields", True)
        globalFieldFilters = kwargs.get("globalFieldFilters", [])
        schema = kwargs.get("schema", {})

        results = {}
        for item in data["results"]:
            # some PDAPS studies doesn't have "display_id" but only "id"
            # but we need "display_id" to populate "project_number" in MDS
            if "id" in item:
                item["display_id"] = item["id"]
            normalized_item = PDAPS.addGen3ExpectedFields(
                item, mappings, keepOriginalFields, globalFieldFilters, schema
            )
            if "display_id" in item:
                results[item["display_id"]] = {
                    "_guid_type": "discovery_metadata",
                    "gen3_discovery": normalized_item,
                }
            else:
                continue

        perItemValues = kwargs.get("perItemValues", None)
        if perItemValues is not None:
            RemoteMetadataAdapter.setPerItemValues(results, perItemValues)

        return results


class HarvardDataverse(RemoteMetadataAdapter):
    """
    Adapter class for Harvard Dataverse
    """

    @retry(
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(httpx.TimeoutException),
        wait=wait_random_exponential(multiplier=1, max=10),
    )
    def getRemoteDataAsJson(self, **kwargs) -> Tuple[Dict, str]:
        results = {"results": []}

        mds_url = kwargs.get("mds_url")
        if mds_url is None:
            return results

        if "filters" not in kwargs or kwargs.get("filters") is None:
            return results

        persistent_ids = kwargs["filters"].get("persistent_ids", [])

        for persistent_id in persistent_ids:
            try:
                dataset_url = (
                    f"{mds_url}/datasets/:persistentId/?persistentId={persistent_id}"
                )
                response = httpx.get(dataset_url)
                response.raise_for_status()

                data = response.json()
                if "data" not in data:
                    raise ValueError("unknown response")

                dataset_results = data["data"]["latestVersion"]
                dataset_output = {
                    "id": persistent_id,
                    "url": data["data"]["persistentUrl"],
                    "data_availability": "available"
                    if dataset_results["versionState"] == "RELEASED"
                    else "pending",
                }
                citation_fields = (
                    dataset_results.get("metadataBlocks", {})
                    .get("citation", {})
                    .get("fields", [])
                )
                for field in citation_fields:
                    if field["typeClass"] != "compound":
                        dataset_output[field["typeName"]] = field["value"]
                    else:
                        for entry in field["value"]:
                            for subfield, subfield_info in entry.items():
                                if subfield in dataset_output:
                                    dataset_output[subfield].append(
                                        subfield_info["value"]
                                    )
                                else:
                                    dataset_output[subfield] = [subfield_info["value"]]

                dataset_output["files"] = []
                dataset_files = dataset_results.get("files", [])
                for file in dataset_files:
                    data_file = file["dataFile"]
                    data_file["data_dictionary"] = []

                    ddi_url = (
                        f"{mds_url}/access/datafile/{data_file['id']}/metadata/ddi"
                    )
                    ddi_response = httpx.get(ddi_url)
                    if ddi_response.status_code == 200:
                        ddi_entry = xmltodict.parse(ddi_response.text)
                        vars = (
                            ddi_entry.get("codeBook", {})
                            .get("dataDscr", {})
                            .get("var", [])
                        )
                        if isinstance(vars, dict):
                            vars = [vars]
                        for var_iter, var in enumerate(vars):
                            data_file["data_dictionary"].append(
                                {
                                    "name": var.get("@name", f"var{var_iter + 1}"),
                                    "label": var.get("labl", {}).get("#text"),
                                    "interval": var.get("@intrvl"),
                                    "type": var.get("varFormat", {}).get("@type"),
                                }
                            )

                    dataset_output["files"].append(data_file)

                results["results"].append(dataset_output)

            except httpx.TimeoutException as exc:
                logger.error(f"An timeout error occurred while requesting {mds_url}.")
                raise
            except httpx.HTTPError as exc:
                logger.error(
                    f"An HTTP error {exc.response.status_code if exc.response is not None else ''} occurred while requesting {exc.request.url}. Returning {len(results['results'])} results"
                )
                break  # need to break here as cannot be assured of leaving while loop
            except ValueError as exc:
                logger.error(
                    f"An error occurred while requesting {mds_url} {exc}. Returning {len(results['results'])} results."
                )
                break
            except Exception as exc:
                logger.error(
                    f"An error occurred while requesting {mds_url} {exc}. Returning {len(results['results'])} results."
                )
                break

        return results

    @staticmethod
    def addGen3ExpectedFields(item, mappings, keepOriginalFields, globalFieldFilters):
        results = item
        files = results.pop("files", [])
        if mappings is not None:
            mapped_fields = RemoteMetadataAdapter.mapFields(
                item, mappings, globalFieldFilters
            )
            if keepOriginalFields:
                results.update(mapped_fields)
            else:
                results = mapped_fields

        results["data_dictionary"] = {}
        for i, file in enumerate(files):
            results["data_dictionary"][file["filename"]] = file["data_dictionary"]

        for field in ["summary", "study_description_summary"]:
            if isinstance(results[field], list):
                results[field] = " ".join(results[field])

        for field in [
            "subjects",
            "investigators",
            "investigators_name",
            "institutions",
        ]:
            if isinstance(results[field], list):
                results[field] = ", ".join(results[field])

        return results

    def normalizeToGen3MDSFields(self, data, **kwargs) -> Dict:
        mappings = kwargs.get("mappings", None)
        keepOriginalFields = kwargs.get("keepOriginalFields", True)
        globalFieldFilters = kwargs.get("globalFieldFilters", [])

        results = {}
        for item in data["results"]:
            normalized_item = self.addGen3ExpectedFields(
                item, mappings, keepOriginalFields, globalFieldFilters
            )
            # TODO: Confirm the appropriate ID to use for each item
            results[item["id"]] = {
                "_guid_type": "discovery_metadata",
                "gen3_discovery": normalized_item,
            }

        perItemValues = kwargs.get("perItemValues", None)
        if perItemValues is not None:
            self.setPerItemValues(results, perItemValues)

        return results


class Gen3Adapter(RemoteMetadataAdapter):
    """
    Simple adapter for Gen3 Metadata Service
    """

    @retry(
        stop=stop_after_attempt(10),
        retry=retry_if_exception_type(httpx.TimeoutException),
        wait=wait_random_exponential(multiplier=1, max=20),
        before_sleep=before_sleep_log(logger, logging.DEBUG),
    )
    def getRemoteDataAsJson(self, **kwargs) -> Dict:
        results = {"results": {}}

        mds_url = kwargs.get("mds_url", None)
        if mds_url is None:
            return results

        if mds_url[-1] != "/":
            mds_url += "/"

        config = kwargs.get("config", {})
        guid_type = config.get("guid_type", "discovery_metadata")
        field_name = config.get("field_name", None)
        field_value = config.get("field_value", None)
        filters = config.get("filters", None)
        batchSize = config.get("batchSize", 1000)
        maxItems = config.get("maxItems", None)

        offset = 0
        limit = min(maxItems, batchSize) if maxItems is not None else batchSize
        moreData = True
        # extend httpx timeout
        # timeout = httpx.Timeout(connect=60, read=120, write=5, pool=60)
        while moreData:
            try:
                url = f"{mds_url}mds/metadata?data=True&_guid_type={guid_type}&limit={limit}&offset={offset}"
                if filters:
                    url += f"&{filters}"
                if field_name is not None and field_value is not None:
                    url += f"&{guid_type}.{field_name}={field_value}"
                response = httpx.get(url, timeout=60)
                response.raise_for_status()

                data = response.json()
                results["results"].update(data)
                numReturned = len(data)

                if numReturned == 0 or numReturned <= limit:
                    moreData = False
                offset += numReturned

            except httpx.TimeoutException:
                logger.error(f"An timeout error occurred while requesting {url}.")
                raise
            except httpx.HTTPError as exc:
                logger.error(
                    f"An HTTP error {exc if exc is not None else ''} occurred while requesting {exc.request.url}. Returning {len(results['results'])} results."
                )
                break

        return results

    @staticmethod
    def addGen3ExpectedFields(
        item, mappings, keepOriginalFields, globalFieldFilters, schema
    ) -> Dict[str, Any]:
        """
        Given an item (metadata as a dict), map the item's keys into
        the fields defined in mappings. If the original fields should
        be preserved set keepOriginalFields to setPerItemValues.
        * item: metadata to map fields from -> to
        * mapping: dict to map field_name (possible a JSON Path) to a  normalize_name
        * keepOriginalFields: if True keep all data in the item, if False only those fields in mappings
                              will be in the returned item
        * globalFieldFilters: filters to apply to all fields
        """
        results = item
        if mappings is not None:
            mapped_fields = RemoteMetadataAdapter.mapFields(
                item, mappings, globalFieldFilters, schema
            )
            if keepOriginalFields:
                results.update(mapped_fields)
            else:
                results = mapped_fields

        return results

    def normalizeToGen3MDSFields(self, data, **kwargs) -> Dict[str, Any]:
        """
        Iterates over the response.
        * data: input metadata to normalize
        * kwargs: key value parameters passed as a dict
        return: dict of results where the keys are identifiers in the Gen3 metadata format:
                "GUID" : {
                    "_guid_type": "discovery_metadata",
                    "gen3_discovery": normalize_item_metadata
                    }
        """

        mappings = kwargs.get("mappings", None)
        config = kwargs.get("config", {})
        study_field = config.get("study_field", "gen3_discovery")
        data_dict_field = config.get("data_dict_field", None)
        keepOriginalFields = kwargs.get("keepOriginalFields", True)
        globalFieldFilters = kwargs.get("globalFieldFilters", [])
        schema = kwargs.get("schema", {})

        results = {}
        for guid, record in data["results"].items():
            if study_field not in record:
                logger.error("Study field not in record. Skipping")
                continue
            item = Gen3Adapter.addGen3ExpectedFields(
                record[study_field],
                mappings,
                keepOriginalFields,
                globalFieldFilters,
                schema,
            )
            results[guid] = {
                "_guid_type": "discovery_metadata",
                "gen3_discovery": item,
            }
            # for VLMD, bring it into AggMDS records
            if data_dict_field is not None and data_dict_field in record:
                results[guid][data_dict_field] = record[data_dict_field]

        perItemValues = kwargs.get("perItemValues", None)
        if perItemValues is not None:
            RemoteMetadataAdapter.setPerItemValues(results, perItemValues)

        return results


class DRSIndexdAdapter(RemoteMetadataAdapter):
    """
    Pulls the DRS hostname from a ga4gh (indexd) server to cache
    them to support local compact DRS resolution.
    """

    @staticmethod
    def clean_dist_entry(s: str) -> str:
        """
        Cleans the string returning a proper DRS prefix
        @param s: string to clean
        @return: cleaned string
        """
        return s.replace("\\.", ".").replace(".*", "")

    @staticmethod
    def clean_http_url(s: str) -> str:
        """
        Cleans input string removing http(s) prefix and all trailing paths
        @param s: string to clean
        @return: cleaned string
        """
        return (
            s.replace("/index", "")[::-1]
            .replace("/", "", 1)[::-1]
            .replace("http://", "")
            .replace("https://", "")
            .replace("/ga4gh/drs/v1/objects", "")
        )

    def getRemoteDataAsJson(self, **kwargs) -> Dict:
        from datetime import datetime, timezone

        results = {"results": {}}

        mds_url = kwargs.get("mds_url", None)
        if mds_url is None:
            return results

        try:
            response = httpx.get(f"{mds_url}/index/_dist")
            response.raise_for_status()
            data = response.json()
            # process the entries and create a DRS cache
            results = {
                "info": {
                    "created": datetime.now(timezone.utc).strftime(
                        "%m/%d/%Y %H:%M:%S:%Z"
                    )
                },
                "cache": {},
            }
            for entry in data:
                if entry["type"] != "indexd":
                    continue
                host = DRSIndexdAdapter.clean_http_url(entry["host"])
                name = entry.get("name", "")
                for x in entry["hints"]:
                    drs_prefix = DRSIndexdAdapter.clean_dist_entry(x)
                    results["cache"][drs_prefix] = {
                        "host": host,
                        "name": name,
                        "type": entry["type"],
                    }

        except httpx.HTTPError as exc:
            logger.error(
                f"An HTTP error {exc.response.status_code if exc.response is not None else ''} occurred while requesting {exc.request.url}."
            )

        return results

    def normalizeToGen3MDSFields(self, data, **kwargs) -> Dict[str, Any]:
        return data


class ICDCAdapter(RemoteMetadataAdapter):
    """
    Simple adapter for Integrated Canine Data Commons
    """

    @retry(
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(httpx.TimeoutException),
        wait=wait_random_exponential(multiplier=1, max=10),
    )
    def getRemoteDataAsJson(self, **kwargs) -> Dict:
        results = {"results": []}

        mds_url = kwargs.get("mds_url", None)
        if mds_url is None:
            return results

        queryObj = {
            "variables": {},
            "query": "{\n  studiesByProgram {\n    program_id\n    clinical_study_designation\n    clinical_study_name\n    clinical_study_type\n    numberOfCases\n    numberOfCaseFiles\n    numberOfStudyFiles\n    numberOfImageCollections\n    numberOfPublications\n    accession_id\n    study_disposition\n    numberOfCRDCNodes\n    CRDCLinks {\n      text\n      url\n      __typename\n    }\n    __typename\n  }\n}\n",
        }
        try:
            response = httpx.post(mds_url, json=queryObj)
            response.raise_for_status()
            results["results"].append(response.json())

        except httpx.TimeoutException as exc:
            logger.error(f"An timeout error occurred while requesting {mds_url}.")
            raise
        except httpx.HTTPError as exc:
            logger.error(
                f"An HTTP error {exc.response.status_code if exc.response is not None else ''} occurred while requesting {exc.request.url}."
            )

        return results

    @staticmethod
    def addGen3ExpectedFields(
        item, mappings, keepOriginalFields, globalFieldFilters, schema
    ):
        """
        Map item fields to gen3 normalized fields
        using the mapping and adding the location
        """
        results = item
        if mappings is not None:
            mapped_fields = RemoteMetadataAdapter.mapFields(
                item, mappings, globalFieldFilters, schema
            )
            if keepOriginalFields:
                results.update(mapped_fields)
            else:
                results = mapped_fields

        return results

    def normalizeToGen3MDSFields(self, data, **kwargs) -> Dict[str, Any]:
        """
        Iterates over the response.
        :param data:
        :return:
        """

        mappings = kwargs.get("mappings", None)
        keepOriginalFields = kwargs.get("keepOriginalFields", True)
        globalFieldFilters = kwargs.get("globalFieldFilters", [])
        schema = kwargs.get("schema", {})

        results = {}
        for item in data["results"][0]["data"]["studiesByProgram"]:
            item = flatten(item)
            normalized_item = ICDCAdapter.addGen3ExpectedFields(
                item, mappings, keepOriginalFields, globalFieldFilters, schema
            )
            results[item["clinical_study_designation"]] = {
                "_guid_type": "discovery_metadata",
                "gen3_discovery": normalized_item,
            }

        perItemValues = kwargs.get("perItemValues", None)
        if perItemValues is not None:
            RemoteMetadataAdapter.setPerItemValues(results, perItemValues)

        return results


class GDCAdapter(RemoteMetadataAdapter):
    """
    Simple adapter for Genomic Data Commons
    Expected Parameters:
        size: number of studies to pull in a single call, default=1000 and therefore optional
    """

    @retry(
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(httpx.TimeoutException),
        wait=wait_random_exponential(multiplier=1, max=10),
    )
    def getRemoteDataAsJson(self, **kwargs) -> Dict:
        results = {"results": []}

        mds_url = kwargs.get("mds_url", None)
        if mds_url is None:
            return results

        batchSize = kwargs["filters"].get("size", 1000)
        offset = 0
        remaining = True
        data = []

        while remaining:
            try:
                response = httpx.get(
                    f"{mds_url}?expand=summary&from={offset}&size={batchSize}"
                )
                response.raise_for_status()

                response_data = response.json()
                data = response_data["data"]["hits"]
                results["results"] += data

                remaining = len(data) == batchSize
                offset += batchSize

            except httpx.TimeoutException as exc:
                logger.error(f"An timeout error occurred while requesting {mds_url}.")
                raise
            except httpx.HTTPError as exc:
                logger.error(
                    f"An HTTP error {exc.response.status_code if exc.response is not None else ''} occurred while requesting {exc.request.url}. Returning {len(results['results'])} results"
                )
                break
            except Exception as exc:
                logger.error(
                    f"An error occurred while requesting {mds_url} {exc}. Returning {len(results['results'])} results."
                )
                break

        return results

    @staticmethod
    def addGen3ExpectedFields(
        item, mappings, keepOriginalFields, globalFieldFilters, schema
    ):
        """
        Map item fields to gen3 normalized fields
        using the mapping and adding the location
        """
        results = item
        if mappings is not None:
            mapped_fields = RemoteMetadataAdapter.mapFields(
                item, mappings, globalFieldFilters, schema
            )
            if keepOriginalFields:
                results.update(mapped_fields)
            else:
                results = mapped_fields

        return results

    def normalizeToGen3MDSFields(self, data, **kwargs) -> Dict[str, Any]:
        """
        Iterates over the response.
        :param data:
        :return:
        """
        mappings = kwargs.get("mappings", None)
        keepOriginalFields = kwargs.get("keepOriginalFields", False)
        globalFieldFilters = kwargs.get("globalFieldFilters", [])
        schema = kwargs.get("schema", {})

        results = {}
        for item in data["results"]:
            normalized_item = GDCAdapter.addGen3ExpectedFields(
                item,
                mappings,
                keepOriginalFields,
                globalFieldFilters,
                schema,
            )
            normalized_item[
                "description"
            ] = f"Genomic Data Commons study of {normalized_item['disease_type']} in {normalized_item['primary_site']}"

            normalized_item["tags"] = [
                {
                    "name": normalized_item[tag][0] if normalized_item[tag] else "",
                    "category": tag,
                }
                for tag in ["disease_type", "primary_site"]
            ]

            results[normalized_item["_unique_id"]] = {
                "_guid_type": "discovery_metadata",
                "gen3_discovery": normalized_item,
            }

        return results


class CIDCAdapter(RemoteMetadataAdapter):
    """
    Simple adapter for Cancer Imaging Data Commons
    """

    @retry(
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(httpx.TimeoutException),
        wait=wait_random_exponential(multiplier=1, max=10),
    )
    def getRemoteDataAsJson(self, **kwargs) -> Dict:
        results = {"results": []}

        mds_url = kwargs.get("mds_url", None)
        if mds_url is None:
            return results

        data = []

        try:
            response = httpx.get(mds_url)
            response.raise_for_status()

            response_data = response.json()
            data = response_data["collections"]
            results["results"] = data

        except httpx.TimeoutException as exc:
            logger.error(f"An timeout error occurred while requesting {mds_url}.")
            raise
        except httpx.HTTPStatusError as exc:
            logger.error(
                f"An HTTP error {exc.response.status_code if exc.response is not None else ''} occurred while requesting {exc.request.url}. Returning {len(results['results'])} results"
            )
        except Exception as exc:
            logger.error(
                f"An error occurred while requesting {mds_url} {exc}. Returning {len(results['results'])} results."
            )

        return results

    @staticmethod
    def addGen3ExpectedFields(
        item, mappings, keepOriginalFields, globalFieldFilters, schema
    ):
        """
        Map item fields to gen3 normalized fields
        using the mapping and adding the location
        """
        results = item
        if mappings is not None:
            mapped_fields = RemoteMetadataAdapter.mapFields(
                item, mappings, globalFieldFilters, schema
            )
            if keepOriginalFields:
                results.update(mapped_fields)
            else:
                results = mapped_fields

        return results

    def normalizeToGen3MDSFields(self, data, **kwargs) -> Dict[str, Any]:
        """
        Iterates over the response.
        :param data:
        :return:
        """
        mappings = kwargs.get("mappings", None)
        keepOriginalFields = kwargs.get("keepOriginalFields", False)
        globalFieldFilters = kwargs.get("globalFieldFilters", [])
        schema = kwargs.get("schema", {})

        results = {}
        for item in data["results"]:
            normalized_item = CIDCAdapter.addGen3ExpectedFields(
                item,
                mappings,
                keepOriginalFields,
                globalFieldFilters,
                schema,
            )
            normalized_item["tags"] = [
                {
                    "name": normalized_item[tag] if normalized_item[tag] else "",
                    "category": tag,
                }
                for tag in ["disease_type", "data_type", "primary_site"]
            ]

            results[normalized_item["_unique_id"]] = {
                "_guid_type": "discovery_metadata",
                "gen3_discovery": normalized_item,
            }

        return results


class PDCAdapter(RemoteMetadataAdapter):
    """
    Simple adapter for Proteomic Data Commons
    Expected Parameters:
        size:   Number of studies to pull in a single call, default=5 and therefore optional
        #Note - The API doesn't seem to do very well with large requests,
                hence confining it to a smaller number
    """

    @retry(
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(httpx.TimeoutException),
        wait=wait_random_exponential(multiplier=1, max=10),
    )
    def getRemoteDataAsJson(self, **kwargs) -> Dict:
        results = {"results": []}

        mds_url = kwargs.get("mds_url", None)
        if mds_url is None:
            return results
        batchSize = kwargs["filters"].get("size", 5)

        subject_catalog_query = "{studyCatalog(acceptDUA: true){pdc_study_id}}"
        try:
            response = httpx.post(mds_url, json={"query": subject_catalog_query})
            response.raise_for_status()
            response_data = response.json()
            pid_list = [
                record["pdc_study_id"]
                for record in response_data["data"]["studyCatalog"]
            ]
            total = len(pid_list)

            record_list = {}
            for i in range(0, total, batchSize):
                subject_query_string = (
                    "{"
                    + " ".join(
                        [
                            (
                                f"{study_id} : "
                                f'  study (pdc_study_id: "{study_id}" acceptDUA: true) {{'
                                "    submitter_id_name"
                                "    study_id"
                                "    study_name"
                                "    study_shortname"
                                "    analytical_fraction"
                                "    experiment_type"
                                "    embargo_date"
                                "    acquisition_type"
                                "    cases_count"
                                "    filesCount {"
                                "      files_count"
                                "    }"
                                "    disease_type"
                                "    analytical_fraction"
                                "    program_name"
                                "    program_id"
                                "    project_name"
                                "    project_id"
                                "    project_submitter_id"
                                "    primary_site"
                                "    pdc_study_id"
                                "  }"
                            )
                            for study_id in pid_list[i : i + batchSize]
                        ]
                    )
                    + "}"
                )
                response = httpx.post(
                    mds_url, json={"query": subject_query_string}, timeout=60
                )
                response.raise_for_status()
                record_list.update(response.json()["data"])
                logger.info(
                    f"Fetched ({min(i+batchSize,total)}/{total} records from PDC)"
                )
            results["results"] = [value[0] for _, value in record_list.items()]
        except httpx.TimeoutException as exc:
            logger.error(f"An timeout error occurred while requesting {mds_url}.")
            raise
        except httpx.HTTPError as exc:
            logger.error(
                f"An HTTP error occurred while requesting {mds_url} {exc}. Returning {len(results['results'])} results."
            )
        except Exception as exc:
            logger.error(
                f"An error occurred while requesting {mds_url} {exc}. Returning {len(results['results'])} results."
            )
        return results

    @staticmethod
    def addGen3ExpectedFields(
        item, mappings, keepOriginalFields, globalFieldFilters, schema
    ):
        """
        Map item fields to gen3 normalized fields
        using the mapping and adding the location
        """
        results = item
        if mappings is not None:
            mapped_fields = RemoteMetadataAdapter.mapFields(
                item, mappings, globalFieldFilters, schema
            )
            if keepOriginalFields:
                results.update(mapped_fields)
            else:
                results = mapped_fields

        return results

    def normalizeToGen3MDSFields(self, data, **kwargs) -> Dict[str, Any]:
        """
        Iterates over the response.
        :param data:
        :return:
        """
        mappings = kwargs.get("mappings", None)
        keepOriginalFields = kwargs.get("keepOriginalFields", False)
        globalFieldFilters = kwargs.get("globalFieldFilters", [])
        schema = kwargs.get("schema", {})

        results = {}
        for item in data["results"]:
            normalized_item = PDCAdapter.addGen3ExpectedFields(
                item,
                mappings,
                keepOriginalFields,
                globalFieldFilters,
                schema,
            )
            normalized_item["tags"] = [
                {
                    "name": normalized_item[tag] if normalized_item[tag] else "",
                    "category": tag,
                }
                for tag in ["disease_type", "primary_site"]
            ]
            results[normalized_item["_unique_id"]] = {
                "_guid_type": "discovery_metadata",
                "gen3_discovery": normalized_item,
            }

        return results


class PDCSubjectAdapter(RemoteMetadataAdapter):
    """
    Simple adapter for Proteomic Data Commons
    Expected Parameters:
        size:   Number of studies to pull in a single call, default=5 and therefore optional
        #Note - The API doesn't seem to do very well with large requests,
                hence confining it to a smaller number
    """

    @retry(
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(httpx.TimeoutException),
        wait=wait_random_exponential(multiplier=1, max=10),
    )
    def getRemoteDataAsJson(self, **kwargs) -> Dict:
        results = {"results": []}

        mds_url = kwargs.get("mds_url", None)
        if mds_url is None:
            return results
        batchSize = kwargs["filters"].get("size", 5)

        query = """
query FilteredClinicalDataPaginated($offset_value: Int, $limit_value: Int, $sort_value: String, $program_name_filter: String!, $project_name_filter: String!, $study_name_filter: String!, $disease_filter: String!, $filterValue: String!, $analytical_frac_filter: String!, $exp_type_filter: String!, $ethnicity_filter: String!, $race_filter: String!, $gender_filter: String!, $tumor_grade_filter: String!, $sample_type_filter: String!, $acquisition_type_filter: String!, $data_category_filter: String!, $file_type_filter: String!, $access_filter: String!, $downloadable_filter: String!, $case_status_filter: String!, $biospecimen_status_filter: String!, $getAll: Boolean!) {
  getPaginatedUIClinical(offset: $offset_value, limit: $limit_value, sort: $sort_value, program_name: $program_name_filter, project_name: $project_name_filter, study_name: $study_name_filter, disease_type: $disease_filter, primary_site: $filterValue, analytical_fraction: $analytical_frac_filter, experiment_type: $exp_type_filter, ethnicity: $ethnicity_filter, race: $race_filter, gender: $gender_filter, tumor_grade: $tumor_grade_filter, sample_type: $sample_type_filter, acquisition_type: $acquisition_type_filter, data_category: $data_category_filter, file_type: $file_type_filter, access: $access_filter, downloadable: $downloadable_filter, case_status: $case_status_filter, biospecimen_status: $biospecimen_status_filter, getAll: $getAll) {
    total
    uiClinical {
      case_submitter_id
      external_case_id
      ethnicity
      gender
      race
      morphology
      primary_diagnosis
      site_of_resection_or_biopsy
      tissue_or_organ_of_origin
      tumor_grade
      tumor_stage
      age_at_diagnosis
      classification_of_tumor
      days_to_recurrence
      case_id
      disease_type
      primary_site
      program_name
      project_name
      status
      cause_of_death
      days_to_birth
      days_to_death
      vital_status
      year_of_birth
      year_of_death
      age_at_index
      premature_at_birth
      weeks_gestation_at_birth
      age_is_obfuscated
      cause_of_death_source
      occupation_duration_years
      country_of_residence_at_enrollment
      days_to_last_follow_up
      days_to_last_known_disease_status
      last_known_disease_status
      progression_or_recurrence
      prior_malignancy
      ajcc_clinical_m
      ajcc_clinical_n
      ajcc_clinical_stage
      ajcc_clinical_t
      ajcc_pathologic_m
      ajcc_pathologic_n
      ajcc_pathologic_stage
      ajcc_pathologic_t
      ajcc_staging_system_edition
      ann_arbor_b_symptoms
      ann_arbor_clinical_stage
      ann_arbor_extranodal_involvement
      ann_arbor_pathologic_stage
      best_overall_response
      burkitt_lymphoma_clinical_variant
      circumferential_resection_margin
      colon_polyps_history
      days_to_best_overall_response
      days_to_diagnosis
      days_to_hiv_diagnosis
      days_to_new_event
      figo_stage
      hiv_positive
      hpv_positive_type
      hpv_status
      iss_stage
      laterality
      ldh_level_at_diagnosis
      ldh_normal_range_upper
      lymph_nodes_positive
      lymphatic_invasion_present
      method_of_diagnosis
      peripancreatic_lymph_nodes_positive
      peripancreatic_lymph_nodes_tested
      supratentorial_localization
      tumor_confined_to_organ_of_origin
      tumor_focality
      tumor_regression_grade
      vascular_invasion_type
      wilms_tumor_histologic_subtype
      breslow_thickness
      gleason_grade_group
      igcccg_stage
      international_prognostic_index
      largest_extrapelvic_peritoneal_focus
      masaoka_stage
      new_event_anatomic_site
      new_event_type
      overall_survival
      perineural_invasion_present
      prior_treatment
      progression_free_survival
      progression_free_survival_event
      residual_disease
      vascular_invasion_present
      year_of_diagnosis
      icd_10_code
      synchronous_malignancy
      metastasis_at_diagnosis
      metastasis_at_diagnosis_site
      mitosis_karyorrhexis_index
      non_nodal_regional_disease
      non_nodal_tumor_deposits
      ovarian_specimen_status
      ovarian_surface_involvement
      percent_tumor_invasion
      peritoneal_fluid_cytological_status
      primary_gleason_grade
      secondary_gleason_grade
      weiss_assessment_score
      adrenal_hormone
      ann_arbor_b_symptoms_described
      diagnosis_is_primary_disease
      eln_risk_classification
      figo_staging_edition_year
      gleason_grade_tertiary
      gleason_patterns_percent
      margin_distance
      margins_involved_site
      pregnant_at_diagnosis
      satellite_nodule_present
      sites_of_involvement
      tumor_depth
      who_cns_grade
      who_nte_grade
      diagnosis_uuid
      anaplasia_present
      anaplasia_present_type
      child_pugh_classification
      cog_liver_stage
      cog_neuroblastoma_risk_group
      cog_renal_stage
      cog_rhabdomyosarcoma_risk_group
      enneking_msts_grade
      enneking_msts_metastasis
      enneking_msts_stage
      enneking_msts_tumor_site
      esophageal_columnar_dysplasia_degree
      esophageal_columnar_metaplasia_present
      first_symptom_prior_to_diagnosis
      gastric_esophageal_junction_involvement
      goblet_cells_columnar_mucosa_present
      gross_tumor_weight
      inpc_grade
      inpc_histologic_group
      inrg_stage
      inss_stage
      irs_group
      irs_stage
      ishak_fibrosis_score
      lymph_nodes_tested
      medulloblastoma_molecular_classification
      externalReferences {
        reference_resource_shortname
        reference_entity_location
        __typename
      }
      exposures {
        exposure_id
        exposure_submitter_id
        alcohol_days_per_week
        alcohol_drinks_per_day
        alcohol_history
        alcohol_intensity
        asbestos_exposure
        cigarettes_per_day
        coal_dust_exposure
        environmental_tobacco_smoke_exposure
        pack_years_smoked
        radon_exposure
        respirable_crystalline_silica_exposure
        smoking_frequency
        time_between_waking_and_first_smoke
        tobacco_smoking_onset_year
        tobacco_smoking_quit_year
        tobacco_smoking_status
        type_of_smoke_exposure
        type_of_tobacco_used
        years_smoked
        age_at_onset
        alcohol_type
        exposure_duration
        exposure_duration_years
        exposure_type
        marijuana_use_per_week
        parent_with_radiation_exposure
        secondhand_smoke_as_child
        smokeless_tobacco_quit_age
        tobacco_use_per_day
        __typename
      }
      follow_ups {
        follow_up_id
        follow_up_submitter_id
        adverse_event
        adverse_event_grade
        aids_risk_factors
        barretts_esophagus_goblet_cells_present
        bmi
        body_surface_area
        cause_of_response
        cd4_count
        cdc_hiv_risk_factors
        comorbidity
        comorbidity_method_of_diagnosis
        days_to_adverse_event
        days_to_comorbidity
        days_to_follow_up
        days_to_imaging
        days_to_progression
        days_to_progression_free
        days_to_recurrence
        diabetes_treatment_type
        disease_response
        dlco_ref_predictive_percent
        ecog_performance_status
        evidence_of_recurrence_type
        eye_color
        fev1_ref_post_bronch_percent
        fev1_ref_pre_bronch_percent
        fev1_fvc_pre_bronch_percent
        fev1_fvc_post_bronch_percent
        haart_treatment_indicator
        height
        hepatitis_sustained_virological_response
        history_of_tumor
        history_of_tumor_type
        hiv_viral_load
        hormonal_contraceptive_type
        hormonal_contraceptive_use
        hormone_replacement_therapy_type
        hpv_positive_type
        hysterectomy_margins_involved
        hysterectomy_type
        imaging_result
        imaging_type
        immunosuppressive_treatment_type
        karnofsky_performance_status
        menopause_status
        nadir_cd4_count
        pancreatitis_onset_year
        pregnancy_outcome
        procedures_performed
        progression_or_recurrence
        progression_or_recurrence_anatomic_site
        progression_or_recurrence_type
        recist_targeted_regions_number
        recist_targeted_regions_sum
        reflux_treatment_type
        risk_factor
        risk_factor_treatment
        scan_tracer_used
        undescended_testis_corrected
        undescended_testis_corrected_age
        undescended_testis_corrected_laterality
        undescended_testis_corrected_method
        undescended_testis_history
        undescended_testis_history_laterality
        viral_hepatitis_serologies
        weight
        __typename
      }
      treatments {
        treatment_id
        treatment_submitter_id
        days_to_treatment_end
        days_to_treatment_start
        initial_disease_status
        regimen_or_line_of_therapy
        therapeutic_agents
        treatment_anatomic_site
        treatment_effect
        treatment_intent_type
        treatment_or_therapy
        treatment_outcome
        treatment_type
        chemo_concurrent_to_radiation
        number_of_cycles
        reason_treatment_ended
        route_of_administration
        treatment_arm
        treatment_dose
        treatment_dose_units
        treatment_effect_indicator
        treatment_frequency
        __typename
      }
      samples {
        sample_id
        sample_submitter_id
        annotation
        __typename
      }
      __typename
    }
    pagination {
      count
      sort
      from
      page
      total
      pages
      size
      __typename
    }
    __typename
  }
}
                """

        variables = {
            "offset_value": 0,
            "limit_value": 250,
            "sort_value": "",
            "program_name_filter": "Applied Proteogenomics OrganizationaL Learning and Outcomes - APOLLO",
            "project_name_filter": "",
            "study_name_filter": "",
            "disease_filter": "",
            "filterValue": "",
            "analytical_frac_filter": "",
            "exp_type_filter": "",
            "ethnicity_filter": "",
            "race_filter": "",
            "gender_filter": "",
            "tumor_grade_filter": "",
            "sample_type_filter": "",
            "acquisition_type_filter": "",
            "data_category_filter": "",
            "file_type_filter": "",
            "access_filter": "",
            "downloadable_filter": "",
            "case_status_filter": "",
            "biospecimen_status_filter": "",
            "getAll": False,
        }

        try:
            response = httpx.post(
                mds_url, json={"query": query, "variables": variables}
            )
            response.raise_for_status()
            response_data = response.json()
            results["results"] = response_data["data"]["getPaginatedUIClinical"][
                "uiClinical"
            ]
            logger.info(
                f"Fetched {response_data['data']['getPaginatedUICase']['total']} records from PDC)"
            )

        except httpx.TimeoutException as exc:
            logger.error(f"An timeout error occurred while requesting {mds_url}.")
            raise
        except httpx.HTTPError as exc:
            logger.error(
                f"An HTTP error occurred while requesting {mds_url} {exc}. Returning {len(results['results'])} results."
            )
        except Exception as exc:
            logger.error(
                f"An error occurred while requesting {mds_url} {exc}. Returning {len(results['results'])} results."
            )
        return results

    @staticmethod
    def addGen3ExpectedFields(
        item, mappings, keepOriginalFields, globalFieldFilters, schema
    ):
        """
        Map item fields to gen3 normalized fields
        using the mapping and adding the location
        """
        results = item
        if mappings is not None:
            mapped_fields = RemoteMetadataAdapter.mapFields(
                item, mappings, globalFieldFilters, schema
            )
            if keepOriginalFields:
                results.update(mapped_fields)
            else:
                results = mapped_fields

        return results

    def normalizeToGen3MDSFields(self, data, **kwargs) -> Dict[str, Any]:
        """
        Iterates over the response.
        :param data:
        :return:
        """
        mappings = kwargs.get("mappings", None)
        keepOriginalFields = kwargs.get("keepOriginalFields", False)
        globalFieldFilters = kwargs.get("globalFieldFilters", [])
        schema = kwargs.get("schema", {})

        results = {}
        for item in data["results"]:
            normalized_item = PDCSubjectAdapter.addGen3ExpectedFields(
                item,
                mappings,
                keepOriginalFields,
                globalFieldFilters,
                schema,
            )
            normalized_item["tags"] = [
                {
                    "name": normalized_item[tag] if normalized_item[tag] else "",
                    "category": tag,
                }
                for tag in ["disease_type", "primary_site"]
            ]
            results[normalized_item["_unique_id"]] = {
                "_guid_type": "discovery_metadata",
                "gen3_discovery": normalized_item,
            }

        return results


class PDCStudyAdapter(RemoteMetadataAdapter):
    """
    Simple adapter for Proteomic Data Commons
    Expected Parameters:
        size:   Number of studies to pull in a single call, default=5 and therefore optional
        #Note - The API doesn't seem to do very well with large requests,
                hence confining it to a smaller number
    """

    @retry(
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(httpx.TimeoutException),
        wait=wait_random_exponential(multiplier=1, max=10),
    )
    def getRemoteDataAsJson(self, **kwargs) -> Dict:
        results = {"results": []}

        mds_url = kwargs.get("mds_url", None)
        if mds_url is None:
            return results
        batchSize = kwargs["filters"].get("size", 5)

        query = """
                    query FilteredStudiesDataPaginated(
                    $offset_value: Int
                    $limit_value: Int
                    $sort_value: String
                    $program_name_filter: String!
                    $project_name_filter: String!
                    $study_name_filter: String!
                    $disease_filter: String!
                    $filterValue: String!
                    $analytical_frac_filter: String!
                    $exp_type_filter: String!
                    $ethnicity_filter: String!
                    $race_filter: String!
                    $gender_filter: String!
                    $tumor_grade_filter: String!
                    $sample_type_filter: String!
                    $acquisition_type_filter: String!
                    $data_category_filter: String!
                    $file_type_filter: String!
                    $access_filter: String!
                    $downloadable_filter: String!
                    $biospecimen_status_filter: String!
                    $case_status_filter: String!
                    $getAll: Boolean!
                    ) {
                    getPaginatedUIStudy(
                        offset: $offset_value
                        limit: $limit_value
                        sort: $sort_value
                        program_name: $program_name_filter
                        project_name: $project_name_filter
                        study_name: $study_name_filter
                        disease_type: $disease_filter
                        primary_site: $filterValue
                        analytical_fraction: $analytical_frac_filter
                        experiment_type: $exp_type_filter
                        ethnicity: $ethnicity_filter
                        race: $race_filter
                        gender: $gender_filter
                        tumor_grade: $tumor_grade_filter
                        sample_type: $sample_type_filter
                        acquisition_type: $acquisition_type_filter
                        data_category: $data_category_filter
                        file_type: $file_type_filter
                        access: $access_filter
                        downloadable: $downloadable_filter
                        biospecimen_status: $biospecimen_status_filter
                        case_status: $case_status_filter
                        getAll: $getAll
                    ) {
                        total
                        uiStudies {
                        study_id
                        pdc_study_id
                        submitter_id_name
                        study_description
                        study_submitter_id
                        program_name
                        project_name
                        disease_type
                        primary_site
                        analytical_fraction
                        experiment_type
                        embargo_date
                        cases_count
                        aliquots_count
                        filesCount {
                            file_type
                            data_category
                            files_count
                            __typename
                        }
                        supplementaryFilesCount {
                            data_category
                            file_type
                            files_count
                            __typename
                        }
                        nonSupplementaryFilesCount {
                            data_category
                            file_type
                            files_count
                            __typename
                        }
                        contacts {
                            name
                            institution
                            email
                            url
                            __typename
                        }
                        versions {
                            number
                            __typename
                        }
                        __typename
                        }
                        pagination {
                        count
                        sort
                        from
                        page
                        total
                        pages
                        size
                        __typename
                        }
                        __typename
                    }
                    }
                """

        variables = {
            "offset_value": 0,
            "limit_value": 10,
            "sort_value": "",
            "program_name_filter": "Applied Proteogenomics OrganizationaL Learning and Outcomes - APOLLO",
            "project_name_filter": "",
            "study_name_filter": "",
            "disease_filter": "",
            "filterValue": "",
            "analytical_frac_filter": "",
            "exp_type_filter": "",
            "ethnicity_filter": "",
            "race_filter": "",
            "gender_filter": "",
            "tumor_grade_filter": "",
            "sample_type_filter": "",
            "acquisition_type_filter": "",
            "data_category_filter": "",
            "file_type_filter": "",
            "access_filter": "",
            "downloadable_filter": "",
            "biospecimen_status_filter": "",
            "case_status_filter": "",
            "getAll": False,
        }

        try:
            response = httpx.post(
                mds_url, json={"query": query, "variables": variables}
            )
            response.raise_for_status()
            response_data = response.json()
            results["results"] = response_data["data"]["getPaginatedUIStudy"][
                "uiStudies"
            ]
            logger.info(
                f"Fetched {response_data['data']['getPaginatedUICase']['total']} records from PDC)"
            )

        except httpx.TimeoutException as exc:
            logger.error(f"An timeout error occurred while requesting {mds_url}.")
            raise
        except httpx.HTTPError as exc:
            logger.error(
                f"An HTTP error occurred while requesting {mds_url} {exc}. Returning {len(results['results'])} results."
            )
        except Exception as exc:
            logger.error(
                f"An error occurred while requesting {mds_url} {exc}. Returning {len(results['results'])} results."
            )
        return results

    @staticmethod
    def addGen3ExpectedFields(
        item, mappings, keepOriginalFields, globalFieldFilters, schema
    ):
        """
        Map item fields to gen3 normalized fields
        using the mapping and adding the location
        """
        results = item
        if mappings is not None:
            mapped_fields = RemoteMetadataAdapter.mapFields(
                item, mappings, globalFieldFilters, schema
            )
            if keepOriginalFields:
                results.update(mapped_fields)
            else:
                results = mapped_fields

        return results

    def normalizeToGen3MDSFields(self, data, **kwargs) -> Dict[str, Any]:
        """
        Iterates over the response.
        :param data:
        :return:
        """
        mappings = kwargs.get("mappings", None)
        keepOriginalFields = kwargs.get("keepOriginalFields", False)
        globalFieldFilters = kwargs.get("globalFieldFilters", [])
        schema = kwargs.get("schema", {})

        results = {}
        for item in data["results"]:
            normalized_item = PDCSubjectAdapter.addGen3ExpectedFields(
                item,
                mappings,
                keepOriginalFields,
                globalFieldFilters,
                schema,
            )
            tag_list = []
            for tag_category in ["disease_type", "primary_site"]:
                if normalized_item[tag_category]:
                    tag_string = normalized_item[tag_category]
                    if ";" in tag_string:
                        tags = tag_string.split(";")
                        for tag in tags:
                            tag_list.append({"name": tag, "category": tag_category})
                    else:
                        tag_list.append({"name": tag_string, "category": tag_category})

                else:
                    tag_list.append({"name": "", "category": tag_category})

            normalized_item["tags"] = tag_list
            results[normalized_item["_unique_id"]] = {
                "_guid_type": "discovery_metadata",
                "gen3_discovery": normalized_item,
            }

        return results


class TCIAAdapter(RemoteMetadataAdapter):
    """
    Simple adapter for TCIA (cancerimagingarchive.net)
    """

    @retry(
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(httpx.TimeoutException),
        wait=wait_random_exponential(multiplier=1, max=10),
    )
    def getRemoteDataAsJson(self, **kwargs) -> Dict:
        results = {"results": []}

        mds_url = kwargs.get("mds_url", None)
        if mds_url is None:
            return results

        try:
            response = httpx.get(mds_url)
            response.raise_for_status()

            response_data = response.json()
            results["results"] = response_data

        except httpx.TimeoutException as exc:
            logger.error(f"An timeout error occurred while requesting {mds_url}.")
            raise
        except httpx.HTTPError as exc:
            logger.error(
                f"An HTTP error {exc.response.status_code if exc.response is not None else ''} occurred while requesting {exc.request.url}. Returning {len(results['results'])} results"
            )
        except Exception as exc:
            logger.error(
                f"An error occurred while requesting {mds_url} {exc}. Returning {len(results['results'])} results."
            )

        return results

    @staticmethod
    def addGen3ExpectedFields(
        item, mappings, keepOriginalFields, globalFieldFilters, schema
    ):
        """
        Map item fields to gen3 normalized fields
        using the mapping and adding the location
        """
        results = item
        if mappings is not None:
            mapped_fields = RemoteMetadataAdapter.mapFields(
                item, mappings, globalFieldFilters, schema
            )
            if keepOriginalFields:
                results.update(mapped_fields)
            else:
                results = mapped_fields

        return results

    def normalizeToGen3MDSFields(self, data, **kwargs) -> Dict[str, Any]:
        """
        Iterates over the response.
        :param data:
        :return:
        """
        mappings = kwargs.get("mappings", None)
        keepOriginalFields = kwargs.get("keepOriginalFields", False)
        globalFieldFilters = kwargs.get("globalFieldFilters", [])
        schema = kwargs.get("schema", {})

        results = {}
        for item in data["results"]:
            normalized_item = TCIAAdapter.addGen3ExpectedFields(
                item,
                mappings,
                keepOriginalFields,
                globalFieldFilters,
                schema,
            )

            normalized_item[
                "description"
            ] = f"TCIA data from collection: {normalized_item['program_name']}."

            normalized_item["tags"] = [
                {
                    "name": normalized_item[tag] if normalized_item[tag] else "",
                    "category": tag,
                }
                for tag in ["program_name"]
            ]

            results[normalized_item["_unique_id"]] = {
                "_guid_type": "discovery_metadata",
                "gen3_discovery": normalized_item,
            }

        return results


class WindberSubjectAdapter(RemoteMetadataAdapter):
    @retry(
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(httpx.TimeoutException),
        wait=wait_random_exponential(multiplier=1, max=10),
    )
    def getRemoteDataAsJson(self, **kwargs) -> Dict:
        results = {"results": []}

        mds_url = kwargs.get("mds_url", None)
        if mds_url is None:
            return results

        try:
            response = httpx.get(mds_url)
            response.raise_for_status()

            response_data = response.json()
            results["results"] = response_data

        except httpx.TimeoutException as exc:
            logger.error(f"An timeout error occurred while requesting {mds_url}.")
            raise
        except httpx.HTTPError as exc:
            logger.error(
                f"An HTTP error {exc.response.status_code if exc.response is not None else ''} occurred while requesting {exc.request.url}. Returning {len(results['results'])} results"
            )
        except Exception as exc:
            logger.error(
                f"An error occurred while requesting {mds_url} {exc}. Returning {len(results['results'])} results."
            )

        return results

    @staticmethod
    def addGen3ExpectedFields(
        item, mappings, keepOriginalFields, globalFieldFilters, schema
    ):
        """
        Map item fields to gen3 normalized fields
        using the mapping and adding the location
        """
        results = item
        if mappings is not None:
            mapped_fields = RemoteMetadataAdapter.mapFields(
                item, mappings, globalFieldFilters, schema
            )
            if keepOriginalFields:
                results.update(mapped_fields)
            else:
                results = mapped_fields

        return results

    def normalizeToGen3MDSFields(self, data, **kwargs) -> Dict[str, Any]:
        """
        Iterates over the response.
        :param data:
        :return:
        """
        mappings = kwargs.get("mappings", None)
        keepOriginalFields = kwargs.get("keepOriginalFields", False)
        globalFieldFilters = kwargs.get("globalFieldFilters", [])
        schema = kwargs.get("schema", {})

        results = {}
        for item in data["results"]:
            normalized_item = WindberSubjectAdapter.addGen3ExpectedFields(
                item,
                mappings,
                keepOriginalFields,
                globalFieldFilters,
                schema,
            )

            normalized_item["description"] = "Windber data from collection"

            normalized_item["tags"] = [
                {
                    "name": normalized_item[tag] if normalized_item[tag] else "",
                    "category": tag,
                }
                for tag in ["primary_disease", "cancer_type"]
            ]

            results[normalized_item["_unique_id"]] = {
                "_guid_type": "Windber_subject_metadata",
                "gen3_discovery": normalized_item,
            }

        return results


def gather_metadata(
    gather,
    mds_url,
    config,
    filters,
    mappings,
    perItemValues,
    keepOriginalFields,
    globalFieldFilters,
    schema,
):
    try:
        json_data = gather.getRemoteDataAsJson(
            mds_url=mds_url, filters=filters, config=config
        )
        results = gather.normalizeToGen3MDSFields(
            json_data,
            config=config,
            mappings=mappings,
            perItemValues=perItemValues,
            keepOriginalFields=keepOriginalFields,
            globalFieldFilters=globalFieldFilters,
            schema=schema,
        )
        logger.debug("Result after normalizing: ")
        logger.debug(results)
        return results
    except ValueError as exc:
        logger.error(f"Exception occurred: {exc}. Returning no results")
    except RetryError:
        logger.error("Multiple retries failed. Returning no results")
    return {}


adapters = {
    "icpsr": ISCPSRDublin,
    "clinicaltrials": ClinicalTrials,
    "pdaps": PDAPS,
    "mps": MPSAdapter,
    "gen3": Gen3Adapter,
    "drs_indexd": DRSIndexdAdapter,
    "harvard_dataverse": HarvardDataverse,
    "icdc": ICDCAdapter,
    "gdc": GDCAdapter,
    "cidc": CIDCAdapter,
    "pdc": PDCAdapter,
    "tcia": TCIAAdapter,
    "pdcsubject": PDCSubjectAdapter,
    "pdcstudy": PDCStudyAdapter,
    "windbersubject": WindberSubjectAdapter,
}


def get_metadata(
    adapter_name,
    mds_url,
    filters,
    config=None,
    mappings=None,
    perItemValues=None,
    keepOriginalFields=False,
    globalFieldFilters=None,
    schema=None,
):
    if config is None:
        config = {}

    if globalFieldFilters is None:
        globalFieldFilters = []

    gather = None
    if adapter_name in adapters:
        gather = adapters[adapter_name]()

    if gather is None:
        logger.error(
            f"unknown adapter for commons {adapter_name}. Returning no results."
        )
        return {}

    return gather_metadata(
        gather,
        mds_url=mds_url,
        filters=filters,
        config=config,
        mappings=mappings,
        perItemValues=perItemValues,
        keepOriginalFields=keepOriginalFields,
        globalFieldFilters=globalFieldFilters,
        schema=schema,
    )
