import collections.abc
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Tuple, Union
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


class FieldFilters:
    filters = {
        "strip_html": strip_html,
        "strip_email": strip_email,
        "add_icpsr_source_url": add_icpsr_source_url,
        "add_clinical_trials_source_url": add_clinical_trials_source_url,
        "uppercase": uppercase,
        "prepare_cidc_description": prepare_cidc_description,
        "aggregate_pdc_file_count": aggregate_pdc_file_count,
    }

    @classmethod
    def execute(cls, name, value):
        if name not in FieldFilters.filters:
            logger.warning(f"filter {name} not found: returning original value.")
            return value
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
                for flt in filters:
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
