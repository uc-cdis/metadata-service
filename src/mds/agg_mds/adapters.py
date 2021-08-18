import collections.abc
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Tuple, Union
from jsonpath_ng import parse
import httpx
import xmltodict
import bleach
import re
from mds import logger


def strip_email(text: str):
    rgx = r"[\w.+-]+@[\w-]+\.[\w.-]+"
    matches = re.findall(rgx, text)
    for cur_word in matches:
        text = text.replace(cur_word, "")
    return text


def strip_html(s: str):
    return bleach.clean(s, tags=[], strip=True)


def add_icpsr_source_url(id: str):
    return f"https://www.icpsr.umich.edu/web/NAHDAP/studies/{id}"


def add_clinical_trials_source_url(id: str):
    return f"https://clinicaltrials.gov/ct2/show/{id}"


class FieldFilters:
    filters = {
        "strip_html": strip_html,
        "strip_email": strip_email,
        "add_icpsr_source_url": add_icpsr_source_url,
        "add_clinical_trials_source_url": add_clinical_trials_source_url,
    }

    @classmethod
    def execute(cls, name, value):
        if name not in FieldFilters.filters:
            logger.warning(f"filter {name} not found: returning original value.")
            return value
        return FieldFilters.filters[name](value)


def get_json_path_value(expression: str, item: dict) -> Union[str, List[Any]]:
    """
    Given a JSON Path expression and a dictionary, using the path expression
    to find the value. If not found return an empty string
    """

    if expression is None:
        return ""

    jsonpath_expr = parse(expression)
    v = jsonpath_expr.find(item)
    if len(v) == 0:  # nothing found use default value of empty string
        return ""

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
    dictionary and normalizeToGen3MDSField to get closer to the expected Gen3 MDS format, although this will be subject
    to change
    """

    @abstractmethod
    def getRemoteDataAsJson(self, **kwargs) -> Tuple[Dict, str]:
        """ needs to be implemented in derived class """

    @abstractmethod
    def normalizeToGen3MDSFields(self, data, **kwargs) -> Dict:
        """ needs to be implemented in derived class """

    @staticmethod
    def mapFields(item: dict, mappings: dict, global_filters: list = []) -> dict:
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
        }

        :param item: dictionary to map fields to
        :param mappings:
        :return:
        """

        results = {}

        for key, value in mappings.items():
            if isinstance(value, dict):  # have a complex assignment
                expression = value.get("path", None)
                field_value = get_json_path_value(expression, item)

                filters = value.get("filters", [])
                for filter in filters:
                    field_value = FieldFilters.execute(filter, field_value)

            elif "path:" in value:
                # process as json path
                expression = value.split("path:")[1]
                field_value = get_json_path_value(expression, item)
            else:
                field_value = value

            for f in global_filters:
                field_value = FieldFilters.execute(f, field_value)
            results[key] = field_value
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


class ISCPSRDublin(RemoteMetadataAdapter):
    """
    Simple adapter for ICPSR
    parameters: filters which currently should be study_ids=id,id,id...
    """

    def __init__(self, baseURL):
        self.baseURL = baseURL

    def getRemoteDataAsJson(self, **kwargs) -> Dict:
        results = {"results": []}
        if "filters" not in kwargs or kwargs["filters"] is None:
            return results

        study_ids = kwargs["filters"].get("study_ids", [])

        if len(study_ids) > 0:
            for id in study_ids:
                url = f"{self.baseURL}?verb=GetRecord&metadataPrefix=oai_dc&identifier={id}"
                response = httpx.get(url)

                if response.status_code == 200:
                    xmlData = response.text
                    data_dict = xmltodict.parse(xmlData)
                    results["results"].append(data_dict)
                else:
                    logger.error(
                        f"A {response.status_code} error occurred while requesting {url}"
                    )
                    raise ValueError(f"An error occurred while requesting {url}")

        return results

    @staticmethod
    def buildIdentifier(id: str):
        return id.replace("http://doi.org/", "").replace("dc:", "")

    @staticmethod
    def addGen3ExpectedFields(item, mappings, keepOriginalFields, globalFieldFilters):
        results = item
        if mappings is not None:
            mapped_fields = RemoteMetadataAdapter.mapFields(
                item, mappings, globalFieldFilters
            )
            if keepOriginalFields:
                results.update(mapped_fields)
            else:
                results = mapped_fields

        if isinstance(results["investigators"], list):
            results["investigators"] = ",".join(results["investigators"])
        return results

    def normalizeToGen3MDSFields(self, data, **kwargs) -> Tuple[Dict, str]:
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
                item, mappings, keepOriginalFields, globalFieldFilters
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

    def __init__(self, baseURL="https://clinicaltrials.gov/api/query/full_studies"):
        self.baseURL = baseURL

    def getRemoteDataAsJson(self, **kwargs) -> Dict:
        results = {"results": []}

        if "filters" not in kwargs or kwargs["filters"] is None:
            return results

        term = kwargs["filters"].get("term", None)

        if "term" == None:
            return results

        term = term.replace(" ", "+")

        batchSize = kwargs["filters"].get("batchSize", 100)
        maxItems = kwargs["filters"].get("maxItems", None)
        offset = 1
        remaining = 1
        limit = min(maxItems, batchSize) if maxItems is not None else batchSize
        try:
            while remaining > 0:
                response = httpx.get(
                    f"{self.baseURL}?expr={term}"
                    f"&fmt=json&min_rnk={offset}&max_rnk={offset + limit - 1}"
                )

                if response.status_code == 200:

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
                    results["results"].extend(
                        data["FullStudiesResponse"]["FullStudies"]
                    )
                    if maxItems is not None and len(results["results"]) >= maxItems:
                        return results
                    remaining = remaining - numReturned
                    offset += numReturned
                    limit = min(remaining, batchSize)
                else:
                    logger.error(
                        f"A {response.status_code} error occurred while requesting {self.baseURL}"
                    )
                    raise ValueError(
                        f"An error occurred while requesting {self.baseURL}."
                    )

        except Exception as ex:
            logger.error(f"An error occurred while requesting {self.baseURL} {ex}.")
            raise ValueError(f"An error occurred while requesting {self.baseURL} {ex}.")

        return results

    @staticmethod
    def addGen3ExpectedFields(item, mappings, keepOriginalFields, globalFieldFilters):
        """
        Map item fields to gen3 normalized fields
        using the mapping and adding the location
        """
        results = item
        if mappings is not None:
            mapped_fields = RemoteMetadataAdapter.mapFields(
                item, mappings, globalFieldFilters
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

    def normalizeToGen3MDSFields(self, data, **kwargs) -> Tuple[Dict, str]:
        """
        Iterates over the response.
        :param data:
        :return:
        """

        mappings = kwargs.get("mappings", None)
        keepOriginalFields = kwargs.get("keepOriginalFields", True)
        globalFieldFilters = kwargs.get("globalFieldFilters", [])

        results = {}
        for item in data["results"]:
            item = item["Study"]
            item = flatten(item)
            normalized_item = ClinicalTrials.addGen3ExpectedFields(
                item, mappings, keepOriginalFields, globalFieldFilters
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

    def __init__(self, baseURL: str = "https://api.monqcle.com/"):
        self.baseURL = baseURL

    def getRemoteDataAsJson(self, **kwargs) -> Dict:
        results = {"results": []}

        if "filters" not in kwargs or kwargs["filters"] is None:
            return results

        datasets = kwargs["filters"].get("datasets", None)

        if datasets == None:
            return results

        for id in datasets:
            try:
                response = httpx.get(
                    f"{self.baseURL}siteitem/{id}/get_by_dataset?site_key=56e805b9d6c9e75c1ac8cb12"
                )
                if response.status_code == 200:
                    results["results"].append(response.json())
                else:
                    logger.error(
                        f"A {response.status_code} error occurred while requesting {self.baseURL}."
                    )
                    raise ValueError(
                        f"An error occurred while requesting {self.baseURL}."
                    )
            except:
                logger.error(f"An error occurred while requesting {self.baseURL}.")
                raise ValueError(f"An error occurred while requesting {self.baseURL}.")

        return results

    @staticmethod
    def addGen3ExpectedFields(item, mappings, keepOriginalFields, globalFieldFilters):
        """
        Maps the items fields into Gen3 expected fields
        if keepOriginal is False: only those fields will be included in the final entry
        """
        results = item
        if mappings is not None:
            mapped_fields = RemoteMetadataAdapter.mapFields(
                item, mappings, globalFieldFilters
            )
            if keepOriginalFields:
                results.update(mapped_fields)
            else:
                results = mapped_fields

        if isinstance(results["investigators"], list):
            results["investigators"] = results["investigators"].join(", ")
        return results

    def normalizeToGen3MDSFields(self, data, **kwargs) -> Tuple[Dict, str]:
        """
        Iterates over the response.
        :param data:
        :return:
        """

        mappings = kwargs.get("mappings", None)
        keepOriginalFields = kwargs.get("keepOriginalFields", True)
        globalFieldFilters = kwargs.get("globalFieldFilters", [])

        results = {}
        for item in data["results"]:
            normalized_item = PDAPS.addGen3ExpectedFields(
                item, mappings, keepOriginalFields, globalFieldFilters
            )
            if "display_id" not in item:
                continue
            results[item["display_id"]] = {
                "_guid_type": "discovery_metadata",
                "gen3_discovery": normalized_item,
            }

        perItemValues = kwargs.get("perItemValues", None)
        if perItemValues is not None:
            RemoteMetadataAdapter.setPerItemValues(results, perItemValues)

        return results


class Gen3Adapter(RemoteMetadataAdapter):
    """
    Simple adapter for Gen3 Metadata Service
    """

    def getRemoteDataAsJson(self, **kwargs) -> Dict:
        results = {"results": {}}

        mds_url = kwargs.get("mds_url", None)
        if mds_url is None:
            return results
        guid_type = kwargs.get("guid_type", "discovery_metadata")
        field_name = kwargs.get("field_name", None)
        field_value = kwargs.get("field_value", None)
        batchSize = kwargs.get("batchSize", 1000)
        maxItems = kwargs.get("maxItems", None)
        offset = 0
        limit = min(maxItems, batchSize) if maxItems is not None else batchSize
        moreData = True
        while moreData:
            try:
                url = f"{mds_url}mds/metadata?data=True&_guid_type={guid_type}&limit={limit}&offset={offset}"
                if field_name is not None and field_value is not None:
                    url += f"&{guid_type}.{field_name}={field_value}"
                response = httpx.get(url)
                if response.status_code == 200:
                    data = response.json()
                    results["results"].update(data)
                    numReturned = len(data)

                    if numReturned == 0 or numReturned < limit:
                        moreData = False
                    offset += numReturned
                else:
                    logger.error(
                        f"{response.status_code} error occurred while requesting {self.mds_url}."
                    )
                    raise ValueError(f"An error occurred while requesting {mds_url}.")
            except httpx.RequestError as exc:
                logger.error(f"An error occurred while requesting {exc.request.url}.")
                raise ValueError(
                    f"An error occurred while requesting {exc.request.url}."
                )

        return results

    @staticmethod
    def addGen3ExpectedFields(item, mappings, keepOriginalFields, globalFieldFilters):
        """"""
        results = item
        if mappings is not None:
            mapped_fields = RemoteMetadataAdapter.mapFields(
                item, mappings, globalFieldFilters
            )
            if keepOriginalFields:
                results.update(mapped_fields)
            else:
                results = mapped_fields

        return results

    def normalizeToGen3MDSFields(self, data, **kwargs) -> Tuple[Dict, str]:
        """
        Iterates over the response.
        :param data:
        :return:
        """

        mappings = kwargs.get("mappings", None)
        study_field = kwargs.get("study_field", "gen3_discovery")
        keepOriginalFields = kwargs.get("keepOriginalFields", True)
        globalFieldFilters = kwargs.get("globalFieldFilters", [])

        results = {}
        for guid, record in data["results"].items():
            item = Gen3Adapter.addGen3ExpectedFields(
                record[study_field], mappings, keepOriginalFields, globalFieldFilters
            )
            results[guid] = {
                "_guid_type": "discovery_metadata",
                "gen3_discovery": item,
            }

        perItemValues = kwargs.get("perItemValues", None)
        if perItemValues is not None:
            RemoteMetadataAdapter.setPerItemValues(results, perItemValues)

        return results


def get_metadata(
    adapter_name,
    mds_url,
    filters,
    mappings=None,
    perItemValues=None,
    keepOriginalFields=False,
    globalFieldFilters=[],
):
    if adapter_name == "icpsr":
        gather = ISCPSRDublin(mds_url)
        json_data = gather.getRemoteDataAsJson(filters=filters)
        results = gather.normalizeToGen3MDSFields(
            json_data,
            mappings=mappings,
            perItemValues=perItemValues,
            keepOriginalFields=keepOriginalFields,
            globalFieldFilters=globalFieldFilters,
        )
        return results
    if adapter_name == "clinicaltrials":
        gather = ClinicalTrials(mds_url)
        json_data = gather.getRemoteDataAsJson(filters=filters)
        results = gather.normalizeToGen3MDSFields(
            json_data,
            mappings=mappings,
            perItemValues=perItemValues,
            keepOriginalFields=keepOriginalFields,
            globalFieldFilters=globalFieldFilters,
        )
        return results
    if adapter_name == "pdaps":
        gather = PDAPS(mds_url)
        json_data = gather.getRemoteDataAsJson(filters=filters)
        results = gather.normalizeToGen3MDSFields(
            json_data,
            mappings=mappings,
            perItemValues=perItemValues,
            keepOriginalFields=keepOriginalFields,
            globalFieldFilters=globalFieldFilters,
        )
        return results
    if adapter_name == "gen3":
        gather = Gen3Adapter()
        json_data = gather.getRemoteDataAsJson(mds_url=mds_url, filters=filters)
        results = gather.normalizeToGen3MDSFields(
            json_data,
            mappings=mappings,
            perItemValues=perItemValues,
            keepOriginalFields=keepOriginalFields,
            globalFieldFilters=globalFieldFilters,
        )
        return results

    logger.error(f"unknown adapter for commons {name}.")
    raise Exception(f"unknown adapter for commons: {name}")
