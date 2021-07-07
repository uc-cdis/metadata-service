import collections.abc
from abc import ABC, abstractmethod
from typing import Dict, Tuple

import httpx
import xmltodict


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
        pass

    @abstractmethod
    def normalizeToGen3MDSFields(self, data, **kwargs) -> Dict:
        pass

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
                    raise ValueError(f"An error occurred while requesting {url}")

                more = False

        return results

    @staticmethod
    def buildIdentifier(id: str):
        return id.replace("http://doi.org/", "").replace("dc:", "")

    @staticmethod
    def addGen3ExpectedFields(item):
        item["authz"] = ""
        item["tags"] = []

    def normalizeToGen3MDSFields(self, data, **kwargs) -> Tuple[Dict, str]:
        """
        Iterates over the response.
        :param data:
        :return:
        """
        # TODO: add asserts/checks for existence of the fields
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
                    else:
                        item[str.replace(key, "dc:", "")] = value
            ISCPSRDublin.addGen3ExpectedFields(item)
            results[item["identifier"]] = {
                "_guid_type": "discovery_metadata",
                "gen3_discovery": item,
            }
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
                    raise ValueError(
                        f"An error occurred while requesting {self.baseURL}."
                    )

        except Exception as ex:
            raise ValueError(f"An error occurred while requesting {self.baseURL} {ex}.")

        return results

    @staticmethod
    def addGen3ExpectedFields(item):
        item["authz"] = ""
        item["tags"] = []

    def normalizeToGen3MDSFields(self, data, **kwargs) -> Tuple[Dict, str]:
        """
        Iterates over the response.
        :param data:
        :return:
        """
        # TODO: add asserts/checks for existence of the fields
        results = {}
        for item in data["results"]:
            item = item["Study"]
            item = flatten(item)
            ClinicalTrials.addGen3ExpectedFields(item)
            results[item["NCTId"]] = {
                "_guid_type": "discovery_metadata",
                "gen3_discovery": item,
            }
        return results


def get_metadata(adapter_name, mds_url, filters):
    if adapter_name == "icpsr":
        gather = ISCPSRDublin(mds_url)
        json_data = gather.getRemoteDataAsJson(filters=filters)
        results = gather.normalizeToGen3MDSFields(json_data)
        return results
    if adapter_name == "clinicaltrials":
        gather = ClinicalTrials(mds_url)
        json_data = gather.getRemoteDataAsJson(filters=filters)
        results = gather.normalizeToGen3MDSFields(json_data)
        return results
    else:
        raise Exception(f"unknown adapter for commons: {name}")
