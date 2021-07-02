import httpx
import json
import xmltodict
from typing import Dict, List, Tuple
from abc import ABC, abstractmethod


class RemoteMetadataAdapter(ABC):
    """
    Abstact base class for a Metadata adapter. You must implement getRemoteDataAsJson to return a possibly empty
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
        ## TODO: add asserts/checks for existence of the fields
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


def get_metadata(adapter_name, mds_url, filters):
    if adapter_name == "icpsr":
        gather = ISCPSRDublin(mds_url)
        json_data = gather.getRemoteDataAsJson(filters=filters)
        results = gather.normalizeToGen3MDSFields(json_data)
        return results
    else:
        raise Exception(f"unknown adapter for commons: {name}")
