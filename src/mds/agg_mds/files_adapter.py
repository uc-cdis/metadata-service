from mds.agg_mds.adapters import Gen3Adapter, RemoteMetadataAdapter
from mds import logger


class MC2DPFilesAdapter(Gen3Adapter):
    def normalizeToGen3MDSFields(self, data, **kwargs) -> Dict[str, Any]:
        """
        Iterates over the response extracting each data file from the _data_files field.
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
            # get all of the data files in the _data_files field

            if "_data_files" not in record:
                logger.error("Data files field not in record. Skipping")
                continue

            items = record["_data_files"]

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
