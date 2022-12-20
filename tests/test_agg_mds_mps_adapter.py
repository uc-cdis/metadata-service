import respx
import httpx

from mds.agg_mds.adapters import get_metadata


@respx.mock
def test_get_metadata_mps():
    json_response = {
        "id": "23",
        "name": "Motif-PS-Chipshop",
        "data_group": "Taylor_MPS",
        "study_types": "CC",
        "start_date": "2015-09-30",
        "description": "Motif polystyrene devices, using different flow setups. Human hepatocytes fresh isolated were used in. (3 cell model)",
    }

    field_mappings = {
        "tags": [
            {"name": "MPS", "category": "Commons"},
            {"name": "Physiological", "category": "Data Type"},
        ],
        "authz": "",
        "sites": "",
        "summary": "path:description",
        "study_url": {"path": "url", "default": ""},
        "location": "path:data_group",
        "subjects": "",
        "__manifest": "",
        "study_name": "path:name",
        "study_type": "path:study_types",
        "institutions": "path:data_group",
        "year_awarded": "",
        "investigators": "path:data_group",
        "project_title": {"path": "title", "default": ""},
        "protocol_name": "",
        "study_summary": "",
        "_file_manifest": "",
        "dataset_1_type": "",
        "dataset_2_type": "",
        "dataset_3_type": "",
        "dataset_4_type": "",
        "dataset_5_type": "",
        "project_number": "",
        "dataset_1_title": "",
        "dataset_2_title": "",
        "dataset_3_title": "",
        "dataset_4_title": "",
        "dataset_5_title": "",
        "administering_ic": "",
        "advSearchFilters": [],
        "dataset_category": "",
        "research_program": "",
        "research_question": "",
        "study_description": "",
        "clinical_trial_link": "",
        "dataset_description": "",
        "research_focus_area": "",
        "dataset_1_description": "",
        "dataset_2_description": "",
        "dataset_3_description": "",
        "dataset_4_description": "",
        "dataset_5_description": "",
        "data_availability": "",
    }

    assert get_metadata("mps", "http://test/ok", filters=None, config=None) == {}

    assert (
        get_metadata("mps", "http://test/ok", filters=None, config={"batchSize": 256})
        == {}
    )
    assert (
        get_metadata("mps", mds_url=None, filters={"study_ids": [23]}, config=None)
        == {}
    )

    # Test remote client-side error handling
    respx.get("http://test/err404/23/").mock(
        return_value=httpx.Response(
            status_code=404,
            json={},
        )
    )
    assert (
        get_metadata(
            "mps",
            "http://test/err404",
            filters={"study_ids": [23]},
            mappings=field_mappings,
            keepOriginalFields=False,
        )
        == {}
    )

    # Test general exception handling
    respx.get("http://test/textresponse/23/").mock(
        return_value=httpx.Response(
            status_code=200,
            text="hello!",
        )
    )
    assert (
        get_metadata(
            "mps",
            "http://test/textresponse",
            filters={"study_ids": [23]},
            mappings=field_mappings,
            keepOriginalFields=False,
        )
        == {}
    )

    respx.get("http://test/ok/23/").mock(
        return_value=httpx.Response(
            status_code=200,
            json=json_response,
        )
    )

    assert get_metadata(
        "mps",
        "http://test/ok",
        filters={"study_ids": [23]},
        mappings=field_mappings,
        keepOriginalFields=False,
    ) == {
        "MPS_study_23": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "tags": [
                    {"name": "MPS", "category": "Commons"},
                    {"name": "Physiological", "category": "Data Type"},
                ],
                "authz": "",
                "sites": "",
                "summary": "Motif polystyrene devices, using different flow setups. Human hepatocytes fresh isolated were used in. (3 cell model)",
                "study_url": "",
                "location": "Taylor_MPS",
                "subjects": "",
                "__manifest": "",
                "study_name": "Motif-PS-Chipshop",
                "study_type": "CC",
                "institutions": "Taylor_MPS",
                "year_awarded": "",
                "investigators": "Taylor_MPS",
                "project_title": "",
                "protocol_name": "",
                "study_summary": "",
                "_file_manifest": "",
                "dataset_1_type": "",
                "dataset_2_type": "",
                "dataset_3_type": "",
                "dataset_4_type": "",
                "dataset_5_type": "",
                "project_number": "",
                "dataset_1_title": "",
                "dataset_2_title": "",
                "dataset_3_title": "",
                "dataset_4_title": "",
                "dataset_5_title": "",
                "administering_ic": "",
                "advSearchFilters": [],
                "dataset_category": "",
                "research_program": "",
                "research_question": "",
                "study_description": "",
                "clinical_trial_link": "",
                "dataset_description": "",
                "research_focus_area": "",
                "dataset_1_description": "",
                "dataset_2_description": "",
                "dataset_3_description": "",
                "dataset_4_description": "",
                "dataset_5_description": "",
                "data_availability": "",
            },
        }
    }

    assert get_metadata(
        "mps",
        "http://test/ok",
        filters={"study_ids": [23]},
        mappings=field_mappings,
        keepOriginalFields=True,
    ) == {
        "MPS_study_23": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "tags": [
                    {"name": "MPS", "category": "Commons"},
                    {"name": "Physiological", "category": "Data Type"},
                ],
                "authz": "",
                "sites": "",
                "summary": "Motif polystyrene devices, using different flow setups. Human hepatocytes fresh isolated were used in. (3 cell model)",
                "study_url": "",
                "location": "Taylor_MPS",
                "subjects": "",
                "__manifest": "",
                "study_name": "Motif-PS-Chipshop",
                "study_type": "CC",
                "institutions": "Taylor_MPS",
                "year_awarded": "",
                "investigators": "Taylor_MPS",
                "project_title": "",
                "protocol_name": "",
                "study_summary": "",
                "_file_manifest": "",
                "dataset_1_type": "",
                "dataset_2_type": "",
                "dataset_3_type": "",
                "dataset_4_type": "",
                "dataset_5_type": "",
                "project_number": "",
                "dataset_1_title": "",
                "dataset_2_title": "",
                "dataset_3_title": "",
                "dataset_4_title": "",
                "dataset_5_title": "",
                "administering_ic": "",
                "advSearchFilters": [],
                "dataset_category": "",
                "research_program": "",
                "research_question": "",
                "study_description": "",
                "clinical_trial_link": "",
                "dataset_description": "",
                "research_focus_area": "",
                "dataset_1_description": "",
                "dataset_2_description": "",
                "dataset_3_description": "",
                "dataset_4_description": "",
                "dataset_5_description": "",
                "data_availability": "",
                "id": "23",
                "name": "Motif-PS-Chipshop",
                "data_group": "Taylor_MPS",
                "study_types": "CC",
                "start_date": "2015-09-30",
                "description": "Motif polystyrene devices, using different flow setups. Human hepatocytes fresh isolated were used in. (3 cell model)",
            },
        }
    }
    try:
        from mds.agg_mds.adapters import MPSAdapter

        MPSAdapter.getRemoteDataAsJson.retry.wait = wait_none()

        respx.get("http://test/timeouterror/23/",).mock(
            side_effect=httpx.TimeoutException,
        )

        get_metadata(
            "mps",
            "http://test/timeouterror",
            filters={"study_ids": [23]},
            mappings=field_mappings,
            keepOriginalFields=True,
        )

    except Exception as exc:
        assert isinstance(exc, RetryError) == True
