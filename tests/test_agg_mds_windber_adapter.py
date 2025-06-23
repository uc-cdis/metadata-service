import respx
import httpx

from mds.agg_mds.adapters import get_metadata


@respx.mock
def test_get_metadata_windbersubject():
    windber_response_subject = """
    [
        {
            "external_windber_id": "PR-ABCDEFG",
            "apollo_id": null,
            "age_at_index": null,
            "age_at_index_gt89": null,
            "gender": "Male",
            "race": "White",
            "ethnicity": "Not Spanish/Hispanic/Latino",
            "primary_disease": "PDA",
            "cancer_type": "CTA",
            "metastasis": null,
            "cancer_grade": null,
            "exposure_type": "",
            "chemicals_exposed_to": null
        },
        {
            "external_windber_id": "PR-HIJKLM",
            "apollo_id": "AP-1234",
            "age_at_index": null,
            "age_at_index_gt89": null,
            "gender": "Female",
            "race": "",
            "ethnicity": "",
            "primary_disease": "PDB",
            "cancer_type": null,
            "metastasis": null,
            "cancer_grade": null,
            "exposure_type": "",
            "chemicals_exposed_to": null
        }
    ]
    """

    field_mappings_subject = {
        "_unique_id": "path:external_windber_id",
        "apollo_id": "path:apollo_id",
        "commons": "Windber",
        "age_at_index": "path:age_at_index",
        "age_at_index_gt89": "path:age_at_index_gt89",
        "gender": "path:gender",
        "race": "path:race",
        "ethnicity": "path:ethnicity",
        "primary_disease": "path:primary_disease",
        "cancer_type": "path:cancer_type",
        "metastasis": "path:metastasis",
        "cancer_grade": "path:cancer_grade",
        "exposure_type": "path:exposure_type",
        "chemicals_exposed_to": "path:chemicals_exposed_to",
    }

    respx.get("http://test/ok").mock(side_effect=httpx.HTTPError)
    assert (
        get_metadata(
            "windbersubject",
            "http://test/ok",
            filters=None,
            mappings=field_mappings_subject,
        )
        == {}
    )

    respx.get("http://test/ok").mock(side_effect=Exception)
    assert (
        get_metadata(
            "windbersubject",
            "http://test/ok",
            filters=None,
            mappings=field_mappings_subject,
        )
        == {}
    )

    filters = {"size": 5}
    respx.get(
        "http://test/ok",
    ).mock(
        return_value=httpx.Response(status_code=200, content=windber_response_subject)
    )
    assert get_metadata(
        "windbersubject",
        "http://test/ok",
        filters=filters,
        mappings=field_mappings_subject,
    ) == {
        "PR-ABCDEFG": {
            "_guid_type": "Windber_subject_metadata",
            "gen3_discovery": {
                "_unique_id": "PR-ABCDEFG",
                "apollo_id": None,
                "age_at_index": None,
                "age_at_index_gt89": None,
                "commons": "Windber",
                "gender": "Male",
                "race": "White",
                "ethnicity": "Not Spanish/Hispanic/Latino",
                "primary_disease": "PDA",
                "cancer_type": "CTA",
                "metastasis": None,
                "cancer_grade": None,
                "exposure_type": "",
                "chemicals_exposed_to": None,
                "tags": [
                    {"category": "primary_disease", "name": "PDA"},
                    {"category": "cancer_type", "name": "CTA"},
                ],
            },
        },
        "PR-HIJKLM": {
            "_guid_type": "Windber_subject_metadata",
            "gen3_discovery": {
                "_unique_id": "PR-HIJKLM",
                "apollo_id": "AP-1234",
                "age_at_index": None,
                "age_at_index_gt89": None,
                "commons": "Windber",
                "gender": "Female",
                "race": "",
                "ethnicity": "",
                "primary_disease": "PDB",
                "cancer_type": None,
                "metastasis": None,
                "cancer_grade": None,
                "exposure_type": "",
                "chemicals_exposed_to": None,
                "tags": [
                    {"category": "primary_disease", "name": "PDB"},
                    {"category": "cancer_type", "name": None},
                ],
            },
        },
    }
