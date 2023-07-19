import respx
import httpx

from mds.agg_mds.adapters import get_metadata


@respx.mock
def test_get_metadata_gdc():
    json_response = """{
    "data": {
      "hits": [
        {
          "id": "TARGET-NBL",
          "summary": {
            "file_count": 5358,
            "case_count": 1132,
            "file_size": 16831864288520
          },
          "primary_site": [
            "Retroperitoneum and peritoneum",
            "Lymph nodes"
          ],
          "dbgap_accession_number": "phs000467",
          "project_id": "TARGET-NBL",
          "disease_type": [
            "Neuroepitheliomatous Neoplasms",
            "Not Applicable"
          ],
          "name": "Neuroblastoma",
          "releasable": true,
          "state": "open",
          "released": true
        },
        {
          "id": "GENIE-GRCC",
          "summary": {
            "file_count": 1038,
            "case_count": 1038,
            "file_size": 2642030
          },
          "primary_site": [
            "Other and ill-defined sites in lip, oral cavity and pharynx",
            "Uterus, NOS"
          ],
          "name": "AACR Project GENIE - Contributed by Institut Gustave Roussy",
          "releasable": true,
          "state": "open",
          "released": true
        },
        {
          "id": "GENIE-DFCI",
          "summary": {
            "file_count": 28464,
            "case_count": 14232,
            "file_size": 410474608
          },
          "primary_site": [
            "Nasal cavity and middle ear",
            "Colon"
          ],
          "dbgap_accession_number": null,
          "project_id": "GENIE-DFCI",
          "disease_type": [
            "Lymphatic Vessel Tumors",
            "Granular Cell Tumors and Alveolar Soft Part Sarcomas"
          ],
          "name": "AACR Project GENIE - Contributed by Dana-Farber Cancer Institute",
          "releasable": true,
          "state": "open",
          "released": true
        }
      ]
}
}
"""
    field_mappings = {
        "commons": "CRDC Genomic Data Commons",
        "short_name": "path:id",
        "full_name": "path:name",
        "disease_type": "path:disease_type",
        "primary_site": "path:primary_site",
        "_unique_id": "path:id",
        "tags": [],
        "project_id": "path:id",
        "study_title": "path:id",
        "accession_number": "path:id",
        "description": "",
        "funding": "",
        "source": "",
        "dbgap_accession_number": "path:dbgap_accession_number",
        "_subjects_count": "path:summary.case_count",
        "subjects_count": "path:summary.case_count",
        "files_count": "path:summary.file_count",
        "study_metadata.minimal_info.alternative_study_name": "path:name",
    }

    filters = {"size": 1000}
    respx.get("http://test/ok?expand=summary&from=0&size=1000").mock(
        side_effect=httpx.TimeoutException
    )

    assert (
        get_metadata("gdc", "http://test/ok", filters=filters, mappings=field_mappings)
        == {}
    )

    respx.get("http://test/ok?expand=summary&from=0&size=1000").mock(
        side_effect=httpx.HTTPError
    )

    assert (
        get_metadata("gdc", "http://test/ok", filters=filters, mappings=field_mappings)
        == {}
    )

    respx.get("http://test/ok?expand=summary&from=0&size=1000").mock(
        side_effect=Exception
    )

    assert (
        get_metadata("gdc", "http://test/ok", filters=filters, mappings=field_mappings)
        == {}
    )

    respx.get("http://test/ok?expand=summary&from=0&size=1000").mock(
        return_value=httpx.Response(status_code=200, content=json_response)
    )

    assert get_metadata("gdc", None, filters=filters, mappings=field_mappings) == {}

    assert get_metadata(
        "gdc",
        "http://test/ok",
        filters=filters,
        mappings=field_mappings,
        keepOriginalFields=True,
    ) == {
        "TARGET-NBL": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "id": "TARGET-NBL",
                "summary": {
                    "file_count": 5358,
                    "case_count": 1132,
                    "file_size": 16831864288520,
                },
                "primary_site": ["Retroperitoneum and peritoneum", "Lymph nodes"],
                "dbgap_accession_number": "phs000467",
                "project_id": "TARGET-NBL",
                "disease_type": ["Neuroepitheliomatous Neoplasms", "Not Applicable"],
                "name": "Neuroblastoma",
                "releasable": True,
                "state": "open",
                "released": True,
                "commons": "CRDC Genomic Data Commons",
                "short_name": "TARGET-NBL",
                "full_name": "Neuroblastoma",
                "_unique_id": "TARGET-NBL",
                "tags": [
                    {
                        "name": "Neuroepitheliomatous Neoplasms",
                        "category": "disease_type",
                    },
                    {
                        "name": "Retroperitoneum and peritoneum",
                        "category": "primary_site",
                    },
                ],
                "study_title": "TARGET-NBL",
                "accession_number": "TARGET-NBL",
                "description": "Genomic Data Commons study of ['Neuroepitheliomatous Neoplasms', 'Not Applicable'] in ['Retroperitoneum and peritoneum', 'Lymph nodes']",
                "funding": "",
                "source": "",
                "_subjects_count": 1132,
                "subjects_count": 1132,
                "files_count": 5358,
                "study_metadata": {
                    "minimal_info": {"alternative_study_name": "Neuroblastoma"},
                },
            },
        },
        "GENIE-GRCC": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "id": "GENIE-GRCC",
                "summary": {
                    "file_count": 1038,
                    "case_count": 1038,
                    "file_size": 2642030,
                },
                "primary_site": [
                    "Other and ill-defined sites in lip, oral cavity and pharynx",
                    "Uterus, NOS",
                ],
                "name": "AACR Project GENIE - Contributed by Institut Gustave Roussy",
                "releasable": True,
                "state": "open",
                "released": True,
                "commons": "CRDC Genomic Data Commons",
                "short_name": "GENIE-GRCC",
                "full_name": "AACR Project GENIE - Contributed by Institut Gustave Roussy",
                "disease_type": None,
                "_unique_id": "GENIE-GRCC",
                "tags": [
                    {"name": "", "category": "disease_type"},
                    {
                        "name": "Other and ill-defined sites in lip, oral cavity and pharynx",
                        "category": "primary_site",
                    },
                ],
                "project_id": "GENIE-GRCC",
                "study_title": "GENIE-GRCC",
                "accession_number": "GENIE-GRCC",
                "description": "Genomic Data Commons study of None in ['Other and ill-defined sites in lip, oral cavity and pharynx', 'Uterus, NOS']",
                "funding": "",
                "source": "",
                "dbgap_accession_number": None,
                "_subjects_count": 1038,
                "subjects_count": 1038,
                "files_count": 1038,
                "study_metadata": {
                    "minimal_info": {
                        "alternative_study_name": "AACR Project GENIE - Contributed by Institut Gustave Roussy"
                    },
                },
            },
        },
        "GENIE-DFCI": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "id": "GENIE-DFCI",
                "summary": {
                    "file_count": 28464,
                    "case_count": 14232,
                    "file_size": 410474608,
                },
                "primary_site": ["Nasal cavity and middle ear", "Colon"],
                "dbgap_accession_number": None,
                "project_id": "GENIE-DFCI",
                "disease_type": [
                    "Lymphatic Vessel Tumors",
                    "Granular Cell Tumors and Alveolar Soft Part Sarcomas",
                ],
                "name": "AACR Project GENIE - Contributed by Dana-Farber Cancer Institute",
                "releasable": True,
                "state": "open",
                "released": True,
                "commons": "CRDC Genomic Data Commons",
                "short_name": "GENIE-DFCI",
                "full_name": "AACR Project GENIE - Contributed by Dana-Farber Cancer Institute",
                "_unique_id": "GENIE-DFCI",
                "tags": [
                    {"name": "Lymphatic Vessel Tumors", "category": "disease_type"},
                    {"name": "Nasal cavity and middle ear", "category": "primary_site"},
                ],
                "study_title": "GENIE-DFCI",
                "accession_number": "GENIE-DFCI",
                "description": "Genomic Data Commons study of ['Lymphatic Vessel Tumors', 'Granular Cell Tumors and Alveolar Soft Part Sarcomas'] in ['Nasal cavity and middle ear', 'Colon']",
                "funding": "",
                "source": "",
                "_subjects_count": 14232,
                "subjects_count": 14232,
                "files_count": 28464,
                "study_metadata": {
                    "minimal_info": {
                        "alternative_study_name": "AACR Project GENIE - Contributed by Dana-Farber Cancer Institute"
                    },
                },
            },
        },
    }

    assert get_metadata(
        "gdc", "http://test/ok", filters=filters, mappings=field_mappings
    ) == {
        "TARGET-NBL": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "commons": "CRDC Genomic Data Commons",
                "short_name": "TARGET-NBL",
                "full_name": "Neuroblastoma",
                "disease_type": ["Neuroepitheliomatous Neoplasms", "Not Applicable"],
                "primary_site": ["Retroperitoneum and peritoneum", "Lymph nodes"],
                "_unique_id": "TARGET-NBL",
                "tags": [
                    {
                        "name": "Neuroepitheliomatous Neoplasms",
                        "category": "disease_type",
                    },
                    {
                        "name": "Retroperitoneum and peritoneum",
                        "category": "primary_site",
                    },
                ],
                "project_id": "TARGET-NBL",
                "study_title": "TARGET-NBL",
                "accession_number": "TARGET-NBL",
                "description": "Genomic Data Commons study of ['Neuroepitheliomatous Neoplasms', 'Not Applicable'] in ['Retroperitoneum and peritoneum', 'Lymph nodes']",
                "funding": "",
                "source": "",
                "dbgap_accession_number": "phs000467",
                "_subjects_count": 1132,
                "subjects_count": 1132,
                "files_count": 5358,
                "study_metadata": {
                    "minimal_info": {"alternative_study_name": "Neuroblastoma"},
                },
            },
        },
        "GENIE-GRCC": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "commons": "CRDC Genomic Data Commons",
                "short_name": "GENIE-GRCC",
                "full_name": "AACR Project GENIE - Contributed by Institut Gustave Roussy",
                "disease_type": None,
                "primary_site": [
                    "Other and ill-defined sites in lip, oral cavity and pharynx",
                    "Uterus, NOS",
                ],
                "_unique_id": "GENIE-GRCC",
                "tags": [
                    {"name": "", "category": "disease_type"},
                    {
                        "name": "Other and ill-defined sites in lip, oral cavity and pharynx",
                        "category": "primary_site",
                    },
                ],
                "project_id": "GENIE-GRCC",
                "study_title": "GENIE-GRCC",
                "accession_number": "GENIE-GRCC",
                "description": "Genomic Data Commons study of None in ['Other and ill-defined sites in lip, oral cavity and pharynx', 'Uterus, NOS']",
                "funding": "",
                "source": "",
                "dbgap_accession_number": None,
                "_subjects_count": 1038,
                "subjects_count": 1038,
                "files_count": 1038,
                "study_metadata": {
                    "minimal_info": {
                        "alternative_study_name": "AACR Project GENIE - Contributed by Institut Gustave Roussy"
                    },
                },
            },
        },
        "GENIE-DFCI": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "commons": "CRDC Genomic Data Commons",
                "short_name": "GENIE-DFCI",
                "full_name": "AACR Project GENIE - Contributed by Dana-Farber Cancer Institute",
                "disease_type": [
                    "Lymphatic Vessel Tumors",
                    "Granular Cell Tumors and Alveolar Soft Part Sarcomas",
                ],
                "primary_site": ["Nasal cavity and middle ear", "Colon"],
                "_unique_id": "GENIE-DFCI",
                "tags": [
                    {"name": "Lymphatic Vessel Tumors", "category": "disease_type"},
                    {"name": "Nasal cavity and middle ear", "category": "primary_site"},
                ],
                "project_id": "GENIE-DFCI",
                "study_title": "GENIE-DFCI",
                "accession_number": "GENIE-DFCI",
                "description": "Genomic Data Commons study of ['Lymphatic Vessel Tumors', 'Granular Cell Tumors and Alveolar Soft Part Sarcomas'] in ['Nasal cavity and middle ear', 'Colon']",
                "funding": "",
                "source": "",
                "dbgap_accession_number": None,
                "_subjects_count": 14232,
                "subjects_count": 14232,
                "files_count": 28464,
                "study_metadata": {
                    "minimal_info": {
                        "alternative_study_name": "AACR Project GENIE - Contributed by Dana-Farber Cancer Institute"
                    },
                },
            },
        },
    }
