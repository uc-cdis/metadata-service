import respx
import httpx

from mds.agg_mds.adapters import get_metadata


@respx.mock
def test_get_metadata_pdc():
    pid_response = """
      {
        "data": {
          "studyCatalog": [
            {
              "pdc_study_id": "PDC000109"
            },
            {
              "pdc_study_id": "PDC000110"
            },
            {
              "pdc_study_id": "PDC000111"
            }
          ]
        }
      }
    """

    json_response = r"""
{
  "data": {
    "PDC000109": [
      {
        "submitter_id_name": null,
        "study_id": "bb67ec40-57b8-11e8-b07a-00a098d917f8",
        "study_name": "Prospective Colon VU Proteome",
        "study_shortname": "Prospective COAD Proteome S037-1",
        "analytical_fraction": "Proteome",
        "experiment_type": "Label Free",
        "embargo_date": null,
        "acquisition_type": null,
        "cases_count": 100,
        "filesCount": [
          {
            "files_count": 14
          },
          {
            "files_count": 600
          }
        ],
        "disease_type": "Colon Adenocarcinoma",
        "program_name": "Clinical Proteomic Tumor Analysis Consortium",
        "program_id": "10251935-5540-11e8-b664-00a098d917f8",
        "project_name": "CPTAC2 Confirmatory",
        "project_id": "48653303-5546-11e8-b664-00a098d917f8",
        "project_submitter_id": null,
        "primary_site": "Colon",
        "pdc_study_id": "PDC000109"
      }
    ],
    "PDC000110": [
      {
        "submitter_id_name": null,
        "study_id": "6338aad7-851b-4eea-ba90-ba50237cb875",
        "study_name": "Prospective Ovarian JHU Proteome",
        "study_shortname": "Prospective Ovarian JHU Proteome v2",
        "analytical_fraction": "Proteome",
        "experiment_type": "TMT10",
        "embargo_date": null,
        "acquisition_type": null,
        "cases_count": 97,
        "filesCount": [
          {
            "files_count": 13
          },
          {
            "files_count": 312
          }
        ],
        "disease_type": "Other;Ovarian Serous Cystadenocarcinoma",
        "program_name": "Clinical Proteomic Tumor Analysis Consortium",
        "program_id": "10251935-5540-11e8-b664-00a098d917f8",
        "project_name": "CPTAC2 Confirmatory",
        "project_id": "48653303-5546-11e8-b664-00a098d917f8",
        "project_submitter_id": null,
        "primary_site": "Not Reported;Ovary",
        "pdc_study_id": "PDC000110"
      }
    ],
    "PDC000111": [
      {
        "submitter_id_name": null,
        "study_id": "b998098f-57b8-11e8-b07a-00a098d917f8",
        "study_name": "TCGA Colon Cancer Proteome",
        "study_shortname": "TCGA COAD Proteome S016-1",
        "analytical_fraction": "Proteome",
        "experiment_type": "Label Free",
        "embargo_date": null,
        "acquisition_type": null,
        "cases_count": 90,
        "filesCount": [
          {
            "files_count": 5
          },
          {
            "files_count": 1425
          }
        ],
        "disease_type": "Colon Adenocarcinoma;Rectum Adenocarcinoma",
        "program_name": "Clinical Proteomic Tumor Analysis Consortium",
        "program_id": "10251935-5540-11e8-b664-00a098d917f8",
        "project_name": "CPTAC2 Retrospective",
        "project_id": "48af5040-5546-11e8-b664-00a098d917f8",
        "project_submitter_id": null,
        "primary_site": "Colon;Rectum",
        "pdc_study_id": "PDC000111"
      }
    ]
  }
}
    """
    field_mappings = {
        "commons": "CRDC Proteomic Data Commons",
        "_unique_id": "path:pdc_study_id",
        "study_title": "path:pdc_study_id",
        "accession_number": "path:pdc_study_id",
        "short_name": "path:study_shortname",
        "full_name": "path:study_name",
        "disease_type": "path:disease_type",
        "primary_site": "path:primary_site",
        "analytical_fraction": "path:analytical_fraction",
        "experiment_type": "path:experiment_type",
        "cases_count": "path:cases_count",
        "program_name": "path:program_name",
        "project_name": "path:project_name",
        "description": "",
        "files_count": {"path": "filesCount", "filters": ["aggregate_pdc_file_count"]},
        "tags": [],
    }

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
                for study_id in ["PDC000109", "PDC000110", "PDC000111"]
            ]
        )
        + "}"
    )
    respx.post(
        "http://test/ok",
        json={"query": "{studyCatalog(acceptDUA: true){pdc_study_id}}"},
    ).mock(return_value=httpx.Response(status_code=200, content=pid_response))

    respx.post(
        "http://test/ok",
        json={"query": subject_query_string},
    ).mock(return_value=httpx.Response(status_code=200, content=json_response))

    filters = {"size": 5}

    assert get_metadata("pdc", None, filters=filters, mappings=field_mappings) == {}

    assert get_metadata(
        "pdc", "http://test/ok", filters=filters, mappings=field_mappings
    ) == {
        "PDC000109": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "commons": "CRDC Proteomic Data Commons",
                "_unique_id": "PDC000109",
                "study_title": "PDC000109",
                "accession_number": "PDC000109",
                "short_name": "Prospective COAD Proteome S037-1",
                "full_name": "Prospective Colon VU Proteome",
                "disease_type": "Colon Adenocarcinoma",
                "primary_site": "Colon",
                "analytical_fraction": "Proteome",
                "experiment_type": "Label Free",
                "cases_count": 100,
                "program_name": "Clinical Proteomic Tumor Analysis Consortium",
                "project_name": "CPTAC2 Confirmatory",
                "description": "",
                "files_count": 614,
                "tags": [
                    {"name": "Colon Adenocarcinoma", "category": "disease_type"},
                    {"name": "Colon", "category": "primary_site"},
                ],
            },
        },
        "PDC000110": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "commons": "CRDC Proteomic Data Commons",
                "_unique_id": "PDC000110",
                "study_title": "PDC000110",
                "accession_number": "PDC000110",
                "short_name": "Prospective Ovarian JHU Proteome v2",
                "full_name": "Prospective Ovarian JHU Proteome",
                "disease_type": "Other;Ovarian Serous Cystadenocarcinoma",
                "primary_site": "Not Reported;Ovary",
                "analytical_fraction": "Proteome",
                "experiment_type": "TMT10",
                "cases_count": 97,
                "program_name": "Clinical Proteomic Tumor Analysis Consortium",
                "project_name": "CPTAC2 Confirmatory",
                "description": "",
                "files_count": 325,
                "tags": [
                    {
                        "name": "Other;Ovarian Serous Cystadenocarcinoma",
                        "category": "disease_type",
                    },
                    {"name": "Not Reported;Ovary", "category": "primary_site"},
                ],
            },
        },
        "PDC000111": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "commons": "CRDC Proteomic Data Commons",
                "_unique_id": "PDC000111",
                "study_title": "PDC000111",
                "accession_number": "PDC000111",
                "short_name": "TCGA COAD Proteome S016-1",
                "full_name": "TCGA Colon Cancer Proteome",
                "disease_type": "Colon Adenocarcinoma;Rectum Adenocarcinoma",
                "primary_site": "Colon;Rectum",
                "analytical_fraction": "Proteome",
                "experiment_type": "Label Free",
                "cases_count": 90,
                "program_name": "Clinical Proteomic Tumor Analysis Consortium",
                "project_name": "CPTAC2 Retrospective",
                "description": "",
                "files_count": 1430,
                "tags": [
                    {
                        "name": "Colon Adenocarcinoma;Rectum Adenocarcinoma",
                        "category": "disease_type",
                    },
                    {"name": "Colon;Rectum", "category": "primary_site"},
                ],
            },
        },
    }

    assert get_metadata(
        "pdc",
        "http://test/ok",
        filters=filters,
        mappings=field_mappings,
        keepOriginalFields=True,
    ) == {
        "PDC000109": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "submitter_id_name": None,
                "study_id": "bb67ec40-57b8-11e8-b07a-00a098d917f8",
                "study_name": "Prospective Colon VU Proteome",
                "study_shortname": "Prospective COAD Proteome S037-1",
                "analytical_fraction": "Proteome",
                "experiment_type": "Label Free",
                "embargo_date": None,
                "acquisition_type": None,
                "cases_count": 100,
                "filesCount": [{"files_count": 14}, {"files_count": 600}],
                "disease_type": "Colon Adenocarcinoma",
                "program_name": "Clinical Proteomic Tumor Analysis Consortium",
                "program_id": "10251935-5540-11e8-b664-00a098d917f8",
                "project_name": "CPTAC2 Confirmatory",
                "project_id": "48653303-5546-11e8-b664-00a098d917f8",
                "project_submitter_id": None,
                "primary_site": "Colon",
                "pdc_study_id": "PDC000109",
                "commons": "CRDC Proteomic Data Commons",
                "_unique_id": "PDC000109",
                "study_title": "PDC000109",
                "accession_number": "PDC000109",
                "short_name": "Prospective COAD Proteome S037-1",
                "full_name": "Prospective Colon VU Proteome",
                "description": "",
                "files_count": 614,
                "tags": [
                    {"name": "Colon Adenocarcinoma", "category": "disease_type"},
                    {"name": "Colon", "category": "primary_site"},
                ],
            },
        },
        "PDC000110": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "submitter_id_name": None,
                "study_id": "6338aad7-851b-4eea-ba90-ba50237cb875",
                "study_name": "Prospective Ovarian JHU Proteome",
                "study_shortname": "Prospective Ovarian JHU Proteome v2",
                "analytical_fraction": "Proteome",
                "experiment_type": "TMT10",
                "embargo_date": None,
                "acquisition_type": None,
                "cases_count": 97,
                "filesCount": [{"files_count": 13}, {"files_count": 312}],
                "disease_type": "Other;Ovarian Serous Cystadenocarcinoma",
                "program_name": "Clinical Proteomic Tumor Analysis Consortium",
                "program_id": "10251935-5540-11e8-b664-00a098d917f8",
                "project_name": "CPTAC2 Confirmatory",
                "project_id": "48653303-5546-11e8-b664-00a098d917f8",
                "project_submitter_id": None,
                "primary_site": "Not Reported;Ovary",
                "pdc_study_id": "PDC000110",
                "commons": "CRDC Proteomic Data Commons",
                "_unique_id": "PDC000110",
                "study_title": "PDC000110",
                "accession_number": "PDC000110",
                "short_name": "Prospective Ovarian JHU Proteome v2",
                "full_name": "Prospective Ovarian JHU Proteome",
                "description": "",
                "files_count": 325,
                "tags": [
                    {
                        "name": "Other;Ovarian Serous Cystadenocarcinoma",
                        "category": "disease_type",
                    },
                    {"name": "Not Reported;Ovary", "category": "primary_site"},
                ],
            },
        },
        "PDC000111": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "submitter_id_name": None,
                "study_id": "b998098f-57b8-11e8-b07a-00a098d917f8",
                "study_name": "TCGA Colon Cancer Proteome",
                "study_shortname": "TCGA COAD Proteome S016-1",
                "analytical_fraction": "Proteome",
                "experiment_type": "Label Free",
                "embargo_date": None,
                "acquisition_type": None,
                "cases_count": 90,
                "filesCount": [{"files_count": 5}, {"files_count": 1425}],
                "disease_type": "Colon Adenocarcinoma;Rectum Adenocarcinoma",
                "program_name": "Clinical Proteomic Tumor Analysis Consortium",
                "program_id": "10251935-5540-11e8-b664-00a098d917f8",
                "project_name": "CPTAC2 Retrospective",
                "project_id": "48af5040-5546-11e8-b664-00a098d917f8",
                "project_submitter_id": None,
                "primary_site": "Colon;Rectum",
                "pdc_study_id": "PDC000111",
                "commons": "CRDC Proteomic Data Commons",
                "_unique_id": "PDC000111",
                "study_title": "PDC000111",
                "accession_number": "PDC000111",
                "short_name": "TCGA COAD Proteome S016-1",
                "full_name": "TCGA Colon Cancer Proteome",
                "description": "",
                "files_count": 1430,
                "tags": [
                    {
                        "name": "Colon Adenocarcinoma;Rectum Adenocarcinoma",
                        "category": "disease_type",
                    },
                    {"name": "Colon;Rectum", "category": "primary_site"},
                ],
            },
        },
    }

    respx.post(
        "http://test/ok",
        json={"query": "{studyCatalog(acceptDUA: true){pdc_study_id}}"},
    ).mock(side_effect=httpx.TimeoutException)

    assert (
        get_metadata("pdc", "http://test/ok", filters=filters, mappings=field_mappings)
        == {}
    )

    respx.post(
        "http://test/ok",
        json={"query": "{studyCatalog(acceptDUA: true){pdc_study_id}}"},
    ).mock(side_effect=httpx.HTTPError("This is a HTTP Error"))

    assert (
        get_metadata("pdc", "http://test/ok", filters=filters, mappings=field_mappings)
        == {}
    )

    respx.post(
        "http://test/ok",
        json={"query": "{studyCatalog(acceptDUA: true){pdc_study_id}}"},
    ).mock(side_effect=Exception)

    assert (
        get_metadata("pdc", "http://test/ok", filters=filters, mappings=field_mappings)
        == {}
    )
