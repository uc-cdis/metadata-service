import respx
import httpx

from mds.agg_mds.adapters import get_metadata


@respx.mock
def test_get_metadata_icdc():
    json_response = r"""{
   "data":{
      "studiesByProgram":[
         {
            "program_id":"COP",
            "clinical_study_designation":"COTC007B",
            "clinical_study_name":"Preclinical Comparison of Three Indenoisoquinoline Candidates in Tumor-Bearing Dogs",
            "clinical_study_type":"Clinical Trial",
            "numberOfCases":84,
            "numberOfCaseFiles":0,
            "numberOfStudyFiles":0,
            "numberOfImageCollections":0,
            "numberOfPublications":2,
            "accession_id":"000001",
            "study_disposition":"Unrestricted",
            "numberOfCRDCNodes":0,
            "CRDCLinks":[

            ],
            "__typename":"StudyOfProgram"
         },
         {
            "program_id":"CMCP",
            "clinical_study_designation":"GLIOMA01",
            "clinical_study_name":"Comparative Molecular Life History of Spontaneous Canine and Human Gliomas",
            "clinical_study_type":"Genomics",
            "numberOfCases":81,
            "numberOfCaseFiles":858,
            "numberOfStudyFiles":0,
            "numberOfImageCollections":2,
            "numberOfPublications":1,
            "accession_id":"000003",
            "study_disposition":"Unrestricted",
            "numberOfCRDCNodes":2,
            "CRDCLinks":[
               {
                  "text":"ICDC-Glioma01 - TCIA",
                  "url":"https://doi.org/10.7937/TCIA.SVQT-Q016",
                  "__typename":"Link"
               },
               {
                  "text":"ICDC-Glioma (GLIOMA01) - IDC",
                  "url":"https://imaging.datacommons.cancer.gov/explore/?filters_for_load\u003d[{\"filters\":[{\"id\":\"120\",\"values\":[\"icdc_glioma\"]}]}]",
                  "__typename":"Link"
               }
            ],
            "__typename":"StudyOfProgram"
         },
         {
            "program_id":"CMCP",
            "clinical_study_designation":"MGT01",
            "clinical_study_name":"Molecular Homology and Differences Between Spontaneous Canine Mammary Cancer and Human Breast Cancer",
            "clinical_study_type":"Genomics",
            "numberOfCases":13,
            "numberOfCaseFiles":68,
            "numberOfStudyFiles":0,
            "numberOfImageCollections":0,
            "numberOfPublications":2,
            "accession_id":"000007",
            "study_disposition":"Unrestricted",
            "numberOfCRDCNodes":0,
            "CRDCLinks":[

            ],
            "__typename":"StudyOfProgram"
         }
      ]
   }
}"""

    field_mappings = {
        "authz": "/open",
        "tags": [],
        "_unique_id": "path:accession_id",
        "study_id": "path:clinical_study_designation",
        "study_description": "",
        "full_name": "path:clinical_study_name",
        "short_name": "N/A",
        "commons": "CRDC Integrated Canine Data Commons",
        "study_url": "N/A",
        "_subjects_count": {"path": "numberOfCases", "default": 0},
        "commons_url": "nci-crdc.datacommons.io",
    }

    respx.post("http://test/ok").mock(
        return_value=httpx.Response(status_code=200, content=json_response)
    )

    assert get_metadata(
        "icdc", "http://test/ok", filters=None, mappings=field_mappings
    ) == {
        "COTC007B": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "authz": "/open",
                "tags": [],
                "_unique_id": "000001",
                "study_id": "COTC007B",
                "study_description": "",
                "full_name": "Preclinical Comparison of Three Indenoisoquinoline Candidates in Tumor-Bearing Dogs",
                "short_name": "N/A",
                "commons": "CRDC Integrated Canine Data Commons",
                "study_url": "N/A",
                "_subjects_count": 84,
                "commons_url": "nci-crdc.datacommons.io",
            },
        },
        "GLIOMA01": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "authz": "/open",
                "tags": [],
                "_unique_id": "000003",
                "study_id": "GLIOMA01",
                "study_description": "",
                "full_name": "Comparative Molecular Life History of Spontaneous Canine and Human Gliomas",
                "short_name": "N/A",
                "commons": "CRDC Integrated Canine Data Commons",
                "study_url": "N/A",
                "_subjects_count": 81,
                "commons_url": "nci-crdc.datacommons.io",
            },
        },
        "MGT01": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "authz": "/open",
                "tags": [],
                "_unique_id": "000007",
                "study_id": "MGT01",
                "study_description": "",
                "full_name": "Molecular Homology and Differences Between Spontaneous Canine Mammary Cancer and Human Breast Cancer",
                "short_name": "N/A",
                "commons": "CRDC Integrated Canine Data Commons",
                "study_url": "N/A",
                "_subjects_count": 13,
                "commons_url": "nci-crdc.datacommons.io",
            },
        },
    }
