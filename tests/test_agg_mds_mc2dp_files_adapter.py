import respx
import httpx
from tenacity import wait_none

from mds.agg_mds.adapters import Gen3Adapter, get_metadata


@respx.mock
def test_get_metadata_mc2dp_files():
    mc2dp_files_response = """
    {
        "MC-0C2123": {
          "_guid_type": "aggmds_metadata_entry",
          "gen3_discovery": {
            "race": "white",
            "tags": [
              {
                "name": "male",
                "category": "biological_sex",
                "data_source": "VPODC"
              },
              {
                "name": "lung, lower lobe cancer",
                "category": "primary_disease",
                "data_source": "VPODC"
              },
              {
                "name": "squamous cell carcinoma, nos",
                "category": "cancer_type",
                "data_source": "VPODC"
              },
              {
                "name": "no",
                "category": "metastasis",
                "data_source": "VPODC"
              },
              {
                "name": "white",
                "category": "race",
                "data_source": "VPODC"
              },
              {
                "name": "not hispanic or latino",
                "category": "ethnicity",
                "data_source": "VPODC"
              },
              {
                "name": "II",
                "category": "cancer_stage",
                "data_source": "VPODC"
              },
              {
                "name": "GRADE II",
                "category": "cancer_grade",
                "data_source": "VPODC"
              },
              {
                "name": "VA-PODR-COHORT-A",
                "source": "VPODC",
                "category": "data_source_project"
              }
            ],
            "data_type": [
              {
                "file_name": "C1077452144_1_R2.fastq.gz",
                "file_subtype": null,
                "file_data_size": 1211846784.0,
                "file_data_type": "unaligned_reads_file",
                "file_permission": "Controlled",
                "mc2dp_global_id": "MC-0C2123",
                "file_data_source": "VPODC",
                "file_download_url": "https://vpodc.data-commons.org/files/dg.F738/beaa17b2-2321-4ed5-96bc-66a52c71fc0d",
                "file_data_source_id": "AP-C5DU",
                "file_number_of_files": "1",
                "file_unique_identifier": "dg.F738/beaa17b2-2321-4ed5-96bc-66a52c71fc0d",
                "file_data_source_project": "VA-PODR-COHORT-A"
              },
              {
                "file_name": "C1077452144_1_R1.fastq.gz",
                "file_subtype": null,
                "file_data_size": 1182066994.0,
                "file_data_type": "unaligned_reads_file",
                "file_permission": "Controlled",
                "mc2dp_global_id": "MC-0C2123",
                "file_data_source": "VPODC",
                "file_download_url": "https://vpodc.data-commons.org/files/dg.F738/2aa79e81-defd-409a-a1b8-aaa1bcde1688",
                "file_data_source_id": "AP-C5DU",
                "file_number_of_files": "1",
                "file_unique_identifier": "dg.F738/2aa79e81-defd-409a-a1b8-aaa1bcde1688",
                "file_data_source_project": "VA-PODR-COHORT-A"
              },
              {
                "file_name": "C1077452144.jpg",
                "file_subtype": null,
                "file_data_size": 123439.0,
                "file_data_type": "pathology_slide",
                "file_permission": "Controlled",
                "mc2dp_global_id": "MC-0C2123",
                "file_data_source": "VPODC",
                "file_download_url": "https://vpodc.data-commons.org/files/dg.F738/98e93d3d-790c-44d2-a166-e4a81c455fc2",
                "file_data_source_id": "AP-C5DU",
                "file_number_of_files": "1",
                "file_unique_identifier": "dg.F738/98e93d3d-790c-44d2-a166-e4a81c455fc2",
                "file_data_source_project": "VA-PODR-COHORT-A"
              },
              {
                "file_name": "C1077452144_1.jpg",
                "file_subtype": null,
                "file_data_size": 2421356.0,
                "file_data_type": "pathology_slide",
                "file_permission": "Controlled",
                "mc2dp_global_id": "MC-0C2123",
                "file_data_source": "VPODC",
                "file_download_url": "https://vpodc.data-commons.org/files/dg.F738/75be7922-03bd-455b-8010-38ca77c98041",
                "file_data_source_id": "AP-C5DU",
                "file_number_of_files": "1",
                "file_unique_identifier": "dg.F738/75be7922-03bd-455b-8010-38ca77c98041",
                "file_data_source_project": "VA-PODR-COHORT-A"
              }
            ],
            "ethnicity": "not hispanic or latino",
            "treatment": null,
            "metabolite": null,
            "metastasis": "no",
            "cancer_type": "squamous cell carcinoma, nos",
            "data_source": "VPODC",
            "cancer_grade": "GRADE II",
            "cancer_stage": "II",
            "is_synthetic": null,
            "is_data_files": "True",
            "biological_sex": "male",
            "tumor_mutation": null,
            "mc2dp_global_id": "MC-0C2123",
            "primary_disease": "lung, lower lobe cancer",
            "data_source_project": "VA-PODR-COHORT-A",
            "values_of_exposures": null,
            "data_source_ids_array": [
              {
                "id_type": "UCHICAGO",
                "id_value": "AP-C5DU"
              }
            ],
            "type_of_exposures_array": [],
            "values_of_data_source_ids": "AP-C5DU"
          }
        },
        "MC-0C36E1": {
          "_guid_type": "aggmds_metadata_entry",
          "gen3_discovery": {
            "race": "white",
            "tags": [
              {
                "name": "male",
                "category": "biological_sex",
                "data_source": "VPODC"
              },
              {
                "name": "pituitary cancer",
                "category": "primary_disease",
                "data_source": "VPODC"
              },
              {
                "name": "pituitary adenoma, nos",
                "category": "cancer_type",
                "data_source": "VPODC"
              },
              {
                "name": "no",
                "category": "metastasis",
                "data_source": "VPODC"
              },
              {
                "name": "white",
                "category": "race",
                "data_source": "VPODC"
              },
              {
                "name": "not hispanic or latino",
                "category": "ethnicity",
                "data_source": "VPODC"
              },
              {
                "name": "UNKNOWN",
                "category": "cancer_grade",
                "data_source": "VPODC"
              },
              {
                "name": "VA-PODR-COHORT-A",
                "source": "VPODC",
                "category": "data_source_project"
              }
            ],
            "data_type": [
              {
                "file_name": "C1169933484_1.jpg",
                "file_subtype": null,
                "file_data_size": 524228.0,
                "file_data_type": "pathology_slide",
                "file_permission": "Controlled",
                "mc2dp_global_id": "MC-0C36E1",
                "file_data_source": "VPODC",
                "file_download_url": "https://vpodc.data-commons.org/files/dg.F738/16c3586c-7f9e-4e21-b54f-9c4246001ba5",
                "file_data_source_id": "AP-X39F",
                "file_number_of_files": "1",
                "file_unique_identifier": "dg.F738/16c3586c-7f9e-4e21-b54f-9c4246001ba5",
                "file_data_source_project": "VA-PODR-COHORT-A"
              }
            ],
            "ethnicity": "not hispanic or latino",
            "treatment": null,
            "metabolite": null,
            "metastasis": "no",
            "cancer_type": "pituitary adenoma, nos",
            "data_source": "VPODC",
            "cancer_grade": "UNKNOWN",
            "cancer_stage": null,
            "is_synthetic": null,
            "is_data_files": "True",
            "biological_sex": "male",
            "tumor_mutation": null,
            "mc2dp_global_id": "MC-0C36E1",
            "primary_disease": "pituitary cancer",
            "data_source_project": "VA-PODR-COHORT-A",
            "values_of_exposures": null,
            "data_source_ids_array": [
              {
                "id_type": "UCHICAGO",
                "id_value": "AP-X39F"
              }
            ],
            "type_of_exposures_array": [],
            "values_of_data_source_ids": "AP-X39F"
          }
        }
    }
    """

    field_mappings_files = {
        "file_name": "path:file_name",
        "file_subtype": "path:file_subtype",
        "file_data_size": "path:file_data_size",
        "file_data_type": "path:file_data_type",
        "file_permission": "path:file_permission",
        "mc2dp_global_id": "path:mc2dp_global_id",
        "file_data_source": "path:file_data_source",
        "file_download_url": "path:file_download_url",
        "file_data_source_id": "path:file_data_source_id",
        "file_number_of_files": "path:file_number_of_files",
        "file_unique_identifier": "path:file_unique_identifier",
        "file_data_source_project": "path:file_data_source_project",
    }

    # don't sleep between tenacity retries
    Gen3Adapter.getRemoteDataAsJson.retry.wait = wait_none()

    respx.get("http://test/ok/mds/metadata").mock(side_effect=httpx.TimeoutException)
    assert (
        get_metadata(
            "mc2dpFiles",
            "http://test/ok",
            filters=None,
            mappings=field_mappings_files,
        )
        == {}
    )

    filters = {"size": 5}
    respx.get(
        "http://test/ok/mds/metadata",
    ).mock(return_value=httpx.Response(status_code=200, content=mc2dp_files_response))
    assert get_metadata(
        "mc2dpFiles",
        "http://test/ok",
        filters=filters,
        mappings=field_mappings_files,
    ) == {
        "dg.F738/beaa17b2-2321-4ed5-96bc-66a52c71fc0d": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "file_name": "C1077452144_1_R2.fastq.gz",
                "file_subtype": None,
                "file_data_size": 1211846784.0,
                "file_data_type": "unaligned_reads_file",
                "file_permission": "Controlled",
                "mc2dp_global_id": "MC-0C2123",
                "file_data_source": "VPODC",
                "file_download_url": "https://vpodc.data-commons.org/files/dg.F738/beaa17b2-2321-4ed5-96bc-66a52c71fc0d",
                "file_data_source_id": "AP-C5DU",
                "file_number_of_files": "1",
                "file_unique_identifier": "dg.F738/beaa17b2-2321-4ed5-96bc-66a52c71fc0d",
                "file_data_source_project": "VA-PODR-COHORT-A",
            },
        },
        "dg.F738/2aa79e81-defd-409a-a1b8-aaa1bcde1688": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "file_name": "C1077452144_1_R1.fastq.gz",
                "file_subtype": None,
                "file_data_size": 1182066994.0,
                "file_data_type": "unaligned_reads_file",
                "file_permission": "Controlled",
                "mc2dp_global_id": "MC-0C2123",
                "file_data_source": "VPODC",
                "file_download_url": "https://vpodc.data-commons.org/files/dg.F738/2aa79e81-defd-409a-a1b8-aaa1bcde1688",
                "file_data_source_id": "AP-C5DU",
                "file_number_of_files": "1",
                "file_unique_identifier": "dg.F738/2aa79e81-defd-409a-a1b8-aaa1bcde1688",
                "file_data_source_project": "VA-PODR-COHORT-A",
            },
        },
        "dg.F738/98e93d3d-790c-44d2-a166-e4a81c455fc2": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "file_name": "C1077452144.jpg",
                "file_subtype": None,
                "file_data_size": 123439.0,
                "file_data_type": "pathology_slide",
                "file_permission": "Controlled",
                "mc2dp_global_id": "MC-0C2123",
                "file_data_source": "VPODC",
                "file_download_url": "https://vpodc.data-commons.org/files/dg.F738/98e93d3d-790c-44d2-a166-e4a81c455fc2",
                "file_data_source_id": "AP-C5DU",
                "file_number_of_files": "1",
                "file_unique_identifier": "dg.F738/98e93d3d-790c-44d2-a166-e4a81c455fc2",
                "file_data_source_project": "VA-PODR-COHORT-A",
            },
        },
        "dg.F738/75be7922-03bd-455b-8010-38ca77c98041": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "file_name": "C1077452144_1.jpg",
                "file_subtype": None,
                "file_data_size": 2421356.0,
                "file_data_type": "pathology_slide",
                "file_permission": "Controlled",
                "mc2dp_global_id": "MC-0C2123",
                "file_data_source": "VPODC",
                "file_download_url": "https://vpodc.data-commons.org/files/dg.F738/75be7922-03bd-455b-8010-38ca77c98041",
                "file_data_source_id": "AP-C5DU",
                "file_number_of_files": "1",
                "file_unique_identifier": "dg.F738/75be7922-03bd-455b-8010-38ca77c98041",
                "file_data_source_project": "VA-PODR-COHORT-A",
            },
        },
        "dg.F738/16c3586c-7f9e-4e21-b54f-9c4246001ba5": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "file_name": "C1169933484_1.jpg",
                "file_subtype": None,
                "file_data_size": 524228.0,
                "file_data_type": "pathology_slide",
                "file_permission": "Controlled",
                "mc2dp_global_id": "MC-0C36E1",
                "file_data_source": "VPODC",
                "file_download_url": "https://vpodc.data-commons.org/files/dg.F738/16c3586c-7f9e-4e21-b54f-9c4246001ba5",
                "file_data_source_id": "AP-X39F",
                "file_number_of_files": "1",
                "file_unique_identifier": "dg.F738/16c3586c-7f9e-4e21-b54f-9c4246001ba5",
                "file_data_source_project": "VA-PODR-COHORT-A",
            },
        },
    }
