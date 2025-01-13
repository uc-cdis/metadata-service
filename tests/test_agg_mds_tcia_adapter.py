import respx
import httpx

from mds.agg_mds.adapters import get_metadata


@respx.mock
def test_get_metadata_tcia():
    tcia_response_study = """
    [
        {
            "StudyInstanceUID": "study_id_1",
            "StudyDate": "",
            "StudyDescription": "Collection One.",
            "PatientAge": "",
            "PatientID": "",
            "PatientName": "",
            "PatientSex": "",
            "EthnicGroup": "",
            "Collection": "Collection1",
            "SeriesCount": 1,
            "LongitudinalTemporalEventType": "",
            "LongitudinalTemporalOffsetFromEvent": 0
        },
        {
            "StudyInstanceUID": "study_id_2",
            "StudyDate": "",
            "StudyDescription": "Collection Two.",
            "PatientAge": "",
            "PatientID": "",
            "PatientName": "",
            "PatientSex": "",
            "EthnicGroup": "",
            "Collection": "Collection2",
            "SeriesCount": 2,
            "LongitudinalTemporalEventType": "",
            "LongitudinalTemporalOffsetFromEvent": 1
        }
    ]
    """

    field_mappings_study = {
        "_unique_id": "path:StudyInstanceUID",
        "commons": "TCIA",
        "study_title": "path:StudyDescription",
        "program_name": "path:Collection",
        "description": "",
        "tags": [],
    }

    tcia_response_series = """
    [
        {
            "SeriesInstanceUID": "series_id_1",
            "StudyInstanceUID": "study_id_1",
            "Modality": "A",
            "ProtocolName": "",
            "SeriesDate": "1970-01-01 00:00:00.0",
            "SeriesDescription": "",
            "SeriesNumber": 1,
            "Collection": "Collection1",
            "PatientID": "",
            "Manufacturer": "",
            "ManufacturerModelName": "",
            "ImageCount": 10,
            "TimeStamp": "1970-01-01 00:00:00.0",
            "LicenseName": "",
            "LicenseURI": "",
            "CollectionURI": "",
            "FileSize": 100,
            "DateReleased": "1970-01-01 00:00:00.0",
            "StudyDesc": "",
            "StudyDate": "1970-01-01 00:00:00.0",
            "ThirdPartyAnalysis": ""
        },
        {
            "SeriesInstanceUID": "series_id_2",
            "StudyInstanceUID": "study_id_2",
            "Modality": "B",
            "ProtocolName": "",
            "SeriesDate": "1970-01-01 10:00:00.0",
            "SeriesDescription": "",
            "BodyPartExamined": "",
            "SeriesNumber": 2,
            "Collection": "Collection2",
            "PatientID": "",
            "Manufacturer": "",
            "ManufacturerModelName": "",
            "SoftwareVersions": "",
            "ImageCount": 20,
            "TimeStamp": "1970-01-01 10:00:00.0",
            "LicenseName": "",
            "LicenseURI": "",
            "CollectionURI": "",
            "FileSize": 200,
            "DateReleased": "1970-01-01 10:00:00.0",
            "StudyDesc": "",
            "StudyDate": "1970-01-01 10:00:00.0",
            "ThirdPartyAnalysis": ""
        }
    ]
    """

    field_mappings_series = {
        "_unique_id": "path:SeriesInstanceUID",
        "study_id": "path:StudyInstanceUID",
        "commons": "TCIA",
        "study_title": "path:SeriesDescription",
        "program_name": "path:Collection",
        "image_count": "path:ImageCount",
        "description": "",
        "tags": [],
    }

    respx.get("http://test/ok").mock(side_effect=httpx.HTTPError)
    assert (
        get_metadata(
            "tcia", "http://test/ok", filters=None, mappings=field_mappings_study
        )
        == {}
    )

    respx.get("http://test/ok").mock(side_effect=Exception)
    assert (
        get_metadata(
            "tcia", "http://test/ok", filters=None, mappings=field_mappings_study
        )
        == {}
    )

    respx.get(
        "http://test/ok",
    ).mock(return_value=httpx.Response(status_code=200, content=tcia_response_study))

    filters = {"size": 5}

    assert get_metadata(
        "tcia", "http://test/ok", filters=filters, mappings=field_mappings_study
    ) == {
        "study_id_1": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "_unique_id": "study_id_1",
                "commons": "TCIA",
                "description": "TCIA data from collection: Collection1.",
                "program_name": "Collection1",
                "study_title": "Collection One.",
                "tags": [{"category": "program_name", "name": "Collection1"}],
            },
        },
        "study_id_2": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "_unique_id": "study_id_2",
                "commons": "TCIA",
                "description": "TCIA data from collection: Collection2.",
                "program_name": "Collection2",
                "study_title": "Collection Two.",
                "tags": [{"category": "program_name", "name": "Collection2"}],
            },
        },
    }

    respx.get(
        "http://test/ok",
    ).mock(return_value=httpx.Response(status_code=200, content=tcia_response_series))

    assert get_metadata(
        "tcia", "http://test/ok", filters=filters, mappings=field_mappings_series
    ) == {
        "series_id_1": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "_unique_id": "series_id_1",
                "study_id": "study_id_1",
                "commons": "TCIA",
                "study_title": "",
                "program_name": "Collection1",
                "image_count": "10",
                "description": "TCIA data from collection: Collection1.",
                "tags": [{"category": "program_name", "name": "Collection1"}],
            },
        },
        "series_id_2": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "_unique_id": "series_id_2",
                "study_id": "study_id_2",
                "commons": "TCIA",
                "study_title": "",
                "program_name": "Collection2",
                "image_count": "20",
                "description": "TCIA data from collection: Collection2.",
                "tags": [{"category": "program_name", "name": "Collection2"}],
            },
        },
    }
