import respx
import httpx

from mds.agg_mds.adapters import get_metadata


@respx.mock
def test_get_metadata_tcia():
    pid_response = """
    [
        {
            "StudyInstanceUID": "study_id_1",
            "StudyDate": "",
            "StudyDescription": "",
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
            "StudyDescription": "",
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

    field_mappings = {
        "commons": "TCIA",
        "study_title": "path:StudyDescription",
        "program_name": "path:Collection",
        "description": "",
        "tags": [],
    }

    respx.post(
        "http://test/ok",
    ).mock(return_value=httpx.Response(status_code=200, content=pid_response))

    filters = {"size": 5}

    assert get_metadata("pdc", None, filters=filters, mappings=field_mappings) == {}

    assert get_metadata(
        "tcia", "http://test/ok", filters=filters, mappings=field_mappings
    ) == {
        "data": {
            "study_id_1": [
                {
                    "StudyInstanceUID": "study_id_1",
                    "StudyDate": "",
                    "StudyDescription": "",
                    "PatientAge": "",
                    "PatientID": "",
                    "PatientName": "",
                    "PatientSex": "",
                    "EthnicGroup": "",
                    "Collection": "Collection1",
                    "SeriesCount": 1,
                    "LongitudinalTemporalEventType": "",
                    "LongitudinalTemporalOffsetFromEvent": 0,
                }
            ],
            "study_id_2": [
                {
                    "StudyInstanceUID": "study_id_1",
                    "StudyDate": "",
                    "StudyDescription": "",
                    "PatientAge": "",
                    "PatientID": "",
                    "PatientName": "",
                    "PatientSex": "",
                    "EthnicGroup": "",
                    "Collection": "Collection2",
                    "SeriesCount": 2,
                    "LongitudinalTemporalEventType": "",
                    "LongitudinalTemporalOffsetFromEvent": 1,
                }
            ],
        }
    }
