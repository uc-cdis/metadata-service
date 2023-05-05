from more_itertools import side_effect
import respx
import asyncio
from mds.agg_mds.adapters import (
    get_metadata,
    get_json_path_value,
    strip_email,
    strip_html,
    add_icpsr_source_url,
    FieldFilters,
    get_json_path_value,
)
import httpx


def test_filters_with_bad_entries():
    assert strip_email(100) == 100
    assert strip_html(99) == 99
    assert add_icpsr_source_url(77) == 77


def test_non_existing_filters():
    assert FieldFilters().execute("nofilter", "passthru") == "passthru"


def test_json_path():
    assert get_json_path_value(None, {}) is None
    assert get_json_path_value("shark", {"shark": ["great", "white"]}) == [
        "great",
        "white",
    ]


@respx.mock
def test_drs_indexd():
    json_data = [
        {
            "hints": [".*dg\\.XXTS.*"],
            "host": "https://mytest1.commons.io/",
            "name": "DataSTAGE",
            "type": "indexd",
        },
        {
            "hints": [".*dg\\.TSXX.*"],
            "host": "https://commons2.io/index/",
            "name": "Environmental DC",
            "type": "indexd",
        },
    ]

    expected = {
        "info": {"created": "07/07/2022 15:28:46:UTC"},
        "cache": {
            "dg.XXTS": {
                "host": "mytest1.commons.io",
                "name": "DataSTAGE",
                "type": "indexd",
            },
            "dg.TSXX": {
                "host": "commons2.io",
                "name": "Environmental DC",
                "type": "indexd",
            },
        },
    }

    respx.get("http://test/index/_dist").mock(
        return_value=httpx.Response(
            status_code=200,
            json=json_data,
        )
    )

    results = get_metadata(
        "drs_indexd",
        "http://test",
        filters=None,
    )

    assert results["cache"] == expected["cache"]

    respx.get("http://test/index/_dist").mock(
        return_value=httpx.Response(
            status_code=404,
            json=None,
        )
    )

    results = get_metadata(
        "drs_indexd",
        "http://test",
        filters=None,
    )

    assert results == {"results": {}}


def test_missing_adapter():
    try:
        get_metadata("notAKnownAdapter", "http://test/ok/", None, None)
    except Exception as err:
        assert isinstance(err, ValueError) == True


def test_json_path_expression():
    sample1 = {
        "study1": {
            "study_description_summary": "This is a summary",
            "id": "2334.5.555ad",
            "contributors": ["Bilbo Baggins"],
            "datasets": ["results1.csv", "results2.csv", "results3.csv"],
        },
        "study3": {
            "study_description_summary": "This is another summary",
            "id": "333.33222.ad",
            "datasets": ["results4.csv", "results5.csv", "results6.csv"],
        },
    }

    assert (
        get_json_path_value("study1.study_description_summary", sample1)
        == "This is a summary"
    )

    # test non existent path
    assert get_json_path_value("study2.study_description_summary", sample1) is None

    # test bad path
    assert get_json_path_value(".contributors", sample1) is None

    # test single array
    assert get_json_path_value("study1.contributors", sample1) == ["Bilbo Baggins"]

    # test array whose length is greater than 1
    assert get_json_path_value("*.datasets", sample1) == [
        ["results1.csv", "results2.csv", "results3.csv"],
        ["results4.csv", "results5.csv", "results6.csv"],
    ]
