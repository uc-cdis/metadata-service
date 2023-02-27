import datetime
import pytest

from unittest.mock import patch, Mock
from mds import config

DETERMINISTIC_START_TIME = datetime.datetime(1970, 1, 1)  # Unix epoch


def reset_time():
    """
    Reset time returned by mocked_utcnow to DETERMINISTIC_START_TIME.
    Used for resetting mocked time between test runs.
    """
    global TIME_DELTA
    TIME_DELTA = datetime.timedelta(days=-1)


def mocked_utcnow():
    """
    Begin at DETERMINISTIC_START_TIME and increment one day every time this function is called.
    Mocks datetime.datetime.utcnow(), which is used as default `created_date` in `models` module.
    Used for mocking endpoints which return `created_date` attribute.
    """
    global DETERMINISTIC_START_TIME, TIME_DELTA
    TIME_DELTA += datetime.timedelta(days=1)
    return DETERMINISTIC_START_TIME + TIME_DELTA


@pytest.mark.parametrize(
    "key", ["test_semi_structured", "dg.1234/test_semi_structured"]
)
@pytest.mark.dependency(name="test_delete")
def test_delete(client, key):
    """
    Try to DELETE a semi-structed data record.
    Ensure 404 if record doesn't exist or trying to delete metadata record.
    """
    client.post(f"/semi-structured/{key}", json={}).raise_for_status()

    resp = client.delete(f"/semi-structured/{key}")
    assert resp.status_code == 204

    resp = client.delete(f"/semi-structured/{key}")
    assert resp.status_code == 404

    client.post(f"/metadata/{key}", json={}).raise_for_status()
    try:
        resp = client.delete(f"/semi-structured/{key}")
        assert resp.status_code == 404
    finally:
        client.delete(f"/metadata/{key}")


@pytest.mark.parametrize(
    "key,guid,guid_type",
    [
        ("test_semi_structured", None, None),
        ("dg.1234/test_semi_structured", None, None),
        ("dg.2345/test_semi_structured", "dg.2345/test_semi_structured", None),
        ("dg.3456/test_semi_structured", "anything else", None),
        ("dg.4567/test_semi_structured", None, "semi-structured"),
        ("dg.5678/test_semi_structured", None, "anything else"),
    ],
)
@pytest.mark.parametrize("is_post", [True, False])
@pytest.mark.dependency(depends=["test_delete"])
def test_create_get(client, key, guid, guid_type, is_post):
    """
    Create a semi-structed data record with either POST or PUT.
    Ensure that if either guid or guid_type are specified in data that they are set appropriately.
    Ensure that able to GET newly created record.
    Ensure that duplicated creation causes 409 and doesn't alter existing record.
    """

    data = dict(a=1, b=2)
    if guid is not None:
        data["guid"] = guid
    if guid_type is not None:
        data["guid_type"] = guid_type

    expected_resp = {
        "guid": key,
        "guid_type": "semi-structured",
    } | data

    if is_post:
        resp = client.post(f"/semi-structured/{key}", json=data)
    else:
        # use PUT instead of POST, should result in same behavior for new keys
        resp = client.put(f"/semi-structured/{key}", json=data)

    if (guid and guid != key) or (guid_type and guid_type != "semi-structured"):
        assert resp.status_code == 400
    else:
        assert resp.status_code == 201
        try:
            assert resp.json() == expected_resp

            resp = client.post(f"/semi-structured/{key}", json=data)
            assert resp.status_code == 409

            # check that GET works + that 409 didn't alter existing record
            resp = client.get(f"/semi-structured/{key}")
            assert resp.status_code == 200
            assert resp.json() == expected_resp

        finally:
            client.delete(f"/semi-structured/{key}").raise_for_status()


@pytest.mark.parametrize(
    "old_key,new_key",
    [
        ("test_semi_structured", None),
        ("test_semi_structured", "test_semi_structured"),
        ("dg.1234/test_semi_structured", "dg.2345/test_semi_structured"),
    ],
)
@pytest.mark.dependency(depends=["test_delete"])
def test_update(client, old_key, new_key):
    """
    Create a semi-structed data record with POST then try to create new updated record with PUT.
    Ensure that old record is not changed by update.
    """
    old_data = dict(a=1, b=2)
    new_data = dict(c=3)
    if new_key is not None:
        new_data["guid"] = new_key

    old_expected_resp = {
        "guid": old_key,
        "guid_type": "semi-structured",
    } | old_data

    new_expected_resp = {
        "guid": new_key,
        "guid_type": "semi-structured",
    } | new_data

    # Do not support updating metadata record
    client.post(f"/metadata/{old_key}", json=old_data).raise_for_status()
    try:
        resp = client.put(f"/semi-structured/{old_key}", json=new_data)
        assert resp.status_code == 400
    finally:
        client.delete(f"/metadata/{old_key}").raise_for_status()

    client.post(f"/semi-structured/{old_key}", json=old_data).raise_for_status()
    try:
        resp = client.put(f"/semi-structured/{old_key}", json=new_data)
        if not new_key:
            assert resp.status_code == 400
        elif new_key == old_key:
            assert resp.status_code == 409
        else:
            assert resp.status_code == 201
            try:
                assert resp.json() == new_expected_resp

                resp = client.get(f"/semi-structured/{old_key}")
                resp.raise_for_status()
                assert resp.json() == old_expected_resp

                resp = client.get(f"/semi-structured/{new_key}")
                resp.raise_for_status()
                assert resp.json() == new_expected_resp

            finally:
                client.delete(f"/semi-structured/{new_key}").raise_for_status()
    finally:
        client.delete(f"/semi-structured/{old_key}").raise_for_status()


@pytest.mark.parametrize(
    "key1,key2,key3",
    [
        ("test_semi_structured1", "test_semi_structured2", "test_semi_structured3"),
        (
            "dg.1234/test_semi_structured",
            "dg.2345/test_semi_structured",
            "dg.3456/test_semi_structured",
        ),
    ],
)
@pytest.mark.parametrize("data", [False, True])
@patch("datetime.datetime", Mock(utcnow=mocked_utcnow))
@pytest.mark.dependency(depends=["test_delete"])
def test_versions(client, key1, key2, key3, data):
    """
    Perform several updates and verify integrity of version history.
    Check both types of output by setting `data` test paramater.
    Ensure 404 if record with specified key doesn't exist.
    """
    reset_time()  # resets mocked time to DETERMINISTIC_START_TIME between test runs

    data1 = dict(guid=key1, a=1, b=2)
    data2 = dict(guid=key2, c=3)
    data3 = dict(guid=key3, d=4, e=5, f=6)

    if data:
        expected_resp = dict(
            versions=[
                data1
                | {
                    "guid_type": "semi-structured",
                    "created_date": "1970-01-01T00:00:00",
                },
                data2
                | {
                    "guid_type": "semi-structured",
                    "created_date": "1970-01-02T00:00:00",
                },
                data3
                | {
                    "guid_type": "semi-structured",
                    "created_date": "1970-01-03T00:00:00",
                },
            ]
        )
    else:
        expected_resp = dict(
            versions=[
                {"guid": key1},
                {"guid": key2},
                {"guid": key3},
            ]
        )

    # 404 if no existing record
    resp = client.get(f"/semi-structured/{key1}/versions")
    assert resp.status_code == 404

    client.post(f"/semi-structured/{key1}", json=data1).raise_for_status()
    try:
        # support for even just one version
        client.get(f"/semi-structured/{key1}/versions").raise_for_status()

        client.put(f"/semi-structured/{key1}", json=data2).raise_for_status()
        try:
            client.put(f"/semi-structured/{key1}", json=data3).raise_for_status()
            try:
                resp = client.get(f"/semi-structured/{key1}/versions?data={data}")
                assert resp.status_code == 200
                assert resp.json() == expected_resp

                resp = client.get(f"/semi-structured/{key2}/versions?data={data}")
                assert resp.status_code == 200
                assert resp.json() == expected_resp

                resp = client.get(f"/semi-structured/{key3}/versions?data={data}")
                assert resp.status_code == 200
                assert resp.json() == expected_resp

            finally:
                client.delete(f"/semi-structured/{key3}").raise_for_status()
        finally:
            client.delete(f"/semi-structured/{key2}").raise_for_status()
    finally:
        client.delete(f"/semi-structured/{key1}").raise_for_status()


@pytest.mark.parametrize(
    "key1,key2,key3",
    [
        ("test_semi_structured1", "test_semi_structured2", "test_semi_structured3"),
        (
            "dg.1234/test_semi_structured",
            "dg.2345/test_semi_structured",
            "dg.3456/test_semi_structured",
        ),
    ],
)
@patch("datetime.datetime", Mock(utcnow=mocked_utcnow))
@pytest.mark.dependency(depends=["test_delete"])
def test_latest(client, key1, key2, key3):
    """
    Perform several updates and verify latest record.
    Ensure 400 if calling on metadata record.
    Ensure 404 if record with specified key doesn't exist
    """
    reset_time()  # resets mocked time to DETERMINISTIC_START_TIME between test runs

    data1 = dict(guid=key1, a=1, b=2)
    data2 = dict(guid=key2, c=3)
    data3 = dict(guid=key3, d=4, e=5, f=6)

    expected_resp = {
        "guid_type": "semi-structured",
        "created_date": "1970-01-03T00:00:00",
    } | data3

    # 400 if metadata record
    client.post(f"/metadata/{key1}", json=data1).raise_for_status()
    try:
        resp = client.get(f"/semi-structured/{key1}/latest")
        assert resp.status_code == 400
    finally:
        client.delete(f"/metadata/{key1}").raise_for_status()

    # 404 if no existing record
    resp = client.get(f"/semi-structured/{key1}/latest")
    assert resp.status_code == 404

    client.post(f"/semi-structured/{key1}", json=data1).raise_for_status()
    try:
        # support for even just one version
        client.get(f"/semi-structured/{key1}/latest").raise_for_status()

        client.put(f"/semi-structured/{key1}", json=data2).raise_for_status()
        try:
            client.put(f"/semi-structured/{key1}", json=data3).raise_for_status()
            try:
                resp = client.get(f"/semi-structured/{key1}/latest")
                assert resp.status_code == 200
                assert resp.json() == expected_resp

                resp = client.get(f"/semi-structured/{key2}/latest")
                assert resp.status_code == 200
                assert resp.json() == expected_resp

                resp = client.get(f"/semi-structured/{key3}/latest")
                assert resp.status_code == 200
                assert resp.json() == expected_resp

            finally:
                client.delete(f"/semi-structured/{key3}").raise_for_status()
        finally:
            client.delete(f"/semi-structured/{key2}").raise_for_status()
    finally:
        client.delete(f"/semi-structured/{key1}").raise_for_status()


def test_authorization(client):
    """
    Ensure that create, update, and delete endpoints are protected by admin authorization.
    """
    tmp = config.DEBUG
    config.DEBUG = False

    try:
        assert client.post(f"/semi-structured/foobar", json={}).status_code == 403
        assert client.put(f"/semi-structured/foobar", json={}).status_code == 403
        assert client.delete(f"/semi-structured/foobar", json={}).status_code == 403

    finally:
        config.DEBUG = tmp
