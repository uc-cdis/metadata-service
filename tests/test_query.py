import gino
import pytest


@pytest.mark.parametrize("key", ["test_get", "dg.1234/test_get"])
def test_get(client, key):
    data = dict(a=1, b=2)
    client.post("/metadata/" + key, json=data).raise_for_status()
    try:
        assert client.get("/metadata/" + key).json() == data

        resp = client.get("/metadata/{}_not_exist".format(key))
        assert resp.status_code == 404
    finally:
        client.delete("/metadata/" + key)


def test_query_data(client):
    data = dict(a=1, b=2)
    try:
        client.post("/metadata/tqd_1", json=data).raise_for_status()
        client.post("/metadata/tqd_2", json=data).raise_for_status()
        assert client.get("/metadata").json() == ["tqd_1", "tqd_2"]
        assert client.get("/metadata?data=true").json() == {
            "tqd_1": data,
            "tqd_2": data,
        }
    finally:
        client.delete("/metadata/tqd_1")
        client.delete("/metadata/tqd_2")


def test_query_offset(client):
    """
    Test that the query offset and limit works as expected for various limits
    """
    try:
        client.post(
            "/metadata",
            json=[dict(guid=f"tqo_{i}", data=dict(x=1)) for i in range(64)],
        ).raise_for_status()
        for step in [1, 5, 10]:
            got = []
            offset = 0
            while len(got) < 64:
                delta = client.get(f"/metadata?offset={offset}&limit={step}").json()
                assert delta
                offset += len(delta)
                got += delta
            assert len(got) == 64
            assert len(set(got)) == 64
    finally:
        for i in range(64):
            client.delete(f"/metadata/tqo_{i}")


def test_query_offset_order(client):
    """
    Test that the query offset and limit returns the same order each time to ensure
    consistency in paginating.
    """
    try:
        client.post(
            "/metadata",
            json=[dict(guid=f"tqo_{i}", data=dict(x=1)) for i in range(64)],
        ).raise_for_status()
        for offset in [0, 1, 10, 64]:
            for limit in [0, 1, 10, 64]:
                a = client.get(f"/metadata?offset={offset}&limit={limit}").json()
                b = client.get(f"/metadata?offset={offset}&limit={limit}").json()
                c = client.get(f"/metadata?offset={offset}&limit={limit}").json()
                assert a == b == c
    finally:
        for i in range(64):
            client.delete(f"/metadata/tqo_{i}")


def test_query_offset_with_filter(client):
    """
    Test that the query offset and limit works even when filtering/subsetting the records
    by a specific field
    """
    try:
        client.post(
            "/metadata",
            json=[dict(guid=f"x_2_{i}", data=dict(x=2)) for i in range(64)],
        ).raise_for_status()
        client.post(
            "/metadata",
            json=[dict(guid=f"x_1_{i}", data=dict(x=1)) for i in range(64)],
        ).raise_for_status()
        for step in [1, 5, 10]:
            got = []
            offset = 0
            while len(got) < 64:
                delta = client.get(f"/metadata?offset={offset}&limit={step}&x=1").json()
                assert delta
                offset += len(delta)
                got += delta
            assert len(got) == 64
            assert len(set(got)) == 64
    finally:
        for i in range(64):
            client.delete(f"/metadata/x_2_{i}")
            client.delete(f"/metadata/x_1_{i}")


@pytest.mark.skipif(
    gino.__version__ <= "0.8.5", reason="https://github.com/fantix/gino/pull/609"
)
def test_query_filter(client):
    try:
        for guid, data in [
            ("tq_1", dict(a=dict(b=dict(c=3), d=4), e=5)),
            ("tq_2", dict(a=dict(b=dict(c=3), d=40), e=5)),
            ("tq_3", dict(a=dict(b=dict(c=3, d=4)), e=5)),
            ("tq_4", dict(a=dict(b=dict(c=3, d=4, e=5)))),
        ]:
            client.post(f"/metadata/{guid}", json=data).raise_for_status()
        assert list(sorted(client.get("/metadata").json())) == [
            "tq_1",
            "tq_2",
            "tq_3",
            "tq_4",
        ]
        assert list(sorted(client.get("/metadata?e=5").json())) == [
            "tq_1",
            "tq_2",
            "tq_3",
        ]
        assert list(sorted(client.get("/metadata?a.d=4").json())) == ["tq_1"]
        assert list(sorted(client.get("/metadata?a.b.d=4").json())) == ["tq_3", "tq_4"]
        assert list(sorted(client.get("/metadata?c=3&d=4").json())) == []
        assert list(sorted(client.get("/metadata?a.b.c=3&d=4").json())) == []
        assert list(sorted(client.get("/metadata?c=3&a.d=4").json())) == []
        assert list(sorted(client.get("/metadata?a.b.c=3&a.d=4").json())) == ["tq_1"]
        assert list(sorted(client.get("/metadata?a.b.c=3&a.d=4&a.d=40").json())) == [
            "tq_1",
            "tq_2",
        ]
    finally:
        client.delete("/metadata/tq_1")
        client.delete("/metadata/tq_2")
        client.delete("/metadata/tq_3")
        client.delete("/metadata/tq_4")


def test_query_filter_all_values(client):
    try:
        for guid, data in [
            ("tq_1", dict(a=1)),
            ("tq_2", dict(a=2)),
            ("tq_3", dict(a="")),
            ("tq_4", dict(a="*")),
            ("tq_5", dict(a=dict(b=3))),
            ("tq_6", dict(b=dict(a=1))),
            ("tq_7", dict(b=3)),
        ]:
            client.post(f"/metadata/{guid}", json=data).raise_for_status()

        # query all records with a value for field "a"
        assert list(sorted(client.get("/metadata?a=*").json())) == [
            "tq_1",
            "tq_2",
            "tq_3",
            "tq_4",
            "tq_5",
        ]

        # query all records with a value for field "a.b"
        assert list(sorted(client.get("/metadata?a.b=*").json())) == ["tq_5"]

        # query all records with a == "*"
        assert list(sorted(client.get("/metadata?a=\*").json())) == ["tq_4"]
    finally:
        for i in range(1, 8):
            client.delete(f"/metadata/tq_{i}")
