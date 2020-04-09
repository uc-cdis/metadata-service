import pytest


@pytest.mark.parametrize("key", ["test_create", "dg.1234/test_create"])
def test_create(client, key):
    data = dict(a=1, b=2)
    resp = client.post("/metadata/" + key, json=data)
    resp.raise_for_status()
    try:
        assert resp.status_code == 201
        assert resp.json() == data

        assert client.get("/metadata/" + key).json() == data

        resp = client.post("/metadata/" + key, json=data)
        assert resp.status_code == 409

        resp = client.post("/metadata/{}?overwrite=true".format(key), json=data)
        assert resp.status_code == 200
        assert resp.json() == data

    finally:
        client.delete("/metadata/" + key)


def test_batch_create(client):
    data = dict(a=1, b=2)
    try:
        resp = client.post(
            "/metadata", json=[dict(guid=f"tbc_{i}", data=data) for i in range(64)]
        )
        resp.raise_for_status()
        assert len(resp.json()["created"]) == 64
        assert len(resp.json()["updated"]) == 0
        assert len(resp.json()["conflict"]) == 0

        resp = client.post(
            "/metadata", json=[dict(guid=f"tbc_{i}", data=data) for i in range(32, 96)]
        )
        resp.raise_for_status()
        assert len(resp.json()["created"]) == 32
        assert len(resp.json()["updated"]) == 32
        assert len(resp.json()["conflict"]) == 0

        resp = client.post(
            "/metadata?overwrite=false",
            json=[dict(guid=f"tbc_{i}", data=data) for i in range(64, 128)],
        )
        resp.raise_for_status()
        assert len(resp.json()["created"]) == 32
        assert len(resp.json()["updated"]) == 0
        assert len(resp.json()["conflict"]) == 32

    finally:
        for i in range(128):
            client.delete(f"/metadata/tbc_{i}")


@pytest.mark.parametrize("key", ["test_update", "dg.1234/test_update"])
def test_update(client, key):
    data = dict(a=1, b=2)
    client.post("/metadata/" + key, json={}).raise_for_status()
    try:
        resp = client.put("/metadata/" + key, json=data)
        resp.raise_for_status()
        assert resp.json() == data

        resp = client.get("/metadata/" + key)
        resp.raise_for_status()
        assert resp.json() == data

        resp = client.put("/metadata/{}_not_existing".format(key), json=data)
        assert resp.status_code == 404

    finally:
        client.delete("/metadata/" + key)


@pytest.mark.parametrize("merge", [True, False])
@pytest.mark.parametrize("key", ["test_update_merge", "dg.1234/test_update_merge"])
def test_update_merge(client, merge, key):
    orig = dict(a=dict(x=1, y=2), b=2)
    new = dict(a=dict(z=3), c=3)
    key = "/metadata/" + key
    client.post(key, json=orig).raise_for_status()
    try:
        resp = client.put(key, params=dict(merge=merge), json=new)
        resp.raise_for_status()
        if merge:
            expected = orig.copy()
            expected.update(new)
        else:
            expected = new
        assert resp.json() == expected

        resp = client.get(key)
        resp.raise_for_status()
        assert resp.json() == expected

    finally:
        client.delete(key)


@pytest.mark.parametrize("key", ["test_delete", "dg.1234/test_delete"])
def test_delete(client, key):
    client.post("/metadata/" + key, json={}).raise_for_status()
    client.delete("/metadata/" + key).raise_for_status()
    resp = client.delete("/metadata/" + key)
    assert resp.status_code == 404
