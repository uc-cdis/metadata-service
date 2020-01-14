import importlib

from mds import config


def test_list(client):
    resp = client.get("/metadata_index")
    resp.raise_for_status()
    assert resp.json() == []

    client.post("/metadata_index/a.b.c").raise_for_status()

    try:
        resp = client.get("/metadata_index")
        resp.raise_for_status()
        assert resp.json() == ["a.b.c"]

    finally:
        client.delete("/metadata_index/a.b.c")


def test_list_access_control(monkeypatch, client):
    monkeypatch.setenv("DEBUG", "0")
    monkeypatch.setenv("ADMIN_LOGINS", "abc:123,def:456")
    importlib.reload(config)

    resp = client.get("/metadata_index")
    assert resp.status_code == 403

    resp = client.get("/metadata_index", auth=("abc", "456"))
    assert resp.status_code == 403

    resp = client.get("/metadata_index", auth=("abc", "123"))
    resp.raise_for_status()
    assert resp.json() == []


def test_create(client):
    assert client.get("/metadata_index").json() == []

    resp = client.post("/metadata_index/a.b.c")

    try:
        resp.raise_for_status()
        assert resp.status_code == 201

        resp = client.post("/metadata_index/a.b.c")
        assert resp.status_code == 409

    finally:
        client.delete("/metadata_index/a.b.c")


def test_delete(client):
    assert client.get("/metadata_index").json() == []

    client.post("/metadata_index/a.b.c").raise_for_status()
    resp = client.delete("/metadata_index/a.b.c")
    resp.raise_for_status()

    resp = client.delete("/metadata_index/a.b.c")
    assert resp.status_code == 404
