def test_create(client):
    data = dict(a=1, b=2)
    resp = client.post("/metadata/test_create", json=data)
    resp.raise_for_status()
    try:
        assert resp.status_code == 201
        assert resp.json() == data

        assert client.get("/metadata/test_create").json() == data

        resp = client.post("/metadata/test_create", json=data)
        assert resp.status_code == 409

        resp = client.post("/metadata/test_create?overwrite=true", json=data)
        assert resp.status_code == 200
        assert resp.json() == data

    finally:
        client.delete("/metadata/test_create")


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


def test_update(client):
    data = dict(a=1, b=2)
    client.post("/metadata/test_update", json={}).raise_for_status()
    try:
        resp = client.put("/metadata/test_update", json=data)
        resp.raise_for_status()
        assert resp.json() == data

        resp = client.get("/metadata/test_update")
        resp.raise_for_status()
        assert resp.json() == data

        resp = client.put("/metadata/test_update_not_existing", json=data)
        assert resp.status_code == 404

    finally:
        client.delete("/metadata/test_update")


def test_delete(client):
    client.post("/metadata/test_delete", json={}).raise_for_status()
    client.delete("/metadata/test_delete").raise_for_status()
    resp = client.delete("/metadata/test_delete")
    assert resp.status_code == 404
