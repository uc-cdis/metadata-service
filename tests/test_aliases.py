import httpx
import pytest
import urllib.parse


@pytest.mark.parametrize(
    "guid,aliases",
    [
        ("test_get_aliases", ["alias_a"]),
        ("dg.1234/test_get_aliases", ["alias_b"]),
        ("dg.2345/test_get_aliases", ["alias_b", "alias_b_2"]),
        ("dg.3456/test_get_aliases", ["1", "2", "3", "4", "5"]),
        ("dg.4567/test_get_aliases", []),
        ("dg.5678/test_get_aliases", None),
        ("dg.6789/test_get_aliases", ["dg.1234/test_get_aliases"]),
        ("dg.7890/test_get_aliases", ["!@(#_*&$)^-!@#)%*_(&"]),
        ("dg.8890/test_get_aliases", ["/\\|_.,-;__"]),
    ],
)
@pytest.mark.parametrize("is_post", [True, False])
def test_create_read_delete_new_aliases(guid, aliases, client, is_post):
    """
    Create a metadata record, then try POST and PUT new aliases.
    Ensure you are able to GET the new aliases and DELETE them one by one.
    """

    data = dict(a=1, b=2)
    client.post(f"/metadata/{guid}", json=data).raise_for_status()

    if is_post:
        res = client.post(f"/metadata/{guid}/aliases", json={"aliases": aliases})
        if aliases is None:  # the endpoint expects a list
            with pytest.raises(httpx.HTTPStatusError, match="422 Unprocessable Entity"):
                res.raise_for_status()
        else:
            res.raise_for_status()
    else:
        # use PUT instead of POST, should result in same behavior for new aliases
        res = client.put(f"/metadata/{guid}/aliases", json={"aliases": aliases})
        if aliases is None:  # the endpoint expects a list
            with pytest.raises(httpx.HTTPStatusError, match="422 Unprocessable Entity"):
                res.raise_for_status()
        else:
            res.raise_for_status()

    # convert None to empty list for comparison of output
    # because we always expect the output to be a list, even if the input is not
    aliases = aliases or []

    try:
        assert client.get(f"/metadata/{guid}").json() == data
        assert client.get(f"/metadata/{guid}/aliases").json().get("aliases") == sorted(
            aliases
        )
        assert client.get(f"/metadata/{guid}/aliases").json().get("guid") == guid

        for alias in aliases:
            # NOTE you have to percent encode aliases in urls
            assert client.get(f"/metadata/{urllib.parse.quote(alias)}").json() == data

        for alias in aliases:
            client.delete(
                f"/metadata/{guid}/aliases/{urllib.parse.quote(alias)}"
            ).raise_for_status()
            assert alias not in client.get(f"/metadata/{guid}/aliases").json().get(
                "aliases"
            )
    finally:
        client.delete(f"/metadata/{guid}").raise_for_status()


@pytest.mark.parametrize(
    "guid,aliases",
    [
        ("test_get_aliases", ["alias_a"]),
        ("dg.1234/test_get_aliases", ["alias_b"]),
        ("dg.2345/test_get_aliases", ["alias_b", "alias_b_2"]),
        ("dg.3456/test_get_aliases", ["1", "2", "3", "4", "5"]),
        ("dg.5678/test_get_aliases", ["dg.1234/test_get_aliases"]),
        ("dg.6789/test_get_aliases", ["!@(#_*&$)^-!@#)%*_(&"]),
        ("dg.7890/test_get_aliases", ["/\\|_.,-;__"]),
    ],
)
def test_create_already_created_aliases(guid, aliases, client):
    """
    Ensure non-successful response when trying to POST an already existing record
    """
    data = dict(a=1, b=2)
    client.post(f"/metadata/{guid}", json=data).raise_for_status()

    client.post(
        f"/metadata/{guid}/aliases", json={"aliases": aliases}
    ).raise_for_status()

    try:
        response = client.post(
            f"/metadata/{guid}/aliases", json={"aliases": ["new", "stuff"]}
        )
        assert str(response.status_code) == "409"

        # ensure the original aliases data did not get modified
        assert client.get(f"/metadata/{guid}").json() == data
        assert client.get(f"/metadata/{guid}/aliases").json().get("aliases") == sorted(
            aliases
        )
        assert client.get(f"/metadata/{guid}/aliases").json().get("guid") == guid

        for alias in aliases:
            # NOTE you have to percent encode aliases in urls
            assert client.get(f"/metadata/{urllib.parse.quote(alias)}").json() == data
    finally:
        client.delete(f"/metadata/{guid}").raise_for_status()


@pytest.mark.parametrize(
    "guid1,aliases1,guid2,aliases2",
    [
        ("test_get_aliases", ["alias_a"], "test_get_aliases2", ["alias_a"]),
        (
            "dg.1234/test_get_aliases",
            ["alias_b"],
            "dg.1234/test_get_aliases2",
            ["alias_b"],
        ),
        (
            "dg.2345/test_get_aliases",
            ["alias_b", "alias_b_2"],
            "dg.1234/test_get_aliases2",
            ["alias_b_2", "new_one"],
        ),
        (
            "dg.6789/test_get_aliases",
            ["!@(#_*&$)^-!@#)%*_(&"],
            "dg.1234/test_get_aliases2",
            ["!@(#_*&$)^-!@#)%*_(&", "new_one"],
        ),
        (
            "dg.7890/test_get_aliases",
            ["/\\|_.,-;__"],
            "dg.1234/test_get_aliases2",
            ["!@(#_*&$)^-!@#)%*_(&", "new_one", "/\\|_.,-;__"],
        ),
    ],
)
def test_create_already_created_aliases_on_different_guid(
    guid1, aliases1, guid2, aliases2, client
):
    """
    Ensure non-successful response when trying to POST an already existing alias
    """
    data = dict(a=1, b=2)
    client.post(f"/metadata/{guid1}", json=data).raise_for_status()
    client.post(f"/metadata/{guid2}", json=data).raise_for_status()

    client.post(
        f"/metadata/{guid1}/aliases", json={"aliases": aliases1}
    ).raise_for_status()

    try:
        response = client.post(f"/metadata/{guid2}/aliases", json={"aliases": aliases2})
        assert str(response.status_code) == "409"

        # ensure the original aliases data did not get modified
        assert client.get(f"/metadata/{guid1}").json() == data
        assert client.get(f"/metadata/{guid1}/aliases").json().get("aliases") == sorted(
            aliases1
        )
        assert client.get(f"/metadata/{guid1}/aliases").json().get("guid") == guid1

        for alias in aliases1:
            # NOTE you have to percent encode aliases in urls
            assert client.get(f"/metadata/{urllib.parse.quote(alias)}").json() == data
    finally:
        client.delete(f"/metadata/{guid1}").raise_for_status()
        client.delete(f"/metadata/{guid2}").raise_for_status()


@pytest.mark.parametrize(
    "guid,aliases,updates",
    [
        ("test_get_aliases", ["alias_a"], ["alias_a", "another_alias"]),
        ("test_get_aliases", ["alias_a"], ["alias_a"]),
        (
            "test_get_aliases",
            ["alias_a"],
            ["new_alias", "alias_a", "another_alias"],
        ),
        ("test_get_aliases", ["alias_a"], ["only_alias"]),
        (
            "test_get_aliases",
            ["alias_a"],
            ["duplicate_alias", "duplicate_alias"],
        ),
        ("test_get_aliases", ["alias_a"], []),
    ],
)
@pytest.mark.parametrize("merge", [True, False])
def test_update_already_created_aliases(guid, aliases, updates, merge, client):
    """
    Ensure successful response when trying to PUT an already existing record with
    new aliases
    """
    data = dict(a=1, b=2)
    client.post(f"/metadata/{guid}", json=data).raise_for_status()

    client.post(
        f"/metadata/{guid}/aliases", json={"aliases": aliases}
    ).raise_for_status()

    try:
        response = client.put(
            f"/metadata/{guid}/aliases?merge={merge}", json={"aliases": updates}
        )
        assert str(response.status_code).startswith("20")

        if merge:
            expected_aliases = sorted(list(set(aliases) | set(updates)))
        else:
            expected_aliases = sorted(list(set(updates)))

        # ensure the new aliases exist
        assert client.get(f"/metadata/{guid}").json() == data
        assert (
            client.get(f"/metadata/{guid}/aliases").json().get("aliases")
            == expected_aliases
        )
        assert client.get(f"/metadata/{guid}/aliases").json().get("guid") == guid

        for alias in expected_aliases:
            # NOTE you have to percent encode aliases in urls
            assert client.get(f"/metadata/{urllib.parse.quote(alias)}").json() == data

        # old aliases should no longer work/exist
        for alias in aliases:
            if alias not in expected_aliases:
                response = client.get(f"/metadata/{urllib.parse.quote(alias)}")
                assert str(response.status_code) == "404"
    finally:
        client.delete(f"/metadata/{guid}").raise_for_status()


@pytest.mark.parametrize(
    "guid,aliases",
    [
        ("test_get_aliases", ["alias_a"]),
        ("dg.1234/test_get_aliases", ["alias_b"]),
        ("dg.2345/test_get_aliases", ["alias_b", "alias_b_2"]),
        ("dg.3456/test_get_aliases", ["1", "2", "3", "4", "5"]),
        ("dg.5678/test_get_aliases", ["dg.1234/test_get_aliases"]),
        ("dg.6789/test_get_aliases", ["!@(#_*&$)^-!@#)%*_(&"]),
        ("dg.7890/test_get_aliases", ["/\\|_.,-;__"]),
        ("dg.1234/test_get_aliases", []),
    ],
)
def test_delete_all_aliases(guid, aliases, client):
    """
    Ensure the delete all endpoint works as expected and removes all aliases
    """
    data = dict(a=1, b=2)
    client.post(f"/metadata/{guid}", json=data).raise_for_status()

    client.post(
        f"/metadata/{guid}/aliases", json={"aliases": aliases}
    ).raise_for_status()

    try:
        response = client.delete(f"/metadata/{guid}/aliases")
        if aliases:
            assert str(response.status_code).startswith("20")
        else:
            # if there is no data to delete, expect a 404
            assert str(response.status_code) == "404"

        # ensure no aliases exists but the GUID is still there with data
        assert client.get(f"/metadata/{guid}").json() == data
        assert not client.get(f"/metadata/{guid}/aliases").json().get("aliases")
        assert client.get(f"/metadata/{guid}/aliases").json().get("guid") == guid

        # old aliases should no longer work/exist
        for alias in aliases:
            response = client.get(f"/metadata/{urllib.parse.quote(alias)}")
            assert str(response.status_code) == "404"
    finally:
        client.delete(f"/metadata/{guid}").raise_for_status()
