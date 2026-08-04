"""Tests for the Alembic database URL rendering used by ``migrations/env.py``.

A DB password containing reserved characters (e.g. ``&``, ``*``) gets
percent-encoded by SQLAlchemy when the connection URL is rendered
(``&`` -> ``%26``). Alembic stores ``sqlalchemy.url`` in a
``configparser.ConfigParser``, where ``%`` starts an interpolation token, so the
un-escaped value used to raise ``ValueError: invalid interpolation syntax``.
``render_alembic_dsn`` escapes ``%`` -> ``%%`` to prevent that.
"""

import configparser

from sqlalchemy.engine.url import URL, make_url

from mds.config import render_alembic_dsn

# Password with characters SQLAlchemy percent-encodes: @ & * and a literal %.
SPECIAL_PASSWORD = "p@ss&word*30%value"


def _make_dsn(password=SPECIAL_PASSWORD):
    return URL.create(
        drivername="postgresql+asyncpg",
        username="metadata_user",
        password=password,
        host="db",
        port=5432,
        database="metadata",
    )


def _configparser_roundtrip(rendered):
    """Mimic how Alembic stores/reads sqlalchemy.url via ConfigParser."""
    parser = configparser.ConfigParser()
    parser.add_section("alembic")
    parser.set("alembic", "sqlalchemy.url", rendered)
    return parser.get("alembic", "sqlalchemy.url")


def test_render_alembic_dsn_is_configparser_safe():
    """Rendering must survive ConfigParser and round-trip to the real URL."""
    rendered = render_alembic_dsn(_make_dsn())

    # Before the fix this line raised ValueError: invalid interpolation syntax.
    stored = _configparser_roundtrip(rendered)

    url = make_url(stored)
    assert url.password == SPECIAL_PASSWORD
    assert url.drivername == "postgresql"
    assert url.username == "metadata_user"
    assert url.host == "db"
    assert url.port == 5432
    assert url.database == "metadata"


def test_render_alembic_dsn_escapes_every_percent():
    """All percent signs must be doubled so ConfigParser treats them literally."""
    rendered = render_alembic_dsn(_make_dsn())

    assert "%%26" in rendered  # &
    assert "%%2A" in rendered  # *
    assert "%%40" in rendered  # @
    assert "%%25" in rendered  # literal %
    # No lone/odd percent may remain after collapsing the escaped pairs.
    assert "%" not in rendered.replace("%%", "")


def test_render_alembic_dsn_plain_password_has_no_escaping():
    """A password without reserved characters renders without any '%'."""
    rendered = render_alembic_dsn(_make_dsn(password="simplepass123"))

    assert "%" not in rendered
    assert make_url(rendered).password == "simplepass123"


def test_render_alembic_dsn_forces_postgresql_driver():
    """The async driver is normalized to plain postgresql for Alembic."""
    rendered = render_alembic_dsn(_make_dsn())
    assert rendered.startswith("postgresql://")
